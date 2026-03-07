#!/usr/bin/env python3
"""
Multi-query streaming test for demoflight.

Tests:
1. Multiple concurrent queries on different entity types
2. Incremental streaming (data arrives over time, not all at once)
3. Parallel consumption of multiple streams

Usage:
    cd /home/matt/projects/deadlock/demoflight
    .venv/bin/python scripts/multi_query_test.py
"""

import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import httpx
import pyarrow.flight as flight

DEADLOCK_API_BASE = "https://api.deadlock-api.com"
DEMOFLIGHT_ADDR = "grpc://localhost:50051"

DEMOFLIGHT_BIN = Path(__file__).parent.parent / "target" / "release" / "demoflight"
if not DEMOFLIGHT_BIN.exists():
    DEMOFLIGHT_BIN = Path(__file__).parent.parent / "target" / "debug" / "demoflight"


def get_api_key() -> str | None:
    key_path = Path.home() / ".deadlock-api.key"
    if key_path.exists():
        return key_path.read_text().strip()
    return os.environ.get("DEADLOCK_API_KEY")


def get_broadcast_url() -> tuple[int, str]:
    """Find an active match with broadcast URL."""
    api_key = get_api_key()
    headers = {"X-API-Key": api_key} if api_key else {}

    resp = httpx.get(
        f"{DEADLOCK_API_BASE}/v1/matches/active", headers=headers, timeout=30
    )
    resp.raise_for_status()
    matches = resp.json()

    if not matches:
        raise RuntimeError("No active matches found")

    matches.sort(key=lambda m: m.get("spectators", 0), reverse=True)

    for match in matches[:20]:
        match_id = match["match_id"]
        try:
            resp = httpx.get(
                f"{DEADLOCK_API_BASE}/v1/matches/{match_id}/live/url",
                headers=headers,
                timeout=10,
            )
            if resp.status_code == 200:
                broadcast_url = resp.json().get("broadcast_url")
                if broadcast_url:
                    return match_id, broadcast_url
        except Exception:
            continue

    raise RuntimeError("No match with broadcast URL found")


def _encode_varint(value: int) -> bytes:
    result = []
    while value > 127:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value)
    return bytes(result)


def _decode_varint(data: bytes, start: int) -> tuple[int, int]:
    result = 0
    shift = 0
    i = start
    while i < len(data):
        byte = data[i]
        result |= (byte & 0x7F) << shift
        i += 1
        if (byte & 0x80) == 0:
            break
        shift += 7
    return result, i


def encode_register_source_request(source_url: str) -> bytes:
    source_type = 1
    source_type_bytes = b"\x08" + bytes([source_type])
    url_bytes = source_url.encode("utf-8")
    url_field = b"\x12" + _encode_varint(len(url_bytes)) + url_bytes
    return source_type_bytes + url_field


def decode_register_source_response(data: bytes) -> dict:
    result = {"session_token": "", "tables": []}
    i = 0
    while i < len(data):
        if i >= len(data):
            break
        field_byte = data[i]
        field_num = field_byte >> 3
        wire_type = field_byte & 0x07
        i += 1
        if wire_type == 2:
            length, i = _decode_varint(data, i)
            value = data[i : i + length]
            i += length
            if field_num == 1:
                result["session_token"] = value.decode("utf-8")
            elif field_num == 2:
                table_name = ""
                j = 0
                while j < len(value):
                    if value[j] == 0x0A:
                        j += 1
                        name_len, j = _decode_varint(value, j)
                        table_name = value[j : j + name_len].decode("utf-8")
                        break
                    j += 1
                if table_name:
                    result["tables"].append(table_name)
        else:
            break
    return result


def encode_command_statement_query(query: str) -> bytes:
    from google.protobuf.any_pb2 import Any as AnyProto

    query_bytes = query.encode("utf-8")
    length_varint = _encode_varint(len(query_bytes))
    command_bytes = b"\x0a" + length_varint + query_bytes

    any_msg = AnyProto()
    any_msg.type_url = (
        "type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery"
    )
    any_msg.value = command_bytes
    return any_msg.SerializeToString()


def start_server() -> subprocess.Popen:
    if not DEMOFLIGHT_BIN.exists():
        raise FileNotFoundError(f"demoflight binary not found at {DEMOFLIGHT_BIN}")

    env = os.environ.copy()
    env["DEMOFLIGHT_JWT_SECRET"] = "multi-query-test-secret-12345"
    env["DEMOFLIGHT_LISTEN_ADDR"] = "[::]:50051"
    env["RUST_LOG"] = "info"

    proc = subprocess.Popen(
        [str(DEMOFLIGHT_BIN)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    time.sleep(1.5)

    if proc.poll() is not None:
        output = proc.stdout.read() if proc.stdout else ""
        raise RuntimeError(f"demoflight failed to start: {output}")

    print(f"[+] Server started (PID {proc.pid})")
    return proc


def timestamp() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


class QueryStreamer(threading.Thread):
    """Thread that consumes a single query stream."""

    def __init__(self, name: str, ticket: flight.Ticket, stop_event: threading.Event):
        super().__init__(daemon=True)
        self.name = name
        self.ticket = ticket
        self.stop_event = stop_event
        self.batches = []
        self.total_rows = 0
        self.error = None
        self.schema = None
        self.first_batch_time = None
        self.last_batch_time = None

    def run(self):
        try:
            client = flight.FlightClient(DEMOFLIGHT_ADDR)
            reader = client.do_get(self.ticket)

            for chunk in reader:
                if self.stop_event.is_set():
                    break
                if chunk.data is None:
                    continue

                now = time.time()
                if self.first_batch_time is None:
                    self.first_batch_time = now
                    self.schema = chunk.data.schema
                self.last_batch_time = now

                rows = chunk.data.num_rows
                self.total_rows += rows
                self.batches.append((now, rows))

                # Print first few batches with timestamp
                if len(self.batches) <= 5:
                    print(
                        f"  [{timestamp()}] {self.name}: batch {len(self.batches)}, {rows} rows (total: {self.total_rows})"
                    )
                elif len(self.batches) == 6:
                    print(f"  [{timestamp()}] {self.name}: ... (continuing)")

        except Exception as e:
            self.error = e
            print(f"  [{timestamp()}] {self.name}: ERROR - {e}")


def run_multi_query_test(broadcast_url: str, match_id: int, duration_secs: int = 30):
    print(f"\n{'=' * 70}")
    print(f"MULTI-QUERY STREAMING TEST - Match {match_id}")
    print(f"{'=' * 70}")
    print(f"Broadcast: {broadcast_url[:60]}...")
    print(f"Duration: {duration_secs} seconds")

    client = flight.FlightClient(DEMOFLIGHT_ADDR)

    # Step 1: Register source
    print(f"\n[{timestamp()}] Registering source...")
    action_body = encode_register_source_request(broadcast_url)
    action = flight.Action("demoflight.register_source", action_body)
    results = list(client.do_action(action))
    response = decode_register_source_response(results[0].body.to_pybytes())
    session_token = response["session_token"]
    tables = response["tables"]
    print(f"  Session token: {session_token[:40]}...")
    print(f"  Tables: {len(tables)}")

    # Step 2: Submit multiple queries (NO LIMIT - continuous streaming)
    queries = [
        ("PlayerPawn", "SELECT tick, entity_index, delta_type FROM CCitadelPlayerPawn"),
        ("Trooper", "SELECT tick, entity_index, delta_type FROM CNPC_Trooper"),
        (
            "Controller",
            "SELECT tick, entity_index, delta_type FROM CCitadelPlayerController",
        ),
    ]

    options = flight.FlightCallOptions(
        headers=[(b"authorization", f"Bearer {session_token}".encode())]
    )

    tickets = []
    print(f"\n[{timestamp()}] Submitting {len(queries)} queries...")
    for name, sql in queries:
        cmd_bytes = encode_command_statement_query(sql)
        descriptor = flight.FlightDescriptor.for_command(cmd_bytes)
        info = client.get_flight_info(descriptor, options)
        ticket = info.endpoints[0].ticket
        tickets.append((name, ticket))
        print(f"  {name}: submitted")

    # Step 3: Start streaming all queries in parallel
    print(f"\n[{timestamp()}] Starting parallel streaming...")
    stop_event = threading.Event()
    streamers = []

    for name, ticket in tickets:
        streamer = QueryStreamer(name, ticket, stop_event)
        streamer.start()
        streamers.append(streamer)

    # Step 4: Let it run for duration_secs
    print(f"\n[{timestamp()}] Streaming data (will run for {duration_secs}s)...\n")
    try:
        start_time = time.time()
        last_status = start_time

        while time.time() - start_time < duration_secs:
            time.sleep(0.5)

            # Print status every 5 seconds
            if time.time() - last_status >= 5:
                last_status = time.time()
                elapsed = int(time.time() - start_time)
                totals = ", ".join(f"{s.name}:{s.total_rows}" for s in streamers)
                print(f"  [{timestamp()}] {elapsed}s elapsed - rows: {totals}")

    except KeyboardInterrupt:
        print(f"\n[{timestamp()}] Interrupted by user")

    # Step 5: Stop and collect results
    print(f"\n[{timestamp()}] Stopping streams...")
    stop_event.set()

    for streamer in streamers:
        streamer.join(timeout=2)

    # Step 6: Print summary
    print(f"\n{'=' * 70}")
    print("RESULTS SUMMARY")
    print(f"{'=' * 70}")

    all_success = True
    for streamer in streamers:
        status = "OK" if streamer.error is None else f"ERROR: {streamer.error}"
        batches = len(streamer.batches)
        rows = streamer.total_rows

        if streamer.first_batch_time and streamer.last_batch_time:
            duration = streamer.last_batch_time - streamer.first_batch_time
            duration_str = f"{duration:.1f}s"
        else:
            duration_str = "N/A"

        print(f"\n{streamer.name}:")
        print(f"  Status: {status}")
        print(f"  Schema: {streamer.schema}")
        print(f"  Batches: {batches}")
        print(f"  Total rows: {rows}")
        print(f"  Stream duration: {duration_str}")

        if batches > 0:
            avg_batch_size = rows / batches
            print(f"  Avg batch size: {avg_batch_size:.1f} rows")

        if streamer.error:
            all_success = False
        if batches == 0:
            all_success = False
            print(f"  WARNING: No data received!")

    # Verify incremental streaming
    print(f"\n{'=' * 70}")
    print("INCREMENTAL STREAMING VERIFICATION")
    print(f"{'=' * 70}")

    for streamer in streamers:
        if len(streamer.batches) >= 2:
            times = [b[0] for b in streamer.batches]
            first_to_last = times[-1] - times[0]
            gaps = [times[i + 1] - times[i] for i in range(min(5, len(times) - 1))]
            avg_gap = sum(gaps) / len(gaps) if gaps else 0

            print(f"\n{streamer.name}:")
            print(f"  First batch to last batch: {first_to_last:.2f}s")
            print(f"  Avg gap between first 5 batches: {avg_gap * 1000:.0f}ms")

            if first_to_last > 1.0:
                print(
                    f"  ✓ INCREMENTAL: Data arrived over {first_to_last:.1f}s (not all at once)"
                )
            else:
                print(
                    f"  ? Data arrived quickly ({first_to_last:.2f}s) - may not be truly incremental"
                )

    # Step 7: Close session
    print(f"\n[{timestamp()}] Closing session...")
    try:
        token_bytes = session_token.encode()
        close_body = b"\x0a" + _encode_varint(len(token_bytes)) + token_bytes
        action = flight.Action("demoflight.close_session", close_body)
        list(client.do_action(action))
        print("  Session closed")
    except Exception as e:
        print(f"  Failed to close session: {e}")

    print(f"\n{'=' * 70}")
    if all_success:
        print("TEST PASSED - All queries streamed successfully")
    else:
        print("TEST FAILED - Some queries had errors or no data")
    print(f"{'=' * 70}")

    return all_success


def main():
    print("=" * 70)
    print("DEMOFLIGHT MULTI-QUERY STREAMING TEST")
    print("=" * 70)

    # Get broadcast URL
    print("\n[*] Finding active match...")
    match_id, broadcast_url = get_broadcast_url()
    print(f"[+] Found match {match_id}")

    # Start server
    print("\n[*] Starting demoflight server...")
    server_proc = start_server()

    try:
        time.sleep(1)
        success = run_multi_query_test(broadcast_url, match_id, duration_secs=30)
        sys.exit(0 if success else 1)

    finally:
        print("\n[*] Stopping server...")
        server_proc.send_signal(signal.SIGTERM)
        try:
            server_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server_proc.kill()
        print("[+] Server stopped")


if __name__ == "__main__":
    main()
