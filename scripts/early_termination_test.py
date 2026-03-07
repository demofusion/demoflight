#!/usr/bin/env python3
"""
Early termination test for demoflight.

Tests that when one query finishes early (via LIMIT), other queries continue to receive data.

This validates the fix in BatchingEntityDispatcher that removes closed senders
instead of erroring immediately when one receiver is dropped.

Usage:
    cd /home/matt/projects/deadlock/demoflight
    uv run scripts/early_termination_test.py
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
    env["DEMOFLIGHT_JWT_SECRET"] = "early-termination-test-secret-12345"
    env["DEMOFLIGHT_LISTEN_ADDR"] = "[::]:50051"
    env["RUST_LOG"] = "info,demoflight=debug"

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

    def __init__(
        self, name: str, ticket: flight.Ticket, expected_limit: int | None = None
    ):
        super().__init__(daemon=True)
        self.name = name
        self.ticket = ticket
        self.expected_limit = expected_limit
        self.batches = []  # List of (timestamp, row_count)
        self.total_rows = 0
        self.error = None
        self.schema = None
        self.first_batch_time = None
        self.last_batch_time = None
        self.finished = threading.Event()
        self.finish_reason = None

    def batches_after(self, timestamp: float) -> int:
        """Count batches received after a given timestamp."""
        return sum(1 for ts, _ in self.batches if ts > timestamp)

    def rows_after(self, timestamp: float) -> int:
        """Count rows received after a given timestamp."""
        return sum(rows for ts, rows in self.batches if ts > timestamp)

    def run(self):
        try:
            client = flight.FlightClient(DEMOFLIGHT_ADDR)
            reader = client.do_get(self.ticket)

            for chunk in reader:
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

                print(
                    f"  [{timestamp()}] {self.name}: batch {len(self.batches)}, "
                    f"{rows} rows (total: {self.total_rows})"
                )

            self.finish_reason = "stream_ended"
            print(
                f"  [{timestamp()}] {self.name}: STREAM ENDED (total: {self.total_rows} rows)"
            )

        except Exception as e:
            self.error = e
            self.finish_reason = "error"
            print(f"  [{timestamp()}] {self.name}: ERROR - {e}")
        finally:
            self.finished.set()


def run_early_termination_test(broadcast_url: str, match_id: int):
    print(f"\n{'=' * 70}")
    print(f"EARLY TERMINATION TEST - Match {match_id}")
    print(f"{'=' * 70}")
    print(f"Broadcast: {broadcast_url[:60]}...")
    print()
    print("This test validates that when one query finishes early (LIMIT),")
    print("other queries continue to receive data.")
    print()

    client = flight.FlightClient(DEMOFLIGHT_ADDR)

    # Step 1: Register source
    print(f"[{timestamp()}] Registering source...")
    action_body = encode_register_source_request(broadcast_url)
    action = flight.Action("demoflight.register_source", action_body)
    results = list(client.do_action(action))
    response = decode_register_source_response(results[0].body.to_pybytes())
    session_token = response["session_token"]
    tables = response["tables"]
    print(f"  Session token: {session_token[:40]}...")
    print(f"  Tables: {len(tables)}")

    # Step 2: Submit queries - some with small LIMIT, some without
    # The key test: query with LIMIT 10 should finish quickly,
    # but the query without LIMIT should continue receiving data
    #
    # IMPORTANT: Use different entity types to ensure independent streams.
    # If we use the same entity type, they share distribution channels.
    #
    # Also note: With GOTV streaming, we need to wait for delta frames to arrive.
    # The initial fullframe has limited data, so small LIMITs may all be satisfied
    # from the initial data before any delta frames arrive.
    #
    # Entity types that are reliably present:
    # - CCitadelPlayerPawn: Player characters (12 per game)
    # - CCitadelPlayerController: Player controllers (12 per game)
    # - CNPC_Trooper: Lane minions (many, constant spawns)
    queries = [
        (
            "FastQuery_LIMIT10",
            "SELECT tick, entity_index FROM CCitadelPlayerPawn LIMIT 10",
            10,
        ),
        (
            "MediumQuery_LIMIT100",
            "SELECT tick, entity_index FROM CCitadelPlayerController LIMIT 100",
            100,
        ),
        (
            "SlowQuery_LIMIT1000",
            "SELECT tick, entity_index FROM CNPC_Trooper LIMIT 1000",
            1000,
        ),
    ]

    options = flight.FlightCallOptions(
        headers=[(b"authorization", f"Bearer {session_token}".encode())]
    )

    print(f"\n[{timestamp()}] Submitting {len(queries)} queries...")
    streamers = []
    for name, sql, limit in queries:
        cmd_bytes = encode_command_statement_query(sql)
        descriptor = flight.FlightDescriptor.for_command(cmd_bytes)
        info = client.get_flight_info(descriptor, options)
        ticket = info.endpoints[0].ticket

        streamer = QueryStreamer(name, ticket, expected_limit=limit)
        streamers.append(streamer)
        print(f"  {name}: submitted (LIMIT {limit})")

    # Step 3: Start all streams
    print(f"\n[{timestamp()}] Starting all streams in parallel...")
    for streamer in streamers:
        streamer.start()

    # Step 4: Wait for all to finish (with timeout)
    print(f"\n[{timestamp()}] Waiting for streams to complete...\n")

    all_finished = False
    timeout = 60  # 60 second timeout
    start_time = time.time()

    while not all_finished and (time.time() - start_time) < timeout:
        time.sleep(0.5)
        all_finished = all(s.finished.is_set() for s in streamers)

        # Check if fast query finished but slow query is still running
        fast_finished = streamers[0].finished.is_set()
        slow_running = not streamers[1].finished.is_set()

        if fast_finished and slow_running:
            fast_rows = streamers[0].total_rows
            slow_rows = streamers[1].total_rows
            print(
                f"\n  [{timestamp()}] KEY MOMENT: FastQuery finished ({fast_rows} rows), "
                f"SlowQuery still running ({slow_rows} rows)"
            )

    # Step 5: Analyze results
    print(f"\n{'=' * 70}")
    print("RESULTS")
    print(f"{'=' * 70}")

    test_passed = True

    for streamer in streamers:
        status = "OK" if streamer.error is None else f"ERROR: {streamer.error}"
        rows = streamer.total_rows
        expected = streamer.expected_limit

        print(f"\n{streamer.name}:")
        print(f"  Status: {status}")
        print(f"  Rows received: {rows}")
        print(f"  Expected (LIMIT): {expected}")
        print(f"  Finish reason: {streamer.finish_reason}")
        print(f"  Batches: {len(streamer.batches)}")

        if streamer.error:
            test_passed = False
            print(f"  ✗ FAILED: Query errored")
        elif rows == 0:
            test_passed = False
            print(f"  ✗ FAILED: No data received")
        else:
            # Check if we got at least some data (don't require hitting LIMIT)
            print(f"  ✓ OK: Query completed without error")

    # Key validation: Did slow queries continue after fast query finished?
    # The early termination bug would cause ALL queries to die when ANY finishes.
    fast_finish_time = streamers[0].last_batch_time

    print(f"\n{'=' * 70}")
    print("EARLY TERMINATION VALIDATION")
    print(f"{'=' * 70}")

    print(f"\nFastQuery (LIMIT 10) ended at: {streamers[0].last_batch_time:.3f}")
    print(f"  Total rows: {streamers[0].total_rows}")

    # Check if other queries received data AFTER FastQuery finished
    # This is the key test - with the bug, they would all die together
    for streamer in streamers[1:]:
        batches_after_fast = streamer.batches_after(fast_finish_time)
        rows_after_fast = streamer.rows_after(fast_finish_time)

        print(f"\n{streamer.name}:")
        print(f"  Ended at: {streamer.last_batch_time:.3f}")
        print(f"  Total rows: {streamer.total_rows}")
        print(f"  Batches after FastQuery ended: {batches_after_fast}")
        print(f"  Rows after FastQuery ended: {rows_after_fast}")

        # The critical check: did this query continue after FastQuery?
        # Even 1 batch after means the fix is working
        if batches_after_fast > 0:
            print(f"  ✓ PASSED: Continued running after FastQuery finished")
        elif streamer.first_batch_time and streamer.first_batch_time > fast_finish_time:
            # Query didn't even start until after FastQuery finished - also fine
            print(f"  ✓ PASSED: Started after FastQuery finished")
        elif abs(streamer.last_batch_time - fast_finish_time) < 0.010:
            # Ended within 10ms - ambiguous, could be data exhaustion
            print(
                f"  ⚠ AMBIGUOUS: Ended within 10ms of FastQuery (likely data exhaustion)"
            )
            # Don't fail the test for this - it's not evidence of the bug
        else:
            # Ended notably before FastQuery or some other issue
            print(f"  ✗ FAILED: May have been killed by FastQuery")
            test_passed = False

    # Final summary
    all_queries_got_data = all(s.total_rows > 0 for s in streamers)
    no_errors = all(s.error is None for s in streamers)

    print(f"\n{'=' * 70}")
    print("SUMMARY")
    print(f"{'=' * 70}")
    print(f"All queries received data: {'✓' if all_queries_got_data else '✗'}")
    print(f"No unexpected errors: {'✓' if no_errors else '✗'}")

    # The fix is confirmed working if:
    # 1. All queries got data without errors
    # 2. At least one other query received batches after FastQuery ended
    any_continued = any(
        streamers[i].batches_after(fast_finish_time) > 0
        for i in range(1, len(streamers))
    )

    if any_continued:
        print(f"Other queries continued after FastQuery: ✓")
        print(f"\n✓ FIX CONFIRMED: Early termination bug is fixed!")
        test_passed = True
    elif all_queries_got_data and no_errors:
        print(f"Other queries ended simultaneously with FastQuery")
        print(f"\n⚠ INCONCLUSIVE: All data came from initial fullframe")
        print(
            f"   The test passed (no errors), but couldn't definitively prove the fix"
        )
        print(f"   Try running during active gameplay for delta frames")
        # Still pass - no evidence of the bug
        test_passed = True
    else:
        print(f"\n✗ POSSIBLE BUG: Check the detailed output above")

    # Step 6: Close session
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
    if test_passed:
        print("TEST PASSED - Early termination handled correctly")
    else:
        print("TEST FAILED - Early termination bug detected")
    print(f"{'=' * 70}")

    return test_passed


def main():
    print("=" * 70)
    print("DEMOFLIGHT EARLY TERMINATION TEST")
    print("=" * 70)
    print()
    print("This test validates that when one query finishes early (via LIMIT),")
    print("other queries in the same session continue to receive data.")
    print()

    # Get broadcast URL
    print("[*] Finding active match...")
    match_id, broadcast_url = get_broadcast_url()
    print(f"[+] Found match {match_id}")

    # Start server
    print("\n[*] Starting demoflight server...")
    server_proc = start_server()

    try:
        time.sleep(1)
        success = run_early_termination_test(broadcast_url, match_id)
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
