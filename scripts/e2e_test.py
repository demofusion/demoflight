#!/usr/bin/env python3
"""
End-to-end test for demoflight Flight SQL server using the demoflight Python client.

This script:
1. Starts the demoflight server
2. Finds a live Deadlock broadcast via the API
3. Uses the demoflight Python client to:
   - Register the source
   - Submit queries
   - Stream results

Usage:
    # From the demoflight/clients/python directory
    cd /home/matt/projects/deadlock/demoflight/clients/python
    uv run python ../../scripts/e2e_test.py

Requirements:
    - demoflight binary built (cargo build --release)
    - demoflight Python client installed (uv sync in clients/python)
    - httpx for API calls
    - DEADLOCK_API_KEY env var (optional, for faster API access)
"""

import asyncio
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import httpx

DEADLOCK_API_BASE = "https://api.deadlock-api.com"
DEMOFLIGHT_ADDR = "grpc://localhost:50051"

DEMOFLIGHT_BIN = Path(__file__).parent.parent / "target" / "release" / "demoflight"
if not DEMOFLIGHT_BIN.exists():
    DEMOFLIGHT_BIN = Path(__file__).parent.parent / "target" / "debug" / "demoflight"


def get_api_key() -> str | None:
    api_key = os.environ.get("DEADLOCK_API_KEY")
    if api_key:
        return api_key
    key_path = Path.home() / ".deadlock-api.key"
    if key_path.exists():
        return key_path.read_text().strip()
    return None


async def get_fresh_match(client: httpx.AsyncClient) -> tuple[int, str] | None:
    """Find a match with broadcast URL."""
    resp = await client.get(f"{DEADLOCK_API_BASE}/v1/matches/active")
    resp.raise_for_status()
    matches = resp.json()

    if not matches:
        return None

    matches.sort(key=lambda m: m.get("spectators", 0), reverse=True)

    for match in matches[:20]:
        match_id = match["match_id"]
        try:
            resp = await client.get(
                f"{DEADLOCK_API_BASE}/v1/matches/{match_id}/live/url"
            )
            if resp.status_code == 200:
                broadcast_url = resp.json().get("broadcast_url")
                if broadcast_url:
                    return match_id, broadcast_url
        except Exception:
            continue

    return None


def start_demoflight_server() -> subprocess.Popen:
    """Start the demoflight server as a subprocess."""
    if not DEMOFLIGHT_BIN.exists():
        raise FileNotFoundError(
            f"demoflight binary not found at {DEMOFLIGHT_BIN}. "
            "Run 'cargo build --release' first."
        )

    env = os.environ.copy()
    env["DEMOFLIGHT_JWT_SECRET"] = "e2e-test-secret-key-12345"
    env["DEMOFLIGHT_LISTEN_ADDR"] = "[::]:50051"
    env["RUST_LOG"] = "demoflight=debug,info"

    proc = subprocess.Popen(
        [str(DEMOFLIGHT_BIN)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    time.sleep(1)

    if proc.poll() is not None:
        output = proc.stdout.read() if proc.stdout else ""
        raise RuntimeError(f"demoflight failed to start: {output}")

    print(f"[+] demoflight server started (PID {proc.pid})")
    return proc


async def run_e2e_test(broadcast_url: str, match_id: int) -> bool:
    """Run the E2E test against a live broadcast using the demoflight client."""
    from demoflight import Client

    print(f"\n{'=' * 70}")
    print(f"E2E TEST: Match {match_id}")
    print(f"{'=' * 70}")
    print(f"Broadcast URL: {broadcast_url[:60]}...")

    try:
        async with Client(DEMOFLIGHT_ADDR) as client:
            print("\n[1] Registering source...")
            async with await client.register_source(broadcast_url) as session:
                tables = session.get_tables()
                print(f"    Session token: {session.session_token[:50]}...")
                print(f"    Tables discovered: {len(tables)}")
                if tables:
                    print(f"    Sample tables: {tables[:5]}")

                print("\n[2] Checking table schemas...")
                target_table = "CCitadelPlayerPawn"
                schema = session.get_schema(target_table)
                if schema:
                    print(f"    {target_table} schema: {len(schema)} fields")
                    print(f"    First 5 fields: {[f.name for f in list(schema)[:5]]}")
                else:
                    print(
                        f"    {target_table} schema not cached (will be available on query)"
                    )

                print("\n[3] Submitting queries...")
                sql1 = "SELECT tick, entity_index, delta_type FROM CCitadelPlayerPawn LIMIT 100"
                sql2 = "SELECT tick, entity_index FROM CNPC_Trooper LIMIT 50"

                q1 = await session.add_query(sql1)
                print(f"    Query 1 registered: {sql1[:50]}...")

                q2 = await session.add_query(sql2)
                print(f"    Query 2 registered: {sql2[:50]}...")

                print("\n[4] Streaming results from Query 1...")
                total_rows_q1 = 0
                batch_count_q1 = 0
                schema_q1 = None

                async for batch in q1:
                    if schema_q1 is None:
                        schema_q1 = batch.schema
                        print(f"    Schema: {schema_q1}")

                    batch_count_q1 += 1
                    total_rows_q1 += batch.num_rows

                    if batch_count_q1 <= 3:
                        print(f"    Batch {batch_count_q1}: {batch.num_rows} rows")

                    if batch_count_q1 >= 10:
                        print(f"    ... (stopping after 10 batches)")
                        break

                print(f"    Total Q1: {batch_count_q1} batches, {total_rows_q1} rows")

                print("\n[5] Streaming results from Query 2...")
                total_rows_q2 = 0
                batch_count_q2 = 0

                async for batch in q2:
                    batch_count_q2 += 1
                    total_rows_q2 += batch.num_rows

                    if batch_count_q2 <= 2:
                        print(f"    Batch {batch_count_q2}: {batch.num_rows} rows")

                    if batch_count_q2 >= 5:
                        print(f"    ... (stopping after 5 batches)")
                        break

                print(f"    Total Q2: {batch_count_q2} batches, {total_rows_q2} rows")

                print("\n[6] Closing session...")
                # Session closes automatically via context manager

        print("\n" + "=" * 70)
        print("E2E TEST PASSED")
        print("=" * 70)
        return True

    except Exception as e:
        print(f"\nE2E TEST FAILED: {e}")
        import traceback

        traceback.print_exc()
        return False


async def main():
    print("=" * 70)
    print("DEMOFLIGHT E2E TEST (using Python client)")
    print("=" * 70)

    api_key = get_api_key()
    if api_key:
        print("[+] Using Deadlock API key")
    else:
        print("[!] No API key - rate limited to 10 req/30min")

    print("\n[*] Finding active match with broadcast...")
    headers = {"X-API-Key": api_key} if api_key else {}

    async with httpx.AsyncClient(headers=headers, timeout=30.0) as http_client:
        result = await get_fresh_match(http_client)

    if not result:
        print("[!] No active matches found. Try again later.")
        sys.exit(1)

    match_id, broadcast_url = result
    print(f"[+] Found match {match_id}")

    print("\n[*] Starting demoflight server...")
    server_proc = start_demoflight_server()

    try:
        time.sleep(2)

        success = await run_e2e_test(broadcast_url, match_id)

        if not success:
            sys.exit(1)

    finally:
        print("\n[*] Stopping demoflight server...")
        server_proc.send_signal(signal.SIGTERM)
        try:
            server_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server_proc.kill()

        if server_proc.stdout:
            output = server_proc.stdout.read()
            if output:
                print("\n[*] Server logs:")
                for line in output.strip().split("\n")[-20:]:
                    print(f"    {line}")

        print("[+] Server stopped")


if __name__ == "__main__":
    asyncio.run(main())
