#!/usr/bin/env python3
"""
End-to-end test for demoflight Flight SQL server using the demoflight Python client.

This script:
1. Starts the demoflight server (or connects to an existing one)
2. Finds a live Deadlock broadcast via the API
3. Uses the demoflight Python client to:
   - Register the source
   - Submit queries
   - Stream results

Usage:
    # Run against local binary (default)
    cd demoflight/clients/python
    uv run python ../../scripts/e2e_test.py

    # Run against Docker container
    docker run -d -p 50051:50051 -e DEMOFLIGHT_JWT_SECRET=test demoflight
    DEMOFLIGHT_URL=grpc://localhost:50051 uv run python ../../scripts/e2e_test.py

    # Run against remote server
    DEMOFLIGHT_URL=grpc://demoflight.example.com:50051 uv run python ../../scripts/e2e_test.py

Environment variables:
    DEMOFLIGHT_URL      - Server URL (if set, skips starting local server)
    DEADLOCK_API_KEY    - Deadlock API key (optional, for faster API access)

Requirements:
    - demoflight Python client installed (uv sync in clients/python)
    - httpx for API calls
    - If not using DEMOFLIGHT_URL: demoflight binary built (cargo build --release)
"""

import asyncio
import logging
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import httpx


class FlushingStreamHandler(logging.StreamHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()


def setup_logging() -> logging.Logger:
    handler = FlushingStreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(message)s"))

    logger = logging.getLogger("e2e_test")
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


log = setup_logging()

DEADLOCK_API_BASE = "https://api.deadlock-api.com"
DEFAULT_DEMOFLIGHT_ADDR = "grpc://localhost:50051"

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

    log.info("[+] demoflight server started (PID %d)", proc.pid)
    return proc


async def run_e2e_test(broadcast_url: str, match_id: int, server_addr: str) -> bool:
    """Run the E2E test against a live broadcast using the demoflight client."""
    from demoflight import Client

    log.info("")
    log.info("=" * 70)
    log.info("E2E TEST: Match %d", match_id)
    log.info("=" * 70)
    log.info("Broadcast URL: %s...", broadcast_url[:60])

    try:
        async with Client(server_addr) as client:
            log.info("")
            log.info("[1] Registering source...")
            async with await client.register_source(broadcast_url) as session:
                tables = session.get_tables()
                log.info("    Session token: %s...", session.session_token[:50])
                log.info("    Tables discovered: %d", len(tables))
                if tables:
                    log.info("    Sample tables: %s", tables[:5])

                log.info("")
                log.info("[2] Checking table schemas...")
                target_table = "CCitadelPlayerPawn"
                schema = session.get_schema(target_table)
                if schema:
                    log.info("    %s schema: %d fields", target_table, len(schema))
                    log.info(
                        "    First 5 fields: %s", [f.name for f in list(schema)[:5]]
                    )
                else:
                    log.info(
                        "    %s schema not cached (will be available on query)",
                        target_table,
                    )

                log.info("")
                log.info("[3] Submitting queries...")
                sql1 = "SELECT tick, entity_index, delta_type FROM CCitadelPlayerPawn LIMIT 100"
                sql2 = "SELECT tick, entity_index FROM CNPC_Trooper LIMIT 50"

                q1 = await session.add_query(sql1)
                log.info("    Query 1 registered: %s...", sql1[:50])

                q2 = await session.add_query(sql2)
                log.info("    Query 2 registered: %s...", sql2[:50])

                log.info("")
                log.info("[4] Streaming results CONCURRENTLY...")

                results = {
                    "q1": {"batches": 0, "rows": 0, "schema": None},
                    "q2": {"batches": 0, "rows": 0},
                }

                async def drain_q1():
                    async for batch in q1:
                        if results["q1"]["schema"] is None:
                            results["q1"]["schema"] = batch.schema
                            log.info("    Q1 Schema: %s", batch.schema)

                        results["q1"]["batches"] += 1
                        results["q1"]["rows"] += batch.num_rows

                        if results["q1"]["batches"] <= 3:
                            log.info(
                                "    Q1 Batch %d: %d rows",
                                results["q1"]["batches"],
                                batch.num_rows,
                            )

                        if results["q1"]["batches"] >= 10:
                            log.info("    Q1 ... (stopping after 10 batches)")
                            break

                async def drain_q2():
                    async for batch in q2:
                        results["q2"]["batches"] += 1
                        results["q2"]["rows"] += batch.num_rows

                        if results["q2"]["batches"] <= 2:
                            log.info(
                                "    Q2 Batch %d: %d rows",
                                results["q2"]["batches"],
                                batch.num_rows,
                            )

                        if results["q2"]["batches"] >= 5:
                            log.info("    Q2 ... (stopping after 5 batches)")
                            break

                await asyncio.gather(drain_q1(), drain_q2())

                log.info("")
                log.info(
                    "    Total Q1: %d batches, %d rows",
                    results["q1"]["batches"],
                    results["q1"]["rows"],
                )
                log.info(
                    "    Total Q2: %d batches, %d rows",
                    results["q2"]["batches"],
                    results["q2"]["rows"],
                )

                log.info("")
                log.info("[5] Closing session...")

        log.info("")
        log.info("=" * 70)
        log.info("E2E TEST PASSED")
        log.info("=" * 70)
        return True

    except Exception as e:
        log.error("")
        log.error("E2E TEST FAILED: %s", e)
        import traceback

        traceback.print_exc()
        return False


async def main():
    log.info("=" * 70)
    log.info("DEMOFLIGHT E2E TEST (using Python client)")
    log.info("=" * 70)

    external_server = os.environ.get("DEMOFLIGHT_URL")
    server_addr = external_server or DEFAULT_DEMOFLIGHT_ADDR

    if external_server:
        log.info("[+] Using external server: %s", external_server)
    else:
        log.info("[*] Will start local server")

    api_key = get_api_key()
    if api_key:
        log.info("[+] Using Deadlock API key")
    else:
        log.info("[!] No API key - rate limited to 10 req/30min")

    log.info("")
    log.info("[*] Finding active match with broadcast...")
    headers = {"X-API-Key": api_key} if api_key else {}

    async with httpx.AsyncClient(headers=headers, timeout=30.0) as http_client:
        result = await get_fresh_match(http_client)

    if not result:
        log.info("[!] No active matches found. Try again later.")
        sys.exit(1)

    match_id, broadcast_url = result
    log.info("[+] Found match %d", match_id)

    server_proc = None
    if not external_server:
        log.info("")
        log.info("[*] Starting demoflight server...")
        server_proc = start_demoflight_server()
        time.sleep(2)

    try:
        success = await run_e2e_test(broadcast_url, match_id, server_addr)

        if not success:
            sys.exit(1)

    finally:
        if server_proc:
            log.info("")
            log.info("[*] Stopping demoflight server...")
            server_proc.send_signal(signal.SIGTERM)
            try:
                server_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server_proc.kill()

            if server_proc.stdout:
                output = server_proc.stdout.read()
                if output:
                    log.info("")
                    log.info("[*] Server logs:")
                    for line in output.strip().split("\n")[-20:]:
                        log.info("    %s", line)

            log.info("[+] Server stopped")


if __name__ == "__main__":
    asyncio.run(main())
