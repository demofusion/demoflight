"""Pytest fixtures for demoflight client tests."""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import time
from pathlib import Path
from typing import AsyncGenerator, Generator

import httpx
import pytest
import pytest_asyncio

DEMOFLIGHT_BIN = (
    Path(__file__).parent.parent.parent.parent / "target" / "release" / "demoflight"
)
if not DEMOFLIGHT_BIN.exists():
    DEMOFLIGHT_BIN = (
        Path(__file__).parent.parent.parent.parent / "target" / "debug" / "demoflight"
    )

DEADLOCK_API_BASE = "https://api.deadlock-api.com"


def get_api_key() -> str | None:
    api_key = os.environ.get("DEADLOCK_API_KEY")
    if api_key:
        return api_key
    key_path = Path.home() / ".deadlock-api.key"
    if key_path.exists():
        return key_path.read_text().strip()
    return None


@pytest.fixture(scope="session")
def demoflight_server() -> Generator[str, None, None]:
    """Start demoflight server and return its address."""
    external_url = os.environ.get("DEMOFLIGHT_URL")
    if external_url:
        yield external_url
        return

    if not DEMOFLIGHT_BIN.exists():
        pytest.skip(f"demoflight binary not found at {DEMOFLIGHT_BIN}")

    env = os.environ.copy()
    env["DEMOFLIGHT_JWT_SECRET"] = "test-secret-key-for-pytest"
    env["DEMOFLIGHT_LISTEN_ADDR"] = "[::]:50052"
    env["RUST_LOG"] = "demoflight=info"

    proc = subprocess.Popen(
        [str(DEMOFLIGHT_BIN)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    time.sleep(1.5)

    if proc.poll() is not None:
        output = proc.stdout.read() if proc.stdout else b""
        pytest.fail(f"demoflight failed to start: {output.decode()}")

    try:
        yield "grpc://localhost:50052"
    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


@pytest_asyncio.fixture
async def broadcast_url() -> AsyncGenerator[str, None]:
    """Find an active broadcast URL from Deadlock API."""
    api_key = get_api_key()
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}

    async with httpx.AsyncClient(headers=headers, timeout=30) as client:
        resp = await client.get(f"{DEADLOCK_API_BASE}/v1/matches/active")
        resp.raise_for_status()
        matches = resp.json()

        if not matches:
            pytest.skip("No active matches found")

        matches.sort(key=lambda m: m.get("spectators", 0), reverse=True)

        for match in matches[:10]:
            match_id = match["match_id"]
            try:
                resp = await client.get(
                    f"{DEADLOCK_API_BASE}/v1/matches/{match_id}/live/url"
                )
                if resp.status_code == 200:
                    url = resp.json().get("broadcast_url")
                    if url:
                        yield url
                        return
            except Exception:
                continue

        pytest.skip("No broadcast URL available")
