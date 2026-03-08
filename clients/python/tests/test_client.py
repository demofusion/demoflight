"""Integration tests for demoflight Python client.

These tests require either:
1. A running demoflight server (set DEMOFLIGHT_URL env var)
2. The demoflight binary built (cargo build --release)

Tests also require an active Deadlock match with GOTV broadcast.
"""

from __future__ import annotations

import asyncio

import pyarrow as pa
import pytest

from demoflight import Client


class TestClientConnection:
    """Test basic client connection and lifecycle."""

    async def test_client_creates_without_connecting(self):
        """Client should not connect until register_source is called."""
        client = Client("grpc://localhost:50052")
        assert client.uri == "grpc://localhost:50052"
        client.close()

    async def test_client_context_manager(self, demoflight_server: str):
        """Client should work as async context manager."""
        async with Client(demoflight_server) as client:
            assert client.uri == demoflight_server

    async def test_client_close_is_idempotent(self, demoflight_server: str):
        """Closing client multiple times should not raise."""
        client = Client(demoflight_server)
        client.close()
        client.close()


class TestSessionRegistration:
    """Test source registration and session creation."""

    async def test_register_source_returns_session(
        self, demoflight_server: str, broadcast_url: str
    ):
        """Registering a source should return a valid session."""
        async with Client(demoflight_server) as client:
            async with await client.register_source(broadcast_url) as session:
                assert session.session_token
                assert len(session.session_token) > 50

    async def test_session_has_tables(self, demoflight_server: str, broadcast_url: str):
        """Session should discover tables from the broadcast."""
        async with Client(demoflight_server) as client:
            async with await client.register_source(broadcast_url) as session:
                tables = session.get_tables()
                assert len(tables) > 0
                assert "CCitadelPlayerPawn" in tables

    async def test_session_has_schemas(
        self, demoflight_server: str, broadcast_url: str
    ):
        """Session should have Arrow schemas for tables."""
        async with Client(demoflight_server) as client:
            async with await client.register_source(broadcast_url) as session:
                schema = session.get_schema("CCitadelPlayerPawn")
                assert schema is not None
                assert isinstance(schema, pa.Schema)
                assert "tick" in schema.names
                assert "entity_index" in schema.names

    async def test_get_schema_unknown_table_returns_none(
        self, demoflight_server: str, broadcast_url: str
    ):
        """Getting schema for unknown table should return None."""
        async with Client(demoflight_server) as client:
            async with await client.register_source(broadcast_url) as session:
                schema = session.get_schema("NonExistentTable")
                assert schema is None


class TestQueryExecution:
    """Test SQL query submission and result streaming."""

    async def test_add_query_returns_handle(
        self, demoflight_server: str, broadcast_url: str
    ):
        """Adding a query should return a QueryHandle."""
        async with Client(demoflight_server) as client:
            async with await client.register_source(broadcast_url) as session:
                handle = await session.add_query(
                    "SELECT tick, entity_index FROM CCitadelPlayerPawn LIMIT 10"
                )
                assert handle is not None

    async def test_query_streams_batches(
        self, demoflight_server: str, broadcast_url: str
    ):
        """Query should stream Arrow RecordBatches."""
        async with Client(demoflight_server) as client:
            async with await client.register_source(broadcast_url) as session:
                handle = await session.add_query(
                    "SELECT tick, entity_index FROM CCitadelPlayerPawn LIMIT 50"
                )

                batches = []
                async for batch in handle:
                    batches.append(batch)
                    assert isinstance(batch, pa.RecordBatch)

                assert len(batches) > 0
                total_rows = sum(b.num_rows for b in batches)
                assert total_rows == 50

    async def test_query_result_has_correct_schema(
        self, demoflight_server: str, broadcast_url: str
    ):
        """Query results should have the expected schema."""
        async with Client(demoflight_server) as client:
            async with await client.register_source(broadcast_url) as session:
                handle = await session.add_query(
                    "SELECT tick, entity_index, delta_type FROM CCitadelPlayerPawn LIMIT 10"
                )

                async for batch in handle:
                    assert "tick" in batch.schema.names
                    assert "entity_index" in batch.schema.names
                    assert "delta_type" in batch.schema.names
                    break

    async def test_multiple_queries_concurrent(
        self, demoflight_server: str, broadcast_url: str
    ):
        """Multiple queries should be consumable concurrently."""
        async with Client(demoflight_server) as client:
            async with await client.register_source(broadcast_url) as session:
                q1 = await session.add_query(
                    "SELECT tick FROM CCitadelPlayerPawn LIMIT 25"
                )
                q2 = await session.add_query("SELECT tick FROM CNPC_Trooper LIMIT 25")

                results = {"q1": 0, "q2": 0}

                async def consume(handle, key):
                    async for batch in handle:
                        results[key] += batch.num_rows

                await asyncio.gather(
                    consume(q1, "q1"),
                    consume(q2, "q2"),
                )

                assert results["q1"] == 25
                assert results["q2"] == 25


class TestErrorHandling:
    """Test error cases and edge conditions."""

    async def test_invalid_sql_raises(self, demoflight_server: str, broadcast_url: str):
        """Invalid SQL should raise an error."""
        async with Client(demoflight_server) as client:
            async with await client.register_source(broadcast_url) as session:
                with pytest.raises(Exception):
                    await session.add_query("SELEKT * FORM invalid")

    async def test_unknown_table_raises(
        self, demoflight_server: str, broadcast_url: str
    ):
        """Query on unknown table should raise an error."""
        async with Client(demoflight_server) as client:
            async with await client.register_source(broadcast_url) as session:
                with pytest.raises(Exception):
                    await session.add_query("SELECT * FROM NonExistentTable123")

    async def test_invalid_broadcast_url_raises(self, demoflight_server: str):
        """Invalid broadcast URL should raise an error."""
        async with Client(demoflight_server) as client:
            with pytest.raises(Exception):
                await client.register_source(
                    "http://invalid.example.com/not-a-broadcast"
                )

    async def test_session_closed_rejects_queries(
        self, demoflight_server: str, broadcast_url: str
    ):
        """Closed session should reject new queries."""
        async with Client(demoflight_server) as client:
            session = await client.register_source(broadcast_url)
            await session.close()

            with pytest.raises(RuntimeError, match="closed"):
                await session.add_query("SELECT tick FROM CCitadelPlayerPawn LIMIT 1")
