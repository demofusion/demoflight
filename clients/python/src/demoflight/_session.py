"""DemoflightSession - manages a registered source session."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.flight as flight

from demoflight._proto import (
    RegisterSourceResponse,
    encode_close_session_request,
    encode_command_statement_query,
)
from demoflight._query import QueryHandle

if TYPE_CHECKING:
    from demoflight._client import Client


class DemoflightSession:
    """
    Session for a registered GOTV broadcast source.

    Created by Client.register_source(). Provides access to table metadata
    and query execution.

    IMPORTANT: All queries must be registered via add_query() BEFORE consuming
    any results (iterating any QueryHandle). Once iteration starts on any handle,
    the session is locked and no more queries can be added.

    Example:
        async with Client("grpc://localhost:50051") as client:
            async with await client.register_source(broadcast_url) as session:
                tables = session.get_tables()
                schema = session.get_schema("CCitadelPlayerPawn")

                # Register all queries first
                q1 = await session.add_query("SELECT * FROM players LIMIT 10")
                q2 = await session.add_query("SELECT * FROM entities LIMIT 10")

                # Then consume results (session locks on first iteration)
                async for batch in q1:
                    process(batch)
    """

    __slots__ = (
        "_client",
        "_flight_client",
        "_response",
        "_schema_cache",
        "_closed",
    )

    def __init__(
        self,
        client: Client,
        flight_client: flight.FlightClient,
        response: RegisterSourceResponse,
    ) -> None:
        self._client = client
        self._flight_client = flight_client
        self._response = response
        self._schema_cache: dict[str, pa.Schema] = {}
        self._closed = False

        for table in response.tables:
            if table.arrow_schema is not None:
                self._schema_cache[table.name] = table.arrow_schema

    @property
    def session_token(self) -> str:
        """JWT session token for this session."""
        return self._response.session_token

    def get_tables(self) -> list[str]:
        """Return list of available table names."""
        return [t.name for t in self._response.tables]

    def get_schema(self, table_name: str) -> pa.Schema | None:
        """
        Return Arrow schema for a table, or None if not found.

        Schemas are cached from the register_source response.
        """
        return self._schema_cache.get(table_name)

    async def add_query(self, sql: str) -> QueryHandle:
        """
        Register a SQL query and return a handle for streaming results.

        The query is submitted to the server via GetFlightInfo, which returns
        a ticket. The actual DoGet is deferred until iteration begins.

        Multiple queries can be registered before any iteration starts.
        Once iteration begins on ANY handle, the session locks and rejects
        new queries.

        Args:
            sql: SQL query string (e.g., "SELECT * FROM players LIMIT 100")

        Returns:
            QueryHandle for async iteration of RecordBatches
        """
        if self._closed:
            raise RuntimeError("Session is closed")

        cmd_bytes = encode_command_statement_query(sql)
        descriptor = flight.FlightDescriptor.for_command(cmd_bytes)
        options = self._make_call_options()

        info = await asyncio.to_thread(
            self._flight_client.get_flight_info, descriptor, options
        )

        if not info.endpoints:
            raise RuntimeError("Server returned no endpoints for query")

        ticket = info.endpoints[0].ticket

        def do_get():
            return self._flight_client.do_get(ticket, options)

        return QueryHandle(do_get)

    async def close(self) -> None:
        """
        Close the session and release server resources.

        Called automatically when using async context manager.
        """
        if self._closed:
            return

        self._closed = True

        try:
            body = encode_close_session_request(self.session_token)
            action = flight.Action("demoflight.close_session", body)
            await asyncio.to_thread(lambda: list(self._flight_client.do_action(action)))
        except Exception:
            pass

    async def __aenter__(self) -> DemoflightSession:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    def _make_call_options(self) -> flight.FlightCallOptions:
        """Create FlightCallOptions with Bearer token."""
        return flight.FlightCallOptions(
            headers=[(b"authorization", f"Bearer {self.session_token}".encode())]
        )
