"""Client - entry point for demoflight connections."""

from __future__ import annotations

import asyncio
from types import TracebackType

import pyarrow.flight as flight

from demoflight._proto import (
    decode_register_source_response,
    encode_register_source_request,
)
from demoflight._session import DemoflightSession


class Client:
    """
    Demoflight client for connecting to an Arrow Flight server.

    Use as an async context manager to ensure proper cleanup of the
    underlying gRPC connection.

    Example:
        async with Client("grpc://localhost:50051") as client:
            async with await client.register_source(broadcast_url) as session:
                tables = session.get_tables()
                handle = await session.add_query("SELECT * FROM players LIMIT 10")
                async for batch in handle:
                    print(batch)
    """

    __slots__ = ("_uri", "_flight_client", "_closed")

    def __init__(self, uri: str) -> None:
        """
        Create a client for the given server URI.

        No network I/O is performed until register_source() is called.

        Args:
            uri: gRPC URI (e.g., "grpc://localhost:50051")
        """
        self._uri = uri
        self._flight_client: flight.FlightClient | None = None
        self._closed = False

    @property
    def uri(self) -> str:
        """Server URI this client connects to."""
        return self._uri

    def _get_flight_client(self) -> flight.FlightClient:
        if self._closed:
            raise RuntimeError("Client is closed")
        if self._flight_client is None:
            self._flight_client = flight.FlightClient(self._uri)
        return self._flight_client

    async def register_source(self, source_url: str) -> DemoflightSession:
        """
        Register a GOTV broadcast source and create a session.

        Performs schema discovery on the broadcast and returns a session
        that can be used to query data.

        Args:
            source_url: GOTV broadcast URL (Steam CDN URL)

        Returns:
            DemoflightSession for querying the broadcast

        Raises:
            pyarrow.flight.FlightServerError: If registration fails
        """
        flight_client = self._get_flight_client()

        body = encode_register_source_request(source_url)
        action = flight.Action("demoflight.register_source", body)

        results = await asyncio.to_thread(lambda: list(flight_client.do_action(action)))

        if not results:
            raise RuntimeError("Server returned empty response for register_source")

        response_bytes = results[0].body.to_pybytes()
        response = decode_register_source_response(response_bytes)

        return DemoflightSession(self, flight_client, response)

    def close(self) -> None:
        """Close the client and release resources."""
        if self._closed:
            return
        self._closed = True
        if self._flight_client is not None:
            self._flight_client.close()
            self._flight_client = None

    async def __aenter__(self) -> Client:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()
