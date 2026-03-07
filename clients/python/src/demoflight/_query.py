"""QueryHandle - async iterator for streaming Arrow RecordBatches."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Callable

import pyarrow as pa

if TYPE_CHECKING:
    from pyarrow.flight import FlightStreamReader


class QueryHandle:
    """
    Async iterator yielding Arrow RecordBatches from a query result stream.

    Created by DemoflightSession.add_query(). The actual DoGet call is deferred
    until iteration begins, allowing multiple queries to be registered before
    the session locks.

    Example:
        # Register all queries first (session not yet locked)
        q1 = await session.add_query("SELECT * FROM players LIMIT 10")
        q2 = await session.add_query("SELECT * FROM entities LIMIT 10")

        # Iteration triggers DoGet and locks the session
        async for batch in q1:
            print(f"Got {batch.num_rows} rows")
    """

    __slots__ = (
        "_do_get_fn",
        "_reader",
        "_schema",
        "_exhausted",
        "_started",
        "_cancelled",
    )

    def __init__(self, do_get_fn: Callable[[], FlightStreamReader]) -> None:
        self._do_get_fn = do_get_fn
        self._reader: FlightStreamReader | None = None
        self._schema: pa.Schema | None = None
        self._exhausted = False
        self._started = False
        self._cancelled = False

    async def _ensure_started(self) -> FlightStreamReader:
        if self._reader is not None:
            return self._reader

        self._reader = await asyncio.to_thread(self._do_get_fn)
        self._started = True
        return self._reader

    @property
    def schema(self) -> pa.Schema | None:
        """
        Arrow schema for result batches.

        Available after first batch is read, or call read_schema() first.
        """
        return self._schema

    async def read_schema(self) -> pa.Schema:
        """
        Read and return the schema without consuming data.

        Note: This triggers DoGet and locks the session.
        """
        if self._schema is not None:
            return self._schema

        reader = await self._ensure_started()

        def _get_schema() -> pa.Schema:
            return reader.schema

        schema = await asyncio.to_thread(_get_schema)
        self._schema = schema
        return schema

    def cancel(self) -> None:
        """
        Request cancellation of the stream.

        The next read will raise StopAsyncIteration. Does not interrupt
        an in-progress blocking read, but prevents further reads.
        """
        self._cancelled = True
        self._exhausted = True
        if self._reader is not None:
            try:
                self._reader.cancel()
            except Exception:
                pass

    def __aiter__(self) -> QueryHandle:
        return self

    async def __anext__(self) -> pa.RecordBatch:
        if self._exhausted or self._cancelled:
            raise StopAsyncIteration

        reader = await self._ensure_started()

        # Run blocking read in thread, but allow cancellation to interrupt
        loop = asyncio.get_event_loop()
        future = loop.run_in_executor(None, self._read_next_chunk, reader)

        try:
            chunk = await future
        except asyncio.CancelledError:
            # Task was cancelled - cancel the reader and propagate
            self.cancel()
            raise

        if chunk is None:
            self._exhausted = True
            raise StopAsyncIteration

        return chunk

    def _read_next_chunk(self, reader: FlightStreamReader) -> pa.RecordBatch | None:
        """Read next chunk synchronously. Called from thread pool."""
        if self._cancelled:
            return None
        try:
            chunk = reader.read_chunk()
            if chunk.data is not None:
                if self._schema is None:
                    self._schema = chunk.data.schema
                return chunk.data
            return None
        except StopIteration:
            return None
        except Exception:
            # gRPC cancellation or connection errors
            return None
