# demoflight

Python client for the demoflight Arrow Flight server, providing streaming SQL queries over GOTV broadcasts.

## Installation

```bash
pip install demoflight
```

## Quick Start

```python
import asyncio
from demoflight import Client

async def main():
    async with Client("grpc://localhost:50051") as client:
        # Register a GOTV broadcast source
        async with await client.register_source(broadcast_url) as session:
            # Inspect available tables
            print("Tables:", session.get_tables())
            
            # Get schema for a table
            schema = session.get_schema("CCitadelPlayerPawn")
            print("Schema:", schema)
            
            # Run queries - register ALL queries before consuming results
            q1 = await session.add_query("SELECT * FROM CCitadelPlayerPawn LIMIT 100")
            q2 = await session.add_query("SELECT * FROM CNPC_Trooper LIMIT 100")
            
            # Stream results (session locks on first iteration)
            async for batch in q1:
                print(f"Got {batch.num_rows} rows")
            
            async for batch in q2:
                print(f"Got {batch.num_rows} rows")

asyncio.run(main())
```

## API Reference

### `Client(uri: str)`

Async context manager for connecting to a demoflight server.

- **uri**: gRPC URI (e.g., `"grpc://localhost:50051"`)
- No network I/O until `register_source()` is called
- Use as async context manager to ensure proper cleanup

### `await client.register_source(source_url: str) -> DemoflightSession`

Register a GOTV broadcast and create a session.

- **source_url**: GOTV broadcast URL (Steam CDN URL)
- Returns a `DemoflightSession` (use as async context manager)

### `DemoflightSession`

Session for a registered broadcast source.

- **`get_tables() -> list[str]`**: List available table names
- **`get_schema(table_name: str) -> pa.Schema | None`**: Get Arrow schema
- **`await add_query(sql: str) -> QueryHandle`**: Submit a SQL query
- **`await close()`**: Close session (automatic with context manager)

**Important**: Register all queries via `add_query()` BEFORE iterating any `QueryHandle`. Once iteration starts, the session locks and rejects new queries.

### `QueryHandle`

Async iterator for streaming query results.

- **`async for batch in handle`**: Iterate `pa.RecordBatch` objects
- **`await handle.read_schema() -> pa.Schema`**: Get schema (triggers DoGet)
- **`handle.schema -> pa.Schema | None`**: Schema (available after first batch)

Note: The first iteration (or `read_schema()`) triggers the DoGet call which locks the session.

## Session Lifecycle

```
┌─────────────────┐
│  register_source │  → Schema discovery, JWT token returned
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   add_query()   │  → Submit queries (can call multiple times)
│   add_query()   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  iterate/read   │  → Session LOCKS, no more queries allowed
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│     close()     │  → Release server resources
└─────────────────┘
```

## Requirements

- Python 3.11+
- pyarrow >= 17.0
