# demoflight

Arrow Flight server for streaming SQL queries over live Deadlock GOTV broadcasts.

## What is this?

demoflight lets you run SQL queries against live Deadlock matches. Connect any Arrow Flight client (Python, Java, Go, Rust, DBeaver, etc.) and stream real-time game data as Arrow RecordBatches.

Built on [demofusion](https://github.com/demofusion/demofusion).

## Usage

### Python Client

The easiest way to use demoflight is with the Python client:

```bash
cd clients/python
pip install -e .
```

```python
import asyncio
from demoflight import Client

async def main():
    async with Client("grpc://localhost:50051") as client:
        # Connect to a live GOTV broadcast
        async with await client.register_source("http://...steamcontent.com/tv/12345") as session:
            # See what tables are available
            print(session.get_tables())
            # ['CCitadelPlayerPawn', 'CNPC_Trooper', 'CCitadelProjectile', ...]
            
            # Register your queries BEFORE consuming any results
            players = await session.add_query("""
                SELECT tick, entity_index, m_iHealth, m_vecOrigin 
                FROM CCitadelPlayerPawn
            """)
            
            troopers = await session.add_query("""
                SELECT tick, entity_index, m_iTeamNum 
                FROM CNPC_Trooper
            """)
            
            # Consume ALL streams concurrently (required!)
            async def process(query, name):
                async for batch in query:
                    print(f"{name}: {batch.num_rows} rows")
            
            await asyncio.gather(
                process(players, "players"),
                process(troopers, "troopers"),
            )

asyncio.run(main())
```

### Important: Concurrent Consumption

**You must consume all query streams concurrently.** The GOTV parser feeds all streams from a single source - consuming sequentially will deadlock.

```python
# WRONG - will deadlock
for query in queries:
    for batch in query:
        process(batch)

# RIGHT - concurrent consumption
await asyncio.gather(*[consume(q) for q in queries])
```

### Other Clients

Any Arrow Flight client works. The protocol:

1. `DoAction("demoflight.register_source")` - Connect to GOTV, get session token + table list
2. `GetFlightInfo` - Submit SQL queries, get tickets
3. `DoGet` - Stream Arrow batches (consume all tickets concurrently)

Include the session token in the `authorization` header for all requests after registration.

## Running the Server

```bash
# Build
cargo build --release

# Configure (JWT secret is required)
export DEMOFLIGHT_JWT_SECRET="your-secret-key"

# Run
cargo run --release
```

Or with a config file (`demoflight.toml`):

```toml
jwt_secret = "your-secret-key"
listen_addr = "[::]:50051"

# Optional: restrict which GOTV URLs can be connected
allowed_source_patterns = [
    "^https?://[^/]*\\.steamcontent\\.com/.*$",
]
```

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `jwt_secret` | *required* | Secret key for session tokens |
| `listen_addr` | `[::]:50051` | Server address |
| `batch_size` | `8192` | Rows per Arrow batch |
| `reject_pipeline_breakers` | `false` | Block queries with ORDER BY or aggregations |
| `max_sessions` | `100` | Max concurrent sessions |
| `max_queries_per_session` | `10` | Max queries per session |
| `max_session_duration_secs` | `3600` | Session lifetime |
| `inactivity_timeout_secs` | `300` | Idle session timeout |

Set via environment variables with `DEMOFLIGHT_` prefix (e.g., `DEMOFLIGHT_BATCH_SIZE=4096`).

### Batch Size

Controls how many rows are buffered before sending to the client. Smaller batches = lower latency, larger batches = better throughput. Default (8192) works well for most cases.

### Pipeline Breakers

Queries with `ORDER BY`, `GROUP BY`, or aggregations require buffering all data before returning results - they "break" the streaming pipeline. By default these are allowed (with a warning), but you can reject them:

```toml
reject_pipeline_breakers = true
```

With this enabled, queries like `SELECT COUNT(*) FROM players` will fail with `INVALID_ARGUMENT`.

## Errors

| Error | Meaning |
|-------|---------|
| `UNAVAILABLE` | Can't connect to GOTV broadcast |
| `INVALID_ARGUMENT` | Bad SQL, unknown table, or rejected pipeline breaker |
| `FAILED_PRECONDITION` | Tried to add queries after streaming started |
| `UNAUTHENTICATED` | Invalid or expired session token |
| `RESOURCE_EXHAUSTED` | Too many sessions or queries |

## Development

```bash
cargo build    # Build (compiles protos)
cargo test     # Run tests
cargo fmt      # Format code
```

## License

Apache-2.0
