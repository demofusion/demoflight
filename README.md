# demoflight

Arrow Flight server for streaming SQL queries over GOTV broadcasts using [demofusion](https://github.com/demofusion/demofusion).

## Overview

demoflight wraps demofusion with the [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) protocol, enabling any standard Flight client (pyarrow, Java, Go, Rust, DBeaver, etc.) to:

1. **Register** a GOTV broadcast URL
2. **Submit** multiple SQL queries against entity tables
3. **Stream** Arrow RecordBatches for each query result

The server implements both custom FDAP (Flight Data Access Protocol) extensions and standard Flight SQL metadata commands for tooling compatibility.

## Quick Start

```bash
# Build
cargo build --release

# Configure (required: JWT secret)
export DEMOFLIGHT_JWT_SECRET="your-256-bit-secret-key"
export DEMOFLIGHT_LISTEN_ADDR="[::]:50051"

# Run
cargo run --release
```

## Protocol

### Workflow

```
1. DoAction("demoflight.register_source")  →  Connect to GOTV, get JWT + table list
2. GetFlightInfo(queries)                  →  Submit SQL queries, get tickets
3. DoGet(ticket)                           →  Stream Arrow batches (parallel OK)
```

### Session States

Sessions progress through states that enforce query ordering:

```
REGISTERED  →  QUERYING  →  LOCKED  →  COMPLETED  →  EXPIRED
     │              │           │
     │              │           └── All streams consumed
     │              └── First DoGet starts the parser
     └── Queries submitted via GetFlightInfo
```

- Cannot add queries after session is LOCKED (streaming has begun)
- Sessions expire automatically after timeout

### Concurrent Stream Consumption

**All `DoGet` streams must be consumed concurrently.** The underlying GOTV parser feeds all query streams from a single source. If you consume streams sequentially, earlier streams will block waiting for data that can only arrive after later streams drain their buffers.

```python
# WRONG - sequential consumption will deadlock
for endpoint in info.endpoints:
    for batch in client.do_get(endpoint.ticket):
        process(batch)

# RIGHT - concurrent consumption
import asyncio

async def consume(endpoint):
    reader = client.do_get(endpoint.ticket, options)
    async for batch in reader:
        process(batch)

await asyncio.gather(*[consume(ep) for ep in info.endpoints])
```

### JWT Token

The `register_source` action returns a JWT containing:

```json
{
  "sub": "session-uuid",
  "iss": "demoflight",
  "iat": 1709337600,
  "exp": 1709337900,
  "source_type": "gotv_broadcast",
  "source_hash": "a1b2c3d4"
}
```

The available tables are returned in the `RegisterSourceResponse` protobuf message (not in the JWT). Include the token in subsequent requests via the `authorization` header.

### Flight SQL Support

For compatibility with standard tooling (DBeaver, etc.):

| Command | Purpose |
|---------|---------|
| `CommandGetTables` | List entity tables in session |
| `CommandGetSqlInfo` | Server capabilities |
| `GetSchema` | Arrow schema for a table |

## Configuration

Environment variables (or `demoflight.toml`):

| Variable | Default | Description |
|----------|---------|-------------|
| `DEMOFLIGHT_LISTEN_ADDR` | `[::]:50051` | gRPC listen address |
| `DEMOFLIGHT_JWT_SECRET` | *required* | Secret for JWT signing |
| `DEMOFLIGHT_SESSION_TIMEOUT_SECS` | `300` | Session expiry (seconds) |
| `DEMOFLIGHT_MAX_SESSIONS` | `100` | Max concurrent sessions |
| `DEMOFLIGHT_MAX_QUERIES_PER_SESSION` | `10` | Max queries per session |
| `DEMOFLIGHT_CLEANUP_INTERVAL_SECS` | `60` | Cleanup task interval |

Example `demoflight.toml`:

```toml
listen_addr = "[::]:50051"
jwt_secret = "your-secret-key"
session_timeout_secs = 300
max_sessions = 100

allowed_source_patterns = [
    "^https?://[^/]*\\.steamcontent\\.com/.*$",
]
```

## Client Example

A typed Python client is available in `clients/python/` that handles protobuf encoding and concurrent stream consumption:

```bash
cd clients/python
pip install -e .
```

```python
import asyncio
from demoflight import Client

async def main():
    async with Client("grpc://localhost:50051") as client:
        async with await client.register_source("http://...steamcontent.com/tv/12345") as session:
            # Check available tables
            print(session.get_tables())
            
            # Register ALL queries before consuming any results
            q1 = await session.add_query("SELECT tick, entity_index FROM CCitadelPlayerPawn")
            q2 = await session.add_query("SELECT tick, entity_index FROM CNPC_Trooper")
            
            # Consume concurrently (required - see "Concurrent Stream Consumption" above)
            async def drain(handle, name):
                async for batch in handle:
                    print(f"{name}: {batch.num_rows} rows")
            
            await asyncio.gather(
                drain(q1, "players"),
                drain(q2, "troopers"),
            )

asyncio.run(main())
```

## Error Handling

All errors map to standard gRPC/Flight status codes:

| Error | Status |
|-------|--------|
| Session not found | `NOT_FOUND` |
| Session locked | `FAILED_PRECONDITION` |
| Invalid SQL | `INVALID_ARGUMENT` |
| Invalid source URL | `INVALID_ARGUMENT` |
| JWT validation failed | `UNAUTHENTICATED` |
| GOTV connection failed | `UNAVAILABLE` |

## Project Structure

```
src/
├── server.rs           # FlightService implementation
├── config.rs           # Configuration (figment)
├── error.rs            # Error types + status mapping
├── session/
│   ├── manager.rs      # Session storage
│   ├── state.rs        # Session state machine
│   └── jwt.rs          # JWT encode/decode
├── actions/
│   └── register_source.rs
├── commands/
│   └── router.rs       # Route FlightDescriptor commands
├── flight_sql/         # GetTables, GetSqlInfo, GetSchema
├── tickets/
│   └── encoding.rs     # Ticket serialization
└── source/
    ├── gotv.rs         # GOTV connector
    └── validation.rs   # URL validation (SSRF prevention)

proto/
└── demoflight/v1/demoflight.proto

clients/
└── python/             # Python client package
```

## Development

```bash
# Build (compiles protos automatically)
cargo build

# Run tests
cargo test

# Run with local demofusion (create .cargo/config.toml, which is gitignored)
# [patch."https://github.com/demofusion/demofusion.git"]
# demofusion = { path = "../demofusion-public" }
```

## License

Apache-2.0
