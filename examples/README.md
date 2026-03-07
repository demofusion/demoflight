# Demoflight Query Examples

Example queries for streaming Deadlock game data via demoflight.

## Quick Start

```python
import asyncio
from demoflight import Client

async def main():
    async with Client("grpc://localhost:50051") as client:
        async with await client.register_source(broadcast_url) as session:
            # Check available tables
            print(session.get_tables())
            
            # Run a query
            query = await session.add_query("SELECT * FROM CCitadelPlayerPawn LIMIT 10")
            async for batch in query:
                print(batch.to_pandas())

asyncio.run(main())
```

## Important: Concurrent Stream Consumption

All queries **must be consumed concurrently**. The GOTV parser feeds all query
streams from a single source - sequential consumption will deadlock.

```python
# WRONG - will deadlock
async for batch in query1:
    process(batch)
async for batch in query2:  # Never reached
    process(batch)

# RIGHT - concurrent consumption
async def drain(query):
    async for batch in query:
        process(batch)

await asyncio.gather(drain(query1), drain(query2))
```

## Examples

| File | Description |
|------|-------------|
| `hero_positions.py` | Track hero movement over time |
| `lane_control.py` | Monitor zipline node ownership |
| `objectives.py` | Track objective health (Guardian, Walker, Patron) |
| `troopers.py` | Analyze trooper waves and deaths |
| `player_stats.py` | Extract player stats (K/D/A, net worth) |
| `combat_detection.py` | Detect combat engagements via health changes |
| `multi_query.py` | **Concurrent multi-query pattern** - heroes + troopers |

## Table Reference

### Player Entities

- `CCitadelPlayerPawn` - Hero body (position, health, movement)
- `CCitadelPlayerController` - Player identity (steam_id, stats, hero_id)

### NPCs

- `CNPC_Trooper` - Lane minions
- `CNPC_TrooperNeutral` - Jungle camps (denizens)
- `CNPC_TrooperBoss` - Guardian (lane objective)
- `CNPC_Boss_Tier2` - Walker (lane objective)
- `CNPC_Boss_Tier3` - Patron (final objective)
- `CNPC_MidBoss` - Neutral mid boss

### Map Objects

- `CCitadelZipLineNode` - Zipline territory markers
- `CCitadel_Destroyable_Building` - Shrines
- `CCitadelItemPickupRejuv` - Mid Boss drop (Rejuvenator)

## Common Fields

Most entities share these fields:

| Field | Type | Description |
|-------|------|-------------|
| `tick` | int32 | Game tick (64 ticks/second) |
| `entity_index` | int32 | Unique entity identifier |
| `delta_type` | string | `create`, `update`, `delete`, `leave` |
| `m_iHealth` | int32 | Current health |
| `m_iMaxHealth` | int32 | Maximum health |
| `m_iTeamNum` | int32 | Team (2=Amber, 3=Sapphire) |
| `CBodyComponent__m_cellX` | int32 | Cell X coordinate |
| `CBodyComponent__m_cellY` | int32 | Cell Y coordinate |
| `CBodyComponent__m_cellZ` | int32 | Cell Z coordinate |

### Position Calculation

```sql
-- World coordinates from cell coordinates (128-unit resolution)
SELECT 
    tick,
    entity_index,
    (CBodyComponent__m_cellX * 128) - 16384 AS position_x,
    (CBodyComponent__m_cellY * 128) - 16384 AS position_y,
    (CBodyComponent__m_cellZ * 128) - 16384 AS position_z
FROM CCitadelPlayerPawn
```

### Time Calculation

```sql
-- Approximate game time from tick (ignores pauses)
SELECT 
    tick,
    tick / 64.0 AS game_seconds,
    tick / 64 / 60 AS game_minutes
FROM CCitadelPlayerPawn
```
