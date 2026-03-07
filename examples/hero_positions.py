#!/usr/bin/env python3
"""
Track hero positions over time.

Outputs hero position data for heatmap visualization or pathing analysis.
"""

import asyncio
import os

from demoflight import Client

DEMOFLIGHT_URL = os.environ.get("DEMOFLIGHT_URL", "grpc://localhost:50051")

QUERY = """
SELECT 
    tick,
    tick / 64.0 AS game_seconds,
    entity_index,
    delta_type,
    m_iTeamNum AS team,
    m_iHealth AS health,
    m_iMaxHealth AS max_health,
    -- World position from cell coordinates (128-unit resolution)
    (CBodyComponent__m_cellX * 128) - 16384 AS position_x,
    (CBodyComponent__m_cellY * 128) - 16384 AS position_y,
    (CBodyComponent__m_cellZ * 128) - 16384 AS position_z
FROM CCitadelPlayerPawn
WHERE delta_type IN ('create', 'update')
"""


async def main(broadcast_url: str):
    async with Client(DEMOFLIGHT_URL) as client:
        async with await client.register_source(broadcast_url) as session:
            print(f"Connected, {len(session.get_tables())} tables available")

            query = await session.add_query(QUERY)

            row_count = 0
            async for batch in query:
                df = batch.to_pandas()
                row_count += len(df)

                # Sample output - first few rows of each batch
                if row_count <= 100:
                    print(df.to_string(index=False))
                    print("---")

                # Stop after collecting enough data
                if row_count >= 1000:
                    print(f"\nCollected {row_count} position updates")
                    break


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python hero_positions.py <broadcast_url>")
        print("  Or set BROADCAST_URL environment variable")
        sys.exit(1)

    broadcast_url = sys.argv[1] if len(sys.argv) > 1 else os.environ["BROADCAST_URL"]
    asyncio.run(main(broadcast_url))
