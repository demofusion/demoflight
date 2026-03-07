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
    entity_index,
    delta_type,
    m_iTeamNum,
    m_iHealth,
    m_iMaxHealth,
    CBodyComponent__m_cellX,
    CBodyComponent__m_cellY,
    CBodyComponent__m_cellZ
FROM CCitadelPlayerPawn
WHERE delta_type IN ('create', 'update')
LIMIT 500
"""


async def main(broadcast_url: str):
    async with Client(DEMOFLIGHT_URL) as client:
        async with await client.register_source(broadcast_url) as session:
            print(f"Connected, {len(session.get_tables())} tables available")

            query = await session.add_query(QUERY)

            row_count = 0
            async for batch in query:
                df = batch.to_pandas()

                # Calculate world position
                df["position_x"] = (df["CBodyComponent__m_cellX"] * 128) - 16384
                df["position_y"] = (df["CBodyComponent__m_cellY"] * 128) - 16384
                df["position_z"] = (df["CBodyComponent__m_cellZ"] * 128) - 16384
                df["game_seconds"] = df["tick"] / 64.0

                row_count += len(df)

                # Sample output
                if row_count <= 100:
                    print(
                        df[
                            [
                                "tick",
                                "entity_index",
                                "m_iTeamNum",
                                "m_iHealth",
                                "position_x",
                                "position_y",
                            ]
                        ].to_string(index=False)
                    )
                    print("---")

            print(f"\nCollected {row_count} position updates")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python hero_positions.py <broadcast_url>")
        print("  Or set BROADCAST_URL environment variable")
        sys.exit(1)

    broadcast_url = sys.argv[1] if len(sys.argv) > 1 else os.environ["BROADCAST_URL"]
    asyncio.run(main(broadcast_url))
