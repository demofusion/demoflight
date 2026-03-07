#!/usr/bin/env python3
"""
Monitor lane control via zipline node ownership.

Zipline nodes form chains along each lane. Their team ownership indicates
which team controls that section. Track ownership changes to analyze
lane pressure and push timing.
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
    -- Position for lane identification
    (CBodyComponent__m_cellX * 128) - 16384 AS position_x,
    (CBodyComponent__m_cellY * 128) - 16384 AS position_y
FROM CCitadelZipLineNode
WHERE delta_type IN ('create', 'update')
"""


async def main(broadcast_url: str):
    async with Client(DEMOFLIGHT_URL) as client:
        async with await client.register_source(broadcast_url) as session:
            print(f"Connected, {len(session.get_tables())} tables available")

            query = await session.add_query(QUERY)

            # Track node ownership by entity_index
            nodes: dict[int, dict] = {}

            async for batch in query:
                df = batch.to_pandas()

                for _, row in df.iterrows():
                    entity_idx = row["entity_index"]
                    team = row["team"]

                    if row["delta_type"] == "create":
                        # Determine lane from X position
                        x = row["position_x"]
                        if x < -1530:
                            lane = "Yellow"
                        elif x > 1530:
                            lane = "Green"
                        else:
                            lane = "Blue"

                        nodes[entity_idx] = {
                            "lane": lane,
                            "team": team,
                            "position_y": row["position_y"],
                        }
                        print(
                            f"[{row['game_seconds']:.1f}s] Node {entity_idx} created: "
                            f"{lane} lane, team {team}"
                        )

                    elif entity_idx in nodes:
                        old_team = nodes[entity_idx]["team"]
                        if team != old_team:
                            lane = nodes[entity_idx]["lane"]
                            nodes[entity_idx]["team"] = team
                            print(
                                f"[{row['game_seconds']:.1f}s] Node {entity_idx} flipped: "
                                f"{lane} lane, team {old_team} -> {team}"
                            )

                # Summary every batch
                if nodes:
                    amber = sum(1 for n in nodes.values() if n["team"] == 2)
                    sapphire = sum(1 for n in nodes.values() if n["team"] == 3)
                    print(f"  Lane control: Amber {amber}, Sapphire {sapphire}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python lane_control.py <broadcast_url>")
        sys.exit(1)

    asyncio.run(main(sys.argv[1]))
