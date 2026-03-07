#!/usr/bin/env python3
"""
Analyze trooper waves and deaths.

Troopers spawn in squads of 4 and march down lanes. Track their spawns,
deaths, and positions to understand lane pressure and farming patterns.
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
    (CBodyComponent__m_cellX * 128) - 16384 AS position_x,
    (CBodyComponent__m_cellY * 128) - 16384 AS position_y
FROM CNPC_Trooper
"""


def get_lane(x: float) -> str:
    if x < -1530:
        return "Yellow"
    elif x > 1530:
        return "Green"
    else:
        return "Blue"


async def main(broadcast_url: str):
    async with Client(DEMOFLIGHT_URL) as client:
        async with await client.register_source(broadcast_url) as session:
            print(f"Connected, {len(session.get_tables())} tables available")

            query = await session.add_query(QUERY)

            # Stats tracking
            stats = {
                "spawns": {"Amber": 0, "Sapphire": 0},
                "deaths": {"Amber": 0, "Sapphire": 0},
                "by_lane": {
                    "Yellow": {"Amber": 0, "Sapphire": 0},
                    "Blue": {"Amber": 0, "Sapphire": 0},
                    "Green": {"Amber": 0, "Sapphire": 0},
                },
            }
            troopers: dict[int, dict] = {}

            batch_count = 0
            async for batch in query:
                df = batch.to_pandas()
                batch_count += 1

                for _, row in df.iterrows():
                    entity_idx = row["entity_index"]
                    team_name = "Amber" if row["team"] == 2 else "Sapphire"
                    delta = row["delta_type"]
                    lane = get_lane(row["position_x"])

                    if delta == "create":
                        troopers[entity_idx] = {
                            "team": team_name,
                            "lane": lane,
                            "spawn_time": row["game_seconds"],
                        }
                        stats["spawns"][team_name] += 1

                    elif delta == "delete" and entity_idx in troopers:
                        trooper = troopers.pop(entity_idx)
                        stats["deaths"][trooper["team"]] += 1
                        stats["by_lane"][trooper["lane"]][trooper["team"]] += 1

                # Print summary periodically
                if batch_count % 10 == 0:
                    print(f"\n=== Trooper Stats (batch {batch_count}) ===")
                    print(
                        f"Spawns: Amber {stats['spawns']['Amber']}, "
                        f"Sapphire {stats['spawns']['Sapphire']}"
                    )
                    print(
                        f"Deaths: Amber {stats['deaths']['Amber']}, "
                        f"Sapphire {stats['deaths']['Sapphire']}"
                    )
                    print("Deaths by lane:")
                    for lane_name in ["Yellow", "Blue", "Green"]:
                        lane_stats = stats["by_lane"][lane_name]
                        print(
                            f"  {lane_name}: Amber {lane_stats['Amber']}, "
                            f"Sapphire {lane_stats['Sapphire']}"
                        )
                    print(f"Active troopers: {len(troopers)}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python troopers.py <broadcast_url>")
        sys.exit(1)

    asyncio.run(main(sys.argv[1]))
