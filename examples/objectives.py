#!/usr/bin/env python3
"""
Track objective health (Guardian, Walker, Patron).

Monitor objective damage and destruction events. Useful for detecting
objective pushes, team fights at objectives, and game-ending sequences.
"""

import asyncio
import os

from demoflight import Client

DEMOFLIGHT_URL = os.environ.get("DEMOFLIGHT_URL", "grpc://localhost:50051")

# Query multiple objective types concurrently
GUARDIAN_QUERY = """
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
FROM CNPC_TrooperBoss
"""

WALKER_QUERY = """
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
FROM CNPC_Boss_Tier2
"""

PATRON_QUERY = """
SELECT 
    tick,
    tick / 64.0 AS game_seconds,
    entity_index,
    delta_type,
    m_iTeamNum AS team,
    m_iHealth AS health,
    m_iMaxHealth AS max_health
FROM CNPC_Boss_Tier3
"""


async def main(broadcast_url: str):
    async with Client(DEMOFLIGHT_URL) as client:
        async with await client.register_source(broadcast_url) as session:
            print(f"Connected, {len(session.get_tables())} tables available")

            guardian_q = await session.add_query(GUARDIAN_QUERY)
            walker_q = await session.add_query(WALKER_QUERY)
            patron_q = await session.add_query(PATRON_QUERY)

            objectives: dict[tuple[str, int], dict] = {}

            async def track_objectives(query, obj_type: str):
                async for batch in query:
                    df = batch.to_pandas()

                    for _, row in df.iterrows():
                        entity_idx = row["entity_index"]
                        health = row["health"]
                        max_health = row["max_health"]
                        team = row["team"]
                        delta = row["delta_type"]

                        key = (obj_type, entity_idx)

                        if delta == "create":
                            objectives[key] = {
                                "type": obj_type,
                                "team": team,
                                "health": health,
                                "max_health": max_health,
                            }
                            print(
                                f"[{row['game_seconds']:.1f}s] {obj_type} spawned: "
                                f"team {team}, {health}/{max_health} HP"
                            )

                        elif delta == "delete":
                            print(
                                f"[{row['game_seconds']:.1f}s] {obj_type} DESTROYED: "
                                f"team {team}"
                            )
                            objectives.pop(key, None)

                        elif key in objectives:
                            old_health = objectives[key]["health"]
                            if health != old_health:
                                damage = old_health - health
                                pct = health / max_health * 100 if max_health else 0
                                print(
                                    f"[{row['game_seconds']:.1f}s] {obj_type} team {team}: "
                                    f"{old_health} -> {health} ({damage:+d} dmg, {pct:.0f}%)"
                                )
                                objectives[key]["health"] = health

            # MUST consume all queries concurrently
            await asyncio.gather(
                track_objectives(guardian_q, "Guardian"),
                track_objectives(walker_q, "Walker"),
                track_objectives(patron_q, "Patron"),
            )


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python objectives.py <broadcast_url>")
        sys.exit(1)

    asyncio.run(main(sys.argv[1]))
