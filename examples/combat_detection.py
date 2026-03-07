#!/usr/bin/env python3
"""
Detect combat engagements via health changes.

Monitors hero health to detect when combat is happening. Identifies
engagements, damage taken, and potential deaths.
"""

import asyncio
import os
from dataclasses import dataclass

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
FROM CCitadelPlayerPawn
WHERE delta_type IN ('create', 'update', 'delete')
"""


@dataclass
class HeroState:
    entity_index: int
    team: str
    health: int
    max_health: int
    position_x: float
    position_y: float
    last_damage_tick: int
    in_combat: bool


async def main(broadcast_url: str):
    async with Client(DEMOFLIGHT_URL) as client:
        async with await client.register_source(broadcast_url) as session:
            print(f"Connected, {len(session.get_tables())} tables available")

            query = await session.add_query(QUERY)

            heroes: dict[int, HeroState] = {}
            combat_cooldown_ticks = 192  # 3 seconds at 64 ticks/s

            async for batch in query:
                df = batch.to_pandas()

                for _, row in df.iterrows():
                    entity_idx = row["entity_index"]
                    tick = row["tick"]
                    team = "Amber" if row["team"] == 2 else "Sapphire"
                    health = row["health"]
                    max_health = row["max_health"]
                    delta = row["delta_type"]

                    if delta == "create":
                        heroes[entity_idx] = HeroState(
                            entity_index=entity_idx,
                            team=team,
                            health=health,
                            max_health=max_health,
                            position_x=row["position_x"],
                            position_y=row["position_y"],
                            last_damage_tick=0,
                            in_combat=False,
                        )
                        print(
                            f"[{row['game_seconds']:.1f}s] Hero spawned: "
                            f"entity {entity_idx}, {team}"
                        )

                    elif delta == "delete" and entity_idx in heroes:
                        hero = heroes.pop(entity_idx)
                        print(
                            f"[{row['game_seconds']:.1f}s] Hero DIED: "
                            f"entity {entity_idx}, {hero.team} at "
                            f"({hero.position_x:.0f}, {hero.position_y:.0f})"
                        )

                    elif entity_idx in heroes:
                        hero = heroes[entity_idx]
                        old_health = hero.health
                        hero.health = health
                        hero.position_x = row["position_x"]
                        hero.position_y = row["position_y"]

                        # Detect damage
                        if health < old_health:
                            damage = old_health - health
                            pct = health / max_health * 100 if max_health else 0

                            if not hero.in_combat:
                                hero.in_combat = True
                                print(
                                    f"[{row['game_seconds']:.1f}s] COMBAT START: "
                                    f"{hero.team} hero at "
                                    f"({hero.position_x:.0f}, {hero.position_y:.0f})"
                                )

                            hero.last_damage_tick = tick
                            print(
                                f"[{row['game_seconds']:.1f}s] {hero.team} took {damage} dmg "
                                f"({pct:.0f}% HP remaining)"
                            )

                        # Check for combat end
                        elif hero.in_combat:
                            if tick - hero.last_damage_tick > combat_cooldown_ticks:
                                hero.in_combat = False
                                print(
                                    f"[{row['game_seconds']:.1f}s] COMBAT END: "
                                    f"{hero.team} hero disengaged"
                                )


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python combat_detection.py <broadcast_url>")
        sys.exit(1)

    asyncio.run(main(sys.argv[1]))
