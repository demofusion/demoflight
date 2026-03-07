#!/usr/bin/env python3
"""
Multi-query example: track heroes AND troopers concurrently.

Demonstrates the required pattern for consuming multiple query streams.
All streams MUST be drained concurrently to avoid deadlock.
"""

import asyncio
import os
from dataclasses import dataclass, field

from demoflight import Client

DEMOFLIGHT_URL = os.environ.get("DEMOFLIGHT_URL", "grpc://localhost:50051")

HERO_QUERY = """
SELECT 
    tick,
    entity_index,
    delta_type,
    m_iTeamNum AS team,
    m_iHealth AS health
FROM CCitadelPlayerPawn
WHERE delta_type IN ('create', 'update', 'delete')
"""

TROOPER_QUERY = """
SELECT 
    tick,
    entity_index,
    delta_type,
    m_iTeamNum AS team
FROM CNPC_Trooper
WHERE delta_type IN ('create', 'delete')
"""


@dataclass
class GameState:
    heroes_alive: dict = field(default_factory=dict)
    trooper_kills: dict = field(default_factory=lambda: {"Amber": 0, "Sapphire": 0})
    hero_deaths: dict = field(default_factory=lambda: {"Amber": 0, "Sapphire": 0})
    last_tick: int = 0


async def process_heroes(query, state: GameState):
    async for batch in query:
        df = batch.to_pandas()

        for _, row in df.iterrows():
            entity_idx = row["entity_index"]
            team = "Amber" if row["team"] == 2 else "Sapphire"
            delta = row["delta_type"]
            state.last_tick = max(state.last_tick, row["tick"])

            if delta == "create":
                state.heroes_alive[entity_idx] = team

            elif delta == "delete" and entity_idx in state.heroes_alive:
                state.hero_deaths[team] += 1
                del state.heroes_alive[entity_idx]
                game_time = row["tick"] / 64.0
                print(f"[{game_time:.1f}s] HERO DEATH: {team}")


async def process_troopers(query, state: GameState):
    async for batch in query:
        df = batch.to_pandas()

        for _, row in df.iterrows():
            team = "Amber" if row["team"] == 2 else "Sapphire"
            delta = row["delta_type"]
            state.last_tick = max(state.last_tick, row["tick"])

            if delta == "delete":
                state.trooper_kills[team] += 1


async def print_summary(state: GameState):
    while True:
        await asyncio.sleep(5)
        if state.last_tick > 0:
            game_time = state.last_tick / 64.0
            print(f"\n=== Summary @ {game_time:.0f}s ===")
            print(
                f"Hero deaths: Amber {state.hero_deaths['Amber']}, Sapphire {state.hero_deaths['Sapphire']}"
            )
            print(
                f"Trooper kills: Amber {state.trooper_kills['Amber']}, Sapphire {state.trooper_kills['Sapphire']}"
            )
            print(f"Heroes alive: {len(state.heroes_alive)}")


async def main(broadcast_url: str):
    async with Client(DEMOFLIGHT_URL) as client:
        async with await client.register_source(broadcast_url) as session:
            print(f"Connected, {len(session.get_tables())} tables available")

            # Create multiple queries
            hero_q = await session.add_query(HERO_QUERY)
            trooper_q = await session.add_query(TROOPER_QUERY)

            state = GameState()

            # Create summary printer task
            summary_task = asyncio.create_task(print_summary(state))

            try:
                # CRITICAL: Must consume ALL queries concurrently
                # Sequential consumption will deadlock because the GOTV
                # parser feeds all streams from a single source
                await asyncio.gather(
                    process_heroes(hero_q, state),
                    process_troopers(trooper_q, state),
                )
            finally:
                summary_task.cancel()
                try:
                    await summary_task
                except asyncio.CancelledError:
                    pass

            print("\n=== Final Stats ===")
            print(f"Hero deaths: {state.hero_deaths}")
            print(f"Trooper kills: {state.trooper_kills}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python multi_query.py <broadcast_url>")
        sys.exit(1)

    asyncio.run(main(sys.argv[1]))
