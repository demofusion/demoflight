#!/usr/bin/env python3
"""
Extract player stats from PlayerController entities.

PlayerController tracks cumulative stats (kills, deaths, assists, net worth)
while PlayerPawn tracks the physical hero body.
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
    m_steamID AS steam_id,
    m_nHeroID AS hero_id,
    m_iPlayerSlot AS player_slot,
    m_PlayerStats__m_iKills AS kills,
    m_PlayerStats__m_iDeaths AS deaths,
    m_PlayerStats__m_iAssists AS assists,
    m_PlayerStats__m_iNetWorth AS net_worth,
    m_PlayerStats__m_iLevel AS level
FROM CCitadelPlayerController
"""


async def main(broadcast_url: str):
    async with Client(DEMOFLIGHT_URL) as client:
        async with await client.register_source(broadcast_url) as session:
            print(f"Connected, {len(session.get_tables())} tables available")

            query = await session.add_query(QUERY)

            players: dict[int, dict] = {}
            last_print_tick = 0

            async for batch in query:
                df = batch.to_pandas()

                for _, row in df.iterrows():
                    entity_idx = row["entity_index"]
                    delta = row["delta_type"]

                    if delta in ("create", "update"):
                        players[entity_idx] = {
                            "steam_id": row["steam_id"],
                            "hero_id": row["hero_id"],
                            "slot": row["player_slot"],
                            "team": "Amber" if row["team"] == 2 else "Sapphire",
                            "kills": row["kills"],
                            "deaths": row["deaths"],
                            "assists": row["assists"],
                            "net_worth": row["net_worth"],
                            "level": row["level"],
                            "tick": row["tick"],
                        }

                # Print scoreboard every ~30 seconds (1920 ticks)
                current_tick = df["tick"].max() if len(df) > 0 else last_print_tick
                if current_tick - last_print_tick >= 1920:
                    last_print_tick = current_tick
                    game_time = current_tick / 64.0

                    print(f"\n=== Scoreboard @ {game_time:.0f}s ===")

                    # Sort by team then slot
                    sorted_players = sorted(
                        players.values(), key=lambda p: (p["team"], p["slot"])
                    )

                    for team in ["Amber", "Sapphire"]:
                        team_players = [p for p in sorted_players if p["team"] == team]
                        if not team_players:
                            continue

                        print(f"\n{team} Team:")
                        print(
                            f"{'Slot':<5} {'Hero':<8} {'K/D/A':<12} "
                            f"{'Net Worth':<10} {'Level':<6}"
                        )
                        print("-" * 45)

                        for p in team_players:
                            kda = f"{p['kills']}/{p['deaths']}/{p['assists']}"
                            print(
                                f"{p['slot']:<5} {p['hero_id']:<8} {kda:<12} "
                                f"{p['net_worth']:<10} {p['level']:<6}"
                            )


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python player_stats.py <broadcast_url>")
        sys.exit(1)

    asyncio.run(main(sys.argv[1]))
