# MOTH's Rollup — backend/db.py
# Replaces sheets.py. Uses asyncpg with a connection pool.
# All public functions maintain the same signatures as sheets.py
# so main.py changes are minimal.

import os
import asyncpg

_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    """Return the shared connection pool, creating it if needed."""
    global _pool
    if _pool is None:
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            raise RuntimeError("DATABASE_URL environment variable not set.")
        _pool = await asyncpg.create_pool(
            db_url,
            min_size=1,
            max_size=5,
            statement_cache_size=0,  # Required for Neon's PgBouncer pooler
        )
    return _pool


async def close_pool():
    """Gracefully close the pool on shutdown."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


# ---------------------------------------------------------------------------
# Players
# ---------------------------------------------------------------------------

async def get_all_players() -> list[dict]:
    """
    Return all players ordered alphabetically.
    Each dict: {id, name, handicap}
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, name, handicap FROM players ORDER BY name"
        )
    return [dict(r) for r in rows]


async def get_player_handicap(name: str) -> int | None:
    """Return a player's current handicap, or None if not found."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT handicap FROM players WHERE LOWER(name) = LOWER($1)",
            name.strip(),
        )
    return row["handicap"] if row else None


async def add_new_player(name: str, handicap: int) -> None:
    """Insert a new player. Raises if name already exists."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO players (name, handicap)
            VALUES ($1, $2)
            ON CONFLICT (name) DO NOTHING
            """,
            name.strip(),
            handicap,
        )


async def update_player_handicap(player_id: int, new_handicap: int, conn) -> None:
    """Update a player's current handicap (called within save_round_results)."""
    await conn.execute(
        "UPDATE players SET handicap = $1 WHERE id = $2",
        new_handicap,
        player_id,
    )


# ---------------------------------------------------------------------------
# Rounds
# ---------------------------------------------------------------------------

async def save_round_results(results: list[dict], date_str: str) -> None:
    """
    Persist round results to the database.
    Updates each player's current handicap and inserts a rounds row.

    results: list of player dicts with keys name, score, new_handicap.
             Players with score=None are skipped.
    """
    pool = await get_pool()

    # Build name→id lookup
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, name FROM players")
        name_to_id = {r["name"].strip().lower(): r["id"] for r in rows}

        async with conn.transaction():
            for r in results:
                if r.get("score") is None or r.get("new_handicap") is None:
                    continue
                player_id = name_to_id.get(r["name"].strip().lower())
                if player_id is None:
                    continue

                # Update current handicap on players table
                await update_player_handicap(player_id, r["new_handicap"], conn)

                # Insert round record (upsert — safe to call save twice)
                await conn.execute(
                    """
                    INSERT INTO rounds (player_id, date, score, new_handicap)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT DO NOTHING
                    """,
                    player_id,
                    date_str,
                    r["score"],
                    r["new_handicap"],
                )


async def get_last_round_date() -> str:
    """Return the most recent round date as a string, or 'No previous round'."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT MAX(date) AS last_date FROM rounds")
    if row and row["last_date"]:
        return str(row["last_date"])
    return "No previous round"


async def get_last_round_results() -> list[dict]:
    """
    Return scores from the most recent round, sorted by score descending.
    Each dict: {name, score, new_handicap}
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        last_date_row = await conn.fetchrow("SELECT MAX(date) AS last_date FROM rounds")
        if not last_date_row or not last_date_row["last_date"]:
            return []
        last_date = last_date_row["last_date"]

        rows = await conn.fetch(
            """
            SELECT p.name, r.score, r.new_handicap
            FROM rounds r
            JOIN players p ON p.id = r.player_id
            WHERE r.date = $1
            ORDER BY r.score DESC
            """,
            last_date,
        )
    return [dict(r) for r in rows]


async def get_player_history(name: str) -> list[dict]:
    """
    Return full round history for a player, most recent first.
    Each dict: {date, score, new_handicap}
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT r.date, r.score, r.new_handicap
            FROM rounds r
            JOIN players p ON p.id = r.player_id
            WHERE LOWER(p.name) = LOWER($1)
            ORDER BY r.date DESC
            """,
            name.strip(),
        )
    return [dict(r) for r in rows]
