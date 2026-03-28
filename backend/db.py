# MOTH's Rollup — backend/db.py
# Uses psycopg2 (sync) wrapped in asyncio run_in_executor for FastAPI compatibility.

import os
import asyncio
from functools import partial
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool

_pool: ThreadedConnectionPool | None = None


def _get_pool() -> ThreadedConnectionPool:
    global _pool
    if _pool is None:
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            raise RuntimeError("DATABASE_URL environment variable not set.")
        _pool = ThreadedConnectionPool(1, 5, dsn=db_url)
    return _pool


@contextmanager
def _get_conn():
    pool = _get_pool()
    conn = pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


def _run(func, *args, **kwargs):
    """Run a sync function in a thread pool so FastAPI stays non-blocking."""
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(None, partial(func, *args, **kwargs))


# ---------------------------------------------------------------------------
# Schema init (called on startup)
# ---------------------------------------------------------------------------

def _init_schema():
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS players (
                    id          SERIAL PRIMARY KEY,
                    name        TEXT UNIQUE NOT NULL,
                    handicap    INTEGER NOT NULL DEFAULT 0,
                    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS rounds (
                    id           SERIAL PRIMARY KEY,
                    player_id    INTEGER NOT NULL REFERENCES players(id) ON DELETE CASCADE,
                    date         DATE NOT NULL,
                    score        INTEGER NOT NULL,
                    new_handicap INTEGER NOT NULL,
                    recorded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS rounds_date_idx ON rounds(date DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS rounds_player_date_idx ON rounds(player_id, date DESC)")


async def get_pool():
    """Initialise pool and schema on startup."""
    await _run(_get_pool)
    await _run(_init_schema)


async def close_pool():
    global _pool
    if _pool:
        _pool.closeall()
        _pool = None


# ---------------------------------------------------------------------------
# Players
# ---------------------------------------------------------------------------

def _get_all_players():
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, name, handicap FROM players ORDER BY name")
            return [dict(r) for r in cur.fetchall()]


async def get_all_players() -> list[dict]:
    return await _run(_get_all_players)


def _get_player_handicap(name: str):
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT handicap FROM players WHERE LOWER(name) = LOWER(%s)",
                (name.strip(),)
            )
            row = cur.fetchone()
            return row[0] if row else None


async def get_player_handicap(name: str) -> int | None:
    return await _run(_get_player_handicap, name)


def _add_new_player(name: str, handicap: int):
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO players (name, handicap)
                VALUES (%s, %s)
                ON CONFLICT (name) DO NOTHING
                """,
                (name.strip(), handicap)
            )


async def add_new_player(name: str, handicap: int) -> None:
    await _run(_add_new_player, name, handicap)


# ---------------------------------------------------------------------------
# Rounds
# ---------------------------------------------------------------------------

def _save_round_results(results: list[dict], date_str: str):
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name FROM players")
            name_to_id = {row[1].strip().lower(): row[0] for row in cur.fetchall()}

            for r in results:
                if r.get("score") is None or r.get("new_handicap") is None:
                    continue
                player_id = name_to_id.get(r["name"].strip().lower())
                if player_id is None:
                    continue

                cur.execute(
                    "UPDATE players SET handicap = %s WHERE id = %s",
                    (r["new_handicap"], player_id)
                )
                cur.execute(
                    """
                    INSERT INTO rounds (player_id, date, score, new_handicap)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (player_id, date_str, r["score"], r["new_handicap"])
                )


async def save_round_results(results: list[dict], date_str: str) -> None:
    await _run(_save_round_results, results, date_str)


def _get_last_round_date():
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(date) FROM rounds")
            row = cur.fetchone()
            if row and row[0]:
                return str(row[0])
            return "No previous round"


async def get_last_round_date() -> str:
    return await _run(_get_last_round_date)


def _get_last_round_results():
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT MAX(date) AS last_date FROM rounds")
            row = cur.fetchone()
            if not row or not row["last_date"]:
                return []
            last_date = row["last_date"]
            cur.execute(
                """
                SELECT p.name, r.score, r.new_handicap
                FROM rounds r
                JOIN players p ON p.id = r.player_id
                WHERE r.date = %s
                ORDER BY r.score DESC
                """,
                (last_date,)
            )
            return [dict(r) for r in cur.fetchall()]


async def get_last_round_results() -> list[dict]:
    return await _run(_get_last_round_results)


def _get_player_history(name: str):
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT r.date, r.score, r.new_handicap
                FROM rounds r
                JOIN players p ON p.id = r.player_id
                WHERE LOWER(p.name) = LOWER(%s)
                ORDER BY r.date DESC
                """,
                (name.strip(),)
            )
            return [dict(r) for r in cur.fetchall()]


async def get_player_history(name: str) -> list[dict]:
    return await _run(_get_player_history, name)
