# MOTH's Rollup — backend/db.py
# Uses psycopg2 (sync) wrapped in asyncio run_in_executor for FastAPI compatibility.
# v2: rollup_id added throughout — all queries are scoped to a specific rollup.

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
        # Test if connection is alive, reconnect if not
        try:
            conn.cursor().execute("SELECT 1")
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            pool.putconn(conn, close=True)
            conn = pool.getconn()
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
                CREATE TABLE IF NOT EXISTS rollups (
                    id              SERIAL PRIMARY KEY,
                    name            TEXT UNIQUE NOT NULL,
                    ig_search_term  TEXT NOT NULL,
                    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                INSERT INTO rollups (name, ig_search_term)
                VALUES ('MOTH''s Rollup', 'MOTH')
                ON CONFLICT (name) DO NOTHING
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS players (
                    id          SERIAL PRIMARY KEY,
                    rollup_id   INTEGER NOT NULL REFERENCES rollups(id),
                    name        TEXT NOT NULL,
                    handicap    INTEGER NOT NULL DEFAULT 0,
                    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (rollup_id, name)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS rounds (
                    id           SERIAL PRIMARY KEY,
                    player_id    INTEGER NOT NULL REFERENCES players(id) ON DELETE CASCADE,
                    rollup_id    INTEGER NOT NULL REFERENCES rollups(id),
                    date         DATE NOT NULL,
                    score        INTEGER NOT NULL,
                    new_handicap INTEGER NOT NULL,
                    recorded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS players_rollup_idx ON players(rollup_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS rounds_rollup_idx ON rounds(rollup_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS rounds_date_idx ON rounds(date DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS rounds_rollup_date_idx ON rounds(rollup_id, date DESC)")
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
# Rollups
# ---------------------------------------------------------------------------

def _get_all_rollups():
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, name, ig_search_term FROM rollups ORDER BY name")
            return [dict(r) for r in cur.fetchall()]


async def get_all_rollups() -> list[dict]:
    return await _run(_get_all_rollups)


def _get_or_create_rollup(name: str, ig_search_term: str) -> int:
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO rollups (name, ig_search_term)
                VALUES (%s, %s)
                ON CONFLICT (name) DO UPDATE SET ig_search_term = EXCLUDED.ig_search_term
                RETURNING id
            """, (name, ig_search_term))
            return cur.fetchone()[0]


async def get_or_create_rollup(name: str, ig_search_term: str) -> int:
    return await _run(_get_or_create_rollup, name, ig_search_term)


# ---------------------------------------------------------------------------
# Players
# ---------------------------------------------------------------------------

def _get_all_players(rollup_id: int):
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, name, handicap FROM players WHERE rollup_id = %s ORDER BY name",
                (rollup_id,)
            )
            return [dict(r) for r in cur.fetchall()]


async def get_all_players(rollup_id: int) -> list[dict]:
    return await _run(_get_all_players, rollup_id)


def _add_new_player(rollup_id: int, name: str, handicap: int):
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO players (rollup_id, name, handicap)
                VALUES (%s, %s, %s)
                ON CONFLICT (rollup_id, name) DO UPDATE SET handicap = EXCLUDED.handicap
                """,
                (rollup_id, name.strip(), handicap)
            )


async def add_new_player(rollup_id: int, name: str, handicap: int) -> None:
    await _run(_add_new_player, rollup_id, name, handicap)


# ---------------------------------------------------------------------------
# Rounds
# ---------------------------------------------------------------------------

def _save_round_results(results: list[dict], date_str: str, rollup_id: int):
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, name FROM players WHERE rollup_id = %s",
                (rollup_id,)
            )
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
                    INSERT INTO rounds (player_id, rollup_id, date, score, new_handicap)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (player_id, rollup_id, date_str, r["score"], r["new_handicap"])
                )


async def save_round_results(results: list[dict], date_str: str, rollup_id: int) -> None:
    await _run(_save_round_results, results, date_str, rollup_id)


def _get_last_round_date(rollup_id: int):
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(date) FROM rounds WHERE rollup_id = %s",
                (rollup_id,)
            )
            row = cur.fetchone()
            if row and row[0]:
                return str(row[0])
            return "No previous round"


async def get_last_round_date(rollup_id: int) -> str:
    return await _run(_get_last_round_date, rollup_id)


def _get_last_round_results(rollup_id: int):
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT MAX(date) AS last_date FROM rounds WHERE rollup_id = %s",
                (rollup_id,)
            )
            row = cur.fetchone()
            if not row or not row["last_date"]:
                return []
            last_date = row["last_date"]
            cur.execute(
                """
                SELECT p.name, r.score, r.new_handicap
                FROM rounds r
                JOIN players p ON p.id = r.player_id
                WHERE r.rollup_id = %s AND r.date = %s
                ORDER BY r.score DESC
                """,
                (rollup_id, last_date)
            )
            return [dict(r) for r in cur.fetchall()]


async def get_last_round_results(rollup_id: int) -> list[dict]:
    return await _run(_get_last_round_results, rollup_id)


def _get_player_history(name: str, rollup_id: int):
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT r.date, r.score, r.new_handicap
                FROM rounds r
                JOIN players p ON p.id = r.player_id
                WHERE r.rollup_id = %s AND LOWER(p.name) = LOWER(%s)
                ORDER BY r.date DESC
                """,
                (rollup_id, name.strip())
            )
            return [dict(r) for r in cur.fetchall()]


async def get_player_history(name: str, rollup_id: int) -> list[dict]:
    return await _run(_get_player_history, name, rollup_id)


def _get_round_dates(rollup_id: int):
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT date FROM rounds WHERE rollup_id = %s ORDER BY date DESC",
                (rollup_id,)
            )
            return [str(r[0]) for r in cur.fetchall()]


async def get_round_dates(rollup_id: int) -> list[str]:
    return await _run(_get_round_dates, rollup_id)


def _get_round_by_date(date_str: str, rollup_id: int):
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT p.name, r.score, r.new_handicap
                FROM rounds r
                JOIN players p ON p.id = r.player_id
                WHERE r.rollup_id = %s AND r.date = %s
                ORDER BY r.score DESC
                """,
                (rollup_id, date_str)
            )
            return [dict(r) for r in cur.fetchall()]


async def get_round_by_date(date_str: str, rollup_id: int) -> list[dict]:
    return await _run(_get_round_by_date, date_str, rollup_id)
