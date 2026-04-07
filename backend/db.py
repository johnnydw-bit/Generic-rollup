# Bramley Rollup — backend/db.py
# Per-request connections: opens a fresh connection for every DB operation,
# closes it immediately after. Eliminates stale connection errors from Neon
# dropping idle connections after ~5 minutes.

import json
import os
import asyncio
from functools import partial
from contextlib import contextmanager

import psycopg2
import psycopg2.extras


def _get_db_url() -> str:
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL environment variable not set.")
    return db_url


@contextmanager
def _get_conn():
    """Open a fresh connection, yield it, then close it — every time."""
    conn = psycopg2.connect(_get_db_url())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _run(func, *args, **kwargs):
    """Run a sync DB function in a thread pool so FastAPI stays non-blocking."""
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
            # No seed rollup — rollups are created and managed via the app UI
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
            cur.execute("""
                CREATE TABLE IF NOT EXISTS rollup_settings (
                    id                          SERIAL PRIMARY KEY,
                    rollup_id                   INTEGER NOT NULL UNIQUE REFERENCES rollups(id),
                    display_name                TEXT NOT NULL DEFAULT '',
                    ig_search_term              TEXT NOT NULL DEFAULT '',
                    run_days                    TEXT NOT NULL DEFAULT '[]',
                    tee_interval_minutes        INTEGER NOT NULL DEFAULT 8,
                    adjustment_table            TEXT NOT NULL DEFAULT '[
                        {"max_score": 17, "adjustment": 2},
                        {"max_score": 29, "adjustment": 1},
                        {"max_score": 37, "adjustment": 0},
                        {"max_score": 42, "adjustment": -1},
                        {"max_score": null, "adjustment": -2}
                    ]',
                    winner_bonus_enabled        BOOLEAN NOT NULL DEFAULT TRUE,
                    winner_gap_penalty1         INTEGER NOT NULL DEFAULT 0,
                    winner_gap_penalty2         INTEGER NOT NULL DEFAULT 0,
                    entry_fee                   NUMERIC(6,2) NOT NULL DEFAULT 0.00,
                    prize_places                INTEGER NOT NULL DEFAULT 3,
                    prize_pct_1st               INTEGER NOT NULL DEFAULT 60,
                    prize_pct_2nd               INTEGER NOT NULL DEFAULT 30,
                    prize_pct_3rd               INTEGER NOT NULL DEFAULT 10,
                    prize_pct_4th               INTEGER NOT NULL DEFAULT 0,
                    tie_handling                TEXT NOT NULL DEFAULT 'tournament',
                    preferred_team_size         INTEGER NOT NULL DEFAULT 4,
                    team_scoring_method         TEXT NOT NULL DEFAULT 'best2',
                    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS app_credentials (
                    id          INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
                    ig_username TEXT NOT NULL DEFAULT '',
                    ig_pin      TEXT NOT NULL DEFAULT '',
                    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                INSERT INTO app_credentials (id, ig_username, ig_pin)
                VALUES (1, '', '')
                ON CONFLICT (id) DO NOTHING
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS players_rollup_idx ON players(rollup_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS rounds_rollup_idx ON rounds(rollup_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS rounds_date_idx ON rounds(date DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS rounds_rollup_date_idx ON rounds(rollup_id, date DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS rounds_player_date_idx ON rounds(player_id, date DESC)")


async def init_db():
    """Initialise schema on startup."""
    await _run(_init_schema)


async def close_db():
    """No-op — no pool to close."""
    pass


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
            cur.execute("""
                INSERT INTO players (rollup_id, name, handicap)
                VALUES (%s, %s, %s)
                ON CONFLICT (rollup_id, name) DO UPDATE SET handicap = EXCLUDED.handicap
            """, (rollup_id, name.strip(), handicap))


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
                cur.execute("""
                    INSERT INTO rounds (player_id, rollup_id, date, score, new_handicap)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (player_id, rollup_id, date_str, r["score"], r["new_handicap"]))


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
            return str(row[0]) if row and row[0] else "No previous round"


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
            cur.execute("""
                SELECT p.name, r.score, r.new_handicap
                FROM rounds r
                JOIN players p ON p.id = r.player_id
                WHERE r.rollup_id = %s AND r.date = %s
                ORDER BY r.score DESC
            """, (rollup_id, row["last_date"]))
            return [dict(r) for r in cur.fetchall()]


async def get_last_round_results(rollup_id: int) -> list[dict]:
    return await _run(_get_last_round_results, rollup_id)


def _get_player_history(name: str, rollup_id: int):
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT r.date, r.score, r.new_handicap
                FROM rounds r
                JOIN players p ON p.id = r.player_id
                WHERE r.rollup_id = %s AND LOWER(p.name) = LOWER(%s)
                ORDER BY r.date DESC
            """, (rollup_id, name.strip()))
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
            cur.execute("""
                SELECT p.name, r.score, r.new_handicap
                FROM rounds r
                JOIN players p ON p.id = r.player_id
                WHERE r.rollup_id = %s AND r.date = %s
                ORDER BY r.score DESC
            """, (rollup_id, date_str))
            return [dict(r) for r in cur.fetchall()]


async def get_round_by_date(date_str: str, rollup_id: int) -> list[dict]:
    return await _run(_get_round_by_date, date_str, rollup_id)


# ---------------------------------------------------------------------------
# Rollup settings
# ---------------------------------------------------------------------------

DEFAULT_ADJUSTMENT_TABLE = [
    {"max_score": 17,   "adjustment": 2},
    {"max_score": 29,   "adjustment": 1},
    {"max_score": 37,   "adjustment": 0},
    {"max_score": 42,   "adjustment": -1},
    {"max_score": None, "adjustment": -2},
]


def _get_rollup_settings(rollup_id: int) -> dict:
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Always join rollups so name/term are from the canonical table
            cur.execute("""
                SELECT rs.*, r.name AS rollup_name, r.ig_search_term AS rollup_term
                FROM rollup_settings rs
                JOIN rollups r ON r.id = rs.rollup_id
                WHERE rs.rollup_id = %s
            """, (rollup_id,))
            row = cur.fetchone()
            if not row:
                # No settings row yet — return defaults from rollups table
                cur.execute(
                    "SELECT name, ig_search_term FROM rollups WHERE id = %s",
                    (rollup_id,)
                )
                rollup = cur.fetchone()
                return {
                    "rollup_id":            rollup_id,
                    "display_name":         rollup["name"] if rollup else "",
                    "ig_search_term":       rollup["ig_search_term"] if rollup else "",
                    "run_days":             [],
                    "tee_interval_minutes": 8,
                    "adjustment_table":     DEFAULT_ADJUSTMENT_TABLE,
                    "winner_bonus_enabled": True,
                    "winner_gap_penalty1":  0,
                    "winner_gap_penalty2":  0,
                    "entry_fee":            0.00,
                    "prize_places":         3,
                    "prize_pct_1st":        60,
                    "prize_pct_2nd":        30,
                    "prize_pct_3rd":        10,
                    "prize_pct_4th":        0,
                    "tie_handling":         "tournament",
                    "preferred_team_size":  4,
                    "team_scoring_method":  "best2",
                }
            d = dict(row)
            # Always use rollups table as source of truth for name/term
            d["display_name"]    = d["rollup_name"]
            d["ig_search_term"]  = d["rollup_term"]
            d["run_days"]        = json.loads(d["run_days"])
            d["adjustment_table"] = json.loads(d["adjustment_table"])
            d["entry_fee"]       = float(d["entry_fee"])
            return d


async def get_rollup_settings(rollup_id: int) -> dict:
    return await _run(_get_rollup_settings, rollup_id)


def _save_rollup_settings(rollup_id: int, s: dict):
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO rollup_settings (
                    rollup_id, display_name, ig_search_term, run_days,
                    tee_interval_minutes, adjustment_table,
                    winner_bonus_enabled, winner_gap_penalty1, winner_gap_penalty2,
                    entry_fee, prize_places,
                    prize_pct_1st, prize_pct_2nd, prize_pct_3rd, prize_pct_4th,
                    tie_handling, preferred_team_size, team_scoring_method,
                    updated_at
                ) VALUES (
                    %s,%s,%s,%s, %s,%s, %s,%s,%s, %s,%s, %s,%s,%s,%s, %s,%s,%s, NOW()
                )
                ON CONFLICT (rollup_id) DO UPDATE SET
                    display_name            = EXCLUDED.display_name,
                    ig_search_term          = EXCLUDED.ig_search_term,
                    run_days                = EXCLUDED.run_days,
                    tee_interval_minutes    = EXCLUDED.tee_interval_minutes,
                    adjustment_table        = EXCLUDED.adjustment_table,
                    winner_bonus_enabled    = EXCLUDED.winner_bonus_enabled,
                    winner_gap_penalty1     = EXCLUDED.winner_gap_penalty1,
                    winner_gap_penalty2     = EXCLUDED.winner_gap_penalty2,
                    entry_fee               = EXCLUDED.entry_fee,
                    prize_places            = EXCLUDED.prize_places,
                    prize_pct_1st           = EXCLUDED.prize_pct_1st,
                    prize_pct_2nd           = EXCLUDED.prize_pct_2nd,
                    prize_pct_3rd           = EXCLUDED.prize_pct_3rd,
                    prize_pct_4th           = EXCLUDED.prize_pct_4th,
                    tie_handling            = EXCLUDED.tie_handling,
                    preferred_team_size     = EXCLUDED.preferred_team_size,
                    team_scoring_method     = EXCLUDED.team_scoring_method,
                    updated_at              = NOW()
            """, (
                rollup_id,
                s["display_name"],
                s["ig_search_term"],
                json.dumps(s["run_days"]),
                s["tee_interval_minutes"],
                json.dumps(s["adjustment_table"]),
                s["winner_bonus_enabled"],
                s["winner_gap_penalty1"],
                s["winner_gap_penalty2"],
                s["entry_fee"],
                s["prize_places"],
                s["prize_pct_1st"],
                s["prize_pct_2nd"],
                s["prize_pct_3rd"],
                s["prize_pct_4th"],
                s["tie_handling"],
                s["preferred_team_size"],
                s["team_scoring_method"],
            ))
            # Keep rollups table in sync
            cur.execute("""
                UPDATE rollups SET name = %s, ig_search_term = %s WHERE id = %s
            """, (s["display_name"], s["ig_search_term"], rollup_id))


async def save_rollup_settings(rollup_id: int, settings: dict) -> None:
    await _run(_save_rollup_settings, rollup_id, settings)


# ---------------------------------------------------------------------------
# App credentials (global singleton)
# ---------------------------------------------------------------------------

def _get_credentials() -> dict:
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT ig_username, ig_pin FROM app_credentials WHERE id = 1")
            row = cur.fetchone()
            return dict(row) if row else {"ig_username": "", "ig_pin": ""}


async def get_credentials() -> dict:
    return await _run(_get_credentials)


def _save_credentials(ig_username: str, ig_pin: str):
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO app_credentials (id, ig_username, ig_pin, updated_at)
                VALUES (1, %s, %s, NOW())
                ON CONFLICT (id) DO UPDATE SET
                    ig_username = EXCLUDED.ig_username,
                    ig_pin      = EXCLUDED.ig_pin,
                    updated_at  = NOW()
            """, (ig_username, ig_pin))


async def save_credentials(ig_username: str, ig_pin: str) -> None:
    await _run(_save_credentials, ig_username, ig_pin)
