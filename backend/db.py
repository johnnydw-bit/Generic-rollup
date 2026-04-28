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
# Schema init
# ---------------------------------------------------------------------------

def _init_schema():
    with _get_conn() as conn:
        with conn.cursor() as cur:

            # ── Core tables ──────────────────────────────────────────────

            cur.execute("""
                CREATE TABLE IF NOT EXISTS rollups (
                    id              SERIAL PRIMARY KEY,
                    name            TEXT UNIQUE NOT NULL,
                    ig_search_term  TEXT NOT NULL,
                    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS players (
                    id                      SERIAL PRIMARY KEY,
                    rollup_id               INTEGER NOT NULL REFERENCES rollups(id),
                    name                    TEXT NOT NULL,
                    handicap                INTEGER NOT NULL DEFAULT 0,
                    whs_index               NUMERIC(4,1),
                    whs_index_next_round    NUMERIC(4,1),
                    winner_prohibited       BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (rollup_id, name)
                )
            """)

            # Safe upgrade — add WHS columns if not present
            cur.execute("""
                ALTER TABLE players
                    ADD COLUMN IF NOT EXISTS whs_index            NUMERIC(4,1),
                    ADD COLUMN IF NOT EXISTS whs_index_next_round NUMERIC(4,1),
                    ADD COLUMN IF NOT EXISTS winner_prohibited     BOOLEAN NOT NULL DEFAULT FALSE
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS rounds (
                    id              SERIAL PRIMARY KEY,
                    player_id       INTEGER NOT NULL REFERENCES players(id) ON DELETE CASCADE,
                    rollup_id       INTEGER NOT NULL REFERENCES rollups(id),
                    date            DATE NOT NULL,
                    score           INTEGER NOT NULL,
                    new_handicap    INTEGER NOT NULL,
                    whs_mode        BOOLEAN NOT NULL DEFAULT FALSE,
                    whs_index_used  NUMERIC(4,1),
                    new_whs_index   NUMERIC(4,1),
                    course_id       INTEGER REFERENCES courses(id),
                    tee_id          INTEGER REFERENCES tees(id),
                    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)

            cur.execute("""
                ALTER TABLE courses
                    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            """)
            # Remove duplicate courses (keep lowest id)
            cur.execute("""
                DELETE FROM courses WHERE id NOT IN (
                    SELECT MIN(id) FROM courses GROUP BY name, club
                )
            """)
            try:
                cur.execute("""
                    ALTER TABLE courses ADD CONSTRAINT courses_name_club_unique UNIQUE (name, club)
                """)
            except Exception:
                pass  # Constraint already exists

            cur.execute("""
                ALTER TABLE rounds
                    ADD COLUMN IF NOT EXISTS whs_mode       BOOLEAN NOT NULL DEFAULT FALSE,
                    ADD COLUMN IF NOT EXISTS whs_index_used NUMERIC(4,1),
                    ADD COLUMN IF NOT EXISTS new_whs_index  NUMERIC(4,1),
                    ADD COLUMN IF NOT EXISTS course_id      INTEGER REFERENCES courses(id),
                    ADD COLUMN IF NOT EXISTS tee_id         INTEGER REFERENCES tees(id)
            """)

            # ── Course / Tee tables ──────────────────────────────────────

            cur.execute("""
                CREATE TABLE IF NOT EXISTS courses (
                    id          SERIAL PRIMARY KEY,
                    name        TEXT NOT NULL,
                    club        TEXT NOT NULL,
                    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (name, club)
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS tees (
                    id              SERIAL PRIMARY KEY,
                    course_id       INTEGER NOT NULL REFERENCES courses(id),
                    name            TEXT NOT NULL,
                    gender          TEXT NOT NULL,
                    colour          TEXT,
                    yardage         INTEGER,
                    par             INTEGER NOT NULL,
                    course_rating   NUMERIC(4,1) NOT NULL,
                    slope           INTEGER NOT NULL,
                    UNIQUE (course_id, name, gender)
                )
            """)

            # Seed Bramley Golf Club
            cur.execute("""
                INSERT INTO courses (id, name, club)
                VALUES (1, 'Bramley', 'Bramley Golf Club')
                ON CONFLICT DO NOTHING
            """)

            tees = [
                (1, 'Purple', 'Men',   '#6A0DAD', None, 69, 69.4, 123),
                (1, 'White',  'Men',   '#FFFFFF', 5930, 69, 69.3, 122),
                (1, 'White',  'Women', '#FFFFFF', 5930, 69, 74.4, 133),
                (1, 'Yellow', 'Men',   '#FFD700', 5562, 69, 67.6, 118),
                (1, 'Yellow', 'Women', '#FFD700', 5562, 69, 72.3, 125),
                (1, 'Red',    'Men',   '#CC0000', 5281, 69, 66.1, 106),
                (1, 'Red',    'Women', '#CC0000', 5281, 69, 71.2, 126),
            ]
            for t in tees:
                cur.execute("""
                    INSERT INTO tees (course_id, name, gender, colour, yardage, par, course_rating, slope)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (course_id, name, gender) DO NOTHING
                """, t)

            # Seed Clandon Regis Golf Club
            cur.execute("""
                INSERT INTO courses (id, name, club)
                VALUES (2, 'Clandon Regis', 'Clandon Regis Golf Club')
                ON CONFLICT DO NOTHING
            """)

            # Reset sequence to avoid conflicts with explicitly seeded IDs
            cur.execute("SELECT setval('courses_id_seq', (SELECT MAX(id) FROM courses))")

            clandon_tees = [
                (2, 'White',  'Men',   '#FFFFFF', 6464, 72, 71.9, 135),
                (2, 'Yellow', 'Men',   '#FFD700', 5925, 72, 68.9, 128),
                (2, 'Red',    'Women', '#CC0000', 5575, 72, 73.0, 134),
            ]
            for t in clandon_tees:
                cur.execute("""
                    INSERT INTO tees (course_id, name, gender, colour, yardage, par, course_rating, slope)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (course_id, name, gender) DO NOTHING
                """, t)

            # ── Settings table ───────────────────────────────────────────

            cur.execute("""
                CREATE TABLE IF NOT EXISTS rollup_settings (
                    id                          SERIAL PRIMARY KEY,
                    rollup_id                   INTEGER NOT NULL UNIQUE REFERENCES rollups(id),
                    display_name                TEXT NOT NULL DEFAULT '',
                    ig_search_term              TEXT NOT NULL DEFAULT '',
                    run_days                    TEXT NOT NULL DEFAULT '[]',
                    tee_interval_minutes        INTEGER NOT NULL DEFAULT 8,
                    scoring_mode                TEXT NOT NULL DEFAULT 'stableford',
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
                    whs_pct_1st                 NUMERIC(5,2) NOT NULL DEFAULT 0,
                    whs_pct_2nd                 NUMERIC(5,2) NOT NULL DEFAULT 0,
                    whs_pct_3rd                 NUMERIC(5,2) NOT NULL DEFAULT 0,
                    whs_winner_prohibition      BOOLEAN NOT NULL DEFAULT FALSE,
                    course_id                   INTEGER REFERENCES courses(id),
                    tee_id                      INTEGER REFERENCES tees(id),
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

            # Safe upgrade
            cur.execute("""
                ALTER TABLE rollup_settings
                    ADD COLUMN IF NOT EXISTS scoring_mode           TEXT NOT NULL DEFAULT 'stableford',
                    ADD COLUMN IF NOT EXISTS whs_pct_1st            NUMERIC(5,2) NOT NULL DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS whs_pct_2nd            NUMERIC(5,2) NOT NULL DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS whs_pct_3rd            NUMERIC(5,2) NOT NULL DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS whs_winner_prohibition BOOLEAN NOT NULL DEFAULT FALSE,
                    ADD COLUMN IF NOT EXISTS course_id              INTEGER REFERENCES courses(id),
                    ADD COLUMN IF NOT EXISTS tee_id                 INTEGER REFERENCES tees(id)
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

            # ── Indexes ──────────────────────────────────────────────────
            cur.execute("CREATE INDEX IF NOT EXISTS players_rollup_idx ON players(rollup_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS rounds_rollup_idx ON rounds(rollup_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS rounds_date_idx ON rounds(date DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS rounds_rollup_date_idx ON rounds(rollup_id, date DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS rounds_player_date_idx ON rounds(player_id, date DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS tees_course_idx ON tees(course_id)")


async def init_db():
    await _run(_init_schema)


async def close_db():
    pass  # No pool to close


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
            cur.execute("""
                SELECT id, name, handicap, whs_index, whs_index_next_round, winner_prohibited
                FROM players WHERE rollup_id = %s ORDER BY name
            """, (rollup_id,))
            rows = []
            for row in cur.fetchall():
                d = dict(row)
                if d["whs_index"] is not None:
                    d["whs_index"] = float(d["whs_index"])
                if d["whs_index_next_round"] is not None:
                    d["whs_index_next_round"] = float(d["whs_index_next_round"])
                rows.append(d)
            return rows


async def get_all_players(rollup_id: int) -> list[dict]:
    return await _run(_get_all_players, rollup_id)


def _get_all_players_detail(rollup_id: int):
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT p.id, p.name, p.handicap,
                       p.whs_index, p.whs_index_next_round, p.winner_prohibited,
                       COUNT(r.id) AS round_count
                FROM players p
                LEFT JOIN rounds r ON r.player_id = p.id
                WHERE p.rollup_id = %s
                GROUP BY p.id, p.name, p.handicap,
                         p.whs_index, p.whs_index_next_round, p.winner_prohibited
                ORDER BY p.name
            """, (rollup_id,))
            rows = []
            for row in cur.fetchall():
                d = dict(row)
                if d["whs_index"] is not None:
                    d["whs_index"] = float(d["whs_index"])
                if d["whs_index_next_round"] is not None:
                    d["whs_index_next_round"] = float(d["whs_index_next_round"])
                rows.append(d)
            return rows


async def get_all_players_detail(rollup_id: int) -> list[dict]:
    return await _run(_get_all_players_detail, rollup_id)


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


def _update_player_handicap(player_id: int, handicap: int):
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE players SET handicap = %s WHERE id = %s",
                (handicap, player_id)
            )


async def update_player_handicap(player_id: int, handicap: int) -> None:
    await _run(_update_player_handicap, player_id, handicap)


def _update_player_whs_index(player_id: int, whs_index: float):
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE players SET whs_index = %s WHERE id = %s",
                (whs_index, player_id)
            )


async def update_player_whs_index(player_id: int, whs_index: float) -> None:
    await _run(_update_player_whs_index, player_id, whs_index)


def _remove_player(player_id: int, rollup_id: int):
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM players WHERE id = %s AND rollup_id = %s",
                (player_id, rollup_id)
            )


async def remove_player(player_id: int, rollup_id: int) -> None:
    await _run(_remove_player, player_id, rollup_id)


# ---------------------------------------------------------------------------
# Rounds
# ---------------------------------------------------------------------------

def _save_round_results(results: list[dict], date_str: str, rollup_id: int,
                        whs_mode: bool = False, course_id=None, tee_id=None):
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

                if whs_mode:
                    new_whs = r.get("new_whs_index")
                    cur.execute("""
                        UPDATE players SET
                            whs_index_next_round = %s,
                            winner_prohibited    = %s
                        WHERE id = %s
                    """, (new_whs, r.get("winner_prohibited", False), player_id))
                    cur.execute("""
                        INSERT INTO rounds (player_id, rollup_id, date, score,
                            new_handicap, whs_mode, whs_index_used, new_whs_index,
                            course_id, tee_id)
                        VALUES (%s, %s, %s, %s, %s, TRUE, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (player_id, rollup_id, date_str, r["score"],
                          r["new_handicap"], r.get("whs_index_used"), new_whs,
                          course_id, tee_id))
                else:
                    cur.execute(
                        "UPDATE players SET handicap = %s WHERE id = %s",
                        (r["new_handicap"], player_id)
                    )
                    cur.execute("""
                        INSERT INTO rounds (player_id, rollup_id, date, score, new_handicap,
                            course_id, tee_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (player_id, rollup_id, date_str, r["score"], r["new_handicap"],
                           course_id, tee_id))


async def save_round_results(results: list[dict], date_str: str, rollup_id: int,
                              whs_mode: bool = False, course_id=None, tee_id=None) -> None:
    await _run(_save_round_results, results, date_str, rollup_id, whs_mode, course_id, tee_id)


def _get_prohibited_winners(rollup_id: int) -> list[str]:
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name FROM players
                WHERE rollup_id = %s AND winner_prohibited = TRUE
            """, (rollup_id,))
            return [row[0] for row in cur.fetchall()]


async def get_prohibited_winners(rollup_id: int) -> list[str]:
    return await _run(_get_prohibited_winners, rollup_id)


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
                SELECT r.date, r.score, r.new_handicap,
                       r.whs_mode, r.whs_index_used, r.new_whs_index
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
                SELECT p.name, r.score, r.new_handicap,
                       r.whs_mode, r.whs_index_used, r.new_whs_index
                FROM rounds r
                JOIN players p ON p.id = r.player_id
                WHERE r.rollup_id = %s AND r.date = %s
                ORDER BY r.score DESC
            """, (rollup_id, date_str))
            return [dict(r) for r in cur.fetchall()]


async def get_round_by_date(date_str: str, rollup_id: int) -> list[dict]:
    return await _run(_get_round_by_date, date_str, rollup_id)


# ---------------------------------------------------------------------------
# Courses and Tees
# ---------------------------------------------------------------------------

def _get_all_courses():
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, name, club FROM courses ORDER BY name")
            return [dict(r) for r in cur.fetchall()]


async def get_all_courses() -> list[dict]:
    return await _run(_get_all_courses)


def _save_course(name: str, club: str, tees: list[dict]) -> int:
    """Insert a new course and its tees, return the course_id."""
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Insert course and always get the id back
            cur.execute("""
                INSERT INTO courses (name, club)
                VALUES (%s, %s)
                ON CONFLICT (name, club) DO UPDATE SET name = EXCLUDED.name
                RETURNING id
            """, (name, club))
            course_id = cur.fetchone()["id"]

            for t in tees:
                cur.execute("""
                    INSERT INTO tees (course_id, name, gender, colour, yardage, par, course_rating, slope)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (course_id, name, gender) DO UPDATE SET
                        yardage       = EXCLUDED.yardage,
                        par           = EXCLUDED.par,
                        course_rating = EXCLUDED.course_rating,
                        slope         = EXCLUDED.slope,
                        colour        = EXCLUDED.colour
                """, (
                    course_id,
                    t["name"], t["gender"], t.get("colour", "#888888"),
                    t.get("yardage"), t.get("par", 72),
                    t["course_rating"], t["slope"],
                ))
            conn.commit()
            return course_id


async def save_course(name: str, club: str, tees: list[dict]) -> int:
    return await _run(_save_course, name, club, tees)


def _get_tees_for_course(course_id: int):
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, name, gender, colour, yardage, par, course_rating, slope
                FROM tees WHERE course_id = %s
                ORDER BY course_rating DESC
            """, (course_id,))
            rows = []
            for row in cur.fetchall():
                d = dict(row)
                d["course_rating"] = float(d["course_rating"])
                rows.append(d)
            return rows


async def get_tees_for_course(course_id: int) -> list[dict]:
    return await _run(_get_tees_for_course, course_id)


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
            cur.execute("""
                SELECT rs.*, r.name AS rollup_name, r.ig_search_term AS rollup_term,
                       t.course_rating AS tee_course_rating,
                       t.slope         AS tee_slope,
                       t.par           AS tee_par
                FROM rollup_settings rs
                JOIN rollups r ON r.id = rs.rollup_id
                LEFT JOIN tees t ON t.id = rs.tee_id
                WHERE rs.rollup_id = %s
            """, (rollup_id,))
            row = cur.fetchone()
            if not row:
                cur.execute(
                    "SELECT name, ig_search_term FROM rollups WHERE id = %s",
                    (rollup_id,)
                )
                rollup = cur.fetchone()
                return {
                    "rollup_id":                rollup_id,
                    "display_name":             rollup["name"] if rollup else "",
                    "ig_search_term":           rollup["ig_search_term"] if rollup else "",
                    "run_days":                 [],
                    "tee_interval_minutes":     8,
                    "scoring_mode":             "stableford",
                    "adjustment_table":         DEFAULT_ADJUSTMENT_TABLE,
                    "winner_bonus_enabled":     True,
                    "winner_gap_penalty1":      0,
                    "winner_gap_penalty2":      0,
                    "whs_pct_1st":              0.0,
                    "whs_pct_2nd":              0.0,
                    "whs_pct_3rd":              0.0,
                    "whs_winner_prohibition":   False,
                    "course_id":                None,
                    "tee_id":                   None,
                    "tee_course_rating":        None,
                    "tee_slope":                None,
                    "tee_par":                  None,
                    "entry_fee":                0.00,
                    "prize_places":             3,
                    "prize_pct_1st":            60,
                    "prize_pct_2nd":            30,
                    "prize_pct_3rd":            10,
                    "prize_pct_4th":            0,
                    "tie_handling":             "tournament",
                    "preferred_team_size":      4,
                    "team_scoring_method":      "best2",
                }
            d = dict(row)
            # Always use rollups table as source of truth for name/term
            d["display_name"]     = d["rollup_name"]
            d["ig_search_term"]   = d["rollup_term"]
            d["run_days"]         = json.loads(d["run_days"])
            d["adjustment_table"] = json.loads(d["adjustment_table"])
            d["entry_fee"]        = float(d["entry_fee"])
            d["whs_pct_1st"]      = float(d["whs_pct_1st"])
            d["whs_pct_2nd"]      = float(d["whs_pct_2nd"])
            d["whs_pct_3rd"]      = float(d["whs_pct_3rd"])
            if d["tee_course_rating"] is not None:
                d["tee_course_rating"] = float(d["tee_course_rating"])
            return d


async def get_rollup_settings(rollup_id: int) -> dict:
    return await _run(_get_rollup_settings, rollup_id)


def _save_rollup_settings(rollup_id: int, s: dict):
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO rollup_settings (
                    rollup_id, display_name, ig_search_term, run_days,
                    tee_interval_minutes, scoring_mode, adjustment_table,
                    winner_bonus_enabled, winner_gap_penalty1, winner_gap_penalty2,
                    whs_pct_1st, whs_pct_2nd, whs_pct_3rd, whs_winner_prohibition,
                    course_id, tee_id,
                    entry_fee, prize_places,
                    prize_pct_1st, prize_pct_2nd, prize_pct_3rd, prize_pct_4th,
                    tie_handling, preferred_team_size, team_scoring_method,
                    updated_at
                ) VALUES (
                    %s,%s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s,%s, %s,%s,
                    %s,%s, %s,%s,%s,%s, %s,%s,%s, NOW()
                )
                ON CONFLICT (rollup_id) DO UPDATE SET
                    display_name            = EXCLUDED.display_name,
                    ig_search_term          = EXCLUDED.ig_search_term,
                    run_days                = EXCLUDED.run_days,
                    tee_interval_minutes    = EXCLUDED.tee_interval_minutes,
                    scoring_mode            = EXCLUDED.scoring_mode,
                    adjustment_table        = EXCLUDED.adjustment_table,
                    winner_bonus_enabled    = EXCLUDED.winner_bonus_enabled,
                    winner_gap_penalty1     = EXCLUDED.winner_gap_penalty1,
                    winner_gap_penalty2     = EXCLUDED.winner_gap_penalty2,
                    whs_pct_1st             = EXCLUDED.whs_pct_1st,
                    whs_pct_2nd             = EXCLUDED.whs_pct_2nd,
                    whs_pct_3rd             = EXCLUDED.whs_pct_3rd,
                    whs_winner_prohibition  = EXCLUDED.whs_winner_prohibition,
                    course_id               = EXCLUDED.course_id,
                    tee_id                  = EXCLUDED.tee_id,
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
                s["display_name"], s["ig_search_term"],
                json.dumps(s["run_days"]),
                s["tee_interval_minutes"],
                s.get("scoring_mode", "stableford"),
                json.dumps(s["adjustment_table"]),
                s["winner_bonus_enabled"],
                s["winner_gap_penalty1"], s["winner_gap_penalty2"],
                s.get("whs_pct_1st", 0), s.get("whs_pct_2nd", 0), s.get("whs_pct_3rd", 0),
                s.get("whs_winner_prohibition", False),
                s.get("course_id"), s.get("tee_id"),
                s["entry_fee"], s["prize_places"],
                s["prize_pct_1st"], s["prize_pct_2nd"],
                s["prize_pct_3rd"], s["prize_pct_4th"],
                s["tie_handling"], s["preferred_team_size"], s["team_scoring_method"],
            ))
            cur.execute("""
                UPDATE rollups SET name = %s, ig_search_term = %s WHERE id = %s
            """, (s["display_name"], s["ig_search_term"], rollup_id))


async def save_rollup_settings(rollup_id: int, settings: dict) -> None:
    await _run(_save_rollup_settings, rollup_id, settings)


# ---------------------------------------------------------------------------
# App credentials (legacy singleton)
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
