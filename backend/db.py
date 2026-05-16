# Bramley Rollup — backend/db.py
# Per-request connections: opens a fresh connection for every DB operation,
# closes it immediately after. Eliminates stale connection errors from Neon
# dropping idle connections after ~5 minutes.
#
# Multi-tenant architecture (Option A — row-level tenant_id on rollups).
# Tenants are identified by their URL slug; no tenant-level password is
# required (the URL is the access key). Admin auth is handled separately
# in main.py via HTTP Basic Auth against env vars.
#
# To migrate to Option B (schema-per-tenant) later: swap _get_conn() to
# SET search_path = <slug> and remove WHERE tenant_id = %s clauses —
# main.py and the frontend stay unchanged.

import json
import os
import asyncio
import hashlib
import binascii
import secrets
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
# Password helpers  (used for admin credentials only)
# ---------------------------------------------------------------------------

def _hash_password(password: str) -> str:
    salt = os.urandom(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 200_000)
    return binascii.hexlify(salt).decode() + ":" + binascii.hexlify(dk).decode()


def _verify_password(stored_hash: str, password: str) -> bool:
    try:
        salt_hex, dk_hex = stored_hash.split(":")
        salt = binascii.unhexlify(salt_hex)
        dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 200_000)
        return binascii.hexlify(dk).decode() == dk_hex
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Schema init
# ---------------------------------------------------------------------------

def _init_schema():
    with _get_conn() as conn:
        with conn.cursor() as cur:

            # ── Tenants ──────────────────────────────────────────────────
            # One row per club/society. Identified by slug (e.g. "bramley").
            # The slug is embedded in the URL members use to access the app —
            # no tenant-level password is stored or required.

            cur.execute("""
                CREATE TABLE IF NOT EXISTS tenants (
                    id         SERIAL PRIMARY KEY,
                    name       TEXT NOT NULL,
                    slug       TEXT NOT NULL UNIQUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            # ig_tenant: TRUE = members log in with IG credentials (used for scraping)
            #            FALSE = members use custom username/password
            cur.execute("""
                ALTER TABLE tenants
                    ADD COLUMN IF NOT EXISTS ig_tenant BOOLEAN NOT NULL DEFAULT TRUE
            """)

            # One row per club member. IG users: ig_pin stored plaintext (required for
            # screen-scraping IG on their behalf). Non-IG users: password_hash only.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id            SERIAL PRIMARY KEY,
                    tenant_id     INTEGER NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
                    username      TEXT NOT NULL,
                    password_hash TEXT,
                    ig_pin        TEXT,
                    display_name  TEXT,
                    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE(tenant_id, username)
                )
            """)

            # Per-tenant IG credentials (replaces the old app_credentials singleton)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tenant_credentials (
                    tenant_id   INTEGER PRIMARY KEY REFERENCES tenants(id),
                    ig_username TEXT NOT NULL DEFAULT '',
                    ig_pin      TEXT NOT NULL DEFAULT '',
                    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)

            # ── Core tables ──────────────────────────────────────────────

            cur.execute("""
                CREATE TABLE IF NOT EXISTS rollups (
                    id              SERIAL PRIMARY KEY,
                    name            TEXT UNIQUE NOT NULL,
                    ig_search_term  TEXT NOT NULL,
                    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)

            # Add tenant_id to rollups (safe upgrade)
            cur.execute("""
                ALTER TABLE rollups
                    ADD COLUMN IF NOT EXISTS tenant_id INTEGER REFERENCES tenants(id)
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

            cur.execute("""
                ALTER TABLE players
                    ADD COLUMN IF NOT EXISTS whs_index            NUMERIC(4,1),
                    ADD COLUMN IF NOT EXISTS whs_index_next_round NUMERIC(4,1),
                    ADD COLUMN IF NOT EXISTS winner_prohibited     BOOLEAN NOT NULL DEFAULT FALSE,
                    ADD COLUMN IF NOT EXISTS winner_ban_entries    INT NOT NULL DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS winner_ban_original_hc INT,
                    ADD COLUMN IF NOT EXISTS total_prize_won      NUMERIC(8,2) NOT NULL DEFAULT 0
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
            cur.execute("""
                DELETE FROM tees WHERE course_id NOT IN (
                    SELECT MIN(id) FROM courses GROUP BY name, club
                )
            """)
            cur.execute("""
                DELETE FROM courses WHERE id NOT IN (
                    SELECT MIN(id) FROM courses GROUP BY name, club
                )
            """)
            cur.execute("SAVEPOINT before_unique_constraint")
            try:
                cur.execute("""
                    ALTER TABLE courses ADD CONSTRAINT courses_name_club_unique UNIQUE (name, club)
                """)
                cur.execute("RELEASE SAVEPOINT before_unique_constraint")
            except Exception:
                cur.execute("ROLLBACK TO SAVEPOINT before_unique_constraint")

            cur.execute("""
                ALTER TABLE rounds
                    ADD COLUMN IF NOT EXISTS whs_mode       BOOLEAN NOT NULL DEFAULT FALSE,
                    ADD COLUMN IF NOT EXISTS whs_index_used NUMERIC(4,1),
                    ADD COLUMN IF NOT EXISTS new_whs_index  NUMERIC(4,1),
                    ADD COLUMN IF NOT EXISTS course_id      INTEGER REFERENCES courses(id),
                    ADD COLUMN IF NOT EXISTS tee_id         INTEGER REFERENCES tees(id),
                    ADD COLUMN IF NOT EXISTS playing_hc     INTEGER
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

            cur.execute("""
                INSERT INTO courses (id, name, club)
                VALUES (2, 'Clandon Regis', 'Clandon Regis Golf Club')
                ON CONFLICT DO NOTHING
            """)

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

            cur.execute("""
                ALTER TABLE rollup_settings
                    ADD COLUMN IF NOT EXISTS scoring_mode             TEXT NOT NULL DEFAULT 'stableford',
                    ADD COLUMN IF NOT EXISTS whs_pct_1st              NUMERIC(5,2) NOT NULL DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS whs_pct_2nd              NUMERIC(5,2) NOT NULL DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS whs_pct_3rd              NUMERIC(5,2) NOT NULL DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS whs_winner_prohibition   BOOLEAN NOT NULL DEFAULT FALSE,
                    ADD COLUMN IF NOT EXISTS winner_reduction_enabled BOOLEAN NOT NULL DEFAULT FALSE,
                    ADD COLUMN IF NOT EXISTS winner_reduction_pct    INTEGER NOT NULL DEFAULT 25,
                    ADD COLUMN IF NOT EXISTS winner_ban_rounds        INTEGER NOT NULL DEFAULT 3,
                    ADD COLUMN IF NOT EXISTS course_id                INTEGER REFERENCES courses(id),
                    ADD COLUMN IF NOT EXISTS tee_id                   INTEGER REFERENCES tees(id)
            """)

            cur.execute("""
                ALTER TABLE rollup_settings ADD COLUMN IF NOT EXISTS logo_data TEXT
            """)
            cur.execute("""
                ALTER TABLE rollup_settings
                    ADD COLUMN IF NOT EXISTS competition_format TEXT NOT NULL DEFAULT 'stableford',
                    ADD COLUMN IF NOT EXISTS medal_adjustment_table TEXT NOT NULL DEFAULT '[
                        {"max_score": 68, "adjustment": -2},
                        {"max_score": 71, "adjustment": -1},
                        {"max_score": 74, "adjustment": 0},
                        {"max_score": 80, "adjustment": 1},
                        {"max_score": null, "adjustment": 2}
                    ]'
            """)

            # Legacy singleton credentials — kept for backward compatibility
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

            # ── Default tenant seed ──────────────────────────────────────
            # Creates the first tenant from env vars on a fresh deployment.
            # Existing rollups with tenant_id IS NULL are assigned to it.

            cur.execute("SELECT COUNT(*) FROM tenants")
            if cur.fetchone()[0] == 0:
                default_name = os.getenv("DEFAULT_TENANT_NAME", "Bramley Golf Club")
                default_slug = os.getenv("DEFAULT_TENANT_SLUG", "bramley")
                cur.execute("""
                    INSERT INTO tenants (name, slug)
                    VALUES (%s, %s)
                    ON CONFLICT (slug) DO NOTHING
                    RETURNING id
                """, (default_name, default_slug))
                row = cur.fetchone()
                if row:
                    new_id = row[0]
                    cur.execute(
                        "UPDATE rollups SET tenant_id = %s WHERE tenant_id IS NULL",
                        (new_id,)
                    )
                    cur.execute("""
                        INSERT INTO tenant_credentials (tenant_id, ig_username, ig_pin)
                        SELECT %s, ig_username, ig_pin FROM app_credentials WHERE id = 1
                        ON CONFLICT (tenant_id) DO NOTHING
                    """, (new_id,))
                    print(f"[db] Default tenant '{default_slug}' created (id={new_id}).")

            # Assign any rollups still missing a tenant to the first tenant
            cur.execute("""
                UPDATE rollups SET tenant_id = (SELECT MIN(id) FROM tenants)
                WHERE tenant_id IS NULL
                  AND (SELECT COUNT(*) FROM tenants) > 0
            """)

            # ── Indexes ──────────────────────────────────────────────────
            cur.execute("CREATE INDEX IF NOT EXISTS tenants_slug_idx ON tenants(slug)")
            cur.execute("CREATE INDEX IF NOT EXISTS users_tenant_idx ON users(tenant_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS rollups_tenant_idx ON rollups(tenant_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS players_rollup_idx ON players(rollup_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS rounds_rollup_idx ON rounds(rollup_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS rounds_date_idx ON rounds(date DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS rounds_rollup_date_idx ON rounds(rollup_id, date DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS rounds_player_date_idx ON rounds(player_id, date DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS tees_course_idx ON tees(course_id)")


async def init_db():
    await _run(_init_schema)


async def close_db():
    pass


# ---------------------------------------------------------------------------
# Tenants
# ---------------------------------------------------------------------------

def _get_tenant_by_slug(slug: str) -> dict | None:
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, name, slug, ig_tenant FROM tenants WHERE slug = %s",
                (slug.lower(),)
            )
            row = cur.fetchone()
            return dict(row) if row else None


async def get_tenant_by_slug(slug: str) -> dict | None:
    return await _run(_get_tenant_by_slug, slug)


def _get_all_tenants() -> list[dict]:
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, name, slug, ig_tenant, created_at FROM tenants ORDER BY name")
            return [dict(r) for r in cur.fetchall()]


async def get_all_tenants() -> list[dict]:
    return await _run(_get_all_tenants)


def _create_tenant(name: str, slug: str, ig_tenant: bool = True) -> int:
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO tenants (name, slug, ig_tenant)
                VALUES (%s, %s, %s)
                RETURNING id
            """, (name, slug.lower(), ig_tenant))
            return cur.fetchone()[0]


async def create_tenant(name: str, slug: str, ig_tenant: bool = True) -> int:
    return await _run(_create_tenant, name, slug, ig_tenant)


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------

def _create_user(tenant_id: int, username: str,
                 password_hash: str | None = None,
                 ig_pin: str | None = None,
                 display_name: str | None = None) -> int:
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO users (tenant_id, username, password_hash, ig_pin, display_name)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """, (tenant_id, username, password_hash, ig_pin, display_name))
            return cur.fetchone()[0]


async def create_user(tenant_id: int, username: str,
                      password_hash: str | None = None,
                      ig_pin: str | None = None,
                      display_name: str | None = None) -> int:
    return await _run(_create_user, tenant_id, username, password_hash, ig_pin, display_name)


def _get_user_by_username(tenant_id: int, username: str) -> dict | None:
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, tenant_id, username, password_hash, ig_pin, display_name "
                "FROM users WHERE tenant_id = %s AND lower(username) = lower(%s)",
                (tenant_id, username)
            )
            row = cur.fetchone()
            return dict(row) if row else None


async def get_user_by_username(tenant_id: int, username: str) -> dict | None:
    return await _run(_get_user_by_username, tenant_id, username)


def _authenticate_user(tenant_id: int, username: str, credential: str) -> dict | None:
    """Returns user dict if credentials valid, None otherwise.
    IG users: credential is the IG PIN (plaintext comparison).
    Non-IG users: credential is password (PBKDF2 hash verification)."""
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, tenant_id, username, password_hash, ig_pin, display_name "
                "FROM users WHERE tenant_id = %s AND lower(username) = lower(%s)",
                (tenant_id, username)
            )
            row = cur.fetchone()
            if not row:
                return None
            user = dict(row)
            if user["ig_pin"] is not None:
                # IG user — compare PIN directly
                return user if user["ig_pin"] == credential else None
            if user["password_hash"] is not None:
                return user if _verify_password(user["password_hash"], credential) else None
            return None


async def authenticate_user(tenant_id: int, username: str, credential: str) -> dict | None:
    return await _run(_authenticate_user, tenant_id, username, credential)


def _validate_rollup_tenant(rollup_id: int, tenant_id: int) -> bool:
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM rollups WHERE id = %s AND tenant_id = %s",
                (rollup_id, tenant_id)
            )
            return cur.fetchone() is not None


async def validate_rollup_tenant(rollup_id: int, tenant_id: int) -> bool:
    return await _run(_validate_rollup_tenant, rollup_id, tenant_id)


# ---------------------------------------------------------------------------
# Rollups (tenant-scoped)
# ---------------------------------------------------------------------------

def _get_all_rollups(tenant_id: int):
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, name, ig_search_term FROM rollups WHERE tenant_id = %s ORDER BY name",
                (tenant_id,)
            )
            return [dict(r) for r in cur.fetchall()]


async def get_all_rollups(tenant_id: int) -> list[dict]:
    return await _run(_get_all_rollups, tenant_id)


def _get_or_create_rollup(tenant_id: int, name: str, ig_search_term: str) -> int:
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE rollups SET ig_search_term = %s
                WHERE name = %s AND tenant_id = %s
                RETURNING id
            """, (ig_search_term, name, tenant_id))
            row = cur.fetchone()
            if row:
                return row[0]
            cur.execute("""
                INSERT INTO rollups (name, ig_search_term, tenant_id)
                VALUES (%s, %s, %s)
                RETURNING id
            """, (name, ig_search_term, tenant_id))
            return cur.fetchone()[0]


async def get_or_create_rollup(tenant_id: int, name: str, ig_search_term: str) -> int:
    return await _run(_get_or_create_rollup, tenant_id, name, ig_search_term)


# Admin: all rollups across all tenants
def _get_all_rollups_admin() -> list[dict]:
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT r.id, r.name, r.ig_search_term, r.tenant_id,
                       t.name AS tenant_name, t.slug AS tenant_slug
                FROM rollups r
                JOIN tenants t ON t.id = r.tenant_id
                ORDER BY t.name, r.name
            """)
            return [dict(r) for r in cur.fetchall()]


async def get_all_rollups_admin() -> list[dict]:
    return await _run(_get_all_rollups_admin)


# ---------------------------------------------------------------------------
# Players
# ---------------------------------------------------------------------------

def _get_all_players(rollup_id: int):
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, name, handicap, whs_index, whs_index_next_round,
                       winner_prohibited, winner_ban_entries, winner_ban_original_hc,
                       total_prize_won
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
                       p.winner_ban_entries, p.winner_ban_original_hc,
                       p.total_prize_won,
                       COUNT(r.id) AS round_count
                FROM players p
                LEFT JOIN rounds r ON r.player_id = p.id
                WHERE p.rollup_id = %s
                GROUP BY p.id, p.name, p.handicap,
                         p.whs_index, p.whs_index_next_round, p.winner_prohibited,
                         p.winner_ban_entries, p.winner_ban_original_hc, p.total_prize_won
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


def _credit_prize_money(rollup_id: int, prize_map: dict):
    """Add prize amounts to total_prize_won for named players in this rollup."""
    with _get_conn() as conn:
        with conn.cursor() as cur:
            for name, amount in prize_map.items():
                if amount <= 0:
                    continue
                cur.execute("""
                    UPDATE players
                    SET total_prize_won = total_prize_won + %s
                    WHERE rollup_id = %s AND name = %s
                """, (amount, rollup_id, name))


async def credit_prize_money(rollup_id: int, prize_map: dict) -> None:
    await _run(_credit_prize_money, rollup_id, prize_map)


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


def _bulk_import_players(rollup_id: int, rows: list[dict]) -> dict:
    """
    Upsert a list of {name, handicap, whs_index?} rows.
    Returns {added, updated} counts.
    """
    added = updated = 0
    with _get_conn() as conn:
        with conn.cursor() as cur:
            for row in rows:
                name     = row["name"].strip()
                handicap = int(row["handicap"])
                whs      = row.get("whs_index")
                cur.execute(
                    "SELECT id FROM players WHERE rollup_id = %s AND name = %s",
                    (rollup_id, name)
                )
                existing = cur.fetchone()
                if existing:
                    if whs is not None:
                        cur.execute(
                            "UPDATE players SET handicap = %s, whs_index = %s WHERE id = %s",
                            (handicap, whs, existing[0])
                        )
                    else:
                        cur.execute(
                            "UPDATE players SET handicap = %s WHERE id = %s",
                            (handicap, existing[0])
                        )
                    updated += 1
                else:
                    cur.execute(
                        "INSERT INTO players (rollup_id, name, handicap, whs_index) VALUES (%s, %s, %s, %s)",
                        (rollup_id, name, handicap, whs)
                    )
                    added += 1
    return {"added": added, "updated": updated}


async def bulk_import_players(rollup_id: int, rows: list[dict]) -> dict:
    return await _run(_bulk_import_players, rollup_id, rows)


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
                    playing_hc = r.get("playing_hc") if r.get("playing_hc") is not None else r.get("handicap")
                    cur.execute("""
                        INSERT INTO rounds (player_id, rollup_id, date, score, new_handicap,
                            playing_hc, course_id, tee_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (player_id, rollup_id, date_str, r["score"], r["new_handicap"],
                           playing_hc, course_id, tee_id))


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


def _apply_winner_reduction(rollup_id: int, winner_names: list[str],
                             participant_names: list[str],
                             reduction_pct: int = 25,
                             ban_rounds: int = 3) -> list[dict]:
    """Apply the configurable-cut winner reduction rule after a round is saved.

    Winners: HC reduced by reduction_pct%, ban_entries set/reset to ban_rounds,
    original HC stored for reinstatement.
    Banned non-winners: ban_entries decremented; when it hits 0 original HC
    is reinstated and the ban is cleared.

    Returns a list of dicts describing what changed (for response logging).
    """
    lower_winners      = {n.strip().lower() for n in winner_names}
    lower_participants = {n.strip().lower() for n in participant_names}
    multiplier         = 1 - reduction_pct / 100
    changes = []

    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, name, handicap, winner_ban_entries, winner_ban_original_hc
                FROM players WHERE rollup_id = %s
            """, (rollup_id,))
            players = [dict(r) for r in cur.fetchall()]

        with conn.cursor() as cur:
            for p in players:
                name_lower = p["name"].strip().lower()
                if name_lower not in lower_participants:
                    continue

                ban = p["winner_ban_entries"]
                hc  = p["handicap"]

                if name_lower in lower_winners:
                    new_hc = max(1, round(hc * multiplier))
                    if ban == 0:
                        # First win: store original HC, start ban
                        cur.execute("""
                            UPDATE players SET
                                handicap = %s, winner_ban_entries = %s,
                                winner_ban_original_hc = %s, winner_prohibited = TRUE
                            WHERE id = %s
                        """, (new_hc, ban_rounds, hc, p["id"]))
                        changes.append({"name": p["name"], "event": "winner_cut",
                                        "old_hc": hc, "new_hc": new_hc, "ban_entries": ban_rounds})
                    else:
                        # Won again during ban: cut current HC, reset ban — max 2 total cuts
                        orig_hc = p["winner_ban_original_hc"] or hc
                        floor_hc = max(1, round(orig_hc * multiplier * multiplier))
                        new_hc   = max(new_hc, floor_hc)
                        cur.execute("""
                            UPDATE players SET
                                handicap = %s, winner_ban_entries = %s, winner_prohibited = TRUE
                            WHERE id = %s
                        """, (new_hc, ban_rounds, p["id"]))
                        changes.append({"name": p["name"], "event": "repeat_win_cut",
                                        "old_hc": hc, "new_hc": new_hc, "ban_entries": ban_rounds})
                else:
                    if ban > 0:
                        new_ban = ban - 1
                        if new_ban == 0:
                            # Ban expires: reinstate original HC
                            orig_hc = p["winner_ban_original_hc"] or hc
                            cur.execute("""
                                UPDATE players SET
                                    handicap = %s, winner_ban_entries = 0,
                                    winner_ban_original_hc = NULL, winner_prohibited = FALSE
                                WHERE id = %s
                            """, (orig_hc, p["id"]))
                            changes.append({"name": p["name"], "event": "ban_expired",
                                            "reinstated_hc": orig_hc})
                        else:
                            cur.execute("""
                                UPDATE players SET winner_ban_entries = %s WHERE id = %s
                            """, (new_ban, p["id"]))
                            changes.append({"name": p["name"], "event": "ban_tick",
                                            "ban_entries_remaining": new_ban})

    return changes


async def apply_winner_reduction(rollup_id: int, winner_names: list[str],
                                  participant_names: list[str],
                                  reduction_pct: int = 25,
                                  ban_rounds: int = 3) -> list[dict]:
    return await _run(_apply_winner_reduction, rollup_id, winner_names, participant_names,
                      reduction_pct, ban_rounds)


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
                SELECT p.name, r.score, r.new_handicap, r.playing_hc
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


def _dump_tenant_history(tenant_id: int) -> dict:
    """Return all rounds and players for every rollup under a tenant."""
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, name FROM rollups WHERE tenant_id = %s ORDER BY name",
                (tenant_id,)
            )
            rollups = [dict(r) for r in cur.fetchall()]

            rounds_rows = []
            for rollup in rollups:
                cur.execute("""
                    SELECT ro.date, p.name AS player,
                           %s AS rollup,
                           ro.score, ro.new_handicap,
                           ro.whs_mode, ro.whs_index_used, ro.new_whs_index,
                           ro.recorded_at
                    FROM rounds ro
                    JOIN players p ON p.id = ro.player_id
                    WHERE ro.rollup_id = %s
                    ORDER BY ro.date DESC, p.name
                """, (rollup["name"], rollup["id"]))
                rounds_rows.extend([dict(r) for r in cur.fetchall()])

            cur.execute("""
                SELECT p.name, p.handicap, p.whs_index, p.total_prize_won,
                       rol.name AS rollup
                FROM players p
                JOIN rollups rol ON rol.id = p.rollup_id
                WHERE rol.tenant_id = %s
                ORDER BY rol.name, p.name
            """, (tenant_id,))
            player_rows = [dict(r) for r in cur.fetchall()]

            return {"rounds": rounds_rows, "players": player_rows}


async def dump_tenant_history(tenant_id: int) -> dict:
    return await _run(_dump_tenant_history, tenant_id)


def _delete_tenant_history(tenant_id: int) -> int:
    """Delete all rounds for every rollup under a tenant. Returns count deleted."""
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM rounds
                WHERE rollup_id IN (
                    SELECT id FROM rollups WHERE tenant_id = %s
                )
            """, (tenant_id,))
            return cur.rowcount


async def delete_tenant_history(tenant_id: int) -> int:
    return await _run(_delete_tenant_history, tenant_id)


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
                SELECT p.name, r.score, r.new_handicap, r.playing_hc,
                       r.whs_mode, r.whs_index_used, r.new_whs_index,
                       p.winner_ban_entries, p.winner_prohibited
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
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
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

DEFAULT_MEDAL_ADJUSTMENT_TABLE = [
    {"max_score": 68,   "adjustment": -2},
    {"max_score": 71,   "adjustment": -1},
    {"max_score": 74,   "adjustment": 0},
    {"max_score": 80,   "adjustment": 1},
    {"max_score": None, "adjustment": 2},
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
                    "scoring_mode":             "universal",
                    "adjustment_table":         DEFAULT_ADJUSTMENT_TABLE,
                    "winner_bonus_enabled":     True,
                    "winner_gap_penalty1":      0,
                    "winner_gap_penalty2":      0,
                    "whs_pct_1st":              0.0,
                    "whs_pct_2nd":              0.0,
                    "whs_pct_3rd":              0.0,
                    "whs_winner_prohibition":   False,
                    "winner_reduction_pct":     25,
                    "winner_ban_rounds":        3,
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
                    "logo_data":                None,
                    "competition_format":       "stableford",
                    "medal_adjustment_table":   DEFAULT_MEDAL_ADJUSTMENT_TABLE,
                }
            d = dict(row)
            d["display_name"]     = d["rollup_name"]
            d["ig_search_term"]   = d["rollup_term"]
            d["run_days"]               = json.loads(d["run_days"])
            d["adjustment_table"]       = json.loads(d["adjustment_table"])
            d["medal_adjustment_table"] = json.loads(d.get("medal_adjustment_table") or json.dumps(DEFAULT_MEDAL_ADJUSTMENT_TABLE))
            d["competition_format"]     = d.get("competition_format") or "stableford"
            d["entry_fee"]        = float(d["entry_fee"])
            d["whs_pct_1st"]      = float(d["whs_pct_1st"])
            d["whs_pct_2nd"]      = float(d["whs_pct_2nd"])
            d["whs_pct_3rd"]      = float(d["whs_pct_3rd"])
            if d["tee_course_rating"] is not None:
                d["tee_course_rating"] = float(d["tee_course_rating"])
            # Backward-compat: map old DB values to new three-mode names
            raw_mode = d.get("scoring_mode", "stableford")
            raw_wr   = d.get("winner_reduction_enabled", False)
            if raw_mode == "stableford" and raw_wr:
                d["scoring_mode"] = "winners_only_2"
            elif raw_mode == "stableford":
                d["scoring_mode"] = "universal"
            elif raw_mode == "whs":
                d["scoring_mode"] = "winners_only_1"
            d.pop("winner_reduction_enabled", None)
            return d


async def get_rollup_settings(rollup_id: int) -> dict:
    return await _run(_get_rollup_settings, rollup_id)


def _save_rollup_settings(rollup_id: int, s: dict):
    # Map incoming three-mode names back to legacy DB column values
    incoming_mode = s.get("scoring_mode", "universal")
    if incoming_mode == "universal":
        db_scoring_mode = "stableford"
        db_winner_reduction_enabled = False
    elif incoming_mode == "winners_only_1":
        db_scoring_mode = "whs"
        db_winner_reduction_enabled = False
    elif incoming_mode == "winners_only_2":
        db_scoring_mode = "stableford"
        db_winner_reduction_enabled = True
    else:
        # Legacy pass-through
        if incoming_mode == "stableford":
            db_scoring_mode = "stableford"
            db_winner_reduction_enabled = s.get("winner_reduction_enabled", False)
        elif incoming_mode == "whs":
            db_scoring_mode = "whs"
            db_winner_reduction_enabled = False
        else:
            db_scoring_mode = "stableford"
            db_winner_reduction_enabled = False

    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO rollup_settings (
                    rollup_id, display_name, ig_search_term, run_days,
                    tee_interval_minutes, scoring_mode, adjustment_table,
                    winner_bonus_enabled, winner_gap_penalty1, winner_gap_penalty2,
                    whs_pct_1st, whs_pct_2nd, whs_pct_3rd, whs_winner_prohibition,
                    winner_reduction_enabled, winner_reduction_pct, winner_ban_rounds,
                    course_id, tee_id,
                    entry_fee, prize_places,
                    prize_pct_1st, prize_pct_2nd, prize_pct_3rd, prize_pct_4th,
                    tie_handling, preferred_team_size, team_scoring_method,
                    logo_data, competition_format, medal_adjustment_table,
                    updated_at
                ) VALUES (
                    %s,%s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s,%s, %s,%s,%s, %s,%s,
                    %s,%s, %s,%s,%s,%s, %s,%s,%s, %s,%s,%s, NOW()
                )
                ON CONFLICT (rollup_id) DO UPDATE SET
                    display_name              = EXCLUDED.display_name,
                    ig_search_term            = EXCLUDED.ig_search_term,
                    run_days                  = EXCLUDED.run_days,
                    tee_interval_minutes      = EXCLUDED.tee_interval_minutes,
                    scoring_mode              = EXCLUDED.scoring_mode,
                    adjustment_table          = EXCLUDED.adjustment_table,
                    winner_bonus_enabled      = EXCLUDED.winner_bonus_enabled,
                    winner_gap_penalty1       = EXCLUDED.winner_gap_penalty1,
                    winner_gap_penalty2       = EXCLUDED.winner_gap_penalty2,
                    whs_pct_1st               = EXCLUDED.whs_pct_1st,
                    whs_pct_2nd               = EXCLUDED.whs_pct_2nd,
                    whs_pct_3rd               = EXCLUDED.whs_pct_3rd,
                    whs_winner_prohibition    = EXCLUDED.whs_winner_prohibition,
                    winner_reduction_enabled  = EXCLUDED.winner_reduction_enabled,
                    winner_reduction_pct      = EXCLUDED.winner_reduction_pct,
                    winner_ban_rounds         = EXCLUDED.winner_ban_rounds,
                    course_id                 = EXCLUDED.course_id,
                    tee_id                    = EXCLUDED.tee_id,
                    entry_fee                 = EXCLUDED.entry_fee,
                    prize_places              = EXCLUDED.prize_places,
                    prize_pct_1st             = EXCLUDED.prize_pct_1st,
                    prize_pct_2nd             = EXCLUDED.prize_pct_2nd,
                    prize_pct_3rd             = EXCLUDED.prize_pct_3rd,
                    prize_pct_4th             = EXCLUDED.prize_pct_4th,
                    tie_handling              = EXCLUDED.tie_handling,
                    preferred_team_size       = EXCLUDED.preferred_team_size,
                    team_scoring_method       = EXCLUDED.team_scoring_method,
                    logo_data                 = EXCLUDED.logo_data,
                    competition_format        = EXCLUDED.competition_format,
                    medal_adjustment_table    = EXCLUDED.medal_adjustment_table,
                    updated_at                = NOW()
            """, (
                rollup_id,
                s["display_name"], s["ig_search_term"],
                json.dumps(s["run_days"]),
                s["tee_interval_minutes"],
                db_scoring_mode,
                json.dumps(s["adjustment_table"]),
                s["winner_bonus_enabled"],
                s["winner_gap_penalty1"], s["winner_gap_penalty2"],
                s.get("whs_pct_1st", 0), s.get("whs_pct_2nd", 0), s.get("whs_pct_3rd", 0),
                s.get("whs_winner_prohibition", False),
                db_winner_reduction_enabled,
                s.get("winner_reduction_pct", 25),
                s.get("winner_ban_rounds", 3),
                s.get("course_id"), s.get("tee_id"),
                s["entry_fee"], s["prize_places"],
                s["prize_pct_1st"], s["prize_pct_2nd"],
                s["prize_pct_3rd"], s["prize_pct_4th"],
                s["tie_handling"], s["preferred_team_size"], s["team_scoring_method"],
                s.get("logo_data"),
                s.get("competition_format", "stableford"),
                json.dumps(s.get("medal_adjustment_table", DEFAULT_MEDAL_ADJUSTMENT_TABLE)),
            ))
            cur.execute("""
                UPDATE rollups SET name = %s, ig_search_term = %s WHERE id = %s
            """, (s["display_name"], s["ig_search_term"], rollup_id))


async def save_rollup_settings(rollup_id: int, settings: dict) -> None:
    await _run(_save_rollup_settings, rollup_id, settings)


# ---------------------------------------------------------------------------
# Tenant credentials (per-tenant IG login)
# ---------------------------------------------------------------------------

def _get_tenant_credentials(tenant_id: int) -> dict:
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT ig_username, ig_pin FROM tenant_credentials WHERE tenant_id = %s",
                (tenant_id,)
            )
            row = cur.fetchone()
            return dict(row) if row else {"ig_username": "", "ig_pin": ""}


async def get_tenant_credentials(tenant_id: int) -> dict:
    return await _run(_get_tenant_credentials, tenant_id)


def _save_tenant_credentials(tenant_id: int, ig_username: str, ig_pin: str):
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO tenant_credentials (tenant_id, ig_username, ig_pin, updated_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (tenant_id) DO UPDATE SET
                    ig_username = EXCLUDED.ig_username,
                    ig_pin      = EXCLUDED.ig_pin,
                    updated_at  = NOW()
            """, (tenant_id, ig_username, ig_pin))


async def save_tenant_credentials(tenant_id: int, ig_username: str, ig_pin: str) -> None:
    await _run(_save_tenant_credentials, tenant_id, ig_username, ig_pin)


# ---------------------------------------------------------------------------
# Legacy credentials (kept for backward compatibility)
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
