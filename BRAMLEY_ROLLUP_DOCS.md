# Bramley Competition Manager — Technical Documentation

**Version:** May 2026  
**Repository:** `johnnydw-bit/Generic-rollup`  
**Live URL:** `https://competition-manager.onrender.com`

---

## Overview

Bramley Competition Manager is a Progressive Web App (PWA) for managing golf society competitions at Bramley Golf Club. It scrapes player sign-ups from Intelligent Golf (IG), records Stableford scores, calculates handicap adjustments, supports team play, tracks prize money, and maintains a full round history per player.

The app is **multi-tenant**: each golf club has a unique URL slug (e.g. `/bramley`). Within a club, multiple independent competitions (e.g. MOTH's, Wednesday Men, Friday Men) each have their own player list, round history, and settings. Players can play in more than one competition with different handicaps for each.

---

## Architecture

### Stack

| Layer | Technology |
|---|---|
| Backend | Python 3.11, FastAPI |
| Database | PostgreSQL (Neon free tier) |
| DB access | psycopg2 (sync, per-request connections) |
| Frontend | Vanilla JS, single-page PWA |
| Charts | Chart.js 4.4.1 |
| Hosting | Render (free tier) |
| IG scraping | httpx + BeautifulSoup4 (via `backend/scraper.py`) |

### Project Structure

```
/
├── backend/
│   ├── db.py            # Database layer (all queries)
│   ├── handicap.py      # Handicap calculation logic
│   └── scraper.py       # Intelligent Golf scraper
├── frontend/
│   ├── templates/
│   │   └── index.html   # Single-page PWA (all screens)
│   └── static/
│       ├── manifest.json
│       ├── icon-192.png
│       └── MOTHS_APP_LOGO.jpg
├── main.py              # FastAPI app, all API endpoints
└── requirements.txt
```

### Render Configuration

- **Service name:** `competition-manager`
- **Python version:** Set via `PYTHON_VERSION=3.11.9` environment variable
- **Start command:** `uvicorn main:app --host 0.0.0.0 --port $PORT`
- **Environment variables required:**
  - `DATABASE_URL` — Neon PostgreSQL connection string

---

## Multi-Tenant System

Each club is a **tenant** identified by a URL slug (e.g. `bramley`). The SPA is served at `/{slug}`. Admin creates tenants via the `/admin` dashboard.

### Routing

- `/` — Root login page (Competition Manager branding; enter slug to sign in)
- `/{slug}` — Serves the full SPA for that club
- `/admin` — Superadmin dashboard (GitHub OAuth or token)

### Auth Flow

1. User visits `/bramley` → Jinja template is rendered with `tenant_name`
2. User signs in with their member ID and PIN/password
3. Server validates credentials against the `users` table for that tenant
4. A session token is created (in-memory, 30-day TTL) and set as an httpOnly cookie
5. All API calls include this cookie; the `get_current_session` dependency validates it

Session tokens are held in a Python dict (`_user_sessions`). They are **not** persisted — a server restart clears all sessions and users must sign in again.

---

## Database Schema

All tables are created and migrated automatically on startup via `_init_schema()` in `db.py`. Migrations use `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` so they are idempotent and safe to run repeatedly.

### `tenants`

| Column | Type | Notes |
|---|---|---|
| id | SERIAL PK | |
| name | TEXT | Club name, shown in app header |
| slug | TEXT UNIQUE | URL path component, e.g. `bramley` |
| ig_tenant | BOOLEAN | Whether this club uses IG sign-ups |
| created_at | TIMESTAMPTZ | |

### `users`

| Column | Type | Notes |
|---|---|---|
| id | SERIAL PK | |
| tenant_id | INTEGER FK → tenants | |
| username | TEXT | Member ID (case-insensitive lookup) |
| password_hash | TEXT | PBKDF2-SHA256 with random salt |
| ig_pin | TEXT | Stored for IG scraping during session |
| display_name | TEXT | Friendly name |

Unique constraint: `(tenant_id, lower(username))`

### `tenant_credentials`

| Column | Type | Notes |
|---|---|---|
| tenant_id | INTEGER PK FK → tenants | |
| ig_username | TEXT | IG account username for scraping |
| ig_pin | TEXT | IG account PIN |
| updated_at | TIMESTAMPTZ | |

### `rollups`

Stores each competition within a tenant.

| Column | Type | Notes |
|---|---|---|
| id | SERIAL PK | |
| tenant_id | INTEGER FK → tenants | |
| name | TEXT | Display name, e.g. "MOTH's" |
| ig_search_term | TEXT | Filters IG sign-ups, e.g. "MOTH" |
| created_at | TIMESTAMPTZ | |

### `players`

One row per player per competition.

| Column | Type | Default | Notes |
|---|---|---|---|
| id | SERIAL PK | | |
| rollup_id | INTEGER FK → rollups | | |
| name | TEXT | | |
| handicap | INTEGER | 0 | Current playing handicap (integer) |
| whs_index | NUMERIC(4,1) | NULL | WHS handicap index (one decimal place) from IG |
| whs_index_next_round | NUMERIC(4,1) | NULL | Temporary reduced index (Winners Only 1 mode) |
| winner_prohibited | BOOLEAN | FALSE | Legacy flag — prohibited from winning next round |
| winner_ban_entries | INT | 0 | Rounds remaining on winner ban (Winners Only 2) |
| winner_ban_original_hc | INT | NULL | Handicap before first ban reduction, for reinstatement |
| total_prize_won | NUMERIC(8,2) | 0 | Cumulative prize money won across all rounds |
| created_at | TIMESTAMPTZ | | |

Unique constraint: `(rollup_id, name)`

### `rounds`

One row per player per round date.

| Column | Type | Notes |
|---|---|---|
| id | SERIAL PK | |
| player_id | INTEGER FK → players (CASCADE DELETE) | |
| rollup_id | INTEGER FK → rollups | |
| date | DATE | |
| score | INTEGER | Stableford points |
| new_handicap | INTEGER | Handicap after this round |
| whs_mode | BOOLEAN | True if saved in Winners Only 1 mode |
| whs_index_used | NUMERIC(4,1) | WHS index applied this round |
| new_whs_index | NUMERIC(4,1) | Reduced index for next round (if applicable) |
| course_id | INTEGER FK → courses | |
| tee_id | INTEGER FK → tees | |
| recorded_at | TIMESTAMPTZ | |

### `rollup_settings`

One row per competition, created on first save.

| Column | Type | Default | Notes |
|---|---|---|---|
| rollup_id | INTEGER UNIQUE FK → rollups | | |
| display_name | TEXT | '' | Shown in app header |
| ig_search_term | TEXT | '' | Overrides rollup.ig_search_term if set |
| run_days | TEXT (JSON array) | '[]' | e.g. `["Mon","Thu"]` |
| tee_interval_minutes | INTEGER | 8 | |
| scoring_mode | TEXT | 'stableford' | See H/C Adjustment Methods |
| adjustment_table | TEXT (JSON array) | see below | Stableford adjustment bands |
| winner_bonus_enabled | BOOLEAN | TRUE | Universal mode: extra -1 for winner |
| winner_gap_penalty1 | INTEGER | 0 | Extra -1 if gap ≥ N pts |
| winner_gap_penalty2 | INTEGER | 0 | Extra -2 if gap ≥ N pts |
| whs_pct_1st | NUMERIC | 0 | Winners Only 1: % reduction for 1st place |
| whs_pct_2nd | NUMERIC | 0 | Winners Only 1: % reduction for 2nd place |
| whs_pct_3rd | NUMERIC | 0 | Winners Only 1: % reduction for 3rd place |
| whs_winner_prohibition | BOOLEAN | FALSE | Winners Only 1: block last winner |
| winner_reduction_pct | INTEGER | 25 | Winners Only 2: % HC cut applied to winner |
| winner_ban_rounds | INTEGER | 3 | Winners Only 2: rounds of ban countdown |
| entry_fee | NUMERIC(6,2) | 0.00 | Per-player entry fee |
| prize_places | INTEGER | 3 | Number of prize positions (1–4) |
| prize_pct_1st/2nd/3rd/4th | INTEGER | 60/30/10/0 | Must sum to 100 |
| tie_handling | TEXT | 'tournament' | 'tournament' (share) or 'countback' |
| preferred_team_size | INTEGER | 4 | |
| team_scoring_method | TEXT | 'best2' | See Team Play section |
| course_id | INTEGER | NULL | Selected course for tee HC calculation |
| tee_id | INTEGER | NULL | Selected tee for tee HC calculation |
| logo_data | TEXT | NULL | Base64 data URL of club logo |
| updated_at | TIMESTAMPTZ | | |

**`scoring_mode` stored values and backward compat:**

| Stored value | `winner_reduction_enabled` | Displayed as |
|---|---|---|
| `stableford` | FALSE | Universal |
| `stableford` | TRUE | Winners Only 2 |
| `whs` | — | Winners Only 1 |

On read, `_get_rollup_settings` maps these to the current internal values (`universal`, `winners_only_1`, `winners_only_2`) and strips `winner_reduction_enabled` from the response. On write, `_save_rollup_settings` maps back to the legacy stored values.

**Default adjustment table:**
```json
[
  {"max_score": 17, "adjustment": 2},
  {"max_score": 29, "adjustment": 1},
  {"max_score": 37, "adjustment": 0},
  {"max_score": 42, "adjustment": -1},
  {"max_score": null, "adjustment": -2}
]
```

### `courses` and `tees`

Used for tee-based course handicap calculation.

| Table | Key columns |
|---|---|
| `courses` | id, name, club, created_at |
| `tees` | id, course_id, name, rating, slope, par |

---

## Database Connection

`db.py` uses **per-request connections** — a fresh `psycopg2.connect()` is opened for every database operation and closed immediately after. This eliminates stale connection errors from Neon's free tier (drops idle connections after ~5 min) and Render's free tier (sleeps service after ~15 min).

All database functions are synchronous but run via `asyncio.run_in_executor` so FastAPI stays non-blocking.

---

## H/C Adjustment Methods

Three mutually exclusive modes are selected per competition in Settings → H/C Adjustment Method.

### Universal

All players' handicaps adjust each round based on their Stableford score. Uses the configurable adjustment table. An optional **winner penalty** applies an extra -1 to the highest scorer. Optional gap penalties add further cuts if the winner dominates. Handicaps are floored at 0.

Winner penalty logic (individual mode only):
1. Find the player with the highest score
2. Apply table adjustment + base -1 (if `winner_bonus_enabled`)
3. If `winner_gap_penalty2 > 0` and gap to 2nd ≥ threshold → extra -2 (replaces -1)
4. If `winner_gap_penalty1 > 0` and gap to 2nd ≥ threshold → extra -1
5. In team mode, all winner bonuses are suspended

### Winners Only 1 (WHS Position Adjustment)

Based on WHS handicap index. The **top 3 finishers** receive a configurable % reduction to their WHS index. The reduction is stored as `whs_index_next_round` and applied next round only; the permanent `whs_index` is unchanged.

- `whs_pct_1st / 2nd / 3rd` — configurable per position (0% skips that place)
- Rounding: to 1 decimal place using standard rounding (ROUND_HALF_UP)
- Optional **winner prohibition** — if enabled, a player who won the last round cannot win this round; the server returns a 400 error before saving

Course handicap displayed in the score grid = `round(whs_index_next_round ?? whs_index)`, optionally calculated from tee rating/slope if a course is configured.

### Winners Only 2 (Winner Ban)

The **round winner** receives a reduction to their playing handicap for the next N rounds. Other players' handicaps are unchanged.

- **Reduction %** (`winner_reduction_pct`) — default 25%
- **Ban length** (`winner_ban_rounds`) — default 3 rounds
- **Maximum reduction** — capped at two applications (25% twice = floor of `orig_hc × 0.75²`). A re-win during the ban applies no further cut beyond this floor.
- `winner_ban_entries` counts down each round; reaches 0 to lift the ban
- `winner_ban_original_hc` stores the pre-ban handicap so it can be reinstated exactly
- The ban reduction is **display-only on the client** — `initPlayingHC()` derives the course HC from the ratio `handicap / winner_ban_original_hc` × WHS-based HC. It does not modify `whs_index` in the DB.
- Multiple players can be banned simultaneously

---

## Prize Money

### Prize Bar (Score Entry Screen)

When entry fee and prize places are configured, a yellow bar appears above the score grid showing the prize breakdown for that session:

```
Prize pot (18 × £5): 🥇 1st: £54  ·  🥈 2nd: £27  ·  🥉 3rd: £9
```

Player count is the number of loaded players. Amounts are calculated client-side from settings.

### Cumulative Prize Tracking

On every `POST /api/save-round`:
1. Prize pot = `scored_player_count × entry_fee`
2. Players are ranked by score descending
3. Tied players share the combined prizes for the positions they jointly occupy (e.g. two tied for 1st share 1st + 2nd prize equally)
4. Each winner's share is added to `players.total_prize_won` via `UPDATE`

`total_prize_won` is shown in the Player Manager (screen 6) as a running cumulative total. It is never decremented — if a round is overwritten or deleted, the prize credit is not reversed.

---

## Team Play

When team mode is enabled:

- Players are assigned to teams manually or via **🎲 Random assign**
- Team structure is calculated automatically based on player count and `preferred_team_size`
- Multiple valid structures are offered when more than one exists
- Winner penalty is suspended in team mode
- Live team scoreboard is shown above the grid

### Team Scoring Methods

| Method | Description |
|---|---|
| `best1` | Best single score per team |
| `best2` | Best 2 scores per team (default) |
| `best3` | Best 3 scores per team |
| `all` | Sum of all scores |
| `worst2` | Best of the two worst scores |

---

## API Endpoints

Base URL: `https://competition-manager.onrender.com`

All endpoints (except auth) require a valid session cookie. Tenant is derived from the session; users can only access rollups belonging to their own tenant.

### Auth

| Method | Path | Description |
|---|---|---|
| POST | `/auth/login` | Sign in with slug + username + credential |
| POST | `/auth/register` | Register new user for a tenant |
| POST | `/auth/logout` | Clear session |
| GET | `/auth/status?rollup_id=` | Returns date of last round |
| GET | `/auth/check-slug?slug=` | Look up tenant name by slug |

### Competitions (Rollups)

| Method | Path | Description |
|---|---|---|
| GET | `/api/rollups` | List all rollups for current tenant |
| POST | `/api/rollups/add` | Add or update a rollup |

### Players

| Method | Path | Description |
|---|---|---|
| GET | `/api/players?rollup_id=` | List players (name, handicap, WHS, ban fields, total_prize_won) |
| GET | `/api/players/detail?rollup_id=` | List players with id, round count, total_prize_won |
| POST | `/api/new-player` | Add or upsert a player |
| POST | `/api/lookup-player` | Look up a player by name |
| POST | `/api/players/update-handicap` | Update handicap by player id |
| POST | `/api/players/update-whs` | Update WHS index by player id |
| POST | `/api/players/delete` | Remove a player |
| POST | `/api/players/sync-whs` | Sync WHS indices from IG for all players |

### Rounds

| Method | Path | Description |
|---|---|---|
| POST | `/api/load-players` | Scrape IG sign-ups for a date |
| POST | `/api/autosave` | Recalculate handicaps (no DB write) |
| POST | `/api/save-round` | Save completed round, credit prize money |
| GET | `/api/round-dates?rollup_id=` | List all round dates |
| GET | `/api/round?date=&rollup_id=` | Get results for a specific date |
| GET | `/api/last-round?rollup_id=` | Get most recent round |
| GET | `/api/player-history?name=&rollup_id=` | Get all rounds for a player |

### Settings

| Method | Path | Description |
|---|---|---|
| GET | `/api/settings?rollup_id=` | Load rollup settings |
| POST | `/api/settings` | Save rollup settings |
| POST | `/api/upload-logo` | Upload club logo (multipart, 500 KB limit) |

### Courses

| Method | Path | Description |
|---|---|---|
| GET | `/api/courses` | List all courses |
| GET | `/api/tees?course_id=` | List tees for a course |
| POST | `/api/courses` | Save course and tees |
| GET | `/api/search-course?q=` | Search 18Birdies for a course |
| POST | `/api/fetch-course` | Fetch course detail from URL |
| POST | `/api/parse-ncrdb` | Parse NCRDB paste for course data |

### Admin

| Method | Path | Description |
|---|---|---|
| GET | `/admin` | Superadmin dashboard |
| GET | `/admin/login` | Admin auth |
| POST | `/admin/tenants` | Create new tenant |
| GET | `/admin/visit/{slug}` | Impersonate a tenant |

---

## Frontend — Screen Map

The app is a single HTML file (`frontend/templates/index.html`). All screens are hidden/shown via CSS classes. No framework is used.

| Screen | ID | Description |
|---|---|---|
| 0 | `screen0` | Sign in — credentials + competition select |
| 1 | `screen1` | Home — date picker, navigation hub |
| 2 | `screen2` | Score entry — live handicap recalculation, prize bar |
| 3 | `screen3` | Results browser — browse past rounds |
| 4 | `screen4` | Player history — scores & handicap chart |
| 5 | `screen5` | Settings — per-competition configuration |
| 6 | `screen6` | Player manager — add/edit/remove players, prize totals |

### Session Management

Session token is stored as a cookie (set by the server, 30-day TTL). Rollup selection is stored in `localStorage`.

| localStorage key | Value |
|---|---|
| `session_token` | Auth cookie (also set as httpOnly cookie) |
| `rollup_id` | Selected competition ID |
| `rollup_name` | Selected competition name |
| `rollup_term` | IG search term for the competition |

`hasValidSession()` checks that the session cookie and rollup selection exist. If absent, screen0 is shown.

---

## Score Entry Flow

1. User selects a date on screen 1 and taps **Load Players**
2. `POST /api/load-players` scrapes IG for sign-ups matching the competition's search term
3. Names not in the `players` table are flagged as new — a dialog prompts for their starting handicap
4. Score entry screen opens (screen 2):
   - **Course H/C column** is pre-populated via `initPlayingHC()` and is editable
   - For Winners Only 1: uses `whs_index_next_round ?? whs_index`, optionally adjusted by tee rating/slope
   - For Winners Only 2: applies winner ban reduction ratio from `handicap / winner_ban_original_hc`
   - As scores are entered, `POST /api/autosave` recalculates in real time (no DB write)
   - A **prize bar** shows the prize pot breakdown if entry fee is configured
   - Ban badges (⛔N) appear next to banned players' names
5. User taps **Save Round** → `POST /api/save-round`:
   - Writes round results to DB
   - Applies handicap adjustments per mode
   - Credits prize money to `total_prize_won` for each winner
   - Navigates to results screen

Walk-up players can be added at any point during score entry.

---

## Logo Upload

A club logo can be uploaded in Settings → Competition Logo. The image is:
- Converted to a base64 data URL in the browser
- POSTed to `POST /api/upload-logo` as multipart form data
- Validated (≤ 500 KB)
- Stored in `rollup_settings.logo_data` (TEXT column in PostgreSQL)

The logo is served directly from the DB — no filesystem storage is used, which is necessary for Render's ephemeral file system. It replaces the default `MOTHS_APP_LOGO.jpg` on screens 0, 1, and the settings preview.

---

## Settings Reference

### Competition Identity
- **Display name** — shown in the app header (overrides the slug-derived name)
- **IG search term** — filters Intelligent Golf sign-ups
- **Runs on** — day checkboxes (informational)
- **Competition Logo** — upload a custom logo (replaces Bramley default)

### Tee Times
- **Interval** — minutes between tee slots

### H/C Adjustment Method
Three mutually exclusive modes — see [H/C Adjustment Methods](#hc-adjustment-methods) above.

- **Universal** — all players adjust via the score table; optional winner penalty
- **Winners Only 1** — WHS position-based % reduction for top 3 finishers
- **Winners Only 2** — winner ban: % HC cut + N-round countdown; max 2× reduction

### Handicap Adjustment Table (Universal only)
- Configurable score bands, adjustment values (+3 to -3)
- Last row has no upper limit (catch-all)

### Winner Penalty (Universal only)
- Toggle the base -1 penalty on/off
- Optional gap penalties for dominant wins

### WHS Settings (Winners Only 1 only)
- Reduction % per finishing position (1st, 2nd, 3rd)
- Winner prohibition toggle

### Winner Ban Settings (Winners Only 2 only)
- Reduction % (default 25%)
- Ban length in rounds (default 3)

### Entry & Prizes
- Entry fee per player
- Number of prize places (1–4)
- Percentage splits (must sum to 100%)
- Tie handling: share prizes or countback

### Course & Tees
- Select a course and tee for tee-based course handicap calculation

### Team Play
- Preferred team size
- Default scoring method

---

## Player Manager (Screen 6)

Accessible via the **👥 Players** tab.

| Column | Description |
|---|---|
| Player | Name with ban badge (⛔N) and prohibition badge (🚫) |
| HC | Editable integer handicap |
| WHS | Editable WHS index (one decimal place) |
| Next Rd | Temporary WHS index for next round (read-only) |
| Rds | Number of rounds played |
| Prize £ | Cumulative prize money won (green, e.g. £54.00) |
| Actions | Save HC, Save WHS, Delete |

---

## Development Notes

### Adding a New Competition

Via the app: tap **＋ Add new competition** on screen 0, or from the competition switcher.

Via SQL (Neon console):
```sql
INSERT INTO rollups (tenant_id, name, ig_search_term) VALUES (1, 'Wednesday Men', 'WEDS');
```

### Resetting a Player's Handicap

```sql
UPDATE players SET handicap = 24 WHERE name = 'John Smith' AND rollup_id = 1;
```

### Resetting a Player's Prize Total

```sql
UPDATE players SET total_prize_won = 0 WHERE name = 'John Smith' AND rollup_id = 1;
```

### Removing a Winner Ban

```sql
UPDATE players SET winner_ban_entries = 0, winner_ban_original_hc = NULL
WHERE name = 'John Smith' AND rollup_id = 1;
```

### Cleaning Up Test Data

```sql
DELETE FROM players WHERE name LIKE 'Test %';
DELETE FROM rounds WHERE date = '2026-04-01' AND rollup_id = 2;
```

### Clearing Browser Session

Open DevTools console:
```javascript
localStorage.clear();
document.cookie = 'session_token=; expires=Thu, 01 Jan 1970 00:00:00 GMT; path=/';
```

---

## Deployment

Deployed on Render's free tier. Free tier services spin down after 15 minutes of inactivity — the first request after a spin-down takes ~30 seconds.

Per-request DB connections mean there is no connection state to recover after a spin-down.

**To redeploy:** push to the `main` branch of `johnnydw-bit/Generic-rollup`. Render auto-deploys on push.

DB migrations run automatically on startup — new columns are added safely via `ADD COLUMN IF NOT EXISTS`.

Python version is controlled via the `PYTHON_VERSION=3.11.9` environment variable in the Render dashboard.

---

## Known Issues & Backlog

### Bugs
1. Walk-up player dialog does not check team capacity before opening
2. Adjustment table UI — lower bounds are derived; a "Reset to defaults" button is missing
3. `hc_change` column on the results screen is hardcoded to `—` (not calculated from round history)
4. Prize money is not reversed if a round is overwritten or deleted

### Pending Verification
- Winner reduction across multiple rounds including the 2× cap
- Logo upload persistence across server restarts
- All three H/C methods end-to-end
- Backward compat for existing competitions after `scoring_mode` rename
- Multi-tenant isolation (logo and settings must not leak between tenants)

### Potential Enhancements
1. Wire `run_days` into date picker filtering
2. CRUD for rounds (edit/delete a saved round)
3. CRUD for competitions (rename/delete from within the app)
4. Prize payout screen with per-player breakdown
5. Countback tie-breaking (currently informational only)
