# Bramley Rollup Manager — Technical Documentation

**Version:** April 2026  
**Repository:** `johnnydw-bit/Generic-rollup`  
**Live URL:** `https://moths-rollup.onrender.com`

---

## Overview

Bramley Rollup Manager is a Progressive Web App (PWA) for managing golf society rollups at Bramley Golf Club. It scrapes player sign-ups from Intelligent Golf (IG), records Stableford scores, calculates handicap adjustments, supports team play, and maintains a full round history per player.

The app supports multiple independent rollups/societies (e.g. MOTH's, Wednesday Men, Friday Men), each with its own player list, round history, and configurable settings. Players can play in more than one roll-up with different handicaps for each.

---

## Architecture

### Stack

| Layer | Technology |
|---|---|
| Backend | Python 3.11, FastAPI |
| Database | PostgreSQL (Neon free tier) |
| ORM / DB access | psycopg2 (sync, per-request connections) |
| Frontend | Vanilla JS, single-page PWA |
| Charts | Chart.js 4.4.1 |
| Hosting | Render (free tier) |
| IG scraping | Playwright (via `backend/scraper.py`) |

### Project Structure

```
/
├── backend/
│   ├── main.py          # FastAPI app, all API endpoints
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
└── requirements.txt
```

### Render Configuration

- **Service name:** `moths-rollup`
- **Python version:** Set via `PYTHON_VERSION=3.11.9` environment variable (not `runtime.txt`)
- **Start command:** `uvicorn main:app --host 0.0.0.0 --port $PORT`
- **Environment variables required:**
  - `DATABASE_URL` — Neon PostgreSQL connection string

---

## Database Schema

All tables are created automatically on startup via `_init_schema()` in `db.py`.

### `rollups`
Stores each golf society.

| Column | Type | Notes |
|---|---|---|
| id | SERIAL PK | |
| name | TEXT UNIQUE | Display name, e.g. "MOTH's" |
| ig_search_term | TEXT | Used to filter IG sign-ups, e.g. "MOTH" |
| created_at | TIMESTAMPTZ | |

### `players`
One row per player per rollup.

| Column | Type | Notes |
|---|---|---|
| id | SERIAL PK | |
| rollup_id | INTEGER FK → rollups | |
| name | TEXT | |
| handicap | INTEGER | Current playing handicap |
| created_at | TIMESTAMPTZ | |

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
| recorded_at | TIMESTAMPTZ | |

### `rollup_settings`
One row per rollup, created on first save.

| Column | Type | Default | Notes |
|---|---|---|---|
| rollup_id | INTEGER UNIQUE FK → rollups | | |
| display_name | TEXT | '' | |
| ig_search_term | TEXT | '' | |
| run_days | TEXT (JSON array) | '[]' | e.g. `["Mon","Thu"]` |
| tee_interval_minutes | INTEGER | 8 | |
| adjustment_table | TEXT (JSON array) | see below | Handicap adjustment bands |
| winner_bonus_enabled | BOOLEAN | TRUE | |
| winner_gap_penalty1 | INTEGER | 0 | Extra -1 if winner beats 2nd by ≥ N pts |
| winner_gap_penalty2 | INTEGER | 0 | Extra -2 if winner beats 2nd by ≥ N pts |
| entry_fee | NUMERIC(6,2) | 0.00 | |
| prize_places | INTEGER | 3 | |
| prize_pct_1st/2nd/3rd/4th | INTEGER | 60/30/10/0 | Must sum to 100 |
| tie_handling | TEXT | 'tournament' | 'tournament' or 'countback' |
| preferred_team_size | INTEGER | 4 | |
| team_scoring_method | TEXT | 'best2' | see Team Play section |
| updated_at | TIMESTAMPTZ | | |

Default adjustment table:
```json
[
  {"max_score": 17, "adjustment": 2},
  {"max_score": 29, "adjustment": 1},
  {"max_score": 37, "adjustment": 0},
  {"max_score": 42, "adjustment": -1},
  {"max_score": null, "adjustment": -2}
]
```

### `app_credentials` *(legacy — not used in current version)*
Global singleton (id=1). Retained for backwards compatibility but credentials are now session-only (stored in browser localStorage).

---

## Database Connection

`db.py` uses **per-request connections** — a fresh `psycopg2.connect()` is opened for every database operation and closed immediately after. This eliminates stale connection errors caused by Neon's free tier dropping idle connections after ~5 minutes and Render's free tier sleeping the service.

```python
@contextmanager
def _get_conn():
    conn = psycopg2.connect(_get_db_url())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
```

All database functions are synchronous but are run via `asyncio.run_in_executor` so FastAPI stays non-blocking.

---

## Handicap Calculation

All logic is in `backend/handicap.py`. The calculation is driven entirely by settings loaded from the database — there are no hardcoded values.

### Adjustment Table

The score is looked up in the `adjustment_table` array. Rows are evaluated top to bottom; the first row where `score <= max_score` (or `max_score` is null) wins.

### Winner Penalty (Individual Mode Only)

1. The player with the highest score is identified as the round winner.
2. If `winner_bonus_enabled` is true, an additional **-1** is applied on top of their table adjustment.
3. If `winner_gap_penalty1 > 0` and the winner beats 2nd place by ≥ that many points, an extra **-1** is applied.
4. If `winner_gap_penalty2 > 0` and the winner beats 2nd place by ≥ that many points, an extra **-2** is applied (supersedes penalty1).
5. In team mode, the winner penalty is suspended entirely.

Handicaps are floored at 0.

### Team Scoring Methods

| Method | Description |
|---|---|
| `best1` | Best single score per team |
| `best2` | Best 2 scores per team (default) |
| `best3` | Best 3 scores per team |
| `all` | Sum of all scores |
| `worst2` | Best of the two lowest scores |

---

## API Endpoints

Base URL: `https://moths-rollup.onrender.com`

### Rollups

| Method | Path | Description |
|---|---|---|
| GET | `/api/rollups` | List all rollups |
| POST | `/api/rollups/add` | Add or update a rollup |

### Players

| Method | Path | Description |
|---|---|---|
| GET | `/api/players?rollup_id=` | List players (name + handicap) |
| GET | `/api/players/detail?rollup_id=` | List players with id and round count |
| POST | `/api/new-player` | Add or update a player |
| POST | `/api/lookup-player` | Look up a player by name |
| POST | `/api/players/update-handicap` | Update a player's handicap by id |
| POST | `/api/players/delete` | Remove a player from a rollup |

### Rounds

| Method | Path | Description |
|---|---|---|
| POST | `/api/load-players` | Scrape IG sign-ups for a date |
| POST | `/api/autosave` | Recalculate handicaps (no DB write) |
| POST | `/api/save-round` | Save completed round to DB |
| GET | `/api/round-dates?rollup_id=` | List all round dates |
| GET | `/api/round?date=&rollup_id=` | Get results for a specific date |
| GET | `/api/last-round?rollup_id=` | Get most recent round |
| GET | `/api/player-history?name=&rollup_id=` | Get all rounds for a player |

### Settings

| Method | Path | Description |
|---|---|---|
| GET | `/api/settings?rollup_id=` | Load rollup settings |
| POST | `/api/settings` | Save rollup settings |

### Other

| Method | Path | Description |
|---|---|---|
| GET | `/auth/status?rollup_id=` | Returns date of last round |
| GET | `/sw.js` | Minimal service worker (passthrough) |
| GET | `/` | Serves the PWA |

---

## Frontend — Screen Map

The app is a single HTML file (`frontend/templates/index.html`) with all screens hidden/shown via CSS classes. No framework is used.

| Screen | ID | Description |
|---|---|---|
| 0 | `screen0` | Login — IG credentials + rollup select |
| 1 | `screen1` | Home — date picker, navigation hub |
| 2 | `screen2` | Score entry — live handicap recalculation |
| 3 | `screen3` | Results browser — browse past rounds |
| 4 | `screen4` | Player history — scores & handicap chart |
| 5 | `screen5` | Settings — per-rollup configuration |
| 6 | `screen6` | Player manager — add/edit/remove players |

### Session Management

Credentials and rollup selection are stored in `localStorage` only — nothing sensitive is persisted in the database:

| Key | Value |
|---|---|
| `ig_username` | IG member ID (pre-filled on screen0) |
| `ig_pin` | IG PIN (session only) |
| `rollup_id` | Selected rollup ID |
| `rollup_name` | Selected rollup name |
| `rollup_term` | IG search term for the rollup |

`hasValidSession()` checks that all five keys are present. If any are missing, screen0 is shown.

To sign out (or switch user), tap the **↩** button in the screen1 header. This clears all localStorage keys and returns to screen0.

### Rollup Switching

Any signed-in user can switch rollup without signing out by tapping the rollup name in the header on screens 1, 3, or 4, or using the Switch button in Settings. Switching clears all cached state (players, round history, chart) so no data bleeds between rollups.

---

## Score Entry Flow

1. User selects a date on screen1 and taps **Load Players**
2. App calls `POST /api/load-players` which scrapes IG for sign-ups matching the rollup's search term on that date
3. Any scraped names not found in the `players` table are flagged as new players — a dialog prompts for their starting handicap before proceeding
4. Score entry screen opens. As scores are entered, `POST /api/autosave` is called on every change — this recalculates new handicaps and team scores in real time without writing to the DB
5. When all scores are entered, user taps **Save Round** → `POST /api/save-round` writes results to the DB and navigates to the results screen

Walk-up players (not on the IG sign-up list) can be added at any point during score entry.

---

## Team Play

When team mode is enabled:

- Players are assigned to teams manually or via the **🎲 Random assign** button
- Team structure is calculated automatically based on player count and `preferred_team_size` setting
  - Prefers fours (or threes) with minimal remainder teams
  - e.g. 14 players → 3 teams of 4 + 1 team of 2, or 2 teams of 4 + 2 teams of 3
- Multiple valid structures are offered as options when there is more than one
- The winner penalty is suspended in team mode
- Team scores are displayed in a live scoreboard above the grid

---

## Settings Reference

Settings are per-rollup and saved to `rollup_settings`. They take effect immediately on the next round.

### Rollup Identity
- **Display name** — shown in the app header
- **IG search term** — used to filter Intelligent Golf sign-ups (must match the IG contact name for the rollup)
- **Runs on** — day checkboxes (informational for now, future date picker filtering)

### Tee Times
- **Interval** — minutes between tee slots, used to calculate available tee time count

### Handicap Adjustment Table
- Configurable score bands with adjustment values (+3 to -3)
- Last row has no upper limit (catch-all)

### Winner Penalty
- Toggle the base -1 winner penalty on/off
- Optional gap penalties: extra cuts if winner dominates

### Entry & Prizes
- Entry fee, number of prize places (1–4), percentage splits
- Prize percentages must sum to 100%
- Tie handling: shared prizes or countback (manual)

### Team Play
- Preferred team size (Fours or Threes preferred)
- Default scoring method

---

## Player Manager (Screen 6)

Accessible via the **👥 Players** tab in the nav bar.

- Select any rollup from the dropdown at the top
- Shows all players with current handicap and round count
- **Edit handicap** — click the handicap field, type new value, tap Save
- **Add player** — tap ＋ Add, enter name and starting handicap
- **Remove player** — tap ✕, confirm in dialog. The player row is deleted but their round history is preserved (rounds reference player_id which cascades, but only if the player is deleted)

> Note: Removing a player deletes them and their round history (via CASCADE DELETE on the `rounds` table). This is intentional — a player with no rounds can be freely removed; a player with history should only be removed deliberately.

---

## Known Issues & Backlog

### Bugs
1. Walk-up player dialog on score entry does not check team capacity before opening
2. Handicap adjustment table UI — lower bounds are derived/fixed; a "Reset to defaults" button would help
3. Prize % auto-defaults do not fire when prize places is loaded from DB (only on manual change)

### Pending Features
1. Wire `run_days` into date picker filtering
2. Prize money display on scoring screen (requires player count from round)
3. Prize calculation and payout screen
4. Tie handling logic (tournament-style prize sharing)
5. Logo upload in settings
6. Rollup selection / management moved into settings (screen0 bypass when credentials are set)
7. CRUD for rounds (edit/delete a saved round)
8. CRUD for rollups (rename/delete from within the app)

---

## Development Notes

### Adding a New Rollup

Via the app: tap **＋ Add new rollup** on screen0, or use the Add Rollup dialog from the rollup switcher.

Via SQL (Neon console):
```sql
INSERT INTO rollups (name, ig_search_term) VALUES ('My Rollup', 'MYSEARCH');
```

### Cleaning Up Test Data

```sql
-- Remove test players by name pattern
DELETE FROM players WHERE name LIKE 'player %';

-- Remove all rounds for a specific date
DELETE FROM rounds WHERE date = '2026-04-01' AND rollup_id = 2;
```

### Resetting a Player's Handicap

```sql
UPDATE players SET handicap = 24 WHERE name = 'John Smith' AND rollup_id = 1;
```

### Clearing localStorage (browser)

Open DevTools console and run:
```javascript
localStorage.clear();
```
This signs the user out and clears the rollup selection.

---

## Deployment

The app is deployed on Render's free tier. Free tier services spin down after 15 minutes of inactivity — the first request after a spin-down takes ~30 seconds to respond while the service restarts.

Per-request DB connections (rather than a pool) mean there is no connection state to recover after a spin-down.

To redeploy: push to the `main` branch of `johnnydw-bit/Generic-rollup` on GitHub. Render auto-deploys on push.

Python version is controlled via the `PYTHON_VERSION` environment variable in the Render dashboard (set to `3.11.9`). Neither `runtime.txt` nor `render.yaml` `pythonVersion` are reliable on this plan.
