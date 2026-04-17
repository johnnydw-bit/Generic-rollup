# Bramley Rollup - backend/main.py
# v4 04/17: WHS mode, courses/tees, player manager endpoints, WHS index scraper

import os
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from fastapi import Request
from pydantic import BaseModel
from dotenv import load_dotenv

from backend.handicap import (
    calculate_new_handicaps,
    calculate_whs_handicaps,
    calculate_team_scores,
    format_adjustment,
)
from backend.db import (
    get_all_players,
    get_all_players_detail,
    update_player_handicap,
    update_player_whs_index,
    remove_player,
    get_all_rollups,
    get_or_create_rollup,
    save_round_results,
    add_new_player,
    get_last_round_results,
    get_last_round_date,
    get_player_history,
    get_round_dates,
    get_round_by_date,
    get_rollup_settings,
    save_rollup_settings,
    get_credentials,
    save_credentials,
    get_all_courses,
    get_tees_for_course,
    get_prohibited_winners,
    init_db,
    close_db,
)
from backend.scraper import scrape_players

load_dotenv()

app = FastAPI(title="Bramley Rollup")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="frontend/static"), name="static")
templates = Jinja2Templates(directory="frontend/templates")


@app.on_event("startup")
async def startup():
    await init_db()


@app.on_event("shutdown")
async def shutdown():
    await close_db()


@app.get("/sw.js")
async def service_worker():
    from fastapi.responses import Response
    js = "self.addEventListener('fetch', function(event) {});"
    return Response(content=js, media_type="application/javascript")


@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


# ---------------------------------------------------------------------------
# Rollups
# ---------------------------------------------------------------------------

@app.get("/api/rollups")
async def rollups():
    try:
        data = await get_all_rollups()
    except Exception as e:
        raise HTTPException(500, f"Could not load rollups: {str(e)}")
    return {"rollups": data}


class AddRollupRequest(BaseModel):
    name: str
    ig_search_term: str


@app.post("/api/rollups/add")
async def add_rollup(body: AddRollupRequest):
    try:
        rollup_id = await get_or_create_rollup(body.name, body.ig_search_term.upper())
    except Exception as e:
        raise HTTPException(500, f"Could not add rollup: {str(e)}")
    return {"ok": True, "id": rollup_id, "name": body.name,
            "ig_search_term": body.ig_search_term.upper()}


# ---------------------------------------------------------------------------
# Auth / status
# ---------------------------------------------------------------------------

@app.get("/auth/status")
async def auth_status(rollup_id: int = Query(1)):
    try:
        last_date = await get_last_round_date(rollup_id)
    except Exception:
        last_date = None
    return {"last_round_date": last_date}


# ---------------------------------------------------------------------------
# Load players
# ---------------------------------------------------------------------------

class LoadRequest(BaseModel):
    date: str
    ig_username: str
    ig_pin: str
    rollup_id: int
    ig_search_term: str


@app.post("/api/load-players")
async def load_players(body: LoadRequest):
    try:
        scrape_result = await scrape_players(
            body.ig_username,
            body.ig_pin,
            body.date,
            body.ig_search_term,
        )
    except Exception as e:
        raise HTTPException(502, str(e))

    names     = scrape_result["names"]
    tee_times = scrape_result["tee_times"]
    tee_start = scrape_result.get("tee_start", "")

    if not names:
        raise HTTPException(404, "No players found for this date.")

    try:
        all_players = await get_all_players(body.rollup_id)
    except Exception as e:
        raise HTTPException(500, f"Could not read player list from database: {str(e)}")

    name_to_hc = {p["name"].strip().lower(): p["handicap"] for p in all_players}

    players     = []
    new_players = []
    for name in names:
        hc     = name_to_hc.get(name.strip().lower())
        p_data = next(
            (p for p in all_players if p["name"].strip().lower() == name.strip().lower()),
            None
        )
        if hc is None:
            new_players.append(name)
            players.append({
                "name": name, "handicap": None, "score": None, "team": None,
                "new_player": True, "whs_index": None,
                "whs_index_next_round": None, "winner_prohibited": False,
            })
        else:
            players.append({
                "name":                 name,
                "handicap":             hc,
                "score":                None,
                "team":                 None,
                "new_player":           False,
                "whs_index":            float(p_data["whs_index"]) if p_data and p_data.get("whs_index") else None,
                "whs_index_next_round": float(p_data["whs_index_next_round"]) if p_data and p_data.get("whs_index_next_round") else None,
                "winner_prohibited":    p_data.get("winner_prohibited", False) if p_data else False,
            })

    return {
        "date":        body.date,
        "players":     players,
        "new_players": new_players,
        "tee_times":   tee_times,
        "tee_start":   tee_start,
    }


# ---------------------------------------------------------------------------
# Players
# ---------------------------------------------------------------------------

class NewPlayerRequest(BaseModel):
    name: str
    handicap: int
    rollup_id: int


@app.post("/api/new-player")
async def new_player(body: NewPlayerRequest):
    try:
        await add_new_player(body.rollup_id, body.name, body.handicap)
    except Exception as e:
        raise HTTPException(500, f"Could not add player to database: {str(e)}")
    return {"ok": True, "name": body.name, "handicap": body.handicap}


class LookupRequest(BaseModel):
    name: str
    rollup_id: int = 1


@app.post("/api/lookup-player")
async def lookup_player(body: LookupRequest):
    try:
        all_players = await get_all_players(body.rollup_id)
    except Exception as e:
        raise HTTPException(500, f"Could not read player list: {str(e)}")
    for p in all_players:
        if p["name"].strip().lower() == body.name.strip().lower():
            return {"found": True, "name": p["name"], "handicap": p["handicap"]}
    return {"found": False, "name": body.name}


@app.get("/api/players")
async def get_players(rollup_id: int = Query(1)):
    try:
        players = await get_all_players(rollup_id)
    except Exception as e:
        raise HTTPException(500, f"Could not load players: {str(e)}")
    return {"players": [{"name": p["name"], "handicap": p["handicap"]} for p in players]}


@app.get("/api/players/detail")
async def get_players_detail(rollup_id: int = Query(1)):
    """Returns players with id, WHS fields and round count for the player manager."""
    try:
        players = await get_all_players_detail(rollup_id)
    except Exception as e:
        raise HTTPException(500, f"Could not load players: {str(e)}")
    return {"players": players}


class UpdateHandicapRequest(BaseModel):
    player_id: int
    handicap: int
    rollup_id: int


@app.post("/api/players/update-handicap")
async def update_handicap(body: UpdateHandicapRequest):
    if body.handicap < 0 or body.handicap > 54:
        raise HTTPException(400, "Handicap must be 0-54")
    try:
        await update_player_handicap(body.player_id, body.handicap)
    except Exception as e:
        raise HTTPException(500, f"Could not update handicap: {str(e)}")
    return {"ok": True}


class UpdateWhsIndexRequest(BaseModel):
    player_id: int
    whs_index: float
    rollup_id: int


@app.post("/api/players/update-whs-index")
async def update_whs_index(body: UpdateWhsIndexRequest):
    if body.whs_index < 0 or body.whs_index > 54:
        raise HTTPException(400, "WHS index must be 0-54")
    rounded = round(body.whs_index * 10) / 10
    try:
        await update_player_whs_index(body.player_id, rounded)
    except Exception as e:
        raise HTTPException(500, f"Could not update WHS index: {str(e)}")
    return {"ok": True}


class DeletePlayerRequest(BaseModel):
    player_id: int
    rollup_id: int


@app.post("/api/players/delete")
async def delete_player(body: DeletePlayerRequest):
    try:
        await remove_player(body.player_id, body.rollup_id)
    except Exception as e:
        raise HTTPException(500, f"Could not delete player: {str(e)}")
    return {"ok": True}


class SyncWhsRequest(BaseModel):
    ig_username: str
    ig_pin: str
    rollup_id: int


@app.post("/api/scrape-whs-indices")
async def scrape_whs_indices_endpoint(body: SyncWhsRequest):
    """
    Scrape WHS indices from bramleygolfclub.co.uk/hcaplist.php and
    update matching players in the specified rollup.
    """
    from backend.scraper import scrape_whs_indices

    try:
        result = await scrape_whs_indices(body.ig_username, body.ig_pin)
    except Exception as e:
        raise HTTPException(502, f"Could not scrape handicap list: {str(e)}")

    indices     = result["indices"]
    all_players = await get_all_players(body.rollup_id)

    updated   = []
    not_found = []
    for p in all_players:
        name  = p["name"].strip()
        lower = name.lower()
        # Try exact then case-insensitive
        idx = indices.get(name) or next(
            (v for k, v in indices.items() if k.lower() == lower), None
        )
        if idx is not None:
            await update_player_whs_index(p["id"], idx)
            updated.append({"name": name, "whs_index": idx})
        else:
            not_found.append(name)

    return {
        "ok":       True,
        "updated":  len(updated),
        "not_found": not_found,
        "details":  updated,
    }


# ---------------------------------------------------------------------------
# Courses and tees
# ---------------------------------------------------------------------------

@app.get("/api/courses")
async def courses():
    try:
        data = await get_all_courses()
    except Exception as e:
        raise HTTPException(500, f"Could not load courses: {str(e)}")
    return {"courses": data}


@app.get("/api/tees")
async def tees(course_id: int = Query(...)):
    try:
        data = await get_tees_for_course(course_id)
    except Exception as e:
        raise HTTPException(500, f"Could not load tees: {str(e)}")
    return {"tees": data}


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

class ScoreUpdate(BaseModel):
    date: str
    players: list[dict]
    team_mode: bool = False
    rollup_id: int = 1


@app.post("/api/autosave")
async def autosave(body: ScoreUpdate):
    try:
        settings = await get_rollup_settings(body.rollup_id)
    except Exception:
        settings = None

    whs_mode = (settings or {}).get("scoring_mode") == "whs"

    if whs_mode:
        prohibited = await get_prohibited_winners(body.rollup_id)
        whs_result = calculate_whs_handicaps(
            body.players, settings=settings, prohibited_names=prohibited
        )
        results = whs_result["players"]
        for r in results:
            r["adj_display"] = format_adjustment(r.get("adjustment"))
        team_scores = []
        if body.team_mode:
            team_scores = calculate_team_scores(results, settings=settings)
        return {
            "players":           results,
            "team_scores":       team_scores,
            "whs_mode":          True,
            "prohibited_winner": whs_result.get("prohibited_winner"),
            "error":             whs_result.get("error"),
        }
    else:
        results = calculate_new_handicaps(
            body.players, team_mode=body.team_mode, settings=settings
        )
        for r in results:
            r["adj_display"] = format_adjustment(r.get("adjustment"))
        team_scores = []
        if body.team_mode:
            team_scores = calculate_team_scores(results, settings=settings)
        return {"players": results, "team_scores": team_scores, "whs_mode": False}


@app.post("/api/save-round")
async def save_round(body: ScoreUpdate):
    try:
        settings = await get_rollup_settings(body.rollup_id)
    except Exception:
        settings = None

    whs_mode = (settings or {}).get("scoring_mode") == "whs"

    if whs_mode:
        prohibited = await get_prohibited_winners(body.rollup_id)
        whs_result = calculate_whs_handicaps(
            body.players, settings=settings, prohibited_names=prohibited
        )
        if whs_result.get("error"):
            raise HTTPException(400, whs_result["error"])

        results = whs_result["players"]
        try:
            await save_round_results(results, body.date, body.rollup_id, whs_mode=True)
        except Exception as e:
            raise HTTPException(500, f"Failed to save to database: {str(e)}")

        for r in results:
            r["adj_display"] = format_adjustment(r.get("adjustment"))
        team_scores = []
        if body.team_mode:
            team_scores = calculate_team_scores(results, settings=settings)
        return {"ok": True, "players": results, "date": body.date,
                "team_scores": team_scores, "whs_mode": True}
    else:
        results = calculate_new_handicaps(
            body.players, team_mode=body.team_mode, settings=settings
        )
        try:
            await save_round_results(results, body.date, body.rollup_id, whs_mode=False)
        except Exception as e:
            raise HTTPException(500, f"Failed to save to database: {str(e)}")

        for r in results:
            r["adj_display"] = format_adjustment(r.get("adjustment"))
        team_scores = []
        if body.team_mode:
            team_scores = calculate_team_scores(results, settings=settings)
        return {"ok": True, "players": results, "date": body.date,
                "team_scores": team_scores, "whs_mode": False}


# ---------------------------------------------------------------------------
# Results / history
# ---------------------------------------------------------------------------

@app.get("/api/last-round")
async def last_round(rollup_id: int = Query(1)):
    try:
        results = await get_last_round_results(rollup_id)
        date    = await get_last_round_date(rollup_id)
    except Exception as e:
        raise HTTPException(500, f"Could not load last round: {str(e)}")
    return {"players": results, "date": date}


@app.get("/api/round-dates")
async def round_dates(rollup_id: int = Query(1)):
    try:
        dates = await get_round_dates(rollup_id)
    except Exception as e:
        raise HTTPException(500, f"Could not load round dates: {str(e)}")
    return {"dates": dates}


@app.get("/api/round")
async def round_by_date(date: str = Query(...), rollup_id: int = Query(1)):
    try:
        results = await get_round_by_date(date, rollup_id)
    except Exception as e:
        raise HTTPException(500, f"Could not load round: {str(e)}")
    if not results:
        raise HTTPException(404, f"No results found for {date}")
    return {"players": results, "date": date}


@app.get("/api/player-history")
async def player_history(name: str = Query(...), rollup_id: int = Query(1)):
    try:
        history = await get_player_history(name, rollup_id)
    except Exception as e:
        raise HTTPException(500, f"Could not load player history: {str(e)}")
    if not history:
        raise HTTPException(404, f"No history found for {name}")
    return {
        "name": name,
        "rounds": [
            {"date": str(r["date"]), "score": r["score"], "new_handicap": r["new_handicap"]}
            for r in history
        ]
    }


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

@app.get("/api/settings")
async def get_settings(rollup_id: int = Query(...)):
    try:
        settings = await get_rollup_settings(rollup_id)
    except Exception as e:
        raise HTTPException(500, f"Could not load settings: {str(e)}")
    return settings


class RollupSettingsRequest(BaseModel):
    rollup_id: int
    display_name: str
    ig_search_term: str
    run_days: list[str]
    tee_interval_minutes: int = 8
    scoring_mode: str = "stableford"
    adjustment_table: list[dict]
    winner_bonus_enabled: bool = True
    winner_gap_penalty1: int = 0
    winner_gap_penalty2: int = 0
    whs_pct_1st: float = 0.0
    whs_pct_2nd: float = 0.0
    whs_pct_3rd: float = 0.0
    whs_winner_prohibition: bool = False
    course_id: int | None = None
    tee_id: int | None = None
    entry_fee: float = 0.00
    prize_places: int = 3
    prize_pct_1st: int = 60
    prize_pct_2nd: int = 30
    prize_pct_3rd: int = 10
    prize_pct_4th: int = 0
    tie_handling: str = "tournament"
    preferred_team_size: int = 4
    team_scoring_method: str = "best2"


@app.post("/api/settings")
async def post_settings(body: RollupSettingsRequest):
    total_pct = (body.prize_pct_1st + body.prize_pct_2nd +
                 body.prize_pct_3rd + body.prize_pct_4th)
    if total_pct != 100:
        raise HTTPException(400, f"Prize percentages must sum to 100 (currently {total_pct})")
    try:
        await save_rollup_settings(body.rollup_id, body.dict())
    except Exception as e:
        raise HTTPException(500, f"Could not save settings: {str(e)}")
    return {"ok": True}


# ---------------------------------------------------------------------------
# Credentials (legacy — credentials now session-only in localStorage)
# ---------------------------------------------------------------------------

@app.get("/api/credentials")
async def get_creds():
    try:
        creds = await get_credentials()
    except Exception as e:
        raise HTTPException(500, f"Could not load credentials: {str(e)}")
    return {
        "ig_username": creds["ig_username"],
        "ig_pin_set":  bool(creds["ig_pin"]),
    }


class CredentialsRequest(BaseModel):
    ig_username: str
    ig_pin: str = ""


@app.post("/api/credentials")
async def post_credentials(body: CredentialsRequest):
    if not body.ig_username:
        raise HTTPException(400, "Member ID is required")
    try:
        if body.ig_pin:
            await save_credentials(body.ig_username, body.ig_pin)
        else:
            existing = await get_credentials()
            await save_credentials(body.ig_username, existing["ig_pin"])
    except Exception as e:
        raise HTTPException(500, f"Could not save credentials: {str(e)}")
    return {"ok": True}
