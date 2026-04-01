# MOTH's Rollup - main.py
# v2 04/01: multi-rollup support — rollup_id passed through all endpoints

import os
import asyncio
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from fastapi import Request
from pydantic import BaseModel
from dotenv import load_dotenv

from backend.handicap import calculate_new_handicaps, calculate_team_scores, format_adjustment
from backend.db import (
    get_all_players,
    get_all_rollups,
    get_or_create_rollup,
    save_round_results,
    add_new_player,
    get_last_round_results,
    get_last_round_date,
    get_player_history,
    get_round_dates,
    get_round_by_date,
    get_pool,
    close_pool,
)
from backend.scraper import scrape_players

load_dotenv()

app = FastAPI(title="MOTH's Rollup")

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
    await get_pool()


@app.on_event("shutdown")
async def shutdown():
    await close_pool()


@app.get("/sw.js")
async def service_worker():
    from fastapi.responses import Response
    js = "self.addEventListener('fetch', function(event) {});"
    return Response(content=js, media_type="application/javascript")


@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/rollups")
async def rollups():
    """Return all available rollups."""
    try:
        data = await get_all_rollups()
    except Exception as e:
        raise HTTPException(500, f"Could not load rollups: {str(e)}")
    return {"rollups": data}


@app.get("/auth/status")
async def auth_status(rollup_id: int = Query(1)):
    try:
        last_date = await get_last_round_date(rollup_id)
    except Exception:
        last_date = None
    return {"last_round_date": last_date}


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

    names = scrape_result["names"]
    tee_times = scrape_result["tee_times"]

    if not names:
        raise HTTPException(404, "No players found for this date.")

    try:
        all_players = await get_all_players(body.rollup_id)
    except Exception as e:
        raise HTTPException(500, f"Could not read player list from database: {str(e)}")

    name_to_hc = {p["name"].strip().lower(): p["handicap"] for p in all_players}

    players = []
    new_players = []
    for name in names:
        hc = name_to_hc.get(name.strip().lower())
        if hc is None:
            new_players.append(name)
            players.append({"name": name, "handicap": None, "score": None, "team": None, "new_player": True})
        else:
            players.append({"name": name, "handicap": hc, "score": None, "team": None, "new_player": False})

    return {
        "date": body.date,
        "players": players,
        "new_players": new_players,
        "tee_times": tee_times,
    }


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


class ScoreUpdate(BaseModel):
    date: str
    players: list[dict]
    team_mode: bool = False
    rollup_id: int = 1


@app.post("/api/autosave")
async def autosave(body: ScoreUpdate):
    results = calculate_new_handicaps(body.players, team_mode=body.team_mode)
    for r in results:
        r["adj_display"] = format_adjustment(r.get("adjustment"))

    team_scores = []
    if body.team_mode:
        team_scores = calculate_team_scores(results)

    return {"players": results, "team_scores": team_scores}


@app.post("/api/save-round")
async def save_round(body: ScoreUpdate):
    results = calculate_new_handicaps(body.players, team_mode=body.team_mode)
    try:
        await save_round_results(results, body.date, body.rollup_id)
    except Exception as e:
        raise HTTPException(500, f"Failed to save to database: {str(e)}")
    for r in results:
        r["adj_display"] = format_adjustment(r.get("adjustment"))

    team_scores = []
    if body.team_mode:
        team_scores = calculate_team_scores(results)

    return {"ok": True, "players": results, "date": body.date, "team_scores": team_scores}


@app.get("/api/last-round")
async def last_round(rollup_id: int = Query(1)):
    try:
        results = await get_last_round_results(rollup_id)
        date = await get_last_round_date(rollup_id)
    except Exception as e:
        raise HTTPException(500, f"Could not load last round: {str(e)}")
    return {"players": results, "date": date}


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


class AddRollupRequest(BaseModel):
    name: str
    ig_search_term: str


@app.post("/api/rollups/add")
async def add_rollup(body: AddRollupRequest):
    """Add a new rollup or update its search term if it already exists."""
    try:
        rollup_id = await get_or_create_rollup(body.name, body.ig_search_term.upper())
    except Exception as e:
        raise HTTPException(500, f"Could not add rollup: {str(e)}")
    return {"ok": True, "id": rollup_id, "name": body.name, "ig_search_term": body.ig_search_term.upper()}
