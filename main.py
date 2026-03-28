# MOTH's Rollup - main.py
# Updated: 2026-03-28 — migrated from Google Sheets to PostgreSQL (Neon + psycopg2)

import os
import asyncio
from typing import Optional

from fastapi import FastAPI, HTTPException
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
    save_round_results,
    add_new_player,
    get_last_round_results,
    get_last_round_date,
    get_player_history,
    get_round_dates,
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

IG_USERNAME = os.getenv("IG_USERNAME")
IG_PIN = os.getenv("IG_PIN")


@app.on_event("startup")
async def startup():
    await get_pool()


@app.on_event("shutdown")
async def shutdown():
    await close_pool()


@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/auth/status")
async def auth_status():
    try:
        last_date = await get_last_round_date()
    except Exception:
        last_date = None
    return {"last_round_date": last_date}


class LoadRequest(BaseModel):
    date: str


@app.post("/api/load-players")
async def load_players(body: LoadRequest):
    if not IG_USERNAME or not IG_PIN:
        raise HTTPException(500, "Intelligent Golf credentials not configured on server.")

    try:
        names = await scrape_players(IG_USERNAME, IG_PIN, body.date)
    except Exception as e:
        raise HTTPException(502, str(e))

    if not names:
        raise HTTPException(404, "No players found for this date.")

    try:
        all_players = await get_all_players()
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

    return {"date": body.date, "players": players, "new_players": new_players}


class NewPlayerRequest(BaseModel):
    name: str
    handicap: int


@app.post("/api/new-player")
async def new_player(body: NewPlayerRequest):
    try:
        await add_new_player(body.name, body.handicap)
    except Exception as e:
        raise HTTPException(500, f"Could not add player to database: {str(e)}")
    return {"ok": True, "name": body.name, "handicap": body.handicap}


class ScoreUpdate(BaseModel):
    date: str
    players: list[dict]
    team_mode: bool = False


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
        await save_round_results(results, body.date)
    except Exception as e:
        raise HTTPException(500, f"Failed to save to database: {str(e)}")
    for r in results:
        r["adj_display"] = format_adjustment(r.get("adjustment"))

    team_scores = []
    if body.team_mode:
        team_scores = calculate_team_scores(results)

    return {"ok": True, "players": results, "date": body.date, "team_scores": team_scores}


@app.get("/api/last-round")
async def last_round():
    try:
        results = await get_last_round_results()
        date = await get_last_round_date()
    except Exception as e:
        raise HTTPException(500, f"Could not load last round: {str(e)}")
    return {"players": results, "date": date}


class LookupRequest(BaseModel):
    name: str


@app.post("/api/lookup-player")
async def lookup_player(body: LookupRequest):
    try:
        all_players = await get_all_players()
    except Exception as e:
        raise HTTPException(500, f"Could not read player list: {str(e)}")

    for p in all_players:
        if p["name"].strip().lower() == body.name.strip().lower():
            return {"found": True, "name": p["name"], "handicap": p["handicap"]}

    return {"found": False, "name": body.name}


