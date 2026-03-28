"""
Google Sheets helper for MOTH's Rollup.
Uses a service account for authentication - no user OAuth required.

Sheet structure:
  Sheet: "History"
    Col A: Player name
    Col B: Last round score
    Col C: Next round handicap
  Rows start at row 2 (row 1 is the header).
"""

import os
import json

from googleapiclient.discovery import build
from google.oauth2 import service_account


HISTORY_SHEET = "History"
COL_NAME = "A"
COL_SCORE = "B"
COL_HANDICAP = "C"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def _sheets_service():
    """Build a Google Sheets service using the service account credentials."""
    sa_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not sa_json:
        raise Exception("GOOGLE_SERVICE_ACCOUNT_JSON environment variable not set.")
    sa_info = json.loads(sa_json)
    creds = service_account.Credentials.from_service_account_info(
        sa_info, scopes=SCOPES
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def get_all_players(sheet_id: str, _token_info: dict = None) -> list[dict]:
    """
    Read all players from the History sheet.
    Returns list of {name, handicap, row}
    """
    service = _sheets_service()
    result = (
        service.spreadsheets()
        .values()
        .get(
            spreadsheetId=sheet_id,
            range=f"{HISTORY_SHEET}!A2:C",
        )
        .execute()
    )
    rows = result.get("values", [])
    players = []
    for i, row in enumerate(rows):
        name = row[0].strip() if len(row) > 0 else ""
        handicap_str = row[2].strip() if len(row) > 2 else ""
        if not name:
            continue
        try:
            handicap = int(float(handicap_str))
        except (ValueError, TypeError):
            handicap = 0
        players.append({
            "name": name,
            "handicap": handicap,
            "row": i + 2,
        })
    return players


def get_player_handicap(sheet_id: str, _token_info: dict, name: str) -> int | None:
    """Look up a single player's current handicap by name."""
    players = get_all_players(sheet_id)
    for p in players:
        if p["name"].strip().lower() == name.strip().lower():
            return p["handicap"]
    return None


def save_round_results(
    sheet_id: str,
    _token_info: dict,
    results: list[dict],
    date_str: str,
) -> None:
    """
    Save round results to the History sheet.
    Only updates rows for players who played (have a score).
    Also writes the round date into cell E1.
    """
    service = _sheets_service()
    all_players = get_all_players(sheet_id)
    name_to_row = {p["name"].strip().lower(): p["row"] for p in all_players}

    data = []
    for r in results:
        if r.get("score") is None or r.get("new_handicap") is None:
            continue
        key = r["name"].strip().lower()
        row_num = name_to_row.get(key)
        if row_num is None:
            continue
        data.append({
            "range": f"{HISTORY_SHEET}!B{row_num}:C{row_num}",
            "values": [[r["score"], r["new_handicap"]]],
        })

    if not data:
        return

    # Write last round date to E1
    data.append({
        "range": f"{HISTORY_SHEET}!E1",
        "values": [[f"Last round: {date_str}"]],
    })

    service.spreadsheets().values().batchUpdate(
        spreadsheetId=sheet_id,
        body={
            "valueInputOption": "RAW",
            "data": data,
        },
    ).execute()


def add_new_player(
    sheet_id: str,
    _token_info: dict,
    name: str,
    handicap: int,
) -> None:
    """
    Add a new player to the History sheet in alphabetical order.
    """
    service = _sheets_service()
    all_players = get_all_players(sheet_id)

    # Find insertion point (alphabetical)
    insert_before_row = None
    for p in all_players:
        if name.strip().lower() < p["name"].strip().lower():
            insert_before_row = p["row"]
            break

    if insert_before_row is None:
        insert_before_row = (all_players[-1]["row"] + 1) if all_players else 2

    sheet_metadata = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    history_sheet_id = None
    for s in sheet_metadata["sheets"]:
        if s["properties"]["title"] == HISTORY_SHEET:
            history_sheet_id = s["properties"]["sheetId"]
            break

    if history_sheet_id is None:
        raise Exception("Could not find History sheet")

    # Insert a blank row
    service.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id,
        body={
            "requests": [{
                "insertDimension": {
                    "range": {
                        "sheetId": history_sheet_id,
                        "dimension": "ROWS",
                        "startIndex": insert_before_row - 1,
                        "endIndex": insert_before_row,
                    },
                    "inheritFromBefore": False,
                }
            }]
        },
    ).execute()

    # Write player data
    service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=f"{HISTORY_SHEET}!A{insert_before_row}:C{insert_before_row}",
        valueInputOption="RAW",
        body={"values": [[name, "", handicap]]},
    ).execute()


def get_last_round_results(sheet_id: str, _token_info: dict = None) -> list[dict]:
    """
    Read last round scores from History, sorted by score descending.
    """
    service = _sheets_service()
    result = (
        service.spreadsheets()
        .values()
        .get(
            spreadsheetId=sheet_id,
            range=f"{HISTORY_SHEET}!A2:C",
        )
        .execute()
    )
    rows = result.get("values", [])
    scored = []
    for row in rows:
        name = row[0].strip() if len(row) > 0 else ""
        score_str = row[1].strip() if len(row) > 1 else ""
        hc_str = row[2].strip() if len(row) > 2 else ""
        if not name or not score_str:
            continue
        try:
            score = int(float(score_str))
            hc = int(float(hc_str)) if hc_str else 0
        except (ValueError, TypeError):
            continue
        scored.append({"name": name, "score": score, "new_handicap": hc})

    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored


def get_last_round_date(sheet_id: str, _token_info: dict = None) -> str:
    """Read the last round date from E1."""
    service = _sheets_service()
    result = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=sheet_id, range=f"{HISTORY_SHEET}!E1")
        .execute()
    )
    rows = result.get("values", [])
    if rows and rows[0]:
        return rows[0][0]
    return "No previous round"
