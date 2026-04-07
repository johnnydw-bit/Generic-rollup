# Bramley Rollup - backend/handicap.py
# Updated: 2026-04-07 — all logic driven by rollup settings from DB

"""
Handicap calculation for Bramley Rollup.

All rules (adjustment table, winner penalty, team scoring method) are
passed in via a settings dict loaded from the database, so no hardcoded
values remain here.
"""

DEFAULT_ADJUSTMENT_TABLE = [
    {"max_score": 17,   "adjustment": 2},
    {"max_score": 29,   "adjustment": 1},
    {"max_score": 37,   "adjustment": 0},
    {"max_score": 42,   "adjustment": -1},
    {"max_score": None, "adjustment": -2},
]

DEFAULT_SETTINGS = {
    "adjustment_table":     DEFAULT_ADJUSTMENT_TABLE,
    "winner_bonus_enabled": True,
    "winner_gap_penalty1":  0,   # extra -1 if gap vs 2nd >= this (0 = off)
    "winner_gap_penalty2":  0,   # extra -2 if gap vs 2nd >= this (0 = off)
    "team_scoring_method":  "best2",
}


def get_adjustment(score: int, adjustment_table: list[dict]) -> int:
    """
    Look up the handicap adjustment for a given score using the provided table.
    Table is a list of {"max_score": int|None, "adjustment": int} rows,
    ordered low to high. The last row should have max_score=None (catch-all).
    """
    for row in adjustment_table:
        max_score = row.get("max_score")
        if max_score is None or score <= max_score:
            return row["adjustment"]
    # Fallback: return last row's adjustment
    return adjustment_table[-1]["adjustment"]


def calculate_new_handicaps(
    players: list[dict],
    team_mode: bool = False,
    settings: dict | None = None,
) -> list[dict]:
    """
    Given a list of player dicts with keys:
        name, handicap, score (int|None), team (int|None)

    Returns list with added keys:
        new_handicap, adjustment, winner

    Settings drives:
        - adjustment_table
        - winner_bonus_enabled  (individual mode only)
        - winner_gap_penalty1   (extra -1 if winner beats 2nd by >= N pts)
        - winner_gap_penalty2   (extra -2 if winner beats 2nd by >= N pts)
    """
    if settings is None:
        settings = DEFAULT_SETTINGS

    adj_table      = settings.get("adjustment_table", DEFAULT_ADJUSTMENT_TABLE)
    winner_bonus   = settings.get("winner_bonus_enabled", True)
    gap_penalty1   = settings.get("winner_gap_penalty1", 0)   # 0 = off
    gap_penalty2   = settings.get("winner_gap_penalty2", 0)   # 0 = off

    scored = [p for p in players if p.get("score") is not None]

    winner_name = None
    winner_extra = 0  # additional penalty beyond the base -1

    if scored and not team_mode and winner_bonus:
        scores_sorted = sorted([p["score"] for p in scored], reverse=True)
        max_score = scores_sorted[0]
        second_score = scores_sorted[1] if len(scores_sorted) > 1 else max_score
        gap = max_score - second_score

        for p in scored:
            if p["score"] == max_score:
                winner_name = p["name"]
                break

        # Gap penalties (applied on top of the base winner -1)
        if winner_name:
            if gap_penalty2 > 0 and gap >= gap_penalty2:
                winner_extra = -2
            elif gap_penalty1 > 0 and gap >= gap_penalty1:
                winner_extra = -1

    result = []
    for p in players:
        score = p.get("score")
        hc = p.get("handicap") or 0
        is_winner = (p["name"] == winner_name)

        if score is None:
            result.append({**p, "new_handicap": None, "adjustment": None, "winner": False})
            continue

        adj = get_adjustment(score, adj_table)

        if is_winner:
            adj -= 1          # base winner penalty
            adj += winner_extra  # gap penalty (negative, so this increases the cut)

        new_hc = max(0, hc + adj)

        result.append({
            **p,
            "adjustment":   adj,
            "new_handicap": new_hc,
            "winner":       is_winner,
        })

    return result


def calculate_team_scores(
    players: list[dict],
    settings: dict | None = None,
) -> list[dict]:
    """
    Calculate team rankings using the scoring method from settings.

    Methods:
        best1   — best 1 score per team
        best2   — best 2 scores per team (default)
        best3   — best 3 scores per team
        all     — sum of all scores
        worst2  — best of the worst 2 scores (useful for booby prize logic)
    """
    if settings is None:
        settings = DEFAULT_SETTINGS

    method = settings.get("team_scoring_method", "best2")

    teams: dict[int, list[int]] = {}
    for p in players:
        team = p.get("team")
        if team is None:
            continue
        if team not in teams:
            teams[team] = []
        if p.get("score") is not None:
            teams[team].append(p["score"])

    team_results = []
    for team_num, scores in teams.items():
        scores_desc = sorted(scores, reverse=True)

        if method == "best1":
            counted = scores_desc[:1]
        elif method == "best2":
            counted = scores_desc[:2]
        elif method == "best3":
            counted = scores_desc[:3]
        elif method == "all":
            counted = scores_desc
        elif method == "worst2":
            # Best of the two lowest scores
            scores_asc = sorted(scores)
            counted = sorted(scores_asc[:2], reverse=True)[:1]
        else:
            counted = scores_desc[:2]  # fallback to best2

        total = sum(counted)
        team_results.append({
            "team":           team_num,
            "total":          total,
            "scores_counted": len(counted),
        })

    team_results.sort(key=lambda x: x["total"], reverse=True)
    for i, t in enumerate(team_results):
        t["rank"] = i + 1

    return team_results


def format_adjustment(adj: int | None) -> str:
    if adj is None:
        return ""
    if adj > 0:
        return f"(+{adj})"
    elif adj < 0:
        return f"({adj})"
    else:
        return "(0)"
