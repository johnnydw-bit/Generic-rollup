# MOTH's Rollup - backend/handicap.py
# Updated: 2026-03-26

"""
Handicap calculation for MOTH's Rollup.

Adjustment table (Stableford score -> handicap change):
  0-17  -> +2
  18-29 -> +1
  30-37 -> 0
  38-42 -> -1
  43+   -> -2

Individual mode: round winner (highest score) gets an extra -1.
Team mode: winner bonus is suspended.
"""


def get_adjustment(score: int) -> int:
    if score <= 17:
        return 2
    elif score <= 29:
        return 1
    elif score <= 37:
        return 0
    elif score <= 42:
        return -1
    else:
        return -2


def calculate_new_handicaps(players: list[dict], team_mode: bool = False) -> list[dict]:
    """
    Given a list of player dicts with keys:
        name, handicap, score (int|None), team (int|None)

    Returns list with added keys:
        new_handicap, adjustment, winner

    In team_mode, the winner bonus (-1) is suspended.
    """
    scored = [p for p in players if p.get("score") is not None]

    winner_name = None
    if scored and not team_mode:
        max_score = max(p["score"] for p in scored)
        for p in scored:
            if p["score"] == max_score:
                winner_name = p["name"]
                break

    result = []
    for p in players:
        score = p.get("score")
        hc = p.get("handicap") or 0
        is_winner = (p["name"] == winner_name)

        if score is None:
            result.append({**p, "new_handicap": None, "adjustment": None, "winner": False})
            continue

        adj = get_adjustment(score)
        if is_winner:
            adj -= 1

        new_hc = max(0, hc + adj)

        result.append({
            **p,
            "adjustment": adj,
            "new_handicap": new_hc,
            "winner": is_winner,
        })

    return result


def calculate_team_scores(players: list[dict]) -> list[dict]:
    """
    Calculate team rankings based on top-2 scores per team.
    Returns list of {team, total, players, rank} sorted by total descending.
    """
    teams = {}
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
        scores_sorted = sorted(scores, reverse=True)
        top2 = scores_sorted[:2]
        total = sum(top2)
        team_results.append({
            "team": team_num,
            "total": total,
            "scores_counted": len(top2),
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
