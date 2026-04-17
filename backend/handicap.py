# Bramley Rollup - backend/handicap.py
# Updated: 2026-04-17 — WHS position adjustment mode added

"""
Handicap calculation for Bramley Rollup.

Two modes, selected per rollup in settings:

STABLEFORD MODE (default)
  - Integer handicap
  - Adjustment table maps score bands to +/- changes
  - Optional winner bonus (-1) and gap penalties
  - Team mode suspends winner bonus

WHS MODE
  - Decimal WHS index (one decimal place)
  - Top 3 finishers receive a configurable % reduction to their index
  - Reduction is temporary — stored as whs_index_next_round, used next round, then reverted
  - Optional winner prohibition — hard block if enabled
  - Places with 0% reduction are skipped
"""

from decimal import Decimal, ROUND_HALF_UP

DEFAULT_ADJUSTMENT_TABLE = [
    {"max_score": 17,   "adjustment": 2},
    {"max_score": 29,   "adjustment": 1},
    {"max_score": 37,   "adjustment": 0},
    {"max_score": 42,   "adjustment": -1},
    {"max_score": None, "adjustment": -2},
]

DEFAULT_SETTINGS = {
    "scoring_mode":             "stableford",
    "adjustment_table":         DEFAULT_ADJUSTMENT_TABLE,
    "winner_bonus_enabled":     True,
    "winner_gap_penalty1":      0,
    "winner_gap_penalty2":      0,
    "whs_pct_1st":              0.0,
    "whs_pct_2nd":              0.0,
    "whs_pct_3rd":              0.0,
    "whs_winner_prohibition":   False,
    "team_scoring_method":      "best2",
}


# ─────────────────────────────────────────────────────────────────────────────
# Stableford mode
# ─────────────────────────────────────────────────────────────────────────────

def get_adjustment(score: int, adjustment_table: list[dict]) -> int:
    """Look up handicap adjustment for a score using the configured table."""
    for row in adjustment_table:
        max_score = row.get("max_score")
        if max_score is None or score <= max_score:
            return row["adjustment"]
    return adjustment_table[-1]["adjustment"]


def calculate_new_handicaps(
    players: list[dict],
    team_mode: bool = False,
    settings: dict | None = None,
) -> list[dict]:
    """
    Stableford mode handicap calculation.

    Player dict keys expected: name, handicap, score (int|None), team (int|None)
    Returns players with added keys: new_handicap, adjustment, winner
    """
    if settings is None:
        settings = DEFAULT_SETTINGS

    adj_table    = settings.get("adjustment_table", DEFAULT_ADJUSTMENT_TABLE)
    winner_bonus = settings.get("winner_bonus_enabled", True)
    gap_penalty1 = settings.get("winner_gap_penalty1", 0)
    gap_penalty2 = settings.get("winner_gap_penalty2", 0)

    scored = [p for p in players if p.get("score") is not None]

    winner_name  = None
    winner_extra = 0

    if scored and not team_mode and winner_bonus:
        scores_sorted = sorted([p["score"] for p in scored], reverse=True)
        max_score     = scores_sorted[0]
        second_score  = scores_sorted[1] if len(scores_sorted) > 1 else max_score
        gap           = max_score - second_score

        for p in scored:
            if p["score"] == max_score:
                winner_name = p["name"]
                break

        if winner_name:
            if gap_penalty2 > 0 and gap >= gap_penalty2:
                winner_extra = -2
            elif gap_penalty1 > 0 and gap >= gap_penalty1:
                winner_extra = -1

    result = []
    for p in players:
        score = p.get("score")
        hc    = p.get("handicap") or 0
        is_winner = (p["name"] == winner_name)

        if score is None:
            result.append({**p, "new_handicap": None, "adjustment": None, "winner": False})
            continue

        adj = get_adjustment(score, adj_table)
        if is_winner:
            adj -= 1
            adj += winner_extra

        new_hc = max(0, hc + adj)
        result.append({**p, "adjustment": adj, "new_handicap": new_hc, "winner": is_winner})

    return result


# ─────────────────────────────────────────────────────────────────────────────
# WHS mode
# ─────────────────────────────────────────────────────────────────────────────

def _round_whs(value: float) -> float:
    """Round to 1 decimal place using standard rounding."""
    return float(Decimal(str(value)).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP))


def calculate_whs_handicaps(
    players: list[dict],
    settings: dict | None = None,
    prohibited_names: list[str] | None = None,
) -> dict:
    """
    WHS position adjustment mode.

    Player dict keys expected:
        name, whs_index (float|None), whs_index_next_round (float|None),
        score (int|None), winner_prohibited (bool)

    Returns:
        {
            "players": [...],
            "prohibited_winner": str|None,
            "error": str|None,
        }

    Added player keys:
        whs_index_used      float   — index used for this round
        new_whs_index       float|None — reduced index for next round
        new_handicap        int     — integer version of whs_index_used (for grid display)
        adjustment          str     — description e.g. "-15% -> 12.1"
        winner              bool
        position            int|None
    """
    if settings is None:
        settings = DEFAULT_SETTINGS

    pct_map = {
        1: float(settings.get("whs_pct_1st", 0)),
        2: float(settings.get("whs_pct_2nd", 0)),
        3: float(settings.get("whs_pct_3rd", 0)),
    }
    winner_prohibition = settings.get("whs_winner_prohibition", False)
    if prohibited_names is None:
        prohibited_names = []

    scored = sorted(
        [p for p in players if p.get("score") is not None],
        key=lambda p: p["score"],
        reverse=True
    )

    # Assign positions (ties share position)
    positions = {}
    pos = 1
    for i, p in enumerate(scored):
        if i > 0 and p["score"] == scored[i-1]["score"]:
            positions[p["name"]] = positions[scored[i-1]["name"]]
        else:
            positions[p["name"]] = pos
        pos += 1

    # Check winner prohibition
    prohibited_winner = None
    if winner_prohibition and scored:
        winner_name = scored[0]["name"]
        if winner_name in prohibited_names:
            return {
                "players":          players,
                "prohibited_winner": winner_name,
                "error": f"{winner_name} won the last round and cannot win this round. "
                         f"Please review scores before saving.",
            }

    result = []
    for p in players:
        score = p.get("score")

        # Use next-round temp index if set, else permanent index, else fall back to integer HC
        whs_index_used = p.get("whs_index_next_round") or p.get("whs_index")
        if whs_index_used is None:
            whs_index_used = float(p.get("handicap") or 0)
        whs_index_used = float(whs_index_used)

        position      = positions.get(p["name"])
        new_whs_index = None
        adj_display   = "-"
        is_winner     = (position == 1 and score is not None)

        if score is not None and position is not None and position in pct_map:
            pct = pct_map[position]
            if pct > 0:
                reduced       = whs_index_used * (1 - pct / 100)
                new_whs_index = _round_whs(reduced)
                adj_display   = f"-{pct:.0f}% -> {new_whs_index}"

        result.append({
            **p,
            "whs_index_used":    whs_index_used,
            "new_whs_index":     new_whs_index,
            "new_handicap":      int(round(whs_index_used)),
            "adjustment":        adj_display,
            "winner":            is_winner,
            "position":          position,
            "winner_prohibited": p["name"] in prohibited_names,
        })

    return {
        "players":           result,
        "prohibited_winner": prohibited_winner,
        "error":             None,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Team scoring — works for both modes
# ─────────────────────────────────────────────────────────────────────────────

def calculate_team_scores(
    players: list[dict],
    settings: dict | None = None,
) -> list[dict]:
    """
    Calculate team rankings using the scoring method from settings.
    Methods: best1, best2, best3, all, worst2
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
            scores_asc = sorted(scores)
            counted = sorted(scores_asc[:2], reverse=True)[:1]
        else:
            counted = scores_desc[:2]

        team_results.append({
            "team":           team_num,
            "total":          sum(counted),
            "scores_counted": len(counted),
        })

    team_results.sort(key=lambda x: x["total"], reverse=True)
    for i, t in enumerate(team_results):
        t["rank"] = i + 1

    return team_results


# ─────────────────────────────────────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────────────────────────────────────

def format_adjustment(adj) -> str:
    """Format adjustment for display — handles int (stableford) or str (WHS)."""
    if adj is None:
        return ""
    if isinstance(adj, str):
        return f"({adj})"
    if adj > 0:
        return f"(+{adj})"
    elif adj < 0:
        return f"({adj})"
    else:
        return "(0)"
