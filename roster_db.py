"""Roster lookup — resolves requested worker names to worker IDs.

Uses Christian's fuzzy matching strategy:
  1. Exact case-insensitive match
  2. Containment (either direction — handles nicknames)
  3. Levenshtein similarity >= 0.75 (handles typos)
"""

import json
import os

ROSTER_PATH = os.path.join(os.path.dirname(__file__), "partner_configs", "roster.json")
FUZZY_THRESHOLD = 0.75

_cache = None


def _load():
    global _cache
    if _cache is None:
        if os.path.exists(ROSTER_PATH):
            with open(ROSTER_PATH) as f:
                _cache = json.load(f)
        else:
            _cache = {}
    return _cache


def get_roster(company_id):
    """Return list of {name, worker_id} for a company."""
    return _load().get(str(company_id), [])


def _levenshtein(a, b):
    m, n = len(a), len(b)
    dp = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(m + 1):
        dp[i][0] = i
    for j in range(n + 1):
        dp[0][j] = j
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            dp[i][j] = (dp[i-1][j-1] if a[i-1] == b[j-1]
                        else 1 + min(dp[i-1][j], dp[i][j-1], dp[i-1][j-1]))
    return dp[m][n]


def _similarity(a, b):
    dist = _levenshtein(a, b)
    max_len = max(len(a), len(b))
    return 1 - dist / max_len if max_len > 0 else 1


def fuzzy_find_worker(name, roster):
    """Find best matching worker in roster for a given name.

    Returns {name, worker_id} or None.
    """
    if not name or not roster:
        return None

    req = name.lower().strip()

    # Pass 1: exact match
    for entry in roster:
        if entry["name"].lower().strip() == req:
            return entry

    # Pass 2: containment (handles "Andre" matching "Andre Arcane")
    for entry in roster:
        entry_lower = entry["name"].lower().strip()
        if req in entry_lower or entry_lower in req:
            return entry

    # Pass 3: Levenshtein similarity
    best_match = None
    best_score = 0
    for entry in roster:
        entry_lower = entry["name"].lower().strip()
        score = _similarity(req, entry_lower)
        if score > best_score:
            best_score = score
            best_match = entry

    return best_match if best_score >= FUZZY_THRESHOLD else None


def resolve_requested_workers(cell_value, company_id):
    """Parse comma-separated worker names and resolve to IDs.

    Returns comma-separated worker IDs string (for Django CSV).
    """
    if not cell_value:
        return ""

    raw = str(cell_value).strip()
    if not raw:
        return ""

    roster = get_roster(company_id)
    if not roster:
        return ""

    # Split by comma, handle names like "Nancy Werth, Raul Aguilera, Jessica Spencer"
    names = [n.strip() for n in raw.split(",") if n.strip()]
    matched_ids = []

    for name in names:
        match = fuzzy_find_worker(name, roster)
        if match:
            matched_ids.append(match["worker_id"])

    return ",".join(matched_ids)
