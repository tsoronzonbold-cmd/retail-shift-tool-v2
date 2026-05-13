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


def resolve_requested_workers(cell_value, company_id, live_lookup=None):
    """Parse comma-separated worker names and resolve to IDs.

    Returns comma-separated worker IDs string (for Django CSV).

    Resolution order:
      1. live_lookup dict from Mode (if provided) — preferred, fresh data
      2. roster.json fuzzy match — fallback for names Mode didn't find,
         catches typos via Levenshtein

    Names that resolve via neither path are dropped silently here — the
    upload route fetches the unresolved set separately (resolve_workers_batch)
    so it can flash a user-facing warning.
    """
    if not cell_value:
        return ""

    raw = str(cell_value).strip()
    if not raw:
        return ""

    names = [n.strip() for n in raw.split(",") if n.strip()]
    if not names:
        return ""

    live_lookup = live_lookup or {}
    roster = get_roster(company_id)
    matched_ids = []

    for name in names:
        # Try live lookup first (Mode)
        wid = live_lookup.get(name.lower().strip())
        if wid:
            matched_ids.append(str(wid))
            continue
        # Fall back to local fuzzy match (handles typos / aliases)
        match = fuzzy_find_worker(name, roster)
        if match:
            matched_ids.append(str(match["worker_id"]))

    return ",".join(matched_ids)


def resolve_workers_batch(rows, company_id):
    """One-shot worker resolution for a whole upload.

    Scans all rows for unique requested-worker names, calls Mode once
    (batched), then falls back to local roster.json for anything Mode
    didn't resolve. Returns:

      live_lookup: dict {lowercase_name: worker_id} — pass to
                   resolve_requested_workers per row
      unresolved:  list of original-case names that NEITHER Mode nor
                   the local fuzzy match could find. Caller flashes
                   these to the user.
    """
    # Collect unique names across all rows
    seen = set()
    unique_names = []
    for row in rows:
        cell = row.get("requested_workers", "") or ""
        for n in str(cell).split(","):
            s = n.strip()
            if s and s.lower() not in seen:
                seen.add(s.lower())
                unique_names.append(s)

    if not unique_names:
        return {}, []

    # 1. Live lookup via Mode
    live_lookup = {}
    try:
        import mode_client
        if mode_client.worker_query_available():
            live_lookup = mode_client.match_workers(company_id, unique_names)
    except Exception as e:
        # Don't break upload on Mode failure — fuzzy fallback still works
        print(f"[roster_db] Mode worker_lookup failed: {e}")

    # 2. Identify names that still need fuzzy fallback
    roster = get_roster(company_id)
    unresolved = []
    for name in unique_names:
        if name.lower() in live_lookup:
            continue
        match = fuzzy_find_worker(name, roster) if roster else None
        if not match:
            unresolved.append(name)

    return live_lookup, unresolved
