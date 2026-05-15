"""Lightweight usage analytics — event log with dual backend.

Backend selection:
  - Postgres if DATABASE_URL is set (postgres:// or postgresql://). Used on
    Replit so analytics survive every redeploy — the container filesystem
    is ephemeral, so the previous SQLite approach wiped on each push.
  - SQLite otherwise (local dev, tests). Path overridable via USAGE_DB_PATH.

Two tables:
  events      — one row per upload / recheck / configure_new
  mode_calls  — one row per Mode API query (latency + outcome)

All logging is best-effort: any failure here is swallowed so analytics
can never break a real upload.

The /admin/usage page is gated by ADMIN_TOKEN — without it set, the
route 404s.
"""

import os
import time

DATABASE_URL = os.environ.get("DATABASE_URL", "")
_USE_PG = DATABASE_URL.startswith("postgres://") or DATABASE_URL.startswith("postgresql://")

# Always import sqlite — used either as the primary backend OR as a fallback
# when DATABASE_URL is set but the Postgres host is unreachable (Replit's
# internal "helium" hostname doesn't resolve from Deployments, for instance).
import sqlite3
_SQLITE_PATH = os.environ.get(
    "USAGE_DB_PATH",
    os.path.join(os.path.dirname(__file__), "usage.db"),
)

if _USE_PG:
    import psycopg2
    import psycopg2.extras
    _PK = "SERIAL PRIMARY KEY"
    _DATE_EXPR = "to_char(to_timestamp(ts), 'YYYY-MM-DD')"
else:
    _PK = "INTEGER PRIMARY KEY"
    _DATE_EXPR = "date(ts, 'unixepoch')"

_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS events (
  id {_PK},
  ts INTEGER NOT NULL,
  event_type TEXT NOT NULL,
  company_id TEXT,
  company_name TEXT,
  filename TEXT,
  rows_total INTEGER,
  rows_matched INTEGER,
  rows_unmatched INTEGER,
  ai_fired INTEGER,
  ai_status TEXT,
  ai_filled_keys TEXT,
  mode_used INTEGER,
  success INTEGER,
  error_msg TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
CREATE INDEX IF NOT EXISTS idx_events_company ON events(company_id);
CREATE TABLE IF NOT EXISTS mode_calls (
  id {_PK},
  ts INTEGER NOT NULL,
  query_name TEXT NOT NULL,
  duration_ms INTEGER,
  success INTEGER,
  error_msg TEXT,
  result_rows INTEGER
);
CREATE INDEX IF NOT EXISTS idx_mode_ts ON mode_calls(ts);
"""


def _init_schema():
    if _USE_PG:
        conn = psycopg2.connect(DATABASE_URL)
        try:
            with conn.cursor() as cur:
                cur.execute(_SCHEMA)
            conn.commit()
        finally:
            conn.close()
    # SQLite runs the schema in _q() since it's cheap and the file may
    # be created on first use.


# Tracks Postgres availability after first connection attempt. If PG is
# configured but unreachable (bad host, wrong creds), we fall back to SQLite
# silently rather than 500'ing every page. _PG_DEAD stays True for the rest
# of the process so we don't pay the connection-timeout cost per request.
_PG_DEAD = False


# Run once at import. For Postgres this opens a single connection to
# create tables; for SQLite it's deferred to first query.
if _USE_PG:
    try:
        _init_schema()
    except Exception as e:
        # PG configured but unreachable. Falling back to SQLite for the
        # rest of the process.
        print(f"[usage_db] Postgres init failed, falling back to SQLite: {e}")
        _PG_DEAD = True


def _exec(sql, params=(), fetch=False):
    """Run one query. Returns rows as list of dicts if fetch=True, else None.

    Handles placeholder differences (SQLite ? vs Postgres %s) by accepting
    ? in the input and rewriting for Postgres.
    """
    global _PG_DEAD
    if _USE_PG and not _PG_DEAD:
        try:
            pg_sql = sql.replace("?", "%s")
            conn = psycopg2.connect(DATABASE_URL, connect_timeout=5)
            try:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(pg_sql, params)
                    if fetch:
                        return [dict(r) for r in cur.fetchall()]
                conn.commit()
                return None
            finally:
                conn.close()
        except Exception as e:
            # Mark PG dead so subsequent calls fall through to SQLite without
            # paying another timeout. Logged once per process.
            print(f"[usage_db] Postgres unreachable, falling back to SQLite: {e}")
            _PG_DEAD = True
            # Fall through to SQLite below

    conn = sqlite3.connect(_SQLITE_PATH)
    try:
        conn.row_factory = sqlite3.Row
        conn.executescript(_SCHEMA if not _USE_PG else _SCHEMA.replace("SERIAL PRIMARY KEY", "INTEGER PRIMARY KEY"))
        cur = conn.execute(sql, params)
        if fetch:
            return [dict(r) for r in cur.fetchall()]
        conn.commit()
        return None
    finally:
        conn.close()


def log_event(event_type, *, company_id="", company_name="", filename="",
              rows_total=0, rows_matched=0, rows_unmatched=0,
              ai_fired=False, ai_status="", ai_filled_keys=None,
              mode_used=False, success=True, error_msg=""):
    try:
        _exec(
            "INSERT INTO events (ts, event_type, company_id, company_name, filename,"
            " rows_total, rows_matched, rows_unmatched, ai_fired, ai_status,"
            " ai_filled_keys, mode_used, success, error_msg)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (int(time.time()), event_type, str(company_id or ""), company_name or "",
             filename or "", int(rows_total or 0), int(rows_matched or 0),
             int(rows_unmatched or 0), 1 if ai_fired else 0, ai_status or "",
             ",".join(ai_filled_keys or []), 1 if mode_used else 0,
             1 if success else 0, (error_msg or "")[:500]),
        )
    except Exception:
        pass


def clear_failures():
    """Delete all event rows that recorded a failure.

    Used to prune the admin dashboard after we ship fixes for known
    failure modes — keeps "Recent failures" focused on new problems
    instead of historical noise. Doesn't touch successful events.

    Returns the number of rows deleted.
    """
    try:
        # Count first so we can report
        rows = _exec(
            "SELECT COUNT(*) AS n FROM events WHERE success=0 OR error_msg != ''",
            fetch=True,
        ) or []
        n = rows[0].get("n", 0) if rows else 0
        _exec("DELETE FROM events WHERE success=0 OR error_msg != ''")
        return n
    except Exception:
        return 0


def log_mode_call(query_name, duration_ms, success, error_msg="", result_rows=0):
    try:
        _exec(
            "INSERT INTO mode_calls (ts, query_name, duration_ms, success,"
            " error_msg, result_rows) VALUES (?, ?, ?, ?, ?, ?)",
            (int(time.time()), query_name or "unknown", int(duration_ms or 0),
             1 if success else 0, (error_msg or "")[:500], int(result_rows or 0)),
        )
    except Exception:
        pass


def summary(days=30):
    cutoff = int(time.time()) - days * 86400

    headline_rows = _exec(
        "SELECT COUNT(*) AS total,"
        " SUM(CASE WHEN event_type='upload' THEN 1 ELSE 0 END) AS uploads,"
        " SUM(CASE WHEN event_type='recheck' THEN 1 ELSE 0 END) AS rechecks,"
        " SUM(CASE WHEN success=1 THEN 1 ELSE 0 END) AS successes,"
        " SUM(CASE WHEN ai_fired=1 THEN 1 ELSE 0 END) AS ai_fires,"
        " SUM(CASE WHEN ai_fired=1 AND ai_status='ok' THEN 1 ELSE 0 END) AS ai_ok,"
        " COUNT(DISTINCT company_id) AS unique_partners,"
        " SUM(rows_total) AS total_rows,"
        " SUM(rows_unmatched) AS total_unmatched,"
        " SUM(CASE WHEN mode_used=1 THEN 1 ELSE 0 END) AS mode_resolved"
        " FROM events WHERE ts >= ?",
        (cutoff,), fetch=True,
    ) or []

    per_partner = _exec(
        "SELECT company_id, company_name, COUNT(*) AS uploads,"
        " MAX(ts) AS last_ts, AVG(rows_total) AS avg_rows"
        " FROM events WHERE ts >= ? AND event_type='upload'"
        " GROUP BY company_id, company_name"
        " ORDER BY uploads DESC LIMIT 50",
        (cutoff,), fetch=True,
    ) or []

    recent_errors = _exec(
        "SELECT ts, company_name, event_type, error_msg"
        " FROM events WHERE (success=0 OR error_msg != '') AND ts >= ?"
        " ORDER BY ts DESC LIMIT 20",
        (cutoff,), fetch=True,
    ) or []

    mode_per_query = _exec(
        "SELECT query_name, COUNT(*) AS calls,"
        " SUM(CASE WHEN success=1 THEN 1 ELSE 0 END) AS successes,"
        " AVG(duration_ms) AS avg_ms"
        " FROM mode_calls WHERE ts >= ?"
        " GROUP BY query_name ORDER BY calls DESC",
        (cutoff,), fetch=True,
    ) or []

    mode_rows = _exec(
        "SELECT query_name, duration_ms FROM mode_calls"
        " WHERE ts >= ? AND success=1",
        (cutoff,), fetch=True,
    ) or []

    # Pick the right date-bucket expression at call time, not import time,
    # so we use the SQLite form when PG is unreachable and we've fallen back.
    date_expr = (
        "to_char(to_timestamp(ts), 'YYYY-MM-DD')"
        if (_USE_PG and not _PG_DEAD)
        else "date(ts, 'unixepoch')"
    )
    daily = _exec(
        f"SELECT {date_expr} AS day,"
        " SUM(CASE WHEN event_type='upload' THEN 1 ELSE 0 END) AS uploads,"
        " SUM(CASE WHEN success=0 THEN 1 ELSE 0 END) AS failures"
        " FROM events WHERE ts >= ?"
        f" GROUP BY {date_expr} ORDER BY day",
        (cutoff,), fetch=True,
    ) or []

    by_q = {}
    for r in mode_rows:
        by_q.setdefault(r["query_name"], []).append(r["duration_ms"] or 0)
    pct = {}
    for q, vals in by_q.items():
        vals.sort()
        n = len(vals)
        pct[q] = {
            "p50": vals[n // 2],
            "p95": vals[min(int(n * 0.95), n - 1)],
        }

    return {
        "days": days,
        "headline": headline_rows[0] if headline_rows else {},
        "per_partner": per_partner,
        "recent_errors": recent_errors,
        "mode_per_query": mode_per_query,
        "mode_pct": pct,
        "daily": daily,
    }
