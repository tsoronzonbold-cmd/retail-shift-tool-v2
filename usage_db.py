"""Lightweight usage analytics — SQLite event log.

Two tables:
  events      — one row per upload / recheck / configure_new
  mode_calls  — one row per Mode API query (latency + outcome)

All logging is best-effort: any failure here is swallowed so analytics
can never break a real upload.

The /admin/usage page is gated by ADMIN_TOKEN — without it set, the
route 404s.
"""

import os
import sqlite3
import time

_DB_PATH = os.environ.get(
    "USAGE_DB_PATH",
    os.path.join(os.path.dirname(__file__), "usage.db"),
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY,
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
  id INTEGER PRIMARY KEY,
  ts INTEGER NOT NULL,
  query_name TEXT NOT NULL,
  duration_ms INTEGER,
  success INTEGER,
  error_msg TEXT,
  result_rows INTEGER
);
CREATE INDEX IF NOT EXISTS idx_mode_ts ON mode_calls(ts);
"""


def _conn():
    c = sqlite3.connect(_DB_PATH)
    c.row_factory = sqlite3.Row
    c.executescript(_SCHEMA)
    return c


def log_event(event_type, *, company_id="", company_name="", filename="",
              rows_total=0, rows_matched=0, rows_unmatched=0,
              ai_fired=False, ai_status="", ai_filled_keys=None,
              mode_used=False, success=True, error_msg=""):
    try:
        with _conn() as c:
            c.execute(
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
            c.commit()
    except Exception:
        pass


def log_mode_call(query_name, duration_ms, success, error_msg="", result_rows=0):
    try:
        with _conn() as c:
            c.execute(
                "INSERT INTO mode_calls (ts, query_name, duration_ms, success,"
                " error_msg, result_rows) VALUES (?, ?, ?, ?, ?, ?)",
                (int(time.time()), query_name or "unknown", int(duration_ms or 0),
                 1 if success else 0, (error_msg or "")[:500], int(result_rows or 0)),
            )
            c.commit()
    except Exception:
        pass


def summary(days=30):
    cutoff = int(time.time()) - days * 86400
    with _conn() as c:
        h = c.execute(
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
            (cutoff,),
        ).fetchone()

        per_partner = c.execute(
            "SELECT company_id, company_name, COUNT(*) AS uploads,"
            " MAX(ts) AS last_ts, AVG(rows_total) AS avg_rows"
            " FROM events WHERE ts >= ? AND event_type='upload'"
            " GROUP BY company_id, company_name"
            " ORDER BY uploads DESC LIMIT 50",
            (cutoff,),
        ).fetchall()

        recent_errors = c.execute(
            "SELECT ts, company_name, event_type, error_msg"
            " FROM events WHERE (success=0 OR error_msg != '') AND ts >= ?"
            " ORDER BY ts DESC LIMIT 20",
            (cutoff,),
        ).fetchall()

        mode_per_query = c.execute(
            "SELECT query_name, COUNT(*) AS calls,"
            " SUM(CASE WHEN success=1 THEN 1 ELSE 0 END) AS successes,"
            " AVG(duration_ms) AS avg_ms"
            " FROM mode_calls WHERE ts >= ?"
            " GROUP BY query_name ORDER BY calls DESC",
            (cutoff,),
        ).fetchall()

        mode_rows = c.execute(
            "SELECT query_name, duration_ms FROM mode_calls"
            " WHERE ts >= ? AND success=1",
            (cutoff,),
        ).fetchall()

        daily = c.execute(
            "SELECT date(ts, 'unixepoch') AS day,"
            " SUM(CASE WHEN event_type='upload' THEN 1 ELSE 0 END) AS uploads,"
            " SUM(CASE WHEN success=0 THEN 1 ELSE 0 END) AS failures"
            " FROM events WHERE ts >= ?"
            " GROUP BY day ORDER BY day",
            (cutoff,),
        ).fetchall()

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
        "headline": dict(h) if h else {},
        "per_partner": [dict(r) for r in per_partner],
        "recent_errors": [dict(r) for r in recent_errors],
        "mode_per_query": [dict(r) for r in mode_per_query],
        "mode_pct": pct,
        "daily": [dict(r) for r in daily],
    }
