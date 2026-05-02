"""AI-powered column mapper for messy CSVs.

Triggers when auto_detect_columns is missing critical columns OR matches
fewer than MIN_AUTO_DETECT total. Uses Claude Haiku 4.5 (cheap, fast) to
analyze headers + sample rows and return a column mapping with confidence
and per-field reasoning. Standard CSVs skip this entirely — no AI cost
or latency.
"""

import os
import json
import anthropic

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# Cheap, fast model — column mapping is a structured-output task that
# doesn't need a frontier model. Sonnet was overkill and ~5× the cost.
MODEL = "claude-haiku-4-5-20251001"

# Minimum columns auto-detect must match before we skip AI.
MIN_AUTO_DETECT = 8

# Columns the pipeline cannot run without. If any of these are missing
# from auto-detect, call Claude even if total column count is high —
# regex won't catch every variant (e.g. "Club#" vs "Store#").
CRITICAL_COLUMNS = ["store_number", "retailer", "start_date", "start_time"]

# Our standard column names that Claude should map to
STANDARD_COLUMNS = [
    "retailer",
    "store_number",
    "address",
    "city",
    "state",
    "zip",
    "start_date",
    "start_time",
    "end_time",
    "break_length",
    "quantity",
    "requested_workers",
    "position",
    "schedule_name",
    "team_lead",
    "team_lead_phone",
    "team_lead_email",
    "worker_pay_rate",
    "location_instructions",
    "attire_instructions",
    "booking_group",
]


def is_available():
    return bool(ANTHROPIC_API_KEY)


def ai_map_columns(df_columns, sample_rows, partner_name=""):
    """Use Claude to map CSV columns to our standard format.

    Returns a dict with:
      mapping: { std_key: csv_column_name, ... } — validated entries only
      confidence: "high" | "medium" | "low"
      reasoning: { std_key: short string explaining the choice }
      notes: optional free-form observation about the CSV

    On any failure returns the same shape with empty mapping/reasoning.
    """
    empty = {"mapping": {}, "confidence": "low", "reasoning": {}, "notes": ""}
    if not ANTHROPIC_API_KEY:
        return empty

    # Build sample data preview — 10 rows for better signal (Enkhjin uses 10).
    sample_text = "COLUMNS: " + ", ".join(f'"{c}"' for c in df_columns) + "\n\n"
    sample_text += "SAMPLE ROWS:\n"
    for i, row in enumerate(sample_rows[:10]):
        row_vals = [f'{c}: "{row.get(c, "")}"' for c in df_columns[:15]]
        sample_text += f"  Row {i+1}: {', '.join(row_vals)}\n"

    prompt = f"""You map CSV columns for a retail shift scheduling tool.

Partner: "{partner_name}"

{sample_text}

Standard fields:
{json.dumps(STANDARD_COLUMNS, indent=2)}

Rules:
- Each CSV column can map to AT MOST ONE standard field.
- Only map columns you are confident about. Skip ambiguous ones.
- Common synonyms:
    Store / Store # / Store Number / Club / Club# → store_number
    Banner / Retailer / Chain → retailer
    Visit Date / Date / Start Date → start_date
    StartTime / Start Time → start_time
    Request / Quantity / # of Workers / Needs / HC Needed → quantity
    Team Lead / SLead / Onsite Contact / Lead / Supervisor → team_lead
      (prefer SLead / Lead / Team Lead over Supervisor when both exist)
    Lead Phone / Contact Phone / Phone Number → team_lead_phone
    Name of Pros / Requested Workers → requested_workers
    District / Region / Team / Team # / Area → booking_group
- Skip columns that don't match any standard field (Day, WeekDay, Daily Slot, Set Size, etc).

Return JSON with this exact shape:
{{
  "mapping": {{ "<standard_field>": "<csv_column_name>", ... }},
  "confidence": "high" | "medium" | "low",
  "reasoning": {{ "<standard_field>": "<one-sentence reason for the choice>", ... }},
  "notes": "<optional observation about CSV quirks; empty string if none>"
}}

Only return valid JSON. No prose, no markdown fences."""

    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        response = client.messages.create(
            model=MODEL,
            max_tokens=1500,
            temperature=0.1,
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text.strip()

        # Strip markdown fences if Claude wraps output anyway
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        text = text.strip()

        parsed = json.loads(text)

        # Validate the mapping — only keep entries where both keys are valid.
        col_set = set(df_columns)
        valid_mapping = {}
        for std_key, csv_col in (parsed.get("mapping") or {}).items():
            if std_key in STANDARD_COLUMNS and csv_col in col_set:
                valid_mapping[std_key] = csv_col

        # Keep reasoning only for entries that survived validation.
        reasoning = parsed.get("reasoning") or {}
        valid_reasoning = {k: reasoning[k] for k in valid_mapping if k in reasoning}

        confidence = (parsed.get("confidence") or "medium").lower()
        if confidence not in ("high", "medium", "low"):
            confidence = "medium"

        return {
            "mapping": valid_mapping,
            "confidence": confidence,
            "reasoning": valid_reasoning,
            "notes": (parsed.get("notes") or "").strip(),
        }

    except Exception as e:
        print(f"[AI Mapper] Error: {e}")
        return empty


def maybe_ai_map(df_columns, sample_rows, auto_detected, partner_name=""):
    """Call AI when auto-detect missed something important.

    Triggers on either:
      - Total auto-detected count < MIN_AUTO_DETECT, or
      - Any critical column (store_number, retailer, start_date, start_time)
        wasn't detected — these break the pipeline regardless of total count.
        Catches cases like 'Club#' that regex doesn't recognize.

    Returns a dict:
      {
        "mapping": final merged mapping (auto-detected fills win over AI),
        "ai_keys": list of keys that AI added (i.e. regex missed them),
        "confidence": Claude's confidence, or None if AI didn't run,
        "reasoning": Claude's per-field reasoning for keys it filled,
        "notes": Claude's optional notes,
      }
    """
    missing_critical = [c for c in CRITICAL_COLUMNS if c not in auto_detected]
    low_count = len(auto_detected) < MIN_AUTO_DETECT

    if not missing_critical and not low_count:
        return {
            "mapping": auto_detected,
            "ai_keys": [],
            "confidence": None,
            "reasoning": {},
            "notes": "",
        }

    if not is_available():
        return {
            "mapping": auto_detected,
            "ai_keys": [],
            "confidence": None,
            "reasoning": {},
            "notes": "",
        }

    reason_log = []
    if missing_critical:
        reason_log.append(f"missing critical: {missing_critical}")
    if low_count:
        reason_log.append(f"only {len(auto_detected)} columns matched")
    print(f"[AI Mapper] Calling Claude ({MODEL}) — {'; '.join(reason_log)}")

    ai_result = ai_map_columns(df_columns, sample_rows, partner_name)
    ai_mapping = ai_result["mapping"]
    print(f"[AI Mapper] confidence={ai_result['confidence']} mapped={list(ai_mapping.keys())}")

    # Regex (auto_detected) wins over AI on overlap. AI only fills GAPS.
    # If user trusts AI more on a particular key, the reasoning surfaced
    # to the UI lets them spot mismatches.
    merged = {**ai_mapping, **auto_detected}
    ai_keys = [k for k in ai_mapping if k not in auto_detected]
    relevant_reasoning = {k: ai_result["reasoning"].get(k, "") for k in ai_keys}

    return {
        "mapping": merged,
        "ai_keys": ai_keys,
        "confidence": ai_result["confidence"],
        "reasoning": relevant_reasoning,
        "notes": ai_result["notes"],
    }
