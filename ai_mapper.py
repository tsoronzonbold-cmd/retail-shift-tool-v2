"""AI-powered column mapper for messy CSVs.

Uses OpenAI gpt-4o-mini for column detection (cheap, fast, structured
JSON output). Triggers when auto_detect_columns is missing critical
columns OR matches fewer than MIN_AUTO_DETECT total. Standard CSVs skip
this entirely — no AI cost or latency.

API key is read from OPENAI_API_KEY (or GPT_KEY as a fallback name,
since Replit's default secret editor sometimes uses that).
"""

import os
import json

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY") or os.environ.get("GPT_KEY", "")

# Cheap, fast model — column mapping is structured-output classification.
# gpt-4o-mini is ~5× cheaper than Claude Haiku and what Enkhjin's tool uses.
MODEL = "gpt-4o-mini"

# Minimum columns auto-detect must match before we skip AI.
MIN_AUTO_DETECT = 8

# Columns the pipeline cannot run without. If any of these are missing
# from auto-detect, call the AI even if total column count is high —
# regex won't catch every variant (e.g. "Club#" vs "Store#").
CRITICAL_COLUMNS = ["store_number", "retailer", "start_date", "start_time"]

# Our standard column names that the AI should map to
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
    return bool(OPENAI_API_KEY)


def ai_map_columns(df_columns, sample_rows, partner_name=""):
    """Use OpenAI to map CSV columns to our standard format.

    Returns a dict with:
      mapping: { std_key: csv_column_name, ... } — validated entries only
      confidence: "high" | "medium" | "low"
      reasoning: { std_key: short string explaining the choice }
      notes: optional free-form observation about the CSV

    On any failure returns the same shape with empty mapping/reasoning.
    """
    empty = {"mapping": {}, "confidence": "low", "reasoning": {}, "notes": ""}
    if not OPENAI_API_KEY:
        return empty

    # Build sample data preview — 10 rows (Enkhjin uses 10 too).
    sample_text = "COLUMNS: " + ", ".join(f'"{c}"' for c in df_columns) + "\n\n"
    sample_text += "SAMPLE ROWS:\n"
    for i, row in enumerate(sample_rows[:10]):
        row_vals = [f'{c}: "{row.get(c, "")}"' for c in df_columns[:15]]
        sample_text += f"  Row {i+1}: {', '.join(row_vals)}\n"

    user_prompt = f"""Partner: "{partner_name}"

{sample_text}

Standard fields:
{json.dumps(STANDARD_COLUMNS, indent=2)}

Rules:
- Each CSV column maps to AT MOST ONE standard field.
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
  "reasoning": {{ "<standard_field>": "<one-sentence reason>", ... }},
  "notes": "<optional observation about CSV quirks; empty string if none>"
}}"""

    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)
        response = client.chat.completions.create(
            model=MODEL,
            response_format={"type": "json_object"},
            temperature=0.1,
            messages=[
                {"role": "system", "content": "You map CSV columns for a retail shift scheduling tool. Return only valid JSON."},
                {"role": "user", "content": user_prompt},
            ],
        )
        text = response.choices[0].message.content or "{}"
        parsed = json.loads(text)

        # Validate the mapping — only keep entries where both keys are valid.
        col_set = set(df_columns)
        valid_mapping = {}
        for std_key, csv_col in (parsed.get("mapping") or {}).items():
            if std_key in STANDARD_COLUMNS and csv_col in col_set:
                valid_mapping[std_key] = csv_col

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
    """Call AI when auto-detect missed something important, then prefer
    the AI's mappings over the regex output on any overlap.

    Triggers on either:
      - Total auto-detected count < MIN_AUTO_DETECT, or
      - Any critical column (store_number, retailer, start_date,
        start_time) wasn't detected — these break the pipeline.

    Returns a dict:
      mapping     — final merged mapping (AI wins over regex on overlap)
      ai_keys     — keys AI returned (whether new or overriding)
      ai_added    — keys AI returned that regex missed
      ai_changed  — list of (key, old_csv_col, new_csv_col)
      confidence  — AI's overall confidence, or None
      reasoning   — AI's per-field reasoning
      notes       — AI's free-form observations
      status      — "ok" | "skipped" | "no_key" | "error"
      error       — short error message if status == "error" / "no_key"
    """
    missing_critical = [c for c in CRITICAL_COLUMNS if c not in auto_detected]
    low_count = len(auto_detected) < MIN_AUTO_DETECT

    if not missing_critical and not low_count:
        return {
            "mapping": auto_detected, "ai_keys": [], "ai_added": [],
            "ai_changed": [], "confidence": None, "reasoning": {},
            "notes": "", "status": "skipped", "error": "",
        }

    if not is_available():
        return {
            "mapping": auto_detected, "ai_keys": [], "ai_added": [],
            "ai_changed": [], "confidence": None, "reasoning": {},
            "notes": "", "status": "no_key",
            "error": "OPENAI_API_KEY (or GPT_KEY) not set on the server",
        }

    reason_log = []
    if missing_critical:
        reason_log.append(f"missing critical: {missing_critical}")
    if low_count:
        reason_log.append(f"only {len(auto_detected)} columns matched")
    print(f"[AI Mapper] Calling OpenAI ({MODEL}) — {'; '.join(reason_log)}")

    ai_result = ai_map_columns(df_columns, sample_rows, partner_name)
    ai_mapping = ai_result["mapping"]
    print(f"[AI Mapper] confidence={ai_result['confidence']} mapped={list(ai_mapping.keys())}")

    if not ai_mapping:
        return {
            "mapping": auto_detected, "ai_keys": [], "ai_added": [],
            "ai_changed": [], "confidence": None, "reasoning": {},
            "notes": ai_result.get("notes", ""), "status": "error",
            "error": "AI returned no mapping — likely API error "
                     "(check the AI status pill in the header).",
        }

    # AI wins over regex on overlap.
    merged = {**auto_detected, **ai_mapping}
    ai_keys = list(ai_mapping.keys())
    ai_added = [k for k in ai_keys if k not in auto_detected]
    ai_changed = [
        (k, auto_detected[k], ai_mapping[k])
        for k in ai_keys
        if k in auto_detected and auto_detected[k] != ai_mapping[k]
    ]

    return {
        "mapping": merged,
        "ai_keys": ai_keys,
        "ai_added": ai_added,
        "ai_changed": ai_changed,
        "confidence": ai_result["confidence"],
        "reasoning": ai_result["reasoning"],
        "notes": ai_result["notes"],
        "status": "ok",
        "error": "",
    }
