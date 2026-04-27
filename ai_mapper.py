"""AI-powered column mapper for messy CSVs.

Only triggers when auto_detect_columns matches < 5 columns, meaning the
CSV format is too unusual for regex patterns. Uses Claude to analyze
headers + sample rows and return a column mapping.

Standard CSVs skip this entirely — no AI cost or latency.
"""

import os
import json
import anthropic

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# Minimum columns auto-detect must match before we skip AI
MIN_AUTO_DETECT = 5

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

    Args:
        df_columns: list of column header strings from the CSV
        sample_rows: list of dicts (first 5 rows of data)
        partner_name: optional partner name for context

    Returns:
        dict mapping our standard keys → CSV column names
        e.g. {"store_number": "Store", "start_date": "Visit Date"}
    """
    if not ANTHROPIC_API_KEY:
        return {}

    # Build sample data preview
    sample_text = "COLUMNS: " + ", ".join(f'"{c}"' for c in df_columns) + "\n\n"
    sample_text += "SAMPLE ROWS:\n"
    for i, row in enumerate(sample_rows[:5]):
        row_vals = [f'{c}: "{row.get(c, "")}"' for c in df_columns[:15]]
        sample_text += f"  Row {i+1}: {', '.join(row_vals)}\n"

    prompt = f"""You are mapping CSV columns for a retail shift scheduling upload tool.

The partner "{partner_name}" sent a CSV with these columns and sample data:

{sample_text}

Map each CSV column to the closest standard field from this list:
{json.dumps(STANDARD_COLUMNS, indent=2)}

Rules:
- Only map columns you're confident about
- A CSV column can only map to ONE standard field
- "Store", "Store #", "Store Number" → store_number
- "Banner", "Retailer", "Chain" → retailer
- "Visit Date", "Date", "Start Date" → start_date
- "StartTime", "Start Time" → start_time
- "Request", "Quantity", "# of Workers", "Needs" → quantity
- "Team Lead", "Onsite Contact", "Supervisor", "Lead" → team_lead
- "Lead Contact Number", "Contact Phone", "Phone" → team_lead_phone
- "Name of Pros", "Requested Workers", "Any Requested" → requested_workers
- "District", "Area", "Region", "Team", "Team #" → booking_group
- Location instructions about where to meet → location_instructions
- Attire/dress code instructions → attire_instructions
- If a column doesn't match any standard field, skip it
- "Day", "WeekDay", "Week", "Daily Slot", "Set Size" — skip these, they're not standard fields

Return ONLY a JSON object mapping standard field names to CSV column names.
Example: {{"store_number": "Store", "start_date": "Visit Date", "quantity": "Request"}}

Return valid JSON only, no explanation."""

    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text.strip()

        # Parse JSON from response (handle markdown code blocks)
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        text = text.strip()

        mapping = json.loads(text)

        # Validate: only keep mappings where the CSV column actually exists
        valid = {}
        col_set = set(df_columns)
        for std_key, csv_col in mapping.items():
            if std_key in STANDARD_COLUMNS and csv_col in col_set:
                valid[std_key] = csv_col

        return valid

    except Exception as e:
        print(f"[AI Mapper] Error: {e}")
        return {}


def maybe_ai_map(df_columns, sample_rows, auto_detected, partner_name=""):
    """Only call AI if auto-detection matched fewer than MIN_AUTO_DETECT columns.

    Args:
        df_columns: list of column header strings
        sample_rows: list of dicts (first 5 rows)
        auto_detected: dict from auto_detect_columns()
        partner_name: optional partner name

    Returns:
        Final merged mapping (auto-detected + AI fills)
    """
    if len(auto_detected) >= MIN_AUTO_DETECT:
        return auto_detected

    if not is_available():
        return auto_detected

    print(f"[AI Mapper] Auto-detect only found {len(auto_detected)} columns, calling Claude...")
    ai_mapping = ai_map_columns(df_columns, sample_rows, partner_name)
    print(f"[AI Mapper] Claude mapped {len(ai_mapping)} columns: {list(ai_mapping.keys())}")

    # Merge: auto-detected takes priority, AI fills gaps
    merged = {**ai_mapping, **auto_detected}
    return merged
