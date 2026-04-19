"""CSV parsing and generation for retail shift bulk imports."""

import io
import csv
import re
import pandas as pd


def parse_break_length(value):
    """Parse a Break cell into minutes.

    Accepts:
      - integer/decimal (e.g. 30, 30.0) → 30
      - "30" → 30
      - "30 min" → 30
      - "2:00am-2:30am" → 30 (time range, derived from the span)
      - "12:00pm - 12:30pm" → 30
      - "12:00-12:30" → 30
      - NaN / "" → 30 (default)
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return 30
    # Raw integer/float
    if isinstance(value, (int, float)):
        return int(value)

    s = str(value).strip().lower()
    if not s:
        return 30

    # Pure integer string
    m = re.fullmatch(r"(\d+)\s*(?:min(?:utes?)?)?", s)
    if m:
        return int(m.group(1))

    # Time range: "2:00am-2:30am" or "12:00pm - 12:30pm" or "14:00-14:30"
    m = re.match(
        r"(\d{1,2}):?(\d{2})?\s*(am|pm)?\s*[-–]\s*(\d{1,2}):?(\d{2})?\s*(am|pm)?",
        s,
    )
    if m:
        h1, min1, ampm1, h2, min2, ampm2 = m.groups()
        def to_minutes(h, mn, ampm):
            h = int(h)
            mn = int(mn) if mn else 0
            if ampm == "pm" and h < 12:
                h += 12
            elif ampm == "am" and h == 12:
                h = 0
            return h * 60 + mn
        start = to_minutes(h1, min1, ampm1)
        end = to_minutes(h2, min2, ampm2)
        diff = end - start
        if diff < 0:
            diff += 24 * 60  # crossed midnight
        return diff if diff > 0 else 30

    return 30


def parse_time(value):
    """Parse a time value into HH:MM 24h string.

    Accepts datetime.time, string "21:00", "9:00 PM", pandas Timestamp, etc.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    # pandas.Timestamp or datetime
    if hasattr(value, "strftime"):
        return value.strftime("%H:%M")
    s = str(value).strip()
    if not s:
        return ""
    # Already HH:MM or HH:MM:SS
    m = re.match(r"^(\d{1,2}):(\d{2})(?::\d{2})?\s*(am|pm|AM|PM)?$", s)
    if m:
        h = int(m.group(1))
        mn = m.group(2)
        ampm = (m.group(3) or "").lower()
        if ampm == "pm" and h < 12:
            h += 12
        elif ampm == "am" and h == 12:
            h = 0
        return f"{h:02d}:{mn}"
    return s  # give up and return as-is


def parse_date(value):
    """Parse a date value into MM/DD/YYYY string."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    if hasattr(value, "strftime"):
        return value.strftime("%m/%d/%Y")
    return str(value).strip()


def parse_upload(file_content, filename, column_mapping):
    """Parse an uploaded CSV/Excel file using the partner's column mapping.

    Returns a list of dicts with normalized keys. Special handling for
    common real-world messy fields (break time ranges, phone formats,
    Excel datetime/time objects).
    """
    if filename.endswith((".xlsx", ".xls")):
        df = pd.read_excel(io.BytesIO(file_content))
    else:
        text = file_content.decode("utf-8-sig")
        df = pd.read_csv(io.StringIO(text))

    rows = []
    for _, row in df.iterrows():
        parsed = {}
        for norm_key, csv_col in column_mapping.items():
            if csv_col in df.columns:
                val = row[csv_col]
                if pd.isna(val):
                    parsed[norm_key] = ""
                elif norm_key == "break_length":
                    parsed[norm_key] = str(parse_break_length(val))
                elif norm_key in ("start_time", "end_time"):
                    parsed[norm_key] = parse_time(val)
                elif norm_key == "start_date":
                    parsed[norm_key] = parse_date(val)
                else:
                    parsed[norm_key] = str(val).strip()
            else:
                parsed[norm_key] = ""
        rows.append(parsed)

    # Auto-fill empty retailer from the most common non-empty retailer in the
    # file.  Ramses flagged this: Price Chopper CSVs leave some rows blank and
    # he has to fill them manually before uploading.
    if "retailer" in column_mapping:
        non_empty = [r["retailer"] for r in rows if r.get("retailer")]
        if non_empty:
            from collections import Counter
            most_common = Counter(non_empty).most_common(1)[0][0]
            for r in rows:
                if not r.get("retailer"):
                    r["retailer"] = most_common

    return rows, list(df.columns)


def auto_detect_columns(df_columns):
    """Try to auto-detect column mapping from header names.

    Returns a dict of normalized_key -> actual_column_name.
    """
    mapping = {}
    patterns = {
        "retailer": [r"^retailer$", r"^retailer\s*name$", r"^chain$", r"^banner$"],
        "store_number": [r"store\s*#", r"store\s*num", r"store\s*no", r"location\s*#"],
        "address": [r"^address", r"street"],
        "city": [r"^city"],
        "state": [r"^state"],
        "zip": [r"^zip", r"postal"],
        "start_date": [r"start\s*date", r"^date$"],
        "start_time": [r"start\s*time"],
        "end_time": [r"end\s*time"],
        "break_length": [r"^break$", r"^break\s*length", r"lunch"],
        "quantity": [r"^quantity", r"#\s*of\s*worker", r"\bqty\b", r"headcount", r"workers?\s*needed"],
        "requested_workers": [r"requested\s*workers", r"workers?\s*requested"],
        "position": [r"^position$", r"^role$", r"^job\s*title$"],
        "schedule_name": [r"schedule\s*name", r"^schedule$", r"shift\s*name"],
        "team_lead": [r"on.?site\s*contact$", r"team\s*lead(?!\s*phone|\s*email)", r"^contact$", r"supervisor"],
        "team_lead_phone": [r"on.?site\s*contact\s*phone", r"team\s*lead\s*phone", r"contact\s*phone", r"^phone$"],
        "team_lead_email": [r"on.?site\s*contact\s*email", r"team\s*lead\s*email", r"contact\s*email", r"^email"],
        "worker_pay_rate": [r"worker\s*pay\s*rate", r"pay\s*rate", r"hourly\s*rate", r"^rate$", r"worker\s*pay$"],
        "location_instructions": [r"location\s*instructions", r"^instructions$", r"check.?in\s*instructions"],
        "attire_instructions": [r"attire\s*instructions", r"^attire$", r"dress\s*code", r"uniform"],
        "booking_group": [r"^team\s*#", r"booking\s*group"],
    }

    for norm_key, pats in patterns.items():
        for col in df_columns:
            for pat in pats:
                if re.search(pat, col, re.IGNORECASE):
                    mapping[norm_key] = col
                    break
            if norm_key in mapping:
                break

    return mapping


def _normalize_street(addr):
    """Extract the first address component and normalize it for matching.

    "13 Polson St, Toronto, ON M5A 1A4, Canada" -> "13 polson st"
    """
    if not addr:
        return ""
    # Take first comma-separated part (street), lowercase, collapse whitespace
    street = addr.split(",")[0].strip().lower()
    street = re.sub(r"\s+", " ", street)
    # Drop common suffixes
    street = re.sub(r"\b(street|st|avenue|ave|road|rd|boulevard|blvd|drive|dr|lane|ln|parkway|pkwy|court|ct|way)\b\.?", "", street).strip()
    return street


def _extract_store_number(name):
    """Extract store number from a business name.

    Handles: "Albertsons - #25", "Price Chopper #94", "Safeway 1515",
    "Store 99", trailing digits like "Sedano's 8".
    """
    if not name:
        return None
    m = re.search(r"#\s*(\d+)", name)
    if m:
        return m.group(1)
    m = re.search(r"(?:store|loc(?:ation)?)\s*#?\s*(\d+)", name, re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r"\b(\d{2,})\s*$", name.strip())
    if m:
        return m.group(1)
    return None


def format_business_name(retailer, store_number):
    """Format a business name the way Django stores it: 'Brand - #StoreID'."""
    retailer = (retailer or "").strip()
    store_number = str(store_number or "").strip().lstrip("#")
    if retailer and store_number:
        return f"{retailer} - #{store_number}"
    if retailer:
        return retailer
    if store_number:
        return f"#{store_number}"
    return ""


def match_businesses(parsed_rows, existing_businesses, store_number_field="store_number"):
    """Match parsed rows to existing Redshift businesses by store # or street address.

    Uses Enkhjin-style matching: builds "Brand - #StoreID" names and matches
    against existing business names by store number, normalized name, and
    street address. Falls back to fuzzy matching to reduce false negatives.

    Returns (matched, unmatched).
    """
    matched = []
    unmatched = []

    biz_by_store = {}
    biz_by_name_lower = {}
    biz_by_street = {}
    for biz in existing_businesses:
        name = biz.get("business_name", "") or ""
        sn = _extract_store_number(name)
        if sn:
            biz_by_store[sn] = biz
        if name:
            biz_by_name_lower[name.strip().lower()] = biz
        street = _normalize_street(biz.get("address", ""))
        if street:
            biz_by_street.setdefault(street, biz)

    for row in parsed_rows:
        store_num = row.get(store_number_field, "").strip().lstrip("#")
        retailer = row.get("retailer", "").strip()
        row_street = _normalize_street(row.get("address", ""))

        biz = None
        match_method = None

        # 1. Formatted name match: "Albertsons - #25" (most precise — checks retailer + store #)
        if retailer and store_num:
            formatted = format_business_name(retailer, store_num).lower()
            if formatted in biz_by_name_lower:
                biz = biz_by_name_lower[formatted]
                match_method = "name_exact"

        # 2. Partial name match: store number in existing name AND retailer matches
        if not biz and store_num:
            for name_lower, b in biz_by_name_lower.items():
                existing_sn = _extract_store_number(name_lower)
                if existing_sn == store_num:
                    if not retailer or retailer.lower() in name_lower:
                        biz = b
                        match_method = "name_fuzzy"
                        break

        # 3. Store number only (when no retailer column, or retailer-aware checks missed)
        if not biz and store_num and store_num in biz_by_store:
            if not retailer:
                biz = biz_by_store[store_num]
                match_method = "store_number"

        # 4. Street address match
        if not biz and row_street and row_street in biz_by_street:
            biz = biz_by_street[row_street]
            match_method = "address"

        if biz:
            matched.append({
                **row,
                "_business": biz,
                "_status": "existing",
                "_match_method": match_method,
            })
        else:
            expected_name = format_business_name(retailer, store_num)
            unmatched.append({
                **row,
                "_status": "new",
                "_expected_name": expected_name,
            })

    return matched, unmatched


def generate_business_import_csv(new_businesses, partner_config):
    """Generate a CSV for the Django Business import at /backend/business/import/.

    Column spec from ai-playbook/refs/backend-business.md:
        id, company, name, venue_type, parking, automated_overbooking_enabled,
        instructions, address

    - id blank = create
    - address is one full string (not split into city/state/zip)
    - venue_type and parking are required integers
    - boolean fields use 1/0 not TRUE/FALSE
    - special_requirement_ids / certification_ids / training_ids /
      clock_out_task_ids do NOT belong here — they go through separate
      imports (ClockoutTask, BusinessMandatoryCertificate, etc.)
    """
    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow([
        "id", "company", "name", "venue_type", "parking",
        "automated_overbooking_enabled", "instructions", "address",
    ])

    company_id = partner_config.get("_company_id", "")
    venue_type = partner_config.get("default_venue_type", 1)  # 1 = generic
    parking = partner_config.get("default_parking", 2)
    instructions = partner_config.get("worker_instructions", "")
    overbooking = 1 if partner_config.get("automated_overbooking") else 0

    seen = set()
    for biz in new_businesses:
        store_num = biz.get("store_number", "")
        if store_num in seen:
            continue
        seen.add(store_num)

        retailer = biz.get("retailer", "").strip()
        expected_name = biz.get("_expected_name", "")
        if expected_name:
            biz_name = expected_name
        elif retailer and store_num:
            biz_name = format_business_name(retailer, store_num)
        else:
            name_prefix = partner_config.get("name", "").split(" - ")[0] if partner_config.get("name") else ""
            biz_name = f"{name_prefix} - #{store_num}" if store_num else biz.get("address", "")

        # Build full address string
        addr_parts = [
            biz.get("address", ""),
            biz.get("city", ""),
            biz.get("state", ""),
            biz.get("zip", ""),
        ]
        full_address = ", ".join(p for p in addr_parts if p)

        writer.writerow([
            "",  # id (blank = create)
            company_id,
            biz_name,
            venue_type,
            parking,
            overbooking,
            instructions,
            full_address,
        ])

    return output.getvalue()


def generate_tasks_csv(new_business_ids, partner_config):
    """Generate a CSV for the Django ClockoutTask import at /backend/clockouttask/import/.

    Column spec from ai-playbook/refs/backend-shift.md (ClockoutTask section):
        business, position, items, type, is_remove

    - One row per (business, position, type) combination
    - items is a pipe-separated string like "1|2|3" of ClockoutItem IDs
    - type is one of "clockin", "during", "clockout"
    - is_remove: 0 = add items, 1 = remove items
    """
    clock_in_ids = partner_config.get("clock_in_task_ids", [])
    during_ids = partner_config.get("during_task_ids", [])
    clock_out_ids = partner_config.get("clock_out_task_ids", [])
    position_ids = partner_config.get("task_position_ids", [])

    if not clock_in_ids and not during_ids and not clock_out_ids:
        return None

    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow(["business", "position", "items", "type", "is_remove"])

    for biz in new_business_ids:
        bid = biz.get("business_id", "")
        pos_id = position_ids[0] if position_ids else partner_config.get("default_position_id", 29)

        # One row per task type, items pipe-separated
        for task_type, ids in [
            ("clockin", clock_in_ids),
            ("during", during_ids),
            ("clockout", clock_out_ids),
        ]:
            if ids:
                writer.writerow([
                    bid,
                    pos_id,
                    "|".join(str(x) for x in ids),
                    task_type,
                    0,  # 0 = add (not remove)
                ])

    return output.getvalue()


def generate_bulk_import_csv(all_rows, partner_config, task_opts=None):
    """Generate the final bulk import CSV for Django BulkGigRequest.

    Column spec from ai-playbook/refs/backend-gig.md.
    Rows are expanded by quantity.

    task_opts: optional dict with {'is_task': bool, 'is_anywhere': bool} for
    flexible/anytime shifts. When is_task=True, sets Is Task=1 and
    Starts At Minimum to the row's Start Time.
    """
    task_opts = task_opts or {}
    is_task = task_opts.get("is_task", False)
    is_anywhere = task_opts.get("is_anywhere", False)

    output = io.StringIO()
    writer = csv.writer(output)

    # Column spec from ai-playbook/refs/backend-gig.md (BulkGigRequest section).
    # Goes to /backend/bulkgigrequest/add/ — uses human-readable column names.
    # Note: business-level certifications (BusinessMandatoryCertificate) and
    # trainings do not have admin CSV imports per the playbook — they have to
    # be added manually in Django admin. Special requirements are shift-level,
    # attached via the Ability Ids column below.
    writer.writerow([
        "Location Id",
        "Contact Ids",
        "Start Date",
        "Start Time",
        "End Time",
        "Break Length",
        "Position Id",
        "Parking",
        "Position Duties",
        "Attire Instructions",
        "Location Instructions",
        "Creator Id",
        "Fill or Kill",
        "Requested Worker Ids",
        "External Id",
        "Position Tiering Id",
        "Adjusted Base Rate",
        "Position Instructions",
        "Ability Ids",
        "Is Task",
        "Starts At Minimum",
        "Is Anywhere",
    ])

    for row in all_rows:
        biz = row.get("_business", {})
        
        # Quantity: prefer "quantity", fallback to "requested_workers"
        qty = row.get("quantity") or row.get("requested_workers") or 1
        qty = int(qty) if str(qty).strip() else 1

        # Determine values from business template or partner config defaults
        location_id = biz.get("business_id", "")
        contact_id = row.get("_contact_id") or biz.get("contact_id") or partner_config.get("default_contact_id", "")
        creator_id = biz.get("created_by_id") or partner_config.get("default_creator_id", "")
        position_id = biz.get("position") or partner_config.get("default_position_id", 29)
        position_tiering = biz.get("position_tiering_id") or partner_config.get("default_position_tiering_id") or ""
        parking = biz.get("has_parking") if biz.get("has_parking") is not None else partner_config.get("default_parking", 2)
        position_instructions = biz.get("instructions") or partner_config.get("default_position_instructions", "")
        # Attire: prefer CSV value, then business template, then partner config
        attire = row.get("attire_instructions") or biz.get("custom_attire_requirements") or partner_config.get("default_attire", "")
        # Location instructions: prefer CSV value, then partner config
        location_instructions = row.get("location_instructions") or partner_config.get("default_location_instructions", "")
        
        # Pay rate: use from CSV if provided, otherwise use partner config
        pay_rate = row.get("worker_pay_rate", "")
        if pay_rate:
            # Clean up pay rate (remove $, whitespace)
            pay_rate = str(pay_rate).replace("$", "").replace(",", "").strip()
            try:
                adjusted_base_rate = float(pay_rate) if pay_rate else ""
            except ValueError:
                adjusted_base_rate = partner_config.get("adjusted_base_rate") or ""
        else:
            adjusted_base_rate = partner_config.get("adjusted_base_rate") or ""

        # Break length: from row, or default
        break_len = row.get("break_length", "")
        if not break_len:
            break_len = partner_config.get("default_break_length", 30)

        # Position Duties is required and cannot be blank (per Django spec).
        position_duties = (
            position_instructions
            or partner_config.get("default_position_duties", "")
            or "See position instructions"
        )

        # Ability Ids (special requirements) — comma-separated GigRequirement IDs
        ability_ids = ",".join(
            str(x) for x in partner_config.get("special_requirement_ids", [])
        )

        # Task shift fields — Starts At Minimum must be HH:MM local time.
        # We reuse the row's Start Time as the earliest allowed start.
        starts_at_min = row.get("start_time", "") if is_task else ""

        # Expand by quantity
        for _ in range(qty):
            writer.writerow([
                location_id,
                contact_id,
                row.get("start_date", ""),
                row.get("start_time", ""),
                row.get("end_time", ""),
                break_len,
                position_id,
                parking,
                position_duties,
                attire or "See attire instructions in template",
                location_instructions or "See on-site contact for directions",
                creator_id,
                "",  # Fill or Kill
                "",  # Requested Worker Ids
                "",  # External Id
                position_tiering,
                adjusted_base_rate,
                position_instructions,
                ability_ids,
                1 if is_task else 0,
                starts_at_min,
                1 if (is_task and is_anywhere) else 0,
            ])

    return output.getvalue()
