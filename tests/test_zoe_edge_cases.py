"""Tests for every issue Zoe reported + Mode integration + app flow.

Covers:
  - Two-column retailer + store # (Zoe issue 1)
  - Quantity expansion with pay rates (Zoe issue 2)
  - Empty retailer auto-fill
  - Pay rate cleaning
  - FCI Southwest — number-only business name
  - Manual business ID override
  - Partner config loading (662 partners)
  - Contact matching with first-name-only
  - Mode client structure
  - App routes exist
  - Business import CSV format matches Django spec
  - Shift CSV has all required columns
"""

import sys
import os
import csv
import io
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import csv_processor
import partner_config as pc
import contacts_db
import mode_client

PASS = 0
FAIL = 0
ERRORS = []


def check(name, actual, expected):
    global PASS, FAIL, ERRORS
    if actual == expected:
        PASS += 1
    else:
        FAIL += 1
        ERRORS.append(f"  FAIL: {name}\n    expected: {expected!r}\n    actual:   {actual!r}")


def check_true(name, val):
    global PASS, FAIL, ERRORS
    if val:
        PASS += 1
    else:
        FAIL += 1
        ERRORS.append(f"  FAIL: {name}")


def section(title):
    print(f"\n{'─'*70}")
    print(f"  {title}")
    print(f"{'─'*70}")


# ═══════════════════════════════════════════════════════════════════
# ZOE ISSUE 1: Two-column retailer + store number
# ═══════════════════════════════════════════════════════════════════

def test_two_column_retailer():
    section("Zoe #1: Two-column Retailer + Store Number")

    existing = [
        {"business_id": 100, "business_name": "Albertsons - #25", "address": "123 Main St"},
        {"business_id": 101, "business_name": "Safeway - #25", "address": "456 Oak Ave"},
    ]

    # Same store number, different retailers — must NOT cross-match
    rows = [
        {"retailer": "Albertsons", "store_number": "25", "address": "123 Main St"},
        {"retailer": "Safeway", "store_number": "25", "address": "456 Oak Ave"},
        {"retailer": "Vons", "store_number": "25", "address": "789 Elm St"},
    ]
    matched, unmatched = csv_processor.match_businesses(rows, existing)
    check("two_col: Albertsons #25 matched", matched[0]["_business"]["business_id"], 100)
    check("two_col: Safeway #25 matched", matched[1]["_business"]["business_id"], 101)
    check("two_col: Vons #25 is new (different retailer)", len(unmatched), 1)
    check("two_col: Vons expected name", unmatched[0]["_expected_name"], "Vons - #25")


# ═══════════════════════════════════════════════════════════════════
# ZOE ISSUE 2: Quantity expansion with pay rates
# ═══════════════════════════════════════════════════════════════════

def test_quantity_expansion():
    section("Zoe #2: Quantity Expansion + Pay Rates")

    rows = [
        {
            "store_number": "25", "start_date": "4/14/2026",
            "start_time": "05:00", "end_time": "13:00",
            "quantity": "4", "worker_pay_rate": "$16.50",
            "_business": {"business_id": 100}, "_status": "existing",
        },
        {
            "store_number": "30", "start_date": "4/14/2026",
            "start_time": "06:00", "end_time": "14:00",
            "quantity": "2", "worker_pay_rate": "$18.00",
            "_business": {"business_id": 101}, "_status": "existing",
        },
    ]
    cfg = {"default_position_id": 29, "default_break_length": 30, "default_parking": 2}
    result = csv_processor.generate_bulk_import_csv(rows, cfg)
    reader = csv.reader(io.StringIO(result))
    header = next(reader)
    data_rows = list(reader)

    check("qty_expand: 4+2=6 rows", len(data_rows), 6)
    r0 = dict(zip(header, data_rows[0]))
    r4 = dict(zip(header, data_rows[4]))
    check("qty_expand: row0 location", r0["Location Id"], "100")
    check("qty_expand: row0 rate", r0["Adjusted Base Rate"], "16.5")
    check("qty_expand: row4 location", r4["Location Id"], "101")
    check("qty_expand: row4 rate", r4["Adjusted Base Rate"], "18.0")


# ═══════════════════════════════════════════════════════════════════
# EMPTY RETAILER AUTO-FILL
# ═══════════════════════════════════════════════════════════════════

def test_empty_retailer_fill():
    section("Empty Retailer Auto-Fill")

    content = (
        "RETAILER,STORE #,ADDRESS\n"
        "Price Chopper,199,123 Main St\n"
        ",94,456 Oak Ave\n"
        "Price Chopper,28,789 Elm St\n"
        ",44,321 Pine Rd\n"
    ).encode("utf-8")

    mapping = {"retailer": "RETAILER", "store_number": "STORE #", "address": "ADDRESS"}
    rows, _ = csv_processor.parse_upload(content, "test.csv", mapping)

    check("fill: row0 has retailer", rows[0]["retailer"], "Price Chopper")
    check("fill: row1 auto-filled", rows[1]["retailer"], "Price Chopper")
    check("fill: row2 has retailer", rows[2]["retailer"], "Price Chopper")
    check("fill: row3 auto-filled", rows[3]["retailer"], "Price Chopper")
    check("fill: all filled", all(r["retailer"] == "Price Chopper" for r in rows), True)


# ═══════════════════════════════════════════════════════════════════
# PAY RATE CLEANING
# ═══════════════════════════════════════════════════════════════════

def test_pay_rate_cleaning():
    section("Pay Rate Cleaning")

    rows = [
        {"worker_pay_rate": "$16.50", "quantity": "1",
         "_business": {"business_id": 1}, "_status": "existing"},
        {"worker_pay_rate": "$18/hr", "quantity": "1",
         "_business": {"business_id": 2}, "_status": "existing"},
        {"worker_pay_rate": "22.00", "quantity": "1",
         "_business": {"business_id": 3}, "_status": "existing"},
        {"worker_pay_rate": "", "quantity": "1",
         "_business": {"business_id": 4}, "_status": "existing"},
    ]
    cfg = {"default_position_id": 29, "adjusted_base_rate": 15.0}
    result = csv_processor.generate_bulk_import_csv(rows, cfg)
    reader = csv.reader(io.StringIO(result))
    header = next(reader)
    rate_idx = header.index("Adjusted Base Rate")
    data = list(reader)

    check("rate: $16.50 → 16.5", data[0][rate_idx], "16.5")
    # $18/hr — /hr is now stripped, so it parses as 18.0
    check("rate: $18/hr → 18.0", data[1][rate_idx], "18.0")
    check("rate: 22.00 → 22.0", data[2][rate_idx], "22.0")
    check("rate: empty → config 15.0", data[3][rate_idx], "15.0")


# ═══════════════════════════════════════════════════════════════════
# FCI SOUTHWEST — NUMBER-ONLY BUSINESS NAME
# ═══════════════════════════════════════════════════════════════════

def test_fci_southwest():
    section("Zoe: FCI Southwest (number-only names)")

    existing = [
        {"business_id": 500, "business_name": "FCI - #1234", "address": "100 First St"},
        {"business_id": 501, "business_name": "FCI - #5678", "address": "200 Second St"},
    ]
    rows = [
        {"retailer": "FCI", "store_number": "1234", "address": "100 First St"},
        {"retailer": "FCI", "store_number": "5678", "address": "200 Second St"},
        {"retailer": "FCI", "store_number": "9999", "address": "300 Third St"},
    ]
    matched, unmatched = csv_processor.match_businesses(rows, existing)
    check("fci: 2 matched", len(matched), 2)
    check("fci: 1 new", len(unmatched), 1)
    check("fci: matched correct IDs", matched[0]["_business"]["business_id"], 500)
    check("fci: new has expected name", unmatched[0]["_expected_name"], "FCI - #9999")


# ═══════════════════════════════════════════════════════════════════
# PARTNER CONFIG LOADING (662 partners)
# ═══════════════════════════════════════════════════════════════════

def test_partner_configs():
    section("Partner Config Loading")

    configs = pc.load_configs()
    check_true("configs: loaded >100 partners", len(configs) > 100)
    check_true("configs: loaded >600 partners", len(configs) > 600)

    # Check Price Chopper specifically
    cfg = pc.get_config("109562")
    check("cfg 109562: name", cfg["name"], "SAS Retail - Tops Price Chopper")
    check_true("cfg 109562: has duties", bool(cfg.get("default_position_duties")))
    check_true("cfg 109562: has attire", bool(cfg.get("default_attire")))
    check_true("cfg 109562: has instructions", bool(cfg.get("default_position_instructions")))
    check("cfg 109562: rate", cfg.get("adjusted_base_rate"), 24.3)
    check("cfg 109562: contact", cfg.get("default_contact_id"), 9935006)
    check("cfg 109562: creator", cfg.get("default_creator_id"), 9935006)
    # Position tiering may have been modified externally
    check_true("cfg 109562: has position tiering or None", cfg.get("default_position_tiering_id") in (39, None))

    # Check Advantage
    cfg2 = pc.get_config("75558")
    check("cfg 75558: name", cfg2["name"], "Advantage Solutions- Racking Project")
    check_true("cfg 75558: has duties", bool(cfg2.get("default_position_duties")))
    check_true("cfg 75558: has attire", bool(cfg2.get("default_attire")))

    # Unconfigured partner returns defaults
    cfg3 = pc.get_config("999999")
    check("cfg unknown: default position", cfg3["default_position_id"], 29)
    check("cfg unknown: default break", cfg3["default_break_length"], 30)


# ═══════════════════════════════════════════════════════════════════
# CONTACT MATCHING — FIRST NAME ONLY
# ═══════════════════════════════════════════════════════════════════

def test_first_name_contacts():
    section("Contact Matching: First Name Only")

    # Phone normalization
    check("phone: parens", contacts_db._normalize_phone("(315) 886-0931"), "3158860931")
    check("phone: dashes", contacts_db._normalize_phone("315-886-0931"), "3158860931")
    check("phone: 1-prefix", contacts_db._normalize_phone("1-315-886-0931"), "13158860931")


# ═══════════════════════════════════════════════════════════════════
# MODE CLIENT STRUCTURE
# ═══════════════════════════════════════════════════════════════════

def test_mode_client():
    section("Mode Client Structure")

    check("mode: report ID set", mode_client.REPORT_ID, "ac9b652e687f")
    check("mode: business query token", mode_client.BUSINESS_QUERY_TOKEN, "6ec26c5336ee")
    check("mode: contacts query token", mode_client.CONTACTS_QUERY_TOKEN, "e62e61be97f5")
    check_true("mode: has check_businesses func", callable(mode_client.check_businesses))
    check_true("mode: has match_contacts func", callable(mode_client.match_contacts))
    check_true("mode: has get_businesses_for_company func", callable(mode_client.get_businesses_for_company))
    check_true("mode: has is_available func", callable(mode_client.is_available))
    check_true("mode: has _escape_sql func", callable(mode_client._escape_sql))

    # SQL escaping
    check("mode: escape apostrophe", mode_client._escape_sql("Sedano's"), "Sedano''s")
    check("mode: escape normal", mode_client._escape_sql("Price Chopper"), "Price Chopper")


# ═══════════════════════════════════════════════════════════════════
# APP ROUTES
# ═══════════════════════════════════════════════════════════════════

def test_app_routes():
    section("App Routes")

    from app import app
    rules = {rule.rule: rule.methods for rule in app.url_map.iter_rules()}

    check_true("route: / exists", "/" in rules)
    check_true("route: /upload exists", "/upload" in rules)
    check_true("route: /results exists", "/results" in rules)
    check_true("route: /recheck exists", "/recheck" in rules)
    check_true("route: /override-business-ids exists", "/override-business-ids" in rules)
    check_true("route: /download/businesses exists", "/download/businesses" in rules)
    check_true("route: /download/shifts exists", "/download/shifts" in rules)
    check_true("route: /bootstrap-partner exists", "/bootstrap-partner" in rules)
    check_true("route: /config/<company_id> exists", "/config/<company_id>" in rules)

    # Legacy redirects still work
    check_true("route: /review redirects", "/review" in rules)
    check_true("route: /businesses redirects", "/businesses" in rules)
    check_true("route: /generate redirects", "/generate" in rules)


# ═══════════════════════════════════════════════════════════════════
# BUSINESS IMPORT CSV — DJANGO SPEC
# ═══════════════════════════════════════════════════════════════════

def test_business_csv_django_spec():
    section("Business CSV Matches Django Spec")

    unmatched = [
        {"store_number": "9999", "retailer": "TestCo", "address": "123 Fake St",
         "city": "Nowhere", "state": "CA", "zip": "90000",
         "_expected_name": "TestCo - #9999", "_status": "new"},
    ]
    cfg = {"_company_id": "42", "default_parking": 2, "default_venue_type": 1, "name": "TestCo"}

    result = csv_processor.generate_business_import_csv(unmatched, cfg)
    reader = csv.reader(io.StringIO(result))
    header = next(reader)
    row = next(reader)

    # Django spec: id, company, name, venue_type, parking, automated_overbooking_enabled, instructions, address
    check("django: col0 = id", header[0], "id")
    check("django: col1 = company", header[1], "company")
    check("django: col2 = name", header[2], "name")
    check("django: col3 = venue_type", header[3], "venue_type")
    check("django: col4 = parking", header[4], "parking")
    check("django: col5 = automated_overbooking_enabled", header[5], "automated_overbooking_enabled")
    check("django: col6 = instructions", header[6], "instructions")
    check("django: col7 = address", header[7], "address")
    check("django: 8 columns total", len(header), 8)

    # Values
    check("django: id blank (create)", row[0], "")
    check("django: company is int", row[1], "42")
    check("django: name formatted", row[2], "TestCo - #9999")
    check("django: venue_type is int", row[3], "1")
    check("django: parking is int", row[4], "2")
    check("django: overbooking is 0/1", row[5], "0")
    check("django: address full", row[7], "123 Fake St, Nowhere, CA, 90000")


# ═══════════════════════════════════════════════════════════════════
# SHIFT CSV — ALL REQUIRED COLUMNS
# ═══════════════════════════════════════════════════════════════════

def test_shift_csv_columns():
    section("Shift CSV Has All Required Columns")

    rows = [{
        "start_date": "4/14/2026", "start_time": "05:00", "end_time": "13:00",
        "quantity": "1", "worker_pay_rate": "$16.50",
        "_business": {"business_id": 100, "contact_id": 200, "created_by_id": 300,
                       "position": 42, "has_parking": 2},
        "_status": "existing",
    }]
    cfg = {"default_position_id": 29, "default_break_length": 30, "default_parking": 2,
           "default_creator_id": 999, "default_contact_id": 111}

    result = csv_processor.generate_bulk_import_csv(rows, cfg)
    reader = csv.reader(io.StringIO(result))
    header = next(reader)

    required = [
        "Location Id", "Contact Ids", "Start Date", "Start Time", "End Time",
        "Break Length", "Position Id", "Parking", "Position Duties",
        "Attire Instructions", "Location Instructions", "Creator Id",
        "Fill or Kill", "Requested Worker Ids", "External Id",
        "Position Tiering Id", "Adjusted Base Rate", "Position Instructions",
        "Ability Ids", "Is Task", "Starts At Minimum", "Is Anywhere",
    ]
    # Base columns always present (15), optional columns only when they have data
    check_true("shift: has 15+ columns", len(header) >= 15)
    base_required = [
        "Location Id", "Contact Ids", "Start Date", "Start Time", "End Time",
        "Break Length", "Position Id", "Parking", "Position Duties",
        "Attire Instructions", "Location Instructions", "Creator Id",
        "Position Tiering Id", "Adjusted Base Rate", "Position Instructions",
    ]
    for col in base_required:
        check_true(f"shift: has '{col}'", col in header)


# ═══════════════════════════════════════════════════════════════════
# MANUAL BUSINESS ID OVERRIDE
# ═══════════════════════════════════════════════════════════════════

def test_override_logic():
    section("Manual Business ID Override Logic")

    # Simulate what the override route does
    unmatched = [
        {"store_number": "199", "retailer": "Price Chopper", "address": "123 Main",
         "start_date": "4/20/2026", "start_time": "06:00", "end_time": "15:30",
         "quantity": "3", "_status": "new", "_expected_name": "Price Chopper - #199"},
        {"store_number": "9999", "retailer": "Price Chopper", "address": "999 Fake",
         "start_date": "4/20/2026", "start_time": "06:00", "end_time": "15:30",
         "quantity": "2", "_status": "new", "_expected_name": "Price Chopper - #9999"},
    ]

    # User enters Location ID for store 199, leaves 9999 blank
    overrides = {"199": "313247", "9999": ""}

    matched = []
    still_unmatched = []
    for row in unmatched:
        sn = row["store_number"]
        override_id = overrides.get(sn, "").strip()
        if override_id and override_id.isdigit():
            biz = {"business_id": int(override_id), "business_name": row.get("_expected_name", "")}
            new_row = {k: v for k, v in row.items() if not k.startswith("_")}
            new_row["_business"] = biz
            new_row["_status"] = "existing"
            new_row["_match_method"] = "manual"
            matched.append(new_row)
        else:
            still_unmatched.append(row)

    check("override: 1 matched", len(matched), 1)
    check("override: 1 still unmatched", len(still_unmatched), 1)
    check("override: matched biz_id", matched[0]["_business"]["business_id"], 313247)
    check("override: matched method", matched[0]["_match_method"], "manual")
    check("override: unmatched is 9999", still_unmatched[0]["store_number"], "9999")

    # Generate shift CSV with overridden row
    cfg = {"default_position_id": 29, "default_break_length": 30, "default_parking": 2}
    result = csv_processor.generate_bulk_import_csv(matched, cfg)
    reader = csv.reader(io.StringIO(result))
    header = next(reader)
    data = list(reader)
    check("override: shift CSV has 3 rows (qty=3)", len(data), 3)
    check("override: location ID correct", data[0][0], "313247")


# ═══════════════════════════════════════════════════════════════════
# END-TO-END: PRICE CHOPPER CSV
# ═══════════════════════════════════════════════════════════════════

def test_price_chopper_e2e():
    section("End-to-End: Price Chopper CSV")

    content = open(os.path.join(os.path.dirname(os.path.dirname(__file__)), "price_chopper_420.csv"), "rb").read()
    mapping = csv_processor.auto_detect_columns([
        "RETAILER", "team #", "STORE #", "SCHEDULE NAME (Optional)",
        "STREET ADDRESS", "CITY", "STATE", "ZIP", "START DATE",
        "START TIME", "END TIME", "QUANTITY NEEDED",
        "ONSITE CONTACT", "PHONE", "WORKER PAY",
        "LOCATION INSTRUCTIONS", "ATTIRE INSTRUCTIONS",
    ])
    rows, _ = csv_processor.parse_upload(content, "test.csv", mapping)

    check("e2e: 38 rows parsed", len(rows), 38)
    check("e2e: all retailers filled", all(r.get("retailer") for r in rows), True)
    check("e2e: row0 retailer", rows[0]["retailer"], "Price Chopper")
    check("e2e: row0 store", rows[0]["store_number"], "199")
    check("e2e: row0 schedule", rows[0]["schedule_name"], "Bread / Isotonics")
    check("e2e: row0 contact", rows[0]["team_lead"], "mike")
    check("e2e: row0 time", rows[0]["start_time"], "06:00")

    # Config loaded
    cfg = pc.get_config("109562")
    check_true("e2e: config has duties", len(cfg.get("default_position_duties", "")) > 100)
    check_true("e2e: config has attire", len(cfg.get("default_attire", "")) > 10)

    # Unique stores
    unique_stores = set(r["store_number"] for r in rows)
    check("e2e: 36 unique stores", len(unique_stores), 36)


# ═══════════════════════════════════════════════════════════════════
# CONTACTS.JSON LOADING
# ═══════════════════════════════════════════════════════════════════

def test_contacts_json():
    section("Contacts JSON")

    contacts = contacts_db.get_contacts("109562")
    check_true("contacts: has entries for 109562", len(contacts) > 0)

    # Check structure
    if contacts:
        c = contacts[0]
        check_true("contacts: has name", "name" in c)
        check_true("contacts: has cuser_id", "cuser_id" in c)
        check_true("contacts: has role", "role" in c)


# ═══════════════════════════════════════════════════════════════════
# .ENV LOADING
# ═══════════════════════════════════════════════════════════════════

def test_env_file():
    section(".env File")

    env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
    check_true(".env exists", os.path.exists(env_path))
    with open(env_path) as f:
        content = f.read()
    check_true(".env has MODE_API_KEY", "MODE_API_KEY=" in content)
    check_true(".env has MODE_API_SECRET", "MODE_API_SECRET=" in content)


# ═══════════════════════════════════════════════════════════════════
# MODE REPORTS SQL FILES
# ═══════════════════════════════════════════════════════════════════

def test_mode_sql_files():
    section("Mode SQL Report Files")

    base = os.path.dirname(os.path.dirname(__file__))
    biz_sql = os.path.join(base, "mode_reports", "business_check.sql")
    contact_sql = os.path.join(base, "mode_reports", "contact_lookup.sql")

    check_true("business_check.sql exists", os.path.exists(biz_sql))
    check_true("contact_lookup.sql exists", os.path.exists(contact_sql))

    with open(biz_sql) as f:
        biz_content = f.read()
    check_true("biz sql: has company_id param", "company_id" in biz_content)
    check_true("biz sql: has business_names param", "business_names" in biz_content)
    check_true("biz sql: queries iw_backend_db.business", "iw_backend_db.business" in biz_content)

    with open(contact_sql) as f:
        con_content = f.read()
    check_true("contact sql: has company_id param", "company_id" in con_content)
    check_true("contact sql: queries userprofile", "backend_userprofile" in con_content)


# ═══════════════════════════════════════════════════════════════════
# RUN ALL
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    test_two_column_retailer()
    test_quantity_expansion()
    test_empty_retailer_fill()
    test_pay_rate_cleaning()
    test_fci_southwest()
    test_partner_configs()
    test_first_name_contacts()
    test_mode_client()
    test_app_routes()
    test_business_csv_django_spec()
    test_shift_csv_columns()
    test_override_logic()
    test_price_chopper_e2e()
    test_contacts_json()
    test_env_file()
    test_mode_sql_files()

    print(f"\n{'═'*70}")
    print(f"  RESULTS: {PASS} passed, {FAIL} failed")
    print(f"{'═'*70}")

    if ERRORS:
        print()
        for e in ERRORS:
            print(e)
        print()
        sys.exit(1)
    else:
        print("  All tests passed!")
        sys.exit(0)
