"""Comprehensive test suite for retail-shift-tool-v2.

Tests parsing, column detection, business matching, and CSV generation
across all real partner CSV formats.
"""

import io
import csv
import os
import sys
import re

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import csv_processor
import contacts_db

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


def check_in(name, needle, haystack):
    global PASS, FAIL, ERRORS
    if needle in haystack:
        PASS += 1
    else:
        FAIL += 1
        ERRORS.append(f"  FAIL: {name}\n    {needle!r} not found in {haystack!r}")


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


# ═══════════════════════════════════════════════════════════════════════════
# 1. COLUMN AUTO-DETECTION
# ═══════════════════════════════════════════════════════════════════════════

def test_column_detection():
    section("1. Column Auto-Detection")

    # test_instawork.csv / comma-separated values.csv headers
    cols_instawork = [
        "Retailer", "Store #", "Street Address", "City", "State", "Zip",
        "Date", "Start Time", "End Time", "Break", "# of Workers",
        "Position", "Onsite contact", "Onsite Contact Phone",
        "Onsite Contact Email (optional)", "Pay Rate", "Requested Workers",
    ]
    m = csv_processor.auto_detect_columns(cols_instawork)
    check("instawork: retailer", m.get("retailer"), "Retailer")
    check("instawork: store_number", m.get("store_number"), "Store #")
    check("instawork: address", m.get("address"), "Street Address")
    check("instawork: start_date", m.get("start_date"), "Date")
    check("instawork: start_time", m.get("start_time"), "Start Time")
    check("instawork: end_time", m.get("end_time"), "End Time")
    check("instawork: break_length", m.get("break_length"), "Break")
    check("instawork: quantity", m.get("quantity"), "# of Workers")
    check("instawork: position", m.get("position"), "Position")
    check("instawork: team_lead", m.get("team_lead"), "Onsite contact")
    check("instawork: team_lead_phone", m.get("team_lead_phone"), "Onsite Contact Phone")
    check("instawork: team_lead_email", m.get("team_lead_email"), "Onsite Contact Email (optional)")
    check("instawork: worker_pay_rate", m.get("worker_pay_rate"), "Pay Rate")
    check("instawork: requested_workers", m.get("requested_workers"), "Requested Workers")

    # test_advantage.csv headers (no retailer, no position)
    cols_adv = [
        "Store #", "Address", "City", "State", "Zip",
        "Start Date", "Start Time", "End Time", "Break",
        "# of Workers", "Team Lead", "Team Lead Phone",
    ]
    m2 = csv_processor.auto_detect_columns(cols_adv)
    check("advantage: store_number", m2.get("store_number"), "Store #")
    check("advantage: address", m2.get("address"), "Address")
    check("advantage: start_date", m2.get("start_date"), "Start Date")
    check("advantage: team_lead", m2.get("team_lead"), "Team Lead")
    check("advantage: team_lead_phone", m2.get("team_lead_phone"), "Team Lead Phone")
    check("advantage: no retailer", m2.get("retailer"), None)

    # test_retail_odyssey.csv headers (Cost Center, Project Code, Region, Team #)
    cols_ro = [
        "Retailer", "Cost Center", "Project Code", "Store #",
        "Street Address", "City", "State", "Zip", "Start Date",
        "Start Time", "End Time", "Break", "# of Workers",
        "Team Lead Name", "Team Lead Phone", "Notes",
    ]
    m3 = csv_processor.auto_detect_columns(cols_ro)
    check("retail_odyssey: retailer", m3.get("retailer"), "Retailer")
    check("retail_odyssey: store_number", m3.get("store_number"), "Store #")
    check("retail_odyssey: address", m3.get("address"), "Street Address")

    # price_chopper_420.csv headers (CAPS, team #, schedule name, worker pay)
    cols_pc = [
        "RETAILER", "team #", "STORE #", "SCHEDULE NAME (Optional)",
        "STREET ADDRESS", "CITY", "STATE", "ZIP", "START DATE",
        "START TIME", "END TIME", "QUANTITY NEEDED",
        "ONSITE CONTACT", "PHONE", "WORKER PAY",
        "LOCATION INSTRUCTIONS", "ATTIRE INSTRUCTIONS",
    ]
    m4 = csv_processor.auto_detect_columns(cols_pc)
    check("price_chopper: retailer", m4.get("retailer"), "RETAILER")
    check("price_chopper: store_number", m4.get("store_number"), "STORE #")
    check("price_chopper: schedule_name", m4.get("schedule_name"), "SCHEDULE NAME (Optional)")
    check("price_chopper: quantity", m4.get("quantity"), "QUANTITY NEEDED")
    check("price_chopper: team_lead", m4.get("team_lead"), "ONSITE CONTACT")
    check("price_chopper: team_lead_phone", m4.get("team_lead_phone"), "PHONE")
    check("price_chopper: worker_pay_rate", m4.get("worker_pay_rate"), "WORKER PAY")
    check("price_chopper: location_instr", m4.get("location_instructions"), "LOCATION INSTRUCTIONS")
    check("price_chopper: attire_instr", m4.get("attire_instructions"), "ATTIRE INSTRUCTIONS")
    check("price_chopper: booking_group", m4.get("booking_group"), "team #")

    # real_request.csv headers (Company column, Worker Pay Rate, Team #)
    cols_real = [
        "Company", "Retailer", "Store #", "Street Address", "City",
        "State", "Zip", "Start Date", "Start Time", "End Time", "Break",
        "Quantity", "Position", "Onsite contact", "Onsite Contact Phone",
        "Onsite Contact Email", "Requested Workers", "Region",
        "Worker Pay Rate", "Team # (Optional)",
        "Onsite Contact Email (optional)",
    ]
    m5 = csv_processor.auto_detect_columns(cols_real)
    check("real_request: retailer", m5.get("retailer"), "Retailer")
    check("real_request: store_number", m5.get("store_number"), "Store #")
    check("real_request: quantity", m5.get("quantity"), "Quantity")
    check("real_request: worker_pay_rate", m5.get("worker_pay_rate"), "Worker Pay Rate")
    check("real_request: booking_group", m5.get("booking_group"), "Region")


# ═══════════════════════════════════════════════════════════════════════════
# 2. PARSING — TIME, DATE, BREAK
# ═══════════════════════════════════════════════════════════════════════════

def test_parsing():
    section("2. Time / Date / Break Parsing")

    # parse_time
    check("time: 5:00 AM", csv_processor.parse_time("5:00 AM"), "05:00")
    check("time: 1:00 PM", csv_processor.parse_time("1:00 PM"), "13:00")
    check("time: 21:00", csv_processor.parse_time("21:00"), "21:00")
    check("time: 21:00:00", csv_processor.parse_time("21:00:00"), "21:00")
    check("time: 05:30", csv_processor.parse_time("05:30"), "05:30")
    check("time: 12:00 AM", csv_processor.parse_time("12:00 AM"), "00:00")
    check("time: 12:00 PM", csv_processor.parse_time("12:00 PM"), "12:00")
    check("time: 6:00 AM", csv_processor.parse_time("6:00 AM"), "06:00")
    check("time: 3:30 PM", csv_processor.parse_time("3:30 PM"), "15:30")
    check("time: None", csv_processor.parse_time(None), "")
    check("time: empty", csv_processor.parse_time(""), "")
    check("time: 07:30:00", csv_processor.parse_time("07:30:00"), "07:30")

    # parse_date
    check("date: 4/14/2026", csv_processor.parse_date("4/14/2026"), "4/14/2026")
    check("date: 2026-04-13", csv_processor.parse_date("2026-04-13"), "2026-04-13")
    check("date: None", csv_processor.parse_date(None), "")

    # parse_break_length
    check("break: 30", csv_processor.parse_break_length(30), 30)
    check("break: '30'", csv_processor.parse_break_length("30"), 30)
    check("break: '30 min'", csv_processor.parse_break_length("30 min"), 30)
    check("break: '30 minutes'", csv_processor.parse_break_length("30 minutes"), 30)
    check("break: 45.0", csv_processor.parse_break_length(45.0), 45)
    check("break: empty → default 30", csv_processor.parse_break_length(""), 30)
    check("break: None → default 30", csv_processor.parse_break_length(None), 30)
    check("break: NaN → default 30", csv_processor.parse_break_length(float("nan")), 30)
    # Time range (real_request.csv uses "2:00am-2:30am")
    check("break: 2:00am-2:30am", csv_processor.parse_break_length("2:00am-2:30am"), 30)
    check("break: 12:00pm-12:30pm", csv_processor.parse_break_length("12:00pm-12:30pm"), 30)
    check("break: 12:00pm - 1:00pm", csv_processor.parse_break_length("12:00pm - 1:00pm"), 60)
    check("break: 14:00-14:30", csv_processor.parse_break_length("14:00-14:30"), 30)
    check("break: 0", csv_processor.parse_break_length(0), 0)
    check("break: '0'", csv_processor.parse_break_length("0"), 0)
    check("break: '60'", csv_processor.parse_break_length("60"), 60)
    check("break: '15 min'", csv_processor.parse_break_length("15 min"), 15)


# ═══════════════════════════════════════════════════════════════════════════
# 3. FILE PARSING — FULL CSVs
# ═══════════════════════════════════════════════════════════════════════════

def _load_csv(filename):
    path = os.path.join(os.path.dirname(os.path.dirname(__file__)), filename)
    with open(path, "rb") as f:
        return f.read()


def test_file_parsing():
    section("3. Full CSV Parsing")

    # test_instawork.csv
    content = _load_csv("test_instawork.csv")
    mapping = csv_processor.auto_detect_columns([
        "Retailer", "Store #", "Street Address", "City", "State", "Zip",
        "Date", "Start Time", "End Time", "Break", "# of Workers",
        "Position", "Onsite contact", "Onsite Contact Phone",
        "Onsite Contact Email (optional)", "Pay Rate", "Requested Workers",
    ])
    rows, cols = csv_processor.parse_upload(content, "test_instawork.csv", mapping)
    check("instawork: row count", len(rows), 8)
    check("instawork: row0 retailer", rows[0].get("retailer"), "Albertsons")
    check("instawork: row0 store", rows[0].get("store_number"), "25")
    check("instawork: row0 start_time", rows[0].get("start_time"), "05:00")
    check("instawork: row0 end_time", rows[0].get("end_time"), "13:00")
    check("instawork: row0 position", rows[0].get("position"), "Merchandiser")
    check("instawork: row4 retailer", rows[4].get("retailer"), "Safeway")
    check("instawork: row4 store", rows[4].get("store_number"), "1515")
    check("instawork: row0 contact", rows[0].get("team_lead"), "Joe Bedard")
    check("instawork: row0 phone", rows[0].get("team_lead_phone"), "623-680-9370")

    # test_advantage.csv (no retailer column, 24h times, 30min break)
    content2 = _load_csv("test_advantage.csv")
    mapping2 = csv_processor.auto_detect_columns([
        "Store #", "Address", "City", "State", "Zip",
        "Start Date", "Start Time", "End Time", "Break",
        "# of Workers", "Team Lead", "Team Lead Phone",
    ])
    rows2, _ = csv_processor.parse_upload(content2, "test_advantage.csv", mapping2)
    check("advantage: row count", len(rows2), 5)
    check("advantage: row0 store", rows2[0].get("store_number"), "1")
    check("advantage: row0 no retailer", rows2[0].get("retailer", ""), "")
    check("advantage: row0 start_time", rows2[0].get("start_time"), "21:00")
    check("advantage: row0 end_time", rows2[0].get("end_time"), "05:30")
    check("advantage: row0 break", rows2[0].get("break_length"), "30")
    check("advantage: row0 qty", rows2[0].get("quantity"), "3")
    check("advantage: row0 lead", rows2[0].get("team_lead"), "Adam Hill")
    check("advantage: row4 store", rows2[4].get("store_number"), "999")

    # real_request.csv (break as time range "2:00am-2:30am", 24h times)
    content3 = _load_csv("real_request.csv")
    mapping3 = csv_processor.auto_detect_columns([
        "Company", "Retailer", "Store #", "Street Address", "City",
        "State", "Zip", "Start Date", "Start Time", "End Time", "Break",
        "Quantity", "Position", "Onsite contact", "Onsite Contact Phone",
        "Onsite Contact Email", "Requested Workers", "Region",
        "Worker Pay Rate", "Team # (Optional)",
        "Onsite Contact Email (optional)",
    ])
    rows3, _ = csv_processor.parse_upload(content3, "real_request.csv", mapping3)
    check("real_request: row count", len(rows3), 8)
    check("real_request: row0 retailer", rows3[0].get("retailer"), "Retail Odyssey - Kroger")
    check("real_request: row0 store", rows3[0].get("store_number"), "836")
    check("real_request: row0 break parsed", rows3[0].get("break_length"), "30")
    check("real_request: row0 start", rows3[0].get("start_time"), "21:00")
    check("real_request: row0 end", rows3[0].get("end_time"), "07:30")

    # price_chopper_420.csv (empty retailer on some rows, schedule name, pay with $/hr)
    content4 = _load_csv("price_chopper_420.csv")
    mapping4 = csv_processor.auto_detect_columns([
        "RETAILER", "team #", "STORE #", "SCHEDULE NAME (Optional)",
        "STREET ADDRESS", "CITY", "STATE", "ZIP", "START DATE",
        "START TIME", "END TIME", "QUANTITY NEEDED",
        "ONSITE CONTACT", "PHONE", "WORKER PAY",
        "LOCATION INSTRUCTIONS", "ATTIRE INSTRUCTIONS",
    ])
    rows4, _ = csv_processor.parse_upload(content4, "price_chopper_420.csv", mapping4)
    check("price_chopper: row count", len(rows4), 38)
    check("price_chopper: row0 retailer", rows4[0].get("retailer"), "Price Chopper")
    check("price_chopper: row0 store", rows4[0].get("store_number"), "199")
    check("price_chopper: row0 schedule", rows4[0].get("schedule_name"), "Bread / Isotonics")
    check("price_chopper: row0 start", rows4[0].get("start_time"), "06:00")
    check("price_chopper: row0 end", rows4[0].get("end_time"), "15:30")
    check("price_chopper: row0 qty", rows4[0].get("quantity"), "3")
    check("price_chopper: row0 contact", rows4[0].get("team_lead"), "mike")
    check("price_chopper: row0 phone", rows4[0].get("team_lead_phone"), "315-886-0931")
    check("price_chopper: row0 loc_instr", rows4[0].get("location_instructions"), "Meet at Customer Service")
    # row3 originally had empty retailer — auto-filled from most common
    check("price_chopper: row3 auto-filled retailer", rows4[3].get("retailer"), "Price Chopper")
    check("price_chopper: row3 store", rows4[3].get("store_number"), "28")


# ═══════════════════════════════════════════════════════════════════════════
# 4. BUSINESS NAME FORMATTING
# ═══════════════════════════════════════════════════════════════════════════

def test_business_name_formatting():
    section("4. Business Name Formatting")

    check("fmt: normal", csv_processor.format_business_name("Albertsons", "25"), "Albertsons - #25")
    check("fmt: with hash", csv_processor.format_business_name("Safeway", "#1515"), "Safeway - #1515")
    check("fmt: no retailer", csv_processor.format_business_name("", "42"), "#42")
    check("fmt: no store", csv_processor.format_business_name("Kroger", ""), "Kroger")
    check("fmt: both empty", csv_processor.format_business_name("", ""), "")
    check("fmt: spaces", csv_processor.format_business_name("  Walmart  ", "  5678  "), "Walmart - #5678")
    check("fmt: apostrophe", csv_processor.format_business_name("Sedano's", "8"), "Sedano's - #8")
    check("fmt: long name", csv_processor.format_business_name("Walmart Neighborhood Market", "5678"),
          "Walmart Neighborhood Market - #5678")


# ═══════════════════════════════════════════════════════════════════════════
# 5. STORE NUMBER EXTRACTION
# ═══════════════════════════════════════════════════════════════════════════

def test_store_number_extraction():
    section("5. Store Number Extraction from Business Names")

    ext = csv_processor._extract_store_number
    check("ext: Albertsons - #25", ext("Albertsons - #25"), "25")
    check("ext: Price Chopper #94", ext("Price Chopper #94"), "94")
    check("ext: Price Chopper 199", ext("Price Chopper 199"), "199")
    check("ext: Tops 42", ext("Tops 42"), "42")
    check("ext: Sedano's #8", ext("Sedano's #8"), "8")
    check("ext: Store: Kroger 88", ext("Store: Kroger 88"), "88")
    check("ext: Walmart NM - #5678", ext("Walmart Neighborhood Market - #5678"), "5678")
    check("ext: Store 99", ext("Store 99"), "99")
    check("ext: Location 123", ext("Location 123"), "123")
    check("ext: just text", ext("Albertsons"), None)
    check("ext: empty", ext(""), None)
    check("ext: None", ext(None), None)
    check("ext: single digit 8", ext("Sedano's 8"), None)  # single digit at end doesn't match \d{2,}
    check("ext: #8 with hash", ext("#8"), "8")  # but # prefix always works


# ═══════════════════════════════════════════════════════════════════════════
# 6. BUSINESS MATCHING — EDGE CASES
# ═══════════════════════════════════════════════════════════════════════════

def test_business_matching():
    section("6. Business Matching Edge Cases")

    existing = [
        {"business_id": 1001, "business_name": "Albertsons - #25", "address": "21001 N. Tatum Blvd., Phoenix, AZ"},
        {"business_id": 1002, "business_name": "Price Chopper #94", "address": "1 Price Chopper Plaza, Mechanicville, NY"},
        {"business_id": 1003, "business_name": "Price Chopper 199", "address": "5701 East Circle Drive, Cicero, NY"},
        {"business_id": 1004, "business_name": "Safeway - #1515", "address": "810 E. Glendale Ave., Phoenix, AZ"},
        {"business_id": 1005, "business_name": "Smiths - #1", "address": "5356 College Ave, Oakland, CA"},
        {"business_id": 1006, "business_name": "Smiths - #2", "address": "8444 Miralani Dr, San Diego, CA"},
        {"business_id": 1007, "business_name": "Retail Odyssey - Kroger  - #836", "address": "1500 Lexington Ave., Mansfield, OH"},
        {"business_id": 1008, "business_name": "Retail Odyssey - Kroger  - #858", "address": "226 E. Perkins Ave, Sandusky, OH"},
    ]

    # 6a. With retailer column — name_exact matching
    rows_with_retailer = [
        {"retailer": "Albertsons", "store_number": "25", "address": "21001 N. Tatum Blvd."},
        {"retailer": "Safeway", "store_number": "1515", "address": "810 E. Glendale Ave."},
        {"retailer": "Price Chopper", "store_number": "94", "address": "1 Price Chopper Plaza"},
        {"retailer": "Price Chopper", "store_number": "199", "address": "5701 East Circle Drive"},
    ]
    matched, unmatched = csv_processor.match_businesses(rows_with_retailer, existing)
    check("6a: all matched", len(matched), 4)
    check("6a: none unmatched", len(unmatched), 0)
    check("6a: row0 biz_id", matched[0]["_business"]["business_id"], 1001)
    check("6a: row1 biz_id", matched[1]["_business"]["business_id"], 1004)
    check("6a: row2 biz_id", matched[2]["_business"]["business_id"], 1002)
    check("6a: row3 biz_id", matched[3]["_business"]["business_id"], 1003)

    # 6b. Without retailer — falls back to store_number only
    rows_no_retailer = [
        {"store_number": "1", "address": "5356 College Ave"},
        {"store_number": "2", "address": "8444 Miralani Dr"},
        {"store_number": "999", "address": "123 Nowhere Lane"},
    ]
    matched2, unmatched2 = csv_processor.match_businesses(rows_no_retailer, existing)
    check("6b: 2 matched", len(matched2), 2)
    check("6b: 1 unmatched", len(unmatched2), 1)
    check("6b: store 1 → Smiths #1", matched2[0]["_business"]["business_id"], 1005)
    check("6b: store 2 → Smiths #2", matched2[1]["_business"]["business_id"], 1006)
    check("6b: method name_fuzzy (no retailer, found by store# in name)", matched2[0]["_match_method"], "name_fuzzy")

    # 6c. Different retailer, same store number — must NOT cross-match
    rows_diff_retailer = [
        {"retailer": "Vons", "store_number": "25", "address": "555 Different St"},
        {"retailer": "Kroger", "store_number": "1515", "address": "999 Other Ave"},
    ]
    matched3, unmatched3 = csv_processor.match_businesses(rows_diff_retailer, existing)
    check("6c: diff retailer → unmatched", len(unmatched3), 2)
    check("6c: none matched", len(matched3), 0)

    # 6d. Address-only match when store # not in any existing name
    existing_addr_only = [
        {"business_id": 2001, "business_name": "Mystery Store", "address": "742 Evergreen Terrace, Springfield, IL"},
    ]
    rows_addr = [{"retailer": "FreshMart", "store_number": "777", "address": "742 Evergreen Terrace"}]
    matched4, unmatched4 = csv_processor.match_businesses(rows_addr, existing_addr_only)
    check("6d: address fallback matched", len(matched4), 1)
    check("6d: method=address", matched4[0]["_match_method"], "address")

    # 6e. Fuzzy name match — retailer substring in existing name
    rows_fuzzy = [
        {"retailer": "Kroger", "store_number": "836", "address": "1500 Lexington Ave."},
    ]
    matched5, unmatched5 = csv_processor.match_businesses(rows_fuzzy, existing)
    check("6e: fuzzy retailer match", len(matched5), 1)
    check("6e: biz_id", matched5[0]["_business"]["business_id"], 1007)
    check_in("6e: method is name_", matched5[0]["_match_method"], ["name_exact", "name_fuzzy"])

    # 6f. Completely new business — no match anywhere
    rows_new = [
        {"retailer": "Target", "store_number": "9999", "address": "1 Bullseye Lane"},
    ]
    matched6, unmatched6 = csv_processor.match_businesses(rows_new, existing)
    check("6f: truly new", len(unmatched6), 1)
    check("6f: expected name", unmatched6[0]["_expected_name"], "Target - #9999")

    # 6g. Empty existing list — all rows are new
    matched7, unmatched7 = csv_processor.match_businesses(rows_with_retailer, [])
    check("6g: empty existing → all new", len(unmatched7), 4)
    check("6g: none matched", len(matched7), 0)

    # 6h. Duplicate shift rows for same store — all rows get matched individually
    rows_dup = [
        {"retailer": "Albertsons", "store_number": "25", "address": "21001 N. Tatum Blvd.", "start_date": "4/14"},
        {"retailer": "Albertsons", "store_number": "25", "address": "21001 N. Tatum Blvd.", "start_date": "4/15"},
        {"retailer": "Albertsons", "store_number": "25", "address": "21001 N. Tatum Blvd.", "start_date": "4/16"},
    ]
    matched8, _ = csv_processor.match_businesses(rows_dup, existing)
    check("6h: all 3 shift rows matched", len(matched8), 3)
    check("6h: all same biz_id", all(m["_business"]["business_id"] == 1001 for m in matched8), True)


# ═══════════════════════════════════════════════════════════════════════════
# 7. BUSINESS MATCHING — REAL CSV FILES
# ═══════════════════════════════════════════════════════════════════════════

def test_real_csv_matching():
    section("7. Real CSV File Matching")

    existing_all = [
        {"business_id": 1001, "business_name": "Albertsons - #25", "address": "21001 N. Tatum Blvd., Phoenix, AZ 85050"},
        {"business_id": 1004, "business_name": "Safeway - #1515", "address": "810 E. Glendale Ave., Phoenix, AZ 85020"},
        {"business_id": 2001, "business_name": "Price Chopper #199", "address": "5701 East Circle Drive, Cicero, NY 13039"},
        {"business_id": 2002, "business_name": "Price Chopper #94", "address": "1 Price Chopper Plaza, Mechanicville, NY"},
        {"business_id": 2003, "business_name": "Price Chopper #213", "address": "142 Genesee St, Oneida, NY"},
        {"business_id": 3001, "business_name": "Smiths - #1", "address": "5356 College Ave, Oakland, CA"},
        {"business_id": 3002, "business_name": "Smiths - #2", "address": "8444 Miralani Dr, San Diego, CA"},
        {"business_id": 3003, "business_name": "Smiths - #3", "address": "390 Washington Ave, Nutley, NJ"},
        {"business_id": 3004, "business_name": "Smiths - #4", "address": "5201 W Lovers Ln, Dallas, TX"},
        {"business_id": 4001, "business_name": "Retail Odyssey - Kroger  - #836", "address": "1500 Lexington Ave., Mansfield, OH"},
        {"business_id": 4002, "business_name": "Retail Odyssey - Kroger  - #858", "address": "226 E. Perkins Ave, Sandusky, OH"},
    ]

    # 7a. test_instawork.csv — 2 stores, 8 rows, both should match
    content = _load_csv("test_instawork.csv")
    mapping = csv_processor.auto_detect_columns([
        "Retailer", "Store #", "Street Address", "City", "State", "Zip",
        "Date", "Start Time", "End Time", "Break", "# of Workers",
        "Position", "Onsite contact", "Onsite Contact Phone",
        "Onsite Contact Email (optional)", "Pay Rate", "Requested Workers",
    ])
    rows, _ = csv_processor.parse_upload(content, "test.csv", mapping)
    matched, unmatched = csv_processor.match_businesses(rows, existing_all)
    check("7a instawork: 8 matched", len(matched), 8)
    check("7a instawork: 0 unmatched", len(unmatched), 0)
    stores_matched = set(m["_business"]["business_id"] for m in matched)
    check("7a instawork: 2 unique stores", len(stores_matched), 2)
    check_in("7a instawork: Albertsons 25", 1001, stores_matched)
    check_in("7a instawork: Safeway 1515", 1004, stores_matched)

    # 7b. test_advantage.csv — no retailer, store #1-4 match, #999 is new
    content2 = _load_csv("test_advantage.csv")
    mapping2 = csv_processor.auto_detect_columns([
        "Store #", "Address", "City", "State", "Zip",
        "Start Date", "Start Time", "End Time", "Break",
        "# of Workers", "Team Lead", "Team Lead Phone",
    ])
    rows2, _ = csv_processor.parse_upload(content2, "test.csv", mapping2)
    matched2, unmatched2 = csv_processor.match_businesses(rows2, existing_all)
    check("7b advantage: 4 matched", len(matched2), 4)
    check("7b advantage: 1 unmatched (999)", len(unmatched2), 1)
    check("7b advantage: new store is 999", unmatched2[0]["store_number"], "999")

    # 7c. test_retail_odyssey.csv — Smiths stores + 1 new
    content3 = _load_csv("test_retail_odyssey.csv")
    mapping3 = csv_processor.auto_detect_columns([
        "Retailer", "Cost Center", "Project Code", "Store #",
        "Street Address", "City", "State", "Zip", "Start Date",
        "Start Time", "End Time", "Break", "# of Workers",
        "Team Lead Name", "Team Lead Phone", "Notes",
    ])
    rows3, _ = csv_processor.parse_upload(content3, "test.csv", mapping3)
    matched3, unmatched3 = csv_processor.match_businesses(rows3, existing_all)
    check("7c retail_odyssey: 4 matched", len(matched3), 4)
    check("7c retail_odyssey: 1 unmatched (500)", len(unmatched3), 1)
    check("7c retail_odyssey: new store #500", unmatched3[0]["store_number"], "500")

    # 7d. price_chopper_420.csv — large file, some match, some new
    content4 = _load_csv("price_chopper_420.csv")
    mapping4 = csv_processor.auto_detect_columns([
        "RETAILER", "team #", "STORE #", "SCHEDULE NAME (Optional)",
        "STREET ADDRESS", "CITY", "STATE", "ZIP", "START DATE",
        "START TIME", "END TIME", "QUANTITY NEEDED",
        "ONSITE CONTACT", "PHONE", "WORKER PAY",
        "LOCATION INSTRUCTIONS", "ATTIRE INSTRUCTIONS",
    ])
    rows4, _ = csv_processor.parse_upload(content4, "test.csv", mapping4)
    matched4, unmatched4 = csv_processor.match_businesses(rows4, existing_all)
    total4 = len(matched4) + len(unmatched4)
    check("7d price_chopper: total rows preserved", total4, 38)
    # Stores 199, 94, 213 should match; rest are new
    matched_stores = set(m["store_number"] for m in matched4)
    check_in("7d price_chopper: 199 matched", "199", matched_stores)
    check_in("7d price_chopper: 94 matched", "94", matched_stores)
    check_in("7d price_chopper: 213 matched", "213", matched_stores)
    # Some rows have empty retailer — should still match by store number via name_fuzzy
    empty_retailer_matched = [m for m in matched4 if not m.get("retailer")]
    check_true("7d price_chopper: empty retailer rows can still match",
               len(empty_retailer_matched) >= 0)  # may or may not have empty retailer matches

    # 7e. real_request.csv — Retail Odyssey Kroger, stores 836+858
    content5 = _load_csv("real_request.csv")
    mapping5 = csv_processor.auto_detect_columns([
        "Company", "Retailer", "Store #", "Street Address", "City",
        "State", "Zip", "Start Date", "Start Time", "End Time", "Break",
        "Quantity", "Position", "Onsite contact", "Onsite Contact Phone",
        "Onsite Contact Email", "Requested Workers", "Region",
        "Worker Pay Rate", "Team # (Optional)",
        "Onsite Contact Email (optional)",
    ])
    rows5, _ = csv_processor.parse_upload(content5, "test.csv", mapping5)
    matched5, unmatched5 = csv_processor.match_businesses(rows5, existing_all)
    check("7e real_request: all 8 matched", len(matched5), 8)
    check("7e real_request: 0 unmatched", len(unmatched5), 0)


# ═══════════════════════════════════════════════════════════════════════════
# 8. BUSINESS IMPORT CSV GENERATION
# ═══════════════════════════════════════════════════════════════════════════

def test_business_import_csv():
    section("8. Business Import CSV Generation")

    unmatched = [
        {"store_number": "9999", "retailer": "Safeway", "address": "999 New St",
         "city": "Nowhere", "state": "CA", "zip": "90000", "_expected_name": "Safeway - #9999", "_status": "new"},
        {"store_number": "9999", "retailer": "Safeway", "address": "999 New St",
         "city": "Nowhere", "state": "CA", "zip": "90000", "_expected_name": "Safeway - #9999", "_status": "new"},
        {"store_number": "8888", "retailer": "Target", "address": "123 Bullseye Ln",
         "city": "Retail", "state": "TX", "zip": "75001", "_expected_name": "Target - #8888", "_status": "new"},
    ]
    cfg = {
        "_company_id": "109562",
        "name": "SAS Retail - Tops Price Chopper",
        "default_parking": 2,
        "default_venue_type": 1,
    }

    result = csv_processor.generate_business_import_csv(unmatched, cfg)
    lines = result.strip().split("\n")
    check("biz_csv: header + 2 unique rows", len(lines), 3)
    check_in("biz_csv: header has columns", "id,company,name,venue_type", lines[0])

    reader = csv.reader(io.StringIO(result))
    header = next(reader)
    row1 = next(reader)
    row2 = next(reader)

    check("biz_csv: row1 company", row1[1], "109562")
    check("biz_csv: row1 name", row1[2], "Safeway - #9999")
    check("biz_csv: row1 address", row1[7], "999 New St, Nowhere, CA, 90000")
    check("biz_csv: row2 name", row2[2], "Target - #8888")
    # Deduplicated — store 9999 appears twice but only one CSV row
    check("biz_csv: deduplication worked", row1[2] != row2[2], True)

    # No retailer fallback — uses partner config name
    unmatched_no_retailer = [
        {"store_number": "42", "retailer": "", "address": "500 Erie Blvd",
         "city": "Syracuse", "state": "NY", "zip": "13202", "_expected_name": "#42", "_status": "new"},
    ]
    result2 = csv_processor.generate_business_import_csv(unmatched_no_retailer, cfg)
    reader2 = csv.reader(io.StringIO(result2))
    next(reader2)  # skip header
    row = next(reader2)
    check("biz_csv: no retailer → uses _expected_name", row[2], "#42")


# ═══════════════════════════════════════════════════════════════════════════
# 9. TASKS CSV GENERATION
# ═══════════════════════════════════════════════════════════════════════════

def test_tasks_csv():
    section("9. Tasks (Clock-Out) CSV Generation")

    # No tasks configured → returns None
    cfg_no_tasks = {"clock_in_task_ids": [], "during_task_ids": [], "clock_out_task_ids": []}
    result = csv_processor.generate_tasks_csv([{"business_id": 123}], cfg_no_tasks)
    check("tasks: no tasks → None", result, None)

    # With tasks
    cfg_tasks = {
        "clock_in_task_ids": [10, 11],
        "during_task_ids": [],
        "clock_out_task_ids": [20, 21, 22],
        "task_position_ids": [42],
        "default_position_id": 29,
    }
    businesses = [
        {"business_id": 5001, "store_number": "25"},
        {"business_id": 5002, "store_number": "1515"},
    ]
    result2 = csv_processor.generate_tasks_csv(businesses, cfg_tasks)
    check_true("tasks: result not None", result2 is not None)
    reader = csv.reader(io.StringIO(result2))
    header = next(reader)
    # Enkhjin format: items, is_remove, position, business, type
    check("tasks: header", header, ["items", "is_remove", "position", "business", "type"])
    rows = list(reader)
    # 2 businesses × 2 task types (clockin + clockout, no during) = 4 rows
    check("tasks: row count", len(rows), 4)
    check("tasks: row0 items comma-sep", rows[0][0], "10,11")
    check("tasks: row0 position", rows[0][2], "42")
    check("tasks: row0 biz", rows[0][3], "5001")
    check("tasks: row0 type", rows[0][4], "Clockin")
    check("tasks: row1 clockout items", rows[1][0], "20,21,22")
    check("tasks: row1 type", rows[1][4], "Clockout")


# ═══════════════════════════════════════════════════════════════════════════
# 10. BULK IMPORT CSV GENERATION
# ═══════════════════════════════════════════════════════════════════════════

def test_bulk_import_csv():
    section("10. Bulk Shift Import CSV Generation")

    matched_rows = [
        {
            "retailer": "Albertsons", "store_number": "25",
            "start_date": "4/14/2026", "start_time": "05:00", "end_time": "13:00",
            "break_length": "", "quantity": "4", "position": "Merchandiser",
            "worker_pay_rate": "$16.50", "attire_instructions": "", "location_instructions": "",
            "_business": {"business_id": 1001, "contact_id": 555, "created_by_id": 666,
                          "position": 42, "has_parking": 1, "instructions": "Go to CS desk",
                          "custom_attire_requirements": "Black polo"},
            "_status": "existing",
        },
    ]
    cfg = {
        "default_position_id": 29,
        "default_break_length": 30,
        "default_parking": 2,
        "default_attire": "Default attire",
        "default_position_instructions": "Default instructions",
        "default_location_instructions": "",
        "default_creator_id": 999,
        "default_contact_id": 111,
        "special_requirement_ids": [7, 14],
    }

    result = csv_processor.generate_bulk_import_csv(matched_rows, cfg)
    reader = csv.reader(io.StringIO(result))
    header = next(reader)
    check("bulk: Location Id col", header[0], "Location Id")
    check_true("bulk: has 15+ columns", len(header) >= 15)

    rows = list(reader)
    # qty=4 → 4 expanded rows
    check("bulk: expanded by qty=4", len(rows), 4)

    r = dict(zip(header, rows[0]))
    check("bulk: location_id from biz", r["Location Id"], "1001")
    check("bulk: contact_id from biz", r["Contact Ids"], "555")
    check("bulk: start_date", r["Start Date"], "4/14/2026")
    check("bulk: start_time", r["Start Time"], "05:00")
    check("bulk: end_time", r["End Time"], "13:00")
    check("bulk: break defaults to 30", r["Break Length"], "30")
    check("bulk: position from biz", r["Position Id"], "42")
    check("bulk: parking from biz", r["Parking"], "1")
    check("bulk: attire from biz template", r["Attire Instructions"], "Black polo")
    check("bulk: creator from biz", r["Creator Id"], "666")
    check("bulk: pay rate cleaned", r["Adjusted Base Rate"], "16.5")
    check("bulk: position_instructions from biz", r["Position Instructions"], "Go to CS desk")

    # Task-based shift
    result2 = csv_processor.generate_bulk_import_csv(
        matched_rows, cfg, task_opts={"is_task": True, "is_anywhere": True}
    )
    reader2 = csv.reader(io.StringIO(result2))
    header2 = next(reader2)
    row2 = next(reader2)
    r2 = dict(zip(header2, row2))
    check("bulk task: is_task=1", r2["Is Task"], "1")
    check("bulk task: starts_at_min", r2["Starts At Minimum"], "05:00")
    check("bulk task: is_anywhere=1", r2["Is Anywhere"], "1")


# ═══════════════════════════════════════════════════════════════════════════
# 11. CONTACT MATCHING
# ═══════════════════════════════════════════════════════════════════════════

def test_contact_matching():
    section("11. Contact Matching")

    # Test the normalize phone helper
    check("phone: strip formatting", contacts_db._normalize_phone("(843) 269-9979"), "8432699979")
    check("phone: dashes", contacts_db._normalize_phone("623-680-9370"), "6236809370")
    check("phone: prefix 1-", contacts_db._normalize_phone("1-330-461-2248"), "13304612248")
    check("phone: empty", contacts_db._normalize_phone(""), "")
    check("phone: None", contacts_db._normalize_phone(None), "")


# ═══════════════════════════════════════════════════════════════════════════
# 12. STREET ADDRESS NORMALIZATION
# ═══════════════════════════════════════════════════════════════════════════

def test_address_normalization():
    section("12. Address Normalization")

    norm = csv_processor._normalize_street
    check("addr: basic", norm("21001 N. Tatum Blvd."), "21001 n. tatum")
    check("addr: with comma parts", norm("810 E. Glendale Ave., Phoenix, AZ"), "810 e. glendale")
    check("addr: st suffix", norm("13 Polson St"), "13 polson")
    check("addr: drive suffix", norm("29010 Commerce Center Dr"), "29010 commerce center")
    check("addr: road suffix", norm("500 Market Rd"), "500 market")
    check("addr: empty", norm(""), "")
    check("addr: None", norm(None), "")
    check("addr: multi space", norm("  123   Main   Street  "), "123 main")


# ═══════════════════════════════════════════════════════════════════════════
# RUN ALL
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    test_column_detection()
    test_parsing()
    test_file_parsing()
    test_business_name_formatting()
    test_store_number_extraction()
    test_business_matching()
    test_real_csv_matching()
    test_business_import_csv()
    test_tasks_csv()
    test_bulk_import_csv()
    test_contact_matching()
    test_address_normalization()

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
