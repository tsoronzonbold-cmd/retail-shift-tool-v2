"""Head-to-head comparison: Enkhjin's detection logic vs ours.

Reproduces Enkhjin's matching approach in Python (name formatting + Mode-style
matching) and compares it against our Redshift-backed matcher on the same test
cases — including the false-negative edge cases Ramses flagged.
"""

import re
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import csv_processor


# ── Enkhjin's logic, ported to Python ──────────────────────────────────────

def enkhjin_clean_store_name(value, store_id=None):
    """Port of cleanStoreName() from RetailShiftTool/server/routes.ts:54"""
    cleaned = re.sub(r"^(Store|Location|Shop|Branch):\s*", "", value, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if store_id and cleaned:
        escaped = re.escape(store_id)
        pattern = rf"\s*[-–]?\s*#?{escaped}\s*$"
        stripped = re.sub(pattern, "", cleaned, flags=re.IGNORECASE).strip()
        if stripped and re.search(r"[a-zA-Z]", stripped):
            cleaned = stripped
    return cleaned


def enkhjin_format_name(retailer, store_id):
    """Port of the name formatting from routes.ts:1222"""
    brand = enkhjin_clean_store_name(retailer, store_id)
    return f"{brand} - #{store_id}"


def enkhjin_match(csv_rows, mode_results):
    """Simulate Enkhjin's check-businesses flow.

    In her tool: CSV rows → format "Brand - #ID" → send to Mode API →
    Mode returns {input_business_name, status, business_id, match_type}.
    We simulate the Mode response with mode_results dict.
    """
    unique_map = {}
    for i, row in enumerate(csv_rows):
        store_id = str(row.get("store_number", "")).strip().lstrip("#")
        retailer = row.get("retailer", "").strip()
        name = enkhjin_format_name(retailer, store_id)
        if store_id and name not in unique_map:
            address_parts = [row.get("address", ""), row.get("city", ""),
                             row.get("state", ""), row.get("zip", "")]
            address = ", ".join(p for p in address_parts if p)
            unique_map[name] = {
                "id": f"biz_{i}",
                "store_id": store_id,
                "name": name,
                "address": address,
                "isNew": True,
            }

    businesses = []
    for b in unique_map.values():
        mode_row = mode_results.get(b["name"])
        if mode_row:
            b["isNew"] = mode_row.get("status") == "NEW" or not mode_row.get("business_id")
            b["existingBusinessId"] = mode_row.get("business_id")
            b["matchType"] = mode_row.get("match_type", "no_match")
        businesses.append(b)

    return businesses


# ── Test data ──────────────────────────────────────────────────────────────

# Simulated existing businesses in Redshift/Django
EXISTING_BUSINESSES = [
    {"business_id": 1001, "business_name": "Albertsons - #25",
     "address": "21001 N. Tatum Blvd., Phoenix, AZ 85050"},
    {"business_id": 1002, "business_name": "Price Chopper #94",
     "address": "1990 Circle Dr, Niskayuna, NY 12309"},
    {"business_id": 1003, "business_name": "Price Chopper 199",
     "address": "100 Main St, Albany, NY 12203"},
    {"business_id": 1004, "business_name": "Safeway - #1515",
     "address": "810 E. Glendale Ave., Phoenix, AZ 85020"},
    {"business_id": 1005, "business_name": "Tops 42",
     "address": "500 Erie Blvd, Syracuse, NY 13202"},
    {"business_id": 1006, "business_name": "Store: Kroger 88",
     "address": "123 Market St, Columbus, OH 43215"},
    {"business_id": 1007, "business_name": "Sedano's #8",
     "address": "2100 SW 8th St, Miami, FL 33135"},
    {"business_id": 1008, "business_name": "Walmart Neighborhood Market - #5678",
     "address": "4500 W Roosevelt Rd, Chicago, IL 60624"},
]


def build_test_rows():
    """Build test cases covering edge cases Ramses flagged."""
    return [
        # Case 1: Exact match — "Albertsons - #25" exists
        {"retailer": "Albertsons", "store_number": "25",
         "address": "21001 N. Tatum Blvd.", "city": "Phoenix", "state": "AZ", "zip": "85050"},

        # Case 2: Name without # — "Price Chopper 199" (no hash in existing name)
        {"retailer": "Price Chopper", "store_number": "199",
         "address": "100 Main St", "city": "Albany", "state": "NY", "zip": "12203"},

        # Case 3: Name with # but different format — "Price Chopper #94"
        {"retailer": "Price Chopper", "store_number": "94",
         "address": "1990 Circle Dr", "city": "Niskayuna", "state": "NY", "zip": "12309"},

        # Case 4: Truly new business — doesn't exist anywhere
        {"retailer": "Safeway", "store_number": "9999",
         "address": "999 New St", "city": "Nowhere", "state": "CA", "zip": "90000"},

        # Case 5: Match by address when store number differs in naming
        {"retailer": "Tops", "store_number": "42",
         "address": "500 Erie Blvd", "city": "Syracuse", "state": "NY", "zip": "13202"},

        # Case 6: Store name has prefix "Store:" — "Store: Kroger 88"
        {"retailer": "Kroger", "store_number": "88",
         "address": "123 Market St", "city": "Columbus", "state": "OH", "zip": "43215"},

        # Case 7: Sedano's with apostrophe — "Sedano's #8"
        {"retailer": "Sedano's", "store_number": "8",
         "address": "2100 SW 8th St", "city": "Miami", "state": "FL", "zip": "33135"},

        # Case 8: Long store number — "Walmart Neighborhood Market - #5678"
        {"retailer": "Walmart Neighborhood Market", "store_number": "5678",
         "address": "4500 W Roosevelt Rd", "city": "Chicago", "state": "IL", "zip": "60624"},

        # Case 9: Same store number, different retailer — should NOT match Albertsons #25
        {"retailer": "Vons", "store_number": "25",
         "address": "555 Different St", "city": "Los Angeles", "state": "CA", "zip": "90001"},

        # Case 10: Existing business found only by address (store # doesn't appear in name)
        {"retailer": "Safeway", "store_number": "1515",
         "address": "810 E. Glendale Ave.", "city": "Phoenix", "state": "AZ", "zip": "85020"},
    ]


def build_mode_results():
    """Simulate what Mode Analytics would return.

    Mode matches by exact business name OR exact address against
    all businesses for the company. It's essentially doing:
      SELECT * FROM businesses WHERE name = :input_name OR address LIKE :input_addr
    """
    existing_by_name = {}
    existing_by_addr = {}
    for b in EXISTING_BUSINESSES:
        existing_by_name[b["business_name"].lower()] = b
        addr_key = b["address"].split(",")[0].strip().lower()
        existing_by_addr[addr_key] = b

    test_rows = build_test_rows()
    mode_results = {}
    for row in test_rows:
        formatted = enkhjin_format_name(row["retailer"], row["store_number"])
        # Mode matches by exact name
        found = existing_by_name.get(formatted.lower())
        if not found:
            # Mode also tries address matching
            row_addr = row["address"].split(",")[0].strip().lower()
            found = existing_by_addr.get(row_addr)
        if found:
            mode_results[formatted] = {
                "status": "EXISTING",
                "business_id": found["business_id"],
                "match_type": "name_match" if found["business_name"].lower() == formatted.lower() else "address_match",
            }
        else:
            mode_results[formatted] = {"status": "NEW", "business_id": None, "match_type": "no_match"}

    return mode_results


# ── Test runner ────────────────────────────────────────────────────────────

def run_comparison():
    test_rows = build_test_rows()

    # ─── Enkhjin's tool ───
    mode_results = build_mode_results()
    enkhjin_results = enkhjin_match(test_rows, mode_results)
    # Enkhjin deduplicates by formatted name, so "Vons - #25" ≠ "Albertsons - #25"
    enkhjin_by_name = {b["name"]: b for b in enkhjin_results}

    # ─── Our tool ───
    ours_matched, ours_unmatched = csv_processor.match_businesses(test_rows, EXISTING_BUSINESSES)
    # Rebuild results in original row order by matching on original row dicts
    our_by_idx = {}
    for row in ours_matched:
        # Find original index by matching non-internal keys
        orig = {k: v for k, v in row.items() if not k.startswith("_")}
        for i, tr in enumerate(test_rows):
            if tr == orig and i not in our_by_idx:
                our_by_idx[i] = {"isNew": False, "method": row.get("_match_method"), "biz": row["_business"]}
                break
    for row in ours_unmatched:
        orig = {k: v for k, v in row.items() if not k.startswith("_")}
        for i, tr in enumerate(test_rows):
            if tr == orig and i not in our_by_idx:
                our_by_idx[i] = {"isNew": True, "method": None, "biz": None}
                break

    # ─── Compare ───
    cases = [
        (0, "Albertsons #25 (exact match)",           "Albertsons - #25",                    False),
        (1, "Price Chopper 199 (no # in name)",       "Price Chopper - #199",                False),
        (2, "Price Chopper #94 (has #)",              "Price Chopper - #94",                 False),
        (3, "Safeway #9999 (truly new)",              "Safeway - #9999",                     True),
        (4, "Tops 42 (trailing digits)",              "Tops - #42",                          False),
        (5, "Kroger 88 (Store: prefix)",              "Kroger - #88",                        False),
        (6, "Sedano's #8 (apostrophe)",               "Sedano's - #8",                       False),
        (7, "Walmart NM #5678 (long name)",           "Walmart Neighborhood Market - #5678", False),
        (8, "Vons #25 (diff retailer, same #)",       "Vons - #25",                          True),
        (9, "Safeway #1515 (address match)",          "Safeway - #1515",                     False),
    ]

    print("=" * 105)
    print(f"{'CASE':<6} {'INPUT':<37} {'ENKHJIN':<20} {'OURS':<28} {'WINNER':<10}")
    print("=" * 105)

    enkhjin_score = 0
    our_score = 0

    for idx, description, enkhjin_name, expected_new in cases:
        case_num = idx + 1

        # Enkhjin result — look up by formatted name
        e = enkhjin_by_name.get(enkhjin_name, {})
        e_new = e.get("isNew", True)
        e_correct = (e_new == expected_new)
        if e_correct:
            enkhjin_score += 1

        # Our result — look up by row index
        o = our_by_idx.get(idx, {"isNew": True, "method": None})
        o_new = o["isNew"]
        o_method = o.get("method") or ""
        o_correct = (o_new == expected_new)
        if o_correct:
            our_score += 1

        e_label = "NEW" if e_new else "EXISTING"
        o_label = "NEW" if o_new else f"EXISTING ({o_method})"
        e_mark = "✓" if e_correct else "✗"
        o_mark = "✓" if o_correct else "✗"

        if e_correct and o_correct:
            winner = "TIE"
        elif e_correct:
            winner = "ENKHJIN"
        elif o_correct:
            winner = "OURS"
        else:
            winner = "BOTH WRONG"

        print(f"{case_num:<6} {description:<37} {e_mark} {e_label:<17} {o_mark} {o_label:<25} {winner}")

    total = len(cases)
    print("=" * 105)
    print(f"\nSCOREBOARD:  Enkhjin: {enkhjin_score}/{total}   Ours: {our_score}/{total}")
    print()

    # Detail on disagreements
    print("DISAGREEMENTS:")
    print("-" * 80)
    found_any = False
    for idx, description, enkhjin_name, expected_new in cases:
        e = enkhjin_by_name.get(enkhjin_name, {})
        o = our_by_idx.get(idx, {"isNew": True})
        e_new = e.get("isNew", True)
        o_new = o["isNew"]
        if e_new != o_new:
            found_any = True
            exp = "NEW" if expected_new else "EXISTING"
            print(f"  Case {idx+1}: {description}")
            print(f"    Expected: {exp}")
            print(f"    Enkhjin:  {'NEW' if e_new else 'EXISTING'}")
            print(f"    Ours:     {'NEW' if o_new else 'EXISTING'} (method: {o.get('method', 'n/a')})")
            print()
    if not found_any:
        print("  None — both tools agree on all cases.")


if __name__ == "__main__":
    run_comparison()
