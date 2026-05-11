"""Local locations lookup — Christian's Locations sheet (126K businesses).

Matches by companyId-storeNumber key, same as Christian's tool.
Used as first-pass matching before Mode API.
"""

import json
import os

LOCATIONS_PATH = os.path.join(os.path.dirname(__file__), "partner_configs", "locations.json")

_cache = None


def _load():
    global _cache
    if _cache is None:
        if os.path.exists(LOCATIONS_PATH):
            with open(LOCATIONS_PATH) as f:
                _cache = json.load(f)
        else:
            _cache = {}
    return _cache


def lookup(company_id, store_number):
    """Look up a business by company ID + store number.

    Returns dict with business_id, business_name, address, etc. or None.
    """
    data = _load()
    key = f"{company_id}-{store_number}"
    return data.get(key)


def known_business_count(company_id):
    """Return how many businesses we know about for this company.

    Used by the upload sanity check — if a partner has many known
    businesses but zero matched in the upload, the user probably picked
    the wrong partner from the dropdown.
    """
    cid = str(company_id)
    data = _load()
    return sum(1 for v in data.values() if str(v.get("company_id", "")) == cid)


def match_businesses(company_id, parsed_rows):
    """Match parsed rows against local locations database.

    Returns (matched, unmatched) in the same format as csv_processor.match_businesses().
    """
    data = _load()
    matched = []
    unmatched = []

    for row in parsed_rows:
        store_num = row.get("store_number", "").strip().lstrip("#")
        loc = data.get(f"{company_id}-{store_num}")

        if loc and loc.get("business_id"):
            biz = {
                "business_id": loc["business_id"],
                "business_name": loc.get("business_name", ""),
                "address": loc.get("address", ""),
                "regionmapping_id": loc.get("regionmapping_id", ""),
            }
            matched.append({
                **row,
                "_business": biz,
                "_status": "existing",
                "_match_method": "local_db",
            })
        else:
            from csv_processor import format_business_name
            retailer = row.get("retailer", "").strip()
            expected_name = format_business_name(retailer, store_num)
            unmatched.append({
                **row,
                "_status": "new",
                "_expected_name": expected_name,
            })

    return matched, unmatched
