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


def _banner_matches(row_retailer, business_name):
    """Check whether a row's retailer banner is consistent with a candidate
    business name. Used to reject false-positive local matches when the same
    company has multiple stores with the same number but different banners
    (e.g. Chobani has both Safeway 1109 and HyVee 1109; our snapshot only
    keeps one per company-store key, so the lookup would silently mis-assign).

    Returns True (allow match) if:
      - The row has no retailer hint, or
      - The retailer's letters appear in the business name (case-insensitive,
        non-alphanumeric stripped — "HyVee" matches "HyVee - #1109" and
        "HyVee Pharmacy"; "Fry's" matches "Fry's #407")
    Returns False (reject match) when the retailer is set but is clearly a
    different banner — the row gets pushed to unmatched so Mode can resolve.
    """
    if not row_retailer:
        return True  # no banner hint, fall back to existing behavior
    import re
    norm = lambda s: re.sub(r"[^a-z0-9]", "", (s or "").lower())
    r = norm(row_retailer)
    b = norm(business_name)
    if not r or not b:
        return True
    return r in b


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
        retailer = row.get("retailer", "").strip()

        if (loc and loc.get("business_id")
                and _banner_matches(retailer, loc.get("business_name", ""))):
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
            expected_name = format_business_name(retailer, store_num)
            unmatched.append({
                **row,
                "_status": "new",
                "_expected_name": expected_name,
            })

    return matched, unmatched
