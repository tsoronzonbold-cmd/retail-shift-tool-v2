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


import re

_NORM = lambda s: re.sub(r"[^a-z0-9]", "", (s or "").lower())


def _banner_matches(row_retailer, business_name):
    """Check whether a row's retailer banner is consistent with a candidate
    business name. Catches cases like Chobani's HyVee 1109 vs Safeway 1109
    where the snapshot only kept one entry per company-store key.

    Returns True if row has no retailer hint, or if the retailer's letters
    appear in the business name (alphanumeric-only comparison).
    """
    if not row_retailer:
        return True
    r = _NORM(row_retailer)
    b = _NORM(business_name)
    if not r or not b:
        return True
    return r in b


def _address_matches(row, candidate_address):
    """Check whether a row's address roughly matches a candidate business's
    address. Used when the same partner has multiple stores with the SAME
    number under different regions (Retail Odyssey DSD-Central has stores
    keyed both as "832" Cincinnati and "832" Columbus — banner check alone
    can't disambiguate, both are Kroger).

    Match logic: if the row has a city AND state, and the candidate's
    address string is non-empty, require either:
      - the row's city appears in the candidate's address (case-insensitive), OR
      - the row's state appears AND the row's zip appears

    If either side is missing the data needed to compare, allow the match —
    we don't want to over-reject. This is conservative on purpose: we only
    REJECT when we have positive evidence of mismatch.
    """
    if not candidate_address:
        return True
    row_city = (row.get("city") or "").strip().lower()
    row_state = (row.get("state") or "").strip().upper()
    row_zip = (row.get("zip") or "").strip()
    addr_low = candidate_address.lower()
    addr_up = candidate_address.upper()

    # If we don't have a city to check, fall back to zip — if the row's
    # zip is in the candidate's address, that's strong evidence.
    if not row_city and row_zip and row_zip in candidate_address:
        return True
    if not row_city:
        return True  # not enough info to reject

    # Positive city match
    if row_city in addr_low:
        return True
    # City didn't match. State + zip together is a fallback signal.
    if row_state and row_state in addr_up and row_zip and row_zip in candidate_address:
        return True
    # We have a city, candidate has an address, and city isn't in there.
    # That's positive evidence of mismatch — reject.
    return False


def match_businesses(company_id, parsed_rows):
    """Match parsed rows against local locations database.

    Returns (matched, unmatched) in the same format as csv_processor.match_businesses().

    A local-DB match is accepted only if:
      1. (company_id, store_number) keys an entry, AND
      2. Row's retailer banner is consistent with the candidate's business_name
         (rejects HyVee→Safeway-style cross-banner mis-matches), AND
      3. Row's city/state/zip is consistent with the candidate's address
         (rejects same-number-different-region mis-matches like DSD #832)

    Anything that fails any check gets pushed to unmatched so Mode can resolve
    it from live Redshift data.
    """
    data = _load()
    matched = []
    unmatched = []

    for row in parsed_rows:
        store_num = row.get("store_number", "").strip().lstrip("#")
        loc = data.get(f"{company_id}-{store_num}")
        retailer = row.get("retailer", "").strip()

        if (loc and loc.get("business_id")
                and _banner_matches(retailer, loc.get("business_name", ""))
                and _address_matches(row, loc.get("address", ""))):
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
