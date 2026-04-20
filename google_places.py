"""Google Places address validation for new businesses.

Uses the Place Autocomplete API with types=establishment to match
actual businesses/stores, not just any address. Same approach as
Reshav's "reconcile address" button in Django admin.
"""

import os
import requests
from urllib.parse import urlencode

GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY", "AIzaSyDSmcyXYp668M3LzdfZB2G-zFVyl3cIsRc")

AUTOCOMPLETE_URL = "https://maps.googleapis.com/maps/api/place/autocomplete/json"
FIND_PLACE_URL = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
PLACE_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"


def validate_business_address(business_name, address, country_code="US"):
    """Validate a business address using Google Places Autocomplete.

    Uses types=establishment to find actual stores/businesses,
    not just any location. Returns dict with:
      - valid: True if a matching establishment was found
      - place_id: Google Place ID
      - formatted_address: Google's formatted version
      - input_name: what we searched for
      - input_address: what we searched for
    """
    if not business_name or not address:
        return {"valid": False, "error": "Missing business name or address"}

    query = f"{business_name} at {address}"

    params = {
        "key": GOOGLE_API_KEY,
        "input": query,
        "types": "establishment",
        "components": f"country:{country_code.lower()}",
    }

    try:
        resp = requests.get(f"{AUTOCOMPLETE_URL}?{urlencode(params)}", timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") == "OK" and data.get("predictions"):
            prediction = data["predictions"][0]
            return {
                "valid": True,
                "place_id": prediction.get("place_id"),
                "description": prediction.get("description", ""),
                "input_name": business_name,
                "input_address": address,
            }
        else:
            # Fallback: try Find Place from Text (what Django import uses)
            return _fallback_find_place(business_name, address)

    except Exception as e:
        return {"valid": False, "error": str(e), "input_name": business_name, "input_address": address}


def _fallback_find_place(business_name, address):
    """Fallback using Find Place from Text API (same as Django import)."""
    query = f"{business_name} at {address}"
    params = {
        "key": GOOGLE_API_KEY,
        "input": query,
        "inputtype": "textquery",
        "fields": "place_id,formatted_address,name",
    }

    try:
        resp = requests.get(f"{FIND_PLACE_URL}?{urlencode(params)}", timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") == "OK" and data.get("candidates"):
            candidate = data["candidates"][0]
            return {
                "valid": True,
                "place_id": candidate.get("place_id"),
                "description": candidate.get("formatted_address", ""),
                "google_name": candidate.get("name", ""),
                "input_name": business_name,
                "input_address": address,
                "method": "fallback",
            }
    except Exception:
        pass

    return {
        "valid": False,
        "error": "No matching business found on Google",
        "input_name": business_name,
        "input_address": address,
    }


def validate_new_businesses(new_business_rows):
    """Validate a list of new businesses before generating import CSV.

    Returns list of dicts with validation results for each unique store.
    """
    seen = set()
    results = []

    for row in new_business_rows:
        store_num = row.get("store_number", "").strip()
        if store_num in seen:
            continue
        seen.add(store_num)

        name = row.get("_expected_name") or row.get("retailer", "")
        addr_parts = [row.get("address", ""), row.get("city", ""),
                      row.get("state", ""), row.get("zip", "")]
        address = ", ".join(p for p in addr_parts if p)

        result = validate_business_address(name, address)
        result["store_number"] = store_num
        results.append(result)

    return results
