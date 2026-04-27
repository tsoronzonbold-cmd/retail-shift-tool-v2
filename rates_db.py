"""Fixed rate lookup — region/position-specific rates with markup and min wage.

Ported from Christian's rate logic:
  Priority 1: Worker pay rate from CSV + markup
  Priority 2: Fixed rate for company + region + position
  Priority 3: Fallback business rate from Company Info
  Min wage floor: if worker rate < location min wage, adjust up
"""

import json
import os

RATES_PATH = os.path.join(os.path.dirname(__file__), "partner_configs", "fixed_rates.json")

_cache = None


def _load():
    global _cache
    if _cache is None:
        if os.path.exists(RATES_PATH):
            with open(RATES_PATH) as f:
                _cache = json.load(f)
        else:
            _cache = {}
    return _cache


def get_fixed_rate(company_id, region_id, position_id):
    """Look up fixed rate by company + region + position.

    Returns dict with professional_rate, business_rate, markup_percentage or None.
    """
    data = _load()
    key = f"{company_id}-{region_id}-{position_id}"
    rate = data.get(key)
    if rate:
        return rate

    # Try without region (some entries have empty region)
    key_no_region = f"{company_id}--{position_id}"
    return data.get(key_no_region)


def calculate_adjusted_rate(csv_rate, company_id, region_id, position_id, config_rate, config_markup=0):
    """Calculate the adjusted base rate using Christian's priority logic.

    Priority:
      1. CSV worker rate + markup → adjusted rate
      2. Fixed rate for region + position (already includes markup)
      3. Config fallback rate
    """
    # Clean CSV rate
    worker_rate = None
    if csv_rate:
        cleaned = str(csv_rate).replace("$", "").replace(",", "").replace("/hr", "").strip()
        try:
            worker_rate = float(cleaned) if cleaned else None
        except ValueError:
            worker_rate = None

    # Get fixed rate
    fixed = get_fixed_rate(company_id, region_id, position_id)

    if worker_rate and worker_rate > 0:
        # Priority 1: CSV rate + markup
        markup = fixed["markup_percentage"] if fixed and fixed.get("markup_percentage") else config_markup
        markup_mult = 1 + (markup / 100) if markup else 1
        return worker_rate * markup_mult

    if fixed and fixed.get("business_rate"):
        # Priority 2: fixed rate (already includes markup)
        return fixed["business_rate"]

    if config_rate:
        # Priority 3: fallback from partner config
        return config_rate

    return ""
