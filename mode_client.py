"""Mode Analytics API client — proxy to Redshift for Replit deployments.

When running on Replit, we can't connect to Redshift directly (no AWS SSO).
Mode runs parameterized SQL reports on Redshift and returns CSV results.

Two reports:
  - Business matching (acdc985f94f9): checks which businesses exist for a company
  - Contact matching (15823ce00956): finds contacts by phone/name/email
"""

import os
import csv
import io
import re
import time
import base64
import zipfile
import requests

MODE_API_KEY = os.environ.get("MODE_API_KEY", "")
MODE_API_SECRET = os.environ.get("MODE_API_SECRET", "")
MODE_ORG = "instawork"

REPORT_ID = "ac9b652e687f"  # Single report with both queries
BUSINESS_QUERY_TOKEN = "6ec26c5336ee"  # Query 1: business check
CONTACTS_QUERY_TOKEN = "e62e61be97f5"  # Query 2: contact lookup

# How long to poll Mode for results
MAX_POLL_ATTEMPTS = 12
POLL_INTERVAL = 5  # seconds


def is_available():
    """Check if Mode API credentials are configured."""
    return bool(MODE_API_KEY and MODE_API_SECRET)


def _auth_header():
    creds = base64.b64encode(f"{MODE_API_KEY}:{MODE_API_SECRET}".encode()).decode()
    return f"Basic {creds}"


def _run_report(parameters, query_token=None):
    """Run the Mode report and poll until results are ready.

    Both queries (business check + contact lookup) live in one report.
    Use query_token to select which query's CSV to return from the ZIP.
    Returns parsed CSV rows as list of dicts.
    """
    url = f"https://app.mode.com/api/{MODE_ORG}/reports/{REPORT_ID}/runs"
    headers = {
        "Content-Type": "application/json",
        "Authorization": _auth_header(),
    }

    resp = requests.post(url, json={"parameters": parameters}, headers=headers)
    if not resp.ok:
        raise Exception(f"Mode API error {resp.status_code}: {resp.text[:200]}")

    run_data = resp.json()
    content_path = run_data.get("_links", {}).get("content", {}).get("href")
    if not content_path:
        raise Exception("Mode did not return a content URL")

    content_url = content_path if content_path.startswith("http") else f"https://app.mode.com{content_path}"

    for attempt in range(MAX_POLL_ATTEMPTS):
        time.sleep(POLL_INTERVAL)
        content_resp = requests.get(content_url, headers={"Authorization": _auth_header()})

        if content_resp.status_code == 200:
            raw = content_resp.content

            # Mode returns a ZIP with one CSV per query
            if raw[:2] == b"PK":
                zf = zipfile.ZipFile(io.BytesIO(raw))
                csv_names = [n for n in zf.namelist() if n.endswith(".csv")]

                # Pick the right CSV by query token
                target = None
                if query_token:
                    target = next((n for n in csv_names if query_token in n), None)
                if not target and csv_names:
                    target = csv_names[0]

                if target:
                    csv_text = zf.read(target).decode("utf-8-sig")
                    clean = csv_text.replace("\r\n", "\n").replace("\r", "\n")
                    return list(csv.DictReader(io.StringIO(clean)))

            # Raw CSV fallback
            text = content_resp.text
            if "," in text and "\n" in text and text[:2] != "PK":
                clean = text.replace("\r\n", "\n").replace("\r", "\n")
                return list(csv.DictReader(io.StringIO(clean)))

            # JSON status — keep polling
            try:
                data = content_resp.json()
                if data.get("state") == "succeeded" or data.get("completed_at"):
                    continue
            except Exception:
                pass

    raise Exception(f"Mode report timed out after {MAX_POLL_ATTEMPTS * POLL_INTERVAL}s")


def _escape_sql(value):
    """Escape apostrophes for SQL (matches Enkhjin's escapeForSql)."""
    return str(value).replace("'", "''")


def check_businesses(company_id, business_list):
    """Check which businesses exist for a company via Mode.

    business_list: list of dicts with keys: name, address, store_id
    Returns list of dicts with: name, store_id, address, isNew, business_id, match_type
    """
    if not business_list:
        return []

    business_names = "|||".join(_escape_sql(b["name"]) for b in business_list)
    addresses = "|||".join(_escape_sql(b.get("address", "")) for b in business_list)
    store_ids = "|||".join(str(b.get("store_id", "")) for b in business_list)

    rows = _run_report({
        "company_id": str(company_id),
        "business_names": business_names,
        "addresses": addresses,
        "store_ids": store_ids,
        "business_ids": "",
        "names": "",
        "emails": "",
        "phone_numbers": "",
    }, query_token=BUSINESS_QUERY_TOKEN)

    # Build lookup from Mode results
    result_map = {}
    for row in rows:
        input_name = (row.get("input_business_name") or "").strip()
        if input_name:
            result_map[input_name] = row

    # Map back to our business list
    results = []
    for b in business_list:
        mode_row = result_map.get(b["name"])
        status = (mode_row.get("status") or "NEW").upper() if mode_row else "NEW"
        is_new = status == "NEW" or not mode_row or not mode_row.get("business_id")

        result = {
            **b,
            "isNew": is_new,
            "business_id": mode_row.get("business_id") if mode_row and not is_new else None,
            "existing_business_name": mode_row.get("existing_business_name") if mode_row else None,
            "existing_address": mode_row.get("existing_address") if mode_row else None,
            "match_type": mode_row.get("match_type", "no_match") if mode_row else "no_match",
            "regionmapping_id": mode_row.get("regionmapping_id") if mode_row else None,
        }
        results.append(result)

    return results


def match_contacts(company_id, business_ids=None, phone_numbers=None, emails=None, names=None):
    """Match contacts for a company via Mode.

    Returns list of dicts from Mode's contact report.
    """
    params = {
        "company_id": str(company_id),
        "business_ids": "|||".join(str(x) for x in (business_ids or [])),
        "phone_numbers": "|||".join(str(x) for x in (phone_numbers or [])),
        "emails": "|||".join(str(x) for x in (emails or [])),
        "names": "|||".join(str(x) for x in (names or [])),
        "business_names": "",
        "addresses": "",
        "store_ids": "",
    }

    return _run_report(params, query_token=CONTACTS_QUERY_TOKEN)


def get_businesses_for_company(company_id, parsed_rows):
    """Get existing/new business classification for parsed CSV rows.

    This replaces redshift_client.get_businesses_for_company() + csv_processor.match_businesses()
    when running on Replit. Instead of pulling ALL businesses and matching locally,
    we send our specific stores to Mode and get back existing/new status.

    Returns (matched_rows, unmatched_rows) in the same format as csv_processor.match_businesses().
    """
    from csv_processor import format_business_name

    # Build unique business list from parsed rows
    unique_map = {}
    for i, row in enumerate(parsed_rows):
        store_num = row.get("store_number", "").strip().lstrip("#")
        retailer = row.get("retailer", "").strip()
        name = format_business_name(retailer, store_num)

        addr_parts = [row.get("address", ""), row.get("city", ""),
                      row.get("state", ""), row.get("zip", "")]
        address = ", ".join(p for p in addr_parts if p)

        if store_num and name not in unique_map:
            unique_map[name] = {
                "name": name,
                "store_id": store_num,
                "address": address,
            }

    business_list = list(unique_map.values())

    # Query Mode
    mode_results = check_businesses(company_id, business_list)

    # Build lookup: business name → Mode result
    result_by_name = {r["name"]: r for r in mode_results}

    # Classify parsed rows
    matched = []
    unmatched = []
    for row in parsed_rows:
        store_num = row.get("store_number", "").strip().lstrip("#")
        retailer = row.get("retailer", "").strip()
        name = format_business_name(retailer, store_num)

        mode_row = result_by_name.get(name, {})

        if not mode_row.get("isNew", True) and mode_row.get("business_id"):
            biz = {
                "business_id": int(mode_row["business_id"]),
                "business_name": mode_row.get("existing_business_name") or name,
                "address": mode_row.get("existing_address") or "",
            }
            matched.append({
                **row,
                "_business": biz,
                "_status": "existing",
                "_match_method": mode_row.get("match_type", "mode"),
            })
        else:
            unmatched.append({
                **row,
                "_status": "new",
                "_expected_name": name,
            })

    return matched, unmatched
