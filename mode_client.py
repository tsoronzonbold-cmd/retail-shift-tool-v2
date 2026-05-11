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

REPORT_ID = "ac9b652e687f"  # Single report with all queries
BUSINESS_QUERY_TOKEN = "6ec26c5336ee"   # Query 1: business check
CONTACTS_QUERY_TOKEN = "e62e61be97f5"   # Query 2: contact lookup
COMPANIES_QUERY_TOKEN = "eca03db2f4ec"  # Query 3: companies list
BOOTSTRAP_QUERY_TOKEN = "d2b95ef75b11"  # Query 4: bootstrap partner

# How long to poll Mode for results
MAX_POLL_ATTEMPTS = 30
POLL_INTERVAL = 4  # seconds — total ceiling 2 min


def is_available():
    """Check if Mode API credentials are configured."""
    return bool(MODE_API_KEY and MODE_API_SECRET)


def _auth_header():
    creds = base64.b64encode(f"{MODE_API_KEY}:{MODE_API_SECRET}".encode()).decode()
    return f"Basic {creds}"


_QUERY_NAMES = {
    BUSINESS_QUERY_TOKEN: "business_check",
    CONTACTS_QUERY_TOKEN: "contact_lookup",
    COMPANIES_QUERY_TOKEN: "companies_list",
    BOOTSTRAP_QUERY_TOKEN: "bootstrap_partner",
}


def _run_report(parameters, query_token=None):
    """Run the Mode report and poll until results are ready.

    All four queries live inside one report. We fetch results via the
    per-query endpoint (.../runs/{run}/query_runs/{qr}/results/content.csv)
    rather than the report-level content.csv, because the report ZIP
    occasionally omits queries (Mode's behavior — even successful runs
    aren't guaranteed to land in the bundle).

    Use query_token to select which query's results to return.
    Returns parsed CSV rows as list of dicts.
    """
    t0 = time.time()
    qname = _QUERY_NAMES.get(query_token, query_token or "unknown")
    try:
        result = _run_report_inner(parameters, query_token)
        _log_mode(qname, t0, True, "", len(result))
        return result
    except Exception as e:
        _log_mode(qname, t0, False, str(e)[:200], 0)
        raise


def _log_mode(name, t0, success, err, rows):
    try:
        import usage_db
        usage_db.log_mode_call(name, int((time.time() - t0) * 1000), success, err, rows)
    except Exception:
        pass


def _run_report_inner(parameters, query_token=None):
    url = f"https://app.mode.com/api/{MODE_ORG}/reports/{REPORT_ID}/runs"
    headers = {
        "Content-Type": "application/json",
        "Authorization": _auth_header(),
    }

    resp = requests.post(url, json={"parameters": parameters}, headers=headers)
    if not resp.ok:
        raise Exception(f"Mode API error {resp.status_code}: {resp.text[:200]}")

    run_token = resp.json().get("token")
    if not run_token:
        raise Exception("Mode did not return a run token")

    # Poll the run itself until it succeeds
    run_url = f"{url}/{run_token}"
    auth = {"Authorization": _auth_header()}
    state = None
    for _ in range(MAX_POLL_ATTEMPTS):
        time.sleep(POLL_INTERVAL)
        run_resp = requests.get(run_url, headers=auth)
        run_data = run_resp.json()
        state = run_data.get("state")
        if state in ("succeeded", "failed", "cancelled"):
            break

    if state != "succeeded":
        raise Exception(f"Mode run did not succeed (state={state})")

    # Find the query_run that matches our query_token, then fetch its CSV
    qr_path = run_data["_links"]["query_runs"]["href"]
    qr_url = qr_path if qr_path.startswith("http") else f"https://app.mode.com{qr_path}"
    qr_data = requests.get(qr_url, headers=auth).json()

    query_runs = qr_data.get("_embedded", {}).get("query_runs", [])
    target = None
    if query_token:
        target = next((q for q in query_runs if q.get("query_token") == query_token), None)
    if not target and query_runs:
        target = query_runs[0]
    if not target:
        raise Exception("Mode returned no query_runs for this report")

    result_path = target["_links"]["result"]["href"]
    csv_url = f"https://app.mode.com{result_path}/content.csv"
    csv_resp = requests.get(csv_url, headers=auth)
    if not csv_resp.ok:
        raise Exception(f"Mode CSV fetch failed: {csv_resp.status_code} {csv_resp.text[:200]}")

    csv_text = csv_resp.content.decode("utf-8-sig")
    clean = csv_text.replace("\r\n", "\n").replace("\r", "\n")
    return list(csv.DictReader(io.StringIO(clean)))


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


def companies_query_available():
    """True iff the COMPANIES query token has been wired up in Mode."""
    return is_available() and bool(COMPANIES_QUERY_TOKEN)


def bootstrap_query_available():
    """True iff the BOOTSTRAP query token has been wired up in Mode."""
    return is_available() and bool(BOOTSTRAP_QUERY_TOKEN)


def get_companies(search=None, configured_ids=None):
    """List Instawork companies via Mode (replaces redshift_client.get_companies).

    Returns list of dicts: {id, name, configured (bool)}.
    Filters out empty names. Sorted by name.
    """
    if not companies_query_available():
        return []

    params = {
        "company_id": "",
        "business_names": "",
        "addresses": "",
        "store_ids": "",
        "business_ids": "",
        "names": "",
        "emails": "",
        "phone_numbers": "",
        "search": search or "",
    }
    rows = _run_report(params, query_token=COMPANIES_QUERY_TOKEN)

    configured_set = set(str(c) for c in (configured_ids or []))
    out = []
    for r in rows:
        cid = str(r.get("id") or r.get("company_id") or "").strip()
        name = (r.get("name") or r.get("company_name") or "").strip()
        if not cid or not name:
            continue
        out.append({
            "id": int(cid) if cid.isdigit() else cid,
            "name": name,
            "configured": cid in configured_set,
        })
    return out


def bootstrap_partner(company_id):
    """Pull company name + most-recent gigtemplate defaults from Mode.

    Replaces the two redshift_client.execute_query() calls used during
    auto-bootstrap and the /bootstrap-partner route. Returns a dict with
    every field the partner_config setup needs, or {} if not found / not
    available.
    """
    if not bootstrap_query_available() or not company_id:
        return {}

    params = {
        "company_id": str(company_id),
        "business_names": "",
        "addresses": "",
        "store_ids": "",
        "business_ids": "",
        "names": "",
        "emails": "",
        "phone_numbers": "",
        "search": "",
    }
    rows = _run_report(params, query_token=BOOTSTRAP_QUERY_TOKEN)
    if not rows:
        return {}

    r = rows[0]

    def _int_or_none(v):
        try:
            return int(v) if v not in (None, "") else None
        except (ValueError, TypeError):
            return None

    return {
        "company_name": (r.get("company_name") or "").strip(),
        "default_contact_id": _int_or_none(r.get("contact_id")),
        "default_creator_id": _int_or_none(r.get("created_by_id")),
        "default_position_id": _int_or_none(r.get("position_fk_id")) or 29,
        "default_position_tiering_id": _int_or_none(r.get("position_tiering_id")),
        "default_parking": _int_or_none(r.get("has_parking")) if r.get("has_parking") not in (None, "") else 2,
        "default_position_instructions": (r.get("instructions") or "").strip(),
        "default_attire": (r.get("custom_attire_requirements") or "").strip(),
    }


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
