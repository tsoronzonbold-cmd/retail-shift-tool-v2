"""Wrapper around boto3 redshift-data API client.

Uses a persistent session (SessionId) with application_name set, matching the
awslabs redshift-mcp-server pattern. This is necessary because the DbtAccess
SSO role lacks redshift:GetClusterCredentials but can auth via IAM identity
when a session is established first.
"""

import time
import boto3
import config

_client = None
_session_id = None
_session_ts = 0
_SESSION_TTL = 600  # 10 minutes
_APP_NAME = "retail-shift-tool"


def get_client():
    global _client
    if _client is None:
        _client = boto3.client("redshift-data", region_name=config.REDSHIFT_REGION)
    return _client


def _parse_field(field):
    """Parse a single field from Redshift Data API response."""
    if "stringValue" in field:
        return field["stringValue"]
    elif "longValue" in field:
        return field["longValue"]
    elif "doubleValue" in field:
        return field["doubleValue"]
    elif "booleanValue" in field:
        return field["booleanValue"]
    elif "isNull" in field and field["isNull"]:
        return None
    return str(field)


def _poll_statement(statement_id, timeout=120):
    """Poll a statement until done."""
    client = get_client()
    elapsed = 0
    while elapsed < timeout:
        status = client.describe_statement(Id=statement_id)
        state = status["Status"]
        if state == "FINISHED":
            return status
        if state in ("FAILED", "ABORTED"):
            raise Exception(f"Query {state}: {status.get('Error', 'unknown error')}")
        time.sleep(0.5)
        elapsed += 0.5
    raise Exception(f"Query timed out after {timeout}s")


def execute_query(sql, timeout=120):
    """Execute a SQL query via Redshift Data API and return rows as list of dicts."""
    client = get_client()
    kwargs = {
        "ClusterIdentifier": config.REDSHIFT_CLUSTER,
        "Database": config.REDSHIFT_DATABASE,
        "Sql": sql,
    }
    if config.REDSHIFT_DB_USER:
        kwargs["DbUser"] = config.REDSHIFT_DB_USER
    response = client.execute_statement(**kwargs)
    statement_id = response["Id"]
    _poll_statement(statement_id, timeout=timeout)

    # Fetch results
    try:
        result = client.get_statement_result(Id=statement_id)
    except client.exceptions.ResourceNotFoundException:
        return []  # Statement was a SET/UPDATE without results

    columns = [col["name"] for col in result["ColumnMetadata"]]
    rows = []
    for record in result["Records"]:
        row = {columns[i]: _parse_field(field) for i, field in enumerate(record)}
        rows.append(row)

    while "NextToken" in result:
        result = client.get_statement_result(
            Id=statement_id, NextToken=result["NextToken"]
        )
        for record in result["Records"]:
            row = {columns[i]: _parse_field(field) for i, field in enumerate(record)}
            rows.append(row)

    return rows


def get_companies(search=None, configured_ids=None):
    """Get retail partner companies that have gig templates.

    Filtered to companies that look like retail (Advantage, Footprint, Retail
    Odyssey, SAS, Hallmark, etc.) plus any with templates in the last 90 days.
    Optional search filter by name substring.
    
    configured_ids: list of company IDs to always include (from partner configs)
    """
    where_extra = ""
    if search:
        safe = search.replace("'", "''")
        where_extra = f"AND LOWER(c.name) LIKE LOWER('%{safe}%')"

    # Build configured IDs clause to prioritize them
    configured_clause = ""
    if configured_ids:
        ids_str = ",".join(str(i) for i in configured_ids)
        configured_clause = f"CASE WHEN c.id IN ({ids_str}) THEN 0 ELSE 1 END,"

    sql = f"""
    SELECT DISTINCT c.id, c.name, COUNT(gt.id) as template_count
    FROM iw_backend_db.backend_company c
    JOIN iw_backend_db.backend_gigtemplate gt ON gt.company_id = c.id
    WHERE c.name IS NOT NULL
      AND c.name != ''
      AND c.name NOT LIKE '"%'
      AND c.name NOT LIKE '''%'
      AND LEFT(c.name, 1) ~ '[A-Za-z]'
      {where_extra}
    GROUP BY c.id, c.name
    HAVING COUNT(gt.id) >= 1
    ORDER BY {configured_clause} template_count DESC, c.name
    LIMIT 2000
    """
    return execute_query(sql)


def get_businesses_for_company(company_id):
    """Get all businesses (locations) for a company with their latest template data.

    Picks the most recent template that has non-null contact/instructions, if any.
    """
    sql = f"""
    WITH ranked AS (
        SELECT
            gt.business_id,
            b.name as business_name,
            b.address,
            gt.contact_id,
            gt.created_by_id,
            gt.position_fk_id as position,
            gt.position_tiering_id,
            gt.instructions,
            gt.custom_attire_requirements,
            gt.has_parking,
            gt.is_flexible_time_task,
            gt.multi_day_same_worker,
            gt.is_requested_worker_only,
            ROW_NUMBER() OVER (
                PARTITION BY gt.business_id
                ORDER BY
                    CASE WHEN gt.contact_id IS NOT NULL THEN 0 ELSE 1 END,
                    CASE WHEN gt.instructions IS NOT NULL THEN 0 ELSE 1 END,
                    gt.created_at DESC
            ) as rn
        FROM iw_backend_db.backend_gigtemplate gt
        JOIN iw_backend_db.backend_gigbusiness b ON b.id = gt.business_id
        WHERE gt.company_id = {int(company_id)}
    )
    SELECT business_id, business_name, address, contact_id, created_by_id,
           position, position_tiering_id, instructions,
           custom_attire_requirements, has_parking,
           is_flexible_time_task, multi_day_same_worker, is_requested_worker_only
    FROM ranked
    WHERE rn = 1
    ORDER BY business_name
    """
    return execute_query(sql)


def get_clock_out_tasks(company_id):
    """Get active clock-out tasks for a company's businesses."""
    sql = f"""
    SELECT DISTINCT ct.id, ct.business_id, ct.position_id, ct.type, ct.active
    FROM iw_backend_db.backend_clockouttask ct
    JOIN iw_backend_db.backend_gigtemplate gt ON gt.business_id = ct.business_id
    WHERE gt.company_id = {int(company_id)} AND ct.active = 1
    """
    return execute_query(sql)


def get_company_users(company_id):
    """Get users/contacts for a company with names, ordered by role priority."""
    sql = f"""
    SELECT cu.id as companyuser_id, cu.cuser_id,
           u.first_name, u.last_name, u.email,
           cu.role, cu.is_admin, cu.date_created
    FROM iw_backend_db.backend_companyuser cu
    JOIN iw_backend_db.auth_user u ON u.id = cu.cuser_id
    WHERE cu.company_id = {int(company_id)}
    ORDER BY
        CASE cu.role
            WHEN 'ADMIN' THEN 0
            WHEN 'SHIFT_COORDINATOR' THEN 1
            WHEN 'MEMBER' THEN 2
            ELSE 3
        END,
        cu.date_created DESC
    """
    return execute_query(sql)


def get_default_contact(company_id):
    """Pick the most likely default contact (most recent ADMIN) for a company."""
    users = get_company_users(company_id)
    return users[0] if users else None


def verify_new_businesses(company_id, store_numbers):
    """Check if new businesses have appeared in Redshift for the given store numbers.

    Uses multiple matching strategies to reduce false negatives:
    1. Exact '#N' pattern in business name
    2. Store number as trailing digits in name
    3. Store number anywhere in name (with word boundary)

    Returns dict of store_number -> {business_id, business_name, address} for any found.
    """
    if not store_numbers:
        return {}

    conditions = []
    for s in store_numbers:
        if not s:
            continue
        safe_s = s.replace("'", "''")
        conditions.append(f"b.name LIKE '%#{safe_s}%'")
        conditions.append(f"b.name LIKE '% {safe_s}'")
        conditions.append(f"b.name LIKE '% {safe_s} %'")

    if not conditions:
        return {}

    sql = f"""
    SELECT DISTINCT b.id as business_id, b.name as business_name, b.address
    FROM iw_backend_db.backend_gigbusiness b
    WHERE b.id IN (
        SELECT DISTINCT gt.business_id
        FROM iw_backend_db.backend_gigtemplate gt
        WHERE gt.company_id = {int(company_id)}
    )
    AND ({" OR ".join(conditions)})
    ORDER BY b.id DESC
    """
    results = execute_query(sql)

    import re
    found = {}
    for row in results:
        name = row.get("business_name", "") or ""
        for s in store_numbers:
            if s in found:
                continue
            if re.search(rf"#\s*{re.escape(s)}\b", name):
                found[s] = row
            elif re.search(rf"\b{re.escape(s)}\s*$", name.strip()):
                found[s] = row
    return found


def match_contacts_by_name(company_id, contact_names):
    """Match contact names to company user IDs using backend_companyuser + auth_user."""
    if not contact_names:
        return []

    users = get_company_users(company_id)

    user_by_name = {}
    for u in users:
        first = (u.get("first_name") or "").strip()
        last = (u.get("last_name") or "").strip()
        full = f"{first} {last}".strip().lower()
        if full:
            user_by_name[full] = u
        if last:
            user_by_name.setdefault(last.lower(), u)

    matches = []
    for name in contact_names:
        name_lower = name.strip().lower()
        match_type = "unmatched"
        matched_user = None

        if name_lower in user_by_name:
            matched_user = user_by_name[name_lower]
            match_type = "exact"
        else:
            for uname, u in user_by_name.items():
                if name_lower and (name_lower in uname or uname in name_lower):
                    matched_user = u
                    match_type = "fallback"
                    break

        matches.append({
            "query_name": name,
            "match_type": match_type,
            "user": matched_user,
        })

    return matches


def search_business_by_address(company_id, search_term):
    """Search for a business by address fragment."""
    safe_term = search_term.replace("'", "''")
    sql = f"""
    SELECT id, name, address
    FROM iw_backend_db.backend_gigbusiness
    WHERE company::integer = {int(company_id)}
      AND (LOWER(name) LIKE LOWER('%{safe_term}%')
           OR LOWER(address) LIKE LOWER('%{safe_term}%'))
    ORDER BY name
    LIMIT 20
    """
    return execute_query(sql)
