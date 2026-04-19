"""Local contacts lookup, sourced from Christian's Company Users sheet export.

This is the data Redshift's auth_user table doesn't have (names/phones/emails
are blank in the mirror). We use it as the primary source for contact matching
and as the fallback for default_contact_id during partner pre-fill.
"""

import json
import os
import re

CONTACTS_PATH = os.path.join(os.path.dirname(__file__), "partner_configs", "contacts.json")

_cache = None


def _load():
    global _cache
    if _cache is None:
        if os.path.exists(CONTACTS_PATH):
            with open(CONTACTS_PATH) as f:
                _cache = json.load(f)
        else:
            _cache = {}
    return _cache


def get_contacts(company_id):
    """Return list of contacts for a company, or empty list."""
    data = _load()
    entry = data.get(str(company_id), {})
    return entry.get("contacts", [])


def get_company_name(company_id):
    """Return the company name we have on file for this ID."""
    data = _load()
    return data.get(str(company_id), {}).get("company_name", "")


def _normalize_phone(phone):
    """Strip phone to digits only for matching."""
    return re.sub(r"\D", "", phone or "")


def match_contacts(company_id, query_names, query_phones=None):
    """Match a list of names (and optional phones) to contacts for this company.

    Returns list of {query_name, match_type, contact} for each input name.
    match_type is one of: exact, fallback, unmatched.
    """
    contacts = get_contacts(company_id)
    if not contacts:
        return [{"query_name": n, "match_type": "unmatched", "contact": None} for n in query_names]

    # Build lookups
    by_full_name = {}
    by_last_name = {}
    by_phone = {}
    for c in contacts:
        full = (c.get("name") or "").strip().lower()
        if full:
            by_full_name[full] = c
            parts = full.split()
            if len(parts) >= 2:
                by_last_name.setdefault(parts[-1], c)
        phone_digits = _normalize_phone(c.get("phone", ""))
        if phone_digits:
            by_phone[phone_digits] = c

    query_phones = query_phones or [""] * len(query_names)
    results = []
    for i, name in enumerate(query_names):
        name_lower = (name or "").strip().lower()
        phone_digits = _normalize_phone(query_phones[i] if i < len(query_phones) else "")

        contact = None
        match_type = "unmatched"

        # 1. Exact phone match wins (most reliable)
        if phone_digits and len(phone_digits) >= 10:
            phone_key = phone_digits[-10:]
            for p, c in by_phone.items():
                if p.endswith(phone_key) or phone_key.endswith(p[-10:] if len(p) >= 10 else p):
                    contact = c
                    match_type = "exact"
                    break

        # 2. Exact full-name match
        if not contact and name_lower in by_full_name:
            contact = by_full_name[name_lower]
            match_type = "exact"

        # 3. Last-name match (fallback)
        if not contact and name_lower:
            parts = name_lower.split()
            if parts and parts[-1] in by_last_name:
                contact = by_last_name[parts[-1]]
                match_type = "fallback"

        # 4. Substring match (looser fallback)
        if not contact and name_lower:
            for full, c in by_full_name.items():
                if name_lower in full or full in name_lower:
                    contact = c
                    match_type = "fallback"
                    break

        results.append({
            "query_name": name,
            "match_type": match_type,
            "contact": contact,
        })

    return results


def get_default_contact(company_id):
    """Pick a default contact for a company (first ADMIN, then SHIFT_COORDINATOR, then any)."""
    contacts = get_contacts(company_id)
    if not contacts:
        return None
    role_priority = {"ADMIN": 0, "SHIFT_COORDINATOR": 1, "MEMBER": 2}
    sorted_contacts = sorted(contacts, key=lambda c: role_priority.get(c.get("role", ""), 99))
    return sorted_contacts[0] if sorted_contacts else None
