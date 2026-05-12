"""Retail Shift Tool V2 — unified Flask app for retail shift bulk uploads.

Streamlined 3-step flow:
  1. Upload   — pick partner, upload CSV
  2. Results  — auto-detect columns, match businesses, match contacts,
                show everything pre-filled from partner config
  3. Download — download all CSVs (businesses, tasks, shifts)
"""

import os
import io
import csv
import json
import re
import time


def _safe_filename_part(name, fallback="partner"):
    """Sanitize a partner/company name for use in a download filename.

    Django's bulk gig request importer chokes on spaces; some other
    importers also balk at apostrophes / parens / ampersands (e.g.
    "Smith's", "Acosta - Publix (S Florida)"). Allow only alphanumerics,
    collapse runs of separators, and strip leading/trailing underscores.
    """
    if not name:
        return fallback
    cleaned = re.sub(r"[^A-Za-z0-9]+", "_", str(name)).strip("_")
    return cleaned or fallback

# Load .env file for Mode API credentials
from dotenv import load_dotenv
load_dotenv()

from flask import (
    Flask, render_template, request, redirect, url_for,
    session, send_file, flash, jsonify, abort,
)
import pandas as pd

import config
import mode_client
import locations_db
import partner_config as pc
import csv_processor
import usage_db
import contacts_db
import google_places
import roster_db
import ai_mapper

app = Flask(__name__)
app.secret_key = config.SECRET_KEY
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16MB max upload

app.config["SESSION_TYPE"] = "filesystem"
app.config["SESSION_FILE_DIR"] = "/tmp/retail-shift-sessions"
app.config["SESSION_PERMANENT"] = False
from flask_session import Session
Session(app)

os.makedirs(config.UPLOAD_FOLDER, exist_ok=True)
os.makedirs(app.config["SESSION_FILE_DIR"], exist_ok=True)

DJANGO_BASE = "https://admin.instawork.com/backend"
DJANGO_LINKS = {
    "business_import": f"{DJANGO_BASE}/business/import/",
    "clockout_task": f"{DJANGO_BASE}/clockouttask/import/",
    "company_user": f"{DJANGO_BASE}/companyuser/import/",
    "bulk_import": f"{DJANGO_BASE}/bulkgigrequest/add/",
}


def _parse_id_list(s):
    if not s or not s.strip():
        return []
    return [int(x.strip()) for x in s.split(",") if x.strip().isdigit()]


def _get_quantity(row):
    qty = row.get("quantity") or row.get("requested_workers") or 1
    try:
        return int(qty) if str(qty).strip() else 1
    except (ValueError, TypeError):
        return 1


# ══════════════════════════════════════════════════════════════════════
# STEP 1: Upload
# ══════════════════════════════════════════════════════════════════════

@app.route("/", methods=["GET"])
def index():
    configs = pc.load_configs()
    configured = [
        {
            "id": int(cid),
            "name": cfg.get("name") or f"Company {cid}",
            "has_duties": bool(cfg.get("default_position_duties")),
            "has_attire": bool(cfg.get("default_attire")),
            "rate": cfg.get("adjusted_base_rate"),
        }
        for cid, cfg in configs.items()
    ]
    configured.sort(key=lambda c: c["name"])
    configured_ids = [int(cid) for cid in configs.keys()]

    # Mode is the authoritative source.
    all_companies = []
    try:
        all_companies = mode_client.get_companies(configured_ids=configured_ids)
    except Exception:
        pass

    return render_template(
        "upload.html",
        companies=all_companies or configured,
        configured=configured,
        configured_ids=configured_ids,
    )


@app.route("/upload", methods=["POST"])
def upload():
    """Parse CSV, detect businesses, match contacts — all in one shot.
    Redirects to /results with everything ready."""
    company_id = request.form.get("company_id")
    if not company_id:
        flash("Please select a partner.", "error")
        return redirect(url_for("index"))

    file = request.files.get("file")
    if not file or file.filename == "":
        flash("Please upload a file.", "error")
        return redirect(url_for("index"))

    file_content = file.read()
    filename = file.filename
    cfg = pc.get_config(company_id)

    # Auto-bootstrap unconfigured partners via Mode.
    if not cfg.get("name"):
        try:
            bootstrap = mode_client.bootstrap_partner(company_id)
            if bootstrap:
                cfg["name"] = bootstrap.get("company_name") or cfg.get("name", "")
                for key in (
                    "default_contact_id", "default_creator_id", "default_position_id",
                    "default_position_tiering_id", "default_parking",
                    "default_position_instructions", "default_attire",
                ):
                    if bootstrap.get(key) not in (None, ""):
                        cfg[key] = bootstrap[key]
                pc.save_config(company_id, cfg)
                flash(f"Auto-configured new partner: {cfg['name']}", "success")
        except Exception:
            pass

    col_mapping = cfg.get("column_mapping", {})

    # Parse file
    if filename.endswith((".xlsx", ".xls")):
        df = pd.read_excel(io.BytesIO(file_content))
    else:
        text = file_content.decode("utf-8-sig")
        df = pd.read_csv(io.StringIO(text))

    detected_mapping = csv_processor.auto_detect_columns(list(df.columns))

    # AI mapper runs on every upload — partner CSVs are too varied for
    # regex to keep up, and gpt-4o-mini is cheap enough that the cost
    # doesn't matter. The model sees regex's guess and overrides on
    # overlap when its mapping is better.
    sample = df.head(10).to_dict("records")
    ai_result = ai_mapper.maybe_ai_map(
        list(df.columns), sample, detected_mapping,
        partner_name=cfg.get("name", "")
    )
    detected_mapping = ai_result["mapping"]
    status = ai_result.get("status", "")
    confidence = ai_result.get("confidence")
    reasoning = ai_result.get("reasoning") or {}
    ai_added = ai_result.get("ai_added", [])
    ai_changed = ai_result.get("ai_changed", [])
    ai_status_recorded = status
    ai_filled_keys = list(ai_added) + [c[0] for c in ai_changed]

    if status == "ok" and (ai_added or ai_changed):
        parts = []
        for k in sorted(ai_added):
            why = f" ({reasoning[k]})" if reasoning.get(k) else ""
            parts.append(f"+{k}={detected_mapping[k]!r}{why}")
        for k, old, new in ai_changed:
            why = f" ({reasoning[k]})" if reasoning.get(k) else ""
            parts.append(f"~{k}: {old!r}→{new!r}{why}")
        category = "success" if confidence == "high" else "warning"
        conf_str = f" (confidence: {confidence})" if confidence else ""
        flash(f"AI fixed {len(ai_added) + len(ai_changed)} column(s){conf_str}: {'; '.join(parts)}", category)
    elif status == "no_key":
        flash(f"⚠ AI is offline — OPENAI_API_KEY not set. Regex matched {len(detected_mapping)} column(s); some may be missing. Check the AI pill in the header.", "error")
    elif status == "error":
        flash(f"⚠ AI call failed — {ai_result.get('error', 'unknown error')}. Falling back to regex (some columns may be wrong). Hover the AI pill for details.", "error")

    final_mapping = {**detected_mapping, **{k: v for k, v in col_mapping.items() if v in df.columns}}
    rows, raw_columns = csv_processor.parse_upload(file_content, filename, final_mapping)

    # Detect businesses — Mode is the authoritative live source.
    #   1. Local DB (Christian's 127K locations, instant, by companyId-storeNumber)
    #   2. Mode API (live Redshift query, handles name variations)
    #   3. Direct Redshift only when Mode is NOT configured (legacy fallback)
    redshift_error = None
    mode_ran = False
    matched, unmatched = locations_db.match_businesses(company_id, rows)

    # If local DB left unmatched rows, ask Mode (the authoritative source)
    if unmatched and mode_client.is_available():
        try:
            mode_matched, still_unmatched = mode_client.get_businesses_for_company(company_id, unmatched)
            matched = matched + mode_matched
            unmatched = still_unmatched
            mode_ran = True
        except Exception as me:
            redshift_error = f"Mode error: {str(me)}"

    # Sanity check: zero matches but the partner has many known businesses
    # almost always means the wrong partner was picked from the dropdown
    # (this is how Zoe ended up creating 38 "new" DSD-Central stores under
    # the CO-Remodels partner). Warn loudly before they walk through the
    # new-business flow.
    if matched == [] and len(unmatched) >= 3:
        known = locations_db.known_business_count(company_id)
        if known >= 20:
            flash(
                f"⚠ None of your {len(unmatched)} stores matched — but "
                f"this partner has {known} known businesses. Did you pick "
                f"the right partner from the dropdown? Filename or banner "
                f"often hints at the actual partner.",
                "error",
            )

    # Match contacts
    seen = set()
    contact_names, contact_phones = [], []
    for row in matched + unmatched:
        name = (row.get("team_lead") or "").strip()
        phone = (row.get("team_lead_phone") or "").strip()
        key = (name.lower(), phone)
        if name and key not in seen:
            seen.add(key)
            contact_names.append(name)
            contact_phones.append(phone)

    contact_matches = []
    try:
        local_matches = contacts_db.match_contacts(company_id, contact_names, contact_phones)
        for m in local_matches:
            c = m["contact"]
            user = None
            if c:
                full = (c.get("name") or "").split(" ", 1)
                user = {
                    "first_name": full[0] if full else "",
                    "last_name": full[1] if len(full) > 1 else "",
                    "cuser_id": c["cuser_id"],
                    "phone_number": c.get("phone", ""),
                    "email": c.get("email", ""),
                    "role": c.get("role", ""),
                }
            contact_matches.append({
                "query_name": m["query_name"],
                "match_type": m["match_type"],
                "user": user,
            })
    except Exception:
        pass

    # Apply contact IDs to rows
    contact_lookup = {}
    for m in contact_matches:
        if m.get("user"):
            contact_lookup[m["query_name"].lower()] = m["user"]["cuser_id"]
    for row in matched:
        tl = row.get("team_lead", "").strip().lower()
        if tl in contact_lookup:
            row["_contact_id"] = contact_lookup[tl]

    # Validate new business addresses via Google Places
    address_validation = []
    if unmatched:
        try:
            address_validation = google_places.validate_new_businesses(unmatched)
        except Exception:
            pass

    # Generate missing contacts upload CSV
    missing_contacts_csv = None
    unmatched_contact_names = [m for m in contact_matches if m["match_type"] == "unmatched" and m["query_name"]]
    if unmatched_contact_names:
        mc_rows = []
        seen_contacts = set()
        for m in unmatched_contact_names:
            name = m["query_name"].strip()
            if not name or name.lower() in seen_contacts:
                continue
            seen_contacts.add(name.lower())
            # Find phone/email from rows
            phone = ""
            email = ""
            for row in matched + unmatched:
                if (row.get("team_lead") or "").strip().lower() == name.lower():
                    phone = row.get("team_lead_phone", "")
                    email = row.get("team_lead_email", "")
                    break
            parts = name.split(None, 1)
            first = parts[0] if parts else ""
            last = parts[1] if len(parts) > 1 else "A"
            # Format phone as (xxx) xxx-xxxx
            digits = contacts_db._normalize_phone(phone)
            if len(digits) >= 10:
                d = digits[-10:]
                phone_fmt = f"({d[:3]}) {d[3:6]}-{d[6:]}"
            else:
                phone_fmt = phone
            mc_rows.append([company_id, "BOOKING_SHIFT_COORDINATOR", first, last, email, phone_fmt])
        if mc_rows:
            mc_output = io.StringIO()
            mc_writer = csv.writer(mc_output)
            mc_writer.writerow(["company", "role", "given_name", "family_name", "email", "phonenum"])
            for r in mc_rows:
                mc_writer.writerow(r)
            missing_contacts_csv = mc_output.getvalue()

    # Pre-generate shift import CSV (matched rows only — never blocked on
    # the new-business flow)
    cfg["_company_id"] = company_id
    task_opts = {
        "is_task": request.form.get("is_task_request") == "1",
        "is_anywhere": request.form.get("is_anywhere") == "1",
    }
    shift_csv = csv_processor.generate_bulk_import_csv(matched, cfg, task_opts=task_opts)

    # Store everything in session — business CSV is generated AFTER the
    # configure step (or immediately on results if there are no new
    # businesses to configure)
    session["company_id"] = company_id
    session["company_name"] = cfg.get("name", f"Company {company_id}")
    session["parsed_rows"] = rows
    session["matched_rows"] = matched
    session["unmatched_rows"] = unmatched
    session["contact_matches"] = contact_matches
    session["business_import_csv"] = None
    session["shift_import_csv"] = shift_csv
    session["config"] = cfg
    session["missing_contacts_csv"] = missing_contacts_csv
    session["redshift_error"] = redshift_error
    session["address_validation"] = address_validation
    session["column_mapping"] = final_mapping
    session["raw_columns"] = raw_columns
    session["new_business_config"] = None
    session["sr_csv"] = None
    session["cert_csv"] = None
    session["training_csv"] = None
    session["tasks_csv"] = None
    session["is_task_request"] = task_opts["is_task"]
    session["is_anywhere"] = task_opts["is_anywhere"]

    usage_db.log_event(
        "upload",
        company_id=company_id,
        company_name=cfg.get("name", ""),
        filename=filename,
        rows_total=len(rows),
        rows_matched=len(matched),
        rows_unmatched=len(unmatched),
        ai_fired=(ai_status_recorded in ("ok", "error")),
        ai_status=ai_status_recorded,
        ai_filled_keys=ai_filled_keys,
        mode_used=mode_ran,
        success=(redshift_error is None),
        error_msg=redshift_error or "",
    )

    # Fork: if there are new businesses, route through the configuration
    # step before downloads. Otherwise straight to results.
    if unmatched:
        return redirect(url_for("configure_new_businesses"))
    return redirect(url_for("results"))


# ══════════════════════════════════════════════════════════════════════
# STEP 2: Results — one page showing everything
# ══════════════════════════════════════════════════════════════════════

@app.route("/results", methods=["GET"])
def results():
    company_id = session.get("company_id")
    company_name = session.get("company_name", "")
    matched = session.get("matched_rows", [])
    unmatched = session.get("unmatched_rows", [])
    contact_matches = session.get("contact_matches", [])
    cfg = session.get("config", {})
    redshift_error = session.get("redshift_error")
    parsed_rows = session.get("parsed_rows", [])

    if not parsed_rows:
        flash("No data. Please upload a file first.", "error")
        return redirect(url_for("index"))

    # Deduplicate stores for display
    seen_existing, seen_new = set(), set()
    unique_matched, unique_unmatched = [], []
    for row in matched:
        key = (row.get("store_number", ""), row.get("retailer", ""))
        if key not in seen_existing:
            seen_existing.add(key)
            unique_matched.append(row)
    for row in unmatched:
        key = (row.get("store_number", ""), row.get("retailer", ""))
        if key not in seen_new:
            seen_new.add(key)
            unique_unmatched.append(row)

    total_shifts = sum(_get_quantity(r) for r in matched)

    # Contact stats
    exact = sum(1 for m in contact_matches if m["match_type"] == "exact")
    fallback = sum(1 for m in contact_matches if m["match_type"] == "fallback")
    unmatched_contacts = sum(1 for m in contact_matches if m["match_type"] == "unmatched")

    has_biz_csv = bool(session.get("business_import_csv"))
    has_shift_csv = bool(session.get("shift_import_csv"))
    has_missing_contacts = bool(session.get("missing_contacts_csv"))
    has_sr_csv = bool(session.get("sr_csv"))
    has_cert_csv = bool(session.get("cert_csv"))
    has_training_csv = bool(session.get("training_csv"))
    has_tasks_csv = bool(session.get("tasks_csv"))

    # Address validation lookup
    address_validation = session.get("address_validation", [])
    addr_by_store = {v["store_number"]: v for v in address_validation}

    return render_template(
        "results.html",
        company_name=company_name,
        company_id=company_id,
        config=cfg,
        total_rows=len(parsed_rows),
        total_shifts=total_shifts,
        matched=matched,
        unmatched=unmatched,
        unique_matched=unique_matched,
        unique_unmatched=unique_unmatched,
        num_existing=len(seen_existing),
        num_new=len(seen_new),
        contact_matches=contact_matches,
        exact_contacts=exact,
        fallback_contacts=fallback,
        unmatched_contacts=unmatched_contacts,
        has_biz_csv=has_biz_csv,
        has_shift_csv=has_shift_csv,
        has_missing_contacts=has_missing_contacts,
        has_sr_csv=has_sr_csv,
        has_cert_csv=has_cert_csv,
        has_training_csv=has_training_csv,
        has_tasks_csv=has_tasks_csv,
        redshift_error=redshift_error,
        django_links=DJANGO_LINKS,
        addr_validation=addr_by_store,
    )


# ══════════════════════════════════════════════════════════════════════
# STEP 3: Downloads
# ══════════════════════════════════════════════════════════════════════

@app.route("/download/businesses")
def download_businesses():
    csv_data = session.get("business_import_csv", "")
    if not csv_data:
        flash("No business CSV available.", "error")
        return redirect(url_for("results"))
    buf = io.BytesIO(csv_data.encode("utf-8"))
    company_name = _safe_filename_part(session.get("company_name"))
    return send_file(buf, mimetype="text/csv", as_attachment=True,
                     download_name=f"new_businesses_{company_name}.csv")


@app.route("/download/shifts")
def download_shifts():
    csv_data = session.get("shift_import_csv", "")
    if not csv_data:
        flash("No shift CSV available.", "error")
        return redirect(url_for("results"))
    buf = io.BytesIO(csv_data.encode("utf-8"))
    company_name = _safe_filename_part(session.get("company_name"))
    return send_file(buf, mimetype="text/csv", as_attachment=True,
                     download_name=f"shifts_{company_name}.csv")


@app.route("/download/missing-contacts")
def download_missing_contacts():
    csv_data = session.get("missing_contacts_csv", "")
    if not csv_data:
        flash("No missing contacts CSV available.", "error")
        return redirect(url_for("results"))
    buf = io.BytesIO(csv_data.encode("utf-8"))
    return send_file(buf, mimetype="text/csv", as_attachment=True,
                     download_name="new_company_users.csv")


@app.route("/download/tasks")
def download_tasks():
    csv_data = session.get("tasks_csv") or session.get("tasks_import_csv", "")
    if not csv_data:
        flash("No tasks CSV available.", "error")
        return redirect(url_for("results"))
    buf = io.BytesIO(csv_data.encode("utf-8"))
    return send_file(buf, mimetype="text/csv", as_attachment=True,
                     download_name="tasks_import.csv")


@app.route("/download/special-requirements")
def download_special_requirements():
    csv_data = session.get("sr_csv", "")
    if not csv_data:
        flash("No Special Requirements CSV available — set IDs in the configuration step and resync.", "error")
        return redirect(url_for("results"))
    buf = io.BytesIO(csv_data.encode("utf-8"))
    return send_file(buf, mimetype="text/csv", as_attachment=True,
                     download_name="special_requirements.csv")


@app.route("/download/certifications")
def download_certifications():
    csv_data = session.get("cert_csv", "")
    if not csv_data:
        flash("No Certifications CSV available — set IDs in the configuration step and resync.", "error")
        return redirect(url_for("results"))
    buf = io.BytesIO(csv_data.encode("utf-8"))
    return send_file(buf, mimetype="text/csv", as_attachment=True,
                     download_name="certifications.csv")


@app.route("/download/trainings")
def download_trainings():
    csv_data = session.get("training_csv", "")
    if not csv_data:
        flash("No Trainings CSV available — set IDs in the configuration step and resync.", "error")
        return redirect(url_for("results"))
    buf = io.BytesIO(csv_data.encode("utf-8"))
    return send_file(buf, mimetype="text/csv", as_attachment=True,
                     download_name="trainings.csv")


@app.route("/configure-new-businesses", methods=["GET", "POST"])
def configure_new_businesses():
    company_id = session.get("company_id")
    unmatched = session.get("unmatched_rows", [])
    cfg = session.get("config", {})

    if not company_id or not unmatched:
        flash("No new businesses to configure. Upload a file first.", "error")
        return redirect(url_for("index"))

    # Build deduped display list of businesses to create
    seen = set()
    new_businesses = []
    for row in unmatched:
        sn = row.get("store_number", "")
        if sn in seen:
            continue
        seen.add(sn)
        retailer = (row.get("retailer", "") or "").strip()
        expected = row.get("_expected_name", "")
        if expected:
            name = expected
        elif retailer and sn:
            name = csv_processor.format_business_name(retailer, sn)
        else:
            name = sn or row.get("address", "")
        addr_parts = [row.get(k, "") for k in ("address", "city", "state", "zip")]
        full_addr = ", ".join(p for p in addr_parts if p)
        new_businesses.append({"name": name, "address": full_addr, "store_number": sn})

    if request.method == "POST":
        def parse_id_list(s):
            return [x.strip() for x in (s or "").split(",") if x.strip()]

        nb_config = {
            "automated_overbooking": request.form.get("automated_overbooking") == "1",
            "worker_instructions": request.form.get("worker_instructions", "").strip(),
            "special_requirement_ids": parse_id_list(request.form.get("special_requirement_ids", "")),
            "sr_position_ids": parse_id_list(request.form.get("sr_position_ids", "")),
            "cert_gig_position_ids": parse_id_list(request.form.get("cert_gig_position_ids", "")),
            "cert_mandatory": request.form.get("cert_mandatory") == "1",
            "training_ids": parse_id_list(request.form.get("training_ids", "")),
            "training_mandatory": request.form.get("training_mandatory") == "1",
            "clock_in_task_ids": parse_id_list(request.form.get("clock_in_task_ids", "")),
            "during_task_ids": parse_id_list(request.form.get("during_task_ids", "")),
            "clock_out_task_ids": parse_id_list(request.form.get("clock_out_task_ids", "")),
            "task_position_ids": parse_id_list(request.form.get("task_position_ids", "")),
        }
        session["new_business_config"] = nb_config

        # Merge into the live config used by CSV generators
        cfg.update(nb_config)
        cfg["_company_id"] = company_id
        session["config"] = cfg

        # Generate the new businesses CSV with the worker instructions and
        # overbooking flag baked in.
        biz_csv = csv_processor.generate_business_import_csv(unmatched, cfg)
        session["business_import_csv"] = biz_csv

        usage_db.log_event(
            "configure_new",
            company_id=company_id,
            company_name=cfg.get("name", ""),
            rows_unmatched=len(unmatched),
        )

        flash("New business CSV generated. Download it, upload to Django, then resync to get the attribute templates.", "success")
        return redirect(url_for("results"))

    return render_template(
        "configure_new_businesses.html",
        new_businesses=new_businesses,
        num_new=len(new_businesses),
        defaults=cfg,
    )


# ══════════════════════════════════════════════════════════════════════
# API endpoints
# ══════════════════════════════════════════════════════════════════════

@app.route("/api/companies")
def api_companies():
    try:
        return jsonify(mode_client.get_companies())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


_ai_status_cache = {"ok": None, "checked_at": 0.0, "error": None, "key_present": False}
AI_STATUS_TTL = 60  # seconds


@app.route("/api/ai-status")
def api_ai_status():
    """Live health check for the AI provider (OpenAI). Cached for AI_STATUS_TTL."""
    now = time.time()
    force = request.args.get("force") == "1"
    if not force and _ai_status_cache["ok"] is not None and \
            now - _ai_status_cache["checked_at"] < AI_STATUS_TTL:
        return jsonify({
            "ok": _ai_status_cache["ok"],
            "key_present": _ai_status_cache["key_present"],
            "error": _ai_status_cache["error"],
            "cached": True,
            "checked_at": _ai_status_cache["checked_at"],
        })

    api_key = os.environ.get("OPENAI_API_KEY") or os.environ.get("GPT_KEY", "")
    if not api_key:
        _ai_status_cache.update(ok=False, checked_at=now, error="OPENAI_API_KEY (or GPT_KEY) not set", key_present=False)
        return jsonify({"ok": False, "key_present": False, "error": "OPENAI_API_KEY (or GPT_KEY) not set", "cached": False, "checked_at": now})

    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        client.chat.completions.create(
            model="gpt-4o-mini",
            max_tokens=1,
            messages=[{"role": "user", "content": "ping"}],
        )
        _ai_status_cache.update(ok=True, checked_at=now, error=None, key_present=True)
        return jsonify({"ok": True, "key_present": True, "error": None, "cached": False, "checked_at": now})
    except Exception as e:
        msg = str(e)[:200]
        _ai_status_cache.update(ok=False, checked_at=now, error=msg, key_present=True)
        return jsonify({"ok": False, "key_present": True, "error": msg, "cached": False, "checked_at": now})


# Keep the old route name as an alias for backwards compat (header pill JS may still call it)
@app.route("/api/claude-status")
def api_claude_status_alias():
    return api_ai_status()


@app.template_filter("datetimeformat")
def _datetimeformat(ts):
    if not ts:
        return ""
    from datetime import datetime
    return datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M")


@app.route("/admin/usage")
def admin_usage():
    token = os.environ.get("ADMIN_TOKEN", "")
    if not token or request.args.get("token") != token:
        abort(404)
    days = int(request.args.get("days", 30))
    return render_template("admin_usage.html", **usage_db.summary(days=days))


@app.route("/admin/clear-failures", methods=["POST"])
def admin_clear_failures():
    token = os.environ.get("ADMIN_TOKEN", "")
    if not token or request.form.get("token") != token:
        abort(404)
    n = usage_db.clear_failures()
    flash(f"Cleared {n} failure event(s) from the dashboard.", "success")
    return redirect(url_for("admin_usage", token=token,
                            days=request.form.get("days", 30)))


@app.route("/api/search-business")
def api_search_business():
    # Manual address-based business override — not currently wired to any
    # UI. If we resurrect it, port to a Mode query (no Redshift access).
    return jsonify([])


# ══════════════════════════════════════════════════════════════════════
# Bootstrap Partner from Redshift (one-time setup)
# ══════════════════════════════════════════════════════════════════════

@app.route("/bootstrap-partner", methods=["POST"])
def bootstrap_partner():
    company_id = request.form.get("company_id", "").strip()
    if not company_id or not company_id.isdigit():
        flash("Provide a valid company ID.", "error")
        return redirect(url_for("index"))

    try:
        # Pull company name + most-recent gigtemplate defaults from Mode.
        bootstrap = mode_client.bootstrap_partner(company_id)
        if not bootstrap:
            flash(f"Company {company_id} not found in Mode.", "error")
            return redirect(url_for("index"))
        company_name = bootstrap.get("company_name") or ""

        cfg = pc.get_config(company_id)
        cfg["name"] = company_name
        for key in (
            "default_contact_id", "default_creator_id", "default_position_id",
            "default_position_tiering_id", "default_parking",
            "default_position_instructions", "default_attire",
        ):
            if bootstrap.get(key) not in (None, ""):
                cfg[key] = bootstrap[key]

        # Local contacts DB is the authoritative source for default contact.
        local_default = contacts_db.get_default_contact(company_id)
        if local_default:
            cfg["default_contact_id"] = local_default["cuser_id"]
            cfg["default_contact_name"] = local_default.get("name", "")
            cfg["default_contact_role"] = local_default.get("role", "")

        try:
            with open("partner_configs/registry.json") as f:
                reg = json.load(f).get(str(company_id), {})
        except FileNotFoundError:
            reg = {}
        if reg:
            cfg["special_requirement_ids"] = [x["id"] for x in reg.get("special_requirements", [])]
            co_tasks = reg.get("clockout_tasks", {})
            cfg["clock_in_task_ids"] = [x["id"] for x in co_tasks.get("clockin", [])]
            cfg["during_task_ids"] = [x["id"] for x in co_tasks.get("during", [])]
            cfg["clock_out_task_ids"] = [x["id"] for x in co_tasks.get("clockout", [])]
            cfg["training_ids"] = [x["id"] for x in reg.get("trainings", [])]

        pc.save_config(company_id, cfg)
        flash(f"Pre-filled config for {company_name} ({company_id}) from past postings.", "success")
        return redirect(url_for("view_config", company_id=company_id))
    except Exception as e:
        flash(f"Pre-fill failed: {str(e)[:150]}", "error")
        return redirect(url_for("index"))


# ══════════════════════════════════════════════════════════════════════
# Partner Config Management (one-time setup pages)
# ══════════════════════════════════════════════════════════════════════

@app.route("/config/<company_id>", methods=["GET"])
def view_config(company_id):
    cfg = pc.get_config(company_id)
    return render_template("partner_config.html", config=cfg, company_id=company_id)


@app.route("/config/<company_id>", methods=["POST"])
def save_config(company_id):
    cfg = pc.get_config(company_id)
    cfg["name"] = request.form.get("name", cfg["name"])
    cfg["default_position_id"] = int(request.form.get("default_position_id", 29))
    cfg["default_break_length"] = int(request.form.get("default_break_length", 30))
    cfg["default_parking"] = int(request.form.get("default_parking", 2))
    cfg["default_attire"] = request.form.get("default_attire", "")
    cfg["default_location_instructions"] = request.form.get("default_location_instructions", "")
    cfg["default_position_instructions"] = request.form.get("default_position_instructions", "")
    cfg["default_creator_id"] = request.form.get("default_creator_id") or None
    cfg["default_contact_id"] = request.form.get("default_contact_id") or None
    if cfg["default_creator_id"]:
        cfg["default_creator_id"] = int(cfg["default_creator_id"])
    if cfg["default_contact_id"]:
        cfg["default_contact_id"] = int(cfg["default_contact_id"])
    rate = request.form.get("adjusted_base_rate", "")
    cfg["adjusted_base_rate"] = float(rate) if rate else None
    tiering = request.form.get("default_position_tiering_id", "")
    cfg["default_position_tiering_id"] = int(tiering) if tiering else None
    cfg["default_end_time"] = request.form.get("default_end_time", "").strip() or None
    shift_hours = request.form.get("default_shift_hours", "").strip()
    cfg["default_shift_hours"] = float(shift_hours) if shift_hours else None
    pc.save_config(company_id, cfg)
    flash("Partner config saved.", "success")
    return redirect(url_for("view_config", company_id=company_id))


@app.route("/override-business-ids", methods=["POST"])
def override_business_ids():
    """Manually assign Location IDs to 'new' businesses that actually exist.
    Moves them from unmatched to matched and regenerates the shift CSV."""
    company_id = session.get("company_id")
    matched = session.get("matched_rows", [])
    unmatched = session.get("unmatched_rows", [])
    cfg = session.get("config") or pc.get_config(company_id)

    overridden = 0
    still_unmatched = []
    for row in unmatched:
        store_num = row.get("store_number", "").strip()
        override_id = request.form.get(f"override_{store_num}", "").strip()
        if override_id and override_id.isdigit():
            biz = {
                "business_id": int(override_id),
                "business_name": row.get("_expected_name", f"#{store_num}"),
                "address": row.get("address", ""),
            }
            matched.append({
                **{k: v for k, v in row.items() if not k.startswith("_")},
                "_business": biz,
                "_status": "existing",
                "_match_method": "manual",
            })
            overridden += 1
        else:
            still_unmatched.append(row)

    if overridden:
        # Re-apply contact IDs
        contact_lookup = {}
        for m in session.get("contact_matches", []):
            if m.get("user"):
                contact_lookup[m["query_name"].lower()] = m["user"]["cuser_id"]
        for row in matched:
            tl = row.get("team_lead", "").strip().lower()
            if tl in contact_lookup and "_contact_id" not in row:
                row["_contact_id"] = contact_lookup[tl]

        # Regenerate CSVs
        cfg["_company_id"] = company_id
        task_opts = {
            "is_task": session.get("is_task_request", False),
            "is_anywhere": session.get("is_anywhere", False),
        }
        shift_csv = csv_processor.generate_bulk_import_csv(matched, cfg, task_opts=task_opts)
        biz_csv = csv_processor.generate_business_import_csv(still_unmatched, cfg) if still_unmatched else None

        session["matched_rows"] = matched
        session["unmatched_rows"] = still_unmatched
        session["shift_import_csv"] = shift_csv
        session["business_import_csv"] = biz_csv

        flash(f"{overridden} store(s) linked to existing Location IDs. Shift CSV updated.", "success")
    else:
        flash("No overrides entered.", "info")

    return redirect(url_for("results"))


@app.route("/recheck", methods=["POST"])
def recheck():
    """Re-query Mode for new businesses after Django import.
    Regenerates shift CSV with newly available Location IDs."""
    company_id = session.get("company_id")
    parsed_rows = session.get("parsed_rows", [])
    cfg = session.get("config") or pc.get_config(company_id)

    if not parsed_rows:
        flash("No data. Please upload a file first.", "error")
        return redirect(url_for("index"))

    # Re-query — same Mode-authoritative lookup as upload
    matched, unmatched = locations_db.match_businesses(company_id, parsed_rows)
    recheck_error = ""
    mode_ran = False
    if unmatched and mode_client.is_available():
        try:
            mode_matched, still_unmatched = mode_client.get_businesses_for_company(company_id, unmatched)
            matched = matched + mode_matched
            unmatched = still_unmatched
            mode_ran = True
        except Exception as me:
            recheck_error = f"Mode error: {str(me)}"
            flash(recheck_error, "error")

    # Re-match contacts
    contact_lookup = {}
    for m in session.get("contact_matches", []):
        if m.get("user"):
            contact_lookup[m["query_name"].lower()] = m["user"]["cuser_id"]
    for row in matched:
        tl = row.get("team_lead", "").strip().lower()
        if tl in contact_lookup:
            row["_contact_id"] = contact_lookup[tl]

    # Regenerate CSVs — make sure new-business config (instructions, IDs)
    # is merged so attribute templates get the right values
    cfg["_company_id"] = company_id
    nb_config = session.get("new_business_config") or {}
    cfg.update(nb_config)
    biz_csv = csv_processor.generate_business_import_csv(unmatched, cfg) if unmatched else None

    task_opts = {
        "is_task": session.get("is_task_request", False),
        "is_anywhere": session.get("is_anywhere", False),
    }
    shift_csv = csv_processor.generate_bulk_import_csv(matched, cfg, task_opts=task_opts)

    # Build attribute templates for newly-verified businesses (those that
    # got a business_id assigned via Mode this round). We pull them out of
    # `matched` since unmatched ones don't have IDs yet.
    if nb_config:
        new_match_ids = []
        new_store_set = {row.get("store_number", "") for row in session.get("unmatched_rows", [])}
        seen_bid = set()
        for r in matched:
            biz = r.get("_business", {})
            bid = biz.get("business_id")
            if bid and bid not in seen_bid and r.get("store_number", "") in new_store_set:
                seen_bid.add(bid)
                new_match_ids.append({"business_id": bid})
        session["sr_csv"] = csv_processor.generate_special_requirements_csv(new_match_ids, cfg)
        session["cert_csv"] = csv_processor.generate_certifications_csv(new_match_ids, cfg)
        session["training_csv"] = csv_processor.generate_trainings_csv(new_match_ids, cfg)
        session["tasks_csv"] = csv_processor.generate_tasks_csv(new_match_ids, cfg)

    # Update session
    session["matched_rows"] = matched
    session["unmatched_rows"] = unmatched
    session["business_import_csv"] = biz_csv
    session["shift_import_csv"] = shift_csv
    session["redshift_error"] = None

    prev_new = session.get("_prev_new_count", 0)
    now_new = len(set(r.get("store_number", "") for r in unmatched))
    now_existing = len(set(r.get("store_number", "") for r in matched))
    newly_found = prev_new - now_new if prev_new > now_new else 0

    session["_prev_new_count"] = now_new

    if newly_found > 0:
        flash(f"{newly_found} new businesses verified! Shift CSV updated with their Location IDs.", "success")
    elif now_new > 0:
        flash(f"Still {now_new} businesses pending — not in Mode yet. Upload to Django and wait ~1–2 min before resyncing.", "info")
    else:
        flash("All businesses found! Shift CSV is complete.", "success")

    usage_db.log_event(
        "recheck",
        company_id=company_id,
        company_name=cfg.get("name", ""),
        rows_total=len(parsed_rows),
        rows_matched=len(matched),
        rows_unmatched=len(unmatched),
        mode_used=mode_ran,
        success=(not recheck_error),
        error_msg=recheck_error,
    )

    return redirect(url_for("results"))


# Keep old routes as redirects so bookmarks don't break
@app.route("/review", methods=["GET", "POST"])
@app.route("/businesses", methods=["GET"])
@app.route("/configure", methods=["GET", "POST"])
@app.route("/verify", methods=["GET"])
@app.route("/shifts", methods=["GET", "POST"])
@app.route("/contacts", methods=["GET", "POST"])
@app.route("/generate", methods=["GET", "POST"])
def legacy_redirect():
    return redirect(url_for("results"))


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=True, host="0.0.0.0", port=port)
