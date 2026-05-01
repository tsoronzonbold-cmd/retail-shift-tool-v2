"""Retail Shift Tool V2 — unified Flask app for retail shift bulk uploads.

Streamlined 3-step flow:
  1. Upload   — pick partner, upload CSV
  2. Results  — auto-detect columns, match businesses, match contacts,
                show everything pre-filled from partner config
  3. Download — download all CSVs (businesses, tasks, shifts)
"""

import os
import io
import json
import time

# Load .env file for Mode API credentials
from dotenv import load_dotenv
load_dotenv()

from flask import (
    Flask, render_template, request, redirect, url_for,
    session, send_file, flash, jsonify,
)
import pandas as pd

import config
import redshift_client
import mode_client
import locations_db
import partner_config as pc
import csv_processor
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

    all_companies = []
    try:
        all_companies = redshift_client.get_companies(configured_ids=configured_ids)
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

    # Auto-bootstrap unconfigured partners from Redshift on first use (skip on Replit)
    if not cfg.get("name"):
        try:  # noqa: this block is optional — if Redshift is unavailable, we just skip
            rows_q = redshift_client.execute_query(
                f"SELECT name FROM iw_backend_db.backend_company WHERE id = {int(company_id)}"
            )
            if rows_q:
                cfg["name"] = rows_q[0]["name"]
            tmpls = redshift_client.execute_query(f"""
                SELECT contact_id, created_by_id, position_fk_id, position_tiering_id,
                       has_parking, instructions, custom_attire_requirements
                FROM iw_backend_db.backend_gigtemplate
                WHERE company_id = {int(company_id)}
                  AND (contact_id IS NOT NULL OR instructions IS NOT NULL)
                ORDER BY created_at DESC LIMIT 1
            """)
            if tmpls:
                t = tmpls[0]
                cfg["default_contact_id"] = t.get("contact_id") or cfg.get("default_contact_id")
                cfg["default_creator_id"] = t.get("created_by_id") or cfg.get("default_creator_id")
                cfg["default_position_id"] = t.get("position_fk_id") or 29
                cfg["default_position_tiering_id"] = t.get("position_tiering_id")
                cfg["default_parking"] = t.get("has_parking") if t.get("has_parking") is not None else 2
                cfg["default_position_instructions"] = t.get("instructions") or ""
                cfg["default_attire"] = t.get("custom_attire_requirements") or ""
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

    # If auto-detect matched < MIN_AUTO_DETECT columns, use Claude to fill gaps
    if len(detected_mapping) < ai_mapper.MIN_AUTO_DETECT:
        if ai_mapper.is_available():
            sample = df.head(5).to_dict("records")
            detected_mapping = ai_mapper.maybe_ai_map(
                list(df.columns), sample, detected_mapping,
                partner_name=cfg.get("name", "")
            )
        else:
            flash(f"Auto-detect only matched {len(detected_mapping)} columns. Add ANTHROPIC_API_KEY secret to enable AI column mapping.", "info")

    final_mapping = {**detected_mapping, **{k: v for k, v in col_mapping.items() if v in df.columns}}
    rows, raw_columns = csv_processor.parse_upload(file_content, filename, final_mapping)

    # Detect businesses — 3-tier lookup:
    #   1. Local DB (Christian's 127K locations, instant, by companyId-storeNumber)
    #   2. Mode API (live Redshift query, handles name variations)
    #   3. Direct Redshift (fallback, queries gigtemplate)
    redshift_error = None
    matched, unmatched = locations_db.match_businesses(company_id, rows)

    # If local DB left unmatched rows, try Mode for those
    if unmatched and mode_client.is_available():
        try:
            mode_matched, still_unmatched = mode_client.get_businesses_for_company(company_id, unmatched)
            matched = matched + mode_matched
            unmatched = still_unmatched
        except Exception as me:
            redshift_error = f"Mode error: {str(me)}"

    # Last resort: direct Redshift
    if unmatched and not redshift_error:
        try:
            existing = redshift_client.get_businesses_for_company(company_id)
            rs_matched, still_unmatched = csv_processor.match_businesses(unmatched, existing)
            matched = matched + rs_matched
            unmatched = still_unmatched
        except Exception:
            pass  # Redshift unavailable is fine if we already got some matches

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

    # Pre-generate business import CSV if there are new businesses
    cfg["_company_id"] = company_id
    biz_csv = None
    if unmatched:
        biz_csv = csv_processor.generate_business_import_csv(unmatched, cfg)

    # Pre-generate shift import CSV
    task_opts = {
        "is_task": request.form.get("is_task_request") == "1",
        "is_anywhere": request.form.get("is_anywhere") == "1",
    }
    shift_csv = csv_processor.generate_bulk_import_csv(matched, cfg, task_opts=task_opts)

    # Store everything in session
    session["company_id"] = company_id
    session["company_name"] = cfg.get("name", f"Company {company_id}")
    session["parsed_rows"] = rows
    session["matched_rows"] = matched
    session["unmatched_rows"] = unmatched
    session["contact_matches"] = contact_matches
    session["business_import_csv"] = biz_csv
    session["shift_import_csv"] = shift_csv
    session["config"] = cfg
    session["missing_contacts_csv"] = missing_contacts_csv
    session["redshift_error"] = redshift_error
    session["address_validation"] = address_validation
    session["column_mapping"] = final_mapping
    session["raw_columns"] = raw_columns

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
    company_name = session.get("company_name", "partner").replace(" ", "_")
    return send_file(buf, mimetype="text/csv", as_attachment=True,
                     download_name=f"new_businesses_{company_name}.csv")


@app.route("/download/shifts")
def download_shifts():
    csv_data = session.get("shift_import_csv", "")
    if not csv_data:
        flash("No shift CSV available.", "error")
        return redirect(url_for("results"))
    buf = io.BytesIO(csv_data.encode("utf-8"))
    company_name = session.get("company_name", "partner").replace(" ", "_")
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
    csv_data = session.get("tasks_import_csv", "")
    if not csv_data:
        flash("No tasks CSV available.", "error")
        return redirect(url_for("results"))
    buf = io.BytesIO(csv_data.encode("utf-8"))
    return send_file(buf, mimetype="text/csv", as_attachment=True,
                     download_name="tasks_import.csv")


# ══════════════════════════════════════════════════════════════════════
# API endpoints
# ══════════════════════════════════════════════════════════════════════

@app.route("/api/companies")
def api_companies():
    try:
        companies = redshift_client.get_companies()
        return jsonify(companies)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


_claude_status_cache = {"ok": None, "checked_at": 0.0, "error": None, "key_present": False}
CLAUDE_STATUS_TTL = 60  # seconds


@app.route("/api/claude-status")
def api_claude_status():
    """Live health check for the Claude API. Cached for CLAUDE_STATUS_TTL."""
    now = time.time()
    force = request.args.get("force") == "1"
    if not force and _claude_status_cache["ok"] is not None and \
            now - _claude_status_cache["checked_at"] < CLAUDE_STATUS_TTL:
        return jsonify({
            "ok": _claude_status_cache["ok"],
            "key_present": _claude_status_cache["key_present"],
            "error": _claude_status_cache["error"],
            "cached": True,
            "checked_at": _claude_status_cache["checked_at"],
        })

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        _claude_status_cache.update(ok=False, checked_at=now, error="ANTHROPIC_API_KEY not set", key_present=False)
        return jsonify({"ok": False, "key_present": False, "error": "ANTHROPIC_API_KEY not set", "cached": False, "checked_at": now})

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1,
            messages=[{"role": "user", "content": "ping"}],
        )
        _claude_status_cache.update(ok=True, checked_at=now, error=None, key_present=True)
        return jsonify({"ok": True, "key_present": True, "error": None, "cached": False, "checked_at": now})
    except Exception as e:
        msg = str(e)[:200]
        _claude_status_cache.update(ok=False, checked_at=now, error=msg, key_present=True)
        return jsonify({"ok": False, "key_present": True, "error": msg, "cached": False, "checked_at": now})


@app.route("/api/search-business")
def api_search_business():
    company_id = request.args.get("company_id")
    query = request.args.get("q", "")
    if not company_id or not query:
        return jsonify([])
    try:
        results = redshift_client.search_business_by_address(company_id, query)
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


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
        rows = redshift_client.execute_query(
            f"SELECT id, name FROM iw_backend_db.backend_company WHERE id = {int(company_id)}"
        )
        if not rows:
            flash(f"Company {company_id} not found in Redshift.", "error")
            return redirect(url_for("index"))
        company_name = rows[0]["name"]

        tmpls = redshift_client.execute_query(f"""
            SELECT contact_id, created_by_id, position_fk_id, position_tiering_id,
                   has_parking, instructions, custom_attire_requirements
            FROM iw_backend_db.backend_gigtemplate
            WHERE company_id = {int(company_id)}
              AND (contact_id IS NOT NULL OR instructions IS NOT NULL OR custom_attire_requirements IS NOT NULL)
            ORDER BY created_at DESC
            LIMIT 1
        """)

        cfg = pc.get_config(company_id)
        cfg["name"] = company_name
        if tmpls:
            t = tmpls[0]
            cfg["default_contact_id"] = t.get("contact_id") or cfg.get("default_contact_id")
            cfg["default_creator_id"] = t.get("created_by_id") or cfg.get("default_creator_id")
            cfg["default_position_id"] = t.get("position_fk_id") or cfg.get("default_position_id", 29)
            cfg["default_position_tiering_id"] = t.get("position_tiering_id")
            cfg["default_parking"] = t.get("has_parking") if t.get("has_parking") is not None else cfg.get("default_parking", 2)
            cfg["default_position_instructions"] = t.get("instructions") or cfg.get("default_position_instructions", "")
            cfg["default_attire"] = t.get("custom_attire_requirements") or cfg.get("default_attire", "")

        local_default = contacts_db.get_default_contact(company_id)
        if local_default:
            cfg["default_contact_id"] = local_default["cuser_id"]
            cfg["default_contact_name"] = local_default.get("name", "")
            cfg["default_contact_role"] = local_default.get("role", "")
        elif not cfg.get("default_contact_id"):
            contact = redshift_client.get_default_contact(company_id)
            if contact:
                cfg["default_contact_id"] = contact["cuser_id"]
                cfg["default_contact_role"] = contact.get("role", "")

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
    """Re-query Redshift for new businesses after Django import.
    Regenerates shift CSV with newly available Location IDs."""
    company_id = session.get("company_id")
    parsed_rows = session.get("parsed_rows", [])
    cfg = session.get("config") or pc.get_config(company_id)

    if not parsed_rows:
        flash("No data. Please upload a file first.", "error")
        return redirect(url_for("index"))

    # Re-query — same 3-tier lookup as upload
    matched, unmatched = locations_db.match_businesses(company_id, parsed_rows)
    if unmatched and mode_client.is_available():
        try:
            mode_matched, still_unmatched = mode_client.get_businesses_for_company(company_id, unmatched)
            matched = matched + mode_matched
            unmatched = still_unmatched
        except Exception as me:
            flash(f"Mode error: {str(me)}", "error")
    if unmatched:
        try:
            existing = redshift_client.get_businesses_for_company(company_id)
            rs_matched, still_unmatched = csv_processor.match_businesses(unmatched, existing)
            matched = matched + rs_matched
            unmatched = still_unmatched
        except Exception:
            pass

    # Re-match contacts
    contact_lookup = {}
    for m in session.get("contact_matches", []):
        if m.get("user"):
            contact_lookup[m["query_name"].lower()] = m["user"]["cuser_id"]
    for row in matched:
        tl = row.get("team_lead", "").strip().lower()
        if tl in contact_lookup:
            row["_contact_id"] = contact_lookup[tl]

    # Regenerate CSVs
    cfg["_company_id"] = company_id
    biz_csv = csv_processor.generate_business_import_csv(unmatched, cfg) if unmatched else None

    task_opts = {
        "is_task": session.get("is_task_request", False),
        "is_anywhere": session.get("is_anywhere", False),
    }
    shift_csv = csv_processor.generate_bulk_import_csv(matched, cfg, task_opts=task_opts)

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
        flash(f"Still {now_new} businesses pending — not in Redshift yet. Upload to Django and wait for sync.", "info")
    else:
        flash("All businesses found! Shift CSV is complete.", "success")

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
