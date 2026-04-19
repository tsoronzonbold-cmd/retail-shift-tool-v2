# Retail Shift Tool V2 — Cursor Briefing

## What we're building

A unified Replit web app that replaces two existing tools (Christian's Google Sheets + Apps Script tool and Anchen's Replit tool at `retail-shift-tool.replit.app`) with a single workflow for retail shift bulk uploads at Instawork.

## The problem

Instawork's retail ops team manually posts thousands of shifts per week for ~40 retail partners. Each partner sends a spreadsheet request (store list with dates/times/contacts). The team currently:

1. Opens Anchen's Replit tool → uploads CSV → identifies new vs existing businesses → configures new businesses
2. Opens Christian's Google Sheet tool → uploads same CSV → tool maps partner-specific columns (position instructions, attire, contact IDs, creator IDs, parking, etc.) from historical templates
3. Manually copy-pastes template data from Christian's tool into Anchen's output
4. Downloads final CSV → uploads to Django bulk importer

This takes hours per day. Christian (who built the Sheets tool) is on paternity leave for 2 months. Ramses (retail ops lead) is doing this manually.

## The unified tool flow

### Step 1: UPLOAD
- User selects partner (dropdown of ~40 retail companies)
- User uploads partner request CSV
- Tool auto-detects column mapping (store #, address, city, state, zip, start date, start time, end time, break, team lead name, etc.)

### Step 2: REVIEW
- Show parsed data in a table
- User can edit/fix any mismatched columns

### Step 3: BUSINESSES
- Query Redshift to check which store numbers already exist as businesses under this company
- Show: X existing (green) vs Y new (orange, "configuration required")
- For existing: show the matched business with Location ID
- For new: queue them for creation

### Step 4: CONFIGURE NEW BUSINESSES
- Pre-fill configuration from the **partner config registry** (what clock-out tasks, special requirements, trainings, certifications this company always uses)
- Fields: Worker Instructions, Special Requirements IDs, Certifications IDs, Training IDs, Clock-In/During/Clock-Out Task IDs, Automated Overbooking toggle
- User can override per-business if needed
- Generate business import CSV for Django upload

### Step 5: GENERATE BULK SHIFT IMPORT
- For each row in the partner request, generate the full bulk import row with ALL columns:
  - **From the request**: Start Date, Start Time, End Time, Break Length
  - **From Redshift lookup**: Location Id, Contact Id, Creator Id
  - **From partner config/historical template**: Position Id, Position Tiering, Parking, Position Instructions, Position Duties, Attire Instructions, Location Instructions, Adjusted Base Rate
  - **Partner-specific columns**: Booking Group, Multi-Day Same Worker, Requested Worker IDs, Fill or Kill, Star Minimum (for task-based shifts)
- Expand rows by quantity (if partner requests 3 workers for a store, generate 3 rows)
- Output: downloadable CSV ready for Django bulk importer

## Redshift connection details

- **Cluster**: `instawork-dw`
- **Database**: `instawork`
- **Endpoint**: `instawork-dw.cvgakvku4dlq.us-west-2.redshift.amazonaws.com`
- **Port**: 5439
- **Region**: us-west-2
- **Auth**: AWS IAM (uses redshift-data API, not direct connection)

### Key tables (all in schema `iw_backend_db`)

#### `backend_company` — the partner companies
- `id` (integer) — company ID (e.g. 75558 for Advantage Solutions)
- `name` (varchar) — company name
- `notes` (varchar)
- `w2_employees_only` (smallint)
- `subsidiary` (varchar)

#### `backend_gigbusiness` — individual business locations
- `id` (integer) — the Location ID used in bulk imports
- `name` (varchar) — e.g. "Walmart - #2297, 25450 The Old Rd, Stevenson Ranch, CA"
- `address` (varchar) — full address
- `company` (varchar) — company ID as string
- `business_id` (integer)

#### `backend_gigtemplate` — shift templates (historical postings)
- `id`, `company_id`, `business_id`
- `contact_id` — the Contact ID used in bulk imports
- `created_by_id` — the Creator ID used in bulk imports
- `position` (integer) — position ID (29=General Labor, 42=Merchandiser)
- `position_tiering_id` — position label (39=Merchandiser label)
- `instructions` (varchar) — position instructions (multi-paragraph)
- `custom_attire_requirements` (varchar) — attire instructions
- `has_parking` (integer) — parking availability (0/1/2)
- `same_workers_preferred` (smallint)
- `is_requested_worker_only` (smallint)
- `multi_day_same_worker` (integer)
- `is_flexible_time_task` (smallint) — for anytime/task-based shifts

#### `backend_companyuser` — contacts per company
- `id`, `company_id`, `cuser_id` (the user ID), `role`, `is_admin`

#### `backend_clockouttask` — clock-out tasks per business
- `id`, `business_id`, `position_id`, `active`, `type`

#### `backend_gigrequirement` — special requirements
- `id`, `name`, `description`

#### `backend_companymandatorycertificate` — company-level certs
- `company_id`, `certificatetypegigposition_id`, `is_mandatory`

#### `backend_businessmandatorycertificate` — business-level certs
- `business_id`, `certificatetypegigposition_id`, `is_mandatory`

#### `backend_companyshiftprolist` — roster (favorited workers)
- `list_id`, `user_id`

### Key queries

**Find all businesses for a company:**
```sql
SELECT gt.business_id, b.name, b.address, gt.contact_id, gt.created_by_id,
       gt.position, gt.position_tiering_id, gt.instructions, 
       gt.custom_attire_requirements, gt.has_parking
FROM iw_backend_db.backend_gigtemplate gt
JOIN iw_backend_db.backend_gigbusiness b ON b.id = gt.business_id
WHERE gt.company_id = {company_id}
ORDER BY gt.created_at DESC
```

**Find clock-out tasks for a company's businesses:**
```sql
SELECT ct.id, ct.business_id, ct.position_id, ct.type, ct.active
FROM iw_backend_db.backend_clockouttask ct
JOIN iw_backend_db.backend_gigtemplate gt ON gt.business_id = ct.business_id
WHERE gt.company_id = {company_id} AND ct.active = 1
```

**Find company users/contacts:**
```sql
SELECT cu.id, cu.cuser_id, cu.role, cu.is_admin
FROM iw_backend_db.backend_companyuser cu
WHERE cu.company_id = {company_id}
```

## Bulk import CSV column format (Django)

The final CSV that gets uploaded to Django's bulk importer has these columns in order:

| # | Column | Source |
|---|--------|--------|
| 1 | Location Id | Redshift lookup by store # / address match |
| 2 | Contact Ids | From previous gigtemplate or partner request |
| 3 | Start Date | Partner request (MM/DD/YYYY) |
| 4 | Start Time | Partner request |
| 5 | End Time | Partner request |
| 6 | Break Length | Partner request or default (30 if shift > 4hrs) |
| 7 | Position Id | Partner config (29=GL, 42=Merchandiser) |
| 8 | Parking | Previous template |
| 9 | Position Duties | Previous template |
| 10 | Attire Instructions | Previous template |
| 11 | Location Instructions | Previous template (e.g. "Please meet team lead at Walmart customer service desk area") |
| 12 | Creator Id | Previous template |
| 13 | Fill or Kill | Usually empty for retail |
| 14 | Requested Worker Ids | Roster lookup if partner requests specific workers |
| 15 | External Id | Usually empty |

Optional partner-specific columns:
- **Booking Group** (column P) — for same-worker-across-locations
- **Multi-Day Same Worker** (column Q) — paired with booking group
- **Position Tiering** — 39 for merchandiser label on GL position
- **Adjusted Base Rate** — from company config
- **Star Minimum** — for task-based/anytime shifts

## Partner config registry (new — doesn't exist yet)

Need to build a JSON/database that stores per-company:
```json
{
  "75558": {
    "name": "Advantage Solutions - Racking Project",
    "default_position_id": 29,
    "default_position_tiering_id": null,
    "default_break_length": 30,
    "default_parking": 2,
    "default_attire": "Attire: Black or dark shirt, khakis/slacks...",
    "default_location_instructions": "Please meet team lead at Walmart customer service desk area",
    "default_position_instructions": "Greeting: ...",
    "default_creator_id": 7020569,
    "default_contact_id": 7020069,
    "clock_out_task_ids": [],
    "special_requirement_ids": [],
    "training_ids": [],
    "certification_ids": [],
    "needs_booking_group": false,
    "is_task_based": false,
    "store_number_field": "Store #",
    "adjusted_base_rate": 24.84
  }
}
```

This config can be bootstrapped from Redshift (query the most recent gigtemplate for each company to extract defaults) and then manually refined.

## Tech stack recommendation

- **Frontend**: React or plain HTML/JS (keep it simple for Replit)
- **Backend**: Python (Flask or FastAPI)
- **Database queries**: boto3 redshift-data client (same as the MCP server uses)
- **Data processing**: pandas for CSV manipulation
- **Hosting**: Replit

## What the Loom video showed (step-by-step reference)

Ramses posted an Advantage Solutions - Racking Project request (Walmart stores, overnight racking shifts 21:00-05:30):

1. Opened partner request spreadsheet → filtered to Walmart → 5 unique stores, 19 rows
2. Opened Anchen's tool (`retail-shift-tool.replit.app`) → uploaded → tool found 2 existing + 3 new businesses
3. Anchen's tool showed new business config page (Special Requirements, Certifications, Trainings, Tasks CSVs)
4. Switched to Christian's Sheet tool → clicked sidebar "Upload Task Request" → "New Locations" → "Sync New Businesses"
5. Christian's tool had a tab-naming collision error ("sheet already exists")
6. Navigated through generated tabs — saw the output with Location IDs, Contact IDs, position instructions, attire, etc. all populated from historical templates
7. Ran "Generate Bulk Import" → got the full CSV with 31 shifts
8. Some new businesses showed gaps due to Hightouch 24hr sync lag
9. Went back to Anchen's tool → "Import Verified" → "3 new businesses verified" → "AI-Normalized Shift Data" showing contact names/phones
10. Copied data between tools manually to produce the final CSV

## Context: who is building this

Tuvana Soronzonbold — product operations intern at Instawork, junior at Stanford. Working with Ramses Cardenas (retail ops lead, remote in SW Florida) and Kenneth Luu (data ops). Christian Crynes (who built the Sheets tool) is on paternity leave until June 2026.

The goal is that when Christian comes back, the manual workflow is replaced by this unified tool.
