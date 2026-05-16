"""Output validation layer for generated bulk import CSVs.

The pattern across every bug we've shipped fixes for this month: data
went into the pipeline, got silently corrupted or dropped, and a human
caught it after the fact. This module runs assertions on the OUTPUT
before we return it to the user — so the next class of silent failures
becomes a yellow banner instead of a Slack message tomorrow.

Each check returns either None (passes) or a string describing what
went wrong. The upload route flashes the failures.

Add a new check when:
  - A class of bug got reported (someone caught wrong output after the fact)
  - A field constraint we now know (e.g. rate must be > pro rate)
  - An invariant we want to never violate (e.g. qty=4 → 4 emitted rows)

Don't add a check that's better served by failing earlier in the pipeline.
"""

import csv
import io


def validate_bulk_csv(csv_string, input_rows):
    """Run all assertions on a generated bulk import CSV.

    Args:
      csv_string: the output CSV as a string (or None / "" if not generated yet)
      input_rows: the parsed input rows that fed into generation, for
                  cross-checks (qty totals, original pay rate, etc.)

    Returns:
      list of warning strings — empty if everything passed.
    """
    if not csv_string:
        return []

    reader = csv.DictReader(io.StringIO(csv_string))
    output_rows = list(reader)
    warnings = []

    warnings.extend(_check_row_count_matches_quantity(output_rows, input_rows))
    warnings.extend(_check_required_fields(output_rows))
    warnings.extend(_check_time_ordering(output_rows))
    warnings.extend(_check_rate_sanity(output_rows, input_rows))
    warnings.extend(_check_no_duplicate_shifts(output_rows))

    return warnings


def _check_row_count_matches_quantity(output_rows, input_rows):
    """Matched rows should expand to N output rows where N = sum of qty.

    Catches the case where a quantity column is silently misread (e.g. comma
    in a number, "3 pros" string instead of int) and we under-emit.
    """
    if not input_rows:
        return []
    matched = [r for r in input_rows if r.get("_status") == "existing"]
    if not matched:
        return []
    expected = 0
    for r in matched:
        qty = r.get("quantity") or r.get("requested_workers") or 1
        try:
            qty = int(str(qty).strip() or 1)
        except (ValueError, TypeError):
            qty = 1
        expected += max(1, qty)
    if len(output_rows) != expected:
        return [
            f"⚠ Output has {len(output_rows)} shift rows but input expected "
            f"{expected} based on quantity column. Check for misparsed "
            f"quantities in your CSV."
        ]
    return []


def _check_required_fields(output_rows):
    """Every emitted row needs the columns Django's bulk importer requires."""
    bad = []
    for i, r in enumerate(output_rows, start=1):
        missing = []
        if not (r.get("Location Id") or "").strip():
            missing.append("Location Id")
        if not (r.get("Start Date") or "").strip():
            missing.append("Start Date")
        if not (r.get("Start Time") or "").strip():
            missing.append("Start Time")
        if not (r.get("Position Id") or "").strip():
            missing.append("Position Id")
        if missing:
            bad.append((i, missing))
            if len(bad) >= 3:
                break

    if not bad:
        return []
    examples = "; ".join(f"row {i}: missing {', '.join(m)}" for i, m in bad)
    return [
        f"⚠ {len(bad)} shift row(s) are missing required fields. Django will "
        f"reject those rows. Examples: {examples}"
    ]


def _check_time_ordering(output_rows):
    """End time must come after start time (unless empty)."""
    bad = []
    for i, r in enumerate(output_rows, start=1):
        st = (r.get("Start Time") or "").strip()
        et = (r.get("End Time") or "").strip()
        if not st or not et:
            continue
        st_m = _to_minutes(st)
        et_m = _to_minutes(et)
        if st_m is None or et_m is None:
            continue
        # Allow midnight-wrap: if end-time is BEFORE start-time, the shift
        # wraps past midnight — that's fine for the SAS-style "22:00-06:00".
        # We only flag cases where they're identical (zero-length shift)
        # or trivially wrong (1-minute shifts) which is almost always a
        # parsing error rather than a real overnight gig.
        if st_m == et_m or 0 < et_m - st_m < 30:
            bad.append((i, st, et))
            if len(bad) >= 3:
                break
    if not bad:
        return []
    examples = "; ".join(f"row {i}: {st}–{et}" for i, st, et in bad)
    return [
        f"⚠ {len(bad)} shift(s) have suspect start/end times "
        f"(zero or sub-30-min). Examples: {examples}"
    ]


def _to_minutes(t):
    """'09:00' → 540. Returns None if unparseable."""
    try:
        parts = t.split(":")
        h = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 else 0
        return h * 60 + m
    except (ValueError, IndexError):
        return None


def _check_rate_sanity(output_rows, input_rows):
    """Adjusted Base Rate should be >= the partner's CSV pay rate (no
    markdown emits), and shouldn't be implausibly high or low.

    Cross-checks each output row against the corresponding input row's
    worker_pay_rate to catch cases where we accidentally emit raw pro rate
    or undercut the partner's billing.
    """
    if not output_rows or "Adjusted Base Rate" not in output_rows[0]:
        return []

    # Build a map from (location_id, start_date) → input pay rate. Imperfect
    # (multiple shifts per location possible) but good enough for sanity.
    pay_by_loc = {}
    for r in input_rows or []:
        biz = r.get("_business") or {}
        loc_id = str(biz.get("business_id") or "")
        rate_raw = r.get("worker_pay_rate", "")
        if not loc_id or not rate_raw:
            continue
        try:
            pay_by_loc.setdefault(loc_id, float(
                str(rate_raw).replace("$", "").replace(",", "").replace("/hr", "").strip()
            ))
        except ValueError:
            continue

    bad = []
    for i, r in enumerate(output_rows, start=1):
        rate_str = (r.get("Adjusted Base Rate") or "").strip()
        if not rate_str:
            continue
        try:
            rate = float(rate_str)
        except ValueError:
            continue
        # Absolute sanity: rate has to clear the federal-minimum-ish floor.
        if 0 < rate < 7:
            bad.append((i, rate, "below floor"))
            continue
        # Cross-check against input pay rate
        loc_id = str(r.get("Location Id") or "")
        pay = pay_by_loc.get(loc_id)
        if pay and rate < pay - 0.01:
            bad.append((i, rate, f"below input pay rate ${pay:.2f}"))
        elif pay and rate > pay * 3:
            bad.append((i, rate, f"more than 3x input pay rate ${pay:.2f}"))
        if len(bad) >= 3:
            break

    if not bad:
        return []
    examples = "; ".join(f"row {i}: ${r:.2f} ({why})" for i, r, why in bad)
    return [
        f"⚠ {len(bad)} shift(s) have suspicious Adjusted Base Rate values. "
        f"Examples: {examples}. Verify against the partner's rate sheet."
    ]


def _check_no_duplicate_shifts(output_rows):
    """Same (location, date, start_time) repeated more times than the row's
    quantity would explain almost always means we double-counted somewhere.
    """
    from collections import Counter
    keys = Counter()
    for r in output_rows:
        key = (
            (r.get("Location Id") or "").strip(),
            (r.get("Start Date") or "").strip(),
            (r.get("Start Time") or "").strip(),
        )
        if all(key):
            keys[key] += 1
    # Heuristic: more than 50 identical entries is suspicious — most partners
    # don't book 50 pros for the same shift at the same store.
    over = [(k, c) for k, c in keys.items() if c > 50]
    if not over:
        return []
    k, c = over[0]
    return [
        f"⚠ {c} duplicate output rows for Location {k[0]} on {k[1]} at {k[2]}. "
        f"Likely a parsing error in the quantity column or a duplicated row "
        f"in your CSV."
    ]
