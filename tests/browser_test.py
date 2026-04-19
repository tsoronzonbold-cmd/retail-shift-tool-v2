"""End-to-end browser test for the retail shift tool.

Drives the full 6-step flow in headless Chromium, captures a screenshot at
each step, and asserts the key UI behaviors.

Run: source venv/bin/activate && python tests/browser_test.py

Requires the Flask app to already be running on http://localhost:5001.
"""

import os
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright, expect, TimeoutError as PWTimeout

BASE = "http://localhost:5001"
TEST_CSV = os.path.abspath("test_advantage.csv")
SHOTS = Path("/tmp/retail-shift-screenshots")
SHOTS.mkdir(exist_ok=True)

passed = []
failed = []


def check(name, condition, detail=""):
    if condition:
        passed.append(name)
        print(f"  ✅ {name}")
    else:
        failed.append((name, detail))
        print(f"  ❌ {name}  ({detail})")


def shot(page, step):
    path = SHOTS / f"{step}.png"
    page.screenshot(path=str(path), full_page=True)


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1400, "height": 900})
        page = context.new_page()

        # ---------- STEP 1: UPLOAD ----------
        print("\n=== STEP 1: Upload page ===")
        page.goto(BASE)
        shot(page, "01_upload_page")
        check("Upload page loads", "Retail Shift Tool" in page.title())

        # Datalist search for Advantage
        search = page.locator("#company_search")
        search.fill("Advantage Solutions- Racking")
        check("Datalist search accepts text", search.input_value() == "Advantage Solutions- Racking")

        # Pick the partner — we need to select one so the hidden company_id gets set
        # Datalists work by matching the text; let's type the exact format the script expects
        search.fill("Advantage Solutions- Racking Project (#75558)")
        # Trigger input event to populate hidden field
        page.evaluate(
            """() => {
                const search = document.getElementById('company_search');
                search.dispatchEvent(new Event('input', {bubbles: true}));
            }"""
        )
        hidden_id = page.locator("#company_id").input_value()
        check("Hidden company_id populated from datalist", hidden_id == "75558", f"got {hidden_id!r}")

        # Toggle the task-shift checkbox and back
        task_cb = page.locator("#is_task_request")
        task_opts = page.locator("#task-options")
        check("Task options hidden by default", not task_opts.is_visible())
        task_cb.check()
        check("Task options revealed when checked", task_opts.is_visible())
        task_cb.uncheck()
        check("Task options hidden again when unchecked", not task_opts.is_visible())

        # Attach file and submit
        page.locator("#file").set_input_files(TEST_CSV)
        shot(page, "02_upload_filled")

        with page.expect_navigation():
            page.locator('button:has-text("Upload & Parse")').click()
        check("Upload redirects to /review", "/review" in page.url)

        # ---------- STEP 2: REVIEW ----------
        print("\n=== STEP 2: Review page ===")
        shot(page, "03_review")
        check("Review title present", "Review Parsed Data" in page.content())
        check("5 rows parsed shown", "5 rows parsed" in page.content())
        check("Store # auto-mapped", page.locator('select[name="map_store_number"]').input_value() == "Store #")
        check("Team Lead auto-mapped", page.locator('select[name="map_team_lead"]').input_value() == "Team Lead")
        check("Team Lead Phone auto-mapped", page.locator('select[name="map_team_lead_phone"]').input_value() == "Team Lead Phone")

        # Continue with current mapping
        page.locator('a:has-text("Continue with Current Mapping")').click()
        page.wait_for_url("**/businesses")

        # ---------- STEP 3: BUSINESSES ----------
        print("\n=== STEP 3: Businesses page ===")
        shot(page, "04_businesses")
        stats = page.locator(".stat-number").all_text_contents()
        check("3 stat cards present", len(stats) == 3, f"got {stats}")
        check("Total = 5", stats[0] == "5", f"got {stats[0]}")
        check("Existing = 4", stats[1] == "4", f"got {stats[1]}")
        check("New = 1", stats[2] == "1", f"got {stats[2]}")
        # Real Location IDs should appear
        content = page.content()
        check("Location ID 149836 shown", "149836" in content)
        # New business row
        check("New business '123 Nowhere Lane' shown", "123 Nowhere Lane" in content)

        # Click "Configure New Businesses"
        page.locator('a:has-text("Configure New Businesses")').click()
        page.wait_for_url("**/configure")

        # ---------- STEP 4: CONFIGURE ----------
        print("\n=== STEP 4: Configure page ===")
        shot(page, "05_configure")
        check("Configure title", "New Business Configuration" in page.content())
        check("New business listed in left panel", "999" in page.content())

        # Fill in clock-out task IDs
        page.locator('input[name="clock_out_task_ids"]').fill("100, 200")
        page.locator('input[name="during_task_ids"]').fill("2")
        page.locator('input[name="special_requirement_ids"]').fill("5, 10")
        page.locator('input[name="task_position_ids"]').fill("29")
        page.locator('textarea[name="worker_instructions"]').fill("Please arrive 15 minutes early")

        page.locator('button:has-text("Generate Business CSVs")').click()
        page.wait_for_url("**/verify")

        # ---------- STEP 4b: VERIFY (auto-polling) ----------
        print("\n=== STEP 4b: Verify page (auto-polling) ===")
        shot(page, "06_verify_initial")
        check("Verify page loads", "Generate New Business CSV" in page.content())
        check("Auto-poll status visible", page.locator("#poll-state").is_visible())
        check("1 new businesses shown", "1 new businesses" in page.content())

        # Wait for first auto-poll cycle to fire (it runs immediately on load)
        # The check counter should increment
        page.wait_for_function(
            "document.getElementById('check-count').textContent !== '0'",
            timeout=10000,
        )
        check_count = page.locator("#check-count").text_content()
        check("Auto-poll fired at least once", int(check_count) >= 1, f"check_count={check_count}")

        # Verify pending state (since store 999 is fake)
        poll_title = page.locator("#poll-title").text_content()
        check("Poll shows pending state", "pending" in poll_title.lower(), f"title={poll_title}")

        # Test pause button
        page.locator("#pause-btn").click()
        pause_text = page.locator("#pause-btn").text_content()
        check("Pause toggles button text", "Resume" in pause_text, f"got {pause_text}")

        # Resume
        page.locator("#pause-btn").click()
        check("Resume toggles back", "Pause" in page.locator("#pause-btn").text_content())

        shot(page, "07_verify_polling")

        # Navigate directly to /shifts (skip the fake verify)
        page.goto(f"{BASE}/shifts")

        # ---------- STEP 5: SHIFTS ----------
        print("\n=== STEP 5: Shift Details page ===")
        shot(page, "08_shifts")
        content = page.content()
        check("Shifts title", "Map Shift Details" in content)
        check("12 shift rows from 4 source rows", "12" in content and "4" in content)

        # Editable cell test: find the first quantity cell, edit it, blur, confirm save
        qty_cell = page.locator('td[data-field="quantity"]').first
        original = qty_cell.text_content()
        check("Editable quantity cell present", qty_cell.is_visible())
        check("Quantity cell has contenteditable", qty_cell.get_attribute("contenteditable") == "true")

        qty_cell.click()
        # Clear and type new value
        page.keyboard.press("Control+A")
        page.keyboard.press("Meta+A")  # macOS fallback
        page.keyboard.type("7")
        # Blur by clicking elsewhere
        page.locator("body").click(position={"x": 10, "y": 10})
        # Wait a moment for the fetch to complete
        time.sleep(0.5)

        # Verify the save persisted by reloading the page
        page.reload()
        new_val = page.locator('td[data-field="quantity"]').first.text_content()
        check("Edited cell persists after reload", new_val == "7", f"original={original} -> new={new_val}")

        shot(page, "09_shifts_edited")

        # Continue to contact matching
        page.locator('button:has-text("Continue to Contact Matching")').click()
        page.wait_for_url("**/contacts")

        # ---------- STEP 5b: CONTACT MATCHING ----------
        print("\n=== STEP 5b: Contact Matching page ===")
        shot(page, "10_contacts")
        content = page.content()
        check("Contact Matching title", "Contact Matching" in content)
        # Stats — 4 exact matches
        match_stats = page.locator(".stat-number").all_text_contents()
        check("Exact matches = 4", match_stats[0] == "4", f"got {match_stats}")
        check("Fallback = 0", match_stats[1] == "0")
        check("Unmatched = 0", match_stats[2] == "0")
        # Real cuser IDs from the PDF
        check("Adam Hill contact ID shown", "7021326" in content)

        page.locator('button:has-text("Continue to Generate")').click()
        page.wait_for_url("**/generate")

        # ---------- STEP 6: GENERATE ----------
        print("\n=== STEP 6: Generate page ===")
        shot(page, "11_generate")
        content = page.content()
        check("Generate title", "Download Shift CSV" in content or "Generate Bulk Import" in content)
        check("Stats show total shift rows", "Total Shift Rows" in content)
        check("Download shifts.csv button visible", page.locator('button:has-text("Download shifts.csv")').is_visible())

        # Trigger the download and capture the file
        with page.expect_download() as download_info:
            page.locator('button:has-text("Download shifts.csv")').click()
        download = download_info.value
        download_path = f"/tmp/playwright_shifts_{int(time.time())}.csv"
        download.save_as(download_path)
        check("shifts.csv downloaded", os.path.exists(download_path))

        # Parse the downloaded CSV
        import csv as csvmod
        with open(download_path) as f:
            rows = list(csvmod.reader(f))
        check("CSV has 22 columns", len(rows[0]) == 22, f"got {len(rows[0])}")
        check("CSV has expected data rows", len(rows) - 1 >= 12, f"got {len(rows)-1}")
        # First row should have the Adam Hill contact ID (7021326) since we edited row 0 quantity to 7
        first_contact = rows[1][rows[0].index("Contact Ids")]
        check("First row Contact Id is 7021326 (Adam Hill)", first_contact == "7021326", f"got {first_contact}")
        first_location = rows[1][rows[0].index("Location Id")]
        check("First row Location Id is 149836", first_location == "149836", f"got {first_location}")

        shot(page, "12_generate_downloaded")

        # ---------- TASK SHIFT VARIANT ----------
        print("\n=== EXTRA: Task shift variant ===")
        # Start a fresh flow with is_task checked
        page.goto(BASE)
        page.locator("#company_search").fill("Advantage Solutions- Racking Project (#75558)")
        page.evaluate("document.getElementById('company_search').dispatchEvent(new Event('input', {bubbles: true}))")
        page.locator("#is_task_request").check()
        page.locator('label:has-text("Is Anywhere")').locator('input[type="checkbox"]').check()
        page.locator("#file").set_input_files(TEST_CSV)
        with page.expect_navigation():
            page.locator('button:has-text("Upload & Parse")').click()
        page.goto(f"{BASE}/businesses")  # populate matched
        page.goto(f"{BASE}/generate")

        with page.expect_download() as download_info:
            page.locator('button:has-text("Download shifts.csv")').click()
        task_download = download_info.value
        task_path = f"/tmp/playwright_task_shifts_{int(time.time())}.csv"
        task_download.save_as(task_path)

        with open(task_path) as f:
            task_rows = list(csvmod.reader(f))
        h = task_rows[0]
        is_task_col = h.index("Is Task")
        sam_col = h.index("Starts At Minimum")
        anywhere_col = h.index("Is Anywhere")
        check("Task CSV has Is Task=1", task_rows[1][is_task_col] == "1")
        check("Task CSV has Starts At Min=21:00", task_rows[1][sam_col] == "21:00")
        check("Task CSV has Is Anywhere=1", task_rows[1][anywhere_col] == "1")

        # ---------- BOOTSTRAP ROUTE ----------
        print("\n=== EXTRA: Bootstrap new partner from history ===")
        page.goto(BASE)
        # Use the bootstrap form's number input (placeholder-scoped to avoid
        # collision with the hidden company_id field in the upload form)
        page.get_by_placeholder("Company ID (e.g. 75558)").fill("75196")
        with page.expect_navigation():
            page.locator('button:has-text("Pre-fill from History")').click()
        check("Bootstrap redirects to config page", "/config/75196" in page.url)
        shot(page, "13_bootstrap_result")

        browser.close()

    # ---------- RESULTS ----------
    print("\n" + "=" * 60)
    print(f"PASSED: {len(passed)}")
    print(f"FAILED: {len(failed)}")
    if failed:
        print("\nFailures:")
        for name, detail in failed:
            print(f"  - {name}: {detail}")
    print(f"\nScreenshots: {SHOTS}")
    sys.exit(0 if not failed else 1)


if __name__ == "__main__":
    main()
