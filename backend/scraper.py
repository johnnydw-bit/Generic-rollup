# Bramley Rollup - backend/scraper.py
# Playwright-based scrapers for Intelligent Golf

import re
from playwright.async_api import async_playwright


# ---------------------------------------------------------------------------
# scrape_players — scrape the booking list for a specific date
# ---------------------------------------------------------------------------

async def scrape_players(
    ig_username: str,
    ig_pin: str,
    date_str: str,
    ig_search_term: str,
) -> dict:
    """
    Log in to Intelligent Golf and scrape the booking list for a given date.

    Returns:
        {
            "names":     [str, ...],   # player names in booking order
            "tee_times": int,          # number of distinct tee time slots
            "tee_start": str,          # first tee time e.g. "08:00"
        }
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        try:
            # ── Log in ──────────────────────────────────────────────────────
            await page.goto("https://www.intelligentgolf.co.uk/login.php")
            await page.fill('input[name="username"]', ig_username)
            await page.fill('input[name="password"]', ig_pin)
            await page.click('input[type="submit"]')
            await page.wait_for_load_state("networkidle")

            # ── Navigate to booking sheet ────────────────────────────────────
            # Search for the rollup contact by the ig_search_term
            await page.goto(
                f"https://www.intelligentgolf.co.uk/booking.php"
                f"?date={date_str}&searchterm={ig_search_term}"
            )
            await page.wait_for_load_state("networkidle")

            # ── Extract player names ─────────────────────────────────────────
            # Players appear as links within booking slots
            name_elements = await page.query_selector_all(
                "td.booking-player a, .booking-name a, .player-name"
            )
            names = []
            for el in name_elements:
                text = (await el.inner_text()).strip()
                if text and text not in names:
                    names.append(text)

            # ── Extract tee time info ────────────────────────────────────────
            tee_time_elements = await page.query_selector_all(
                "td.tee-time, .booking-time, td.time"
            )
            tee_times_raw = []
            for el in tee_time_elements:
                text = (await el.inner_text()).strip()
                if text and re.match(r'\d{1,2}:\d{2}', text):
                    tee_times_raw.append(text)

            unique_tee_times = sorted(set(tee_times_raw))
            tee_start = unique_tee_times[0] if unique_tee_times else ""

            return {
                "names":     names,
                "tee_times": len(unique_tee_times),
                "tee_start": tee_start,
            }
        finally:
            await browser.close()


# ---------------------------------------------------------------------------
# scrape_whs_indices — scrape the club handicap index list
# ---------------------------------------------------------------------------

async def scrape_whs_indices(ig_username: str, ig_pin: str) -> dict:
    """
    Log in to Intelligent Golf and scrape the full member WHS handicap index
    list from bramleygolfclub.co.uk/hcaplist.php.

    Returns:
        {
            "indices": {"John Smith": 14.2, ...}
        }
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        try:
            # ── Log in ──────────────────────────────────────────────────────
            await page.goto("https://www.bramleygolfclub.co.uk/member/index.php")
            await page.fill('input[name="username"]', ig_username)
            await page.fill('input[name="password"]', ig_pin)
            await page.click('input[type="submit"]')
            await page.wait_for_load_state("networkidle")

            # ── Navigate to handicap list ────────────────────────────────────
            # sort=0 = sort by name (alphabetical), filter= = no filter
            await page.goto(
                "https://www.bramleygolfclub.co.uk/hcaplist.php"
                "?action=masterhcap&filter=&sort=0"
            )
            await page.wait_for_load_state("networkidle")

            # ── Parse all rows ───────────────────────────────────────────────
            # Structure: <table class="table table-striped">
            #   <tbody>
            #     <tr>
            #       <td><a href="...">Player Name</a></td>
            #       <td style="text-align:center;">14.2</td>   ← may be <span> for away HC
            #     </tr>
            rows = await page.query_selector_all("table.table tbody tr")
            indices = {}
            for row in rows:
                name_el = await row.query_selector("td:first-child a")
                idx_el  = await row.query_selector("td:last-child")
                if not name_el or not idx_el:
                    continue
                name      = (await name_el.inner_text()).strip()
                idx_text  = (await idx_el.inner_text()).strip()
                try:
                    indices[name] = float(idx_text)
                except ValueError:
                    pass  # skip malformed rows (e.g. headers, empty cells)

            return {"indices": indices}
        finally:
            await browser.close()
