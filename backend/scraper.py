import re
from playwright.async_api import async_playwright


async def scrape_players(
    ig_username: str,
    ig_pin: str,
    date_str: str,
    ig_search_term: str,
) -> dict:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        try:
            # ── Log in ───────────────────────────────────────────────────────
            await page.goto("https://www.bramleygolfclub.co.uk/member/index.php")
            await page.wait_for_selector('input[name="username"]', timeout=15000)
            await page.fill('input[name="username"]', ig_username)
            await page.fill('input[name="password"]', ig_pin)
            await page.click('input[type="submit"]')
            await page.wait_for_load_state("networkidle")

            # ── Scrape booking sheet ─────────────────────────────────────────
            await page.goto(
                f"https://www.bramleygolfclub.co.uk/memberbooking/"
                f"?date={date_str}&searchterm={ig_search_term}"
            )
            await page.wait_for_load_state("networkidle")

            name_elements = await page.query_selector_all(
                "td.booking-player a, .booking-name a, .player-name"
            )
            names = []
            for el in name_elements:
                text = (await el.inner_text()).strip()
                if text and text not in names:
                    names.append(text)

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

            # ── Scrape WHS indices ───────────────────────────────────────────
            await page.goto(
                "https://www.bramleygolfclub.co.uk/hcaplist.php"
                "?action=masterhcap&filter=&sort=0"
            )
            await page.wait_for_load_state("networkidle")

            rows = await page.query_selector_all("table.table tbody tr")
            indices = {}
            for row in rows:
                name_el = await row.query_selector("td:first-child a")
                idx_el  = await row.query_selector("td:last-child")
                if not name_el or not idx_el:
                    continue
                name     = (await name_el.inner_text()).strip()
                idx_text = (await idx_el.inner_text()).strip()
                try:
                    indices[name] = float(idx_text)
                except ValueError:
                    pass

            return {
                "names":     names,
                "tee_times": len(unique_tee_times),
                "tee_start": tee_start,
                "indices":   indices,
            }
        finally:
            await browser.close()


async def scrape_whs_indices(ig_username: str, ig_pin: str) -> dict:
    # Indices are now scraped as part of scrape_players in one session.
    # This stub is kept so main.py imports don't break.
    return {"indices": {}}
