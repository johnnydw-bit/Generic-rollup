"""
Intelligent Golf scraper for Bramley Golf Club rollups.
Uses httpx only - no browser required.
Matches any rollup by search term rather than hardcoded MOTH.
"""

from datetime import datetime
import re
import math
import httpx
from bs4 import BeautifulSoup


BASE_URL = "https://www.bramleygolfclub.co.uk"
LOGIN_URL = f"{BASE_URL}/login.php"
CONSENT_URL = f"{BASE_URL}/ttbconsent.php"
BOOKING_URL = f"{BASE_URL}/memberbooking/"
HCAP_URL = f"{BASE_URL}/hcaplist.php"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 "
        "Mobile/15E148 Safari/604.1"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
}


async def scrape_players(
    username: str,
    pin: str,
    date_str: str,
    ig_search_term: str = "MOTH",
) -> dict:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    date_param = dt.strftime("%d-%m-%Y")

    async with httpx.AsyncClient(
        headers=HEADERS,
        follow_redirects=True,
        timeout=30.0,
    ) as client:

        # Step 1: GET login page for CSRF token
        resp = await client.get(LOGIN_URL)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        csrf_input = soup.find("input", {"name": "_csrf_token"})
        if not csrf_input:
            raise Exception("Could not find CSRF token on login page.")
        csrf_token = csrf_input.get("value", "")

        # Step 2: POST login
        login_data = {
            "task": "login",
            "topmenu": "1",
            "memberid": username,
            "pin": pin,
            "cachemid": "1",
            "_csrf_token": csrf_token,
            "Submit": "Login",
        }
        resp = await client.post(LOGIN_URL, data=login_data)
        resp.raise_for_status()

        if str(resp.url).endswith("login.php"):
            raise Exception("Login failed. Please check your username and PIN.")

        # Step 3: Accept consent after login if needed
        if "ttbconsent" in str(resp.url):
            resp = await client.get(f"{CONSENT_URL}?action=accept")
            resp.raise_for_status()

        # Step 4: GET booking page
        resp = await client.get(
            BOOKING_URL,
            params={"date": date_param, "course": "1", "group": "1"},
        )
        resp.raise_for_status()

        if "ttbconsent" in str(resp.url):
            resp = await client.get(f"{CONSENT_URL}?action=accept")
            resp.raise_for_status()
            resp = await client.get(
                BOOKING_URL,
                params={"date": date_param, "course": "1", "group": "1"},
            )
            resp.raise_for_status()

        if "login" in str(resp.url).lower():
            raise Exception("Session expired or login failed.")

        # Step 5: Find the rollup matching ig_search_term
        soup = BeautifulSoup(resp.text, "html.parser")
        rollup_wrappers = soup.find_all("div", class_="isRollup")

        if not rollup_wrappers:
            raise Exception(
                f"No rollups found on the booking page for {date_str}. "
                "Check the date is correct."
            )

        names = []
        tee_times = 0
        for wrapper in rollup_wrappers:
            entrant_divs = wrapper.find_all("div", class_="rollup-entrants-list")
            contact_div = None
            signed_up_div = None
            for div in entrant_divs:
                t = div.get_text(strip=True)
                if "Roll up Contact" in t:
                    contact_div = div
                elif "Signed up" in t:
                    signed_up_div = div

            if contact_div and ig_search_term.upper() in contact_div.get_text().upper():
                if not signed_up_div:
                    raise Exception(f"Found '{ig_search_term}' rollup but no players have signed up yet.")
                italic = signed_up_div.find("i")
                if not italic:
                    raise Exception(f"Found '{ig_search_term}' rollup but could not parse player names.")
                names = [n.strip() for n in italic.get_text(strip=True).split(",") if n.strip()]
                if not names:
                    raise Exception(f"Found '{ig_search_term}' rollup but the signed-up list is empty.")
                tee_times = _count_tee_times(wrapper, names)
                break

        if not names:
            raise Exception(
                f"Could not find a rollup matching '{ig_search_term}' on the booking page for {date_str}. "
                "Check the rollup name and date are correct."
            )

        # Step 6: Scrape WHS indices (same session, already logged in)
        resp = await client.get(
            HCAP_URL,
            params={"action": "masterhcap", "filter": "", "sort": "0"},
        )
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        indices = {}
        for row in soup.select("table.table tbody tr"):
            name_el = row.select_one("td:first-child a")
            idx_el  = row.select_one("td:last-child")
            if not name_el or not idx_el:
                continue
            name     = name_el.get_text(strip=True)
            idx_text = idx_el.get_text(strip=True)
            try:
                indices[name] = float(idx_text)
            except ValueError:
                pass

        return {
            "names":     names,
            "tee_times": tee_times,
            "indices":   indices,
        }


def _count_tee_times(wrapper, names: list[str]) -> int:
    time_span = wrapper.find("span", class_="comp-time-info")
    if time_span:
        match = re.search(r'(\d{2}):(\d{2})-(\d{2}):(\d{2})', time_span.get_text())
        if match:
            start = int(match.group(1)) * 60 + int(match.group(2))
            end   = int(match.group(3)) * 60 + int(match.group(4))
            if end > start:
                return (end - start) // 8 + 1
    return math.ceil(len(names) / 4)


async def scrape_whs_indices(ig_username: str, ig_pin: str) -> dict:
    # Indices now scraped as part of scrape_players in one session.
    return {"indices": {}}
