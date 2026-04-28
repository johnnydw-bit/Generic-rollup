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
        # No tbody — rows are direct children of table; skip header rows (td vs th)
        for row in soup.select("table.table tr"):
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

        print(f"scrape_players: found {len(indices)} WHS indices")
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
    """Scrape WHS indices for all members from the Bramley handicap list page."""
    async with httpx.AsyncClient(
        headers=HEADERS,
        follow_redirects=True,
        timeout=30.0,
    ) as client:
        # Login
        resp = await client.get(LOGIN_URL)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        csrf_input = soup.find("input", {"name": "_csrf_token"})
        if not csrf_input:
            raise Exception("Could not find CSRF token on login page.")
        csrf_token = csrf_input.get("value", "")

        login_data = {
            "task": "login", "topmenu": "1",
            "memberid": ig_username, "pin": ig_pin,
            "cachemid": "1", "_csrf_token": csrf_token, "Submit": "Login",
        }
        resp = await client.post(LOGIN_URL, data=login_data)
        resp.raise_for_status()
        if str(resp.url).endswith("login.php"):
            raise Exception("Login failed. Please check your username and PIN.")

        if "ttbconsent" in str(resp.url):
            resp = await client.get(f"{CONSENT_URL}?action=accept")
            resp.raise_for_status()

        # Fetch handicap list
        resp = await client.get(
            HCAP_URL,
            params={"action": "masterhcap", "filter": "", "sort": "0"},
        )
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        indices = {}
        # No tbody — rows are direct children of table; skip header rows (td vs th)
        for row in soup.select("table.table tr"):
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

        print(f"scrape_whs_indices: found {len(indices)} entries")
        return {"indices": indices}


# ── Course search / tee data scraper ────────────────────────────────────────

async def search_course_on_18birdies(course_name: str) -> list[dict]:
    """
    Fetch tee/rating data for a UK golf course from Golfshake.com.
    Accepts either a direct Golfshake URL or a course name.
    For name searches, returns a hint to use a URL instead since
    server-side search engines block automated requests.
    """
    q = course_name.strip()

    # Direct URL — fetch and parse immediately
    if q.startswith("http") and "golfshake.com" in q:
        return await _fetch_golfshake_course(q)

    # Any other URL — try to fetch it
    if q.startswith("http"):
        return await _fetch_golfshake_course(q)

    # Name search — return a special marker so frontend can guide user
    return [{"hint": True, "name": q}]


async def fetch_course_from_url(url: str, client=None) -> list[dict]:
    """Public alias — fetch course from a Golfshake URL."""
    return await _fetch_golfshake_course(url, client=client)


async def _fetch_golfshake_course(url: str, client=None) -> list[dict]:
    """
    Fetch and parse tee/rating data from a Golfshake course page.
    Scorecard table format:
      Course | Tee | Par | SSS      | Yards
      Club   | White | 72 | 72 [135] | 6464
    SSS = Course Rating, [slope] in brackets.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
    }

    async def _do_fetch(c):
        resp = await c.get(url)
        cs = BeautifulSoup(resp.text, "html.parser")

        # Course name from h1
        h1 = cs.find("h1")
        page_name = h1.get_text(strip=True) if h1 else url.split("/")[-1].replace("_", " ").replace("-", " ").title()

        # Parse scorecard table
        # Row format: Course | Tee | Par | SSS [slope] | Yards
        tees = []
        seen = set()
        for row in cs.select("table tr"):
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cells) < 4:
                continue
            # Find the SSS [slope] cell — matches "72 [135]" or "69 [128]"
            for i, cell in enumerate(cells):
                m = re.match(r"^(\d+)\s*\[(\d+)\]$", cell)
                if m:
                    cr  = float(m.group(1))
                    slope = int(m.group(2))
                    # Tee name is usually cells[1], Par in cells[2], Yards in cells[-1] or cells[i+1]
                    tee_name = cells[1] if len(cells) > 1 else "Unknown"
                    try:
                        par = int(cells[2])
                    except (ValueError, IndexError):
                        par = 72
                    try:
                        yardage = int(cells[-1].replace(",", ""))
                    except (ValueError, IndexError):
                        yardage = None
                    key = (tee_name, "Men")
                    if key not in seen and tee_name.lower() not in ("tee", "course", ""):
                        seen.add(key)
                        tees.append({
                            "name":          tee_name,
                            "gender":        "Men",
                            "yardage":       yardage,
                            "course_rating": cr,
                            "slope":         slope,
                            "par":           par,
                            "colour":        _tee_colour(tee_name),
                        })
                    break

        print(f"_fetch_golfshake_course: {len(tees)} tees from {url}")
        if not tees:
            # Log sample rows for debug
            for row in cs.select("table tr")[:5]:
                cells = [td.get_text(strip=True) for td in row.find_all("td")]
                if cells:
                    print(f"  Sample row: {cells}")

        if tees:
            return [{"club": page_name, "name": page_name, "url": url, "tees": tees}]
        return []

    if client:
        return await _do_fetch(client)
    else:
        async with httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=30.0) as c:
            return await _do_fetch(c)


def _tee_colour(tee_name: str) -> str:
    """Map tee name to a hex colour."""
    colours = {
        "white":  "#FFFFFF",
        "yellow": "#FFD700",
        "red":    "#CC0000",
        "blue":   "#1E90FF",
        "black":  "#222222",
        "gold":   "#FFD700",
        "green":  "#228B22",
        "orange": "#FF8C00",
        "purple": "#6A0DAD",
        "silver": "#C0C0C0",
    }
    return colours.get(tee_name.lower(), "#888888")
