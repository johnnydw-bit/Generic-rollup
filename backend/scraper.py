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
    Search for a golf course by name using DuckDuckGo HTML search,
    then scrape tee data from the 18birdies.com course page.
    Returns a list of candidate courses, each with tee details.
    """
    import urllib.parse
    search_query = f"site:18birdies.com/golf-courses {course_name}"
    search_url   = f"https://html.duckduckgo.com/html/?q={urllib.parse.quote(search_query)}"

    ddg_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
    }

    async with httpx.AsyncClient(headers=ddg_headers, follow_redirects=True, timeout=30.0) as client:
        # Step 1: DuckDuckGo search to find 18birdies URLs
        resp = await client.get(search_url)
        soup = BeautifulSoup(resp.text, "html.parser")

        course_urls = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            # DDG result links are in uddg= param or direct
            if "18birdies.com/golf-courses/club/" in href:
                # Extract actual URL from DDG redirect
                if "uddg=" in href:
                    import urllib.parse as up
                    parsed = up.parse_qs(up.urlparse(href).query)
                    url = parsed.get("uddg", [None])[0]
                    if url:
                        url = urllib.parse.unquote(url)
                else:
                    url = href
                # Strip query string
                url = url.split("?")[0].rstrip("/")
                # Skip review pages
                if url and "/reviews" not in url and url not in course_urls:
                    course_urls.append(url)

        print(f"search_course_on_18birdies: found {len(course_urls)} URLs for '{course_name}'")
        for u in course_urls:
            print(f"  {u}")

        if not course_urls:
            return []

        # Step 2: Fetch and parse each course page (up to 3)
        results = []
        for url in course_urls[:3]:
            try:
                r = await client.get(url)
                cs = BeautifulSoup(r.text, "html.parser")

                # Course name from page title
                title_el = cs.find("h2") or cs.find("h1")
                page_name = title_el.get_text(strip=True) if title_el else url.split("/")[-1].replace("-", " ").title()

                # Parse tee lines — format: "White 5930 yds (122/69.3) for Men"
                tees = []
                seen = set()
                for el in cs.find_all(string=True):
                    import re
                    # 18birdies format: "White 5930 yds (slope/cr) for Men"
                    m = re.match(
                        r"^([\w][\w\s]*?)\s+(\d{3,5})\s+yds\s+\((\d+)/([\d.]+)\)\s+for\s+(Men|Women)$",
                        el.strip()
                    )
                    if m:
                        name, yds, slope, cr, gender = m.groups()
                        key = (name.strip(), gender)
                        if key not in seen:
                            seen.add(key)
                            tees.append({
                                "name":          name.strip(),
                                "gender":        gender,
                                "yardage":       int(yds),
                                "course_rating": float(cr),
                                "slope":         int(slope),
                                "par":           72,
                                "colour":        _tee_colour(name.strip()),
                            })

                print(f"  Parsed {len(tees)} tees from {url}")
                if tees:
                    results.append({
                        "club":     page_name,
                        "name":     page_name,
                        "url":      url,
                        "tees":     tees,
                    })
                else:
                    # Log a sample of text nodes to help diagnose regex mismatches
                    samples = [el.strip() for el in cs.find_all(string=True) if 'yds' in el and len(el.strip()) < 80][:5]
                    print(f"  No tees parsed. Sample yds strings: {samples}")
            except Exception as e:
                print(f"Error fetching {url}: {e}")
                continue

        return results


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
