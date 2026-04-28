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
    Find a UK golf course tee/rating data without any search API.
    1. Generate likely Intelligent Golf subdomain slugs from the name
    2. Probe each one for publicly accessible slope/rating pages
    3. Also try Golfshake as fallback
    Returns URL candidates for the user to pick from.
    """
    import urllib.parse, re as _re
    from bs4 import BeautifulSoup as _BS
    q = course_name.strip()

    # Direct URL — skip search, fetch tee data immediately
    if q.startswith("http"):
        return await fetch_course_from_url(q)

    # Generate slug candidates from the club name
    clean = _re.sub(r"\b(golf|club|gc|g\.c\.)\b", "", q, flags=_re.IGNORECASE)
    clean = _re.sub(r"[^a-z0-9\s]", "", clean.lower()).strip()
    words = clean.split()

    slugs = []
    slugs.append("".join(words))
    if words: slugs.append(words[0])
    if len(words) >= 2:
        slugs.append("-".join(words[:2]))
        slugs.append("".join(words[:2]))
    if len(words) >= 3:
        slugs.append("".join(words[:3]))
        slugs.append("-".join(words[:3]))
    slugs = list(dict.fromkeys(slugs))

    print(f"Trying IG slugs for '{q}': {slugs}")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-GB,en;q=0.9",
    }

    # Public slope/rating pages to try (login-protected pages excluded)
    ig_pages = [
        "/slope_rating",
        "/slope_ratings",
        "/course_and_slope_ratings",
        "/whs_-_course_slope_rating_tables",
        "/course-rating",
        "/course_handicap_table",
        "/scorecard",
    ]

    # Keywords that indicate a page is login-protected or not useful
    protected_keywords = [
        "please log in", "please login", "members only",
        "login required", "sign in to", "log in to access",
        "_csrf_token", "loginform", "memberid",
    ]

    candidates = []
    async with httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=10.0) as client:
        for slug in slugs:
            base = f"https://{slug}.intelligentgolf.co.uk"
            try:
                # Probe root
                probe = await client.get(base + "/", timeout=6.0)
                if probe.status_code not in (200, 301, 302, 403):
                    continue
                page_text_lower = probe.text.lower()
                # Must look like an IG golf site
                if "intelligentgolf" not in page_text_lower and "golf" not in page_text_lower:
                    continue
                print(f"  Found IG site: {base}")

                for page in ig_pages:
                    url = base + page
                    try:
                        r = await client.get(url, timeout=6.0)
                        if r.status_code != 200 or len(r.text) < 500:
                            continue
                        text_lower = r.text.lower()
                        # Skip login/protected pages
                        if any(kw in text_lower for kw in protected_keywords):
                            print(f"    Skipping protected page: {url}")
                            continue
                        # Must contain slope/rating content
                        if not any(kw in text_lower for kw in ["slope", "course rating", "cr ", "sss", "handicap"]):
                            continue
                        # Get club name from page title
                        soup = _BS(r.text, "html.parser")
                        title_el = soup.find("title")
                        raw_title = title_el.get_text(strip=True) if title_el else ""
                        # Clean up IG title format "Page Name :: Club Name"
                        parts = [p.strip() for p in _re.split(r"::|\|", raw_title)]
                        club_name = next((p for p in reversed(parts) if len(p) > 4), slug.replace("-"," ").title() + " Golf Club")
                        page_label = page.strip("/").replace("_"," ").replace("-"," ").title()
                        candidates.append({
                            "search_result": True,
                            "title": f"{club_name} — {page_label}",
                            "url": url,
                        })
                        print(f"    Found public page: {url}")
                        break  # One page per slug is enough
                    except Exception as e:
                        print(f"    Error on {url}: {e}")
                        continue
            except Exception as e:
                print(f"  Error probing {base}: {e}")
                continue

        # Fallback: try Golfshake direct URL construction
        if not candidates:
            print(f"  No IG pages found, trying Golfshake for '{q}'")
            gs_slug = "-".join(
                (_re.sub(r"[^a-z0-9]", "", w.lower()) for w in q.split()
                 if w.lower() not in ("the","a","an","of","at","&","and"))
            )
            gs_url = f"https://www.golfshake.com/course/search.php?name={urllib.parse.quote(q)}"
            try:
                r = await client.get(gs_url, timeout=8.0)
                if r.status_code == 200 and "course/view/" in r.text:
                    soup = _BS(r.text, "html.parser")
                    for a in soup.select("a[href*='/course/view/']")[:4]:
                        href = a.get("href","")
                        if not href.startswith("http"):
                            href = "https://www.golfshake.com" + href
                        href = href.split("?")[0]
                        label = a.get_text(strip=True) or href
                        if href not in [c["url"] for c in candidates]:
                            candidates.append({
                                "search_result": True,
                                "title": label[:80],
                                "url": href,
                            })
                            print(f"    Golfshake result: {href}")
            except Exception as e:
                print(f"  Golfshake error: {e}")

    print(f"Found {len(candidates)} candidates for '{q}'")
    return candidates


async def fetch_course_from_url(url: str, client=None) -> list[dict]:
    """Fetch and parse tee/CR/slope data from a course URL.
    Handles Golfshake pages and Intelligent Golf slope rating pages.
    """
    if "golfshake.com" in url:
        return await _fetch_golfshake_course(url, client=client)
    else:
        return await _fetch_ig_slope_page(url, client=client)


async def _fetch_ig_slope_page(url: str, client=None) -> list[dict]:
    """
    Fetch and parse an Intelligent Golf slope/rating page.
    These pages typically have text like:
      "Men's Yellow tees: Course Rating 68.9, Slope 128"
      or tables with tee/CR/slope data.
    """
    import re as _re
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
    }

    async def _do(c):
        resp = await c.get(url)
        soup = BeautifulSoup(resp.text, "html.parser")
        text = soup.get_text(" ", strip=True)

        # Extract club name
        h1 = soup.find("h1") or soup.find("h2")
        page_name = h1.get_text(strip=True) if h1 else url.split("/")[2].replace(".intelligentgolf.co.uk","").replace("-"," ").title()

        tees = []
        seen = set()

        # Pattern 1: "Yellow tees: Course Rating 68.9, Slope 128, Par 72"
        # Pattern 2: "Yellow Men CR 68.9 Slope 128"
        # Pattern 3: Table rows with tee name, CR, slope

        # Try table parsing first (Golfshake-style SSS[slope])
        for row in soup.select("table tr"):
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cells) < 4:
                continue
            for i, cell in enumerate(cells):
                m = _re.match(r"^([\d.]+)\s*\[(\d+)\]$", cell)
                if m:
                    cr, slope = float(m.group(1)), int(m.group(2))
                    tee_name = cells[1] if len(cells) > 1 else "Unknown"
                    try: par = int(cells[2])
                    except: par = 72
                    try: yardage = int(cells[-1].replace(",",""))
                    except: yardage = None
                    key = (tee_name, "Men")
                    if key not in seen and tee_name.lower() not in ("tee","course",""):
                        seen.add(key)
                        tees.append({"name": tee_name, "gender": "Men", "yardage": yardage,
                                     "course_rating": cr, "slope": slope, "par": par,
                                     "colour": _tee_colour(tee_name)})
                    break

        # Try free-text pattern: "Yellow ... Rating 68.9 ... Slope 128"
        if not tees:
            pattern = _re.compile(
                r"(White|Yellow|Red|Blue|Black|Gold|Silver|Purple|Orange|Green)"
                r"[^.]*?(?:Course\s*)?Rating[:\s]+([0-9.]+)"
                r"[^.]*?Slope[:\s]+(\d+)"
                r"(?:[^.]*?Par[:\s]+(\d+))?",
                _re.IGNORECASE
            )
            for m in pattern.finditer(text):
                tee_name = m.group(1).title()
                cr = float(m.group(2))
                slope = int(m.group(3))
                par = int(m.group(4)) if m.group(4) else 72
                key = (tee_name, "Men")
                if key not in seen:
                    seen.add(key)
                    tees.append({"name": tee_name, "gender": "Men", "yardage": None,
                                 "course_rating": cr, "slope": slope, "par": par,
                                 "colour": _tee_colour(tee_name)})

        print(f"_fetch_ig_slope_page: {len(tees)} tees from {url}")
        if not tees:
            sample = text[:300]
            print(f"  Page text sample: {sample}")

        if tees:
            return [{"club": page_name, "name": page_name, "url": url, "tees": tees}]
        return []

    if client:
        return await _do(client)
    else:
        async with httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=30.0) as c:
            return await _do(c)


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
                m = re.match(r"^([\d.]+)\s*\[(\d+)\]$", cell)
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
