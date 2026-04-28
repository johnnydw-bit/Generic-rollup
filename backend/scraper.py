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
    Search USGA NCRDB for a UK golf course and return tee/CR/slope data.
    Tries the NCRDB search endpoint with Country=England and the course name.
    Falls back to Scotland/Wales/Ireland if England returns nothing.
    Also accepts a direct CourseID or courseTeeInfo URL.
    """
    import re as _re, urllib.parse
    from bs4 import BeautifulSoup as _BS
    q = course_name.strip()

    # Direct CourseID number or URL — fetch immediately
    m = _re.search(r"CourseID=(\d+)", q)
    if m or q.isdigit():
        course_id = m.group(1) if m else q
        url = f"https://ncrdb.usga.org/courseTeeInfo?CourseID={course_id}"
        return await fetch_course_from_url(url)

    # Full courseTeeInfo URL
    if q.startswith("http") and "ncrdb.usga.org" in q:
        return await fetch_course_from_url(q)

    # Search NCRDB by name + country
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
        "Referer": "https://ncrdb.usga.org/NCRListing",
    }

    countries = ["England", "Scotland", "Wales", "Ireland", "Northern Ireland"]
    candidates = []

    async with httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=15.0) as client:
        for country in countries:
            # Try GET with query params first
            search_url = (
                f"https://ncrdb.usga.org/NCRListing"
                f"?CourseName={urllib.parse.quote(q)}&Country={urllib.parse.quote(country)}"
            )
            try:
                resp = await client.get(search_url, timeout=10.0)
                print(f"NCRDB GET {country}: {resp.status_code} len={len(resp.text)}")

                if resp.status_code == 200 and len(resp.text) > 500:
                    soup = _BS(resp.text, "html.parser")
                    # Look for course links in results table
                    for a in soup.find_all("a", href=True):
                        href = a["href"]
                        if "CourseID=" in href:
                            if not href.startswith("http"):
                                href = "https://ncrdb.usga.org" + href
                            club = a.get_text(strip=True).split("(")[0].strip()
                            if href not in [c["url"] for c in candidates]:
                                candidates.append({
                                    "search_result": True,
                                    "title": f"{club} ({country})",
                                    "url": href,
                                })
                            print(f"  Found: {club} -> {href}")

                if candidates:
                    break  # Found results, no need to try other countries

            except Exception as e:
                print(f"NCRDB search error ({country}): {e}")
                continue

        # If GET didn't work, try POST
        if not candidates:
            try:
                resp = await client.post(
                    "https://ncrdb.usga.org/NCRListing",
                    data={"CourseName": q, "Country": "England", "CourseState": ""},
                    timeout=10.0,
                )
                print(f"NCRDB POST: {resp.status_code} len={len(resp.text)}")
                if resp.status_code == 200:
                    soup = _BS(resp.text, "html.parser")
                    for a in soup.find_all("a", href=True):
                        href = a["href"]
                        if "CourseID=" in href:
                            if not href.startswith("http"):
                                href = "https://ncrdb.usga.org" + href
                            club = a.get_text(strip=True).split("(")[0].strip()
                            if href not in [c["url"] for c in candidates]:
                                candidates.append({
                                    "search_result": True,
                                    "title": f"{club}",
                                    "url": href,
                                })
                            print(f"  POST found: {club}")
            except Exception as e:
                print(f"NCRDB POST error: {e}")

    print(f"NCRDB search found {len(candidates)} results for '{q}'")
    return candidates


async def fetch_course_from_url(url: str, client=None) -> list[dict]:
    """Fetch and parse tee/CR/slope data from a course URL.
    Handles NCRDB, Golfshake, and Intelligent Golf slope rating pages.
    """
    if "ncrdb.usga.org" in url:
        return await _fetch_ncrdb_course(url, client=client)
    elif "golfshake.com" in url:
        return await _fetch_golfshake_course(url, client=client)
    else:
        return await _fetch_ig_slope_page(url, client=client)


async def _fetch_ncrdb_course(url: str, client=None) -> list[dict]:
    """Parse the NCRDB courseTeeInfo page."""
    import re as _re
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9,en-US;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "Referer": "https://ncrdb.usga.org/NCRListing",
    }

    async def _do(c):
        resp = await c.get(url)
        if resp.status_code != 200:
            print(f"NCRDB returned {resp.status_code}")
            return []
        soup = BeautifulSoup(resp.text, "html.parser")

        # Club name from first table row
        club_name = ""
        for row in soup.select("table tr"):
            cells = [td.get_text(strip=True) for td in row.find_all(["td","th"])]
            if cells and len(cells) >= 1 and cells[0] and "Tee Name" not in cells[0]:
                club_name = cells[0].split("(")[0].strip()  # remove trailing (id)
                if club_name:
                    break

        # Find the tee data table — has "Tee Name" header
        tees = []
        seen = set()
        for table in soup.find_all("table"):
            headers_row = table.find("tr")
            if not headers_row:
                continue
            col_texts = [th.get_text(strip=True).lower() for th in headers_row.find_all(["th","td"])]
            if "tee name" not in col_texts:
                continue
            # Map column indices
            try:
                i_name   = col_texts.index("tee name")
                i_gender = col_texts.index("gender")
                i_par    = col_texts.index("par")
                i_cr     = next(i for i, h in enumerate(col_texts) if "course rating" in h)
                i_slope  = next(i for i, h in enumerate(col_texts) if "slope rating" in h)
            except (ValueError, StopIteration):
                continue

            for row in table.find_all("tr")[1:]:
                cells = [td.get_text(strip=True) for td in row.find_all("td")]
                if len(cells) <= max(i_name, i_gender, i_par, i_cr, i_slope):
                    continue
                try:
                    tee_name = cells[i_name].title()
                    gender   = "Men" if cells[i_gender].upper() in ("M", "MEN", "MALE") else "Women"
                    par      = int(cells[i_par])
                    cr       = float(cells[i_cr])
                    slope    = int(cells[i_slope])
                    # Length is usually the last meaningful column
                    length = None
                    for cell in reversed(cells):
                        try:
                            length = int(cell.replace(",",""))
                            if 3000 < length < 8000:
                                break
                            length = None
                        except ValueError:
                            continue
                    key = (tee_name, gender)
                    if key not in seen:
                        seen.add(key)
                        tees.append({
                            "name":          tee_name,
                            "gender":        gender,
                            "par":           par,
                            "course_rating": cr,
                            "slope":         slope,
                            "yardage":       length,
                            "colour":        _tee_colour(tee_name),
                        })
                except (ValueError, IndexError):
                    continue
            if tees:
                break

        print(f"_fetch_ncrdb_course: {len(tees)} tees from {url}")
        if tees:
            return [{"club": club_name, "name": club_name, "url": url, "tees": tees}]
        return []

    if client:
        return await _do(client)
    else:
        async with httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=15.0) as c:
            return await _do(c)


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


def parse_ncrdb_paste(text: str, club_name: str = "") -> list[dict]:
    """
    Parse tee data from plain text copied from an NCRDB courseTeeInfo page.
    Handles both spaced and squashed column formats.
    """
    import re as _re
    tees = []
    seen = set()

    for line in text.split("\n"):
        line = line.strip()
        if not line or "Tee Name" in line or "Gender" in line:
            continue

        # Split on M or F gender marker
        gm = _re.search(r"^(.*?)(M|F)(.+)$", line)
        if not gm:
            continue

        raw_name    = gm.group(1).strip()
        gender_char = gm.group(2)
        numbers_str = gm.group(3)

        # Clean tee name
        name = _re.sub(r"^[\d\s./]+", "", raw_name).strip().title()
        if not name:
            nums_in_name = _re.findall(r"\d+", raw_name)
            name = nums_in_name[-1] if nums_in_name else raw_name.strip()
        if not name or name.lower() in ("tee name", "gender", "par", "course"):
            continue

        # Split on first "/" to isolate par+CR+bogey+slope from F9/B9 fractions
        main = numbers_str.split("/")[0].replace(" ", "")

        # Try 3-digit slope first, then 2-digit if out of range
        par, cr, slope = None, None, None
        for slope_digits in (3, 2):
            pat = (_re.compile(r"^(\d{2})(\d{2,3}\.\d)(\d{2,3}\.\d)(\d{" + str(slope_digits) + r"})"))
            dm = pat.match(main)
            if dm:
                p, c, _, s = int(dm.group(1)), float(dm.group(2)), dm.group(3), int(dm.group(4))
                if 50 <= s <= 155:
                    par, cr, slope = p, c, s
                    break

        if par is None:
            # Fallback: spaced format
            nums = _re.findall(r"[\d]+\.?[\d]*", numbers_str)
            if len(nums) >= 4:
                try:
                    par, cr, slope = int(nums[0]), float(nums[1]), int(nums[3])
                except (ValueError, IndexError):
                    continue

        if par is None:
            continue

        gender = "Men" if gender_char == "M" else "Women"
        if not (50 <= slope <= 155 and 55 <= cr <= 85 and 60 <= par <= 80):
            continue

        key = (name, gender)
        if key not in seen:
            seen.add(key)
            tees.append({
                "name":          name,
                "gender":        gender,
                "par":           par,
                "course_rating": cr,
                "slope":         slope,
                "yardage":       None,
                "colour":        _tee_colour(name),
            })

    # Extract club name from text if not provided
    if not club_name:
        for line in text.split("\n"):
            line = line.strip()
            if not line or len(line) < 5:
                continue
            if "Tee Name" in line or "Gender" in line:
                break
            if not line.startswith("National") and not line.startswith("Course Rating"):
                candidate = line.split("(")[0].strip()
                candidate = _re.sub(r"^[\d\s.]+", "", candidate).strip()
                if candidate:
                    club_name = candidate
                    break

    print(f"parse_ncrdb_paste: {len(tees)} tees, club=\'{club_name}\'")
    if tees:
        return [{"club": club_name or "Golf Club", "name": club_name or "Golf Club",
                 "url": "", "tees": tees}]
    return []


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
