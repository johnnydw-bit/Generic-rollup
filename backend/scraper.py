import re
import httpx
from bs4 import BeautifulSoup

BRAMLEY_BASE = "https://www.bramleygolfclub.co.uk"


async def _bramley_login(client: httpx.AsyncClient, username: str, pin: str):
    login_page = await client.get(f"{BRAMLEY_BASE}/login.php")
    soup = BeautifulSoup(login_page.text, "html.parser")
    form = soup.find("form")
    if not form:
        raise Exception(f"Login form not found. Page: {soup.title.string if soup.title else 'unknown'}")
    action = form.get("action", "/login.php")
    if not action or action == "/":
        login_url = f"{BRAMLEY_BASE}/login.php"
    elif action.startswith("/"):
        login_url = f"{BRAMLEY_BASE}{action}"
    else:
        login_url = action

    payload = {i.get("name"): i.get("value", "") for i in form.find_all("input") if i.get("name")}
    payload["username"] = username
    payload["password"] = pin

    await client.post(login_url, data=payload)


async def scrape_players(
    ig_username: str,
    ig_pin: str,
    date_str: str,
    ig_search_term: str,
) -> dict:
    async with httpx.AsyncClient(follow_redirects=True) as client:
        await _bramley_login(client, ig_username, ig_pin)

        # ── Scrape booking sheet ─────────────────────────────────────────
        booking = await client.get(
            f"{BRAMLEY_BASE}/memberbooking/",
            params={"date": date_str, "searchterm": ig_search_term}
        )
        soup = BeautifulSoup(booking.text, "html.parser")

        names = []
        for el in soup.select("td.booking-player a, .booking-name a, .player-name"):
            text = el.get_text(strip=True)
            if text and text not in names:
                names.append(text)

        tee_times_raw = []
        for el in soup.select("td.tee-time, .booking-time, td.time"):
            text = el.get_text(strip=True)
            if text and re.match(r'\d{1,2}:\d{2}', text):
                tee_times_raw.append(text)

        unique_tee_times = sorted(set(tee_times_raw))
        tee_start = unique_tee_times[0] if unique_tee_times else
