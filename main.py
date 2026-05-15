# Bramley Rollup - main.py
# v5 Multi-tenant:
#   - Tenants identified by URL slug  (GET /{slug} serves the app)
#   - X-Tenant-Slug header on all API calls
#   - Admin login via GitHub OAuth (GITHUB_CLIENT_ID / GITHUB_CLIENT_SECRET env vars)
#   - Admin cross-tenant visibility via /api/admin/* endpoints

import os
import secrets
import time
from fastapi import FastAPI, HTTPException, Query, Header, Depends, Cookie
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from fastapi import Request
from pydantic import BaseModel
from dotenv import load_dotenv
import httpx

from backend.handicap import (
    calculate_new_handicaps,
    calculate_whs_handicaps,
    calculate_team_scores,
    format_adjustment,
)
import backend.db as db
from backend.db import (
    get_all_players,
    get_all_players_detail,
    update_player_handicap,
    update_player_whs_index,
    remove_player,
    get_all_rollups,
    get_or_create_rollup,
    save_round_results,
    add_new_player,
    get_last_round_results,
    get_last_round_date,
    get_player_history,
    get_round_dates,
    get_round_by_date,
    get_rollup_settings,
    save_rollup_settings,
    get_tenant_credentials,
    save_tenant_credentials,
    get_all_courses,
    get_tees_for_course,
    save_course,
    get_prohibited_winners,
    validate_rollup_tenant,
    init_db,
    close_db,
)
from backend.scraper import (
    scrape_players,
    search_course_on_18birdies,
    fetch_course_from_url,
    parse_ncrdb_paste,
)

load_dotenv()

app = FastAPI(title="Rollup Manager")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="frontend/static"), name="static")
templates = Jinja2Templates(directory="frontend/templates")

# ---------------------------------------------------------------------------
# Admin sessions  (in-memory; survives process lifetime — ~24 h TTL)
# ---------------------------------------------------------------------------

_admin_sessions: dict[str, dict] = {}  # token -> {github_username, expires_at}
_ADMIN_SESSION_TTL = 86_400  # 24 hours

# ---------------------------------------------------------------------------
# User sessions  (in-memory; 30-day TTL)
# ---------------------------------------------------------------------------

_user_sessions: dict[str, dict] = {}
_USER_SESSION_TTL = 30 * 86_400


def _create_user_session(user_id: int, tenant_id: int, tenant_slug: str,
                         tenant_name: str, ig_username: str | None,
                         ig_pin: str | None) -> str:
    token = secrets.token_urlsafe(32)
    _user_sessions[token] = {
        "user_id":     user_id,
        "tenant_id":   tenant_id,
        "tenant_slug": tenant_slug,
        "tenant_name": tenant_name,
        "ig_username": ig_username,
        "ig_pin":      ig_pin,
        "expires_at":  time.time() + _USER_SESSION_TTL,
    }
    return token


def _validate_user_session(token: str) -> dict | None:
    s = _user_sessions.get(token)
    if not s:
        return None
    if time.time() > s["expires_at"]:
        _user_sessions.pop(token, None)
        return None
    return s


def _hash_password(password: str) -> str:
    import hashlib, binascii
    salt = os.urandom(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 200_000)
    return binascii.hexlify(salt).decode() + ":" + binascii.hexlify(dk).decode()


def _create_admin_session(github_username: str) -> str:
    token = secrets.token_urlsafe(32)
    _admin_sessions[token] = {
        "github_username": github_username,
        "expires_at":      time.time() + _ADMIN_SESSION_TTL,
    }
    return token


def _validate_admin_session(token: str) -> str | None:
    """Return github_username if token is valid, else None."""
    session = _admin_sessions.get(token)
    if not session:
        return None
    if time.time() > session["expires_at"]:
        _admin_sessions.pop(token, None)
        return None
    return session["github_username"]


# ---------------------------------------------------------------------------
# Startup / shutdown
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def startup():
    await init_db()


@app.on_event("shutdown")
async def shutdown():
    await close_db()


# ---------------------------------------------------------------------------
# Service worker
# ---------------------------------------------------------------------------

@app.get("/sw.js")
async def service_worker():
    from fastapi.responses import Response
    js = "self.addEventListener('fetch', function(event) {});"
    return Response(content=js, media_type="application/javascript")


# ---------------------------------------------------------------------------
# Root — landing / redirect
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    return HTMLResponse("""
    <!DOCTYPE html>
    <html><head><meta charset="UTF-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1"/>
    <title>Rollup Manager</title>
    <style>
      * { box-sizing: border-box; margin: 0; padding: 0; }
      body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
             background: #2D2B5B; display: flex; align-items: center;
             justify-content: center; min-height: 100vh; padding: 20px; }
      .box { background: #fff; border-radius: 16px; padding: 28px 24px;
             max-width: 380px; width: 100%; }
      h1 { color: #2D2B5B; font-size: 22px; font-weight: 700; margin-bottom: 4px; }
      .sub { color: #888; font-size: 13px; margin-bottom: 20px; }
      .tabs { display: flex; border-radius: 8px; overflow: hidden;
              border: 1.5px solid #2D2B5B; margin-bottom: 20px; }
      .tab { flex: 1; padding: 9px; font-size: 13px; font-weight: 600;
             border: none; cursor: pointer; font-family: inherit;
             background: #fff; color: #2D2B5B; }
      .tab.active { background: #2D2B5B; color: #FFD700; }
      .label { font-size: 10px; font-weight: 700; color: #2D2B5B; text-transform: uppercase;
               letter-spacing: 0.06em; margin-bottom: 4px; }
      input { width: 100%; padding: 10px 12px; border-radius: 8px;
              border: 1.5px solid #2D2B5B40; font-size: 15px; margin-bottom: 12px;
              font-family: inherit; }
      input:focus { outline: none; border-color: #2D2B5B; }
      button.go { width: 100%; padding: 13px; background: #2D2B5B; color: #FFD700;
                  border: none; border-radius: 10px; font-size: 15px; font-weight: 700;
                  cursor: pointer; font-family: inherit; margin-top: 4px; }
      .err { color: #A32D2D; font-size: 12px; text-align: center; margin-top: 8px;
             display: none; }
      .hint { color: #888; font-size: 11px; margin-top: -8px; margin-bottom: 12px; }
      .club-name { font-size: 13px; font-weight: 700; color: #2D2B5B; text-align: center;
                   margin-bottom: 12px; min-height: 18px; }
      .admin-link { margin-top: 20px; padding-top: 16px; border-top: 1px solid #eee;
                    text-align: center; font-size: 12px; color: #aaa; }
      .admin-link a { color: #2D2B5B; font-weight: 600; text-decoration: none; }
    </style>
    </head><body>
    <div class="box">
      <h1>Rollup Manager</h1>
      <p class="sub">Sign in to your club's rollup app</p>

      <div class="tabs">
        <button class="tab active" onclick="showTab('login')">Sign in</button>
        <button class="tab" onclick="showTab('register')">Register</button>
      </div>

      <!-- Sign in -->
      <div id="loginForm">
        <div class="label">Club slug</div>
        <input id="li_slug" placeholder="e.g. bramley" autocapitalize="none"
               oninput="onSlugInput('li_slug','li_clubname','li_credlabel','li_credplaceholder')"/>
        <div class="club-name" id="li_clubname"></div>
        <div class="label" id="li_credlabel">Member ID</div>
        <input id="li_user" placeholder="Your member ID" autocomplete="username"/>
        <div class="label">PIN / Password</div>
        <input id="li_pass" type="password" placeholder="PIN or password" autocomplete="current-password"/>
        <button class="go" onclick="doLogin()">Sign in</button>
        <div class="err" id="li_err"></div>
      </div>

      <!-- Register -->
      <div id="registerForm" style="display:none">
        <div class="label">Club slug</div>
        <input id="re_slug" placeholder="e.g. bramley" autocapitalize="none"
               oninput="onSlugInput('re_slug','re_clubname','re_credlabel','re_credplaceholder')"/>
        <div class="club-name" id="re_clubname"></div>
        <div class="label" id="re_credlabel">Member ID / Username</div>
        <input id="re_user" placeholder="Your member ID or chosen username" autocomplete="username"/>
        <div class="label">PIN / Password</div>
        <input id="re_pass" type="password" placeholder="PIN or password" autocomplete="new-password"/>
        <div class="hint" id="re_hint">For Intelligent Golf clubs: use your IG member ID and PIN</div>
        <button class="go" onclick="doRegister()">Create account</button>
        <div class="err" id="re_err"></div>
      </div>

      <div class="admin-link"><a href="/admin/login">Admin login</a></div>
    </div>
    <script>
    function showTab(t) {
      document.getElementById('loginForm').style.display    = t==='login'    ? '' : 'none';
      document.getElementById('registerForm').style.display = t==='register' ? '' : 'none';
      document.querySelectorAll('.tab').forEach((b,i)=>b.classList.toggle('active', (t==='login'?i===0:i===1)));
    }
    let _slugCache = {};
    async function onSlugInput(slugId, nameId, labelId) {
      const slug = document.getElementById(slugId).value.trim().toLowerCase();
      const nameEl = document.getElementById(nameId);
      const labelEl = document.getElementById(labelId);
      if (!slug) { nameEl.textContent=''; return; }
      if (_slugCache[slug] !== undefined) { applyTenantInfo(_slugCache[slug], nameEl, labelEl); return; }
      try {
        const r = await fetch('/api/tenant-info?slug='+encodeURIComponent(slug));
        if (r.ok) {
          const d = await r.json();
          _slugCache[slug] = d;
          applyTenantInfo(d, nameEl, labelEl);
        } else {
          _slugCache[slug] = null;
          nameEl.textContent = 'Club not found';
          nameEl.style.color = '#A32D2D';
        }
      } catch(e) {}
    }
    function applyTenantInfo(d, nameEl, labelEl) {
      if (!d) return;
      nameEl.textContent = d.name;
      nameEl.style.color = '#2D2B5B';
      labelEl.textContent = d.ig_tenant ? 'IG Member ID' : 'Username';
    }
    async function doLogin() {
      const slug = document.getElementById('li_slug').value.trim().toLowerCase();
      const user = document.getElementById('li_user').value.trim();
      const pass = document.getElementById('li_pass').value;
      const err  = document.getElementById('li_err');
      err.style.display = 'none';
      if (!slug || !user || !pass) { err.textContent='All fields required.'; err.style.display='block'; return; }
      try {
        const r = await fetch('/api/login', {method:'POST',
          headers:{'Content-Type':'application/json'},
          body: JSON.stringify({slug, username:user, credential:pass})});
        const d = await r.json();
        if (!r.ok) { err.textContent = d.detail||'Sign in failed.'; err.style.display='block'; return; }
        localStorage.setItem('session_token', d.session_token);
        localStorage.setItem('tenant_slug', d.tenant_slug);
        window.location.href = '/'+d.tenant_slug;
      } catch(e) { err.textContent='Network error.'; err.style.display='block'; }
    }
    async function doRegister() {
      const slug = document.getElementById('re_slug').value.trim().toLowerCase();
      const user = document.getElementById('re_user').value.trim();
      const pass = document.getElementById('re_pass').value;
      const err  = document.getElementById('re_err');
      err.style.display = 'none';
      if (!slug || !user || !pass) { err.textContent='All fields required.'; err.style.display='block'; return; }
      try {
        const r = await fetch('/api/register', {method:'POST',
          headers:{'Content-Type':'application/json'},
          body: JSON.stringify({slug, username:user, credential:pass})});
        const d = await r.json();
        if (!r.ok) { err.textContent = d.detail||'Registration failed.'; err.style.display='block'; return; }
        localStorage.setItem('session_token', d.session_token);
        localStorage.setItem('tenant_slug', d.tenant_slug);
        window.location.href = '/'+d.tenant_slug;
      } catch(e) { err.textContent='Network error.'; err.style.display='block'; }
    }
    </script>
    </body></html>
    """)


# ---------------------------------------------------------------------------
# User auth dependency + tenant-info / login / register / logout endpoints
# ---------------------------------------------------------------------------

async def get_current_session(
    x_session_token: str | None = Header(default=None, alias="X-Session-Token"),
    session_token:   str | None = Cookie(default=None),
) -> dict:
    token = x_session_token or session_token
    if not token:
        raise HTTPException(401, "Not authenticated")
    s = _validate_user_session(token)
    if not s:
        raise HTTPException(401, "Session expired — please sign in again")
    return s


async def get_current_tenant(session: dict = Depends(get_current_session)) -> int:
    return session["tenant_id"]


async def _assert_rollup_access(rollup_id: int, tenant_id: int):
    if not await validate_rollup_tenant(rollup_id, tenant_id):
        raise HTTPException(403, "Access denied to this rollup")


@app.get("/api/tenant-info")
async def tenant_info(slug: str = Query(...)):
    """Public endpoint — returns club name and auth type for the login form."""
    tenant = await db.get_tenant_by_slug(slug)
    if not tenant:
        raise HTTPException(404, f"No club found for '{slug}'")
    return {"name": tenant["name"], "ig_tenant": tenant["ig_tenant"]}


class LoginRequest(BaseModel):
    slug:       str
    username:   str
    credential: str  # IG PIN or password


class RegisterRequest(BaseModel):
    slug:       str
    username:   str
    credential: str
    display_name: str = ""


@app.post("/api/login")
async def login(body: LoginRequest):
    tenant = await db.get_tenant_by_slug(body.slug.lower())
    if not tenant:
        raise HTTPException(404, "Club not found")
    user = await db.authenticate_user(tenant["id"], body.username, body.credential)
    if not user:
        raise HTTPException(401, "Invalid credentials")
    ig_username = user["username"] if tenant["ig_tenant"] else None
    ig_pin      = user["ig_pin"]   if tenant["ig_tenant"] else None
    token = _create_user_session(user["id"], tenant["id"], tenant["slug"],
                                 tenant["name"], ig_username, ig_pin)
    return {"session_token": token, "tenant_slug": tenant["slug"],
            "tenant_name": tenant["name"]}


@app.post("/api/register")
async def register(body: RegisterRequest):
    tenant = await db.get_tenant_by_slug(body.slug.lower())
    if not tenant:
        raise HTTPException(404, "Club not found")
    existing = await db.get_user_by_username(tenant["id"], body.username)
    if existing:
        raise HTTPException(409, "That username is already registered for this club")
    if len(body.credential) < 4:
        raise HTTPException(400, "PIN/password must be at least 4 characters")
    if tenant["ig_tenant"]:
        user_id = await db.create_user(tenant["id"], body.username,
                                       ig_pin=body.credential,
                                       display_name=body.display_name or None)
        ig_username, ig_pin = body.username, body.credential
    else:
        ph = _hash_password(body.credential)
        user_id = await db.create_user(tenant["id"], body.username,
                                       password_hash=ph,
                                       display_name=body.display_name or None)
        ig_username, ig_pin = None, None
    token = _create_user_session(user_id, tenant["id"], tenant["slug"],
                                 tenant["name"], ig_username, ig_pin)
    return {"session_token": token, "tenant_slug": tenant["slug"],
            "tenant_name": tenant["name"]}


@app.post("/api/logout")
async def logout(
    x_session_token: str | None = Header(default=None, alias="X-Session-Token"),
    session_token:   str | None = Cookie(default=None),
):
    token = x_session_token or session_token
    if token:
        _user_sessions.pop(token, None)
    return {"ok": True}


# ---------------------------------------------------------------------------
# Admin auth — GitHub OAuth
# ---------------------------------------------------------------------------
# Required env vars:
#   GITHUB_CLIENT_ID      — GitHub OAuth App client ID
#   GITHUB_CLIENT_SECRET  — GitHub OAuth App client secret
#   ADMIN_GITHUB_USERNAME — GitHub username allowed admin access
#   APP_BASE_URL          — e.g. https://moths-rollup.onrender.com (no trailing slash)

def _admin_callback_url() -> str:
    base = os.getenv("APP_BASE_URL", "http://localhost:8000").rstrip("/")
    return f"{base}/admin/callback"


async def get_admin_user(
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    admin_token:   str | None = Cookie(default=None),
) -> str:
    """Dependency — returns github_username if admin session is valid."""
    token = x_admin_token or admin_token
    if not token:
        raise HTTPException(401, "Admin authentication required")
    username = _validate_admin_session(token)
    if not username:
        raise HTTPException(401, "Admin session expired — please sign in again")
    return username


# Admin login — redirects to GitHub OAuth
@app.get("/admin/login")
async def admin_login():
    client_id = os.getenv("GITHUB_CLIENT_ID")
    if not client_id:
        raise HTTPException(503, "GitHub OAuth not configured (GITHUB_CLIENT_ID missing)")
    callback = _admin_callback_url()
    state    = secrets.token_urlsafe(16)
    github_url = (
        f"https://github.com/login/oauth/authorize"
        f"?client_id={client_id}"
        f"&redirect_uri={callback}"
        f"&scope=read:user"
        f"&state={state}"
    )
    return RedirectResponse(github_url)


# GitHub OAuth callback
@app.get("/admin/callback")
async def admin_callback(code: str, state: str | None = None):
    client_id     = os.getenv("GITHUB_CLIENT_ID")
    client_secret = os.getenv("GITHUB_CLIENT_SECRET")
    allowed_user  = os.getenv("ADMIN_GITHUB_USERNAME", "")

    if not client_id or not client_secret:
        raise HTTPException(503, "GitHub OAuth not configured")

    # Exchange code for access token
    async with httpx.AsyncClient() as client:
        token_resp = await client.post(
            "https://github.com/login/oauth/access_token",
            json={
                "client_id":     client_id,
                "client_secret": client_secret,
                "code":          code,
                "redirect_uri":  _admin_callback_url(),
            },
            headers={"Accept": "application/json"},
        )
        token_data = token_resp.json()
        access_token = token_data.get("access_token")
        if not access_token:
            raise HTTPException(400, "GitHub OAuth failed — could not get access token")

        # Get GitHub user
        user_resp = await client.get(
            "https://api.github.com/user",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Accept":        "application/vnd.github+json",
            },
        )
        user_data = user_resp.json()
        github_username = user_data.get("login", "")

    if not github_username:
        raise HTTPException(400, "Could not retrieve GitHub username")

    if allowed_user and github_username.lower() != allowed_user.lower():
        raise HTTPException(403, f"GitHub user '{github_username}' is not the configured admin")

    session_token = _create_admin_session(github_username)
    response = RedirectResponse("/admin")
    response.set_cookie(
        "admin_token", session_token,
        httponly=True, samesite="lax", max_age=_ADMIN_SESSION_TTL,
    )
    return response


# Admin dashboard
@app.get("/admin", response_class=HTMLResponse)
async def admin_dashboard(
    request:     Request,
    admin_token: str | None = Cookie(default=None),
):
    if not admin_token or not _validate_admin_session(admin_token):
        return RedirectResponse("/admin/login")

    tenants = await db.get_all_tenants()
    base = os.getenv("APP_BASE_URL", "").rstrip("/")
    rows = "".join(
        f"<tr>"
        f"<td>{t['id']}</td>"
        f"<td><strong>{t['name']}</strong></td>"
        f"<td><code>{t['slug']}</code></td>"
        f"<td>{str(t['created_at'])[:10]}</td>"
        f"<td><a href='/admin/visit/{t['slug']}'>Visit →</a></td>"
        f"</tr>"
        for t in tenants
    )
    return HTMLResponse(f"""
    <!DOCTYPE html>
    <html><head><meta charset="UTF-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1"/>
    <title>Admin — Rollup Manager</title>
    <style>
      body {{ font-family: -apple-system, sans-serif; background: #f5f5f0;
               padding: 24px; max-width: 800px; margin: 0 auto; }}
      h1 {{ color: #2D2B5B; }} h2 {{ color: #2D2B5B; font-size: 16px; margin-top: 28px; }}
      table {{ width: 100%; border-collapse: collapse; background: #fff; border-radius: 8px; overflow: hidden; }}
      th {{ background: #2D2B5B; color: #FFD700; padding: 8px 12px; text-align: left; font-size: 13px; }}
      td {{ padding: 8px 12px; font-size: 13px; border-bottom: 1px solid #eee; }}
      code {{ background: #f0f0f0; padding: 2px 6px; border-radius: 4px; }}
      form {{ display: flex; gap: 8px; flex-wrap: wrap; margin-top: 12px; }}
      input {{ padding: 8px 10px; border: 1.5px solid #ccc; border-radius: 6px; font-size: 13px; }}
      button {{ padding: 8px 16px; background: #2D2B5B; color: #FFD700; border: none;
                border-radius: 6px; font-size: 13px; font-weight: 600; cursor: pointer; }}
      a {{ color: #2D2B5B; font-weight: 600; }}
    </style>
    </head><body>
    <h1>Admin Panel</h1>
    <p>Signed in via GitHub. &nbsp;<a href="/admin/logout">Sign out</a></p>

    <h2>Clubs / Tenants</h2>
    <table>
      <thead><tr><th>ID</th><th>Name</th><th>Slug</th><th>Created</th><th></th></tr></thead>
      <tbody>{rows}</tbody>
    </table>

    <h2>Add new club</h2>
    <form method="post" action="/admin/tenants">
      <input name="name" placeholder="Club name" required/>
      <input name="slug" placeholder="slug (e.g. bramley)" required/>
      <label style="font-size:13px;display:flex;align-items:center;gap:6px;margin-top:6px;">
        <input type="checkbox" name="ig_tenant" value="1" checked style="width:auto;margin:0;"/>
        Intelligent Golf club (members use IG credentials)
      </label>
      <button type="submit" style="margin-top:10px;">Create</button>
    </form>
    {"<p style='margin-top:8px;font-size:12px;color:#888;'>Club URL: <code>" + base + "/&lt;slug&gt;</code></p>" if base else ""}
    </body></html>
    """)


@app.get("/admin/logout")
async def admin_logout():
    response = RedirectResponse("/admin/login")
    response.delete_cookie("admin_token")
    return response


@app.get("/admin/visit/{slug}")
async def admin_visit_tenant(slug: str, admin_token: str | None = Cookie(default=None)):
    """Let an admin open any tenant's SPA without needing user credentials."""
    if not admin_token or not _validate_admin_session(admin_token):
        return RedirectResponse("/admin/login")
    tenant = await db.get_tenant_by_slug(slug)
    if not tenant:
        raise HTTPException(404, f"No club found for '{slug}'")
    token = _create_user_session(
        user_id=0,
        tenant_id=tenant["id"],
        tenant_slug=tenant["slug"],
        tenant_name=tenant["name"],
        ig_username=None,
        ig_pin=None,
    )
    response = RedirectResponse(f"/{slug}")
    response.set_cookie("session_token", token, max_age=3600, httponly=False, samesite="lax")
    return response


# Admin: create tenant (form POST from dashboard)
@app.post("/admin/tenants", response_class=HTMLResponse)
async def admin_create_tenant_form(
    request:     Request,
    admin_token: str | None = Cookie(default=None),
):
    if not admin_token or not _validate_admin_session(admin_token):
        return RedirectResponse("/admin/login")
    form = await request.form()
    name      = (form.get("name") or "").strip()
    slug      = (form.get("slug") or "").strip().lower().replace(" ", "-")
    ig_tenant = form.get("ig_tenant") == "1"
    if not name or not slug:
        return RedirectResponse("/admin")
    try:
        await db.create_tenant(name, slug, ig_tenant)
    except Exception:
        pass  # slug conflict — ignore for now
    return RedirectResponse("/admin", status_code=303)


# ---------------------------------------------------------------------------
# Admin JSON API  (X-Admin-Token header)
# ---------------------------------------------------------------------------

@app.get("/api/admin/tenants")
async def admin_list_tenants(_: str = Depends(get_admin_user)):
    return {"tenants": await db.get_all_tenants()}


class AdminCreateTenantRequest(BaseModel):
    name: str
    slug: str


@app.post("/api/admin/tenants")
async def admin_create_tenant(
    body: AdminCreateTenantRequest,
    _: str = Depends(get_admin_user),
):
    slug = body.slug.strip().lower().replace(" ", "-")
    if not slug or not body.name:
        raise HTTPException(400, "name and slug are required")
    try:
        tenant_id = await db.create_tenant(body.name, slug)
    except Exception as e:
        if "unique" in str(e).lower():
            raise HTTPException(409, f"Slug '{slug}' already taken")
        raise HTTPException(500, str(e))
    return {"ok": True, "id": tenant_id, "slug": slug}


@app.get("/api/admin/rollups")
async def admin_list_rollups(_: str = Depends(get_admin_user)):
    return {"rollups": await db.get_all_rollups_admin()}


# ---------------------------------------------------------------------------
# Rollups
# ---------------------------------------------------------------------------

@app.get("/api/rollups")
async def rollups(tenant_id: int = Depends(get_current_tenant)):
    try:
        data = await get_all_rollups(tenant_id)
    except Exception as e:
        raise HTTPException(500, f"Could not load rollups: {str(e)}")
    return {"rollups": data}


class AddRollupRequest(BaseModel):
    name: str
    ig_search_term: str


@app.post("/api/rollups/add")
async def add_rollup(body: AddRollupRequest, tenant_id: int = Depends(get_current_tenant)):
    try:
        rollup_id = await get_or_create_rollup(tenant_id, body.name, body.ig_search_term.upper())
    except Exception as e:
        raise HTTPException(500, f"Could not add rollup: {str(e)}")
    return {"ok": True, "id": rollup_id, "name": body.name,
            "ig_search_term": body.ig_search_term.upper()}


# ---------------------------------------------------------------------------
# Auth / status
# ---------------------------------------------------------------------------

@app.get("/auth/status")
async def auth_status(
    rollup_id: int = Query(1),
    tenant_id: int = Depends(get_current_tenant),
):
    await _assert_rollup_access(rollup_id, tenant_id)
    try:
        last_date = await get_last_round_date(rollup_id)
    except Exception:
        last_date = None
    return {"last_round_date": last_date}


# ---------------------------------------------------------------------------
# Load players
# ---------------------------------------------------------------------------

class LoadRequest(BaseModel):
    date: str
    rollup_id: int
    ig_search_term: str


@app.post("/api/load-players")
async def load_players(body: LoadRequest, session: dict = Depends(get_current_session)):
    tenant_id = session["tenant_id"]
    await _assert_rollup_access(body.rollup_id, tenant_id)

    ig_username = session.get("ig_username") or ""
    ig_pin      = session.get("ig_pin")      or ""
    if not ig_username or not ig_pin:
        raise HTTPException(400, "IG credentials not available in session — please sign in with IG credentials")

    try:
        scrape_result = await scrape_players(
            ig_username, ig_pin, body.date, body.ig_search_term,
        )
    except Exception as e:
        raise HTTPException(502, str(e))

    names     = scrape_result["names"]
    tee_times = scrape_result["tee_times"]
    tee_start = scrape_result.get("tee_start", "")
    indices   = scrape_result.get("indices", {})
    print(f"INDICES SCRAPED: {len(indices)} entries")

    if not names:
        raise HTTPException(404, "No players found for this date.")

    try:
        all_players = await get_all_players(body.rollup_id)
    except Exception as e:
        raise HTTPException(500, f"Could not read player list from database: {str(e)}")

    for p in all_players:
        name  = p["name"].strip()
        lower = name.lower()
        idx = indices.get(name) or next(
            (v for k, v in indices.items() if k.lower() == lower), None
        )
        if idx is not None:
            try:
                await update_player_whs_index(p["id"], idx)
                p["whs_index"] = idx
            except Exception:
                pass

    name_to_hc = {p["name"].strip().lower(): p["handicap"] for p in all_players}
    players, new_players = [], []

    for name in names:
        hc     = name_to_hc.get(name.strip().lower())
        p_data = next(
            (p for p in all_players if p["name"].strip().lower() == name.strip().lower()), None
        )
        if hc is None:
            new_players.append(name)
            players.append({
                "name": name, "handicap": None, "score": None, "team": None,
                "new_player": True, "whs_index": None,
                "whs_index_next_round": None, "winner_prohibited": False,
            })
        else:
            players.append({
                "name":                 name,
                "handicap":             hc,
                "score":                None,
                "team":                 None,
                "new_player":           False,
                "whs_index":            float(p_data["whs_index"]) if p_data and p_data.get("whs_index") else None,
                "whs_index_next_round": float(p_data["whs_index_next_round"]) if p_data and p_data.get("whs_index_next_round") else None,
                "winner_prohibited":    p_data.get("winner_prohibited", False) if p_data else False,
                "winner_ban_entries":   p_data.get("winner_ban_entries", 0) if p_data else 0,
            })

    return {
        "date": body.date, "players": players,
        "new_players": new_players, "tee_times": tee_times, "tee_start": tee_start,
    }


# ---------------------------------------------------------------------------
# Players
# ---------------------------------------------------------------------------

class NewPlayerRequest(BaseModel):
    name: str
    handicap: int
    rollup_id: int


@app.post("/api/new-player")
async def new_player(body: NewPlayerRequest, tenant_id: int = Depends(get_current_tenant)):
    await _assert_rollup_access(body.rollup_id, tenant_id)
    try:
        await add_new_player(body.rollup_id, body.name, body.handicap)
    except Exception as e:
        raise HTTPException(500, f"Could not add player to database: {str(e)}")
    return {"ok": True, "name": body.name, "handicap": body.handicap}


class LookupRequest(BaseModel):
    name: str
    rollup_id: int = 1


@app.post("/api/lookup-player")
async def lookup_player(body: LookupRequest, tenant_id: int = Depends(get_current_tenant)):
    await _assert_rollup_access(body.rollup_id, tenant_id)
    try:
        all_players = await get_all_players(body.rollup_id)
    except Exception as e:
        raise HTTPException(500, f"Could not read player list: {str(e)}")
    for p in all_players:
        if p["name"].strip().lower() == body.name.strip().lower():
            return {"found": True, "name": p["name"], "handicap": p["handicap"]}
    return {"found": False, "name": body.name}


@app.get("/api/players")
async def get_players(rollup_id: int = Query(1), tenant_id: int = Depends(get_current_tenant)):
    await _assert_rollup_access(rollup_id, tenant_id)
    try:
        players = await get_all_players(rollup_id)
    except Exception as e:
        raise HTTPException(500, f"Could not load players: {str(e)}")
    return {"players": [{"name": p["name"], "handicap": p["handicap"]} for p in players]}


@app.get("/api/players/detail")
async def get_players_detail(rollup_id: int = Query(1), tenant_id: int = Depends(get_current_tenant)):
    await _assert_rollup_access(rollup_id, tenant_id)
    try:
        players = await get_all_players_detail(rollup_id)
    except Exception as e:
        raise HTTPException(500, f"Could not load players: {str(e)}")
    return {"players": players}


class UpdateHandicapRequest(BaseModel):
    player_id: int
    handicap: int
    rollup_id: int


@app.post("/api/players/update-handicap")
async def update_handicap(body: UpdateHandicapRequest, tenant_id: int = Depends(get_current_tenant)):
    await _assert_rollup_access(body.rollup_id, tenant_id)
    if body.handicap < 0 or body.handicap > 54:
        raise HTTPException(400, "Handicap must be 0-54")
    try:
        await update_player_handicap(body.player_id, body.handicap)
    except Exception as e:
        raise HTTPException(500, f"Could not update handicap: {str(e)}")
    return {"ok": True}


class UpdateWhsIndexRequest(BaseModel):
    player_id: int
    whs_index: float
    rollup_id: int


@app.post("/api/players/update-whs-index")
async def update_whs_index(body: UpdateWhsIndexRequest, tenant_id: int = Depends(get_current_tenant)):
    await _assert_rollup_access(body.rollup_id, tenant_id)
    if body.whs_index < 0 or body.whs_index > 54:
        raise HTTPException(400, "WHS index must be 0-54")
    rounded = round(body.whs_index * 10) / 10
    try:
        await update_player_whs_index(body.player_id, rounded)
    except Exception as e:
        raise HTTPException(500, f"Could not update WHS index: {str(e)}")
    return {"ok": True}


class DeletePlayerRequest(BaseModel):
    player_id: int
    rollup_id: int


@app.post("/api/players/delete")
async def delete_player(body: DeletePlayerRequest, tenant_id: int = Depends(get_current_tenant)):
    await _assert_rollup_access(body.rollup_id, tenant_id)
    try:
        await remove_player(body.player_id, body.rollup_id)
    except Exception as e:
        raise HTTPException(500, f"Could not delete player: {str(e)}")
    return {"ok": True}


class SyncWhsRequest(BaseModel):
    rollup_id: int


@app.post("/api/scrape-whs-indices")
async def scrape_whs_indices_endpoint(body: SyncWhsRequest, session: dict = Depends(get_current_session)):
    tenant_id   = session["tenant_id"]
    ig_username = session.get("ig_username") or ""
    ig_pin      = session.get("ig_pin")      or ""
    if not ig_username or not ig_pin:
        raise HTTPException(400, "IG credentials not in session")
    await _assert_rollup_access(body.rollup_id, tenant_id)
    from backend.scraper import scrape_whs_indices
    try:
        result = await scrape_whs_indices(ig_username, ig_pin)
    except Exception as e:
        raise HTTPException(502, f"Could not scrape handicap list: {str(e)}")

    indices     = result["indices"]
    all_players = await get_all_players(body.rollup_id)
    updated, not_found = [], []

    for p in all_players:
        name  = p["name"].strip()
        lower = name.lower()
        idx = indices.get(name) or next(
            (v for k, v in indices.items() if k.lower() == lower), None
        )
        if idx is not None:
            await update_player_whs_index(p["id"], idx)
            updated.append({"name": name, "whs_index": idx})
        else:
            not_found.append(name)

    return {"ok": True, "updated": len(updated), "not_found": not_found, "details": updated}


# ---------------------------------------------------------------------------
# Courses and tees  (global — no tenant scoping needed)
# ---------------------------------------------------------------------------

@app.get("/api/courses")
async def courses():
    try:
        data = await get_all_courses()
    except Exception as e:
        raise HTTPException(500, f"Could not load courses: {str(e)}")
    return {"courses": data}


@app.get("/api/tees")
async def tees(course_id: int = Query(...)):
    try:
        data = await get_tees_for_course(course_id)
    except Exception as e:
        raise HTTPException(500, f"Could not load tees: {str(e)}")
    return {"tees": data}


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

class ScoreUpdate(BaseModel):
    date: str
    players: list[dict]
    team_mode: bool = False
    rollup_id: int = 1


@app.post("/api/autosave")
async def autosave(body: ScoreUpdate, tenant_id: int = Depends(get_current_tenant)):
    await _assert_rollup_access(body.rollup_id, tenant_id)
    try:
        settings = await get_rollup_settings(body.rollup_id)
    except Exception:
        settings = None

    whs_mode = (settings or {}).get("scoring_mode") == "whs"

    if whs_mode:
        prohibited = await get_prohibited_winners(body.rollup_id)
        whs_result = calculate_whs_handicaps(body.players, settings=settings, prohibited_names=prohibited)
        results = whs_result["players"]
        for r in results:
            r["adj_display"] = format_adjustment(r.get("adjustment"))
        team_scores = calculate_team_scores(results, settings=settings) if body.team_mode else []
        return {"players": results, "team_scores": team_scores, "whs_mode": True,
                "prohibited_winner": whs_result.get("prohibited_winner"),
                "error": whs_result.get("error")}
    else:
        results = calculate_new_handicaps(body.players, team_mode=body.team_mode, settings=settings)
        for r in results:
            r["adj_display"] = format_adjustment(r.get("adjustment"))
        team_scores = calculate_team_scores(results, settings=settings) if body.team_mode else []
        return {"players": results, "team_scores": team_scores, "whs_mode": False}


@app.post("/api/save-round")
async def save_round(body: ScoreUpdate, tenant_id: int = Depends(get_current_tenant)):
    await _assert_rollup_access(body.rollup_id, tenant_id)
    try:
        settings = await get_rollup_settings(body.rollup_id)
    except Exception:
        settings = None

    whs_mode  = (settings or {}).get("scoring_mode") == "whs"
    course_id = (settings or {}).get("course_id")
    tee_id    = (settings or {}).get("tee_id")
    winner_reduction = (settings or {}).get("winner_reduction_enabled", False)

    if whs_mode:
        prohibited = await get_prohibited_winners(body.rollup_id)
        whs_result = calculate_whs_handicaps(body.players, settings=settings, prohibited_names=prohibited)
        if whs_result.get("error"):
            raise HTTPException(400, whs_result["error"])
        results = whs_result["players"]
        try:
            await save_round_results(results, body.date, body.rollup_id, whs_mode=True,
                                     course_id=course_id, tee_id=tee_id)
        except Exception as e:
            raise HTTPException(500, f"Failed to save to database: {str(e)}")
    else:
        results = calculate_new_handicaps(body.players, team_mode=body.team_mode, settings=settings)
        try:
            await save_round_results(results, body.date, body.rollup_id, whs_mode=False,
                                     course_id=course_id, tee_id=tee_id)
        except Exception as e:
            raise HTTPException(500, f"Failed to save to database: {str(e)}")

    # Winner reduction: 25% HC cut for winner, ban countdown for others
    reduction_changes = []
    if winner_reduction:
        scored = [r for r in results if r.get("score") is not None]
        if scored:
            top_score  = max(r["score"] for r in scored)
            winners    = [r["name"] for r in scored if r["score"] == top_score]
            all_names  = [r["name"] for r in scored]
            reduction_changes = await db.apply_winner_reduction(
                body.rollup_id, winners, all_names,
                reduction_pct=int((settings or {}).get("winner_reduction_pct", 25)),
                ban_rounds=int((settings or {}).get("winner_ban_rounds", 3)),
            )

    for r in results:
        r["adj_display"] = format_adjustment(r.get("adjustment"))
    team_scores = calculate_team_scores(results, settings=settings) if body.team_mode else []
    return {"ok": True, "players": results, "date": body.date,
            "team_scores": team_scores, "whs_mode": whs_mode,
            "reduction_changes": reduction_changes}


# ---------------------------------------------------------------------------
# Results / history
# ---------------------------------------------------------------------------

@app.get("/api/last-round")
async def last_round(rollup_id: int = Query(1), tenant_id: int = Depends(get_current_tenant)):
    await _assert_rollup_access(rollup_id, tenant_id)
    try:
        results = await get_last_round_results(rollup_id)
        date    = await get_last_round_date(rollup_id)
    except Exception as e:
        raise HTTPException(500, f"Could not load last round: {str(e)}")
    return {"players": results, "date": date}


@app.get("/api/round-dates")
async def round_dates(rollup_id: int = Query(1), tenant_id: int = Depends(get_current_tenant)):
    await _assert_rollup_access(rollup_id, tenant_id)
    try:
        dates = await get_round_dates(rollup_id)
    except Exception as e:
        raise HTTPException(500, f"Could not load round dates: {str(e)}")
    return {"dates": dates}


@app.get("/api/round")
async def round_by_date(
    date: str = Query(...), rollup_id: int = Query(1),
    tenant_id: int = Depends(get_current_tenant),
):
    await _assert_rollup_access(rollup_id, tenant_id)
    try:
        results = await get_round_by_date(date, rollup_id)
    except Exception as e:
        raise HTTPException(500, f"Could not load round: {str(e)}")
    if not results:
        raise HTTPException(404, f"No results found for {date}")
    return {"players": results, "date": date}


@app.get("/api/player-history")
async def player_history(
    name: str = Query(...), rollup_id: int = Query(1),
    tenant_id: int = Depends(get_current_tenant),
):
    await _assert_rollup_access(rollup_id, tenant_id)
    try:
        history = await get_player_history(name, rollup_id)
    except Exception as e:
        raise HTTPException(500, f"Could not load player history: {str(e)}")
    if not history:
        raise HTTPException(404, f"No history found for {name}")
    return {
        "name": name,
        "rounds": [
            {"date": str(r["date"]), "score": r["score"], "new_handicap": r["new_handicap"]}
            for r in history
        ]
    }


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

@app.get("/api/settings")
async def get_settings(rollup_id: int = Query(...), tenant_id: int = Depends(get_current_tenant)):
    await _assert_rollup_access(rollup_id, tenant_id)
    try:
        settings = await get_rollup_settings(rollup_id)
    except Exception as e:
        raise HTTPException(500, f"Could not load settings: {str(e)}")
    return settings


class RollupSettingsRequest(BaseModel):
    rollup_id: int
    display_name: str
    ig_search_term: str
    run_days: list[str]
    tee_interval_minutes: int = 8
    scoring_mode: str = "stableford"
    adjustment_table: list[dict]
    winner_bonus_enabled: bool = True
    winner_gap_penalty1: int = 0
    winner_gap_penalty2: int = 0
    whs_pct_1st: float = 0.0
    whs_pct_2nd: float = 0.0
    whs_pct_3rd: float = 0.0
    whs_winner_prohibition:   bool = False
    winner_reduction_enabled: bool = False
    winner_reduction_pct:     int  = 25
    winner_ban_rounds:        int  = 3
    course_id: int | None = None
    tee_id: int | None = None
    entry_fee: float = 0.00
    prize_places: int = 3
    prize_pct_1st: int = 60
    prize_pct_2nd: int = 30
    prize_pct_3rd: int = 10
    prize_pct_4th: int = 0
    tie_handling: str = "tournament"
    preferred_team_size: int = 4
    team_scoring_method: str = "best2"


@app.post("/api/settings")
async def post_settings(body: RollupSettingsRequest, tenant_id: int = Depends(get_current_tenant)):
    await _assert_rollup_access(body.rollup_id, tenant_id)
    total_pct = body.prize_pct_1st + body.prize_pct_2nd + body.prize_pct_3rd + body.prize_pct_4th
    if total_pct != 100:
        raise HTTPException(400, f"Prize percentages must sum to 100 (currently {total_pct})")
    try:
        await save_rollup_settings(body.rollup_id, body.dict())
    except Exception as e:
        raise HTTPException(500, f"Could not save settings: {str(e)}")
    return {"ok": True}


# ---------------------------------------------------------------------------
# Credentials (per-tenant)
# ---------------------------------------------------------------------------

@app.get("/api/credentials")
async def get_creds(tenant_id: int = Depends(get_current_tenant)):
    try:
        creds = await get_tenant_credentials(tenant_id)
    except Exception as e:
        raise HTTPException(500, f"Could not load credentials: {str(e)}")
    return {"ig_username": creds["ig_username"], "ig_pin_set": bool(creds["ig_pin"])}


class CredentialsRequest(BaseModel):
    ig_username: str
    ig_pin: str = ""


@app.post("/api/credentials")
async def post_credentials(body: CredentialsRequest, tenant_id: int = Depends(get_current_tenant)):
    if not body.ig_username:
        raise HTTPException(400, "Member ID is required")
    try:
        if body.ig_pin:
            await save_tenant_credentials(tenant_id, body.ig_username, body.ig_pin)
        else:
            existing = await get_tenant_credentials(tenant_id)
            await save_tenant_credentials(tenant_id, body.ig_username, existing["ig_pin"])
    except Exception as e:
        raise HTTPException(500, f"Could not save credentials: {str(e)}")
    return {"ok": True}


# ---------------------------------------------------------------------------
# Course search & add
# ---------------------------------------------------------------------------

class SaveCourseRequest(BaseModel):
    club: str
    name: str
    tees: list[dict]


@app.get("/api/courses/search")
async def search_courses(q: str):
    if not q or len(q.strip()) < 3:
        raise HTTPException(400, "Query too short")
    try:
        return {"courses": await search_course_on_18birdies(q.strip())}
    except Exception as e:
        raise HTTPException(500, f"Search failed: {str(e)}")


@app.get("/api/courses/fetch")
async def fetch_course(url: str):
    if not url or not url.startswith("http"):
        raise HTTPException(400, "Valid URL required")
    try:
        return {"courses": await fetch_course_from_url(url.strip())}
    except Exception as e:
        raise HTTPException(500, f"Fetch failed: {str(e)}")


class ParsePasteRequest(BaseModel):
    text: str
    club_name: str = ""


@app.post("/api/courses/parse-paste")
async def parse_course_paste(body: ParsePasteRequest):
    try:
        return {"courses": parse_ncrdb_paste(body.text, body.club_name)}
    except Exception as e:
        raise HTTPException(500, f"Parse failed: {str(e)}")


@app.post("/api/courses/save")
async def save_course_endpoint(body: SaveCourseRequest):
    try:
        course_id = await save_course(body.name, body.club, body.tees)
        return {"course_id": course_id, "message": f"Saved {body.name} with {len(body.tees)} tees"}
    except Exception as e:
        raise HTTPException(500, f"Save failed: {str(e)}")


# ---------------------------------------------------------------------------
# Debug
# ---------------------------------------------------------------------------

@app.post("/api/debug-hcap")
async def debug_hcap(session: dict = Depends(get_current_session)):
    from bs4 import BeautifulSoup
    BASE_URL    = "https://www.bramleygolfclub.co.uk"
    LOGIN_URL   = f"{BASE_URL}/login.php"
    CONSENT_URL = f"{BASE_URL}/ttbconsent.php"
    HCAP_URL    = f"{BASE_URL}/hcaplist.php"
    HEADERS = {
        "User-Agent":      "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
    }
    async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True, timeout=30.0) as client:
        resp = await client.get(LOGIN_URL)
        soup = BeautifulSoup(resp.text, "html.parser")
        csrf = soup.find("input", {"name": "_csrf_token"})
        if not csrf:
            return {"error": "No CSRF token found on login page"}
        resp = await client.post(LOGIN_URL, data={
            "task": "login", "topmenu": "1",
            "memberid": session.get("ig_username",""), "pin": session.get("ig_pin",""),
            "cachemid": "1", "_csrf_token": csrf.get("value", ""), "Submit": "Login",
        })
        if str(resp.url).endswith("login.php"):
            return {"error": "Login failed"}
        if "ttbconsent" in str(resp.url):
            await client.get(f"{CONSENT_URL}?action=accept")
        resp = await client.get(HCAP_URL, params={"action": "masterhcap", "filter": "", "sort": "0"})
        soup = BeautifulSoup(resp.text, "html.parser")
        tables = soup.find_all("table")
        return {
            "final_url":    str(resp.url),
            "tables_found": [{"classes": t.get("class", []), "id": t.get("id", ""), "rows": len(t.find_all("tr"))} for t in tables],
            "page_text_snippet": soup.get_text()[:1000],
        }


@app.get("/api/debug-screenshot/{step}")
async def debug_screenshot(step: int):
    paths = {1: "/tmp/bramley_debug_1_login.png", 2: "/tmp/bramley_debug_2_after_login.png", 3: "/tmp/bramley_debug_3_hcaplist.png"}
    path = paths.get(step)
    if not path or not os.path.exists(path):
        return {"error": f"Screenshot {step} not found"}
    return FileResponse(path, media_type="image/png")


# ---------------------------------------------------------------------------
# Club app — /{slug} must be LAST so it doesn't shadow /admin, /api, etc.
# ---------------------------------------------------------------------------

@app.get("/{slug}", response_class=HTMLResponse)
async def club_app(slug: str, request: Request):
    """Serve the SPA for a specific club slug. Returns 404 if slug unknown."""
    tenant = await db.get_tenant_by_slug(slug)
    if not tenant:
        raise HTTPException(404, f"No club found for '{slug}'")

    return templates.TemplateResponse("index.html", {
        "request":     request,
        "tenant_slug": slug,
        "tenant_name": tenant["name"],
    })
