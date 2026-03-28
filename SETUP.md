# MOTH's Rollup — Setup Guide

## Project Structure

```
moths_rollup/
├── main.py                    # FastAPI app + all routes
├── requirements.txt           # Python dependencies
├── render.yaml                # Render deployment config
├── .env.example               # Environment variable template
├── backend/
│   ├── handicap.py            # Handicap calculation logic
│   ├── scraper.py             # Intelligent Golf scraper (Playwright)
│   └── sheets.py              # Google Sheets API helper
└── frontend/
    ├── templates/
    │   └── index.html         # Main PWA (all 3 screens)
    └── static/
        ├── manifest.json      # PWA manifest
        ├── icon-192.png       # App icon (add your own)
        └── icon-512.png       # App icon (add your own)
```

---

## Step 1 — Google OAuth Setup

1. Go to https://console.cloud.google.com
2. Create a new project (e.g. "Moths Rollup")
3. Enable the **Google Sheets API**
4. Go to **APIs & Services → Credentials → Create Credentials → OAuth 2.0 Client ID**
5. Application type: **Web application**
6. Add authorised redirect URIs:
   - `https://your-app.onrender.com/auth/callback` (production)
   - `http://localhost:8000/auth/callback` (local testing)
7. Copy the **Client ID** and **Client Secret**

---

## Step 2 — Google Sheet Setup

1. Open your existing MOTH's Rollup spreadsheet
2. Copy the Sheet ID from the URL:
   `https://docs.google.com/spreadsheets/d/THIS_PART_IS_THE_ID/edit`
3. Share the sheet with yourself (the Google account you'll sign in with)

---

## Step 3 — Deploy to Render

1. Push this project to a GitHub repository
2. Go to https://render.com → New → Web Service
3. Connect your GitHub repo
4. Render will detect `render.yaml` automatically
5. Add environment variables in the Render dashboard:
   - `GOOGLE_CLIENT_ID`
   - `GOOGLE_CLIENT_SECRET`
   - `GOOGLE_REDIRECT_URI` = `https://your-app.onrender.com/auth/callback`
   - `SHEET_ID`
   - `APP_SECRET_KEY` (Render generates this automatically)

---

## Step 4 — Install on Phone

### iPhone (iOS)
1. Open Safari and go to your Render URL
2. Tap the **Share** button
3. Tap **Add to Home Screen**
4. Tap **Add**

### Android
1. Open Chrome and go to your Render URL
2. Tap the three-dot menu
3. Tap **Add to Home Screen**
4. Tap **Add**

---

## Step 5 — First Use

1. Open the app from your home screen
2. Tap **Sign in with Google** and connect your account
3. Enter your Intelligent Golf username and PIN
4. Select the round date
5. Tap **Load Players**

---

## Local Development

```bash
cd moths_rollup
pip install -r requirements.txt
playwright install chromium
cp .env.example .env
# Fill in .env with your values
uvicorn main:app --reload --port 8000
```

Then open http://localhost:8000

---

## Icons

You need to add two icon files:
- `frontend/static/icon-192.png` (192×192px)
- `frontend/static/icon-512.png` (512×512px)

Use the Bramley Golf Club logo resized to these dimensions.

---

## Notes

- The scraper uses Playwright running headless Chromium. First deployment
  on Render may take a few minutes while Chromium installs.
- Sessions are stored in memory — users will need to re-enter credentials
  if the server restarts. A future upgrade could use Redis for persistence.
- The IG PIN is encrypted at rest using the APP_SECRET_KEY.
