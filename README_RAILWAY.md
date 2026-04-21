# Railway Deployment Guide — ID Card Backend

## Your backend folder structure (what to push to GitHub)

```
backend/
├── app.py                  ← your existing Flask file (no changes needed)
├── requirements.txt        ← from this folder
├── railway.toml            ← from this folder
├── nixpacks.toml           ← from this folder
├── Procfile                ← from this folder
├── .gitignore              ← from this folder
├── template_id_card.pdf    ← YOUR template
├── Anton-Regular.ttf       ← YOUR font
├── arialbd.ttf             ← YOUR font
└── student_photo.jpg       ← YOUR fallback photo
```

---

## Step 1 — Push backend to GitHub

Only the `backend/` folder goes to GitHub (not frontend).

```bash
cd your-project/backend
git init
git add .
git commit -m "initial backend"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/idcard-backend.git
git push -u origin main
```

---

## Step 2 — Create Railway project

1. Go to https://railway.app and log in
2. Click **"New Project"**
3. Choose **"Deploy from GitHub repo"**
4. Select your `idcard-backend` repo
5. Railway auto-detects Python and starts building

---

## Step 3 — Set environment variables in Railway

Go to your project → **Variables** tab → add these one by one:

| Variable | Value |
|---|---|
| `PREFETCH_WORKERS` | `8` |
| `MAX_CACHED_PHOTOS` | `200` |
| `SAVE_BATCH_PAGES` | `10` |
| `PHOTO_PX` | `300` |
| `PHOTO_JPEG_QUALITY` | `80` |
| `PHOTO_TIMEOUT` | leave blank (uses default `(4,10)`) |
| `MAX_STUDENTS_PER_REQUEST` | `1000` |
| `MAX_UPLOAD_MB` | `12` |
| `STORAGE_BACKEND` | `local` |
| `FLASK_DEBUG` | `0` |

Railway automatically sets `PORT` — do not add it manually.

---

## Step 4 — Get your Railway URL

1. Go to **Settings** tab → **Networking** → click **"Generate Domain"**
2. You get a URL like: `https://idcard-backend-production.up.railway.app`
3. Copy this URL

---

## Step 5 — Update your Vercel frontend

In your React app, find where the API base URL is set (usually `App.js` or a config file).

Change it from your old Render URL to your new Railway URL:

```js
// Before (Render)
const API_BASE = "https://your-app.onrender.com";

// After (Railway)
const API_BASE = "https://idcard-backend-production.up.railway.app";
```

Then redeploy your frontend on Vercel:
```bash
cd your-project/frontend
git add .
git commit -m "update API url to railway"
git push
```
Vercel auto-deploys on push.

---

## Step 6 — Verify it works

Open your Railway URL in browser:
```
https://idcard-backend-production.up.railway.app/health
```

You should see:
```json
{"status": "ok", "message": "ID Card Generator API is healthy"}
```

Also check:
```
https://idcard-backend-production.up.railway.app/api/schools
```

---

## Troubleshooting

**Build fails with "pymupdf not found"**
→ Make sure `requirements.txt` is in the root of the repo Railway is pointing to (not inside a subfolder). If your repo has `backend/requirements.txt`, set the **Root Directory** in Railway settings to `backend`.

**"Template PDF not found"** in logs
→ Your `template_id_card.pdf` was not committed to git. Check `.gitignore` isn't blocking `.pdf` files. Run:
```bash
git add template_id_card.pdf --force
git commit -m "add template"
git push
```

**CORS errors in browser**
→ Your `app.py` already has `CORS(app, origins=["*"])` so this should not happen. If it does, check Railway logs for a crash on startup.

**App sleeping / slow cold start**
→ Railway free tier does NOT sleep (unlike Render free tier). Your app stays warm. First request after a deploy takes ~5s to boot — normal.

**PORT binding error**
→ Never hardcode a port. The `$PORT` in the start command handles this automatically.

---

## Railway vs Render — key differences

| | Render Free | Railway Starter ($5/mo) |
|---|---|---|
| Sleep after inactivity | Yes (spins down) | No |
| RAM | 512 MB | 512 MB |
| CPU | 0.1 shared | 0.5 vCPU |
| Build time | ~3 min | ~2 min |
| Cold start | ~30s after sleep | ~4s after deploy only |
| Persistent disk | No | No (same as Render) |

Your PDF files are generated per-request and deleted after sending — no persistent disk needed.
