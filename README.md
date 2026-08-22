# Work Order Allocation Tracker

Open-source stack: **FastAPI** (Python) + **SQLAlchemy** + **PostgreSQL**, single-file
HTML/JS frontend.

## This version needs nothing installed on your machine

Everything runs in the cloud — you only need a web browser and three free accounts
(GitHub, Render, Supabase). No admin rights required for any of it.

## Column ownership (matches your existing tracker)

| Range | Fields | Who edits |
|---|---|---|
| A–D | Received Date, Assigned Date, Employee, Employee Name | Team Lead |
| E–O | EDM, Status, Created, Image, Doc, Def Doc Type, Amount, Last Edited By, Description, Division, Deposit Date | Bulk-imported from your inventory export (read-only in-app) |
| P–Z | Posted $, Pending $, BAR Batch, Trans Count, Posting Status, Poster Comment, VENTRA Comment, Escalation Category, Issue Raised/Closed Date, Posted Date | Assigned colleague |
| AA | TAT | Auto-calculated on save (Posted Date − Received Date, minus issue pause window) |

## Step 1 — Put the code on GitHub (no git install needed)

1. Go to github.com, create a free account if you don't have one.
2. Click **New repository** → name it `workorder-tracker` → **Create repository**.
3. On the new repo page, click **uploading an existing file**.
4. Unzip the folder I gave you on your PC (Windows/Mac can unzip without installing
   anything — right-click → Extract All), then drag the whole contents (the `app`
   folder, `static` folder, `requirements.txt`, `README.md`) into the GitHub upload box.
5. Scroll down, click **Commit changes**.

## Step 2 — Create the free database (Supabase)

1. Go to supabase.com → sign up free, no card needed.
2. **New project** → give it any name and a database password (save this password).
3. Once it's created: **Settings** (gear icon) → **Database** → copy the
   **Connection string** under "Connection pooling" (URI format, starts with
   `postgresql://`). Replace `[YOUR-PASSWORD]` in it with the password you set.
   Keep this string handy for Step 3.

## Step 3 — Deploy the app (Render)

1. Go to render.com → sign up free, connect your GitHub account when prompted.
2. **New** → **Web Service** → pick your `workorder-tracker` repo.
3. Fill in:
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
   - **Instance Type:** Free
4. Under **Environment Variables**, add:
   - `DATABASE_URL` = the Supabase connection string from Step 2
   - `SECRET_KEY` = any long random text (mash the keyboard, 30+ characters)
   - `BOOTSTRAP_USERNAME` = the username you want for your own Team Lead login, e.g. `sekar`
   - `BOOTSTRAP_PASSWORD` = a password for that login
5. Click **Create Web Service**. Render will build and deploy automatically —
   takes a few minutes the first time. You'll get a permanent URL like
   `https://workorder-tracker.onrender.com`.
6. Open that URL and log in with the `BOOTSTRAP_USERNAME` / `BOOTSTRAP_PASSWORD`
   you set. That account is created automatically the moment the app starts —
   no script to run anywhere.

Free-tier Render services sleep after 15 minutes of inactivity and take ~30
seconds to wake on the first request of the day — worth knowing for an internal
team tool, but otherwise fine.

## Step 4 — Add your colleagues

Once logged in as Team Lead, open `https://your-app-url.onrender.com/docs` in the
browser — this is an interactive API page Render/FastAPI generates automatically.
1. Click the lock icon → log in with your Team Lead credentials.
2. Find **POST /auth/register** → click **Try it out** → fill in a username,
   full name, password, and role (`colleague`) for each teammate → **Execute**.

No installs, no terminal, all through the browser.

## Step 5 — Try the import

Log into the main app as Team Lead and use "Import Inventory CSV" with the
included `sample_inventory_import.csv` as a template for your real E–O export.
Orders auto-assign to colleagues who have no open file, one at a time.

## If you ever do get access to run things locally

The app also runs with zero cloud accounts using a local SQLite file. If that
access changes later, ask and I'll give you those instructions — for now, the
browser-only path above is the one to use.

## Extending it later

- Swap the plain HTML/JS frontend for React if you want richer editing (sortable
  grid, filters) — the API doesn't need to change.
- Add the priority-based allocation rules (Unmatched EOB routing, division
  grouping, lowest-transaction-count routing) inside `_auto_assign_open_slots()`
  in `app/main.py` if you want the same logic as the original design instead of
  plain round-robin.
- Add email notifications on new assignment via a free service like Resend, or
  keep Power Automate just for that one notification step.
