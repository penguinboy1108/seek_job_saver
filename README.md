# 🔍 SEEK Job Application Scraper

This is a Python-based web scraping tool using Selenium to extract job application data (including title, company, address, posted date, applied date, job description, etc.) from [seek.co.nz](https://www.seek.co.nz/).

It supports:
- Clicking each previously applied job card
- Extracting key information from the job detail page ("View job")
- Saving new entries or updating existing ones into a local SQLite database (`seek_jobs_demo.db`)

---

## 🚀 Setup Instructions

### 1. Clone this repository
```bash
git clone [https://github.com/your-username/seek-job-scraper](https://github.com/penguinboy1108/seek_job_saver).git
```

---

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

Required packages include:

- `selenium`
- `python-dotenv`
- `dateparser` or `python-dateutil`
- `sqlite3` (built-in)

---

### 3. Set up `.env` file

Create a file named `.env` in the root directory with the following format:

```ini
SEEK_URL=https://www.seek.co.nz/my-activity/applications
DB_PATH=seek_jobs_demo.db
CHROME_PROFILE_PATH=C:\Users\{your username}\AppData\Local\Google\Chrome\User Data
CHROME_PROFILE_DIR=Default
CHROMEDRIVER=.\chromedriver.exe
```


If using cookies manually, these aren't needed.

---

### 4. Prepare cookies (⚠️ Important)

This project uses **your SEEK login session** via cookie injection.

#### Option A: Manual Cookie Injection (Recommended)

1. **Login** to your [Seek](https://www.seek.co.nz) account **using Chrome.**
2. After successful login, **close all Chrome browser windows completely.**
3. Then run the script. The script will launch Chrome in **user-data mode** and access your session cookies directly.

> ⚠️ Ensure Chrome is fully closed before running the script, or you will get a profile lock error.

---

## 🧠 How It Works

- Opens the `My Applications` page on Seek
- Finds all job entries you applied for
- Opens each drawer (right-side preview)
- Gets "View job" link and navigates to full job detail page
- Extracts:
  - Job title
  - Company
  - Address
  - Field
  - Job type
  - Posted date (calculated from "Posted 18d ago")
  - Applied date (e.g., "You applied on 29 Jul 2025")
  - Full job description
- Saves into `seek_jobs_demo.db` (SQLite3), avoids duplicates via `job_url` UNIQUE key
- If a duplicate is found, it updates the existing record instead

---

## 🗂 Output: `jobs.db` structure

Table schema:

```sql
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    job_url TEXT UNIQUE,
    job_title TEXT,
    company TEXT,
    address TEXT,
    field TEXT,
    job_type TEXT,
    posted_date TEXT,
    applied_date TEXT,
    jd TEXT,
    created_at TEXT
);
```

---

## ❗Troubleshooting

- ❌ **`stale element reference`**: Re-locate the element after each drawer interaction.
- ❌ **`element click intercepted`**: Use JavaScript `click()` instead of Selenium’s native click.
- ❌ **`user data directory is already in use`**: Fully close all Chrome windows before running the script.
- ❌ **No "View job" button?**: Some jobs may be expired/archived. They’ll be skipped automatically.

---

## 🛡 License

MIT License © 2025 Weizheng Jiang
