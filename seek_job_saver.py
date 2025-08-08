import os
import time
import uuid
import sqlite3
import re
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from dotenv import load_dotenv
from dateutil.relativedelta import relativedelta
from dateutil import parser as date_parser
from datetime import datetime, timedelta

# Load .env file
load_dotenv()
# -----------------------------
# Config (safe for GitHub)
# -----------------------------
# Put your local paths in environment variables (or a .env you don't commit).
CHROME_BINARY = os.getenv("CHROME_BINARY", "")  # optional
CHROME_DRIVER_PATH = os.getenv("CHROMEDRIVER", "chromedriver")
CHROME_USER_DATA_DIR = os.getenv("CHROME_USER_DATA_DIR")
CHROME_PROFILE_DIR = os.getenv("CHROME_PROFILE_DIR", "Default")
DB_PATH = os.getenv("DB_PATH", "seek_jobs_demo.db")
APPLIED_URL = "https://www.seek.co.nz/my-activity/applied-jobs"


# IMPORTANT: Close all Chrome windows before running, so the profile isn't locked.

# -----------------------------
# DB setup (UUID primary key; unique job_url to avoid duplicates)
# -----------------------------
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
cur.execute("""
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
)
""")
conn.commit()

# -----------------------------
# Selenium (reuse your Chrome profile = already logged in)
# -----------------------------
chrome_opts = Options()
chrome_opts.add_argument("--start-maximized")
chrome_opts.add_argument(f"--user-data-dir={CHROME_USER_DATA_DIR}")
chrome_opts.add_argument(f"--profile-directory={CHROME_PROFILE_DIR}")
# Optional: headless (new headless from Chrome 109+)
# chrome_opts.add_argument("--headless=new")
if CHROME_BINARY:
    chrome_opts.binary_location = CHROME_BINARY

service = Service(CHROME_DRIVER_PATH)
driver = webdriver.Chrome(service=service, options=chrome_opts)
wait = WebDriverWait(driver, 15)

def wait_clickable(locator):
    return wait.until(EC.element_to_be_clickable(locator))

def wait_present(locator):
    return wait.until(EC.presence_of_element_located(locator))

def scroll_into_view(element):
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)

# -----------------------------
# Helper: robust text getter
# -----------------------------
def safe_text(by, selector):
    try:
        return driver.find_element(by, selector).text.strip()
    except:
        return ""

# -----------------------------
# 1) Open applied list
# -----------------------------
driver.get(APPLIED_URL)
# If you are not logged in, this will redirect to sign-in. Since we reuse your profile,
# you should land directly on the applied jobs page.
wait_present((By.CSS_SELECTOR, "#tabs-saved-applied_2_panel > div:nth-child(2)"))

# Some lists lazy-load by scrolling; we'll collect titles after a gentle scroll.
last_height = 0
for _ in range(5):
    driver.execute_script("window.scrollBy(0, 1200);")
    time.sleep(0.8)
    new_height = driver.execute_script("return document.body.scrollHeight;")
    if new_height == last_height:
        break
    last_height = new_height

# Collect clickable job-title anchors (each opens the right-side drawer)
# -----------------------------
# Find clickable job blocks
# -----------------------------

title_selector = "//span[@role='button' and .//span[text()='Job Title ']]"
title_blocks = driver.find_elements(By.XPATH, title_selector)
print(f"Found {len(title_blocks)} job entries.")

for i in range(len(title_blocks)):
    # Re-grab job card list every time to avoid stale element
    driver.get(APPLIED_URL)
    wait.until(EC.presence_of_element_located((By.XPATH, title_selector)))
    time.sleep(1)

    title_blocks = driver.find_elements(By.XPATH, title_selector)
    if i >= len(title_blocks):
        break
    el = title_blocks[i]

    title_text = el.text.replace("Job Title", "").strip()
    print(f"[{i + 1}] {title_text}")

    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", el)
    except Exception as e:
        print(f"[Click failed] {title_text}: {e}")
        continue

    # Wait for drawer content
    try:
        wait.until(EC.presence_of_element_located(
            (By.XPATH, "//a[contains(@href, 'job/') and contains(text(),'View job')]")))
    except:
        print(f"[Warning] Drawer not fully loaded for: {title_text}")
        continue

    # Get applied date
    applied_date_text = ""
    try:
        applied_label_el = wait.until(EC.presence_of_element_located(
            (By.XPATH, "//*[contains(text(), 'Applied on SEEK')]")))
        container = applied_label_el.find_element(By.XPATH, "./ancestor::*[self::div or self::section][1]")
        block_text = container.text
        m = re.search(r"\b(\d{1,2}\s+\w{3,}\s+\d{4})\b", block_text)
        applied_date_text = m.group(1) if m else block_text.replace("Applied on SEEK", "").strip()
        print(f"[Applied on SEEK] {applied_date_text}")
    except:
        pass

    # Get View job link
    try:
        view_job_link = wait.until(EC.presence_of_element_located(
            (By.XPATH, "//a[contains(@href, 'job/') and contains(text(),'View job')]")))
        job_href = view_job_link.get_attribute("href")
        job_url = f"https://www.seek.co.nz{job_href}" if job_href.startswith("/") else job_href
        print(f"[View job] {job_url}")
    except Exception as e:
        print(f"[Skip] No 'View job' link for: {title_text} — {e}")
        continue

    # Go to job detail page
    driver.get(job_url)
    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "h1[data-automation='job-detail-title']")))
    except:
        print(f"[Warn] JD page didn't load properly: {job_url}")

    job_title = safe_text(By.CSS_SELECTOR, "h1[data-automation='job-detail-title']")
    company = safe_text(By.CSS_SELECTOR, "span[data-automation='advertiser-name']")
    address = safe_text(By.CSS_SELECTOR, "span[data-automation='job-detail-location']")
    field = safe_text(By.CSS_SELECTOR, "span[data-automation='job-detail-classifications']")
    job_type = safe_text(By.CSS_SELECTOR, "span[data-automation='job-detail-work-type']")
    jd_text = safe_text(By.CSS_SELECTOR, "div[data-automation='jobAdDetails']")

    # --- Get Posted date (e.g., 'Posted 18d ago') ---
    posted_date = ""
    try:
        posted_text = driver.find_element(By.XPATH, "//span[starts-with(text(), 'Posted ')]").text
        m = re.search(r"Posted\s+(\d+)([dwmy])\s+ago", posted_text)
        if m:
            num = int(m.group(1))
            unit = m.group(2)
            today = datetime.today()
            if unit == "d":
                post_date = today - timedelta(days=num)
            elif unit == "w":
                post_date = today - timedelta(weeks=num)
            elif unit == "m":
                post_date = today - relativedelta(months=num)
            elif unit == "y":
                post_date = today - relativedelta(years=num)
            posted_date = post_date.strftime("%Y-%m-%d")
    except:
        posted_date = ""

    # --- Get Applied date (e.g., 'You applied on 29 Jul 2025') ---
    applied_date_text = ""
    try:
        applied_text = driver.find_element(By.XPATH, "//span[starts-with(text(), 'You applied on')]").text
        m = re.search(r"You applied on (.+)", applied_text)
        if m:
            dt = date_parser.parse(m.group(1))
            applied_date_text = dt.strftime("%Y-%m-%d")
            print(f"[Applied on SEEK] {applied_date_text}")
    except:
        pass

    # Save to DB (insert new or update existing)
    now = datetime.utcnow().isoformat()
    job_id = str(uuid.uuid4())

    try:
        # Check if job_url already exists
        cur.execute("SELECT id FROM jobs WHERE job_url = ?", (job_url,))
        row = cur.fetchone()

        if row:
            existing_id = row[0]
            cur.execute("""
                        UPDATE jobs
                        SET job_title    = ?,
                            company      = ?,
                            address      = ?,
                            field        = ?,
                            job_type     = ?,
                            posted_date  = ?,
                            applied_date = ?,
                            jd           = ?,
                            created_at   = ?
                        WHERE id = ?
                        """, (job_title, company, address, field, job_type,
                              posted_date, applied_date_text, jd_text, now, existing_id))
            print(f"[Updated] {job_title} at {company}")
        else:
            cur.execute("""
                        INSERT INTO jobs
                        (id, job_url, job_title, company, address, field, job_type,
                         posted_date, applied_date, jd, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (job_id, job_url, job_title, company, address, field, job_type,
                              posted_date, applied_date_text, jd_text, now))
            print(f"[Inserted] {job_title} at {company}")

        conn.commit()

    except Exception as e:
        print(f"[DB Error] {e} for {job_url}")

# -----------------------------
# Done
# -----------------------------
driver.quit()
conn.close()
print("All done.")