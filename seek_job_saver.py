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

title_blocks = driver.find_elements(By.XPATH, "//span[@role='button' and .//span[text()='Job Title ']]")

print(f"Found {len(title_blocks)} job entries.")

for i, el in enumerate(title_blocks):
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    title_text = el.text.replace("Job Title", "").strip()
    print(f"[{i + 1}] {title_text}")

    try:
        el.click()
    except Exception as e:
        print(f"Click failed on {title_text}: {e}")
        continue

# # We'll iterate by index (re-find each time, because the DOM changes when the drawer opens)
# for i in range(len(title_links)):
#     # Re-find current card
#     titles = driver.find_elements(By.CSS_SELECTOR, "a[data-automation='job-title']")
#     if i >= len(titles):
#         break
#     title_el = titles[i]
#     scroll_into_view(title_el)
#     job_card_title_text = title_el.text.strip()
#
#     # Click the job card to open the right drawer (application details)
#     wait_clickable((By.XPATH, f"(//a[@data-automation='job-title'])[{i+1}]")).click()
#
#     # Wait for the drawer to appear
#     # The drawer content varies; we'll look for either a known container or the "View job" link.
#     try:
#         wait_present((By.XPATH, "//div[contains(@class,'DialogContent') or contains(@class,'drawer')]"))
#     except:
#         pass
#
#     # -----------------------------
#     # Extract "Applied on SEEK" date from the drawer
#     # -----------------------------
#     applied_date_text = ""
#     try:
#         # Find the text node containing "Applied on SEEK" and read its nearby date.
#         # We'll look for any element with that text, then read its following text.
#         applied_label_el = wait_present((By.XPATH, "//*[contains(text(), 'Applied on SEEK')]"))
#         # The date text often sits in the same container; get the whole container text and parse date.
#         container = applied_label_el.find_element(By.XPATH, "./ancestor::*[self::div or self::section][1]")
#         block_text = container.text
#         # Look for a date like "29 Jul 2025" or similar
#         m = re.search(r"\b(\d{1,2}\s+\w{3,}\s+\d{4})\b", block_text)
#         if m:
#             applied_date_text = m.group(1)
#         else:
#             # If not found, use the whole line after the label
#             applied_date_text = block_text.replace("Applied on SEEK", "").strip()
#     except Exception:
#         applied_date_text = ""
#
#     # -----------------------------
#     # Get the JD page link from "View job" (safer to extract href than to rely on new tab)
#     # -----------------------------
#     job_url = ""
#     try:
#         view_job_link = wait_clickable((By.XPATH, "//a[normalize-space()='View job']"))
#         job_url = view_job_link.get_attribute("href") or ""
#     except Exception:
#         # Some cards might not have a view link (archived/removed). Skip them.
#         print(f"[Skip] No 'View job' link for: {job_card_title_text}")
#         # Close drawer if possible
#         try:
#             close_btn = driver.find_element(By.XPATH, "//button[@aria-label='Close' or @aria-label='Close dialog']")
#             close_btn.click()
#         except:
#             pass
#         continue
#
#     # If we've already saved this job_url before, skip (thanks to DB UNIQUE, but we also pre-check).
#     cur.execute("SELECT 1 FROM jobs WHERE job_url = ?", (job_url,))
#     if cur.fetchone():
#         print(f"[Duplicate] {job_url} (skip)")
#         # Close drawer
#         try:
#             close_btn = driver.find_element(By.XPATH, "//button[@aria-label='Close' or @aria-label='Close dialog']")
#             close_btn.click()
#         except:
#             pass
#         continue
#
#     # -----------------------------
#     # 2) Open the JD page and scrape details
#     # -----------------------------
#     driver.get(job_url)
#     try:
#         wait_present((By.CSS_SELECTOR, "h1[data-automation='job-detail-title']"))
#     except:
#         print(f"[Warn] JD page didn't fully load: {job_url}")
#
#     job_title = safe_text(By.CSS_SELECTOR, "h1[data-automation='job-detail-title']")
#     company = safe_text(By.CSS_SELECTOR, "span[data-automation='advertiser-name']")
#     address = safe_text(By.CSS_SELECTOR, "span[data-automation='job-detail-location']")
#     field = safe_text(By.CSS_SELECTOR, "span[data-automation='job-detail-classifications']")
#     job_type = safe_text(By.CSS_SELECTOR, "span[data-automation='job-detail-work-type']")
#     posted_date = safe_text(By.CSS_SELECTOR, "span[data-automation='job-detail-date']")
#     jd_text = safe_text(By.CSS_SELECTOR, "div[data-automation='jobAdDetails']")
#
#     # -----------------------------
#     # 3) Save (UUID id, UNIQUE job_url avoids duplicates across runs)
#     # -----------------------------
#     now_iso = datetime.utcnow().isoformat()
#     job_id = str(uuid.uuid4())
#
#     try:
#         cur.execute("""
#             INSERT OR IGNORE INTO jobs
#             (id, job_url, job_title, company, address, field, job_type, posted_date, applied_date, jd, created_at)
#             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#         """, (job_id, job_url, job_title, company, address, field, job_type, posted_date, applied_date_text, jd_text, now_iso))
#         conn.commit()
#         if cur.rowcount == 0:
#             print(f"[Duplicate] {job_url} (ignored by UNIQUE)")
#         else:
#             print(f"[Saved] {job_title} — {company}")
#     except Exception as e:
#         print(f"[DB Error] {e} for {job_url}")
#
# # -----------------------------
# # Cleanup
# # -----------------------------
# driver.quit()
# conn.close()
# print("Done.")
