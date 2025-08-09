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
# -----------------------------
# 1) Open applied list (page 1)
# -----------------------------
driver.get(APPLIED_URL)
wait_present((By.CSS_SELECTOR, "#tabs-saved-applied_2_panel > div:nth-child(2)"))
time.sleep(1)

def lazy_scroll():
    """Scroll a bit to trigger lazy load on the list."""
    last_height = 0
    for _ in range(5):
        driver.execute_script("window.scrollBy(0, 1200);")
        time.sleep(0.8)
        new_height = driver.execute_script("return document.body.scrollHeight;")
        if new_height == last_height:
            break
        last_height = new_height

def find_title_blocks():
    """Return clickable job blocks on the current page."""
    title_xpath = "//span[@role='button' and .//span[text()='Job Title ']]"
    return driver.find_elements(By.XPATH, title_xpath)

def close_drawer_if_open():
    """Best-effort close of the right-side drawer."""
    try:
        close_btn = driver.find_element(By.XPATH, "//button[@aria-label='Close' or @aria-label='Close dialog']")
        driver.execute_script("arguments[0].click();", close_btn)
        time.sleep(0.3)
    except Exception:
        # Fallback: press ESC
        try:
            from selenium.webdriver.common.keys import Keys
            driver.switch_to.active_element.send_keys(Keys.ESCAPE)
            time.sleep(0.2)
        except Exception:
            pass

def next_page():
    """
    Click 'Next' to go to the next page.
    Returns True if page changed, False if already last page / cannot click.
    """
    try:
        # Snapshot first card text to detect change after clicking next
        before = ""
        blocks = find_title_blocks()
        if blocks:
            before = blocks[0].text

        # 'Next' button is two nested spans; click parent span
        next_btn = driver.find_element(By.XPATH, "//span[.='Next']/parent::span")
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", next_btn)
        time.sleep(0.2)
        driver.execute_script("arguments[0].click();", next_btn)
    except Exception:
        return False

    # Wait for list to change (simple heuristic)
    try:
        WebDriverWait(driver, 8).until(
            lambda d: (find_title_blocks() and find_title_blocks()[0].text != before)
        )
        return True
    except Exception:
        return False

# -----------------------------
# Process all pages
# -----------------------------
page_idx = 1
while True:
    lazy_scroll()

    title_blocks = find_title_blocks()
    print(f"[Page {page_idx}] Found {len(title_blocks)} job entries.")

    # Iterate within the current page
    for i in range(len(title_blocks)):
        # Re-find on each iteration to avoid stale after drawer updates
        title_blocks = find_title_blocks()
        if i >= len(title_blocks):
            break

        el = title_blocks[i]
        title_text = el.text.replace("Job Title", "").strip()
        print(f"[{i + 1}] {title_text}")

        # Open drawer for this job
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            time.sleep(0.4)
            driver.execute_script("arguments[0].click();", el)
        except Exception as e:
            print(f"[Click failed] {title_text}: {e}")
            continue

        # Wait for 'View job'
        try:
            view_job_link = wait.until(EC.presence_of_element_located(
                (By.XPATH, "//a[contains(@href, 'job/') and contains(text(),'View job')]")))
            job_href = view_job_link.get_attribute("href")
            job_url = f"https://www.seek.co.nz{job_href}" if job_href.startswith("/") else job_href
        except Exception as e:
            print(f"[Skip] No 'View job' link for: {title_text} — {e}")
            close_drawer_if_open()
            continue

        print(f"[View job] {job_url}")

        # ---- Open JD in a NEW TAB to keep the current page state
        main_handle = driver.current_window_handle
        driver.execute_script("window.open(arguments[0], '_blank');", job_url)
        driver.switch_to.window(driver.window_handles[-1])

        # ---- Scrape JD page
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "h1[data-automation='job-detail-title']")))
        except:
            print(f"[Warn] JD page didn't load properly: {job_url}")

        job_title  = safe_text(By.CSS_SELECTOR, "h1[data-automation='job-detail-title']")
        company    = safe_text(By.CSS_SELECTOR, "span[data-automation='advertiser-name']")
        address    = safe_text(By.CSS_SELECTOR, "span[data-automation='job-detail-location']")
        field      = safe_text(By.CSS_SELECTOR, "span[data-automation='job-detail-classifications']")
        job_type   = safe_text(By.CSS_SELECTOR, "span[data-automation='job-detail-work-type']")
        jd_text    = safe_text(By.CSS_SELECTOR, "div[data-automation='jobAdDetails']")

        # Posted … ago -> absolute date
        posted_date = ""
        try:
            posted_text = driver.find_element(By.XPATH, "//span[starts-with(text(), 'Posted ')]").text
            m = re.search(r"Posted\s+(\d+)([dwmy])\s+ago", posted_text)
            if m:
                num = int(m.group(1)); unit = m.group(2)
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
        except Exception:
            posted_date = ""

        # You applied on …
        applied_date_text = ""
        try:
            applied_text = driver.find_element(By.XPATH, "//span[starts-with(text(), 'You applied on')]").text
            m = re.search(r"You applied on (.+)", applied_text)
            if m:
                dt = date_parser.parse(m.group(1))
                applied_date_text = dt.strftime("%Y-%m-%d")
        except Exception:
            pass

        # ---- Upsert to DB
        now = datetime.utcnow().isoformat()
        job_id = str(uuid.uuid4())

        try:
            cur.execute("SELECT id FROM jobs WHERE job_url = ?", (job_url,))
            row = cur.fetchone()
            if row:
                existing_id = row[0]
                cur.execute("""
                    UPDATE jobs
                    SET job_title=?, company=?, address=?, field=?, job_type=?,
                        posted_date=?, applied_date=?, jd=?, created_at=?
                    WHERE id=?
                """, (job_title, company, address, field, job_type,
                      posted_date, applied_date_text, jd_text, now, existing_id))
                print(f"[Updated] {job_title} — {company}")
            else:
                cur.execute("""
                    INSERT INTO jobs
                    (id, job_url, job_title, company, address, field, job_type,
                     posted_date, applied_date, jd, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (job_id, job_url, job_title, company, address, field, job_type,
                      posted_date, applied_date_text, jd_text, now))
                print(f"[Inserted] {job_title} — {company}")
            conn.commit()
        except Exception as e:
            print(f"[DB Error] {e} for {job_url}")

        # ---- Close JD tab and return to the list page
        driver.close()
        driver.switch_to.window(main_handle)
        close_drawer_if_open()
        time.sleep(0.2)

    # -----------------------------
    # Try go to next page
    # -----------------------------
    if next_page():
        page_idx += 1
        wait_present((By.CSS_SELECTOR, "#tabs-saved-applied_2_panel > div:nth-child(2)"))
        time.sleep(0.8)
        continue
    else:
        print("[Done] No more pages.")
        break

# -----------------------------
# Done
# -----------------------------
driver.quit()
conn.close()
print("All done.")