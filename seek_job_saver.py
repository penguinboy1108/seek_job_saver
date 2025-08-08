import sqlite3
import time
import uuid
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import browsercookie  # pip install browsercookie

# ----------------------------
# 1. Database setup
# ----------------------------
conn = sqlite3.connect("seek_jobs_demo.db")
cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    job_title TEXT,
    company TEXT,
    address TEXT,
    field TEXT,
    job_type TEXT,
    posted_date TEXT,
    applied_date TEXT,
    jd TEXT
)
""")
conn.commit()

# ----------------------------
# 2. Selenium setup
# ----------------------------
chrome_options = Options()
chrome_options.add_argument("--start-maximized")

# Path to chromedriver (replace with your actual path)
service = Service("C:\Program Files\Google\Chrome\Application\chromedriver-win64\chromedriver.exe")
driver = webdriver.Chrome(service=service, options=chrome_options)

# ----------------------------
# 3. Load Seek with Chrome cookies
# ----------------------------
driver.get("https://www.seek.co.nz")
time.sleep(2)

# Read cookies from local Chrome profile
cj = browsercookie.chrome(domain_name=".seek.co.nz")
for cookie in cj:
    cookie_dict = {
        "name": cookie.name,
        "value": cookie.value,
        "domain": cookie.domain
    }
    # Only set cookies for the same domain
    if ".seek.co.nz" in cookie.domain:
        try:
            driver.add_cookie(cookie_dict)
        except Exception:
            pass

# Refresh to apply cookies
driver.refresh()
time.sleep(3)

# ----------------------------
# 4. Go to Applied Jobs page
# ----------------------------
driver.get("https://www.seek.co.nz/my-activity/applied-jobs")
time.sleep(5)

# Wait until job list is visible
WebDriverWait(driver, 10).until(
    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a[data-automation='job-title']"))
)

# ----------------------------
# 5. Loop through job entries
# ----------------------------
job_cards = driver.find_elements(By.CSS_SELECTOR, "a[data-automation='job-title']")
job_links = [card.get_attribute("href") for card in job_cards if card.get_attribute("href")]

print(f"Found {len(job_links)} job links.")

for job_link in job_links:
    # Click job title link to open right panel
    driver.get(job_link)
    time.sleep(3)

    # Click "View job" button in right panel if available
    try:
        view_job_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.LINK_TEXT, "View job"))
        )
        view_job_button.click()
    except Exception:
        print("No 'View job' button, skipping.")
        continue

    # Switch to new job details page
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "h1[data-automation='job-detail-title']"))
    )

    # Extract job details
    try:
        job_title = driver.find_element(By.CSS_SELECTOR, "h1[data-automation='job-detail-title']").text
    except:
        job_title = ""

    try:
        company = driver.find_element(By.CSS_SELECTOR, "span[data-automation='advertiser-name']").text
    except:
        company = ""

    try:
        address = driver.find_element(By.CSS_SELECTOR, "span[data-automation='job-detail-location']").text
    except:
        address = ""

    try:
        field = driver.find_element(By.CSS_SELECTOR, "span[data-automation='job-detail-classifications']").text
    except:
        field = ""

    try:
        job_type = driver.find_element(By.CSS_SELECTOR, "span[data-automation='job-detail-work-type']").text
    except:
        job_type = ""

    try:
        posted_date = driver.find_element(By.CSS_SELECTOR, "span[data-automation='job-detail-date']").text
    except:
        posted_date = ""

    # Applied date is available from the right panel page, but here we just mark as unknown if not found
    applied_date = "Unknown"

    try:
        jd_text = driver.find_element(By.CSS_SELECTOR, "div[data-automation='jobAdDetails']").text
    except:
        jd_text = ""

    # Check for duplicates (title + company)
    cursor.execute("""
        SELECT 1 FROM jobs WHERE job_title=? AND company=?
    """, (job_title, company))
    exists = cursor.fetchone()

    if exists:
        print(f"Skipped duplicate: {job_title} - {company}")
        continue

    # Save to database
    job_id = str(uuid.uuid4())  # Generate UUID
    cursor.execute("""
        INSERT INTO jobs (id, job_title, company, address, field, job_type, posted_date, applied_date, jd)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (job_id, job_title, company, address, field, job_type, posted_date, applied_date, jd_text))
    conn.commit()

    print(f"Saved: {job_title} - {company}")

# ----------------------------
# 6. Cleanup
# ----------------------------
driver.quit()
conn.close()
