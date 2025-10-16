import os
import time
import uuid
import re
import psycopg2
from psycopg2.extras import Json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from dateutil import parser as date_parser
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from dotenv import load_dotenv
from webdriver_manager.chrome import ChromeDriverManager  # ✅ 新增
from dateutil import parser as date_parser

# -----------------------------
# Load env
# -----------------------------
load_dotenv(dotenv_path="D:\JD_saver\seek_job_saver\.env")

chrome_driver = os.getenv("CHROMEDRIVER") + "\chromedriver.exe"
print(chrome_driver)
CHROME_BINARY = os.getenv("CHROME_BINARY", "")
CHROME_USER_DATA_DIR = os.getenv("CHROME_USER_DATA_DIR")
CHROME_PROFILE_DIR = os.getenv("CHROME_PROFILE_DIR", "Default")
APPLIED_URL = os.getenv("APPLIED_URL")
MODE = os.getenv("MODE", "prod").lower()  # test or prod
print(f"[MODE] Running in {MODE.upper()} mode")

# -----------------------------
# PostgreSQL Setup
# -----------------------------
PG_CONN = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=os.getenv("POSTGRES_PORT", "5432"),
    dbname=os.getenv("POSTGRES_DB", "jobsdb"),
    user=os.getenv("POSTGRES_USER", "postgres"),
    password=os.getenv("POSTGRES_PASSWORD", "postgres")
)
cur = PG_CONN.cursor()
cur.execute("""
CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY,
    job_url TEXT UNIQUE,
    job_title TEXT,
    company TEXT,
    address TEXT,
    field TEXT,
    job_type TEXT,
    posted_date TEXT,
    applied_date TEXT,
    jd TEXT,
    source TEXT,
    status_summary TEXT,
    status_timeline JSONB,
    cv_file BYTEA,
    cl_file BYTEA,
    created_at TEXT
)
""")
PG_CONN.commit()
print("[DB] Connected to PostgreSQL successfully.")

# -----------------------------
# Chrome Setup
# -----------------------------
DOWNLOAD_DIR = os.path.join(os.getcwd(), "downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

chrome_opts = Options()
chrome_opts.add_argument("--window-size=1920,1080")
chrome_opts.add_argument("--disable-gpu")
chrome_opts.add_argument("--no-sandbox")
chrome_opts.add_argument("--disable-dev-shm-usage")
chrome_opts.add_argument("--disable-notifications")

chrome_opts.add_argument(f"--user-data-dir={CHROME_USER_DATA_DIR}")
chrome_opts.add_argument(f"--profile-directory={CHROME_PROFILE_DIR}")

DOWNLOAD_DIR = os.path.join(os.getcwd(), "downloads")  # 可自定义路径
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

prefs = {
    "download.default_directory": DOWNLOAD_DIR,          # 自动下载到该目录
    "download.prompt_for_download": False,               # 禁止弹窗
    "download.directory_upgrade": True,                  # 允许覆盖已存在目录
    "safebrowsing.enabled": True,                        # 允许下载安全文件
    "profile.default_content_setting_values.automatic_downloads": 1
}
chrome_opts.add_experimental_option("prefs", prefs)

# service = Service(chrome_driver)
# 用 webdriver_manager 自动安装匹配版本
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_opts)
wait = WebDriverWait(driver, 150)

# -----------------------------
# Helpers
# -----------------------------
def wait_present(locator):
    return wait.until(EC.presence_of_element_located(locator))

def safe_text(by, selector):
    try:
        return driver.find_element(by, selector).text.strip()
    except:
        return ""

def close_drawer_if_open():
    try:
        close_btn = driver.find_element(By.XPATH, "//button[@aria-label='Close' or @aria-label='Close dialog']")
        driver.execute_script("arguments[0].click();", close_btn)
        time.sleep(0.3)
    except Exception:
        try:
            driver.switch_to.active_element.send_keys(Keys.ESCAPE)
            time.sleep(0.2)
        except Exception:
            pass

def lazy_scroll():
    last_height = 0
    for _ in range(5):
        driver.execute_script("window.scrollBy(0, 1200);")
        time.sleep(0.8)
        new_height = driver.execute_script("return document.body.scrollHeight;")
        if new_height == last_height:
            break
        last_height = new_height

def find_title_blocks():
    return driver.find_elements(By.XPATH, "//span[@role='button' and .//span[text()='Job Title ']]")

def next_page():
    try:
        before = ""
        blocks = find_title_blocks()
        if blocks:
            before = blocks[0].text
        next_btn = driver.find_element(By.XPATH, "//span[.='Next']/parent::span")
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", next_btn)
        driver.execute_script("arguments[0].click();", next_btn)
    except Exception:
        return False

    try:
        WebDriverWait(driver, 8).until(
            lambda d: (find_title_blocks() and find_title_blocks()[0].text != before)
        )
        return True
    except Exception:
        return False

# -----------------------------
# Parse status & source
# -----------------------------
def clean_date_text(raw_text: str) -> str:
    """提取日期首行并格式化为 YYYY-MM-DD"""
    if not raw_text:
        return ""
    text = raw_text.strip().split("\n")[0].replace("\xa0", " ").strip()
    try:
        dt = date_parser.parse(text, fuzzy=True)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return text

def parse_status_timeline_and_source():
    timeline = []
    source_text = "Unknown"
    status_summary = "Unknown"

    try:
        drawer = driver.find_element(By.XPATH, "//div[starts-with(@id,'drawer-view-')]")

        # ---------- 来源判定 ----------
        try:
            source_el = drawer.find_element(
                By.XPATH, ".//div[1]/div[2]/div[2]/div/div/div/div[2]/div/span[1]"
            )
            source_text = source_el.text.strip()
        except:
            source_text = "Unknown"

        # ---------- 外部源（非 SEEK） ----------
        if "Visited employer" in source_text:
            try:
                date_el = drawer.find_element(
                    By.XPATH, ".//div[1]/div[2]/div[2]/div/div/div/div[2]/div/span[2]"
                )
                date_text = clean_date_text(date_el.text)
                timeline.append({
                    "status": "Visited employer’s application site",
                    "date": date_text,
                    "note": ""
                })
                status_summary = "Visited employer’s application site"
            except:
                pass

        # ---------- SEEK 源 ----------
        elif "Applied on SEEK" in drawer.text or "Viewed by employer" in drawer.text:
            source_text = "SEEK"
            try:
                blocks = drawer.find_elements(
                    By.XPATH, ".//div[1]/div[2]/div[2]/div/div/div"
                )
                for i, b in enumerate(blocks, 1):
                    try:
                        status = b.find_element(
                            By.XPATH, ".//div/div[2]/div/span[1]"
                        ).text.strip()
                        date = clean_date_text(
                            b.find_element(By.XPATH, ".//div/div[2]/div/span[2]").text.strip()
                        )
                        try:
                            note = b.find_element(
                                By.XPATH, ".//div/div[2]/div/span[3]"
                            ).text.strip()
                        except:
                            note = ""
                        timeline.append({
                            "status": status,
                            "date": date,
                            "note": note
                        })
                    except:
                        continue

                if timeline:
                    status_summary = timeline[-1]["status"]

            except Exception as e:
                print(f"[Warn] timeline parse failed: {e}")

    except Exception as e:
        print(f"[Warn] parse_status_timeline_and_source failed: {e}")

    # ---------- 清理与排序 ----------
    # 去重
    seen = set()
    cleaned = []
    for t in timeline:
        key = (t["status"], t["date"], t["note"])
        if key not in seen:
            seen.add(key)
            cleaned.append(t)

    # 按日期排序（空日期放最后）
    def sort_key(x):
        return (x["date"] == "", x["date"])
    cleaned.sort(key=sort_key)

    return source_text, status_summary, cleaned
# -----------------------------
# Download CV & CL
# -----------------------------

def download_cv_cl_if_any():
    cv_bytes, cl_bytes = None, None
    try:
        drawer = driver.find_element(By.XPATH, "//div[starts-with(@id,'drawer-view-')]")
    except Exception:
        return cv_bytes, cl_bytes

    def wait_new_file(before_files, timeout=10):
        """等待下载目录出现新文件"""
        end_time = time.time() + timeout
        while time.time() < end_time:
            after = set(os.listdir(DOWNLOAD_DIR))
            new = after - before_files
            if new:
                new_file = list(new)[0]
                file_path = os.path.join(DOWNLOAD_DIR, new_file)
                if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                    return file_path
            time.sleep(0.5)
        return None

    def handle_download(button_xpath, label):
        """通用下载逻辑"""
        try:
            btn = drawer.find_element(By.XPATH, button_xpath)
            before = set(os.listdir(DOWNLOAD_DIR))
            driver.execute_script("arguments[0].click();", btn)
            print(f"[Downloading {label}...]")
            file_path = wait_new_file(before)
            if file_path:
                with open(file_path, "rb") as f:
                    data = f.read()
                os.remove(file_path)
                print(f"[Downloaded {label}] size={len(data)} bytes")
                return data
            else:
                print(f"[Warn] {label} download not detected.")
                return None
        except Exception as e:
            print(f"[Error] {label} download failed:", e)
            return None

    # 依次下载 CV / CL
    cv_bytes = handle_download(".//div[1]/div[2]/div[3]/span[2]/span", "CV")
    cl_bytes = handle_download(".//div[1]/div[2]/div[3]/span[3]/span", "CL")

    return cv_bytes, cl_bytes

# -----------------------------
# Start scraping
# -----------------------------
driver.get(APPLIED_URL)
wait_present((By.CSS_SELECTOR, "#tabs-saved-applied_2_panel > div:nth-child(2)"))
time.sleep(1)

page_idx = 1
while True:
    lazy_scroll()
    title_blocks = find_title_blocks()
    print(f"[Page {page_idx}] Found {len(title_blocks)} job entries.")

    for i in range(len(title_blocks)):
        title_blocks = find_title_blocks()
        if i >= len(title_blocks):
            break

        el = title_blocks[i]
        title_text = el.text.replace("Job Title", "").strip()
        print(f"[{i + 1}] {title_text}")

        # 打开右侧 drawer
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            driver.execute_script("arguments[0].click();", el)
        except Exception as e:
            print(f"[Click failed] {title_text}: {e}")
            continue

        # 获取 View job 链接
        try:
            view_job_link = wait.until(EC.presence_of_element_located(
                (By.XPATH, "//a[contains(@href, 'job/') and contains(text(),'View job')]")))
            job_href = view_job_link.get_attribute("href")
            job_url = f"https://www.seek.co.nz{job_href}" if job_href.startswith("/") else job_href
        except Exception as e:
            print(f"[Skip] No 'View job' link for: {title_text} — {e}")
            close_drawer_if_open()
            continue

        # 解析状态和来源
        source_text, status_summary, status_timeline = parse_status_timeline_and_source()

        # 下载 CV/CL
        cv_bytes, cl_bytes = (None, None)
        if source_text.upper() == "SEEK":
            cv_bytes, cl_bytes = download_cv_cl_if_any()

        now = datetime.utcnow().isoformat()
        job_id = str(uuid.uuid4())

        try:
            cur.execute("SELECT id FROM jobs WHERE job_url = %s", (job_url,))
            row = cur.fetchone()
            if row:
                existing_id = row[0]
                cur.execute("""
                    UPDATE jobs SET source=%(source)s, status_summary=%(status_summary)s,
                    status_timeline=%(status_timeline)s, cv_file=%(cv_file)s, cl_file=%(cl_file)s,
                    created_at=%(created_at)s WHERE id=%(id)s
                """, {
                    "source": source_text,
                    "status_summary": status_summary,
                    "status_timeline": Json(status_timeline),
                    "cv_file": psycopg2.Binary(cv_bytes) if cv_bytes else None,
                    "cl_file": psycopg2.Binary(cl_bytes) if cl_bytes else None,
                    "created_at": now,
                    "id": existing_id
                })
                print(f"[Updated] {title_text}")
            else:
                cur.execute("""
                    INSERT INTO jobs (id, job_url, source, status_summary, status_timeline,
                    cv_file, cl_file, created_at)
                    VALUES (%(id)s, %(job_url)s, %(source)s, %(status_summary)s, %(status_timeline)s,
                    %(cv_file)s, %(cl_file)s, %(created_at)s)
                """, {
                    "id": job_id,
                    "job_url": job_url,
                    "source": source_text,
                    "status_summary": status_summary,
                    "status_timeline": Json(status_timeline),
                    "cv_file": psycopg2.Binary(cv_bytes) if cv_bytes else None,
                    "cl_file": psycopg2.Binary(cl_bytes) if cl_bytes else None,
                    "created_at": now
                })
                print(f"[Inserted] {title_text}")
            PG_CONN.commit()
        except Exception as e:
            PG_CONN.rollback()
            print(f"[DB Error] {e} for {job_url}")

        close_drawer_if_open()
        time.sleep(0.3)

    # 模式控制
    if MODE == "test":
        print("[TEST MODE] Only scraping first page, stopping here.")
        break

    if next_page():
        page_idx += 1
        wait_present((By.CSS_SELECTOR, "#tabs-saved-applied_2_panel > div:nth-child(2)"))
        time.sleep(0.8)
    else:
        print("[Done] No more pages.")
        break

driver.quit()
cur.close()
PG_CONN.close()
print("All done.")