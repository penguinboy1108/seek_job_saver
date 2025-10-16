# -*- coding: utf-8 -*-
"""
SEEK 抓取（无 selenium-wire，CDP 读取 GraphQL）
流程：
1) 进入 https://www.seek.co.nz/my-activity/applied-jobs ，用 CDP 收集 appliedJobs 全量（含顺序、isExternal、isActive、events）
2) 计算 page = idx // 20 + 1，构造抽屉页 URL：https://www.seek.co.nz/my-activity/applied-jobs/{job_id}?page={page}
   - SEEK 源：打开该 URL，CDP 取 ApplicantCount；Selenium 点击按钮下载 CV/CL
   - 非 SEEK 源：跳过抽屉页
3) 详情页统一 HTTPS（带浏览器 Cookie）抓取：field / job_type / JD(innerText) / 静态 HTML(outerHTML)；失败再回退 Selenium
4) 入库（按 job_id 与 job_url 中 id 匹配）：
   - 只追加 timeline（去重，按日期排序），status_summary 取最后一条
   - competitor_count 取 max(已有, 新值)
   - 其它字段仅在库里为空时补齐
"""
import os, re, time, uuid, json, random, requests, psycopg2
from bs4 import BeautifulSoup
from datetime import datetime
from dateutil import parser as date_parser
from psycopg2.extras import Json
from dotenv import load_dotenv

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, InvalidSessionIdException, WebDriverException
)
from webdriver_manager.chrome import ChromeDriverManager


# =========================
# ENV & DB
# =========================
load_dotenv(dotenv_path=r"D:\JD_saver\seek_job_saver\.env")
CHROME_USER_DATA_DIR = os.getenv("CHROME_USER_DATA_DIR")
CHROME_PROFILE_DIR   = os.getenv("CHROME_PROFILE_DIR", "Default")
APPLIED_URL          = "https://www.seek.co.nz/my-activity/applied-jobs"
MODE                 = os.getenv("MODE", "prod").lower()  # test/prod

PG_CONN = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=os.getenv("POSTGRES_PORT", "5432"),
    dbname=os.getenv("POSTGRES_DB", "jobsdb"),
    user=os.getenv("POSTGRES_USER", "postgres"),
    password=os.getenv("POSTGRES_PASSWORD", "postgres"),
)
cur = PG_CONN.cursor()
cur.execute("""
CREATE TABLE IF NOT EXISTS jobsnew (
    id UUID PRIMARY KEY,
    job_url TEXT UNIQUE,
    job_title TEXT,
    company TEXT,
    address TEXT,
    field TEXT,
    job_type TEXT,
    posted_date TEXT,
    salary TEXT,
    competitor_count TEXT,
    jd TEXT,
    html_content TEXT,
    source TEXT,
    status_summary TEXT,
    status_timeline JSONB,
    cv_file BYTEA,
    cl_file BYTEA,
    created_at TEXT
)
""")
PG_CONN.commit()


# =========================
# Chrome（原生 Selenium + Performance Log）
# =========================
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
# 降低自动化痕迹
chrome_opts.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_opts.add_experimental_option("useAutomationExtension", False)
chrome_opts.add_experimental_option("prefs", {
   "download.default_directory": DOWNLOAD_DIR,
   "download.prompt_for_download": False,
   "download.directory_upgrade": True,
   "safebrowsing.enabled": True,
   "profile.default_content_setting_values.automatic_downloads": 1,
})
# 开启 Performance 日志
chrome_opts.set_capability("goog:loggingPrefs", {"performance": "ALL"})

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),
                          options=chrome_opts)
wait = WebDriverWait(driver, 45)
driver.execute_cdp_cmd("Network.enable", {})


# =========================
# Helpers
# =========================
def wait_present(locator): return wait.until(EC.presence_of_element_located(locator))

def close_drawer_if_open():
    try:
        btn = driver.find_element(By.XPATH, "//button[@aria-label='Close' or @aria-label='Close dialog']")
        driver.execute_script("arguments[0].click();", btn)
        time.sleep(0.2)
    except (InvalidSessionIdException, WebDriverException):
        return
    except Exception:
        try:
            driver.switch_to.active_element.send_keys(Keys.ESCAPE)
            time.sleep(0.2)
        except Exception:
            pass

def clean_date_text(text):
    if not text: return ""
    t = str(text).strip().replace("\xa0", " ")
    try: return date_parser.parse(t, fuzzy=True).strftime("%Y-%m-%d")
    except Exception: return t

def uniq_sorted_timeline(events):
    seen=set(); rows=[]
    for e in (events or []):
        st=(e or {}).get("status","")
        ts=((e or {}).get("timestamp") or {})
        dt=ts.get("shortAbsoluteLabel") or ts.get("dateTimeUtc") or ""
        key=(st,dt)
        if key in seen: continue
        seen.add(key)
        rows.append({"status": st, "date": clean_date_text(dt), "note": ""})
    rows.sort(key=lambda x: (x["date"]=="", x["date"]))
    return rows

def build_job_url_from_jobid(job_id: str, is_active: bool=True) -> str:
    if not job_id: return None
    return (f"https://www.seek.co.nz/job/{job_id}?ref=applied"
            if is_active else f"https://www.seek.co.nz/expiredjob/{job_id}?ref=applied")

def build_drawer_url(job_id: str, page_idx: int) -> str:
    return f"https://www.seek.co.nz/my-activity/applied-jobs/{job_id}?page={page_idx}"

def wait_new_file(before_files, timeout=20):
    end = time.time()+timeout
    while time.time() < end:
        after=set(os.listdir(DOWNLOAD_DIR)); new=after-before_files
        if new:
            fp=os.path.join(DOWNLOAD_DIR, list(new)[0])
            if os.path.exists(fp) and os.path.getsize(fp)>0: return fp
        time.sleep(0.4)
    return None

def download_cv_cl_via_buttons():
    """严格通过抽屉按钮下载 CV/CL（要求当前页就是抽屉已展开的 my-activity/{job_id}?page=X）。"""
    cv_bytes,cl_bytes=None,None
    try:
        drawer = driver.find_element(By.XPATH, "//div[starts-with(@id,'drawer-view-')]")
    except Exception:
        return cv_bytes, cl_bytes

    def click_and_read(xp,label):
        try:
            btn=drawer.find_element(By.XPATH,xp)
            before=set(os.listdir(DOWNLOAD_DIR))
            driver.execute_script("arguments[0].click();", btn)
            fp=wait_new_file(before)
            if fp:
                with open(fp,"rb") as f: b=f.read()
                os.remove(fp); print(f"  [Downloaded {label}] {len(b)} bytes")
                return b
        except Exception as e:
            print(f"  [Warn] {label} button failed:", e)
        return None

    cv_bytes = click_and_read(".//div[1]/div[2]/div[3]/span[2]/span","CV")
    cl_bytes = click_and_read(".//div[1]/div[2]/div[3]/span[3]/span","CL")
    return cv_bytes, cl_bytes

def is_session_alive() -> bool:
    try:
        driver.execute_script("return 1")
        _ = driver.window_handles
        return True
    except Exception:
        return False


# =========================
# Performance Log / GraphQL 抓取
# =========================
def _iter_graphql_responses():
    """从 performance 日志里筛选所有 graphql 响应体，返回解析后的 JSON 列表。"""
    out = []
    logs = driver.get_log("performance")  # 读取即清空
    for entry in logs:
        try:
            msg = json.loads(entry["message"])["message"]
        except Exception:
            continue
        if msg.get("method") != "Network.responseReceived":
            continue
        params = msg.get("params", {})
        resp = params.get("response", {})
        url = (resp.get("url") or "").lower()
        typ = params.get("type")
        if "graphql" not in url or typ not in ("XHR", "Fetch"):
            continue
        request_id = params.get("requestId")
        if not request_id:
            continue
        try:
            body = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": request_id})
            text = body.get("body") or ""
            if not text or text[0] not in "{[":  # 不是 JSON
                continue
            data = json.loads(text)
            out.append(data)
        except Exception:
            continue
    return out


# =========================
# HTTPS 详情抓取（先拿 cf_clearance，再请求）
# =========================
VERIFICATION_HINTS = ("verify you are human", "hcaptcha", "robot check", "unusual traffic", "cloudflare")

def ensure_cf_clearance(max_wait=30):
    driver.get(APPLIED_URL)
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            cookies = driver.execute_cdp_cmd("Network.getAllCookies", {})["cookies"]
            have = any((c.get("name") == "cf_clearance" and "seek.co.nz" in (c.get("domain") or "")) for c in cookies)
            if have: return True
        except Exception:
            pass
        time.sleep(1.0)
    return False

def cookies_header_from_cdp(domain_filter="seek.co.nz"):
    try:
        cookies = driver.execute_cdp_cmd("Network.getAllCookies", {})["cookies"]
    except Exception:
        cookies = []
    pairs = []
    for c in cookies:
        d = c.get("domain") or ""
        if domain_filter in d:
            pairs.append(f"{c['name']}={c['value']}")
    return "; ".join(pairs)

def _requests_headers_from_driver():
    try:
        ua = driver.execute_script("return navigator.userAgent;")
    except Exception:
        ua = "Mozilla/5.0"
    return {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "DNT": "1",
        "Referer": APPLIED_URL,
    }

def fetch_detail_via_https(job_id: str, is_active: bool = True, max_retry: int = 2):
    if not job_id: return (None, None, None, None)

    def _url(active: bool):
        return f"https://www.seek.co.nz/job/{job_id}?ref=applied" if active \
               else f"https://www.seek.co.nz/expiredjob/{job_id}?ref=applied"

    try: ensure_cf_clearance()
    except Exception: pass

    headers = _requests_headers_from_driver()
    cookie_header = cookies_header_from_cdp()
    if cookie_header: headers["Cookie"] = cookie_header

    for _ in range(max_retry):
        time.sleep(random.uniform(0.25, 0.8))
        for active_flag in ([is_active, False] if is_active else [False]):
            url = _url(active_flag)
            try:
                resp = requests.get(url, headers=headers, timeout=20, allow_redirects=True)
            except Exception:
                continue
            if resp.status_code != 200:
                continue

            html = resp.text or ""
            lower = html.lower()
            if any(k in lower for k in VERIFICATION_HINTS):
                return (None, None, None, None)

            soup = BeautifulSoup(html, "lxml")
            jd_node = soup.select_one("div[data-automation='jobAdDetails']")
            jd_text = jd_node.get_text("\n", strip=True) if jd_node else None
            html_fragment = str(jd_node) if jd_node else None
            field_node = soup.select_one("[data-automation='job-detail-classifications'] a")
            job_type_node = soup.select_one("[data-automation='job-detail-work-type'] a")
            field_text = field_node.get_text(strip=True) if field_node else None
            job_type_text = job_type_node.get_text(strip=True) if job_type_node else None

            if any([jd_text, html_fragment, field_text, job_type_text]):
                return (field_text, job_type_text, jd_text, html_fragment)

    return (None, None, None, None)


# =========================
# 详情页 Selenium 回退（极少用）
# =========================
def ensure_job_details_node():
    paths = [
        "//div[@data-automation='jobAdDetails']",
        "//*[@id='app']//div[contains(@data-automation,'jobAdDetails')]",
    ]
    for _ in range(8):
        for xp in paths:
            try:
                node = driver.find_element(By.XPATH, xp)
                return node
            except Exception:
                time.sleep(0.5)
        driver.execute_script("window.scrollBy(0,800);")
    return None

def parse_detail_page_via_selenium(job_url):
    """安全的 Selenium 详情页兜底，不关闭主窗口。"""
    if not is_session_alive():
        return (None, None, None, None)

    field_text, job_type_text, jd_text, html_fragment = None, None, None, None

    try:
        orig_handle = driver.current_window_handle
    except Exception:
        return (None, None, None, None)

    before = set(driver.window_handles)
    driver.execute_script("window.open(arguments[0], '_blank');", job_url)

    new_handle = None
    try:
        WebDriverWait(driver, 10).until(lambda d: len(d.window_handles) > len(before))
        for h in driver.window_handles:
            if h not in before:
                new_handle = h
                break
        if not new_handle:
            driver.switch_to.window(orig_handle)
            return (None, None, None, None)
        driver.switch_to.window(new_handle)
    except Exception:
        try: driver.switch_to.window(orig_handle)
        except Exception: pass
        return (None, None, None, None)

    try:
        try: wait.until(EC.presence_of_element_located((By.XPATH, "//h1|//h2")))
        except TimeoutException: time.sleep(0.8)

        try:
            n = driver.find_element(By.XPATH, "//span[@data-automation='job-detail-classifications']/a")
            field_text = n.text.strip()
        except Exception: pass
        try:
            n = driver.find_element(By.XPATH, "//span[@data-automation='job-detail-work-type']/a")
            job_type_text = n.text.strip()
        except Exception: pass

        node = ensure_job_details_node()
        if node:
            jd_text = driver.execute_script("return arguments[0].innerText;", node).strip()
            html_fragment = driver.execute_script("return arguments[0].outerHTML;", node)
        else:
            m = re.search(r"/(?:job|expiredjob)/(\d+)", job_url or "")
            if m and "/expiredjob/" not in job_url:
                expired = f"https://www.seek.co.nz/expiredjob/{m.group(1)}?ref=applied"
                driver.get(expired)
                node = ensure_job_details_node()
                if node:
                    jd_text = driver.execute_script("return arguments[0].innerText;", node).strip()
                    html_fragment = driver.execute_script("return arguments[0].outerHTML;", node)
    finally:
        try:
            handles = driver.window_handles
            if len(handles) > 1:
                for h in list(handles):
                    if h != orig_handle:
                        try:
                            driver.switch_to.window(h)
                            driver.close()
                        except Exception:
                            pass
                try:
                    driver.switch_to.window(orig_handle)
                except Exception:
                    pass
        except Exception:
            pass
        time.sleep(0.2)

    return (field_text, job_type_text, jd_text, html_fragment)


# =========================
# (A) 首次加载：收集 appliedJobs 全量（含顺序）
# =========================
def collect_all_applied_jobs_via_cdp():
    """返回 (jobs_map, ordered_ids)，其中 jobs_map[jid] = {..., is_external, is_active, events}"""
    jobs_map = {}
    ordered_ids = []

    # 清日志并打开 applied-jobs
    _ = driver.get_log("performance")
    driver.get(APPLIED_URL)
    try:
        wait_present((By.XPATH, "//*[contains(@id,'tabs-saved-applied_') and contains(@id,'_panel')]"))
    except TimeoutException:
        pass

    # 轻滚 + 等待请求
    driver.execute_script("window.scrollTo(0,0);")
    for _ in range(6):
        driver.execute_script("window.scrollBy(0,1400);"); time.sleep(0.5)
    time.sleep(1.0)

    # 收割 GraphQL
    for data in _iter_graphql_responses():
        edges = (((data.get("data") or {}).get("viewer") or {}).get("appliedJobs") or {}).get("edges") or []
        for edge in edges:
            node = edge.get("node") or {}
            job  = node.get("job") or {}
            jid  = str(job.get("id") or "")
            if not jid: continue
            if jid not in ordered_ids:
                ordered_ids.append(jid)
            adv = job.get("advertiser") or {}
            loc = job.get("location") or {}
            sal = job.get("salary") or {}
            crt = job.get("createdAt") or {}
            events = node.get("events") or []
            jobs_map[jid] = {
                "job_title": job.get("title"),
                "company": adv.get("name"),
                "address": loc.get("label"),
                "salary": (sal or {}).get("label"),
                "posted_date": clean_date_text((crt or {}).get("label")),
                "events": events,
                "is_active": node.get("isActive", True),
                "is_external": node.get("isExternal", True),
            }
    return jobs_map, ordered_ids


# =========================
# (B) 打开“构造的抽屉页 URL”后：只取 ApplicantCount（不取下载直链）
# =========================
def get_competitor_from_drawer_via_cdp(wait_secs=6):
    """当前页为 /my-activity/applied-jobs/{job_id}?page=X，读取 GraphQL 中的 ApplicantCount。"""
    _ = driver.get_log("performance")
    deadline = time.time() + wait_secs
    competitor = None
    while time.time() < deadline:
        time.sleep(0.6)
        for data in _iter_graphql_responses():
            insights = ((data.get("data") or {}).get("jobDetails") or {}).get("insights") or []
            for ins in insights:
                if isinstance(ins, dict) and ins.get("__typename") == "ApplicantCount":
                    c = ins.get("count")
                    if c is not None:
                        competitor = int(c)
        if competitor is not None:
            break
    return competitor


# =========================
# 时间线 & 竞争者合并策略
# =========================
def merge_timelines(existing, incoming):
    """合并时间线：去重（status, date, note），再按 date 排序（空放最后）"""
    def key(t): return (t.get("status",""), t.get("date",""), t.get("note",""))
    merged = { key(t): t for t in (existing or []) }
    for t in (incoming or []):
        merged[key(t)] = t
    arr = list(merged.values())
    arr.sort(key=lambda x: (x.get("date","")=="" , x.get("date","")))
    return arr

def max_competitor(old_txt, new_int):
    def to_int(x):
        try: return int(x)
        except Exception: return None
    old = to_int(old_txt)
    if old is None: return str(new_int) if new_int is not None else None
    if new_int is None: return str(old)
    return str(max(old, new_int))


# =========================
# 主流程
# =========================
print(f"[MODE] {MODE.upper()}")
print("[INFO] Warmup & collect applied jobs via CDP...")
ensure_cf_clearance()
all_jobs_map, ordered_ids = collect_all_applied_jobs_via_cdp()
print(f"[INIT] collected jobs: {len(ordered_ids)}")

# 逐条处理（按列表顺序，便于计算 page）
for idx, jid in enumerate(ordered_ids):
    base = all_jobs_map.get(jid, {})
    if not base:
        continue

    is_external = base.get("is_external", True)
    is_active   = base.get("is_active", True)
    page_idx    = idx // 20 + 1  # 0-19:1, 20-39:2, ...

    # 1) SEEK 源：进入“构造的抽屉页 URL”，拿 ApplicantCount + 点击下载 CV/CL
    competitor = None
    cv_bytes = cl_bytes = None
    if not is_external:
        drawer_url = build_drawer_url(jid, page_idx)
        driver.get(drawer_url)
        try:
            wait_present((By.XPATH, "//div[starts-with(@id,'drawer-view-')]"))
        except TimeoutException:
            # 某些情况下抽屉自动打开略慢，等一点日志也能拿到 insights
            pass
        competitor = get_competitor_from_drawer_via_cdp(wait_secs=6)
        # 只通过按钮下载
        cv_bytes, cl_bytes = download_cv_cl_via_buttons()

    # 2) 详情页（HTTPS 优先，失败回退 Selenium）
    field, job_type, jd_text, html_fragment = fetch_detail_via_https(jid, is_active=is_active)
    if not any([field, job_type, jd_text, html_fragment]):
        job_url_tmp = build_job_url_from_jobid(jid, is_active=is_active)
        f2, jt2, jd2, html2 = parse_detail_page_via_selenium(job_url_tmp)
        field = field or f2; job_type = job_type or jt2; jd_text = jd_text or jd2; html_fragment = html_fragment or html2

    # 3) 时间线/摘要
    timeline_new = uniq_sorted_timeline(base.get("events"))
    status_summary_new = timeline_new[-1]["status"] if timeline_new else "Applied"
    job_url_final = build_job_url_from_jobid(jid, is_active=is_active)
    source_label = "SEEK" if not is_external else "External"

    # 4) 入库（按 job_id 与 url 中 id 匹配）
    try:
        # 用正则匹配 url 中包含该 job_id（兼容 job/ 与 expiredjob/）
        regex = rf"/(job|expiredjob)/{re.escape(jid)}(\?|$)"
        cur.execute("SELECT id, job_url, status_timeline, competitor_count, "
                    "job_title, company, address, field, job_type, jd, html_content, source "
                    "FROM jobsnew WHERE job_url ~ %s LIMIT 1", (regex,))
        row = cur.fetchone()

        if row:
            row_id, existing_url, st_old, comp_old, \
            jt_old, co_old, ad_old, field_old, jtype_old, jd_old, html_old, src_old = row

            # 只追加时间线（合并去重）
            merged_timeline = merge_timelines(st_old, timeline_new)
            status_summary  = merged_timeline[-1]["status"] if merged_timeline else status_summary_new

            # 竞争者人数只增不减
            comp_final = max_competitor(comp_old, competitor)

            # 仅在库里为空时补齐其它字段
            payload = {
                "id": row_id,
                "job_url": existing_url or job_url_final,
                "job_title": jt_old or base.get("job_title"),
                "company": co_old or base.get("company"),
                "address": ad_old or base.get("address"),
                "field": field_old or field,
                "job_type": jtype_old or job_type,
                "posted_date": base.get("posted_date"),  # 日期通常稳定，可覆盖
                "salary": base.get("salary"),
                "competitor_count": comp_final,
                "jd": jd_old or jd_text,
                "html_content": html_old or html_fragment,
                "source": src_old or source_label,
                "status_summary": status_summary,
                "status_timeline": Json(merged_timeline),
                "cv_file": psycopg2.Binary(cv_bytes) if (cv_bytes and not None) else None,
                "cl_file": psycopg2.Binary(cl_bytes) if (cl_bytes and not None) else None,
                "created_at": datetime.utcnow().isoformat()
            }

            cur.execute("""
                UPDATE jobsnew SET
                  job_title=%(job_title)s, company=%(company)s, address=%(address)s,
                  field=%(field)s, job_type=%(job_type)s, posted_date=%(posted_date)s,
                  salary=%(salary)s, competitor_count=%(competitor_count)s, jd=%(jd)s,
                  html_content=%(html_content)s, source=%(source)s, status_summary=%(status_summary)s,
                  status_timeline=%(status_timeline)s,
                  cv_file=COALESCE(%(cv_file)s, cv_file),
                  cl_file=COALESCE(%(cl_file)s, cl_file),
                  created_at=%(created_at)s
                WHERE id=%(id)s
            """, payload)
            print(f"  ↻ Updated (merge) {jid}")
        else:
            # 新插入
            payload = {
                "id": str(uuid.uuid4()),
                "job_url": job_url_final,
                "job_title": base.get("job_title"),
                "company": base.get("company"),
                "address": base.get("address"),
                "field": field,
                "job_type": job_type,
                "posted_date": base.get("posted_date"),
                "salary": base.get("salary"),
                "competitor_count": str(competitor) if competitor is not None else None,
                "jd": jd_text,
                "html_content": html_fragment,
                "source": source_label,
                "status_summary": status_summary_new,
                "status_timeline": Json(timeline_new),
                "cv_file": psycopg2.Binary(cv_bytes) if cv_bytes else None,
                "cl_file": psycopg2.Binary(cl_bytes) if cl_bytes else None,
                "created_at": datetime.utcnow().isoformat()
            }
            cur.execute("""
                INSERT INTO jobsnew (
                  id, job_url, job_title, company, address, field, job_type,
                  posted_date, salary, competitor_count, jd, html_content,
                  source, status_summary, status_timeline, cv_file, cl_file, created_at
                ) VALUES (
                  %(id)s, %(job_url)s, %(job_title)s, %(company)s, %(address)s, %(field)s, %(job_type)s,
                  %(posted_date)s, %(salary)s, %(competitor_count)s, %(jd)s, %(html_content)s,
                  %(source)s, %(status_summary)s, %(status_timeline)s, %(cv_file)s, %(cl_file)s, %(created_at)s
                )
            """, payload)
            print(f"  ✓ Inserted {jid}")

        PG_CONN.commit()
    except Exception as e:
        PG_CONN.rollback()
        print("  [DB Error]", e)

    # TEST 模式可只跑前若干项
    if MODE == "test" and idx >= 19:
        print("[TEST] processed first 20 items, stopping.")
        break

# 清理
try: driver.quit()
except Exception: pass
cur.close(); PG_CONN.close()
print("✅ All done.")
