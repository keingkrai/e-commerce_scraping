import asyncio
import sys
import random
import json
import re
import math
import pandas as pd

from pathlib import Path
from datetime import datetime, timedelta
from urllib.parse import quote_plus
from cloakbrowser import launch_persistent_context_async
import pprint

pp = pprint.PrettyPrinter(indent=2, sort_dicts=False)

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

USER_DATA_DIR = "./data/lazada/my_session"          # Browser session (cookies, local storage, etc.)
OUTPUT_DIR = "./data/lazada/collected_data"         # Final output data
TEMP_DIR = "./data/lazada/collected_data/temp"      # Temp checkpoint

# Log folders
LOG_DIR = "./data/lazada/log"
PRODUCT_LOG_DIR = "./data/lazada/log/product"
DETAIL_LOG_DIR = "./data/lazada/log/detail"
REVIEW_LOG_DIR = "./data/lazada/log/review"
SEARCH_LOG_DIR = "./data/lazada/log/search"
CAPTCHA_LOG_DIR = "./data/lazada/log/captcha"

# Delay request
REQUEST_DELAY_MIN = 2.5
REQUEST_DELAY_MAX = 5.0

# Cool-down: พักยาวเป็นระยะๆ ระหว่างดึงข้อมูล เพื่อตัด pattern การยิง request
# ถี่สม่ำเสมอ (สาเหตุหลักที่โดนบอทตรวจจับตอนดึงข้อมูลจำนวนมาก เช่น 4000+ products)
SHORT_COOLDOWN_EVERY = 25            # พักสั้นทุกๆ N request
SHORT_COOLDOWN_RANGE = (45, 90)      # วินาที
LONG_COOLDOWN_EVERY  = 200           # พักยาวทุกๆ N request
LONG_COOLDOWN_RANGE  = (300, 600)    # วินาที (5-10 นาที)

# เปลี่ยนเป็น True เฉพาะตอนต้องการ Login / สร้าง session ใหม่
CREATE_SESSION = False

# Checkpoint Frequency
PRODUCT_CHECKPOINT_EVERY_PAGES = 50
DETAIL_CHECKPOINT_EVERY_URLS = 50
REVIEW_CHECKPOINT_EVERY_PAGES = 50
SEARCH_CHECKPOINT_EVERY_PAGES = 50

# True: โหลดข้อมูล temp เดิมจาก collected_data/temp/ กลับมาใช้ต่อ, False: เริ่มทำงานใหม่
RESUME_FROM_BACKUP = False

for folder in [
    USER_DATA_DIR,
    OUTPUT_DIR,
    TEMP_DIR,
    LOG_DIR,
    PRODUCT_LOG_DIR,
    DETAIL_LOG_DIR,
    REVIEW_LOG_DIR,
    SEARCH_LOG_DIR,
    CAPTCHA_LOG_DIR,
]:
    Path(folder).mkdir(parents=True, exist_ok=True)

print(f"🌐 Browser session folder: {USER_DATA_DIR}")
print(f"📂 Output folder: {OUTPUT_DIR}")
print(f"💾 Temp checkpoint folder: {TEMP_DIR}")
print(f"🧾 Log folder: {LOG_DIR}")

PRODUCT_DEDUPE_COLS = ["product_id", "sku_id", "product_url"]
DETAIL_DEDUPE_COLS  = ["product_id", "product_url"]
REVIEW_DEDUPE_COLS  = ["product_id", "user_name", "review_date_raw", "comment"]
SEARCH_DEDUPE_COLS  = ["search_keyword", "product_id", "sku_id", "product_url"]

async def create_browser_context(block_images=False):
    """
    เปิด CloakBrowser แบบ persistent context เพื่อเก็บ session/cookies
    หมายเหตุ: ห้ามเปิดหลาย browser พร้อมกันโดยใช้ USER_DATA_DIR เดียวกัน
    """
    context = await launch_persistent_context_async(
        USER_DATA_DIR,
        headless=False,
        viewport={"width": 1366, "height": 768},
        locale="th-TH",
        timezone_id="Asia/Bangkok",
        humanize=True,
    )
    page = context.pages[0] if context.pages else await context.new_page()
    if block_images:
        async def block_image_route(route):
            await route.abort()
        await page.route("**/*.{png,jpg,jpeg,webp,gif,svg}", block_image_route)
    return context, page


_request_counter = {"n": 0}

async def human_delay(min_delay=REQUEST_DELAY_MIN, max_delay=REQUEST_DELAY_MAX):
    """
    หน่วงเวลาแบบสุ่มทุก request + คูลดาวน์พักยาวเป็นระยะๆ (short/long)
    เพื่อตัด pattern การยิง request ถี่สม่ำเสมอที่ระบบตรวจจับบอทมักจับได้ง่าย
    ใช้ counter เดียวกันข้ามทุก scraper (product/detail/review/search) เพราะเรียกผ่านฟังก์ชันนี้ทั้งหมด
    """
    await asyncio.sleep(random.uniform(min_delay, max_delay))

    _request_counter["n"] += 1
    n = _request_counter["n"]

    if n % LONG_COOLDOWN_EVERY == 0:
        pause = random.uniform(*LONG_COOLDOWN_RANGE)
        print(f"   🧊 [LONG COOLDOWN] รวม {n} requests — พัก {pause/60:.1f} นาที ก่อนไปต่อ...")
        await asyncio.sleep(pause)
    elif n % SHORT_COOLDOWN_EVERY == 0:
        pause = random.uniform(*SHORT_COOLDOWN_RANGE)
        print(f"   🧊 [COOLDOWN] รวม {n} requests — พัก {pause:.0f}s ก่อนไปต่อ...")
        await asyncio.sleep(pause)


def make_timestamp():
    """สร้าง timestamp สำหรับชื่อไฟล์"""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def make_collected_at():
    """บันทึกเวลาที่ดึงข้อมูล"""
    return datetime.now().strftime("%Y-%m-%d")


# ## Lazada Session Setup

async def create_lazada_session():
    """
    ใช้สำหรับ Login และบันทึก Session
    ควรรันเฉพาะกรณี: ใช้งานครั้งแรก / Session หมดอายุ / Lazada บังคับ Login ใหม่
    """
    context, page = await create_browser_context(block_images=False)
    await page.goto("https://www.lazada.co.th", wait_until="domcontentloaded", timeout=60000)
    print("\n" + "—" * 60)
    print("⚡ [SESSION] Browser is active.")
    print("—" * 60)
    print(" 1. Login Lazada manually.")
    print(" 2. Confirm logistics/payment settings if required.")
    print(" 3. Close browser window when finished.")
    print("—" * 60 + "\n")
    while len(context.pages) > 0:
        await asyncio.sleep(1)
    print(f"✅ [SUCCESS] Session saved to '{USER_DATA_DIR}'.")


# ## General Helpers

def safe_float(val, default=0.0):
    """แปลงค่าเป็น float แบบปลอดภัย"""
    try:
        if val is None or str(val).strip() == "":
            return default
        text = re.sub(r"[,\s฿]", "", str(val))
        text = text.replace("THB", "").replace("thb", "")
        return float(text)
    except Exception:
        return default

def safe_int(val, default=0):
    """แปลงค่าเป็น int แบบปลอดภัย"""
    number = safe_float(val, default=None)
    if number is None:
        return default
    try:
        return int(number)
    except Exception:
        return default

def parse_sold_count(sold_str):
    """
    แปลงยอดขายจากข้อความ Lazada เป็นตัวเลข
    เช่น: '9.4K ชิ้น' -> 9400 | '1.2M sold' -> 1200000
    """
    if not sold_str:
        return 0
    s = (
        str(sold_str).lower()
        .replace("ชิ้น", "").replace("ขายแล้ว", "").replace("sold", "")
        .replace(",", "").strip()
    )
    if "k" in s:
        return int(safe_float(s.replace("k", "")) * 1_000)
    if "m" in s:
        return int(safe_float(s.replace("m", "")) * 1_000_000)
    return safe_int(re.sub(r"[^\d]", "", s))

def clean_brand(brand):
    """ทำความสะอาดชื่อแบรนด์"""
    if not brand:
        return ""
    brand = str(brand).strip()
    if re.search(r"no\s*brand", brand, re.IGNORECASE):
        return ""
    return brand

def clean_url(url):
    """แปลง URL ให้เป็น absolute URL"""
    if not url:
        return ""
    url = str(url).strip()
    if url.startswith("http"):
        return url
    if url.startswith("//"):
        return f"https:{url}"
    if url.startswith("/"):
        return f"https://www.lazada.co.th{url}"
    return f"https://www.lazada.co.th/{url}"

def clean_full_text(text):
    """Description: ยุบบรรทัดว่างซ้ำ ยุบช่องว่างซ้ำ คงโครงสร้างหลายบรรทัด"""
    if not text:
        return ""
    text = re.sub(r"\n\s*\n", "\n", str(text))
    text = re.sub(r" +", " ", text)
    return text.strip()

def clean_minimal(text):
    """Specs/Qualification: ยุบทุกอย่างให้เหลือบรรทัดเดียว"""
    if not text:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()

def safe_json_loads(raw_text, default=None):
    """แปลง JSON string แบบปลอดภัย"""
    if default is None:
        default = {}
    if not raw_text:
        return default
    try:
        return json.loads(raw_text)
    except Exception:
        return default
    
def safe_rating(product):
    """คืน rating_score เป็น float หรือ None ถ้าไม่มีคะแนน"""
    raw = product.get("ratingScore")
    if raw is None or str(raw).strip() == "":
        return None
    val = safe_float(raw, default=None)
    return val if val and val > 0 else None

# ## Date Helper
THAI_ABBR_MONTHS = {
    "ม.ค.": "Jan", "ก.พ.": "Feb", "มี.ค.": "Mar", "เม.ย.": "Apr",
    "พ.ค.": "May", "มิ.ย.": "Jun", "ก.ค.": "Jul", "ส.ค.": "Aug",
    "ก.ย.": "Sep", "ต.ค.": "Oct", "พ.ย.": "Nov", "ธ.ค.": "Dec",
}

def thai_date_to_datetime(date_str):
    """
    แปลงวันที่เป็น YYYY-MM-DD
    รองรับ: '05 Apr 2025' | '05 ม.ค. 2025'
    """
    if not date_str:
        return None
    translated = str(date_str).strip()
    for thai_abbr, eng_abbr in THAI_ABBR_MONTHS.items():
        if thai_abbr in translated:
            translated = translated.replace(thai_abbr, eng_abbr)
            break
    try:
        return datetime.strptime(translated, "%d %b %Y").strftime("%Y-%m-%d")
    except ValueError:
        return None

def convert_time_interval(interval):
    """แปลงเวลาจาก Lazada ให้เป็น YYYY-MM-DD"""
    if not interval:
        return None
    ref = datetime.now()
    interval = str(interval).strip()
    lower = interval.lower()
    if lower in ["วันนี้", "today", "just now"]:
        return ref.strftime("%Y-%m-%d")
    if lower in ["เมื่อวาน", "yesterday"]:
        return (ref - timedelta(days=1)).strftime("%Y-%m-%d")
    parts = interval.split()
    if len(parts) >= 2:
        qty = safe_int(parts[0], default=None)
        if qty is not None:
            mapping = [
                (["นาทีที่แล้ว", "minute ago", "minutes ago"],  timedelta(minutes=qty)),
                (["ชั่วโมงที่แล้ว", "hour ago", "hours ago"],   timedelta(hours=qty)),
                (["วันที่แล้ว", "day ago", "days ago"],          timedelta(days=qty)),
                (["สัปดาห์ที่แล้ว", "week ago", "weeks ago"],   timedelta(weeks=qty)),
                (["เดือนที่แล้ว", "month ago", "months ago"],   timedelta(days=qty * 30)),
                (["ปีที่แล้ว", "year ago", "years ago"],        timedelta(days=qty * 365)),
            ]
            for keywords, delta in mapping:
                if any(k in interval for k in keywords):
                    return (ref - delta).strftime("%Y-%m-%d")
    formatted = thai_date_to_datetime(interval)
    return formatted if formatted else interval

# ## Log & Temp Helpers
def get_scraper_log_paths(scraper_name):
    """คืน path ของ log file และ error jsonl ตามประเภท scraper"""
    mapping = {
        "product_by_shop": PRODUCT_LOG_DIR, "product": PRODUCT_LOG_DIR,
        "product_detail":  DETAIL_LOG_DIR,  "detail":  DETAIL_LOG_DIR,
        "reviews":         REVIEW_LOG_DIR,  "review":  REVIEW_LOG_DIR,
        "search_keyword":  SEARCH_LOG_DIR,  "search":  SEARCH_LOG_DIR,
    }
    log_dir = mapping.get(scraper_name, LOG_DIR)
    return {
        "log_file":   Path(log_dir) / f"{scraper_name}_scraper.log",
        "error_file": Path(log_dir) / f"{scraper_name}_error.jsonl",
    }

def append_text_log(scraper_name, level, message, context=None):
    """เขียน log แบบ text append ทีละบรรทัด"""
    paths = get_scraper_log_paths(scraper_name)
    paths["log_file"].parent.mkdir(parents=True, exist_ok=True)
    context = context or {}
    line = f"{make_collected_at()} | {level.upper()} | {scraper_name} | {message} | {json.dumps(context, ensure_ascii=False, default=str)}\n"
    with open(paths["log_file"], "a", encoding="utf-8") as f:
        f.write(line)

def append_error_jsonl(scraper_name, record):
    """เขียน error/warning แบบ JSONL append ทีละ record"""
    paths = get_scraper_log_paths(scraper_name)
    paths["error_file"].parent.mkdir(parents=True, exist_ok=True)
    with open(paths["error_file"], "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

def add_scrape_log(log_rows, scraper_name, level, message, **context):
    """
    เพิ่ม log record ระหว่าง scraping
    - print บนหน้าจอ
    - append ลง .log
    - ถ้า WARNING/ERROR จะ append ลง .jsonl ด้วย
    """
    level = level.upper()
    record = {"logged_at": make_collected_at(), "scraper_name": scraper_name, "level": level, "message": message}
    record.update(context)
    log_rows.append(record)
    icons = {"ERROR": "❌", "WARNING": "⚠️", "INFO": "ℹ️"}
    print(f"   {icons.get(level, 'ℹ️')} [{scraper_name}] {message}")
    if context and level in ["ERROR", "WARNING"]:
        print(f"   > Context: {context}")
    append_text_log(scraper_name, level, message, context)
    if level in ["WARNING", "ERROR"]:
        append_error_jsonl(scraper_name, record)
    return record

def save_scrape_checkpoint(rows, data_prefix, dedupe_cols=None, reason="checkpoint", overwrite_latest=True):
    """บันทึก checkpoint ระหว่าง scraping เป็น CSV ลง collected_data/temp/"""
    Path(TEMP_DIR).mkdir(parents=True, exist_ok=True)
    if not rows:
        print(f"   ⚠️ No data to save at checkpoint: {reason}")
        return {}
    df = pd.DataFrame(rows)
    if dedupe_cols:
        valid = [c for c in dedupe_cols if c in df.columns]
        if valid:
            df = df.drop_duplicates(subset=valid)
    fname = f"{data_prefix}.csv" if overwrite_latest else f"{data_prefix}_{make_timestamp()}.csv"
    path = Path(TEMP_DIR) / fname
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"   💾 Checkpoint: {path} | rows={len(df):,} | reason={reason}")
    return {"data": path}

def _sanitize_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    """Strip control characters openpyxl refuses to write into a cell
    (raises IllegalCharacterError, aborting the save with no file written
    otherwise). Seen live: a scraped shop name containing a literal
    backspace char (\\x08) — a real artifact of Lazada's own data, not a
    scraping bug."""
    from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
    str_cols = df.select_dtypes(include="object").columns
    for col in str_cols:
        df[col] = df[col].map(lambda v: ILLEGAL_CHARACTERS_RE.sub("", v) if isinstance(v, str) else v)
    return df

def save_final_data(rows, final_prefix, dedupe_cols=None, output_cols=None):
    """
    บันทึก final file เป็น Excel ลง collected_data/

    output_cols (optional): dict หรือ list สำหรับเลือก/เปลี่ยนชื่อ column ในไฟล์ที่บันทึก
      - list  → เลือกเฉพาะ column เหล่านั้น (ชื่อคงเดิม)
               เช่น ["qualification_info", "all_specs", "description", "description_images"]
      - dict  → เลือก + เปลี่ยนชื่อ {ชื่อเดิม: ชื่อใหม่}
               เช่น {"all_specs": "specifications", "description_images": "description_image_urls"}

    หมายเหตุ: deduplication และ done_urls ทำงานกับ column ครบชุดเสมอ
              output_cols ถูก apply เฉพาะตอน write Excel เท่านั้น
    """
    if not rows:
        print(f"   ⚠️ No data to save: {final_prefix}")
        return pd.DataFrame(), None
    df = pd.DataFrame(rows)
    if dedupe_cols:
        valid = [c for c in dedupe_cols if c in df.columns]
        if valid:
            df = df.drop_duplicates(subset=valid)
    # ── Column selection / rename (apply only to saved file) ──────────────────
    df_out = df.copy()
    if output_cols is not None:
        if isinstance(output_cols, dict):
            # เลือก + เปลี่ยนชื่อ: {old_name: new_name}
            valid_keys = [k for k in output_cols if k in df_out.columns]
            df_out = df_out[valid_keys].rename(columns=output_cols)
        elif isinstance(output_cols, list):
            # เลือกเฉพาะ columns ที่มีอยู่จริง
            valid_cols = [c for c in output_cols if c in df_out.columns]
            df_out = df_out[valid_cols]
    df_out = _sanitize_for_excel(df_out)
    path = Path(OUTPUT_DIR) / f"{final_prefix}_{make_timestamp()}.xlsx"
    df_out.to_excel(path, index=False)
    print("\n" + "=" * 60)
    print(f"✅ FINAL FILE SAVED : {final_prefix}")
    print(f"📊 Total Rows       : {len(df_out):,}")
    print(f"📋 Columns          : {df_out.columns.tolist()}")
    print(f"💾 File             : {path}")
    print("=" * 60)
    return df, path  # คืน df ครบชุด (ไม่ถูก filter) เพื่อให้ caller ใช้งานต่อได้

def save_failed_log(failed_rows, prefix, scraper_name="general"):
    """บันทึก failed log เป็น JSONL"""
    if not failed_rows:
        return None
    paths = get_scraper_log_paths(scraper_name)
    paths["error_file"].parent.mkdir(parents=True, exist_ok=True)
    with open(paths["error_file"], "a", encoding="utf-8") as f:
        for row in failed_rows:
            f.write(json.dumps({"logged_at": make_collected_at(), "type": prefix, **row}, ensure_ascii=False, default=str) + "\n")
    print(f"⚠️ Failed log saved: {paths['error_file']}")
    return paths["error_file"]

def save_debug_html(html, prefix, key="unknown"):
    """บันทึก HTML สำหรับ debug กรณีดึง Detail ไม่ได้"""
    Path(DETAIL_LOG_DIR).mkdir(parents=True, exist_ok=True)
    path = Path(DETAIL_LOG_DIR) / f"{prefix}_{key}_{make_timestamp()}.html"
    path.write_text(html, encoding="utf-8")
    print(f"⚠️ Debug HTML saved: {path}")
    return path

def load_existing_backup(data_prefix, dedupe_cols=None):
    """โหลด temp backup เดิม (.csv/.xlsx) จาก collected_data/temp/"""
    csv_path  = Path(TEMP_DIR) / f"{data_prefix}.csv"
    xlsx_path = Path(TEMP_DIR) / f"{data_prefix}.xlsx"
    if csv_path.exists():
        backup_path, reader = csv_path, pd.read_csv
    elif xlsx_path.exists():
        backup_path, reader = xlsx_path, pd.read_excel
    else:
        print(f"ℹ️ No existing temp backup: {csv_path} or {xlsx_path}")
        return []
    try:
        df = reader(backup_path)
        if dedupe_cols:
            valid = [c for c in dedupe_cols if c in df.columns]
            if valid:
                df = df.drop_duplicates(subset=valid)
        rows = df.to_dict(orient="records")
        print(f"🔁 Backup loaded: {backup_path} | rows={len(rows):,}")
        return rows
    except Exception as e:
        print(f"⚠️ Failed to load backup: {backup_path} | error={e}")
        return []

# ## Page Check Helper

async def log_captcha_encounter(page, label):
    """บันทึก page source และ screenshot เมื่อพบ captcha/verification"""
    ts = make_timestamp()
    safe_label = re.sub(r"[^\w\-]", "_", label)[:60]
    base = Path(CAPTCHA_LOG_DIR) / f"{ts}_{safe_label}"
    try:
        html = await page.content()
        html_path = base.with_suffix(".html")
        html_path.write_text(html, encoding="utf-8")
    except Exception as e:
        print(f"   ⚠️ [CAPTCHA LOG] Could not save HTML: {e}")
        html_path = None
    try:
        png_path = base.with_suffix(".png")
        await page.screenshot(path=str(png_path), full_page=False)
    except Exception as e:
        print(f"   ⚠️ [CAPTCHA LOG] Could not save screenshot: {e}")
        png_path = None
    meta = {
        "logged_at": make_collected_at(),
        "label": label,
        "url": page.url,
        "html_file": str(html_path) if html_path else None,
        "screenshot_file": str(png_path) if png_path else None,
    }
    meta_path = base.with_suffix(".json")
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"   📸 [CAPTCHA LOG] Saved: {base}.{{html,png,json}}")
    return meta


async def handle_page_check(page, label, max_wait_seconds=120):
    """
    ตรวจสอบว่าหน้าเป็น:
    - JSON API ปกติ  → return content
    - HTML ปกติ      → return content
    - หน้า verification → log captcha artifacts แล้วรอให้ผู้ใช้ผ่านก่อน แล้วค่อย return
    """
    start = datetime.now()
    captcha_logged = False
    images_unblocked = False
    while True:
        if (datetime.now() - start).total_seconds() > max_wait_seconds:
            raise TimeoutError(f"Page check timeout at: {label}")
        try:
            content = (await page.inner_text("body", timeout=5000)).strip()
        except Exception:
            content = ""
        if not content:
            await asyncio.sleep(1)
            continue
        if content.startswith("{") or content.startswith("["):
            return content
        url_lower = page.url.lower()
        try:
            # :visible matters — Lazada preloads the nocaptcha widget's
            # container/CSS on every page (even ones with no active
            # challenge), so a plain "count() > 0" on these selectors
            # matches constantly and false-positives on ordinary pages,
            # sending the run into an unsolvable manual-check loop (nothing
            # is actually showing to solve). Only count it if the widget is
            # actually being displayed.
            has_slider = await page.locator("#nocaptcha:visible, #px-captcha:visible, .btn_slide:visible").count() > 0
        except Exception:
            has_slider = False
        is_verify_url  = any(k in url_lower for k in ["verify", "punish", "captcha"])
        is_verify_text = any(k.lower() in content.lower() for k in [
            "verify", "verification", "captcha", "security check",
            "กรุณายืนยัน", "ยืนยันตัวตน",
        ])
        # Network/IP-reputation block (harsher than a per-page captcha — Lazada
        # has flagged the whole connection, not just this request). Different
        # wording from the slider/verify texts above, so needs its own check.
        is_network_block = any(k.lower() in content.lower() for k in [
            "การเข้าชมที่ผิดปกติ", "เครือข่ายของคุณ", "ลองใหม่อีกครั้งในภายหลัง",
            "unusual traffic", "unusual activity", "traffic from your network",
        ])
        if not has_slider and not is_verify_url and not is_verify_text and not is_network_block:
            return content
        if not captcha_logged:
            await log_captcha_encounter(page, label)
            captcha_logged = True
        if is_network_block:
            print(
                f"\n   🚫 [NETWORK BLOCK] {label} — Lazada flagged this network/IP as abnormal traffic.\n"
                "      Clicking anything won't fix this — it needs time (often hours) and/or a different IP.\n"
                "      Recommended: stop the run now, wait, then resume later with RESUME_FROM_BACKUP=True.\n"
            )
            # Unlike a solvable slider/verify captcha, no amount of waiting at
            # an input() prompt fixes an IP-level block — it just hangs the
            # run forever (or crashes with EOFError with no TTY attached).
            # Fail this URL now so the caller's per-URL try/except logs it
            # and moves on instead of stalling indefinitely.
            raise RuntimeError(f"Network block (abnormal traffic) at: {label}")
        if not images_unblocked:
            # Search/reviews/shop scraping run with block_images=True (aborts
            # all image requests for speed). The nocaptcha slider's puzzle-
            # piece graphics are images too, so under that route they never
            # load — the slider looks blank and can't be solved correctly no
            # matter how it's dragged, so Lazada just keeps re-issuing a new
            # challenge (looks like the page endlessly refreshing). Unblock
            # images for the rest of this browser session and reload so the
            # already-aborted image requests actually get fetched this time.
            try:
                await page.unroute("**/*.{png,jpg,jpeg,webp,gif,svg}")
                print("   🖼️  [CAPTCHA] Unblocked images and reloading so the slider is visible...")
                await page.reload(wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(1)
            except Exception as e:
                print(f"   ⚠️  [CAPTCHA] Could not reload after unblocking images: {e}")
            images_unblocked = True
            continue
        prompt = f"\n >>> ⚠️ [MANUAL CHECK] {label} — Please complete verification in browser, then press ENTER..."
        await asyncio.get_event_loop().run_in_executor(None, input, prompt)
        await page.wait_for_load_state("domcontentloaded")
        await asyncio.sleep(1)
        captcha_logged = False

_TRANSIENT_NETWORK_ERRORS = (
    "err_internet_disconnected", "err_name_not_resolved", "err_connection_reset",
    "err_connection_closed", "err_connection_refused", "err_network_changed",
    "err_timed_out", "getaddrinfo enotfound",
)

async def _goto_with_network_retry(page, url, attempts=4, wait_seconds=8, **goto_kwargs):
    """page.goto wrapper that retries on local network drops (e.g. wifi
    blips causing net::ERR_INTERNET_DISCONNECTED / DNS ENOTFOUND) instead of
    failing the whole product/page immediately. These are unrelated to
    Lazada's anti-bot — they mean this machine had no internet for a moment
    — so a short wait-and-retry is enough, unlike WAF/network-block cases
    which need real time or a different IP."""
    goto_kwargs.setdefault("wait_until", "domcontentloaded")
    goto_kwargs.setdefault("timeout", 60000)
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            return await page.goto(url, **goto_kwargs)
        except Exception as e:
            last_error = e
            if not any(k in str(e).lower() for k in _TRANSIENT_NETWORK_ERRORS):
                raise
            print(f"   🌐 [NETWORK] Local connection drop (attempt {attempt}/{attempts}): {e}")
            if attempt < attempts:
                await asyncio.sleep(wait_seconds)
    raise last_error


# ## Product Info Helpers
def extract_product_id_from_url(url):
    """ดึง product_id จาก URL Lazada"""
    if not url:
        return "unknown"
    url = str(url)
    match = re.search(r"-i(\d+)", url)
    if match:
        return match.group(1)
    match = re.search(r"[?&]itemId=(\d+)", url)
    if match:
        return match.group(1)
    return "unknown"

async def safe_inner_text(locator, timeout=3000, default=""):
    """ดึง inner_text แบบปลอดภัย"""
    try:
        return await locator.inner_text(timeout=timeout)
    except Exception:
        return default

async def safe_get_attr(locator, attr_name, timeout=3000, default=""):
    """ดึง attribute แบบปลอดภัย"""
    try:
        value = await locator.get_attribute(attr_name, timeout=timeout)
        return value if value else default
    except Exception:
        return default

async def get_product_info(page, url):
    """
    ดึงข้อมูลพื้นฐานจากหน้าสินค้า: product_id, product_name, shop_name
    Fallback selectors เรียงจาก v2 (ใหม่) → v1 (เก่า) → generic
    """
    product_id = extract_product_id_from_url(url)

    product_name_selectors = [
        "h1.pdp-mod-product-badge-title-v2",  # v2 (ใหม่)
        "h1.pdp-mod-product-badge-title",     # v1 (เก่า)
        "h1",                                  # fallback generic
    ]
    product_name = ""
    for selector in product_name_selectors:
        try:
            product_name = (await page.locator(selector).first.inner_text(timeout=5000)).strip()
            if product_name:
                break
        except Exception:
            continue
    if not product_name:
        product_name = f"Lazada Product {product_id}"

    shop_name_selectors = [
        ".seller-name-v2__detail-name",                                            # v2 (ใหม่)
        ".seller-name__detail-name",                                                # v1 (เก่า)
        ".pdp-link.pdp-link_size_l.pdp-link_theme_black.seller-name__detail-name", # fallback
    ]
    shop_name = ""
    for selector in shop_name_selectors:
        try:
            shop_name = (await page.locator(selector).first.inner_text(timeout=5000)).strip()
            if shop_name:
                break
        except Exception:
            continue
    if not shop_name:
        shop_name = "Lazada Store"

    return product_id, product_name, shop_name

# ## Shop URL Helper
def extract_shop_key_from_url(shop_input):
    """
    ดึง shop_key จาก URL หรือรับ shop_key ตรง ๆ
    รองรับ: 'mizumi-bomi' | 'https://www.lazada.co.th/shop/mizumi-bomi/'
    """
    if not shop_input:
        return ""
    text = str(shop_input).strip()
    if text.startswith("http"):
        match = re.search(r"lazada\.co\.th/shop/([^/?#]+)/?", text)
        if match:
            return match.group(1).strip()
        match = re.search(r"lazada\.co\.th/([^/?#]+)/?", text)
        if match:
            key = match.group(1).strip()
            if key not in ["products", "catalog"]:
                return key
        return ""
    text = text.strip("/")
    if text.startswith("shop/"):
        return text.split("/", 1)[1].strip("/")
    return text

# ## Product Description Helpers
async def wait_and_scroll_until_detail_loaded(page, max_rounds=8):
    """Lazada มักโหลด specs/qualification/description หลัง scroll"""
    for _ in range(max_rounds):
        spec_count = await page.locator(".specification-keys .key-li").count()
        qual_count = await page.locator(".pdp-mod-qualification-items .col").count()
        desc_count = await page.locator(".detail-content").count()
        img_count  = await page.locator(".detail-content img").count()
        if spec_count > 0 or qual_count > 0 or desc_count > 0 or img_count > 0:
            return {"spec_count": spec_count, "qual_count": qual_count,
                    "desc_count": desc_count, "img_count": img_count, "loaded": True}
        for pct in [0.35, 0.60, 0.85]:
            await page.evaluate(f"window.scrollTo(0, document.body.scrollHeight * {pct})")
            await asyncio.sleep(1.2)
        await page.mouse.wheel(0, 1800)
        await asyncio.sleep(1.5)
    return {
        "spec_count": await page.locator(".specification-keys .key-li").count(),
        "qual_count": await page.locator(".pdp-mod-qualification-items .col").count(),
        "desc_count": await page.locator(".detail-content").count(),
        "img_count":  await page.locator(".detail-content img").count(),
        "loaded": False
    }

async def extract_product_specs(page):
    """ดึง specification table"""
    specs_data = {}
    rows = page.locator(".specification-keys .key-li")
    for i in range(await rows.count()):
        try:
            row = rows.nth(i)
            key   = clean_minimal(await safe_inner_text(row.locator(".key-title"), timeout=3000))
            value = clean_minimal(await safe_inner_text(row.locator(".key-value"),  timeout=3000))
            if key:
                specs_data[key] = value
        except Exception:
            continue
    return specs_data

async def extract_product_qualification(page):
    """ดึง qualification / product highlights"""
    qual_data = {}
    cols = page.locator(".pdp-mod-qualification-items .col")
    for i in range(await cols.count()):
        try:
            col     = cols.nth(i)
            title   = clean_minimal(await safe_inner_text(col.locator(".title"),   timeout=3000))
            content = clean_minimal(await safe_inner_text(col.locator(".content"), timeout=3000))
            if title:
                qual_data[title] = content
        except Exception:
            continue
    return qual_data

async def extract_product_description(page):
    """ดึง description text"""
    loc = page.locator(".detail-content")
    if await loc.count() > 0:
        return clean_full_text(await safe_inner_text(loc, timeout=8000))
    return ""

async def extract_product_description_images(page):
    """ดึงรูปภาพใน description"""
    desc_images = []
    imgs = page.locator(".detail-content img")
    for i in range(await imgs.count()):
        try:
            img = imgs.nth(i)
            src = (
                await safe_get_attr(img, "src")
                or await safe_get_attr(img, "data-src")
                or await safe_get_attr(img, "data-ks-lazyload")
                or await safe_get_attr(img, "data-lazy")
                or ""
            )
            src = str(src).strip()
            if src and not src.startswith("data:"):
                desc_images.append(clean_url(src))
        except Exception:
            continue
    return list(dict.fromkeys(desc_images))

# ## Listing Helper

# domClass ที่ Lazada ใช้บ่งบอกว่าเป็น LazMall
# จาก JSON ที่สังเกต: domClass '68959' = img badge ขนาดเล็ก น่าจะเป็น LazMall
# ถ้ายังไม่แน่ใจ → เปิด DEBUG_ICONS = True แล้วรัน scrape ใหม่
LAZMALL_DOM_CLASSES = {"68959"}

def extract_is_mall(product):
    """
    ตรวจสอบว่าสินค้าเป็น LazMall หรือไม่
    ใช้ domClass แทน bizType (v6+)
    """
    icons = product.get("icons", [])
    if not isinstance(icons, list):
        return False
    if not LAZMALL_DOM_CLASSES:
        return None
    return any(
        isinstance(icon, dict) and str(icon.get("domClass", "")) in LAZMALL_DOM_CLASSES
        for icon in icons
    )

# True = print icons ทุกสินค้า + เก็บ icons_raw ไว้ใน row
# False = ปิด debug (production)
DEBUG_ICONS = False

def debug_icons(product_rows, n=20):
    """
    แสดง icons ของสินค้าแต่ละชิ้น เพื่อหา domClass ของ LazMall

    วิธีใช้:
    1. เปิด DEBUG_ICONS = True แล้วรัน scrape
    2. หรือรัน debug_icons(df_products.to_dict('records')) หลัง scrape
    3. ดู domClass ที่มีเฉพาะใน LazMall แล้ว update LAZMALL_DOM_CLASSES
    """
    print(f"{'product_id':<15} {'is_mall':<10} {'shop_name':<25} {'dom_classes'}")
    print("-" * 80)
    for row in product_rows[:n]:
        icons_raw = row.get("icons_raw", [])
        if not icons_raw:
            # print(f"{str(row.get('product_id','')):<15} {'?':<10} {str(row.get('shop_name','')):<25} icons_raw ไม่มี → เปิด DEBUG_ICONS=True แล้วรัน scrape ใหม่")
            continue
        dom_classes = [str(i.get("domClass", "")) for i in icons_raw if isinstance(i, dict)]
        is_mall = row.get("is_mall", "?")
        print(f"{str(row.get('product_id','')):<15} {str(is_mall):<10} {str(row.get('shop_name','')):<25} {dom_classes}")

# ## Row Builders

def build_product_by_shop_row(product, shop_key):
    """สร้าง row สำหรับ Product by Shop"""
    # categories = product.get("categories", [])
    # categories_text = json.dumps(categories, ensure_ascii=False) if isinstance(categories, (list, dict)) else str(categories)
    discount_price = safe_float(product.get("price"))
    sold_count = parse_sold_count(product.get("itemSoldCntShow", "0"))
    row = {
        "product_id":      product.get("itemId", ""),
        "sku_id":          product.get("skuId", ""),
        "product_name":    product.get("name", ""),
        "brand_name":      clean_brand(product.get("brandName", "")),
        # "categories":    categories_text,
        "discount_price":  discount_price,
        "original_price":  safe_float(product.get("originalPrice")),
        "sold_count":      sold_count,
        # "sold_count_raw":  product.get("itemSoldCntShow", "0"),
        "rating_score":    safe_rating(product),
        "review_count":    safe_int(product.get("review")),
        "in_stock":        product.get("inStock", True),
        "is_sponsored":    product.get("isSponsored", False),
        "shop_id":         product.get("sellerId", ""),
        "shop_name":       product.get("sellerName", shop_key),
        "shop_key":        shop_key,
        "is_mall":         extract_is_mall(product),
        "location":        product.get("location", ""),
        "product_url":     clean_url(product.get("itemUrl", "")),
        "image_url":       clean_url(product.get("image", "")),
        "sales_values":    round(discount_price * sold_count, 2),
        "source_platform": "Lazada",
        "collected_at":    make_collected_at(),
    }
    # DEBUG: เก็บ icons_raw และ print ระหว่าง scrape
    if DEBUG_ICONS:
        icons_raw = product.get("icons", [])
        row["icons_raw"] = icons_raw
        dom_classes = [str(i.get("domClass", "")) for i in icons_raw if isinstance(i, dict)]
        print(f"      >> [DEBUG] {row['product_id']} | is_mall={row['is_mall']} | domClasses={dom_classes}")
    return row


def build_search_keyword_row(product, keyword):
    """สร้าง row สำหรับ Search Keyword"""
    discount_price = safe_float(product.get("price"))
    sold_count = parse_sold_count(product.get("itemSoldCntShow", "0"))
    return {
        "search_keyword":  keyword,
        "product_id":      product.get("itemId", ""),
        "sku_id":          product.get("skuId", ""),
        "product_name":    product.get("name", ""),
        "brand_name":      clean_brand(product.get("brandName", "")),
        "discount_price":  discount_price,
        "original_price":  safe_float(product.get("originalPrice")),
        "sold_count":      sold_count,
        "sold_count_raw":  product.get("itemSoldCntShow", "0"),
        "rating_score":    safe_rating(product),
        "review_count":    safe_int(product.get("review")),
        "is_sponsored":    product.get("isSponsored", False),
        "shop_id":         product.get("sellerId", ""),
        "shop_name":       product.get("sellerName", ""),
        "is_mall":         extract_is_mall(product),
        "location":        product.get("location", ""),
        "product_url":     clean_url(product.get("itemUrl", "")),
        "image_url":       clean_url(product.get("image", "")),
        "sales_values":    round(discount_price * sold_count, 2),
        "source_platform": "Lazada",
        "collected_at":    make_collected_at(),
    }


def build_review_row(r, product_id, product_name, shop_name, product_url):
    """สร้าง row สำหรับ Review"""
    review_time_raw = r.get("reviewTime") or ""
    return {
        "product_id":       product_id,
        "product_name":     product_name,
        "comment_url":      product_url,
        "user_name":        r.get("buyerName", ""),
        "rating_star":      safe_float(r.get("rating", "")) or None,
        "review_date_raw":  review_time_raw,
        "comment_date":     convert_time_interval(review_time_raw),
        "option":           r.get("skuInfo", ""),
        "comment":          r.get("reviewContent") or "",
        "shop_id":          r.get("sellerId", ""),
        "shop_name":        shop_name,
        "source":           "Lazada",
        "collected_at":     make_collected_at(),
    }


def build_product_detail_row(product_id, product_name, shop_name, product_url,
                              qual_data, specs_data, description_text, desc_images):
    """สร้าง row สำหรับ Product Detail"""
    return {
        "product_id":         product_id,
        "product_name":       product_name,
        "product_url":        product_url,
        "shop_name":          shop_name,
        "qualification_info": json.dumps(qual_data,   ensure_ascii=False),
        "all_specs":          json.dumps(specs_data,  ensure_ascii=False),
        "description":        description_text,
        "description_images": json.dumps(desc_images, ensure_ascii=False),
        "source_platform":    "Lazada",
        "collected_at":       make_collected_at(),
    }


# # Data Scrapers

# ## Product by Shop

# In[76]:


# In[ ]:


async def run_product_by_shop(shop_inputs):
    """ดึงรายการสินค้าจากหน้าร้าน Lazada ผ่าน ajax=true"""
    scraper_name = "product_by_shop"
    all_products = load_existing_backup("temp_lazada_products", PRODUCT_DEDUPE_COLS) if RESUME_FROM_BACKUP else []
    product_log_rows = []
    failed_shops = []
    seen_products = {
        (str(r.get("product_id","")), str(r.get("sku_id","")), str(r.get("product_url","")))
        for r in all_products
    }

    print(f"📂 [SESSION] Loading: '{USER_DATA_DIR}'")
    context, page = await create_browser_context(block_images=True)
    async with context:
        try:
            for idx, shop_input in enumerate(shop_inputs, start=1):
                shop_key = extract_shop_key_from_url(shop_input)
                print("\n" + "-" * 60)
                print(f"[{idx}/{len(shop_inputs)}] Shop: {shop_key} | URL: https://www.lazada.co.th/shop/{shop_key}/")
                if not shop_key:
                    failed_shops.append({"shop_input": shop_input, "error": "Cannot extract shop_key"})
                    add_scrape_log(product_log_rows, scraper_name, "ERROR", "Cannot extract shop_key", shop_input=shop_input)
                    continue

                shop_products = []
                base_url = (
                    f"https://www.lazada.co.th/{shop_key}/"
                    "?ajax=true"
                    "&from=wangpu"
                    "&isFirstRequest=true"
                    "&langFlag=th"
                    "&page=1"
                    "&pageTypeId=2"
                    "&q=All-Products"
                )

                try:
                    await _goto_with_network_retry(page, base_url)
                    raw = await handle_page_check(page, f"Shop Meta: {shop_key}")
                    data = safe_json_loads(raw)
                    if not data:
                        failed_shops.append({"shop_key": shop_key, "error": "Empty JSON at shop meta"})
                        add_scrape_log(product_log_rows, scraper_name, "ERROR", "Empty JSON at shop meta", shop_key=shop_key)
                        save_scrape_checkpoint(all_products, "temp_lazada_products", PRODUCT_DEDUPE_COLS, "shop meta error")
                        continue

                    main_info    = data.get("mainInfo", {}) or {}
                    total_results = safe_int(main_info.get("totalResults", 0))
                    page_size    = safe_int(main_info.get("pageSize", 40), default=40)
                    total_pages  = math.ceil(total_results / page_size) if total_results > 0 and page_size > 0 else 1
                    print(f"   > Total: {total_results:,} products | Pages: {total_pages:,}")

                    for page_no in range(1, total_pages + 1):
                        current_url = base_url.replace("page=1", f"page={page_no}")
                        await _goto_with_network_retry(page, current_url)
                        await human_delay()

                        page_raw  = await handle_page_check(page, f"{shop_key} | Page {page_no}")
                        page_json = safe_json_loads(page_raw)
                        if not page_json:
                            failed_shops.append({"shop_key": shop_key, "page_no": page_no, "error": "JSON Decode Error"})
                            add_scrape_log(product_log_rows, scraper_name, "ERROR", "JSON Decode Error", shop_key=shop_key, page_no=page_no)
                            save_scrape_checkpoint(all_products, "temp_lazada_products", PRODUCT_DEDUPE_COLS, "page json error")
                            break

                        items = (page_json.get("mods", {}).get("listItems", [])) or []
                        print(f"   > Page {page_no}/{total_pages}: {len(items)} products")
                        if not items:
                            add_scrape_log(product_log_rows, scraper_name, "WARNING", "No items", shop_key=shop_key, page_no=page_no)
                            break

                        new_rows = []
                        for product in items:
                            row = build_product_by_shop_row(product, shop_key)
                            key = (str(row.get("product_id","")), str(row.get("sku_id","")), str(row.get("product_url","")))
                            if key not in seen_products:
                                new_rows.append(row)
                                seen_products.add(key)
                        shop_products.extend(new_rows)
                        all_products.extend(new_rows)

                        if page_no % PRODUCT_CHECKPOINT_EVERY_PAGES == 0:
                            add_scrape_log(product_log_rows, scraper_name, "INFO", "Checkpoint saved", shop_key=shop_key, page_no=page_no, rows=len(all_products))
                            save_scrape_checkpoint(all_products, "temp_lazada_products", PRODUCT_DEDUPE_COLS, f"every {PRODUCT_CHECKPOINT_EVERY_PAGES} pages")

                    print(f"   >>> Collected {len(shop_products):,} new products from '{shop_key}'")

                except Exception as e:
                    print(f"❌ [SKIP] Error in shop '{shop_key}': {e}")
                    failed_shops.append({"shop_key": shop_key, "error": str(e)})
                    add_scrape_log(product_log_rows, scraper_name, "ERROR", "Error scraping shop", shop_key=shop_key, error=str(e))
                    save_scrape_checkpoint(all_products, "temp_lazada_products", PRODUCT_DEDUPE_COLS, "shop error")

        finally:
            save_scrape_checkpoint(all_products, "temp_lazada_products", PRODUCT_DEDUPE_COLS, "final backup before close")

    def _sanitize_shop_key(value):
        value = str(value or "").strip().lower()
        return re.sub(r"[^a-z0-9_\-]", "_", value)

    shop_keys = [_sanitize_shop_key(extract_shop_key_from_url(s)) for s in shop_inputs if extract_shop_key_from_url(s)]
    if len(shop_keys) == 1:
        final_prefix = f"step_2_lazada_product_{shop_keys[0]}"
    elif len(shop_keys) > 1:
        joined = "_".join(shop_keys)
        if len(joined) > 100:
            joined = "multiple_shops"
        final_prefix = f"step_2_lazada_product_{joined}"
    else:
        final_prefix = "step_2_lazada_product"

    df_products, _ = save_final_data(all_products, final_prefix, PRODUCT_DEDUPE_COLS)
    save_failed_log(failed_shops, "failed_shops", scraper_name)
    return df_products

# ## Product Detail

async def run_product_detail(product_urls=None, df_product=None):
    """ดึงรายละเอียดสินค้า Lazada (specs, qualification, description, images)

    Parameters
    ----------
    product_urls : list[str], optional
        Plain list of product URLs to scrape.
    df_product : pd.DataFrame, optional
        DataFrame that contains a 'product_url' column.  URLs are extracted
        from it and, after scraping, the new spec/description columns are
        merged back (left-join on 'product_url') so the caller gets one
        enriched DataFrame instead of two separate ones.

    Exactly one of the two parameters must be supplied.
    """
    # ── Resolve inputs ──────────────────────────────────────────
    if df_product is not None:
        if "product_url" not in df_product.columns:
            raise ValueError("df_product must contain a 'product_url' column")
        product_urls = df_product["product_url"].dropna().unique().tolist()
        print(f"ℹ️  df_product supplied — extracted {len(product_urls)} unique URL(s) from 'product_url' column")
    elif product_urls is None:
        raise ValueError("Provide either product_urls (list/str) or df_product (DataFrame)")
    elif isinstance(product_urls, str):
        product_urls = [product_urls]
    elif isinstance(product_urls, pd.Series):
        product_urls = product_urls.dropna().unique().tolist()
    elif isinstance(product_urls, tuple):
        product_urls = list(product_urls)
    elif not isinstance(product_urls, list):
        raise ValueError("product_urls must be a list, tuple, pandas Series, or single URL string")

    product_urls = [str(u).strip() for u in product_urls if u and str(u).strip()]
    if not product_urls:
        raise ValueError("No valid product URLs were provided")

    scraper_name = "product_detail"
    all_details = load_existing_backup("temp_lazada_details", DETAIL_DEDUPE_COLS) if RESUME_FROM_BACKUP else []
    detail_log_rows = []
    failed_details = []
    done_urls = {str(row.get("product_url", "")) for row in all_details if str(row.get("product_url", ""))}

    print(f"📂 [SESSION] Loading: '{USER_DATA_DIR}'")
    context, page = await create_browser_context(block_images=False)
    async with context:
        try:
            for idx, url in enumerate(product_urls, start=1):
                if url in done_urls:
                    print(f"   ⏭️ Skip already scraped: {url}")
                    continue

                print("\n" + "-" * 60)
                print(f"[{idx}/{len(product_urls)}] Product Detail: {url}")

                try:
                    await _goto_with_network_retry(page, url, timeout=90000)
                    await handle_page_check(page, f"Product Detail: {idx}")

                    try:
                        await page.wait_for_load_state("networkidle", timeout=90000)
                    except Exception as e:
                        add_scrape_log(detail_log_rows, scraper_name, "WARNING", "Networkidle timeout, continue scraping", product_url=url, warning=str(e))

                    product_id, product_name, shop_name = await get_product_info(page, url)
                    load_result = await wait_and_scroll_until_detail_loaded(page)

                    if not load_result["loaded"]:
                        add_scrape_log(detail_log_rows, scraper_name, "WARNING", "Detail content may not be fully loaded", product_url=url, product_id=product_id)

                    specs_data       = await extract_product_specs(page)
                    qual_data        = await extract_product_qualification(page)
                    description_text = await extract_product_description(page)
                    desc_images      = await extract_product_description_images(page)

                    print(f"   > ID: {product_id} | Specs: {len(specs_data)} | Qual: {len(qual_data)} | Images: {len(desc_images)} | Desc: {len(description_text)} chars")

                    if not any([specs_data, qual_data, description_text, desc_images]):
                        html = await page.content()
                        save_debug_html(html, "debug_empty_detail", product_id)
                        add_scrape_log(detail_log_rows, scraper_name, "WARNING", "Empty product detail content", product_url=url, product_id=product_id)

                    row = build_product_detail_row(
                        product_id=product_id, product_name=product_name,
                        shop_name=shop_name, product_url=url,
                        qual_data=qual_data, specs_data=specs_data,
                        description_text=description_text, desc_images=desc_images
                    )
                    all_details.append(row)
                    done_urls.add(url)

                    if idx % DETAIL_CHECKPOINT_EVERY_URLS == 0:
                        add_scrape_log(detail_log_rows, scraper_name, "INFO", "Checkpoint saved", url_index=idx, rows=len(all_details))
                        save_scrape_checkpoint(all_details, "temp_lazada_details", DETAIL_DEDUPE_COLS, f"every {DETAIL_CHECKPOINT_EVERY_URLS} urls")

                    await human_delay()

                except Exception as e:
                    print(f"   ❌ [ERROR] {e}")
                    failed_details.append({"product_url": url, "url_index": idx, "error": str(e)})
                    add_scrape_log(detail_log_rows, scraper_name, "ERROR", "Error scraping product detail", product_url=url, error=str(e))
                    save_scrape_checkpoint(all_details, "temp_lazada_details", DETAIL_DEDUPE_COLS, "product detail error")

        finally:
            save_scrape_checkpoint(all_details, "temp_lazada_details", DETAIL_DEDUPE_COLS, "final backup before close")

    # ── Expand all_specs JSON → individual columns (like Shopee's _product_description_to_df) ──
    # Internal rows keep all_specs as a JSON string for dedupe / resume / checkpointing.
    # Before saving to Excel we parse it back into one column per spec key so each
    # spec field gets its own column instead of a single JSON blob — matching the
    # output shape of Shopee's get_product_description / _product_description_to_df.
    if all_details:
        df_details_full = pd.DataFrame(all_details)
        if DETAIL_DEDUPE_COLS:
            valid = [c for c in DETAIL_DEDUPE_COLS if c in df_details_full.columns]
            if valid:
                df_details_full = df_details_full.drop_duplicates(subset=valid)

        # Parse all_specs JSON string → dict, then expand into individual columns
        specs_expanded = df_details_full["all_specs"].apply(
            lambda v: json.loads(v) if isinstance(v, str) and v.strip().startswith("{") else {}
        )
        specs_df = pd.DataFrame(specs_expanded.tolist(), index=df_details_full.index)

        # Build final output: identity cols + expanded spec cols + description cols
        identity_cols = ["product_id", "product_name", "shop_name", "product_url"]
        tail_cols     = ["qualification_info", "description", "description_images", "source_platform", "collected_at"]
        present_identity = [c for c in identity_cols if c in df_details_full.columns]
        present_tail     = [c for c in tail_cols     if c in df_details_full.columns]

        df_out = pd.concat(
            [df_details_full[present_identity].reset_index(drop=True),
             specs_df.reset_index(drop=True),
             df_details_full[present_tail].reset_index(drop=True)],
            axis=1,
        )
        shop_name = df_out["shop_name"].iloc[0] if "shop_name" in df_out.columns and not df_out["shop_name"].empty else "unknown_shop"
        path = Path(OUTPUT_DIR) / f"step_3_lazada_detail_{shop_name}_{make_timestamp()}.xlsx"
        df_out = _sanitize_for_excel(df_out)
        df_out.to_excel(path, index=False)
        print("\n" + "=" * 60)
        print(f"✅ FINAL FILE SAVED : lazada_detail")
        print(f"📊 Total Rows       : {len(df_out):,}")
        print(f"📋 Columns          : {df_out.columns.tolist()}")
        print(f"💾 File             : {path}")
        print("=" * 60)

        df_details = df_details_full  # return full internal df for caller
    else:
        df_details = pd.DataFrame()

    save_failed_log(failed_details, "failed_details", scraper_name)

    # ── Merge spec columns back into df_product when it was supplied ──────────
    # This gives the caller one enriched DataFrame (all original columns +
    # every spec column + qualification_info / description / description_images)
    # joined on product_url, matching the Shopee _product_description_to_df pattern.
    if df_product is not None and not df_details.empty:
        # Build the expanded spec DataFrame the same way as df_out above
        specs_expanded_merge = df_details["all_specs"].apply(
            lambda v: json.loads(v) if isinstance(v, str) and v.strip().startswith("{") else {}
        )
        specs_df_merge = pd.DataFrame(specs_expanded_merge.tolist(), index=df_details.index)

        detail_extra_cols = ["qualification_info", "description", "description_images",
                             "source_platform", "collected_at"]
        present_extra = [c for c in detail_extra_cols if c in df_details.columns]

        df_detail_flat = pd.concat(
            [df_details[["product_url"]].reset_index(drop=True),
             specs_df_merge.reset_index(drop=True),
             df_details[present_extra].reset_index(drop=True)],
            axis=1,
        )

        df_merged = df_product.merge(df_detail_flat, on="product_url", how="left")
        print(f"ℹ️  Merged detail columns back into df_product → {len(df_merged)} rows, {len(df_merged.columns)} columns")
        return df_merged

    return df_details

# ## Reviews
async def run_reviews(product_urls, max_pages_per_product=None):
    """ดึงข้อมูลรีวิวสินค้าจาก Lazada Review API

    max_pages_per_product : int, optional
        จำกัดจำนวนหน้ารีวิวต่อสินค้า (pageSize=50/หน้า) — สินค้าที่มีรีวิวหลักหมื่น
        (247+ หน้า) จะกินเวลาหลายสิบนาทีต่อชิ้นถ้าดึงจนครบ ถ้าต้องการแค่ตัวอย่าง
        รีวิวล่าสุดสำหรับวิเคราะห์ (ไม่ต้องครบทุกรีวิว) ให้ตั้งค่านี้ เช่น 10
        (= สูงสุด 500 รีวิวล่าสุดต่อสินค้า) ค่า default None = ดึงจนครบทุกหน้าเหมือนเดิม
    """
    scraper_name = "reviews"
    all_reviews = load_existing_backup("temp_lazada_reviews", REVIEW_DEDUPE_COLS) if RESUME_FROM_BACKUP else []
    review_log_rows = []
    failed_products = []
    seen_reviews = {
        (str(r.get("product_id","")), str(r.get("user_name","")),
         str(r.get("review_date_raw","")), str(r.get("comment","")))
        for r in all_reviews
    }

    print(f"📂 [SESSION] Loading: '{USER_DATA_DIR}'")
    context, page = await create_browser_context(block_images=True)
    async with context:
        try:
            for idx, url in enumerate(product_urls, start=1):
                print("\n" + "-" * 60)
                print(f"[{idx}/{len(product_urls)}] Review URL: {url}")
                product_reviews = []

                try:
                    await _goto_with_network_retry(page, url)
                    await handle_page_check(page, f"Product Page: {idx}")
                    product_id, product_name, shop_name = await get_product_info(page, url)
                    print(f"   > ID: {product_id} | Name: {product_name}")

                    if product_id == "unknown":
                        raise ValueError("Cannot extract product_id from URL")

                    page_no = 1
                    total_pages = None

                    while True:
                        review_api_url = (
                            "https://my.lazada.co.th/pdp/review/getReviewList"
                            f"?itemId={product_id}&pageSize=50&filter=0&sort=0&pageNo={page_no}"
                        )
                        await _goto_with_network_retry(page, review_api_url)
                        await human_delay()

                        raw  = await handle_page_check(page, f"Review API | Product {idx} | Page {page_no}")
                        data = safe_json_loads(raw)
                        if not data:
                            failed_products.append({"product_url": url, "product_id": product_id, "page_no": page_no, "error": "JSON Decode Error"})
                            add_scrape_log(review_log_rows, scraper_name, "ERROR", "JSON Decode Error at review page", product_id=product_id, page_no=page_no)
                            save_scrape_checkpoint(all_reviews, "temp_lazada_reviews", REVIEW_DEDUPE_COLS, "review json error")
                            break

                        model = data.get("model", {}) or {}
                        items = model.get("items", []) or []
                        if not items:
                            print(f"   > Page {page_no}: No more reviews.")
                            break

                        if total_pages is None:
                            review_count = safe_int(model.get("ratings", {}).get("reviewCount", 0))
                            if review_count > 0:
                                total_pages = math.ceil(review_count / 50)

                        new_reviews = []
                        for r in items:
                            row = build_review_row(r, product_id, product_name, shop_name, url)
                            if not row["comment"].strip():
                                continue
                            key = (str(row.get("product_id","")), str(row.get("user_name","")),
                                   str(row.get("review_date_raw","")), str(row.get("comment","")))
                            if key not in seen_reviews:
                                new_reviews.append(row)
                                seen_reviews.add(key)

                        product_reviews.extend(new_reviews)
                        all_reviews.extend(new_reviews)
                        print(f"   > Page {page_no}: {len(new_reviews)} reviews | Total: {len(product_reviews):,}")

                        if page_no % REVIEW_CHECKPOINT_EVERY_PAGES == 0:
                            add_scrape_log(review_log_rows, scraper_name, "INFO", "Checkpoint saved", product_id=product_id, page_no=page_no)
                            save_scrape_checkpoint(all_reviews, "temp_lazada_reviews", REVIEW_DEDUPE_COLS, f"every {REVIEW_CHECKPOINT_EVERY_PAGES} pages")

                        if total_pages is not None and page_no >= total_pages:
                            print("   > Reached last page. Stop.")
                            break
                        if max_pages_per_product is not None and page_no >= max_pages_per_product:
                            print(f"   > Reached max_pages_per_product cap ({max_pages_per_product}). Stop.")
                            break
                        page_no += 1

                    print(f"   >>> Collected {len(product_reviews):,} reviews for this product.")

                except Exception as e:
                    print(f"   ❌ [ERROR] {e}")
                    failed_products.append({"product_url": url, "error": str(e)})
                    add_scrape_log(review_log_rows, scraper_name, "ERROR", "Error scraping reviews", product_url=url, error=str(e))
                    save_scrape_checkpoint(all_reviews, "temp_lazada_reviews", REVIEW_DEDUPE_COLS, "review error")

        finally:
            save_scrape_checkpoint(all_reviews, "temp_lazada_reviews", REVIEW_DEDUPE_COLS, "final backup before close")

    REVIEW_OUTPUT_COLS = [
        "shop_name", "product_name", "comment_date", "option",
        "comment", "rating_star", "comment_url", "source", "collected_at",
    ]
    df_reviews, _ = save_final_data(all_reviews, "step_4_lazada_review", REVIEW_DEDUPE_COLS, output_cols=REVIEW_OUTPUT_COLS)
    save_failed_log(failed_products, "failed_reviews", scraper_name)
    return df_reviews[REVIEW_OUTPUT_COLS] if not df_reviews.empty else df_reviews


# ## Search Keyword
async def run_search_keyword(
    keywords,
    official_only=False,
    start_page_by_keyword=None,
    end_page_by_keyword=None
):
    """
    ดึงรายการสินค้าจากหน้า search Lazada ผ่าน ajax=true

    Optional resume:
    start_page_by_keyword={"อายไลเนอร์": 59}
    end_page_by_keyword={"อายไลเนอร์": 102}
    """
    scraper_name = "search_keyword"
    if start_page_by_keyword is None: 
        start_page_by_keyword = {}
    if end_page_by_keyword   is None: 
        end_page_by_keyword   = {}

    all_products = load_existing_backup("temp_lazada_search", SEARCH_DEDUPE_COLS) if RESUME_FROM_BACKUP else []
    search_log_rows  = []
    failed_keywords  = []
    seen_search_products = {
        (str(r.get("search_keyword","")), str(r.get("product_id","")),
         str(r.get("sku_id","")),         str(r.get("product_url","")))
        for r in all_products
    }

    print(f"📂 [SESSION] Loading: '{USER_DATA_DIR}'")
    context, page = await create_browser_context(block_images=True)
    async with context:
        try:
            for idx, keyword in enumerate(keywords, start=1):
                keyword_products  = []
                encoded_keyword   = quote_plus(keyword)
                official_param    = "&service=official" if official_only else ""
                print("\n" + "-" * 60)
                print(f"[{idx}/{len(keywords)}] Keyword: '{keyword}' | LazMall only: {official_only}")

                base_url = (
                    "https://www.lazada.co.th/catalog/"
                    f"?ajax=true&q={encoded_keyword}&page=1&sort=sold{official_param}"
                )

                try:
                    await _goto_with_network_retry(page, "https://www.lazada.co.th/")
                    await asyncio.sleep(1.5)
                    await _goto_with_network_retry(page, base_url)

                    raw  = await handle_page_check(page, f"Search Meta: {keyword}")
                    data = safe_json_loads(raw)
                    if not data:
                        failed_keywords.append({"keyword": keyword, "error": "Empty JSON at search meta"})
                        add_scrape_log(search_log_rows, scraper_name, "ERROR", "Empty JSON at search meta", keyword=keyword)
                        save_scrape_checkpoint(all_products, "temp_lazada_search", SEARCH_DEDUPE_COLS, "search meta error")
                        continue

                    main_info     = data.get("mainInfo", {}) or {}
                    total_results = safe_int(main_info.get("totalResults", 0))

                    # NOTE: ตอน production ใช้ total_pages จริง (comment บรรทัดด้านล่างออก)
                    page_size   = safe_int(main_info.get("pageSize", 40), default=40)
                    total_pages = math.ceil(total_results / page_size) if total_results > 0 and page_size > 0 else 1
                    # total_pages = 5  # TESTING LIMIT

                    start_page = int(start_page_by_keyword.get(keyword, 1))
                    end_page   = int(min(end_page_by_keyword.get(keyword, total_pages), total_pages))
                    print(f"   > Total: {total_results:,} items | Pages: {total_pages:,} | Scraping: {start_page}→{end_page}")

                    if start_page > end_page:
                        add_scrape_log(search_log_rows, scraper_name, "WARNING", "start_page > end_page, skip", keyword=keyword)
                        continue

                    for page_no in range(start_page, end_page + 1):
                        current_url = base_url.replace("page=1", f"page={page_no}")

                        # A slow/busy machine (e.g. other apps competing for CPU) can
                        # make a single page take longer than handle_page_check's
                        # timeout even with no real WAF/verification involved — that
                        # used to raise straight out to the keyword-level except
                        # below and abandon every remaining page for this keyword.
                        # Retry the page itself a couple of times first.
                        page_json = None
                        last_page_error = None
                        for page_attempt in range(1, 4):
                            try:
                                await page.goto(current_url, wait_until="domcontentloaded", timeout=60000)
                                await human_delay()
                                page_raw = await handle_page_check(page, f"Search: {keyword} | Page {page_no}")
                                page_json = safe_json_loads(page_raw)
                                if page_json:
                                    break
                                last_page_error = "JSON Decode Error"
                            except Exception as e:
                                last_page_error = str(e)
                            print(f"   ⚠️  Page {page_no} attempt {page_attempt}/3 failed ({last_page_error}) — retrying...")
                            await asyncio.sleep(random.uniform(3.0, 6.0))

                        if not page_json:
                            failed_keywords.append({"keyword": keyword, "page_no": page_no, "error": last_page_error or "JSON Decode Error"})
                            add_scrape_log(search_log_rows, scraper_name, "ERROR", "Search page failed after retries", keyword=keyword, page_no=page_no, error=last_page_error)
                            save_scrape_checkpoint(all_products, "temp_lazada_search", SEARCH_DEDUPE_COLS, "search page error after retries")
                            break

                        items = (page_json.get("mods", {}).get("listItems", [])) or []
                        print(f"   > Page {page_no}/{end_page}: {len(items)} items")
                        if not items:
                            add_scrape_log(search_log_rows, scraper_name, "WARNING", "No items at search page", keyword=keyword, page_no=page_no)
                            break

                        new_rows = []
                        for product in items:
                            row = build_search_keyword_row(product, keyword)
                            key = (str(row.get("search_keyword","")), str(row.get("product_id","")),
                                   str(row.get("sku_id","")),         str(row.get("product_url","")))
                            if key not in seen_search_products:
                                new_rows.append(row)
                                seen_search_products.add(key)

                        keyword_products.extend(new_rows)
                        all_products.extend(new_rows)

                        pages_done = page_no - start_page + 1
                        if pages_done % SEARCH_CHECKPOINT_EVERY_PAGES == 0 or page_no == end_page:
                            add_scrape_log(search_log_rows, scraper_name, "INFO", "Checkpoint saved", keyword=keyword, page_no=page_no)
                            save_scrape_checkpoint(all_products, "temp_lazada_search", SEARCH_DEDUPE_COLS, f"checkpoint at page {page_no}")

                    print(f"   >>> Collected {len(keyword_products):,} new items for '{keyword}'.")

                except Exception as e:
                    print(f"❌ [SKIP] Error in keyword '{keyword}': {e}")
                    failed_keywords.append({"keyword": keyword, "error": str(e)})
                    add_scrape_log(search_log_rows, scraper_name, "ERROR", "Error scraping keyword", keyword=keyword, error=str(e))
                    save_scrape_checkpoint(all_products, "temp_lazada_search", SEARCH_DEDUPE_COLS, "keyword error")

        finally:
            save_scrape_checkpoint(all_products, "temp_lazada_search", SEARCH_DEDUPE_COLS, "final backup before close")

    # `all_products` / the temp checkpoint is intentionally cumulative across every
    # keyword ever scraped with RESUME_FROM_BACKUP=True (so an interrupted run of
    # THIS keyword batch can resume). But that means it also carries older keyword
    # batches from previous runs — filter the final deliverable down to just the
    # keyword(s) requested in this call so each run's Excel only has its own data.
    current_keywords = set(keywords)
    final_rows = [r for r in all_products if str(r.get("search_keyword", "")) in current_keywords]
    df_search, _ = save_final_data(final_rows, "step_1_lazada_search", SEARCH_DEDUPE_COLS)
    save_failed_log(failed_keywords, "failed_keywords", scraper_name)
    return df_search