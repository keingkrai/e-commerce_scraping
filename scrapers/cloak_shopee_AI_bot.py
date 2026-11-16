import asyncio
import sys
import random
import json
import re
import math
import base64
import io
import pandas as pd
import requests
import numpy as np
from openai import OpenAI
import os
from dotenv import load_dotenv

load_dotenv()
# Optional: PIL for local CV-based captcha solving (faster, free, no API cost)
try:
    from PIL import Image
    _PIL_AVAILABLE = True
except ImportError:
    _PIL_AVAILABLE = False
    print("⚠️  Pillow not installed. Local CV captcha solver disabled. Run: pip install Pillow numpy")

from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import List, Optional
from urllib.parse import quote, quote_plus
from cloakbrowser import launch_persistent_context_async
from pydantic import BaseModel, Field
API_KEY =  os.environ.get("OPEN_ROUTER_API") or os.environ.get("OPENROUTER_API_KEY", "")

# Windows-specific fix to prevent "I/O operation on closed pipe" tracebacks
if sys.platform == 'win32':
    import functools
    import asyncio.proactor_events
    import asyncio.base_subprocess
    
    # Silence the warning
    def silence_event_loop_closed(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except Exception:
                pass
        return wrapper
        
    asyncio.proactor_events._ProactorBasePipeTransport.__del__ = silence_event_loop_closed(asyncio.proactor_events._ProactorBasePipeTransport.__del__)
    asyncio.base_subprocess.BaseSubprocessTransport.__del__ = silence_event_loop_closed(asyncio.base_subprocess.BaseSubprocessTransport.__del__)
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# ════════════════════════════════════════════════════════════════
# Directories & Config
# ════════════════════════════════════════════════════════════════

USER_DATA_DIR = "./data/shopee/my_session"
OUTPUT_DIR    = "./data/shopee/collected_data"
TEMP_DIR      = "./data/shopee/collected_data/temp"

LOG_DIR         = "./data/shopee/log"
SHOP_LOG_DIR    = "./data/shopee/log/shop"
DETAIL_LOG_DIR  = "./data/shopee/log/detail"
REVIEW_LOG_DIR  = "./data/shopee/log/review"
SEARCH_LOG_DIR  = "./data/shopee/log/search"
CAPTCHA_LOG_DIR = "./data/shopee/log/captcha"

REQUEST_DELAY_MIN = 2.5
REQUEST_DELAY_MAX = 5.0

# Cool-down: พักยาวเป็นระยะๆ ระหว่างดึงข้อมูล เพื่อตัด pattern การยิง request
# ถี่สม่ำเสมอ (สาเหตุหลักที่โดนบอทตรวจจับตอนดึงข้อมูลจำนวนมาก)
SHORT_COOLDOWN_EVERY = 25            # พักสั้นทุกๆ N page/item
SHORT_COOLDOWN_RANGE = (45, 90)      # วินาที
LONG_COOLDOWN_EVERY  = 200           # พักยาวทุกๆ N page/item
LONG_COOLDOWN_RANGE  = (300, 600)    # วินาที (5-10 นาที)

# True: โหลด temp เดิมกลับมาต่อ | False: เริ่มใหม่
RESUME_FROM_BACKUP = True

# Checkpoint frequency
SHOP_CHECKPOINT_EVERY_PAGES    = 10
DETAIL_CHECKPOINT_EVERY_URLS   = 20
REVIEW_CHECKPOINT_EVERY_PAGES  = 20
SEARCH_CHECKPOINT_EVERY_PAGES  = 10

for folder in [
    USER_DATA_DIR, OUTPUT_DIR, TEMP_DIR,
    LOG_DIR, SHOP_LOG_DIR, DETAIL_LOG_DIR,
    REVIEW_LOG_DIR, SEARCH_LOG_DIR, CAPTCHA_LOG_DIR,
]:
    Path(folder).mkdir(parents=True, exist_ok=True)

print(f"🌐 Browser session folder : {USER_DATA_DIR}")
print(f"📂 Output folder          : {OUTPUT_DIR}")
print(f"💾 Temp checkpoint folder : {TEMP_DIR}")
print(f"🧾 Log folder             : {LOG_DIR}")

TZ_THAI = timezone(timedelta(hours=7))

SHOP_DEDUPE_COLS   = ["itemid", "shopid", "product_url"]
DETAIL_DEDUPE_COLS = ["product_url"]
REVIEW_DEDUPE_COLS = ["product_url", "user_name", "comment_date", "comment"]
SEARCH_DEDUPE_COLS = ["search_keyword", "itemid", "shopid", "product_url"]

# Module-level overflow flag
with_comments_count_overflow: bool = False

# ════════════════════════════════════════════════════════════════
# Pydantic Models 
# ════════════════════════════════════════════════════════════════

class ProductModel(BaseModel):
    itemid: int
    shopid: int
    name: str
    brand: str = ""
    shop_name: str = ""
    shop_location: str = ""
    product_url: str = ""
    catid: int = 0
    price: float
    price_original: float
    price_min: float = 0.0
    price_max: float = 0.0
    discount_pct: int = 0
    promotion_type: Optional[int] = None
    bundle_deal_label: Optional[str] = None
    is_on_flash_sale: bool = False
    is_lowest_price: bool = False
    sold_monthly: int = 0
    sold_total: int = 0
    liked_count: int = 0
    view_count: Optional[int] = 0
    stock: int = 0
    ctime: datetime
    product_age_days: int = 0
    rating_star: float = 0.0
    rating_count: int = 0
    rating_with_text: int = 0
    rating_with_image: int = 0
    comment_count: int = 0
    rating_1star: int = 0
    rating_2star: int = 0
    rating_3star: int = 0
    rating_4star: int = 0
    rating_5star: int = 0
    is_official_shop: bool = False
    is_shopee_verified: bool = False
    shopee_verified: bool = False
    is_preferred_plus_seller: bool = False
    can_use_cod: bool = False
    is_mart: bool = False
    can_use_wholesale: bool = False
    is_ads: bool = False
    traffic_source: int = 0
    matched_keywords: List[str] = Field(default_factory=list)
    item_identities: List[str] = Field(default_factory=list)
    label_ids: List[int] = Field(default_factory=list)
    image_url: str = ""
    image_urls: List[str] = Field(default_factory=list)
    has_video: bool = False
    video_url: Optional[str] = None
    tier_variations: List[dict] = Field(default_factory=list)
    item_config: dict = Field(default_factory=dict)
    source_platform: str = "Shopee"
    collected_at: str = Field(default_factory=lambda: datetime.now(tz=TZ_THAI).strftime("%Y-%m-%d %H:%M:%S"))

class ReviewModel(BaseModel):
    product_url: str = ""
    user_name: str = ""
    comment_date: str = ""
    option: str = ""
    variation: str = ""
    comment: str = ""
    rating_star: int = 0
    source: str = "Shopee"
    collected_at: str = Field(default_factory=lambda: datetime.now(tz=TZ_THAI).strftime("%Y-%m-%d %H:%M:%S"))

# ════════════════════════════════════════════════════════════════
# General Helpers
# ════════════════════════════════════════════════════════════════
def make_timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def make_collected_at() -> str:
    return datetime.now(tz=TZ_THAI).strftime("%Y-%m-%d")

def safe_float(val, default=0.0) -> float:
    try:
        if val is None or str(val).strip() == "":
            return default
        return float(re.sub(r"[,\s฿]", "", str(val)).replace("THB", ""))
    except Exception:
        return default

def safe_int(val, default=0) -> int:
    try:
        return int(safe_float(val, default=None) or default)
    except Exception:
        return default

def parse_sold_count(text: str) -> int:
    if not text:
        return 0
    raw = str(text).strip()

    # Strip Thai sold prefixes: "ขายได้", "ขายแล้ว" and suffixes: "ชิ้น/เดือน", "ชิ้น"
    raw = re.sub(r'^(ขายได้|ขายแล้ว)\s*', '', raw)
    raw = re.sub(r'\s*(ชิ้น/เดือน|ชิ้น|/เดือน).*$', '', raw).strip()

    # Extract the number part (may be "1พัน+" → "1" + "พัน" or "8,500")
    m = re.search(r"([\d,\.]+)", raw)
    if not m:
        return 0

    num = float(m.group(1).replace(",", ""))

    # Check for Thai suffixes (case-sensitive; before lowercasing)
    if "พัน" in raw:  # Thai for "thousand"
        num *= 1_000
    elif "แสน" in raw:  # Thai for "hundred thousand"
        num *= 100_000
    elif "ล้าน" in raw:  # Thai for "million"
        num *= 1_000_000
    else:
        # Check for English suffixes (case-insensitive)
        raw_lower = raw.lower()
        if "k" in raw_lower:
            num *= 1_000
        elif "m" in raw_lower:
            num *= 1_000_000

    result = int(num)
    return result

# ════════════════════════════════════════════════════════════════
# Log & Checkpoint Helpers 
# ════════════════════════════════════════════════════════════════
def _log_dir_for(scraper_name: str) -> Path:
    mapping = {
        "shop": SHOP_LOG_DIR, "shop_products": SHOP_LOG_DIR,
        "detail": DETAIL_LOG_DIR, "product_detail": DETAIL_LOG_DIR,
        "review": REVIEW_LOG_DIR, "reviews": REVIEW_LOG_DIR,
        "search": SEARCH_LOG_DIR, "search_keyword": SEARCH_LOG_DIR,
    }
    return Path(mapping.get(scraper_name, LOG_DIR))

def append_text_log(scraper_name: str, level: str, message: str, context: dict = None):
    log_path = _log_dir_for(scraper_name) / f"{scraper_name}_scraper.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    context = context or {}
    line = f"{make_collected_at()} | {level.upper()} | {scraper_name} | {message} | {json.dumps(context, ensure_ascii=False, default=str)}\n"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(line)

def append_error_jsonl(scraper_name: str, record: dict):
    error_path = _log_dir_for(scraper_name) / f"{scraper_name}_error.jsonl"
    error_path.parent.mkdir(parents=True, exist_ok=True)
    with open(error_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

def add_scrape_log(log_rows: list, scraper_name: str, level: str, message: str, **context) -> dict:
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

def save_scrape_checkpoint(rows: list, data_prefix: str, dedupe_cols: list = None,
                            reason: str = "checkpoint", overwrite_latest: bool = True) -> dict:
    Path(TEMP_DIR).mkdir(parents=True, exist_ok=True)
    if not rows:
        print(f"   ⚠️ No data at checkpoint: {reason}")
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
    otherwise) — real artifacts occasionally present in scraped shop/product
    text, not a scraping bug."""
    from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
    str_cols = df.select_dtypes(include="object").columns
    for col in str_cols:
        df[col] = df[col].map(lambda v: ILLEGAL_CHARACTERS_RE.sub("", v) if isinstance(v, str) else v)
    return df

def save_final_data(rows: list, final_prefix: str, dedupe_cols: list = None,
                    output_cols=None) -> tuple:
    if not rows:
        print(f"   ⚠️ No data to save: {final_prefix}")
        return pd.DataFrame(), None
    df = pd.DataFrame(rows)
    if dedupe_cols:
        valid = [c for c in dedupe_cols if c in df.columns]
        if valid:
            df = df.drop_duplicates(subset=valid)
    df_out = df.copy()
    if output_cols is not None:
        if isinstance(output_cols, dict):
            valid_keys = [k for k in output_cols if k in df_out.columns]
            df_out = df_out[valid_keys].rename(columns=output_cols)
        elif isinstance(output_cols, list):
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
    return df, path

def save_reviews_per_url(rows: list, url: str, dedupe_cols: list = None,
                          output_cols: list = None, sub_dir: str = "per_url_reviews") -> None:
    """
    Save reviews for a single product URL to its own Excel file.

    File name is derived from the Shopee item-id embedded in the URL so
    it is both human-readable and collision-free:
        shopee_reviews_<shop_id>_<item_id>_<timestamp>.xlsx

    Falls back to a sanitised URL slug if the numeric IDs can't be parsed.
    """
    if not rows:
        return
    import re as _re

    # Extract shop_id / item_id from Shopee URL pattern  i.<shop_id>.<item_id>
    m = _re.search(r"i\.([0-9]+)\.([0-9]+)", url)
    if m:
        file_stem = f"shopee_reviews_{m.group(1)}_{m.group(2)}"
    else:
        # Fallback: take last path segment, strip query string, keep first 60 chars
        slug = url.split("?")[0].rstrip("/").split("/")[-1]
        slug = _re.sub(r"[^\w\-]", "_", slug)[:60]
        file_stem = f"shopee_reviews_{slug}"

    ts = make_timestamp()
    out_dir = Path(OUTPUT_DIR) / sub_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{file_stem}_{ts}.xlsx"

    df = pd.DataFrame(rows)
    if dedupe_cols:
        valid = [c for c in dedupe_cols if c in df.columns]
        if valid:
            df = df.drop_duplicates(subset=valid)
    if output_cols:
        valid_cols = [c for c in output_cols if c in df.columns]
        df = df[valid_cols]

    df = _sanitize_for_excel(df)
    df.to_excel(path, index=False)
    print(f"   📥 Per-URL Excel saved : {path}  ({len(df):,} rows)")


def save_pagination_report(pages_scraped: dict, report_prefix: str) -> None:
    """
    Save a pagination completion report to Excel.

    For every URL (or URL|⭐star key) that was tracked, records:
      - product_url
      - star_filter     (if applicable)
      - pages_scraped   (how many pages were collected)
      - pages_total     (how many pages existed)
      - completed       (True if pages_scraped == pages_total)
      - reviews_missed  (estimated = (pages_total - pages_scraped) * 6)

    Only entries that did NOT complete all pages are marked incomplete;
    full-completion entries are still included for a complete audit trail.
    """
    if not pages_scraped:
        return

    rows = []
    for key, (done, total) in pages_scraped.items():
        if "|⭐" in key:
            url, star_part = key.split("|⭐", 1)
            star_filter = f"⭐{star_part}"
        else:
            url = key
            star_filter = "all (with comments)"

        completed = done >= total
        rows.append({
            "product_url":    url,
            "star_filter":    star_filter,
            "pages_scraped":  done,
            "pages_total":    total,
            "completed":      completed,
            "reviews_missed": max(0, (total - done) * 6),
        })

    df = pd.DataFrame(rows)
    df.sort_values(["completed", "reviews_missed"], ascending=[True, False], inplace=True)

    ts = make_timestamp()
    out_dir = Path(OUTPUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{report_prefix}_{ts}.xlsx"

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="All URLs")

        incomplete = df[~df["completed"]]
        if not incomplete.empty:
            incomplete.to_excel(writer, index=False, sheet_name="Incomplete Only")

    incomplete_count = (~df["completed"]).sum()
    print(f"\n📊 Pagination report saved: {path}")
    print(f"   Total URLs tracked : {len(df)}")
    print(f"   Completed          : {df['completed'].sum()}")
    print(f"   Incomplete         : {incomplete_count}")
    if incomplete_count > 0:
        print(f"   Est. reviews missed: {incomplete['reviews_missed'].sum():,}")


def save_failed_log(failed_rows: list, prefix: str, scraper_name: str = "general"):
    if not failed_rows:
        return None
    error_path = _log_dir_for(scraper_name) / f"{scraper_name}_error.jsonl"
    error_path.parent.mkdir(parents=True, exist_ok=True)
    with open(error_path, "a", encoding="utf-8") as f:
        for row in failed_rows:
            f.write(json.dumps({"logged_at": make_collected_at(), "type": prefix, **row},
                                ensure_ascii=False, default=str) + "\n")
    print(f"⚠️ Failed log saved: {error_path}")
    return error_path

def load_existing_backup(data_prefix: str, dedupe_cols: list = None) -> list:
    csv_path  = Path(TEMP_DIR) / f"{data_prefix}.csv"
    xlsx_path = Path(TEMP_DIR) / f"{data_prefix}.xlsx"
    if csv_path.exists():
        backup_path, reader = csv_path, pd.read_csv
    elif xlsx_path.exists():
        backup_path, reader = xlsx_path, pd.read_excel
    else:
        print(f"ℹ️ No existing temp backup: {data_prefix}")
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

def load_done_keys(data_prefix: str) -> set:
    """
    โหลด key ที่ scrape 'จบสมบูรณ์' แล้ว (เช่น product_url ที่ดึงรีวิวครบทุกหน้าแล้ว)
    ต่างจาก load_existing_backup ตรงที่ backup เก็บ 'ข้อมูลที่ได้แล้ว' (ใช้ dedupe ค่ารายแถว)
    ส่วนไฟล์นี้เก็บ 'สถานะความสมบูรณ์ต่อ URL' เพื่อให้ resume แล้วข้าม URL ที่จบแล้วได้ทันที
    แทนที่จะต้องเปิดหน้าใหม่ + ไล่ pagination ซ้ำทุกหน้าเหมือนเริ่มใหม่หมด
    """
    path = Path(TEMP_DIR) / f"{data_prefix}_done.json"
    if not path.exists():
        return set()
    try:
        keys = set(json.loads(path.read_text(encoding="utf-8")))
        print(f"🔁 Done-keys loaded: {path} | count={len(keys):,}")
        return keys
    except Exception as e:
        print(f"⚠️ Failed to load done-keys: {path} | error={e}")
        return set()

def save_done_keys(data_prefix: str, keys: set) -> None:
    """บันทึก key ที่ scrape จบสมบูรณ์แล้วลง collected_data/temp/*_done.json"""
    Path(TEMP_DIR).mkdir(parents=True, exist_ok=True)
    path = Path(TEMP_DIR) / f"{data_prefix}_done.json"
    path.write_text(json.dumps(sorted(keys), ensure_ascii=False, indent=2), encoding="utf-8")

# ════════════════════════════════════════════════════════════════
# CloakBrowser Context
# ════════════════════════════════════════════════════════════════
async def create_browser_context(block_images: bool = False):
    """เปิด CloakBrowser แบบ persistent context เพื่อเก็บ session/cookies"""
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
        async def _block(route):
            await route.abort()
        await page.route("**/*.{png,jpg,jpeg,webp,gif,svg}", _block)
    return context, page

async def human_delay(min_delay: float = REQUEST_DELAY_MIN, max_delay: float = REQUEST_DELAY_MAX):
    await asyncio.sleep(random.uniform(min_delay, max_delay))

_request_counter = {"n": 0}

async def cooldown_tick():
    """
    เรียกครั้งเดียวต่อ page/item ที่ scrape เสร็จ (ทุก scraper เรียก counter ร่วมกัน)
    เพื่อแทรกจังหวะพักยาวเป็นระยะๆ (short/long) ตัด pattern การยิง request ถี่สม่ำเสมอ
    ที่ระบบตรวจจับบอทมักจับได้ง่าย โดยเฉพาะตอนดึงข้อมูลจำนวนมาก (หลักพัน product/review)
    """
    _request_counter["n"] += 1
    n = _request_counter["n"]
    if n % LONG_COOLDOWN_EVERY == 0:
        pause = random.uniform(*LONG_COOLDOWN_RANGE)
        print(f"   🧊 [LONG COOLDOWN] รวม {n} pages/items — พัก {pause/60:.1f} นาที ก่อนไปต่อ...")
        await asyncio.sleep(pause)
    elif n % SHORT_COOLDOWN_EVERY == 0:
        pause = random.uniform(*SHORT_COOLDOWN_RANGE)
        print(f"   🧊 [COOLDOWN] รวม {n} pages/items — พัก {pause:.0f}s ก่อนไปต่อ...")
        await asyncio.sleep(pause)

async def log_captcha_encounter(page, label: str):
    ts = make_timestamp()
    safe_label = re.sub(r"[^\w\-]", "_", label)[:60]
    base = Path(CAPTCHA_LOG_DIR) / f"{ts}_{safe_label}"
    try:
        html = await page.content()
        base.with_suffix(".html").write_text(html, encoding="utf-8")
    except Exception as e:
        print(f"   ⚠️ [CAPTCHA LOG] Could not save HTML: {e}")
    try:
        await page.screenshot(path=str(base.with_suffix(".png")), full_page=False)
    except Exception as e:
        print(f"   ⚠️ [CAPTCHA LOG] Could not save screenshot: {e}")
    meta = {
        "logged_at": make_collected_at(), "label": label,
        "url": page.url,
    }
    base.with_suffix(".json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"   📸 [CAPTCHA LOG] Saved: {base}.{{html,png,json}}")

# ════════════════════════════════════════════════════════════════
# Captcha / Anti-Bot Helpers
# ════════════════════════════════════════════════════════════════
async def _is_captcha_detected(page) -> bool:
    try:
        url_lower = page.url.lower()
        if any(kw in url_lower for kw in ["/verify", "/challenge", "captcha=", "blocked"]):
            return True
        hard_selectors = [
            "iframe[src*='captcha']", "iframe[src*='recaptcha']", "iframe[src*='hcaptcha']",
            ".shopee-captcha", "#captcha-verify", "[class*='captcha-dialog']",
            "[id*='captcha-container']", "div.verify-wrap", "div.captcha__image",
        ]
        for sel in hard_selectors:
            try:
                el = await page.query_selector(sel)
                if el and await el.is_visible():
                    return True
            except Exception:
                continue
        title = (await page.title()).lower()
        if any(kw in title for kw in ["verify", "captcha", "security check", "access denied"]):
            return True
        try:
            body_text = await page.locator("body").inner_text(timeout=3000)
            body_lower = body_text.lower().strip()
            captcha_phrases = [
                "please complete the security check", "security verification",
                "slide to verify", "เลื่อนเพื่อยืนยัน", "กรุณายืนยันตัวตนของคุณ",
            ]
            if any(phrase in body_lower for phrase in captcha_phrases) and len(body_text) < 3000:
                return True
            # Network/IP-reputation block — harsher than a per-page captcha (whole
            # connection flagged, not just this request). This can appear as a modal
            # on top of a normal, long product/shop page, so no length gate here.
            network_block_phrases = [
                "การเข้าชมที่ผิดปกติ", "เครือข่ายของคุณ", "ลองใหม่อีกครั้งในภายหลัง",
                "unusual traffic", "unusual activity", "traffic from your network",
            ]
            if any(phrase in body_lower for phrase in network_block_phrases):
                return True
        except Exception:
            pass
    except Exception:
        pass
    return False

_OPENROUTER_CLIENT = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=API_KEY,
)
_OPENROUTER_EXTRA_HEADERS = {
    "HTTP-Referer": "http://localhost:3000",
    "X-Title": "GIC Dept-Wide Auditor & Orchestrator",
    "X-OpenRouter-Extra-Headers": '{"provider": {"data_collection": "deny"}}',
}

# ── Vision model (fallback when local CV can't find gap) ──────────
# Only used if Pillow/numpy are unavailable OR local CV confidence is low.
_CAPTCHA_MODEL = "google/gemini-2.5-flash-image"   # fallback: "openai/gpt-4o"

# ════════════════════════════════════════════════════════════════
# Captcha DOM constants — derived from live HTML analysis
# ════════════════════════════════════════════════════════════════
#
# Shopee's slider captcha DOM layout (confirmed from HTML inspection):
#
#   #NEW_CAPTCHA
#    └─ div._3IQdy6  (aria-label="แก้ Captcha …")
#        ├─ div.kzPOuc  style="width:280px; height:150px"
#        │    └─ <img class="DfwepB">       ← background image (280×150 JPEG)
#        ├─ div (absolute overlay)
#        │    └─ #puzzleContainer  style="transform:translateX(?px) translateY(?px)"
#        │         └─ #puzzleImgComponent   ← puzzle PIECE image (103×103 RGBA PNG)
#        └─ div.DgnNfM  style="width:280px"  ← slider TRACK
#             ├─ div.PEG3Mo  style="width:44px; transform:scaleX(?)"  ← filled bar
#             └─ #sliderContainer  style="width:40px;height:40px;transform:translateX(?px)"
#                  └─ (drag handle — arrow icon)
#
# Key facts:
#   • Background is always 280×150 px.
#   • Puzzle piece is always 103×103 px RGBA; opaque area ~87×65 px.
#   • Slider handle (#sliderContainer) starts at translateX(0) → left edge of track.
#   • drag_distance = gap_left_x  (left edge of where piece fits in BG).
#   • After solving: #sliderContainer translateX == gap_left_x.
#   • The "refresh" button has aria-label="ดูรูปภาพใหม่" (class XAny99).
#
_CAPTCHA_ROOT_SELECTORS = [
    "#NEW_CAPTCHA",
    "#captcha-verify",
    "#shopee-captcha-root",
    ".shopee-captcha",
    "div.captcha__image",
    "div.verify-wrap",
    "[class*='captcha-dialog']",
    "[id*='captcha-container']",
]

_SLIDER_KEYWORDS = ["slide", "drag", "slider", "puzzle", "jigsaw", "เลื่อน", "ลาก"]
_ROTATE_KEYWORDS = ["rotat", "turn", "หมุน"]
_CLICK_KEYWORDS  = ["click", "select", "tap", "กด", "เลือก"]


# ════════════════════════════════════════════════════════════════
# Local CV solver  (Pillow + numpy, no API cost, fast & accurate)
# ════════════════════════════════════════════════════════════════

def _cv_find_gap_x(bg_b64: str, piece_b64: str) -> int | None:
    """
    Given base64-encoded background (JPEG 280×150) and puzzle piece (PNG 103×103 RGBA),
    return the LEFT-EDGE x-coordinate of where the piece fits in the background.

    Strategy:
      1. Build an outline mask from the piece's alpha channel (border pixels of shape).
      2. Slide this outline mask over the background's edge map.
      3. The position with maximum edge-correlation is the gap.

    Returns None if Pillow/numpy are unavailable or any error occurs.
    """
    if not _PIL_AVAILABLE:
        return None
    try:
        bg_img   = Image.open(io.BytesIO(base64.b64decode(bg_b64))).convert('RGB')
        piece_img = Image.open(io.BytesIO(base64.b64decode(piece_b64))).convert('RGBA')

        bw, bh = bg_img.size      # 280 × 150
        pw, ph = piece_img.size   # 103 × 103

        # ── Background grayscale & edge map ───────────────────────
        bg_gray = np.array(bg_img.convert('L'), dtype=np.float32)

        # Simple Sobel-like edge magnitude using numpy (no scipy needed)
        ky = np.array([[-1,0,1],[-2,0,2],[-1,0,1]], dtype=np.float32)
        kx = ky.T
        from numpy.lib.stride_tricks import sliding_window_view
        # Pad by 1
        bg_pad = np.pad(bg_gray, 1, mode='edge')
        # Manual convolution via sliding window for the 3×3 Sobel
        windows = sliding_window_view(bg_pad, (3, 3))  # shape: (H, W, 3, 3)
        gx = (windows * kx).sum(axis=(-1, -2))
        gy = (windows * ky).sum(axis=(-1, -2))
        bg_edges = np.hypot(gx, gy).astype(np.float32)

        # ── Piece outline mask ─────────────────────────────────────
        piece_alpha = np.array(piece_img)[:, :, 3].astype(np.float32)
        mask = (piece_alpha > 50).astype(np.float32)

        # Outline = dilation − original  (1-px border of the opaque shape)
        # Simple max-pool dilation with a 3×3 kernel
        from numpy.lib.stride_tricks import sliding_window_view as swv
        mask_pad = np.pad(mask, 1, mode='constant', constant_values=0)
        dilated  = swv(mask_pad, (3, 3)).max(axis=(-1, -2))
        outline  = (dilated - mask).clip(0, 1)

        outline_sum = outline.sum()
        if outline_sum == 0:
            return None

        # ── Slide outline over BG edges ───────────────────────────
        max_x  = bw - pw      # last valid left edge
        max_y  = bh - ph      # last valid top edge

        best_score = -1.0
        best_x     = 0

        # We know the piece Y comes from translateY in the DOM, but we don't have
        # that value here — scan a Y range instead (usually 20-80px from top).
        y_search_min = max(0,  20)
        y_search_max = min(max_y, bh - ph)

        for y in range(y_search_min, y_search_max + 1, 2):
            for x in range(0, max_x + 1, 1):
                region = bg_edges[y: y + ph, x: x + pw]
                if region.shape != outline.shape:
                    continue
                score = float((region * outline).sum()) / outline_sum
                if score > best_score:
                    best_score = score
                    best_x = x

        print(f"   🔬 [CV] gap_left_x={best_x}  (edge-correlation score={best_score:.2f})")
        return best_x

    except Exception as e:
        print(f"   ⚠️ [CV] Local solver error: {e}")
        return None


async def _extract_captcha_images(page) -> tuple[str | None, str | None]:
    """
    Extract base64 strings for the background image and puzzle piece from the DOM.
    Returns (bg_b64, piece_b64) or (None, None) on failure.

    Uses the confirmed selectors from HTML analysis:
      Background : img.DfwepB  (src="data:image/…;base64,…")
      Piece      : #puzzleImgComponent (src="data:image/…;base64,…")
    """
    try:
        result = await page.evaluate("""() => {
            // Background image
            let bgB64 = null;
            const bgImg = document.querySelector('img.DfwepB') ||
                          document.querySelector('#puzzleContainer img') ||
                          document.querySelector('#NEW_CAPTCHA img:not(#puzzleImgComponent)');
            if (bgImg && bgImg.src && bgImg.src.startsWith('data:')) {
                bgB64 = bgImg.src.split(',')[1];
            }

            // Puzzle piece
            let pieceB64 = null;
            const pieceImg = document.querySelector('#puzzleImgComponent') ||
                             document.querySelector('#puzzleContainer img');
            if (pieceImg && pieceImg.src && pieceImg.src.startsWith('data:')) {
                pieceB64 = pieceImg.src.split(',')[1];
            }

            return { bgB64, pieceB64 };
        }""")
        return result.get('bgB64'), result.get('pieceB64')
    except Exception as e:
        print(f"   ⚠️ [CAPTCHA] Could not extract images from DOM: {e}")
        return None, None


async def _get_slider_handle_page_coords(page) -> tuple[float, float] | None:
    """
    Return the (page_x, page_y) center of the slider drag handle (#sliderContainer).
    Falls back to searching the track bar if the ID is missing.
    """
    try:
        result = await page.evaluate("""() => {
            // Primary: #sliderContainer
            let el = document.getElementById('sliderContainer');

            // Fallback: first element whose class contains 'slider' inside the captcha
            if (!el) {
                const root = document.getElementById('NEW_CAPTCHA') || document.body;
                el = root.querySelector('[class*="slider"]') ||
                     root.querySelector('[class*="drag"]') ||
                     root.querySelector('[class*="handle"]');
            }
            if (!el) return null;

            const rect = el.getBoundingClientRect();
            return {
                x: rect.left + rect.width  / 2,
                y: rect.top  + rect.height / 2,
                width: rect.width,
                height: rect.height
            };
        }""")
        if result:
            return result['x'], result['y']
        return None
    except Exception as e:
        print(f"   ⚠️ [CAPTCHA] Could not locate slider handle: {e}")
        return None


# ════════════════════════════════════════════════════════════════
# Captcha type classification
# ════════════════════════════════════════════════════════════════

async def _classify_captcha_type(page) -> str:
    """
    Returns one of: 'slider', 'rotate', 'click', 'login_wall', 'unknown'.
    Uses URL path first (fast), then body text keywords.
    """
    try:
        url_lower = page.url.lower()
        if "/verify/captcha" in url_lower or "captcha" in url_lower:
            await page.wait_for_timeout(1500)
            try:
                body_text = (await page.locator("body").inner_text(timeout=4000)).lower()
            except Exception:
                body_text = ""
            if any(kw in body_text for kw in _ROTATE_KEYWORDS):
                return "rotate"
            if any(kw in body_text for kw in _CLICK_KEYWORDS):
                return "click"
            return "slider"   # default for /verify/captcha

        try:
            body_text = (await page.locator("body").inner_text(timeout=4000)).lower()
        except Exception:
            body_text = ""

        if any(kw in body_text for kw in [
            "เข้าสู่ระบบ", "เข้าสู่หน้าที่ต้องการไม่สาเร็จ", "login", "sign in"
        ]):
            return "login_wall"
        if any(kw in body_text for kw in _ROTATE_KEYWORDS):
            return "rotate"
        if any(kw in body_text for kw in _SLIDER_KEYWORDS):
            return "slider"
        if any(kw in body_text for kw in _CLICK_KEYWORDS):
            return "click"
    except Exception:
        pass
    return "unknown"


async def _find_captcha_root(page):
    """Return first visible captcha root element, or None."""
    for sel in _CAPTCHA_ROOT_SELECTORS:
        try:
            el = await page.query_selector(sel)
            if el and await el.is_visible():
                return el
        except Exception:
            continue
    return None


async def _wait_for_captcha_widget(page, timeout_ms: int = 15000) -> bool:
    """
    Wait until the captcha widget has both images loaded in the DOM.
    Checks for #puzzleImgComponent (piece) AND img.DfwepB (background) —
    the two confirmed element IDs from HTML analysis.
    Polls every 1.2 s up to timeout_ms.
    """
    deadline = asyncio.get_running_loop().time() + timeout_ms / 1000
    while asyncio.get_running_loop().time() < deadline:
        await page.wait_for_timeout(1200)
        try:
            ready = await page.evaluate("""() => {
                // Primary check: confirmed real selectors from HTML
                const piece = document.getElementById('puzzleImgComponent');
                const bg    = document.querySelector('img.DfwepB');
                if (piece && bg &&
                    piece.src && piece.src.startsWith('data:') &&
                    bg.src    && bg.src.startsWith('data:')) {
                    return true;
                }
                // Fallback: any captcha root with canvas or img
                const roots = ['#NEW_CAPTCHA','#captcha-verify','#shopee-captcha-root',
                                '.shopee-captcha','div.verify-wrap'];
                for (const sel of roots) {
                    const root = document.querySelector(sel);
                    if (!root) continue;
                    if (root.querySelector('canvas') ||
                        root.querySelector('img[src^="data:"]') ||
                        root.querySelector('[class*="slide"]') ||
                        root.querySelector('[class*="puzzle"]')) {
                        return true;
                    }
                }
                return false;
            }""")
            if ready:
                return True
        except Exception:
            pass
    return False


# ════════════════════════════════════════════════════════════════
# Main solver: local CV  +  AI vision fallback
# ════════════════════════════════════════════════════════════════

async def _ai_solve_slider_captcha(page) -> bool:
    """
    Solve one slider captcha challenge.

    Solving order:
      1. LOCAL CV (Pillow+numpy) — extract BG + piece from DOM, run edge-
         correlation template matching → pixel-accurate gap_x, zero API cost.
      2. AI VISION fallback — if CV fails or returns implausible distance,
         take a full screenshot and ask claude-3.5-sonnet for coordinates.

    After getting gap_left_x:
      • Locate #sliderContainer bounding box for the actual page coordinates.
      • Execute a human-like smoothstep drag of gap_left_x pixels.
      • Returns True if _is_captcha_detected returns False afterward.
    """
    gap_left_x: int | None = None

    # ── Step 1: Local CV ──────────────────────────────────────────
    bg_b64, piece_b64 = await _extract_captcha_images(page)
    if bg_b64 and piece_b64:
        gap_left_x = _cv_find_gap_x(bg_b64, piece_b64)
    else:
        print("   ⚠️ [CAPTCHA CV] Could not extract images — falling back to AI vision")

    # ── Step 2: AI vision fallback ────────────────────────────────
    if gap_left_x is None or not (10 <= gap_left_x <= 260):
        print(f"   🔄 [CAPTCHA] Local CV gap_x={gap_left_x} — using AI vision fallback")
        try:
            # Take a widget-scoped screenshot
            captcha_el = await _find_captcha_root(page)
            if captcha_el:
                try:
                    await captcha_el.scroll_into_view_if_needed()
                    await page.wait_for_timeout(300)
                    shot_bytes = await captcha_el.screenshot()
                except Exception:
                    shot_bytes = await page.screenshot()
            else:
                shot_bytes = await page.screenshot()

            img_b64_shot = base64.standard_b64encode(shot_bytes).decode()
            prompt = (
                "You are analysing a Shopee slider CAPTCHA screenshot.\n\n"
                "Layout:\n"
                "  • Background image: ~280 px wide, ~150 px tall.\n"
                "  • A puzzle-piece shaped GAP/HOLE exists somewhere in the background.\n"
                "  • Below the background is a slider TRACK (280 px wide).\n"
                "  • A small puzzle piece / arrow handle sits at the LEFT of the track.\n\n"
                "Your task:\n"
                "  gap_left_x — the X pixel of the LEFT EDGE of the gap (NOT its center).\n"
                "  handle_y   — Y pixel of the CENTER of the drag handle.\n\n"
                "Rules:\n"
                "  • gap_left_x is the number of pixels the handle must be dragged.\n"
                "  • gap_left_x is normally between 50 and 240.\n"
                "  • The GAP looks darker / shadowed compared to the surroundings.\n\n"
                "Reply ONLY with valid JSON (no markdown):\n"
                '{"gap_left_x": <int>, "handle_y": <int>}\n\n'
                "If you cannot identify the elements with confidence:\n"
                '{"gap_left_x": null, "handle_y": null}'
            )
            response = _OPENROUTER_CLIENT.chat.completions.create(
                extra_headers=_OPENROUTER_EXTRA_HEADERS,
                extra_body={},
                model=_CAPTCHA_MODEL,
                max_tokens=64,
                messages=[{
                    "role": "user",
                    "content": [
                        {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{img_b64_shot}"}},
                        {"type": "text", "text": prompt},
                    ],
                }],
            )
            raw = response.choices[0].message.content.strip()
            raw = re.sub(r"^```[a-z]*\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
            coords = json.loads(raw)
            ai_x = coords.get("gap_left_x")
            ai_y = coords.get("handle_y")
            if ai_x is not None and 10 <= int(ai_x) <= 260:
                gap_left_x = int(ai_x)
                print(f"   🤖 [CAPTCHA AI] gap_left_x={gap_left_x}  handle_y={ai_y}")
            else:
                print(f"   ⚠️ [CAPTCHA AI] AI returned implausible gap_left_x={ai_x}")
                return False
        except Exception as e:
            print(f"   ⚠️ [CAPTCHA AI] Vision call failed: {e}")
            return False

    drag_distance = gap_left_x
    print(f"   📐 [CAPTCHA] drag_distance={drag_distance}px")

    # ── Step 3: Locate handle on page & execute drag ──────────────
    try:
        handle_coords = await _get_slider_handle_page_coords(page)
        if handle_coords is None:
            print("   ⚠️ [CAPTCHA] Could not locate #sliderContainer on page")
            return False

        hx, hy = handle_coords
        print(f"   🖱️  [CAPTCHA] Handle page coords: ({hx:.1f}, {hy:.1f})")

        # ── Human-like smoothstep drag with micro-jitter ──────────
        await page.mouse.move(hx, hy)
        await page.wait_for_timeout(random.randint(250, 450))
        await page.mouse.down()
        await page.wait_for_timeout(random.randint(120, 220))

        steps = random.randint(28, 42)
        for i in range(1, steps + 1):
            t = i / steps
            eased = t * t * (3 - 2 * t)            # smoothstep
            jitter_y = random.uniform(-2.0, 2.0)
            if i % random.randint(8, 12) == 0:      # occasional micro-pause
                await page.wait_for_timeout(random.randint(10, 35))
            await page.mouse.move(
                hx + drag_distance * eased,
                hy + jitter_y,
            )
            await page.wait_for_timeout(random.randint(7, 22))

        # Overshoot → settle
        overshoot = random.randint(2, 7)
        await page.mouse.move(hx + drag_distance + overshoot, hy)
        await page.wait_for_timeout(random.randint(50, 110))
        await page.mouse.move(hx + drag_distance, hy + random.uniform(-0.5, 0.5))
        await page.wait_for_timeout(random.randint(80, 150))
        await page.mouse.up()

        # Wait for Shopee's backend validation
        await page.wait_for_timeout(random.randint(2500, 3500))

        if not await _is_captcha_detected(page):
            print("   ✅ [CAPTCHA] Slider solved!")
            return True

        print("   ⚠️ [CAPTCHA] Drag completed but captcha still present")
        return False

    except Exception as e:
        print(f"   ❌ [CAPTCHA] Drag simulation error: {e}")
        return False


async def _reload_captcha_page(page, captcha_url: str) -> bool:
    """
    Reload the captcha page for a fresh puzzle challenge.

    Key: scroll slightly after load — Shopee's React widget sometimes
    only mounts after the first scroll event.
    Returns True when the widget images are fully loaded.
    """
    print("   🔄 [CAPTCHA] Reloading for fresh challenge...")
    await page.wait_for_timeout(random.randint(2800, 4800))
    try:
        await page.goto(captcha_url, wait_until="domcontentloaded", timeout=60000)
    except Exception as e:
        print(f"   ⚠️ [CAPTCHA] Reload navigation failed: {e}")
        return False

    await page.wait_for_timeout(1000)
    try:
        await page.mouse.wheel(0, 100)   # trigger React widget mount
    except Exception:
        pass

    await page.wait_for_timeout(random.randint(700, 1400))
    ready = await _wait_for_captcha_widget(page, timeout_ms=15000)
    if not ready:
        print("   ⚠️ [CAPTCHA] Widget not ready after reload")
    return ready


async def _handle_captcha(page, destination_url: str, max_wait_seconds: int = 120) -> bool:
    """
    Unified captcha entry point called throughout all scrapers.

    Flow:
      • Classify captcha type from page URL + body text.
      • For 'slider': a single AI/CV solve attempt. On success, navigates to
        destination_url and checks whether another captcha immediately
        appears (re-captcha'd) before declaring victory.
      • For other types / on failure: manual prompt straight away — no
        repeated reload-and-retry loop (matches Lazada's captcha flow: don't
        rapid-refresh hunting for a solvable puzzle, just hand it to a human).

    Critical fix: saves the CAPTCHA PAGE URL (page.url at call-time) separately
    from destination_url — reloads must go to /verify/captcha?..., NOT back to
    the product/shop page, otherwise Shopee does NOT issue a new puzzle.
    """
    safe_label = re.sub(r'[^\w]', '_', destination_url)[:40]
    await log_captcha_encounter(page, f"captcha_at_{safe_label}")

    captcha_type = await _classify_captcha_type(page)
    captcha_page_url = page.url   # /verify/captcha?... — save for reloads
    print(f"   🔍 [CAPTCHA] type={captcha_type}  captcha_url={captcha_page_url[:80]}")

    if captcha_type == "slider":
        widget_ready = await _wait_for_captcha_widget(page, timeout_ms=15000)
        if not widget_ready:
            print("   ⚠️ [CAPTCHA] Widget never appeared — going to manual fallback")
        else:
            print("   🤖 [CAPTCHA] Single auto-solve attempt ...")

            if await _ai_solve_slider_captcha(page):
                try:
                    await page.goto(destination_url, wait_until="domcontentloaded", timeout=60000)
                    await human_delay(5.0, 9.0)
                except Exception as e:
                    print(f"   ❌ [CAPTCHA] Post-solve navigation failed: {e}")
                    return False

                if not await _is_captcha_detected(page):
                    print("   ✅ [CAPTCHA] Fully recovered!")
                    return True
                print("   ⚠️ [CAPTCHA] Re-captcha appeared — falling back to manual...")
            else:
                # No rapid reload-and-retry loop — one miss goes straight to
                # manual instead of repeatedly reloading hunting for a
                # solvable puzzle (that "รัวๆ" refresh behavior is exactly
                # what stalls unattended runs without actually helping).
                print("   ⚠️ [CAPTCHA] Auto-solve failed — falling back to manual...")

    elif captcha_type in ("rotate", "click"):
        print(f"   ℹ️ [CAPTCHA] '{captcha_type}' type — AI not supported, requesting manual solve.")

    # ── Manual fallback ───────────────────────────────────────────
    print(f"\n   >>> Current page: {page.url}")
    prompt = (
        f"\n >>> ⚠️  [MANUAL] Captcha type='{captcha_type}'.\n"
        "     Solve it in the browser window, then press ENTER here..."
    )
    await asyncio.get_running_loop().run_in_executor(None, input, prompt)

    try:
        await page.goto(destination_url, wait_until="domcontentloaded", timeout=60000)
        await human_delay(8.0, 14.0)
    except Exception as e:
        print(f"   ❌ [CAPTCHA] Post-manual navigation failed: {e}")
        return False

    if await _is_captcha_detected(page):
        print("   ❌ [CAPTCHA] Still showing captcha after manual solve — skipping")
        return False

    print("   ✅ [CAPTCHA] Recovered from manual solve!")
    return True

async def _close_shopee_popups(page):
    popup_selectors = [
        ".shopee-popup__close-btn",
        ".shopee-modal__close",
        ".new-user-campaign-dialog__close-btn",
    ]
    for sel in popup_selectors:
        try:
            btn = page.locator(sel).first
            if await btn.is_visible(timeout=500):
                await btn.click()
                await page.wait_for_timeout(500)
        except Exception:
            pass

async def _find_review_filter_button(page, filter_text: str):
    """
    Find a review filter button by visible text using fallbacks.

    Shopee TH uses <div class="product-rating-overview__filter"><span>TEXT</span></div>
    for all filter buttons — NOT <button> elements.

    The "with comments" filter appears as Thai text: ความคิดเห็น (N)
    The star filters appear as: N ดาว (N)

    Strategy:
      1. Try JS-based search directly on .product-rating-overview__filter divs
         (fastest & most reliable — avoids Playwright locator stale-element issues)
      2. Fallback to Playwright locator approach with extended selector list
    """
    # Build keyword patterns including Thai equivalents
    is_comment_filter = "comment" in filter_text.lower()

    # --- Strategy 1: JS direct DOM search (primary) ---
    try:
        element_handle = await page.evaluate_handle("""(filterText) => {
            const isCommentFilter = filterText.toLowerCase().includes('comment');

            // Get all filter divs
            const filterDivs = Array.from(document.querySelectorAll(
                '.product-rating-overview__filter, .product-rating-overview__filters > div'
            ));

            for (const div of filterDivs) {
                const text = div.innerText.trim();

                if (isCommentFilter) {
                    // Match Thai "ความคิดเห็น" or English "with comment" variants
                    if (/ความคิดเห็น|with\\s*comments?|คอมเมนต์|มีรีวิว/i.test(text)) {
                        return div;
                    }
                } else {
                    // For star filters like "5 star", "4 star", etc.
                    // The Thai button shows "5 ดาว (N)" — match on the star number
                    const starMatch = filterText.match(/^(\\d+)\\s*star/i);
                    if (starMatch) {
                        const starNum = starMatch[1];
                        // Match "5 ดาว" or "5 star" at start of text
                        if (new RegExp('^' + starNum + '\\\\s*(ดาว|star)', 'i').test(text)) {
                            return div;
                        }
                    } else {
                        // Generic text match
                        if (new RegExp(filterText, 'i').test(text)) {
                            return div;
                        }
                    }
                }
            }
            return null;
        }""", filter_text)

        # Check if we got a valid element (not null/undefined)
        js_type = await page.evaluate("(el) => el ? el.tagName : null", element_handle)
        if js_type:
            # Wrap the JS handle as a Playwright element locator
            # We need to use the element handle directly for clicking
            return element_handle
    except Exception:
        pass

    # --- Strategy 2: Playwright locator fallback ---
    patterns = [
        re.compile(rf"{re.escape(filter_text)}", re.IGNORECASE),
    ]
    if is_comment_filter:
        patterns.append(re.compile(r"with\s*comments?", re.IGNORECASE))
        patterns.append(re.compile(r"(ความคิดเห็น|comment|คอมเมนต์|มีรีวิว)", re.IGNORECASE))
    else:
        # For star filters: "5 star" → also match "5 ดาว"
        star_match = re.match(r"^(\d+)\s*star", filter_text, re.IGNORECASE)
        if star_match:
            star_num = star_match.group(1)
            patterns.append(re.compile(rf"^{star_num}\s*(ดาว|star)", re.IGNORECASE))

    roots = [page]
    section = page.locator(".product-rating-overview__filters").first
    if await section.count() > 0:
        roots.insert(0, section)

    # Prioritize .product-rating-overview__filter (the actual DOM element used by Shopee TH)
    selectors = [
        ".product-rating-overview__filter",
        ".product-rating-overview__filters > div",
        "button",
        "a",
        "div",
        "span",
    ]
    for root in roots:
        for patt in patterns:
            for sel in selectors:
                try:
                    candidate = root.locator(sel).filter(has_text=patt).first
                    if await candidate.count() > 0:
                        return candidate
                except Exception:
                    continue
    return None

async def _human_like_scroll(page, scroll_amount: int = 600):
    steps = random.randint(2, 4)
    per_step = scroll_amount // steps
    for _ in range(steps):
        jitter = random.randint(-80, 80)
        await page.mouse.wheel(0, per_step + jitter)
        await page.wait_for_timeout(random.randint(200, 500))

async def _scroll_review_into_view(page):
    """
    Scroll so that the review comment list AND its pagination bar are centred
    in the viewport.  Called at the start of every review-page iteration and
    before clicking the next-page button so that Shopee's React renderer keeps
    the review section mounted and visible.
    """
    try:
        await page.evaluate("""() => {
            // Prefer the pagination bar so the next-button is always on screen
            const targets = [
                '.shopee-page-controller',
                '.product-rating-detail .shopee-page-controller',
                '.shopee-product-comment-list',
                '[class*="product-comment"]',
                '[class*="rating-comment"]',
                '.product-rating-overview__filters',
            ];
            for (const sel of targets) {
                const el = document.querySelector(sel);
                if (el) {
                    el.scrollIntoView({ behavior: 'instant', block: 'center' });
                    return;
                }
            }
        }""")
        await page.wait_for_timeout(400)
    except Exception:
        pass

async def _move_mouse_randomly(page):
    try:
        x = random.randint(200, 1400)
        y = random.randint(200, 700)
        await page.mouse.move(x, y)
        await page.wait_for_timeout(random.randint(100, 400))
    except Exception:
        pass

async def _get_active_review_page_num(page) -> int:
    """
    Read the currently-highlighted page number from Shopee's review pagination.
    Returns the active page number (1-based), or 0 if undetectable.
    """
    try:
        num = await page.evaluate("""() => {
            // Active page button has class 'shopee-icon-button--active' or aria-current="true"
            // or is a plain button with the 'active' variant inside the page-controller
            const selectors = [
                '.shopee-page-controller .shopee-button-solid--primary',
                '.shopee-page-controller button[class*="active"]',
                '.shopee-mini-page-controller__current',
                '.product-rating-detail .shopee-page-controller .shopee-button-solid--primary',
            ];
            for (const sel of selectors) {
                const el = document.querySelector(sel);
                if (el) {
                    const t = el.innerText.trim();
                    const n = parseInt(t, 10);
                    if (!isNaN(n)) return n;
                }
            }
            // Fallback: look for all page-number buttons and find the one that looks active
            const btns = document.querySelectorAll(
                '.shopee-page-controller button, .shopee-mini-page-controller button'
            );
            for (const btn of btns) {
                if (btn.classList.toString().includes('primary') ||
                    btn.getAttribute('aria-current') === 'true') {
                    const n = parseInt(btn.innerText.trim(), 10);
                    if (!isNaN(n)) return n;
                }
            }
            return 0;
        }""")
        return int(num) if num else 0
    except Exception:
        return 0


async def _navigate_to_review_page_num(page, target_page: int,
                                        old_first_text: str,
                                        timeout_ms: int = 15000) -> bool:
    """
    Click the numbered page button for `target_page` in Shopee's review pagination.

    Strategy:
      1. Look for a visible button whose text equals `target_page`.
      2. Click it and wait for content to change.
      3. If the exact button isn't visible, use next/prev arrows until we reach it,
         re-clicking the numbered button if it appears mid-way.

    Returns True if the target page was reached, False otherwise.
    """
    # Try clicking the exact numbered button first
    try:
        clicked = await page.evaluate(f"""() => {{
            const btns = document.querySelectorAll(
                '.shopee-page-controller button, .product-rating-detail .shopee-page-controller button'
            );
            for (const btn of btns) {{
                if (btn.innerText.trim() === '{target_page}' && !btn.disabled) {{
                    btn.click();
                    return true;
                }}
            }}
            return false;
        }}""")
        if clicked:
            await page.wait_for_timeout(500)
            changed = await _wait_for_review_page_change(page, old_first_text, timeout_ms=timeout_ms)
            if changed:
                print(f"   ✅ Jumped directly to page {target_page} via numbered button")
                return True
    except Exception:
        pass

    # Fallback: use the next-arrow repeatedly until current page == target_page
    print(f"   🔄 Numbered button for page {target_page} not visible — using next-arrow fallback")
    for _attempt in range(target_page + 5):  # safety cap
        current = await _get_active_review_page_num(page)
        if current == target_page:
            return True
        if current > target_page:
            # overshot somehow — give up
            return False

        snap = await _get_first_review_text(page)
        next_btn = None
        for next_sel in (
            ".shopee-icon-button.shopee-icon-button--right:not([disabled])",
            ".shopee-icon-button.shopee-icon-button--right",
        ):
            candidate = page.locator(next_sel).first
            if await candidate.count() > 0:
                try:
                    if await candidate.is_visible(timeout=2000):
                        is_en = await candidate.evaluate("node => !node.disabled && node.offsetParent !== null")
                        if is_en:
                            next_btn = candidate
                            break
                except Exception:
                    continue

        if next_btn is None:
            print(f"   ⚠️ No next button while advancing to page {target_page}")
            return False

        try:
            await next_btn.click(force=True, timeout=10000)
        except Exception as e:
            print(f"   ⚠️ Arrow click failed: {e}")
            return False

        await _wait_for_review_page_change(page, snap, timeout_ms=10000)
        await page.wait_for_timeout(random.randint(600, 1200))

        # After every arrow click, check if the exact numbered button is now visible
        try:
            jumped = await page.evaluate(f"""() => {{
                const btns = document.querySelectorAll(
                    '.shopee-page-controller button, .product-rating-detail .shopee-page-controller button'
                );
                for (const btn of btns) {{
                    if (btn.innerText.trim() === '{target_page}' && !btn.disabled) {{
                        btn.click();
                        return true;
                    }}
                }}
                return false;
            }}""")
            if jumped:
                snap2 = await _get_first_review_text(page)
                await _wait_for_review_page_change(page, snap2, timeout_ms=10000)
                print(f"   ✅ Jumped to page {target_page} via numbered button mid-sequence")
                return True
        except Exception:
            pass

    return False


async def _wait_for_review_page_change(page, old_snapshot: str, timeout_ms: int = 15000) -> bool:
    """
    Two-phase wait for Shopee React pagination to fully settle.

    Phase 1 – loading started: old cmtids disappear from DOM
              (React unmounts them while fetching the next page).
    Phase 2 – loading done:   new cmtids appear that differ from old_snapshot.

    Polls every 300 ms for faster detection.
    Returns True when content changed, False on timeout.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_ms / 1000
    loading_detected = False
    while loop.time() < deadline:
        await page.wait_for_timeout(300)
        try:
            snap = await _get_first_review_text(page)
            if not snap:
                loading_detected = True   # DOM cleared — loading spinner showing
                continue
            if snap != old_snapshot:
                return True               # New reviews rendered
        except Exception:
            pass
    return False


_NEXT_BTN_JS = """
    const CONTAINER_SELECTORS = [
        '.product-rating-detail',
        '.shopee-product-rating',
        '.product-ratings__page-controller',
        '.product-rating-overview',
    ];
    const NEXT_BTN_SELECTORS = [
        '.shopee-page-controller .shopee-icon-button--right',
        '.shopee-icon-button.shopee-icon-button--right',
    ];
    function isVisible(el) {
        if (el.offsetParent !== null) return true;
        const r = el.getBoundingClientRect();
        return r.width > 0 && r.height > 0;
    }
    function isDisabled(el) {
        return !!(
            el.disabled ||
            el.getAttribute('aria-disabled') === 'true' ||
            el.classList.contains('shopee-icon-button--disabled') ||
            el.classList.toString().includes('disabled')
        );
    }
    function findBtn() {
        for (const cSel of CONTAINER_SELECTORS) {
            const c = document.querySelector(cSel);
            if (!c) continue;
            for (const bSel of NEXT_BTN_SELECTORS) {
                const btn = c.querySelector(bSel);
                if (btn && isVisible(btn)) return { btn, disabled: isDisabled(btn) };
            }
        }
        for (const bSel of NEXT_BTN_SELECTORS) {
            const btn = document.querySelector(bSel);
            if (btn && isVisible(btn)) return { btn, disabled: isDisabled(btn) };
        }
        return null;
    }
"""

async def _check_next_button_state(page) -> str:
    """
    Inspect the next-page button state WITHOUT clicking it.

    Shopee disables the next button in two distinct situations:
      (a) during a page-load/transition animation (transient)
      (b) permanently when the user is on the genuine last page

    The old _click_next_review_page conflated both cases because it
    clicked the button to check its state — turning a state-inspection
    into a side-effecting action.  This function is side-effect-free
    so callers can safely poll button state after a content-change
    timeout without accidentally firing another navigation.

    Returns: "enabled" | "disabled" | "not_found"
    """
    result = await page.evaluate(f"""() => {{
        {_NEXT_BTN_JS}
        const found = findBtn();
        if (!found) return 'not_found';
        return found.disabled ? 'disabled' : 'enabled';
    }}""")
    return result or "not_found"


async def _click_next_review_page(page) -> str:
    """
    Click the next-page button.  Scopes to the review container first so
    we never accidentally click an unrelated image-carousel arrow.

    Returns: "clicked" | "disabled" | "not_found"
    """
    result = await page.evaluate(f"""() => {{
        {_NEXT_BTN_JS}
        const found = findBtn();
        if (!found) return 'not_found';
        if (found.disabled) return 'disabled';
        found.btn.click();
        return 'clicked';
    }}""")
    return result or "not_found"

async def _get_first_review_text(page) -> str:
    """
    Return a stable fingerprint of ALL cmtids currently in the DOM joined
    with the first review's text.  Using all cmtids means we never miss a
    page flip even when the first review's text happens to be identical
    across two pages.
    """
    try:
        snapshot = await page.evaluate("""() => {
            // Collect all data-cmtid values as primary fingerprint
            const cmtNodes = Array.from(document.querySelectorAll('[data-cmtid]'));
            if (cmtNodes.length > 0) {
                const ids = cmtNodes.map(n => n.getAttribute('data-cmtid')).join(',');
                // Also grab first review text for human-readable logs
                const firstText = cmtNodes[0].innerText.trim().slice(0, 80);
                return ids + '|' + firstText;
            }
            // Fallback: first rating content text
            const selectors = [
                '.q2b7Oq[data-cmtid] .YNedDV',
                '.q2b7Oq[data-cmtid]',
                'div[data-cmtid]',
                '.shopee-product-rating__content',
            ];
            for (const sel of selectors) {
                const el = document.querySelector(sel);
                if (el) return el.innerText.trim();
            }
            return '';
        }""")
        return snapshot or ""
    except Exception:
        return ""

async def _nudge_review_section(page):
    """Scroll the review section into view with real mouse-wheel events."""
    await _scroll_review_into_view(page)
    for _ in range(2):
        try:
            await page.mouse.wheel(0, random.randint(150, 350))
            await page.wait_for_timeout(random.randint(300, 600))
        except Exception:
            pass
    await _scroll_review_into_view(page)


async def _advance_review_page(page, old_first_text: str, target_page: int,
                                max_retries: int = 4) -> str:
    """
    Navigate to the next review page and confirm the content changed.

    ROOT CAUSE OF PREVIOUS BUG
    ───────────────────────────
    The previous version called _click_next_review_page() at the START of
    every retry loop iteration — meaning attempt 2, 3, … also clicked the
    button just to read its state.  Shopee disables the next button during
    page-load/transition animations (to prevent double-clicks), not only when
    the user is genuinely on the last page.  So:

        attempt 1 → click → content slowly loads → timeout (18 s not enough)
        attempt 2 → click → button was still "loading-disabled" → returned
                    "disabled" → declared "last_page" — WRONG.

    FIX
    ───
    1.  ONE initial click.  Subsequent retries NEVER click again unless we
        have confirmed (via active page-number) that we are still on the
        same page and the button is back to "enabled".
    2.  After a content-change timeout, use _get_active_review_page_num as
        the primary oracle — if Shopee's pagination indicator already shows
        target_page, we advanced successfully even if our DOM fingerprint
        comparison missed it.
    3.  Check button state with _check_next_button_state (no side-effects)
        AFTER a settling wait, so transient loading-disabled is over.

    Returns:
      "advanced"  — we are now on target_page
      "last_page" — confirmed no more pages exist
      "stuck"     — button is clickable but every retry timed out — caller
                    should log a warning and stop pagination for this URL
    """
    # ── Pre-flight: is the button already gone/disabled before we even click?
    pre_state = await _check_next_button_state(page)
    if pre_state in ("disabled", "not_found"):
        return "last_page"

    # ── Single initial click ─────────────────────────────────────────────────
    click_result = await _click_next_review_page(page)
    if click_result in ("disabled", "not_found"):
        return "last_page"

    # ── Wait → check → maybe retry ──────────────────────────────────────────
    snapshot = old_first_text
    for attempt in range(1, max_retries + 1):
        await _nudge_review_section(page)

        # Patience grows on later attempts (Shopee may be rate-throttling)
        timeout_ms = 15000 + attempt * 8000   # 23 s, 31 s, 39 s, 47 s
        changed = await _wait_for_review_page_change(page, snapshot, timeout_ms=timeout_ms)
        if changed:
            return "advanced"

        # ── Content didn't change within the timeout ─────────────────────
        # Primary oracle: what page does Shopee's own UI say we're on?
        active = await _get_active_review_page_num(page)
        if active == target_page:
            # Pagination indicator already shows target_page — we DID navigate;
            # the content fingerprint just wasn't different enough (e.g. Shopee
            # served identical review IDs, or the loading phase was too fast
            # for our 300 ms poll to catch the empty interim state).
            return "advanced"

        # Give any in-flight loading/transition animation time to settle
        # before inspecting the button.  This is the critical wait that
        # prevents mis-reading a transient "loading-disabled" state as
        # a permanent "last-page-disabled" state.
        await page.wait_for_timeout(random.randint(3000, 5000))

        # Secondary oracle: button state (WITHOUT clicking)
        btn_state = await _check_next_button_state(page)

        if btn_state in ("disabled", "not_found"):
            # The button is disabled after waiting for settling — but is that
            # because we navigated to the last page, or because we're still
            # on the old page and it's genuinely the end?
            active2 = await _get_active_review_page_num(page)
            if active2 == target_page:
                # We did navigate; this page happens to be the last one.
                return "advanced"
            # Still on old page, button truly disabled → genuine last page.
            return "last_page"

        # Button is enabled → we didn't navigate yet; retry click
        if attempt < max_retries:
            print(f"   ⚠️ Page {target_page} content did not change (attempt {attempt}/{max_retries}) — retrying click")
            snapshot = await _get_first_review_text(page)
            click_result = await _click_next_review_page(page)
            if click_result in ("disabled", "not_found"):
                return "last_page"
        else:
            print(f"   ⚠️ Page {target_page} content did not change after {max_retries} attempts — stopping")

    return "stuck"


async def _recover_stalled_review_pagination(page, url: str, filter_text: str,
                                              target_page: int) -> bool:
    """
    Recover from a false "last_page"/"stuck" signal in _advance_review_page.

    ROOT CAUSE THIS FIXES
    ──────────────────────
    Shopee occasionally disables/hides the next-review-page button for a few
    seconds (rate-limiting / anti-bot throttling on the SPA) even though more
    review pages genuinely exist. _advance_review_page only had ~2 clicks and
    under a minute of total patience before it trusted "button disabled" as
    proof of a genuine last page — so a transient block was silently reported
    as "no more reviews" and pagination stopped dozens/hundreds of pages early
    (e.g. stopping at page 14/184 with only 83/~1100 reviews collected).

    FIX
    ───
    Instead of giving up immediately, do a full reload of the product page,
    re-apply the review filter from scratch (clears whatever client-side
    state caused the stall), then jump straight back to target_page using
    the existing numbered-pagination / next-arrow logic in
    _navigate_to_review_page_num.

    Returns True if we're confirmed back on target_page, False otherwise.
    """
    print(f"   🔁 Stall recovery: reloading page and re-applying filter '{filter_text}' "
          f"to resume at page {target_page}...")
    try:
        reopen_ok = await _human_reopen_product(page, url, filter_text)
        if not reopen_ok:
            print("   ⚠️ Stall recovery: reopen failed")
            return False
        if await _is_captcha_detected(page):
            print("   ⚠️ Stall recovery: captcha shown after reopen")
            return False

        filter_container = page.locator(".product-rating-overview__filters").first
        try:
            await filter_container.wait_for(state="visible", timeout=20000)
            await filter_container.scroll_into_view_if_needed()
            await page.wait_for_timeout(random.randint(1500, 3000))
        except Exception as e:
            print(f"   ⚠️ Stall recovery: filter section not visible ({e})")
            return False

        btn = await _find_review_filter_button(page, filter_text)
        if btn is None:
            print("   ⚠️ Stall recovery: filter button not found")
            return False

        await _close_shopee_popups(page)
        try:
            await btn.wait_for(state="visible", timeout=10000)
        except Exception:
            pass
        try:
            await btn.click(force=True, timeout=10000)
        except Exception:
            await page.evaluate("(el) => el && el.click()", btn)
        await page.wait_for_timeout(random.randint(2500, 4000))
        await _scroll_review_into_view(page)

        snap = await _get_first_review_text(page)
        recovered = await _navigate_to_review_page_num(page, target_page, snap, timeout_ms=25000)
        if recovered:
            print(f"   ✅ Stall recovery succeeded — resumed at page {target_page}")
        else:
            print(f"   ⚠️ Stall recovery failed — could not reach page {target_page}")
        return recovered
    except Exception as e:
        print(f"   ⚠️ Stall recovery error: {e}")
        return False

# ════════════════════════════════════════════════════════════════
# Product Mapper
# ════════════════════════════════════════════════════════════════
IMG_BASE = "https://down-th.img.susercontent.com/file/"

# One-shot debug flag: dump the first unmappable raw item so the exact
# (possibly new) Shopee JSON shape can be inspected without spamming logs.
_RAW_ITEM_DEBUG_DUMPED = False


def _dump_raw_item_debug(item_wrap: dict, info: Optional[dict] = None, context: str = "search") -> None:
    """
    Save + print the structure of the first problem item, once per run.

    Prints the wrap's top-level keys, the resolved info dict's keys, and the
    keys of every nested dict inside it — enough to see exactly where Shopee
    relocated name / price / shop fields — and saves the full raw JSON.
    """
    global _RAW_ITEM_DEBUG_DUMPED
    if _RAW_ITEM_DEBUG_DUMPED:
        return
    _RAW_ITEM_DEBUG_DUMPED = True
    try:
        keys = list(item_wrap.keys()) if isinstance(item_wrap, dict) else type(item_wrap).__name__
        print(f"   🧪 [DEBUG] {context} item — wrap top-level keys: {keys}")
        if isinstance(info, dict):
            print(f"   🧪 [DEBUG] resolved info keys: {list(info.keys())}")
            for k, v in info.items():
                if isinstance(v, dict) and v:
                    print(f"   🧪 [DEBUG]   info['{k}'] (dict) keys: {list(v.keys())}")
                elif isinstance(v, list) and v and isinstance(v[0], dict):
                    print(f"   🧪 [DEBUG]   info['{k}'][0] (list of dict) keys: {list(v[0].keys())}")
        path = Path(SEARCH_LOG_DIR) / f"raw_item_debug_{make_timestamp()}.json"
        path.write_text(json.dumps(item_wrap, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(f"   🧪 [DEBUG] Full raw item saved → {path}")
    except Exception as e:
        print(f"   ⚠️ [DEBUG] Could not dump raw item: {e}")


def _merge_new_item_shape(item_wrap: dict) -> Optional[dict]:
    """
    Synthesize a classic ``item_basic``-style dict from Shopee's 2026 PC-search
    shape, where ``item_basic`` is null and the product fields are split across:

      • item_data                 → ids, catid, ctime, rating, liked, shop_data,
                                     nested price (item_card_display_price) and
                                     sold (item_card_display_sold_count)
      • item_card_displayed_asset → name, image, images, shop_location

    Returns a flat dict using the SAME field names ``_map_product`` already reads
    (so the mapper needs no changes), or None if this isn't that shape.
    """
    item_data = item_wrap.get("item_data")
    asset     = item_wrap.get("item_card_displayed_asset")
    item_data = item_data if isinstance(item_data, dict) else {}
    asset     = asset     if isinstance(asset, dict) else {}

    itemid = item_data.get("itemid") or item_wrap.get("itemid")
    shopid = item_data.get("shopid") or item_wrap.get("shopid")
    if not (itemid and shopid):
        return None

    disp_price = (item_data.get("item_card_display_price")
                  or asset.get("display_price") or {})
    if not isinstance(disp_price, dict):
        disp_price = {}
    sold      = item_data.get("item_card_display_sold_count") or {}
    sold      = sold if isinstance(sold, dict) else {}
    shop_data = item_data.get("shop_data") or {}
    shop_data = shop_data if isinstance(shop_data, dict) else {}
    brand     = item_data.get("global_brand") or {}
    brand     = brand if isinstance(brand, dict) else {}

    name  = asset.get("name", "") or ""
    price = disp_price.get("price", 0) or 0

    # Only treat this as the new shape when there's a real signal (name or price
    # or an explicit displayed-asset block); otherwise let the caller fall through
    # to the older container/flat branches.
    if not (name or price or asset):
        return None

    return {
        "itemid": itemid,
        "shopid": shopid,
        "name": name,
        "brand": brand.get("display_name", "") or "",
        "shop_name": shop_data.get("shop_name", "") or "",
        "shop_location": asset.get("shop_location", "") or "",
        "catid": item_data.get("catid", 0) or 0,
        "price": price,
        "price_before_discount": (disp_price.get("strikethrough_price")
                                  or disp_price.get("original_price") or 0),
        "price_min": price,
        "price_max": disp_price.get("strikethrough_price", 0) or 0,
        "raw_discount": disp_price.get("discount", 0) or 0,
        "is_on_flash_sale": disp_price.get("promotion_type") == 301,
        "sold": sold.get("monthly_sold_count", 0) or 0,
        "historical_sold": sold.get("historical_sold_count", 0) or 0,
        "liked_count": item_data.get("liked_count", 0) or 0,
        "shopee_verified": item_data.get("shopee_verified", False) or False,
        "show_shopee_verified_label": item_data.get("shopee_verified", False) or False,
        "ctime": item_data.get("ctime", 0) or 0,
        "item_rating": item_data.get("item_rating") or {},
        "item_card_display_price": disp_price,
        "image": asset.get("image", "") or "",
        "images": asset.get("images", []) or [],
        "label_ids": item_data.get("label_ids") or [],
        "tier_variations": item_data.get("tier_variations") or [],
    }


def _resolve_item_info(item_wrap: dict) -> Optional[dict]:
    """
    Return the dict that actually holds the product fields
    (itemid / shopid / name / price …), tolerating the several
    result shapes Shopee has shipped over time:

      • classic : {"item_basic": {…}}
      • split    : {"item_basic": null, "item_data": {…},
                    "item_card_displayed_asset": {…}}   (new 2026 PC search)
      • nested  : {"item_data": {"item_basic_data"|"item_basic": {…}}}
      • flat    : {itemid, shopid, price, …}  (fields at the top level)

    Returns None if nothing recognisable is found.
    """
    if not isinstance(item_wrap, dict):
        return None

    # 1. Classic location — value may be a populated dict.
    info = item_wrap.get("item_basic")
    if isinstance(info, dict) and (info.get("itemid") or info.get("shopid")):
        return info

    # 2. New split shape (2026): fields spread across item_data +
    #    item_card_displayed_asset. Merge them into one classic-style dict.
    if isinstance(item_wrap.get("item_card_displayed_asset"), dict) or \
       isinstance(item_wrap.get("item_data"), dict):
        merged = _merge_new_item_shape(item_wrap)
        if merged is not None:
            return merged

    # 3. Older sibling containers that hold the product dict.
    for container_key in ("item_data", "item", "data"):
        container = item_wrap.get(container_key)
        if isinstance(container, dict):
            for inner_key in ("item_basic_data", "item_basic", "item_card_data"):
                inner = container.get(inner_key)
                if isinstance(inner, dict) and (inner.get("itemid") or inner.get("shopid")):
                    return inner
            if container.get("itemid") or container.get("shopid"):
                return container

    # 4. Flat structure — the fields already sit at the top level.
    if item_wrap.get("itemid") and item_wrap.get("shopid"):
        return item_wrap

    return None


def _map_product(item_wrap: dict) -> ProductModel:
    info = _resolve_item_info(item_wrap)
    if info is None:
        # Structure not recognised — record it once for diagnosis, then fail
        # loudly so the caller's error counter reports it (instead of silently
        # producing a garbage/empty row).
        _dump_raw_item_debug(item_wrap)
        raise ValueError("Unrecognised item structure (no item_basic / itemid found)")
    # If we resolved an id-bearing dict but the human-readable fields are
    # missing, Shopee has nested them elsewhere — dump the shape once so we
    # can see exactly where name / price / shop_name moved.
    if not (info.get("name") or info.get("shop_name")):
        _dump_raw_item_debug(item_wrap, info, context="thin-item")
    item_config = item_wrap.get("item_config") or info.get("item_config") or {}
    rating = info.get("item_rating") or {}
    rating_counts = rating.get("rating_count", [0] * 6)

    def _safe(lst, i): return lst[i] if len(lst) > i else 0

    product_url = f"https://shopee.co.th/product/{info.get('shopid')}/{info.get('itemid')}"
    ctime_raw = info.get("ctime", 0) or 0
    ctime_dt = datetime.fromtimestamp(ctime_raw, tz=TZ_THAI) if ctime_raw else datetime.now(tz=TZ_THAI)
    product_age_days = (datetime.now(tz=TZ_THAI) - ctime_dt).days

    is_ads = False
    matched_keywords = []
    # These historically lived on the wrap alongside item_basic; on the
    # null-basic shape they can sit inside the resolved info dict instead.
    traffic_source = item_wrap.get("traffic_source", info.get("traffic_source", 0)) or 0
    try:
        tracking = json.loads(
            item_wrap.get("bff_item_tracking") or info.get("bff_item_tracking") or "{}"
        )
        is_ads = tracking.get("is_ads", False)
        matched_keywords = list(set(tracking.get("matched_keywords", [])))
    except Exception:
        pass

    video_url = None
    has_video = False
    video_list = info.get("video_info_list") or []
    if video_list:
        has_video = True
        try:
            video_url = video_list[0].get("default_format", {}).get("url")
        except Exception:
            pass

    bundle_deal_label = None
    bundle_info = info.get("bundle_deal_info")
    if bundle_info:
        bundle_deal_label = bundle_info.get("bundle_deal_label")

    display_price = info.get("item_card_display_price") or {}
    promotion_type = display_price.get("promotion_type")

    return ProductModel(
        itemid=info.get("itemid", 0),
        shopid=info.get("shopid", 0),
        name=info.get("name", ""),
        brand=info.get("brand", ""),
        shop_name=info.get("shop_name", ""),
        shop_location=info.get("shop_location", ""),
        product_url=product_url,
        catid=info.get("catid", 0) or 0,
        price=info.get("price", 0) / 100000,
        price_original=info.get("price_before_discount", 0) / 100000,
        price_min=info.get("price_min", 0) / 100000,
        price_max=info.get("price_max", 0) / 100000,
        discount_pct=info.get("raw_discount", 0) or 0,
        promotion_type=promotion_type,
        bundle_deal_label=bundle_deal_label,
        is_on_flash_sale=info.get("is_on_flash_sale", False) or False,
        is_lowest_price=info.get("is_lowest_price", False) or False,
        sold_monthly=info.get("sold", 0) or 0,
        sold_total=info.get("historical_sold", 0) or 0,
        liked_count=info.get("liked_count", 0) or 0,
        view_count=info.get("view_count"),
        stock=info.get("stock", 0) or 0,
        ctime=ctime_dt,
        product_age_days=product_age_days,
        rating_star=rating.get("rating_star", 0.0) or 0.0,
        rating_count=rating_counts[0] if rating_counts else 0,
        rating_with_text=rating.get("rcount_with_context", 0) or 0,
        rating_with_image=rating.get("rcount_with_image", 0) or 0,
        comment_count=info.get("cmt_count", 0) or 0,
        rating_1star=_safe(rating_counts, 1),
        rating_2star=_safe(rating_counts, 2),
        rating_3star=_safe(rating_counts, 3),
        rating_4star=_safe(rating_counts, 4),
        rating_5star=_safe(rating_counts, 5),
        is_official_shop=info.get("is_official_shop", False) or False,
        is_shopee_verified=info.get("show_shopee_verified_label", False) or False,
        shopee_verified=info.get("shopee_verified", False) or False,
        is_preferred_plus_seller=info.get("is_preferred_plus_seller", False) or False,
        can_use_cod=info.get("can_use_cod", False) or False,
        is_mart=info.get("is_mart", False) or False,
        can_use_wholesale=info.get("can_use_wholesale", False) or False,
        is_ads=is_ads,
        traffic_source=traffic_source,
        matched_keywords=matched_keywords,
        item_identities=item_wrap.get("item_identities") or info.get("item_identities") or [],
        label_ids=info.get("label_ids") or [],
        image_url=f"{IMG_BASE}{info['image']}" if info.get("image") else "",
        image_urls=[f"{IMG_BASE}{img}" for img in info.get("images", []) if img],
        has_video=has_video,
        video_url=video_url,
        tier_variations=info.get("tier_variations") or [],
        item_config=item_config,
        collected_at=make_collected_at(),
    )

def _product_to_row(product: ProductModel) -> dict:
    row = product.model_dump()
    row["ctime"] = product.ctime.strftime("%Y-%m-%d %H:%M:%S") if product.ctime else ""
    return row

def _review_to_row(r: ReviewModel) -> dict:
    return r.model_dump()

# ════════════════════════════════════════════════════════════════
# Review Page Parser
# ════════════════════════════════════════════════════════════════
async def _parse_reviews_on_page(page, product_url: str) -> List[ReviewModel]:
    """
    Parse review cards from the current page.
    """
    reviews: List[ReviewModel] = []

    # Primary selector — try with parent, then bare, then legacy, then new structures.
    # Use a full timeout only for the most likely primary selector; fall through to
    # fallbacks with a short timeout so empty-page detection doesn't stall 42+ seconds.
    SELECTORS = (
        ".shopee-product-comment-list .q2b7Oq[data-cmtid]",
        ".q2b7Oq[data-cmtid]",
        "div[class*='product-comment'] [data-cmtid]",
        "div[class*='rating-comment'] [data-cmtid]",
        ".shopee-product-rating",
        "div.shopee-comment-item",
        "div[class*='comment-item']",
    )
    cards = None
    for idx_sel, sel in enumerate(SELECTORS):
        try:
            # Give the primary selector a full 6s; fallbacks get 1.5s (already loaded if present)
            t = 6000 if idx_sel == 0 else 1500
            await page.wait_for_selector(sel, timeout=t)
            cards = await page.query_selector_all(sel)
            if cards and len(cards) > 0:
                print(f"   ✅ Found reviews using selector: {sel}")
                break
            cards = None  # selector matched but returned empty list — try next
        except Exception:
            continue

    if not cards:
        print("   ⚠️  No reviews found on this page")
        return reviews

    print(f"   📝 Found {len(cards)} reviews")

    for card in cards:
        try:
            # ── Star rating ──────────────────────────────────────────
            rating_star = 0
            # Try primary selector: .BnXxkm .rGdC5O (confirmed from DOM)
            star_container = await card.query_selector(".BnXxkm .rGdC5O")
            if star_container:
                svg_stars = await star_container.query_selector_all("svg.YBGCRA")
                rating_star = min(len(svg_stars), 5)
            # Fallback 1: just .rGdC5O (rating container alone)
            if rating_star == 0:
                star_container = await card.query_selector(".rGdC5O")
                if star_container:
                    svg_stars = await star_container.query_selector_all("svg.YBGCRA")
                    rating_star = min(len(svg_stars), 5)
            # Fallback 2: .BnXxkm alone (star wrapper alone)
            if rating_star == 0:
                star_container = await card.query_selector(".BnXxkm")
                if star_container:
                    svg_stars = await star_container.query_selector_all("svg.YBGCRA")
                    rating_star = min(len(svg_stars), 5)

            # ── Date & Option ─────────────────────────────
            raw_comment_date = ""
            for date_sel in (".XYk98l", ".j5ucs4 > div", ".shopee-product-rating__time"):
                el = await card.query_selector(date_sel)
                if el:
                    t = (await el.inner_text()).strip()
                    if t:
                        raw_comment_date = t
                        break

            comment_date = raw_comment_date
            option_from_date = ""

            # Extract Variation from comment_date if present (e.g. "2026-05-26 21:07 | Variation: Solei")
            if " | " in raw_comment_date:
                parts = raw_comment_date.split(" | ", 1)
                comment_date = parts[0].strip()
                var_part = parts[1].strip()
                
                # Clean up prefix like "Variation:" or "Variasi:" to leave only the option name
                option_from_date = re.sub(r'^(Variation|Variasi|ตัวเลือกสินค้า):\s*', '', var_part, flags=re.IGNORECASE).strip()

            # Extract date only (YYYY-MM-DD), discard time portion
            date_match = re.search(r'\d{4}-\d{2}-\d{2}', comment_date)
            if date_match:
                comment_date = date_match.group(0)

            # ── Username ─────────────────────────────────────────────
            user_name = ""
            for name_sel in (".InK5kS", ".shopeescore__name", ".shopee-product-rating__author", ".shopee-product-rating__author-name"):
                el = await card.query_selector(name_sel)
                if el:
                    t = (await el.inner_text()).strip()
                    if t:
                        user_name = t
                        break

            # ── Variation + Comment ───────────────────────
            raw_comment = ""
            variation = ""

            meq = await card.query_selector(".meQyXP")
            if meq:
                extracted = await meq.evaluate("""node => {
                    const out = { comment: '', variation: '', option: '' };

                    // Comment lives in .YNedDV
                    const cd = node.querySelector('.YNedDV');
                    if (cd) out.comment = cd.innerText.trim();

                    // Variation rows: span.K5v3lN (label) + next sibling span (value)
                    const pairs = [];
                    node.querySelectorAll('span.K5v3lN').forEach(lb => {
                        const label = lb.innerText.replace(/:$/, '').trim();
                        const vEl = lb.nextElementSibling;
                        const value = vEl ? vEl.innerText.trim() : '';
                        if (!label || !value) return;
                        if (/^(Option|Variasi|Variation)$/i.test(label)) {
                            out.option = value;
                        } else {
                            pairs.push(label + ': ' + value);
                        }
                    });
                    if (pairs.length > 0) {
                        out.variation = pairs.join(' | ');
                    }
                    return out;
                }""") or {}
                raw_comment = extracted.get("comment", "")
                variation = extracted.get("variation", "")
                option_from_data = extracted.get("option", "")
                # option_from_data from .meQyXP takes priority over date-embedded option
                if option_from_data:
                    option_from_date = option_from_data  # unified name used in ReviewModel below
            else:
                # Legacy layout: comment directly in card
                for sel in (".shopee-product-rating__content",):
                    el = await card.query_selector(sel)
                    if el:
                        raw_comment = (await el.inner_text()).strip()
                        break

            # Join variation and comment as requested
            if variation and raw_comment:
                joined_comment = f"{variation} | {raw_comment}"
            elif variation:
                joined_comment = variation
            else:
                joined_comment = raw_comment

            reviews.append(ReviewModel(
                product_url=product_url,
                user_name=user_name,
                comment_date=comment_date,
                option=option_from_date,
                variation=variation,
                comment=joined_comment,
                rating_star=rating_star,
                collected_at=make_collected_at(),
            ))
        except Exception as e:
            print(f"   ⚠️  Parse review error: {e}")
            continue
            
    return reviews


async def _is_product_not_found(page) -> bool:
    """Detect Shopee's 'product not found / delisted' page state, so callers
    can skip immediately instead of waiting out the full ~20s filter-section
    timeout (and, in the by-stars flow, an unwarranted session-blocked
    cooldown) for a listing that will never render a review section."""
    try:
        return await page.evaluate("""() => {
            const text = document.body.innerText || "";
            return /ไม่พบสินค้า|page not found|this page is not available/i.test(text);
        }""")
    except Exception:
        return False


# ════════════════════════════════════════════════════════════════
# Human-like Navigation Helper
# ════════════════════════════════════════════════════════════════

async def _human_reopen_product(page, product_url: str, label: str) -> bool:
    """แวะ homepage แล้วค่อยเปิดหน้าสินค้าใหม่แบบมนุษย์"""
    try:
        await page.goto("https://shopee.co.th", wait_until="domcontentloaded", timeout=30000)
    except Exception:
        pass

    browse_time = random.uniform(4.0, 9.0)
    deadline = asyncio.get_running_loop().time() + browse_time
    while asyncio.get_running_loop().time() < deadline:
        await _move_mouse_randomly(page)
        await page.mouse.wheel(0, random.randint(100, 350))
        await page.wait_for_timeout(random.randint(600, 1400))

    await asyncio.sleep(random.uniform(2.0, 5.0))

    try:
        await page.goto(product_url, wait_until="domcontentloaded", timeout=60000)
    except Exception as e:
        print(f"   ❌ Re-open failed: {e}")
        return False

    if await _is_captcha_detected(page):
        return False

    await page.wait_for_timeout(random.randint(2500, 5000))
    for _ in range(random.randint(2, 4)):
        await _move_mouse_randomly(page)
        await page.mouse.wheel(0, random.randint(150, 400))
        await page.wait_for_timeout(random.randint(700, 1800))

    print(f"   🖱️  Scrolling to review section ({label})...")
    for _ in range(12):
        await _human_like_scroll(page, random.randint(500, 750))
        await page.wait_for_timeout(random.randint(700, 1300))
        if await page.query_selector(".product-rating-overview__filters"):
            print("   ✅ Found review section!")
            break
    return True


# ════════════════════════════════════════════════════════════════
# Session Setup
# ════════════════════════════════════════════════════════════════

async def create_shopee_session():
    """Login to Shopee and save browser session. Run once on first use or after expiry."""
    context, page = await create_browser_context(block_images=False)
    await page.goto("https://shopee.co.th", wait_until="domcontentloaded", timeout=60000)
    print("\n" + "—" * 60)
    print("⚡ [SESSION] Browser is active.")
    print("—" * 60)
    print(" 1. Login Shopee manually.")
    print(" 2. Complete any verification if prompted.")
    print(" 3. Close browser window when finished.")
    print("—" * 60 + "\n")
    while len(context.pages) > 0:
        await asyncio.sleep(1)
    print(f"✅ [SUCCESS] Session saved to '{USER_DATA_DIR}'.")


# ════════════════════════════════════════════════════════════════
# Scraper 1: Shop Products
# ════════════════════════════════════════════════════════════════

async def run_shop_products(shop_urls: List[str]) -> pd.DataFrame:
    """ดึงรายการสินค้าทั้งหมดจากหน้าร้านค้า Shopee"""
    scraper_name = "shop_products"
    all_products = load_existing_backup("temp_shopee_shop_products", SHOP_DEDUPE_COLS) if RESUME_FROM_BACKUP else []
    log_rows = []
    failed_shops = []
    seen = {
        (str(r.get("itemid", "")), str(r.get("shopid", "")), str(r.get("product_url", "")))
        for r in all_products
    }

    print(f"📂 [SESSION] Loading: '{USER_DATA_DIR}'")
    context, page = await create_browser_context(block_images=True)
    async with context:
        try:
            for idx, shop_url in enumerate(shop_urls, start=1):
                base_shop_url = shop_url.split("?")[0]
                shop_name = base_shop_url.rstrip("/").split("/")[-1] or "unknown_shop"
                shop_key_str = shop_name  # Keep both to prevent NameError in logging
                print("\n" + "-" * 60)
                print(f"[{idx}/{len(shop_urls)}] Shop: {shop_name}")

                shop_products_intercepted: dict = {}

                def make_intercept_handler(dst: dict):
                    async def _handler(response):
                        try:
                            if response.request.method == "OPTIONS":
                                return
                            url_lower = response.url.lower()
                            if not any(k in url_lower for k in ["search_items", "get_shop_items", "get_shop_products"]):
                                return
                            body = await response.json()

                            def find_item_wraps(data):
                                res = []
                                if isinstance(data, dict):
                                    if "item_basic" in data:
                                        res.append(data)
                                    elif "itemid" in data and "shopid" in data and "price" in data:
                                        res.append({"item_basic": data})
                                    else:
                                        for v in data.values():
                                            res.extend(find_item_wraps(v))
                                elif isinstance(data, list):
                                    for item in data:
                                        res.extend(find_item_wraps(item))
                                return res

                            for wrap in find_item_wraps(body):
                                try:
                                    product = _map_product(wrap)
                                    if product.itemid and product.itemid not in dst:
                                        dst[product.itemid] = product
                                except Exception:
                                    pass
                        except Exception:
                            pass
                    return _handler

                handler = make_intercept_handler(shop_products_intercepted)
                page.on("response", handler)

                try:
                    await page.goto(base_shop_url, wait_until="domcontentloaded", timeout=60000)
                    await page.wait_for_timeout(random.randint(3000, 5000))

                    if await _is_captcha_detected(page):
                        print("🚨 Captcha detected on shop page")
                        if not await _handle_captcha(page, base_shop_url):
                            failed_shops.append({"shop_url": shop_url, "error": "Captcha not recovered"})
                            add_scrape_log(log_rows, scraper_name, "ERROR", "Captcha not recovered", shop_url=shop_url)
                            continue

                    # Click All Products tab
                    try:
                        all_tab = page.locator("a.navbar-with-more-menu__item").filter(
                            has_text=re.compile(r"^(All Products|สินค้าทั้งหมด)$", re.IGNORECASE)
                        ).first
                        if await all_tab.count() == 0:
                            all_tab = page.locator(
                                "a.navbar-with-more-menu__item[href*='product_list']:not([href*='shopCollection'])"
                            ).first
                        if await all_tab.count() > 0:
                            await _close_shopee_popups(page)
                            await all_tab.scroll_into_view_if_needed()
                            await page.wait_for_timeout(random.randint(500, 1000))
                            await all_tab.click()
                            print("   ✅ Clicked 'All Products' tab")
                            await page.wait_for_timeout(random.randint(2000, 3500))
                    except Exception as e:
                        print(f"   ⚠️ Could not click All Products tab: {e}")

                    # Scroll to section
                    await page.evaluate("""() => {
                        const sec = document.querySelector('.shop-all-product-view');
                        if (sec) sec.scrollIntoView({behavior: 'smooth', block: 'start'});
                    }""")
                    await page.wait_for_timeout(random.randint(2000, 3000))

                    # Read total pages
                    max_pages = 1
                    try:
                        await page.wait_for_selector(".shopee-mini-page-controller__total", timeout=5000)
                        max_pages = int(await page.inner_text(".shopee-mini-page-controller__total"))
                        await page.evaluate("window.scrollBy(0, 600)")
                        await page.wait_for_timeout(random.randint(1500, 2500))
                        print(f"   📊 {max_pages} pages found")
                    except Exception:
                        try:
                            await page.wait_for_selector(".shopee-page-controller", timeout=10000)
                            max_pages = await page.evaluate("""() => {
                                const ctrl = document.querySelector('.shopee-page-controller');
                                if (!ctrl) return 1;
                                const nums = Array.from(ctrl.querySelectorAll('button'))
                                    .map(b => b.innerText.trim())
                                    .filter(t => /^\\d+$/.test(t))
                                    .map(n => parseInt(n, 10));
                                return nums.length > 0 ? Math.max(...nums) : 1;
                            }""")
                            print(f"   📊 {max_pages} pages found (from bottom controller)")
                        except Exception:
                            print("   ⚠️ Pagination not found — assuming 1 page")

                    current_page = 1
                    while current_page <= max_pages:
                        print(f"\n   📄 Page {current_page}/{max_pages}")

                        if await _is_captcha_detected(page):
                            if not await _handle_captcha(page, base_shop_url):
                                break

                        # Scroll to load cards
                        target = page.locator(".shop-all-product-view .shopee_ic")
                        if await target.count() == 0:
                            target = page.locator(".shop-search-result-view__item .shopee_ic")

                        prev_count = 0
                        stuck = 0
                        for _ in range(20):
                            await page.evaluate("window.scrollBy(0, 600)")
                            await page.wait_for_timeout(random.randint(700, 1200))
                            cur = await target.count()
                            if cur == prev_count and cur > 0:
                                stuck += 1
                                if stuck >= 3:
                                    break
                            else:
                                stuck = 0
                            prev_count = cur

                        # DOM fallback for items not caught by interceptor
                        product_cards = await target.all()
                        for card in product_cards:
                            try:
                                a_tag = card.locator("a[href*='-i.']").first
                                if await a_tag.count() == 0:
                                    a_tag = card.locator("a").first
                                if await a_tag.count() == 0:
                                    continue
                                href = await a_tag.get_attribute("href")
                                if not href:
                                    continue
                                m = re.search(r"-i\.(\d+)\.(\d+)", href) or re.search(r"i\.(\d+)\.(\d+)", href)
                                if not m:
                                    continue
                                shopid, itemid = int(m.group(1)), int(m.group(2))
                                if itemid in shop_products_intercepted:
                                    continue
                                card_text = await card.evaluate("node => node.innerText") or ""
                                name = ""
                                img_tag = card.locator("img[alt]").first
                                if await img_tag.count() > 0:
                                    name = await img_tag.get_attribute("alt") or ""

                                # FIX: Prefer clean `src` over srcset to avoid @resize suffix.
                                # New Shopee layout uses <picture><source srcset="...@resize_w320...">
                                # <img src="..."> — the img src is the cleanest URL.
                                img_url = await card.evaluate("""(node) => {
                                    const PLACEHOLDER = 'data:image/gif;base64,R0lGODlh';
                                    const imgs = node.querySelectorAll('img');
                                    for (const img of imgs) {
                                        // Prefer src first (clean URL without @resize suffix)
                                        const src = img.getAttribute('src') || '';
                                        if (src && !src.startsWith(PLACEHOLDER) && !src.startsWith('data:')
                                                && src.startsWith('http')) {
                                            // Strip any @resize_... query fragment
                                            return src.split('@')[0].split('?')[0];
                                        }
                                        // Fallback: lazy-load attrs, take first URL token from srcset
                                        const attrs = ['data-src', 'data-ks-lazyload', 'data-lazy', 'data-original'];
                                        for (const attr of attrs) {
                                            const v = img.getAttribute(attr) || '';
                                            if (v && !v.startsWith(PLACEHOLDER) && !v.startsWith('data:')) {
                                                const first = v.split(/[,\\s]/)[0].trim().split('@')[0];
                                                if (first.startsWith('http') || first.startsWith('//')) {
                                                    return first.startsWith('//') ? 'https:' + first : first;
                                                }
                                            }
                                        }
                                    }
                                    return '';
                                }""") or ""

                                if not name:
                                    name = href.split("?")[0].split("/")[-1].replace("-", " ")
                                prices = re.findall(r"฿\s*([\d,]+(?:\.\d+)?)", card_text)
                                price_val = float(prices[0].replace(",", "")) if prices else 0.0

                                # FIX: Shopee now renders sold count as Tailwind space-separated
                                # classes — e.g. class="truncate text-shopee-black87 text-xs"
                                # (not dot-joined). Also the text format is "ขายได้ X ชิ้น/เดือน".
                                sold_text = await card.evaluate("""node => {
                                    // New layout: Tailwind space-separated classes
                                    const candidates = node.querySelectorAll(
                                        'div[class*="text-shopee-black87"], span[class*="text-shopee-black87"]'
                                    );
                                    for (const el of candidates) {
                                        const t = el.innerText.trim();
                                        if (/ขายได้|ขายแล้ว|sold/i.test(t)) return t;
                                    }
                                    // Fallback: any element whose text contains sold-related keywords
                                    const divs = Array.from(node.querySelectorAll('div, span'));
                                    const found = divs.find(d => {
                                        const t = d.innerText.trim();
                                        return /ขายได้|ขายแล้ว/i.test(t) || /\\d+.*sold/i.test(t);
                                    });
                                    return found ? found.innerText.trim() : '';
                                }""") or ""

                                shop_products_intercepted[itemid] = ProductModel(
                                    itemid=itemid, shopid=shopid, name=name.strip(),
                                    product_url=f"https://shopee.co.th{href}",
                                    price=price_val, price_original=price_val,
                                    ctime=datetime.fromtimestamp(0, tz=TZ_THAI),
                                    image_url=img_url,
                                    sold_total=parse_sold_count(sold_text),
                                    shop_location="",
                                    collected_at=make_collected_at(),
                                )
                            except Exception:
                                pass

                        # Merge intercepted + DOM results for this page
                        new_rows_count = 0
                        for product in shop_products_intercepted.values():
                            row = _product_to_row(product)
                            row["shop_key"] = shop_key_str  # Inject the shop key
                            key = (str(row["itemid"]), str(row["shopid"]), str(row["product_url"]))
                            if key not in seen:
                                all_products.append(row)
                                seen.add(key)
                                new_rows_count += 1

                        print(f"   ✅ Page {current_page}: {new_rows_count} new products | Total: {len(all_products):,}")

                        if current_page % SHOP_CHECKPOINT_EVERY_PAGES == 0:
                            add_scrape_log(log_rows, scraper_name, "INFO", "Checkpoint", shop=shop_name, page=current_page)
                            save_scrape_checkpoint(all_products, "temp_shopee_shop_products", SHOP_DEDUPE_COLS, f"page {current_page}")

                        if current_page < max_pages:
                            next_page_num = current_page + 1

                            # Snapshot first card href before clicking — used to confirm real DOM change
                            old_first_href = await page.evaluate("""() => {
                                const a = document.querySelector('.shop-all-product-view .shopee_ic a[href*="-i."], .shopee_ic a[href*="-i."]');
                                return a ? a.getAttribute('href') : '';
                            }""") or ""

                            # FIX: the bottom `.shopee-page-controller` sits right after the
                            # product grid and keeps shifting position while cards/images are
                            # still lazy-rendering — that's why Playwright's actionability
                            # ("stable") check on hover()/click() kept timing out, even with
                            # force=True (force skips the check on click but NOT on the hover
                            # that ran right before it, and the position can still be wrong
                            # under the mouse at click time). The TOP `.shopee-mini-page-controller`
                            # (the same element we already read max_pages from) lives in the
                            # sort bar above the grid and never reflows from card loading, so
                            # try that first. Fall back to the bottom controller (now with a
                            # real scroll + bounding-box-stability wait) only if the mini one
                            # isn't present, then finally fall back to URL navigation.

                            async def _wait_box_stable(loc, checks=3, interval=150, max_iters=20):
                                last = None
                                stable_count = 0
                                for _ in range(max_iters):
                                    try:
                                        box = await loc.bounding_box()
                                    except Exception:
                                        box = None
                                    if box and last and (
                                        abs(box["x"] - last["x"]) < 0.5
                                        and abs(box["y"] - last["y"]) < 0.5
                                    ):
                                        stable_count += 1
                                        if stable_count >= checks:
                                            return True
                                    else:
                                        stable_count = 0
                                    last = box
                                    await page.wait_for_timeout(interval)
                                return False

                            clicked = False
                            click_method = None

                            # --- Attempt 1: top mini-controller (stable, same place max_pages came from) ---
                            try:
                                mini_next = page.locator(".shopee-mini-page-controller__next-btn").first
                                if await mini_next.count() > 0:
                                    is_disabled = await mini_next.evaluate(
                                        "(el) => el.disabled || el.classList.contains('shopee-button-outline--disabled')"
                                    )
                                    if not is_disabled:
                                        await _move_mouse_randomly(page)
                                        await mini_next.scroll_into_view_if_needed()
                                        await _wait_box_stable(mini_next)
                                        await mini_next.hover(force=True, timeout=5000)
                                        await page.wait_for_timeout(random.randint(200, 500))
                                        await mini_next.click(force=True, timeout=5000)
                                        clicked = True
                                        click_method = "mini_controller"
                            except Exception as e:
                                print(f"   ⚠️ Mini next-button error: {e}")

                            # --- Attempt 2: bottom page controller (settles after scroll + wait) ---
                            if not clicked:
                                try:
                                    bottom_next = page.locator(
                                        ".shopee-page-controller .shopee-icon-button--right"
                                    ).first
                                    if await bottom_next.count() > 0:
                                        await bottom_next.scroll_into_view_if_needed()
                                        await _wait_box_stable(bottom_next)
                                        is_disabled = await bottom_next.evaluate(
                                            "(el) => el.disabled || el.classList.contains('shopee-icon-button--disabled')"
                                        )
                                        if not is_disabled:
                                            await _move_mouse_randomly(page)
                                            await bottom_next.hover(force=True, timeout=5000)
                                            await page.wait_for_timeout(random.randint(200, 500))
                                            await bottom_next.click(force=True, timeout=5000)
                                            clicked = True
                                            click_method = "bottom_controller"
                                except Exception as e:
                                    print(f"   ⚠️ Bottom next-button error: {e}")

                            if clicked:
                                try:
                                    print(f"   ➡️ Advanced via {click_method}")
                                    # Wait for the active page indicator (mini controller's current
                                    # page count, or the bottom controller's highlighted button) to
                                    # advance to next_page_num.
                                    await page.wait_for_function(
                                        f"""() => {{
                                            const miniCur = document.querySelector('.shopee-mini-page-controller__current');
                                            if (miniCur && miniCur.innerText.trim() === '{next_page_num}') return true;
                                            const btns = document.querySelectorAll('.shopee-page-controller button');
                                            for (const btn of btns) {{
                                                if (btn.classList.contains('shopee-button-solid--primary') &&
                                                    btn.innerText.trim() === '{next_page_num}') {{
                                                    return true;
                                                }}
                                            }}
                                            return false;
                                        }}""",
                                        timeout=15000,
                                    )
                                    # Now wait for actual product cards to change (DOM sync)
                                    deadline = asyncio.get_running_loop().time() + 10
                                    while asyncio.get_running_loop().time() < deadline:
                                        await page.wait_for_timeout(600)
                                        new_href = await page.evaluate("""() => {
                                            const a = document.querySelector('.shop-all-product-view .shopee_ic a[href*="-i."], .shopee_ic a[href*="-i."]');
                                            return a ? a.getAttribute('href') : '';
                                        }""") or ""
                                        if new_href and new_href != old_first_href:
                                            break
                                    await page.wait_for_timeout(random.randint(1000, 2000))
                                except Exception:
                                    # FIX: URL fallback uses next_page_num (not current_page)
                                    next_url = f"{base_shop_url}?page={next_page_num - 1}&sortBy=pop&tab=all#product_list"
                                    await page.goto(next_url, wait_until="domcontentloaded", timeout=60000)
                                    await page.wait_for_timeout(random.randint(3000, 5000))
                            else:
                                # FIX: URL fallback uses next_page_num (not current_page)
                                next_url = f"{base_shop_url}?page={next_page_num - 1}&sortBy=pop&tab=all#product_list"
                                try:
                                    await page.goto(next_url, wait_until="domcontentloaded", timeout=60000)
                                    await page.wait_for_timeout(random.randint(3000, 5000))
                                except Exception as e:
                                    print(f"   ❌ URL fallback failed: {e}")
                                    break

                            if current_page % 5 == 0:
                                extra = random.uniform(2, 5)
                                print(f"   ⏸️ Long pause {extra:.1f}s...")
                                await asyncio.sleep(extra)

                        await cooldown_tick()
                        current_page += 1
                        shop_products_intercepted.clear()

                    add_scrape_log(log_rows, scraper_name, "INFO", "Shop done",
                                   shop=shop_name, total=len(all_products))

                except Exception as e:
                    print(f"❌ [SKIP] Shop '{shop_name}': {e}")
                    failed_shops.append({"shop_url": shop_url, "error": str(e)})
                    add_scrape_log(log_rows, scraper_name, "ERROR", "Error scraping shop",
                                   shop_url=shop_url, error=str(e))
                    save_scrape_checkpoint(all_products, "temp_shopee_shop_products", SHOP_DEDUPE_COLS, "shop error")
                finally:
                    try:
                        page.remove_listener("response", handler)
                    except Exception:
                        pass

        finally:
            save_scrape_checkpoint(all_products, "temp_shopee_shop_products", SHOP_DEDUPE_COLS, "final backup")

    # Map raw data to the requested output format
    processed_products = []
    processed_products = []
    for row in all_products:
        discount_price = row.get("price", 0.0)
        
        # Robustly check all possible fields for the sold amount
        sold_count = row.get("sold_total", 0)
        if not sold_count:
            sold_count = row.get("sold_monthly", 0)
        if not sold_count and isinstance(row.get("shop_location"), str):
            sold_count = parse_sold_count(row.get("shop_location", ""))
        
        processed_products.append({
            "product_id": row.get("itemid"),
            "sku_id": "",  # Standard shop scrape doesn't yield variation SKU IDs
            "product_name": row.get("name"),
            "brand_name": row.get("brand"),
            "discount_price": discount_price,
            "original_price": row.get("price_original"),
            "sold_count": sold_count,
            "rating_score": row.get("rating_star"),
            "review_count": row.get("rating_count"),
            "in_stock": row.get("stock"),
            "is_sponsored": row.get("is_ads"),
            "shop_id": row.get("shopid"),
            "shop_name": row.get("shop_name"),
            "shop_key": row.get("shop_key", ""),
            "is_mall": row.get("is_official_shop"),
            "location": row.get("shop_location"),
            "product_url": row.get("product_url"),
            "image_url": row.get("image_url"),
            "source_platform": row.get("source_platform", "Shopee"),
            "collected_at": row.get("collected_at"),
            "sales_values": discount_price * sold_count
        })

    SHOP_OUTPUT_COLS = [
        "product_id", "sku_id", "product_name", "brand_name", "discount_price",
        "original_price", "sold_count", "rating_score", "review_count", "in_stock",
        "sales_values","product_url", "image_url", "source_platform", "collected_at"
    ]
    
    # --- NEW CODE: Extract brand/shop names from the URLs for the filename ---
    shop_names = []
    for u in shop_urls:
        base_u = u.split("?")[0]
        name = base_u.rstrip("/").split("/")[-1]
        if name:
            shop_names.append(name)
            
    # Join names with underscores (limit length to 100 characters to prevent OS file path errors)
    brands_str = "_".join(shop_names)[:100]
    print(f"\n📁 Output filename will include shop names: '{brands_str}'" if brands_str else "\n📁 No shop names extracted for filename.")
    final_prefix = f"shopee_shop_products_{brands_str}" if brands_str else "shopee_shop_products"
    # -------------------------------------------------------------------------

    # Save the strictly formatted data with the dynamic brand file name
    df, _ = save_final_data(
        processed_products, 
        final_prefix, 
        ["product_id", "shop_id", "product_url"], 
        output_cols=SHOP_OUTPUT_COLS
    )
    save_failed_log(failed_shops, "failed_shops", scraper_name)
    return df


# ════════════════════════════════════════════════════════════════
# Scraper 2: Product Detail (Specifications + Description)
# ════════════════════════════════════════════════════════════════

async def run_product_detail(product_urls: List[str]) -> pd.DataFrame:
    """ดึง Specifications และ Product Description จากหน้าสินค้า"""
    scraper_name = "product_detail"
    all_details = load_existing_backup("temp_shopee_details", DETAIL_DEDUPE_COLS) if RESUME_FROM_BACKUP else []
    log_rows = []
    failed_urls = []
    done_urls = {str(r.get("product_url", "")) for r in all_details}

    print(f"📂 [SESSION] Loading: '{USER_DATA_DIR}'")
    context, page = await create_browser_context(block_images=False)
    async with context:
        try:
            for idx, url in enumerate(product_urls, start=1):
                if url in done_urls:
                    print(f"   ⏭️ Skip already scraped: {url}")
                    continue
                print("\n" + "-" * 60)
                print(f"[{idx}/{len(product_urls)}] Detail: {url}")

                success = False
                for attempt in range(5):
                    try:
                        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                        await page.wait_for_timeout(random.randint(2500, 4500))

                        if await _is_captcha_detected(page):
                            print("🚨 Captcha detected on detail page")
                            recovered = await _handle_captcha(page, url)
                            if not recovered:
                                raise Exception("Captcha not recovered")

                        # Scroll until specs or description section is visible
                        for _ in range(9):
                            await _move_mouse_randomly(page)
                            await _human_like_scroll(page, random.randint(400, 700))
                            await page.wait_for_timeout(random.randint(600, 1000))
                            try:
                                found = await page.evaluate("""() =>
                                    document.body.innerText.includes('Product Description') ||
                                    document.body.innerText.includes('Product Specifications') ||
                                    document.body.innerText.includes('ข้อมูลจำเพาะ') ||
                                    document.body.innerText.includes('รายละเอียดสินค้า') ||
                                    document.querySelectorAll('div.ybxj32, label.q9bOvi, label').length > 0
                                """)
                                if found:
                                    break
                            except Exception:
                                pass

                        # Extract specs + description + image via JS
                        # Extract specs + description + image via JS
                        data = await page.evaluate("""() => {
                            const result = { specifications: {}, description: "", image_url: "" };

                            function assignRow(key, valueEl) {
                                if (!key || !valueEl) return false;
                                if (key === "Category" || key === "หมวดหมู่") {
                                    const links = valueEl.querySelectorAll("a");
                                    result.specifications[key] = links.length > 0
                                        ? Array.from(links).map(a => a.innerText.trim()).filter(t => t).join(" > ")
                                        : valueEl.innerText.trim();
                                } else {
                                    result.specifications[key] = valueEl.innerText.trim();
                                }
                                return true;
                            }

                            function extractRows(container) {
                                // Current layout (2026): each spec row is
                                //   <div><h3>KEY</h3><div>VALUE</div></div>
                                // The wrapper div's class is an obfuscated hash
                                // that Shopee rotates (was "ybxj32", now "c0ztn1",
                                // …), so match on the <h3> key node directly
                                // instead of any class name.
                                let got = 0;
                                container.querySelectorAll("h3").forEach(h3El => {
                                    const key = h3El.innerText.trim();
                                    if (assignRow(key, h3El.nextElementSibling)) got++;
                                });
                                if (got > 0) return;

                                // Older layout used <label> for the spec key.
                                container.querySelectorAll("label").forEach(labelEl => {
                                    assignRow(labelEl.innerText.trim(), labelEl.nextElementSibling);
                                });
                            }

                            let foundSpec = false, foundDesc = false;
                            
                            // Find all sections or generic containers by looking at h2 tags
                            document.querySelectorAll("h2").forEach(h2 => {
                                const title = h2.innerText.trim().toLowerCase();
                                const parent = h2.parentElement;
                                if (!parent) return;

                                if (!foundSpec && (title.includes("product specifications") || title.includes("ข้อมูลจำเพาะ") || title.includes("specifications"))) {
                                    extractRows(parent); 
                                    foundSpec = true;
                                }
                                
                                if (!foundDesc && (title.includes("product description") || title.includes("รายละเอียดสินค้า") || title.includes("description"))) {
                                    // The text might be in a nested div, or directly in the parent
                                    const contentDiv = parent.querySelector("div.e8lZp3, div.Gf4Ro0, span[style*='line-clamp']") || parent;
                                    let text = contentDiv.innerText.trim();
                                    
                                    // Clean up if the parent grabbed the h2 title text as well
                                    const rawTitle = h2.innerText.trim();
                                    if (text.startsWith(rawTitle)) {
                                        text = text.substring(rawTitle.length).trim();
                                    }
                                    result.description = text; 
                                    foundDesc = true;
                                }
                            });

                            if (Object.keys(result.specifications).length === 0) {
                                extractRows(document);
                            }

                            // Image Extraction targeting Shopee's new <picture> elements
                            const PLACEHOLDER = 'data:image/gif;base64,R0lGODlh';
                            const imgCandidates = [
                                document.querySelector('img.J3Az6e'),
                                document.querySelector('picture img'),
                                document.querySelector('div[class*="product-img"] img'),
                                document.querySelector('img[class*="product-image"]'),
                                ...Array.from(document.querySelectorAll('div[class*="pdp-"] img, div[class*="product"] img')).slice(0, 5),
                            ].filter(Boolean);
                            
                            for (const img of imgCandidates) {
                                const attrs = ['data-src', 'data-ks-lazyload', 'data-lazy', 'data-original', 'src'];
                                for (const attr of attrs) {
                                    const v = img.getAttribute(attr) || '';
                                    if (v && !v.startsWith(PLACEHOLDER) && !v.startsWith('data:') && v.startsWith('http')) {
                                        result.image_url = v.split('?')[0];
                                        break;
                                    }
                                }
                                if (result.image_url) break;
                            }

                            const s = result.specifications;
                            const knownAliases = new Set([
                                "Category","หมวดหมู่","Discount stock","Stock","คลัง","Other Stock",
                                "Brand","แบรนด์","Country of Origin","ประเทศต้นกำเนิด",
                                "Volume","ปริมาณ","Formulation","สูตร","Shelf Life","อายุการเก็บรักษา",
                                "Ships From","ส่งจาก","FDA Registration No.","หมายเลขใบอนุญาต/อย.",
                                "Ingredient Preference","Ingredient","ส่วนประกอบ"
                            ]);
                            const mapped = {};
                            mapped["Category"]           = s["Category"] || s["หมวดหมู่"] || "";
                            mapped["Discount stock"]     = s["Discount stock"] || s["Stock"] || s["คลัง"] || "";
                            mapped["Other Stock"]        = s["Other Stock"] || "";
                            mapped["Brand"]              = s["Brand"] || s["แบรนด์"] || "";
                            mapped["Country of Origin"]  = s["Country of Origin"] || s["ประเทศต้นกำเนิด"] || "";
                            mapped["Volume"]             = s["Volume"] || s["ปริมาณ"] || "";
                            mapped["Formulation"]        = s["Formulation"] || s["สูตร"] || "";
                            mapped["Shelf Life"]         = s["Shelf Life"] || s["อายุการเก็บรักษา"] || "";
                            mapped["Ships From"]         = s["Ships From"] || s["ส่งจาก"] || "";
                            mapped["Ingredient Preference"] = s["Ingredient Preference"] || s["Ingredient"] || s["ส่วนประกอบ"] || "Other";
                            mapped["FDA Registration No."]  = s["FDA Registration No."] || s["หมายเลขใบอนุญาต/อย."] || "Other";
                            mapped["Description"]        = result.description;
                            const otherParts = [];
                            Object.entries(s).forEach(([k, v]) => {
                                if (!knownAliases.has(k) && v) otherParts.push(k + ": " + v);
                            });
                            mapped["Other"] = otherParts.join(" | ");
                            result.specifications = mapped;
                            
                            return result;
                        }""")

                        specs = data.get("specifications", {})
                        if not specs:
                            if await _is_captcha_detected(page):
                                raise Exception("Captcha after empty specs")
                            raise Exception("Empty specs — page may not have loaded")

                        row = {
                            "product_url": url,
                            "image_url": data.get("image_url", ""),
                            **specs,
                            "collected_at": make_collected_at(),
                        }
                        all_details.append(row)
                        done_urls.add(url)
                        print(f"   ✅ [{idx}] Detail scraped | specs={len(specs)} fields")
                        success = True
                        break

                    except Exception as e:
                        err = str(e).splitlines()[0]
                        print(f"   ⚠️ Attempt {attempt + 1}/{5}: {err}")
                        if attempt < 4:
                            await asyncio.sleep(random.uniform(5.0, 8.0))

                if not success:
                    failed_urls.append({"product_url": url, "error": "Max retries exceeded"})
                    add_scrape_log(log_rows, scraper_name, "ERROR", "Max retries exceeded", product_url=url)

                if idx % DETAIL_CHECKPOINT_EVERY_URLS == 0:
                    add_scrape_log(log_rows, scraper_name, "INFO", "Checkpoint", url_index=idx)
                    save_scrape_checkpoint(all_details, "temp_shopee_details", DETAIL_DEDUPE_COLS, f"every {DETAIL_CHECKPOINT_EVERY_URLS} urls")

                await asyncio.sleep(random.uniform(1.0, 2.5))
                await cooldown_tick()

        finally:
            save_scrape_checkpoint(all_details, "temp_shopee_details", DETAIL_DEDUPE_COLS, "final backup")

    df, _ = save_final_data(all_details, "shopee_product_detail", DETAIL_DEDUPE_COLS)
    save_failed_log(failed_urls, "failed_details", scraper_name)
    return df


# ════════════════════════════════════════════════════════════════
# Scraper 3: Reviews — With Comments
# ════════════════════════════════════════════════════════════════

async def run_product_reviews_with_comments(product_urls: List[str]) -> pd.DataFrame:
    """ดึงรีวิวที่มี comment จากปุ่ม 'with comments' filter"""
    global with_comments_count_overflow
    scraper_name = "reviews"
    all_reviews = load_existing_backup("temp_shopee_reviews_comments", REVIEW_DEDUPE_COLS) if RESUME_FROM_BACKUP else []
    log_rows = []
    failed_urls = []
    pages_scraped_comments: dict = {}   # url -> (pages_done, pages_total)
    seen = {
        (str(r.get("product_url", "")), str(r.get("user_name", "")),
         str(r.get("comment_date", "")), str(r.get("comment", "")))
        for r in all_reviews
    }
    # URLs already fully paginated in a previous run — skip entirely instead of
    # re-opening the product page and re-walking every review page from page 1.
    done_urls = load_done_keys("temp_shopee_reviews_comments") if RESUME_FROM_BACKUP else set()
    MAX_CAPTCHA_RETRY = 8
    filter_text = "with comments"

    print(f"📂 [SESSION] Loading: '{USER_DATA_DIR}'")
    context, page = await create_browser_context(block_images=False)
    async with context:
        try:
            for idx, url in enumerate(product_urls, start=1):
                print("\n" + "-" * 60)
                print(f"[{idx}/{len(product_urls)}] Reviews (with comments): {url}")

                if url in done_urls:
                    print(f"   ⏭️ Skip — already fully scraped in a previous run")
                    continue

                fully_completed = False

                try:
                    reopen_ok = await _human_reopen_product(page, url, filter_text)
                    if not reopen_ok or await _is_captcha_detected(page):
                        recovered = await _handle_captcha(page, url)
                        if not recovered:
                            failed_urls.append({"product_url": url, "error": "Captcha not recovered"})
                            add_scrape_log(log_rows, scraper_name, "ERROR", "Captcha not recovered", product_url=url)
                            continue
                        for _ in range(12):
                            await _human_like_scroll(page, random.randint(500, 750))
                            await page.wait_for_timeout(random.randint(700, 1300))
                            if await page.query_selector(".product-rating-overview__filters"):
                                break

                    if await _is_product_not_found(page):
                        add_scrape_log(log_rows, scraper_name, "WARNING", "Product not found / delisted — skipping", product_url=url)
                        failed_urls.append({"product_url": url, "error": "Product not found / delisted"})
                        continue

                    # Wait for filter section
                    filter_container = page.locator(".product-rating-overview__filters").first
                    try:
                        await filter_container.wait_for(state="visible", timeout=20000)
                        await filter_container.scroll_into_view_if_needed()
                        await page.wait_for_timeout(random.randint(1500, 3000))
                    except Exception as e:
                        add_scrape_log(log_rows, scraper_name, "WARNING", "Filter section not visible", product_url=url, error=str(e))
                        failed_urls.append({"product_url": url, "error": "Filter section timeout"})
                        continue

                    # Click "with comments" button
                    comment_btn = await _find_review_filter_button(page, filter_text)
                    if comment_btn is None:
                        add_scrape_log(log_rows, scraper_name, "WARNING", "with comments button not found", product_url=url)
                        failed_urls.append({"product_url": url, "error": "Button not found"})
                        continue

                    await _close_shopee_popups(page)
                    # Support both Playwright Locator and JS ElementHandle (from _find_review_filter_button)
                    try:
                        await comment_btn.wait_for(state="visible", timeout=10000)
                        btn_label = (await comment_btn.inner_text()).strip()
                    except Exception:
                        # JS ElementHandle path
                        btn_label = str(await page.evaluate("(el) => el ? el.innerText.trim() : ''", comment_btn))

                    m = re.search(r"\(([\d,\.]+)([kKmM]?)\)", btn_label)
                    if not m:
                        add_scrape_log(log_rows, scraper_name, "INFO", "No count on button — skip", product_url=url)
                        continue

                    num_val = float(m.group(1).replace(",", ""))
                    unit = m.group(2).lower()
                    if unit == "k":
                        total_count = int(num_val * 1_000)
                    elif unit == "m":
                        total_count = int(num_val * 1_000_000)
                    else:
                        total_count = int(num_val)

                    if total_count == 0:
                        add_scrape_log(log_rows, scraper_name, "INFO", "0 reviews — skip", product_url=url)
                        # Genuinely nothing to scrape here — mark done so resume doesn't
                        # keep re-opening this URL every run just to find 0 again.
                        done_urls.add(url)
                        save_done_keys("temp_shopee_reviews_comments", done_urls)
                        continue

                    if total_count > 3000:
                        with_comments_count_overflow = True
                        add_scrape_log(log_rows, scraper_name, "INFO",
                                       f"with comments > 3000 ({total_count}) — use run_product_reviews_by_stars",
                                       product_url=url)
                        # This scraper can never handle this URL (UI caps "with comments" at
                        # ~3000) — mark done so it stops retrying; use run_product_reviews_by_stars instead.
                        done_urls.add(url)
                        save_done_keys("temp_shopee_reviews_comments", done_urls)
                        continue

                    max_pages = min(math.ceil(total_count / 6), 2000)
                    print(f"   📊 {btn_label} → {total_count} reviews ({max_pages} pages)")

                    await _move_mouse_randomly(page)
                    await page.wait_for_timeout(random.randint(400, 900))
                    try:
                        await comment_btn.click(force=True, timeout=10000)
                    except Exception:
                        # JS ElementHandle path — use evaluate to click
                        await page.evaluate("(el) => el && el.click()", comment_btn)
                    await page.wait_for_timeout(random.randint(2500, 4000))

                    page_num = 0
                    consecutive_captcha = 0
                    product_reviews = []
                    skip_url = False
                    stall_recoveries = 0
                    MAX_STALL_RECOVERIES = 3

                    while True:
                        page_num += 1
                        print(f"\n   📄 Review page {page_num}/{max_pages}")

                        if await _is_captcha_detected(page):
                            consecutive_captcha += 1
                            if consecutive_captcha >= MAX_CAPTCHA_RETRY:
                                add_scrape_log(log_rows, scraper_name, "ERROR", "Too many captchas", product_url=url)
                                break
                            recovered = await _handle_captcha(page, url)
                            if not recovered:
                                break
                            add_scrape_log(log_rows, scraper_name, "INFO",
                                           "Captcha solved manually — skipping current product",
                                           product_url=url)
                            skip_url = True
                            break
                        else:
                            consecutive_captcha = 0

                        # Anchor the viewport to the review section before each parse
                        await _scroll_review_into_view(page)
                        await page.wait_for_timeout(random.randint(1500, 2500))

                        page_revs = await _parse_reviews_on_page(page, url)
                        for r in page_revs:
                            row = _review_to_row(r)
                            key = (str(row["product_url"]), str(row["user_name"]),
                                   str(row["comment_date"]), str(row["comment"]))
                            if key not in seen:
                                product_reviews.append(row)
                                all_reviews.append(row)
                                seen.add(key)

                        if not page_revs:
                            print("   ⚠️ No reviews on this page — stop")
                            fully_completed = True
                            break
                        if page_num >= max_pages:
                            fully_completed = True
                            break

                        # ── Snapshot BEFORE click (for change detection) ──
                        old_first_text = await _get_first_review_text(page)
                        target_page = page_num + 1

                        # Scroll so pagination bar stays in viewport, then JS-click.
                        # JS click avoids Playwright stale-element errors: React
                        # replaces all pagination DOM nodes on every page flip, so a
                        # cached Playwright locator silently clicks a detached node.
                        await _scroll_review_into_view(page)
                        await _move_mouse_randomly(page)
                        await page.wait_for_timeout(random.randint(400, 900))

                        advance_result = await _advance_review_page(page, old_first_text, target_page)
                        if advance_result in ("last_page", "stuck"):
                            # Don't trust a single disabled-button/no-change signal as proof
                            # there are no more reviews — Shopee can transiently throttle the
                            # next-page button well before the real last page. Try reloading
                            # and jumping back to target_page before giving up for good.
                            if page_num < max_pages and stall_recoveries < MAX_STALL_RECOVERIES:
                                stall_recoveries += 1
                                reason = "last-page signal" if advance_result == "last_page" else "no content change"
                                print(f"   ⚠️ {reason} at page {page_num}/{max_pages} "
                                      f"— attempting stall recovery ({stall_recoveries}/{MAX_STALL_RECOVERIES})")
                                if await _recover_stalled_review_pagination(page, url, filter_text, target_page):
                                    print(f"   ✅ Resumed at page {target_page}")
                                    continue
                            if advance_result == "last_page":
                                print(f"   ✅ Reached last page — next button disabled/absent (no more reviews)")
                                fully_completed = True
                            else:
                                print(f"   ⚠️ Still no content change after retries and recovery — stopping pagination")
                            pages_scraped_comments[url] = (page_num, page_num)
                            break
                        stall_recoveries = 0
                        print(f"   ✅ Advanced to page {target_page}")

                        # ── Detect page reset (next-click caused full reload to page 1) ──
                        await page.wait_for_timeout(random.randint(600, 1200))
                        active_pg = await _get_active_review_page_num(page)
                        if active_pg > 0 and active_pg != target_page:
                            if active_pg < target_page:
                                print(f"   🔁 Page reset detected (active={active_pg}, expected={target_page}) — re-applying filter...")
                                try:
                                    comment_btn2 = await _find_review_filter_button(page, filter_text)
                                    if comment_btn2:
                                        try:
                                            await comment_btn2.click(force=True, timeout=10000)
                                        except Exception:
                                            await page.evaluate("(el) => el && el.click()", comment_btn2)
                                        await page.wait_for_timeout(random.randint(2000, 3500))
                                except Exception:
                                    pass
                                await _scroll_review_into_view(page)
                                snap_after_reset = await _get_first_review_text(page)
                                recovered = await _navigate_to_review_page_num(
                                    page, target_page, snap_after_reset, timeout_ms=20000
                                )
                                if not recovered:
                                    print(f"   ⚠️ Could not recover to page {target_page} after reset — stopping")
                                    pages_scraped_comments[url] = (page_num, max_pages)
                                    break
                                print(f"   ✅ Recovered to page {target_page} after reset")

                        if page_num % 5 == 0:
                            extra = random.uniform(2, 5)
                            print(f"   ⏸️ Long pause {extra:.1f}s...")
                            await asyncio.sleep(extra)

                        await cooldown_tick()

                        if page_num % REVIEW_CHECKPOINT_EVERY_PAGES == 0:
                            save_scrape_checkpoint(all_reviews, "temp_shopee_reviews_comments", REVIEW_DEDUPE_COLS, f"page {page_num}")

                    if skip_url:
                        continue
                    # Record full success (reached max_pages)
                    if url not in pages_scraped_comments:
                        pages_scraped_comments[url] = (page_num, max_pages)
                        fully_completed = True
                    print(f"   ✅ Got {len(product_reviews)} reviews for this product ({page_num}/{max_pages} pages)")
                    # Save this URL's reviews to its own Excel file immediately
                    _PER_URL_COLS = ["product_url", "comment_date", "option",
                                     "comment", "rating_star", "source", "collected_at"]
                    save_reviews_per_url(product_reviews, url,
                                         dedupe_cols=REVIEW_DEDUPE_COLS,
                                         output_cols=_PER_URL_COLS)

                    if fully_completed:
                        done_urls.add(url)
                        save_done_keys("temp_shopee_reviews_comments", done_urls)

                except Exception as e:
                    print(f"   ❌ [ERROR] {e}")
                    failed_urls.append({"product_url": url, "error": str(e)})
                    add_scrape_log(log_rows, scraper_name, "ERROR", "Error scraping reviews", product_url=url, error=str(e))
                    save_scrape_checkpoint(all_reviews, "temp_shopee_reviews_comments", REVIEW_DEDUPE_COLS, "error")

        finally:
            save_scrape_checkpoint(all_reviews, "temp_shopee_reviews_comments", REVIEW_DEDUPE_COLS, "final backup")

    REVIEW_OUTPUT_COLS = ["product_url","comment_date", "option",
                          "comment", "rating_star", "source", "collected_at"]
    df, _ = save_final_data(all_reviews, "shopee_reviews_with_comments", REVIEW_DEDUPE_COLS, output_cols=REVIEW_OUTPUT_COLS)
    save_failed_log(failed_urls, "failed_reviews_comments", scraper_name)
    save_pagination_report(pages_scraped_comments, "pagination_report_with_comments")
    return df[REVIEW_OUTPUT_COLS] if not df.empty else df


# ════════════════════════════════════════════════════════════════
# Scraper 4: Reviews — By Stars
# ════════════════════════════════════════════════════════════════

async def run_product_reviews_by_stars(product_urls: List[str]) -> pd.DataFrame:
    """ดึงรีวิวแยกตาม star rating (1–5 ดาว)"""
    scraper_name = "reviews"
    all_reviews = load_existing_backup("temp_shopee_reviews_by_stars", REVIEW_DEDUPE_COLS) if RESUME_FROM_BACKUP else []
    log_rows = []
    failed_urls = []
    pages_scraped_stars: dict = {}   # "url|⭐N" -> (pages_done, pages_total)
    seen = {
        (str(r.get("product_url", "")), str(r.get("user_name", "")),
         str(r.get("comment_date", "")), str(r.get("comment", "")))
        for r in all_reviews
    }
    STAR_FILTERS = [5, 4, 3, 2, 1]
    MAX_CAPTCHA_RETRY = 8
    # "url|⭐N" keys already fully paginated in a previous run — skip entirely
    # instead of re-opening the product page and re-walking every review page.
    done_star_keys = load_done_keys("temp_shopee_reviews_by_stars") if RESUME_FROM_BACKUP else set()

    print(f"📂 [SESSION] Loading: '{USER_DATA_DIR}'")
    context, page = await create_browser_context(block_images=False)
    async with context:
        try:
            for idx, url in enumerate(product_urls, start=1):
                print("\n" + "=" * 60)
                print(f"[{idx}/{len(product_urls)}] Reviews (by stars): {url}")
                session_blocked = False
                # Must be defined before the star loop: a delisted product
                # (or any other early `break` out of that loop, e.g. on the
                # very first star) jumps straight past the loop's own
                # `skip_url = False` init to the `if skip_url:` check right
                # after it — an uninitialized read otherwise.
                skip_url = False

                url_all_reviews = []   # accumulates reviews across all star buckets for this URL
                for star in STAR_FILTERS:
                    filter_text = f"{star} star"
                    print(f"\n{'='*50}\n⭐ Star {star}...")

                    if f"{url}|⭐{star}" in done_star_keys:
                        print(f"   ⏭️ Skip ⭐{star} — already fully scraped in a previous run")
                        continue

                    fully_completed = False

                    if session_blocked:
                        cooldown = random.uniform(180.0, 300.0)
                        print(f"🔴 Session blocked — cooling down {cooldown/60:.1f} min...")
                        await asyncio.sleep(cooldown)
                        session_blocked = False

                    reopen_ok = await _human_reopen_product(page, url, filter_text)
                    if not reopen_ok or await _is_captcha_detected(page):
                        recovered = await _handle_captcha(page, url)
                        if not recovered:
                            session_blocked = True
                            continue
                        for _ in range(12):
                            await _human_like_scroll(page, random.randint(500, 750))
                            await page.wait_for_timeout(random.randint(700, 1300))
                            if await page.query_selector(".product-rating-overview__filters"):
                                break

                    if await _is_product_not_found(page):
                        add_scrape_log(log_rows, scraper_name, "WARNING", "Product not found / delisted — skipping",
                                       product_url=url, star=star)
                        failed_urls.append({"product_url": url, "star": star, "error": "Product not found / delisted"})
                        # Delisted product — every star bucket will fail the same way; stop retrying it.
                        break

                    try:
                        fc = page.locator(".product-rating-overview__filters").first
                        await fc.wait_for(state="visible", timeout=20000)
                        await fc.scroll_into_view_if_needed()
                        await page.wait_for_timeout(random.randint(1500, 3000))
                    except Exception as e:
                        add_scrape_log(log_rows, scraper_name, "WARNING", "Filter section not visible",
                                       product_url=url, star=star, error=str(e))
                        session_blocked = True
                        continue

                    # Find and click the star button
                    star_btn = await _find_review_filter_button(page, filter_text)
                    if star_btn is None:
                        add_scrape_log(log_rows, scraper_name, "WARNING", "Star button not found",
                                       product_url=url, star=star)
                        continue

                    await _close_shopee_popups(page)
                    # Support both Playwright Locator and JS ElementHandle
                    try:
                        await star_btn.wait_for(state="visible", timeout=10000)
                        btn_label = (await star_btn.inner_text()).strip()
                    except Exception:
                        btn_label = str(await page.evaluate("(el) => el ? el.innerText.trim() : ''", star_btn))

                    m = re.search(r"\(([\d,\.]+)([kKmM]?)\)", btn_label)
                    if not m:
                        add_scrape_log(log_rows, scraper_name, "INFO", "No count on star button — skip",
                                       product_url=url, star=star)
                        continue

                    num_val = float(m.group(1).replace(",", ""))
                    unit = m.group(2).lower()
                    if unit == "k":
                        total_count = int(num_val * 1_000)
                    elif unit == "m":
                        total_count = int(num_val * 1_000_000)
                    else:
                        total_count = int(num_val)

                    if total_count == 0:
                        add_scrape_log(log_rows, scraper_name, "INFO", "0 reviews — skip", product_url=url, star=star)
                        # Genuinely nothing to scrape for this star bucket — mark done so
                        # resume doesn't keep re-opening this URL every run just to find 0 again.
                        done_star_keys.add(f"{url}|⭐{star}")
                        save_done_keys("temp_shopee_reviews_by_stars", done_star_keys)
                        continue

                    max_pages = min(math.ceil(total_count / 6), 2000)
                    print(f"   📊 {btn_label} → {total_count} reviews ({max_pages} pages)")

                    try:
                        await star_btn.hover()
                        await page.wait_for_timeout(random.randint(300, 700))
                    except Exception:
                        pass
                    await _move_mouse_randomly(page)
                    await page.wait_for_timeout(random.randint(400, 900))
                    try:
                        await star_btn.click(force=True, timeout=10000)
                    except Exception:
                        await page.evaluate("(el) => el && el.click()", star_btn)
                    await page.wait_for_timeout(random.randint(2500, 4000))

                    page_num = 0
                    consecutive_captcha = 0
                    star_reviews = []
                    skip_url = False
                    stall_recoveries = 0
                    MAX_STALL_RECOVERIES = 3

                    while True:
                        page_num += 1
                        print(f"\n   📄 Page {page_num}/{max_pages} (⭐{star})")

                        if await _is_captcha_detected(page):
                            consecutive_captcha += 1
                            if consecutive_captcha >= MAX_CAPTCHA_RETRY:
                                session_blocked = True
                                break
                            recovered = await _handle_captcha(page, url)
                            if not recovered:
                                # try to reopen, but if still blocked mark session and continue
                                reopen_ok = await _human_reopen_product(page, url, filter_text)
                                if not reopen_ok or await _is_captcha_detected(page):
                                    if consecutive_captcha >= MAX_CAPTCHA_RETRY:
                                        session_blocked = True
                                        break
                                    await page.wait_for_timeout(random.randint(5000, 10000))
                                    continue
                                # try to re-acquire filters
                                try:
                                    fc2 = page.locator(".product-rating-overview__filters").first
                                    await fc2.wait_for(state="visible", timeout=15000)
                                    await fc2.scroll_into_view_if_needed()
                                    await page.wait_for_timeout(random.randint(1500, 3000))
                                except Exception:
                                    session_blocked = True
                                    break
                                consecutive_captcha = 0
                                continue
                            add_scrape_log(log_rows, scraper_name, "INFO",
                                           "Captcha solved manually — skipping current product",
                                           product_url=url, star=star)
                            skip_url = True
                            break
                        else:
                            consecutive_captcha = 0

                        # Anchor the viewport to the review section before each parse
                        await _scroll_review_into_view(page)
                        await page.wait_for_timeout(random.randint(1500, 2500))

                        page_revs = await _parse_reviews_on_page(page, url)
                        for r in page_revs:
                            row = _review_to_row(r)
                            row["star_group"] = star
                            row["filter_label"] = btn_label
                            key = (str(row["product_url"]), str(row["user_name"]),
                                   str(row["comment_date"]), str(row["comment"]))
                            if key not in seen:
                                star_reviews.append(row)
                                all_reviews.append(row)
                                seen.add(key)

                        if not page_revs:
                            print("   ⚠️ No reviews — stop")
                            fully_completed = True
                            break
                        if page_num >= max_pages:
                            fully_completed = True
                            break

                        # ── Snapshot BEFORE click (for change detection) ──
                        old_first_text = await _get_first_review_text(page)
                        target_page = page_num + 1

                        # JS click avoids Playwright stale-element errors:
                        # React replaces all pagination DOM nodes on every page flip.
                        await _scroll_review_into_view(page)
                        await _move_mouse_randomly(page)
                        await page.wait_for_timeout(random.randint(400, 900))

                        advance_result = await _advance_review_page(page, old_first_text, target_page)
                        if advance_result in ("last_page", "stuck"):
                            # Same false-stop guard as the "with comments" loop: a disabled
                            # button or stalled content isn't reliable proof of a genuine
                            # last page — try a reload + re-navigate before giving up.
                            if page_num < max_pages and stall_recoveries < MAX_STALL_RECOVERIES:
                                stall_recoveries += 1
                                reason = "last-page signal" if advance_result == "last_page" else "no content change"
                                print(f"   ⚠️ {reason} at page {page_num}/{max_pages} (⭐{star}) "
                                      f"— attempting stall recovery ({stall_recoveries}/{MAX_STALL_RECOVERIES})")
                                if await _recover_stalled_review_pagination(page, url, filter_text, target_page):
                                    print(f"   ✅ Resumed at page {target_page}")
                                    continue
                            if advance_result == "last_page":
                                print(f"   ✅ Reached last page — next button disabled/absent (no more reviews)")
                                fully_completed = True
                            else:
                                print(f"   ⚠️ Still no content change after retries and recovery — stopping pagination")
                            pages_scraped_stars[f"{url}|⭐{star}"] = (page_num, page_num)
                            break
                        stall_recoveries = 0
                        print(f"   ✅ Advanced to page {target_page}")

                        # ── Detect page reset ──
                        await page.wait_for_timeout(random.randint(600, 1200))
                        active_pg = await _get_active_review_page_num(page)
                        if active_pg > 0 and active_pg != target_page:
                            if active_pg < target_page:
                                print(f"   🔁 Page reset detected (active={active_pg}, expected={target_page}) — re-applying star filter...")
                                try:
                                    star_btn2 = await _find_review_filter_button(page, filter_text)
                                    if star_btn2:
                                        try:
                                            await star_btn2.click(force=True, timeout=10000)
                                        except Exception:
                                            await page.evaluate("(el) => el && el.click()", star_btn2)
                                        await page.wait_for_timeout(random.randint(2000, 3500))
                                except Exception:
                                    pass
                                await _scroll_review_into_view(page)
                                snap_after_reset = await _get_first_review_text(page)
                                recovered = await _navigate_to_review_page_num(
                                    page, target_page, snap_after_reset, timeout_ms=20000
                                )
                                if not recovered:
                                    print(f"   ⚠️ Could not recover to page {target_page} after reset — stopping")
                                    pages_scraped_stars[f"{url}|⭐{star}"] = (page_num, max_pages)
                                    break
                                print(f"   ✅ Recovered to page {target_page} after reset")

                        if page_num % 5 == 0:
                            extra = random.uniform(6, 12)
                            print(f"   ⏸️ Pause {extra:.1f}s...")
                            await asyncio.sleep(extra)

                        await cooldown_tick()

                        if page_num % REVIEW_CHECKPOINT_EVERY_PAGES == 0:
                            save_scrape_checkpoint(all_reviews, "temp_shopee_reviews_by_stars", REVIEW_DEDUPE_COLS, f"star{star} page{page_num}")

                    if skip_url:
                        break
                    # Record full success for this star if not already marked partial
                    if f"{url}|⭐{star}" not in pages_scraped_stars:
                        pages_scraped_stars[f"{url}|⭐{star}"] = (page_num, max_pages)
                        fully_completed = True
                    print(f"   ✅ ⭐{star}: {len(star_reviews)} reviews collected ({page_num}/{max_pages} pages)")
                    url_all_reviews.extend(star_reviews)

                    if fully_completed:
                        done_star_keys.add(f"{url}|⭐{star}")
                        save_done_keys("temp_shopee_reviews_by_stars", done_star_keys)

                    if star > 1 and not session_blocked:
                        inter = random.uniform(8.0, 15.0)
                        print(f"\n⏸️ Inter-star pause {inter:.1f}s...")
                        await asyncio.sleep(inter)

                if skip_url:
                    continue

                # Save all stars combined for this URL to its own Excel file
                if url_all_reviews:
                    _PER_URL_COLS = ["product_url", "comment_date", "option",
                                     "comment", "rating_star", "star_group", "filter_label",
                                     "source", "collected_at"]
                    save_reviews_per_url(url_all_reviews, url,
                                         dedupe_cols=REVIEW_DEDUPE_COLS,
                                         output_cols=_PER_URL_COLS,
                                         sub_dir="per_url_reviews")

        finally:
            save_scrape_checkpoint(all_reviews, "temp_shopee_reviews_by_stars", REVIEW_DEDUPE_COLS, "final backup")

    REVIEW_OUTPUT_COLS = ["product_url", "comment_date", "option", 
                          "comment", "rating_star", "star_group", "filter_label", "source", "collected_at"]
    df, _ = save_final_data(all_reviews, "shopee_reviews_by_stars", REVIEW_DEDUPE_COLS, output_cols=REVIEW_OUTPUT_COLS)
    save_failed_log(failed_urls, "failed_reviews_by_stars", scraper_name)
    save_pagination_report(pages_scraped_stars, "pagination_report_by_stars")
    return df[[c for c in REVIEW_OUTPUT_COLS if c in df.columns]] if not df.empty else df


# ════════════════════════════════════════════════════════════════
# Scraper 5: Search by Keyword
# ════════════════════════════════════════════════════════════════

async def run_search_keyword(keywords: List[str], max_pages: Optional[int] = None) -> pd.DataFrame:
    """ดึงสินค้าจาก Shopee search ผ่าน API intercept + requests fallback"""
    scraper_name = "search_keyword"
    all_products = load_existing_backup("temp_shopee_search", SEARCH_DEDUPE_COLS) if RESUME_FROM_BACKUP else []
    log_rows = []
    failed_keywords = []
    seen = {
        (str(r.get("search_keyword", "")), str(r.get("itemid", "")),
         str(r.get("shopid", "")), str(r.get("product_url", "")))
        for r in all_products
    }

    print(f"📂 [SESSION] Loading: '{USER_DATA_DIR}'")
    context, page = await create_browser_context(block_images=True)
    async with context:
        try:
            for kw_idx, keyword in enumerate(keywords, start=1):
                print("\n" + "-" * 60)
                print(f"[{kw_idx}/{len(keywords)}] Keyword: '{keyword}'")

                # Fresh fetch: drop any rows/seen-keys for THIS keyword left over
                # from a previous run so re-scraping REPLACES the old data instead
                # of deduping every new item against stale temp (which made most
                # pages report only a handful of "new" products). Rows for OTHER
                # keywords stay intact, so a multi-keyword batch still resumes.
                stale = sum(1 for r in all_products if str(r.get("search_keyword", "")) == keyword)
                if stale:
                    print(f"   🧹 Fresh fetch: clearing {stale:,} old row(s) for '{keyword}' from temp")
                    all_products = [r for r in all_products if str(r.get("search_keyword", "")) != keyword]
                    seen = {k for k in seen if k[0] != keyword}

                intercepted_pages: dict = {}
                last_token: dict = {"value": None}
                browser_cookies: dict = {}

                async def _on_request(req):
                    if "api/v4/search/search_items" in req.url:
                        token = req.headers.get("af-ac-enc-dat")
                        if token:
                            last_token["value"] = token

                async def _on_response(res):
                    if "api/v4/search/search_items" in res.url:
                        try:
                            data = await res.json()
                            items = data.get("items")
                            if items:
                                # Extract page number from URL
                                m = re.search(r"newest=(\d+)", res.url)
                                newest = int(m.group(1)) if m else 0
                                pg = newest // 60 + 1
                                if pg not in intercepted_pages:
                                    intercepted_pages[pg] = []
                                intercepted_pages[pg].extend(items)
                        except Exception:
                            pass

                page.on("request", _on_request)
                page.on("response", _on_response)

                try:
                    # Navigate to Shopee first, then search
                    print("🌐 Navigating to Shopee homepage...")
                    await page.goto("https://shopee.co.th", wait_until="domcontentloaded", timeout=60000)
                    await asyncio.sleep(random.uniform(6, 10))

                    search_input = "input.shopee-searchbar-input__input"
                    search_button = "button.btn-solid-primary"
                    typed = False
                    try:
                        print(f"⌨️  Typing keyword: {keyword}")
                        await page.click(search_input)
                        await asyncio.sleep(random.uniform(1.5, 2.5))
                        for char in keyword:
                            await page.type(search_input, char, delay=random.randint(50, 200))
                        await asyncio.sleep(random.uniform(7, 12))
                        await page.hover(search_button)
                        await asyncio.sleep(random.uniform(1.5, 2.5))
                        await page.click(search_button)
                        await page.wait_for_load_state("domcontentloaded")
                        await asyncio.sleep(5)
                        typed = True
                    except Exception as e:
                        print(f"⚠️ Search type failed: {e} — fallback goto")
                        await page.goto(
                            f"https://shopee.co.th/search?keyword={quote(keyword)}",
                            wait_until="domcontentloaded", timeout=30000
                        )
                        await asyncio.sleep(5)

                    # Read total pages
                    total_pages_found = 1
                    try:
                        await page.wait_for_selector(".shopee-mini-page-controller__total", timeout=5000)
                        total_pages_found = int(await page.inner_text(".shopee-mini-page-controller__total"))
                        print(f"📑 Found {total_pages_found} pages"
                              + (f" (scraping up to {max_pages})" if max_pages else " (scraping all)"))
                    except Exception:
                        await page.mouse.wheel(0, 2000)
                        await page.wait_for_timeout(3000)
                        try:
                            total_pages_found = int(await page.inner_text(".shopee-mini-page-controller__total"))
                        except Exception:
                            print("⚠️ Pagination not found — assuming 1 page")

                    total_pages_capped = total_pages_found if max_pages is None else min(total_pages_found, max_pages)

                    for current_pg in range(1, total_pages_capped + 1):
                        print(f"\n📄 Page {current_pg}/{total_pages_capped}")
                        if current_pg not in intercepted_pages:
                            print("🖱️ Scrolling to trigger API...")
                            for _ in range(8):
                                await page.mouse.wheel(0, 600)
                                await page.wait_for_timeout(1200)
                                if current_pg in intercepted_pages:
                                    break

                        items = intercepted_pages.get(current_pg, [])
                        print(f"   ⚡ {len(items)} items intercepted")

                        new_count = 0
                        error_count = 0
                        first_error = None
                        for item_wrap in items:
                            if not item_wrap:
                                continue
                            try:
                                product = _map_product(item_wrap)
                                row = _product_to_row(product)
                                row["search_keyword"] = keyword
                                key = (keyword, str(row["itemid"]), str(row["shopid"]), str(row["product_url"]))
                                if key not in seen:
                                    all_products.append(row)
                                    seen.add(key)
                                    new_count += 1
                            except Exception as e:
                                error_count += 1
                                if first_error is None:
                                    first_error = e

                        # Count only THIS keyword's rows — all_products also holds
                        # other keywords still resident in memory (loaded from the
                        # shared temp backup), so len(all_products) overstates progress
                        # and doesn't match the per-keyword row count in the final file.
                        keyword_total = sum(1 for r in all_products if r.get("search_keyword") == keyword)
                        print(f"   > Page {current_pg}: +{new_count} new products | Total for '{keyword}': {keyword_total:,}")
                        if error_count:
                            print(f"      ⚠️ {error_count} item(s) failed to map — first error: {first_error!r}")

                        pages_done = current_pg
                        if pages_done % SEARCH_CHECKPOINT_EVERY_PAGES == 0 or current_pg == total_pages_capped:
                            add_scrape_log(log_rows, scraper_name, "INFO", "Checkpoint",
                                           keyword=keyword, page=current_pg)
                            save_scrape_checkpoint(all_products, "temp_shopee_search", SEARCH_DEDUPE_COLS, f"page {current_pg}")

                        await cooldown_tick()

                        if current_pg < total_pages_capped:
                            try:
                                next_btn = page.locator(".shopee-mini-page-controller__next-btn").first
                                if await next_btn.is_enabled():
                                    await next_btn.click()
                                    await page.wait_for_timeout(6000)
                                else:
                                    print("   ⛔ Next button disabled — stop")
                                    break
                            except Exception as e:
                                print(f"   ⚠️ Next button error: {e} — stop")
                                break

                    # Fallback: pages where API wasn't intercepted
                    missing = [pg for pg in range(1, total_pages_capped + 1) if pg not in intercepted_pages or not intercepted_pages[pg]]
                    if missing and last_token["value"]:
                        print(f"\n🔄 Requests fallback for pages: {missing}")
                        browser_cookies = {c["name"]: c["value"] for c in await context.cookies()}
                        session = requests.Session()
                        session.cookies.update(browser_cookies)
                        headers = {
                            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/136.0.0.0 Safari/537.36",
                            "Accept": "application/json",
                            "Accept-Language": "th-TH,th;q=0.9,en;q=0.8",
                            "Referer": f"https://shopee.co.th/search?keyword={quote(keyword)}",
                            "x-api-source": "pc",
                            "af-ac-enc-dat": last_token["value"],
                            "x-shopee-language": "th",
                            "x-requested-with": "XMLHttpRequest",
                        }
                        limit = 60
                        for pg in missing:
                            newest = (pg - 1) * limit
                            params = {
                                "by": "relevancy", "keyword": keyword,
                                "limit": str(limit), "newest": str(newest),
                                "order": "desc", "page_type": "search",
                                "scenario": "PAGE_GLOBAL_SEARCH", "version": "2",
                            }
                            try:
                                res = session.get(
                                    "https://shopee.co.th/api/v4/search/search_items",
                                    headers=headers, params=params, timeout=15,
                                )
                                if res.status_code == 200:
                                    fb_error = None
                                    for item_wrap in (res.json().get("items") or []):
                                        if not item_wrap:
                                            continue
                                        try:
                                            product = _map_product(item_wrap)
                                            row = _product_to_row(product)
                                            row["search_keyword"] = keyword
                                            key = (keyword, str(row["itemid"]), str(row["shopid"]), str(row["product_url"]))
                                            if key not in seen:
                                                all_products.append(row)
                                                seen.add(key)
                                        except Exception as e:
                                            if fb_error is None:
                                                fb_error = e
                                    if fb_error is not None:
                                        print(f"      ⚠️ Fallback page {pg}: some item(s) failed to map — first error: {fb_error!r}")
                                    await asyncio.sleep(random.uniform(2, 4))
                            except Exception as e:
                                add_scrape_log(log_rows, scraper_name, "WARNING", "Fallback error", keyword=keyword, page=pg, error=str(e))

                    print(f"   >>> Collected for '{keyword}': Total so far {len(all_products):,}")

                except Exception as e:
                    print(f"❌ [SKIP] Keyword '{keyword}': {e}")
                    failed_keywords.append({"keyword": keyword, "error": str(e)})
                    add_scrape_log(log_rows, scraper_name, "ERROR", "Error", keyword=keyword, error=str(e))
                    save_scrape_checkpoint(all_products, "temp_shopee_search", SEARCH_DEDUPE_COLS, "keyword error")
                finally:
                    page.remove_listener("request", _on_request)
                    page.remove_listener("response", _on_response)
                    intercepted_pages.clear()

        finally:
            save_scrape_checkpoint(all_products, "temp_shopee_search", SEARCH_DEDUPE_COLS, "final backup")

    # `all_products` / the temp checkpoint is intentionally cumulative across every
    # keyword ever scraped with RESUME_FROM_BACKUP=True (so an interrupted run of
    # THIS keyword batch can resume). But that means it also carries older keyword
    # batches from previous runs — filter the final deliverable down to just the
    # keyword(s) requested in this call so each run's Excel only has its own data.
    current_keywords = set(keywords)
    final_rows = [r for r in all_products if str(r.get("search_keyword", "")) in current_keywords]
    df, _ = save_final_data(final_rows, "shopee_search", SEARCH_DEDUPE_COLS)
    save_failed_log(failed_keywords, "failed_keywords", scraper_name)
    return df