"""
cloak_konvy.py
────────────────────────────────────────────────────────────────
Konvy Thailand scraper — mirrors cloak_watsons.py structure.

Ported from the user's two exploratory notebooks:
  • konvy_scraping_brands_1.ipynb   (Selenium listing + detail scrape)
  • Konvy_comment_scraper.ipynb     (requests-based review API)

Differences from the notebooks:
  • Uses CloakBrowser (Playwright) instead of raw Selenium — consistent
    with the rest of this project, no new selenium/webdriver_manager dep.
  • No login step — Konvy's catalog/detail/review data is all public
    (the notebook's login attempt was failing anyway and isn't needed).
  • Listing pagination now scrolls until the tile count stabilises
    instead of a fixed sleep — the notebook's fixed-sleep approach was
    observed returning "Found 0 products" on every brand.
  • First empty listing page auto-dumps HTML + screenshot for diagnosis
    (mirrors cloak_shopee_AI_bot.py's captcha/raw-item debug dumps).
  • Reviews use the confirmed internal JSON API (ajax_comment.php) —
    no browser needed for that part at all.

Public entry-points:
  run_search_keyword(keywords)        → pd.DataFrame   full detail rows (brand/price/product_id/…)
  run_product_detail(product_urls)    → pd.DataFrame   detail rows (standalone re-scrape)
  run_all_brands(...)                 → pd.DataFrame   EVERY brand on Konvy's brand index
  run_product_reviews(product_ids)    → pd.DataFrame   review rows (needs product_id from a detail scrape)
"""

import asyncio
import csv
import json
import random
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import quote, unquote, urlparse

import pandas as pd
from bs4 import BeautifulSoup
from cloakbrowser import launch_persistent_context_async

# ════════════════════════════════════════════════════════════════
# Directories & Config
# ════════════════════════════════════════════════════════════════

OUTPUT_DIR = "./data/konvy/collected_data"
TEMP_DIR   = "./data/konvy/collected_data/temp"
LOG_DIR    = "./data/konvy/log"

REQUEST_DELAY_MIN = 1.5
REQUEST_DELAY_MAX = 4.0

CHECKPOINT_EVERY_PRODUCTS = 10

# Hard ceiling per product-detail page — guards against a stalled page
# (anti-bot challenge, dead navigation) hanging the whole run.
PRODUCT_DETAIL_TIMEOUT = 90

RESUME_FROM_BACKUP = False

for _folder in [OUTPUT_DIR, TEMP_DIR, LOG_DIR]:
    Path(_folder).mkdir(parents=True, exist_ok=True)

print(f"📂 Output folder          : {OUTPUT_DIR}")
print(f"💾 Temp checkpoint folder : {TEMP_DIR}")

TZ_THAI = timezone(timedelta(hours=7))

LISTING_BASE = "https://www.konvy.com/list/"
DETAIL_DEDUPE_COLS = ["product_url"]
REVIEW_DEDUPE_COLS = ["review_id"]

# Attribute-box label aliases (Thai/English) — anything not matched here
# is kept verbatim in `other_attributes` (JSON string), same pattern as
# Shopee's spec-mapping "Other" bucket.
_PRODUCT_ID_LABELS = {"product id", "รหัสสินค้า", "sku", "รหัส", "product code"}
_BRAND_LABELS = {"brand", "แบรนด์", "ยี่ห้อ"}

# One-shot debug flag: dump the first empty listing page so the real HTML
# (possibly changed since the notebook was written) can be inspected.
_LISTING_DEBUG_DUMPED = False

# Windows event-loop fix
if sys.platform == "win32":
    import functools, asyncio.proactor_events, asyncio.base_subprocess

    def _silence(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except Exception:
                pass
        return wrapper

    asyncio.proactor_events._ProactorBasePipeTransport.__del__ = _silence(
        asyncio.proactor_events._ProactorBasePipeTransport.__del__
    )
    asyncio.base_subprocess.BaseSubprocessTransport.__del__ = _silence(
        asyncio.base_subprocess.BaseSubprocessTransport.__del__
    )
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())


# ════════════════════════════════════════════════════════════════
# General Helpers
# ════════════════════════════════════════════════════════════════
def _make_timestamp() -> str:
    return datetime.now(TZ_THAI).strftime("%Y%m%d_%H%M")


def _scraped_date() -> str:
    return datetime.now(TZ_THAI).strftime("%d/%m/%Y")


def _collected_at() -> str:
    return datetime.now(TZ_THAI).strftime("%Y-%m-%d %H:%M:%S")


async def _human_delay(min_d: float = REQUEST_DELAY_MIN, max_d: float = REQUEST_DELAY_MAX):
    await asyncio.sleep(random.uniform(min_d, max_d))


def _save_checkpoint(rows: list, data_prefix: str, dedupe_cols: Optional[List[str]] = None,
                      reason: str = "checkpoint"):
    if not rows:
        return
    df = pd.DataFrame(rows)
    if dedupe_cols:
        valid = [c for c in dedupe_cols if c in df.columns]
        if valid:
            df = df.drop_duplicates(subset=valid)
    path = Path(TEMP_DIR) / f"{data_prefix}.csv"
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"   💾 Checkpoint [{reason}]: {path}  ({len(df):,} rows)")


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


def _save_final(rows: list, final_prefix: str, dedupe_cols: Optional[List[str]] = None) -> pd.DataFrame:
    if not rows:
        print(f"   ⚠️  No data to save: {final_prefix}")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    if dedupe_cols:
        valid = [c for c in dedupe_cols if c in df.columns]
        if valid:
            df = df.drop_duplicates(subset=valid)

    ts   = _make_timestamp()
    xlsx = Path(OUTPUT_DIR) / f"{final_prefix}_{ts}.xlsx"

    df = _sanitize_for_excel(df)
    df.to_excel(xlsx, index=False)

    print("\n" + "=" * 60)
    print(f"✅ FINAL FILE SAVED : {final_prefix}")
    print(f"📊 Total Rows       : {len(df):,}")
    print(f"📋 Columns          : {df.columns.tolist()}")
    print(f"💾 XLSX             : {xlsx}")
    print("=" * 60)
    return df


def _load_backup(data_prefix: str, dedupe_cols: Optional[List[str]] = None) -> list:
    path = Path(TEMP_DIR) / f"{data_prefix}.csv"
    if not path.exists():
        print(f"ℹ️  No backup found: {path}")
        return []
    try:
        df = pd.read_csv(path)
        if dedupe_cols:
            valid = [c for c in dedupe_cols if c in df.columns]
            if valid:
                df = df.drop_duplicates(subset=valid)
        rows = df.to_dict(orient="records")
        print(f"🔁 Backup loaded: {path}  ({len(rows):,} rows)")
        return rows
    except Exception as e:
        print(f"⚠️  Failed to load backup {path}: {e}")
        return []


# ════════════════════════════════════════════════════════════════
# CloakBrowser context
# ════════════════════════════════════════════════════════════════
async def _create_browser_context(block_images: bool = False):
    """Konvy is a public site — no saved login session needed, but we
    still use CloakBrowser for stealth / human-like fingerprinting."""
    context = await launch_persistent_context_async(
        "./data/konvy/browser_session",
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


async def _accept_cookie(page) -> bool:
    """Dismiss Konvy's cookie/notice banner if present."""
    selectors = [
        "div.got-it",
        ".accept-cookie",
        "button:has-text('ยอมรับ')",
        "button:has-text('Accept')",
    ]
    for sel in selectors:
        try:
            btn = page.locator(sel).first
            if await btn.count() > 0 and await btn.is_visible(timeout=3000):
                await btn.click()
                await asyncio.sleep(0.5)
                return True
        except Exception:
            continue
    return False


async def _dump_listing_debug(page, keyword: str, page_num: int) -> None:
    """
    Save + print HTML/screenshot of the first EMPTY listing page encountered
    this run. The notebook this was ported from logged "Found 0 products" on
    every brand it tried — this dump makes the next real run self-diagnosing
    instead of guessing at CSS selectors blind.
    """
    global _LISTING_DEBUG_DUMPED
    if _LISTING_DEBUG_DUMPED:
        return
    _LISTING_DEBUG_DUMPED = True
    ts = _make_timestamp()
    safe_kw = re.sub(r"[^\w-]", "_", keyword)[:30]
    base = Path(LOG_DIR) / f"listing_debug_{safe_kw}_p{page_num}_{ts}"
    try:
        html = await page.content()
        base.with_suffix(".html").write_text(html, encoding="utf-8")
    except Exception as e:
        print(f"   ⚠️  [DEBUG] Could not save HTML: {e}")
    try:
        await page.screenshot(path=str(base.with_suffix(".png")), full_page=True)
    except Exception as e:
        print(f"   ⚠️  [DEBUG] Could not save screenshot: {e}")
    print(f"   🧪 [DEBUG] Empty listing page dumped → {base}.{{html,png}}")


async def _scroll_until_stable(page, max_rounds: int = 12) -> int:
    """
    Konvy lazy-loads product tiles on scroll. The source notebook used a
    fixed 1-second sleep after two scroll calls, which was observed to
    return 0 tiles — scroll repeatedly instead and stop once the tile
    count stops growing for two consecutive rounds (same pattern already
    proven on Shopee's shop-listing scraper).
    """
    prev_count = -1
    stable_rounds = 0
    for _ in range(max_rounds):
        try:
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        except Exception:
            break
        await asyncio.sleep(1.0)
        try:
            count = await page.evaluate("document.querySelectorAll('p.product-name').length")
        except Exception:
            count = prev_count
        if count == prev_count and count > 0:
            stable_rounds += 1
            if stable_rounds >= 2:
                break
        else:
            stable_rounds = 0
        prev_count = count
    return max(prev_count, 0)


def _parse_listing_tiles(html: str, keyword: str) -> List[dict]:
    """
    Product tiles of a Konvy listing page — title search (/list/?title=…) and
    brand page (/brand/<slug>/) share one template — as {product_name_listing,
    product_url, image_url, keyword_searched}. Not de-duplicated.
    """
    soup = BeautifulSoup(html, "lxml")
    rows: List[dict] = []
    for name_tag in soup.find_all("p", class_="product-name"):
        # Konvy's actual DOM nesting may not put the link on the direct
        # parent <div> — walk up a few ancestor levels looking for an
        # <a href> so a slightly different wrapper structure than the
        # source notebook assumed doesn't silently yield zero rows.
        link_tag = None
        img_tag = None
        ancestor = name_tag
        for _ in range(4):
            ancestor = ancestor.find_parent(["div", "li", "a"])
            if not ancestor:
                break
            link_tag = (
                ancestor if ancestor.name == "a" and ancestor.has_attr("href")
                else ancestor.find("a", href=True)
            )
            img_tag = ancestor.find("img")
            if link_tag:
                break

        # Konvy's product URLs are SEO slugs ending in "-<id>.html"
        # (e.g. ".../shokubutsu-monogatari-shower-cream-...-121077.html")
        # — the old "/products/" check was inherited from the source
        # notebook and doesn't match this at all, so it silently
        # rejected every single product link, however correctly the
        # ancestor walk above found it. Confirmed against a real
        # listing-page dump (konvy/log/listing_debug_*.html).
        href_candidate = link_tag.get("href", "") if link_tag else ""
        if not link_tag or not re.search(r"-\d+\.html", href_candidate):
            continue
        href = link_tag["href"]
        full_link = href if href.startswith("http") else f"https:{href}"
        img_url = (img_tag.get("original") or img_tag.get("src") or "") if img_tag else ""

        rows.append({
            "product_name_listing": name_tag.get_text(strip=True),
            "product_url": full_link,
            "image_url": img_url,
            "keyword_searched": keyword,
        })
    return rows


def _parse_paging(html: str) -> tuple:
    """
    (last page number, total product count or None) from a listing page's
    `div.sc_paging`, e.g. text "(649) 1 2 … 9 ถัดไป สุดท้าย" with hrefs
    "?page=2" … "?page=17".

    The numbered links are only a window (Cute Press shows 1–9 of 17 pages),
    so the real last page comes from the largest ?page=N in any href — the
    "สุดท้าย" (last) link carries it. The "(N)" in front is the total number
    of products, used to check a brand came out complete.
    """
    soup = BeautifulSoup(html, "lxml")
    paging = soup.select_one("div.sc_paging")
    if paging is None:
        return 1, None
    pages = [1]
    for a in paging.find_all("a"):
        label = a.get_text(strip=True)
        if label.isdigit():
            pages.append(int(label))
        m = re.search(r"[?&]page=(\d+)", a.get("href") or "")
        if m:
            pages.append(int(m.group(1)))
    m = re.match(r"\s*\((\d[\d,]*)\)", paging.get_text(" ", strip=True))
    total = int(m.group(1).replace(",", "")) if m else None
    return max(pages), total


def _page_url(url: str, pg: int) -> str:
    return f"{url}{'&' if '?' in url else '?'}page={pg}"


# ════════════════════════════════════════════════════════════════
# Low-level: paginate a listing page and collect product links
# ════════════════════════════════════════════════════════════════
async def _collect_listing(page, url: str, keyword: str, cookie_dismissed: bool = False) -> tuple:
    """
    Open a Konvy listing URL (title search or /brand/<slug>/), paginate to the
    real last page, and return (list of {product_name_listing, product_url,
    image_url, keyword_searched}, expected_total_or_None, cookie_dismissed).
    `expected_total` is the "(N)" product count Konvy prints above the page
    links — compare it with len(list) to see whether the listing came out whole.
    """
    collected: List[dict] = []

    await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
    await page.wait_for_selector("body", timeout=15_000)

    if not cookie_dismissed:
        await _accept_cookie(page)
        cookie_dismissed = True

    await asyncio.sleep(2)

    max_page, expected = 1, None
    pg = 0
    while pg < max_page:
        pg += 1
        print(f"   📄 Listing page {pg}/{max_page} …")
        if pg > 1:
            try:
                await page.goto(_page_url(url, pg), wait_until="domcontentloaded", timeout=60_000)
            except Exception as e:
                print(f"   ⚠️  Page {pg} navigation failed: {e}")
                break
            await asyncio.sleep(1.5)

        tile_count = await _scroll_until_stable(page)
        html = await page.content()
        if pg == 1:
            try:
                max_page, expected = _parse_paging(html)
            except Exception:
                pass
            if max_page > 1 or expected:
                print(f"   🔢 {max_page} page(s)" + (f", {expected:,} product(s) listed" if expected else ""))

        if tile_count == 0:
            print(f"   ⚠️  Page {pg}: 0 product tiles found (JS selector check).")
            await _dump_listing_debug(page, keyword, pg)
            continue

        rows = _parse_listing_tiles(html, keyword)
        collected.extend(rows)
        print(f"   ✅ Page {pg}: {len(rows)} product(s) ({len(collected)} total so far).")

        # Tiles were detected (tile_count > 0) but link/href extraction still
        # came back empty — the DOM nesting assumption doesn't match Konvy's
        # current markup. Dump the real HTML so the actual structure can be
        # inspected instead of guessing at another selector blind.
        if not rows:
            print(f"   ⚠️  Page {pg}: tiles found but 0 could be mapped to a product URL.")
            await _dump_listing_debug(page, keyword, pg)

    seen_urls: set = set()
    deduped: List[dict] = []
    for item in collected:
        if item["product_url"] not in seen_urls:
            seen_urls.add(item["product_url"])
            deduped.append(item)
    return deduped, expected, cookie_dismissed


async def _collect_links_from_url(page, keyword: str, cookie_dismissed: bool = False) -> tuple:
    """
    Navigate to Konvy's title-search listing for *keyword*, paginate through
    all pages, and return (list of {product_name_listing, product_url,
    image_url, keyword_searched}, cookie_dismissed).
    """
    links, _expected, cookie_dismissed = await _collect_listing(
        page, f"{LISTING_BASE}?title={quote(keyword)}", keyword, cookie_dismissed
    )
    return links, cookie_dismissed


def _parse_event_params(html: str) -> dict:
    """
    Every Konvy product page embeds a clean analytics tracking object —
    `event_params = {...}; kys.viewProduct(event_params);` — with
    brand_name, a 3-level category hierarchy, and live stock count, all as
    valid JSON. Far more reliable than scraping the equivalent UI elements
    (the visible brand/category text isn't always present, but this
    tracking snippet is templated onto every product page regardless).
    """
    m = re.search(r"event_params\s*=\s*(\{.*?\});", html)
    if not m:
        return {}
    try:
        return json.loads(m.group(1))
    except Exception:
        return {}


def _brand_from_url(url: str) -> str:
    """
    Konvy's product URLs are /{brand-slug}/{product-slug}-{id}.html — the
    first path segment is the brand, hyphen-slugified (e.g.
    "shokubutsu-monogatari", "benice").
    """
    try:
        path = urlparse(url).path.strip("/")
        first_segment = path.split("/")[0] if path else ""
        return " ".join(w.capitalize() for w in first_segment.split("-")) if first_segment else ""
    except Exception:
        return ""


# ════════════════════════════════════════════════════════════════
# Low-level: scrape one product detail page
# ════════════════════════════════════════════════════════════════
async def _scrape_product_detail(page, item: dict) -> Optional[dict]:
    """
    Navigate to item['product_url'] and extract product_name, brand,
    product_id, price, and any other attribute-box fields.

    *item* may just be {"product_url": url} (standalone re-scrape) or the
    richer dict returned by `_collect_links_from_url` (listing name/image
    used as fallbacks when the detail page's own fields come back empty).
    """
    url = item.get("product_url", "")
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        await page.wait_for_selector("body", timeout=15_000)
        await asyncio.sleep(1.5)
        await _accept_cookie(page)

        html = await page.content()
        soup = BeautifulSoup(html, "lxml")

        product_name = ""
        if soup.title and soup.title.text:
            product_name = soup.title.text.split("-")[0].strip()
        if not product_name:
            product_name = item.get("product_name_listing", "")

        price_el = soup.select_one(".pro-top-con-price .ky-fw-500, .ky-fw-500")
        sale_price = price_el.get_text(strip=True) if price_el else ""

        # Original (pre-discount) price is the struck-through <span>; the
        # discount percentage is its sibling in the same wrapping <p>.
        original_price = ""
        discount_percent = ""
        strike_el = soup.select_one('span[style*="line-through"]')
        if strike_el:
            original_price = strike_el.get_text(strip=True)
            parent_p = strike_el.find_parent("p")
            if parent_p:
                pct_el = parent_p.find("span", class_="ky-fw-b")
                if pct_el:
                    discount_percent = pct_el.get_text(strip=True)

        event_params = _parse_event_params(html)
        stock = str(event_params.get("stock", "")) if event_params.get("stock") not in (None, "") else ""
        category = event_params.get("first_category_name", "")
        sub_category = event_params.get("second_category_name", "")
        leaf_category = event_params.get("third_category_name", "")

        # The purchase-count badge ("<N> สั่งแล้ว" / "<N> Bought") lives in
        # span.pro_dotteh_Bought — easy to miss because it sits inside a
        # div.soldStatus wrapper alongside actual "sold out" stock-status
        # text, so a plain substring search for "sold" surfaces the wrong
        # element first. Named `sold` here (not `bought_count`) to match
        # the column name Watsons/Shopee use for the same purchase-volume
        # signal, so downstream cross-platform analysis can join on it.
        sold = ""
        sold_el = soup.select_one("span.pro_dotteh_Bought")
        if sold_el:
            sold = sold_el.get_text(strip=True)

        review_count = ""
        review_el = soup.select_one("p.review_hover span, .review_hover span")
        if review_el:
            review_count = review_el.get_text(strip=True)

        rating_score = ""
        grade_lt = soup.select_one("div.pro-detail-right-grade_lt")
        if grade_lt:
            score_el = grade_lt.find("span", class_="fl")
            if score_el:
                rating_score = score_el.get_text(strip=True)

        brand = ""
        product_id = ""
        other_attrs: dict = {}

        attr_box = soup.find("div", class_="attribute-box")
        if attr_box:
            for li in attr_box.find_all("li"):
                label_el = li.find("span", class_="left")
                value_el = li.find("span", class_="sub_attribute") or li.find("a")
                if not (label_el and value_el):
                    continue
                label = label_el.get_text(strip=True)
                # Konvy joins multi-value attributes (e.g. several concern
                # tags) as sibling <a> tags separated by literal ",  " text
                # nodes — get_text() picks up a stray trailing comma from
                # the last one, so strip it rather than losing the
                # meaningful commas *between* genuinely separate values.
                value = value_el.get_text(strip=True).strip(", ").strip()
                label_key = label.lower().rstrip(":").strip()
                if label_key in _PRODUCT_ID_LABELS:
                    product_id = value
                elif label_key in _BRAND_LABELS:
                    brand = value
                elif label and value:
                    other_attrs[label] = value

        if not brand:
            # No explicit brand field on this product's attribute box —
            # common when searching by category keyword rather than brand
            # name. event_params.brand_name is the most reliable fallback
            # (it's a clean tracking value, not scraped UI text); the URL
            # slug and search keyword are progressively weaker fallbacks
            # for the rare case event_params itself isn't present.
            brand = event_params.get("brand_name", "") or _brand_from_url(url) or item.get("keyword_searched", "")

        if not product_id:
            product_id = str(event_params.get("product_id", "") or "")

        return {
            "scraped_date": _scraped_date(),
            "product_name": product_name,
            "brand": brand,
            "product_id": product_id,
            "sale_price": sale_price,
            "original_price": original_price,
            "discount_percent": discount_percent,
            "stock": stock,
            "category": category,
            "sub_category": sub_category,
            "leaf_category": leaf_category,
            "sold": sold,
            "review_count": review_count,
            "rating_score": rating_score,
            "image_url": item.get("image_url", ""),
            "product_url": url,
            "keyword_searched": item.get("keyword_searched", ""),
            "other_attributes": json.dumps(other_attrs, ensure_ascii=False),
            "source": "Konvy",
            "collected_at": _collected_at(),
        }
    except Exception as e:
        print(f"   ❌ Error scraping detail {url}: {e}")
        return None


# ════════════════════════════════════════════════════════════════
# Public API: Search / Detail
# ════════════════════════════════════════════════════════════════
async def run_search_keyword(keywords: List[str]) -> pd.DataFrame:
    """
    Search Konvy by brand/keyword (Konvy's listing is a title search, so
    "keyword" here is typically a brand name) and scrape full product
    detail (brand, price, product_id, image, …) for every listing found —
    one call, one merged dataframe, matching the Shopee/Lazada/Watsons
    listing behaviour.

    Returns
    -------
    pd.DataFrame — one row per product, detail columns + `keyword_searched`.
    """
    all_rows: List[dict] = (
        _load_backup("temp_konvy_search", DETAIL_DEDUPE_COLS)
        if RESUME_FROM_BACKUP else []
    )
    seen_urls = {r["product_url"] for r in all_rows}

    print(f"\n🔍 Konvy — Search by brand/keyword  ({len(keywords)} keyword(s))")
    context, page = await _create_browser_context(block_images=False)
    cookie_dismissed = False

    async with context:
        for kw in keywords:
            print(f"\n🔑 Keyword: '{kw}'  →  {LISTING_BASE}?title={quote(kw)}")

            links, cookie_dismissed = await _collect_links_from_url(page, kw, cookie_dismissed)

            if not links:
                print(f"   ⚠️  No products found for '{kw}'.")
                continue

            print(f"   ✅ {len(links)} product link(s) collected — fetching detail for each…")

            for i, item in enumerate(links, 1):
                url = item["product_url"]
                if url in seen_urls:
                    print(f"   ⏭️  [{i}/{len(links)}] Already collected — skip.")
                    continue

                print(f"   ⏳ [{kw}] {i}/{len(links)} → {url}")
                try:
                    row = await asyncio.wait_for(
                        _scrape_product_detail(page, item), timeout=PRODUCT_DETAIL_TIMEOUT
                    )
                except asyncio.TimeoutError:
                    print(f"      ⏱️  Timed out after {PRODUCT_DETAIL_TIMEOUT}s — skipping.")
                    row = None

                if row:
                    all_rows.append(row)
                    seen_urls.add(url)

                    if len(all_rows) % CHECKPOINT_EVERY_PRODUCTS == 0:
                        _save_checkpoint(all_rows, "temp_konvy_search", DETAIL_DEDUPE_COLS, "auto")

                await _human_delay()

    _save_checkpoint(all_rows, "temp_konvy_search", DETAIL_DEDUPE_COLS, "final")
    return _save_final(all_rows, "konvy_search_list", DETAIL_DEDUPE_COLS)


async def run_product_detail(
    product_urls: Optional[List[str]] = None,
    df_product: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Scrape detail pages for an explicit list of Konvy product URLs
    (standalone re-scrape entry point — e.g. re-running after a failure,
    or feeding in URLs collected elsewhere).
    """
    if product_urls is None:
        product_urls = []
    if df_product is not None and not df_product.empty and "product_url" in df_product.columns:
        extra = df_product["product_url"].dropna().unique().tolist()
        product_urls = list(dict.fromkeys(product_urls + extra))

    if not product_urls:
        print("❌ run_product_detail: no URLs provided.")
        return pd.DataFrame()

    all_rows: List[dict] = (
        _load_backup("temp_konvy_detail", DETAIL_DEDUPE_COLS)
        if RESUME_FROM_BACKUP else []
    )
    seen_urls = {r["product_url"] for r in all_rows}

    print(f"\n📝 Konvy — Product Detail  ({len(product_urls)} URL(s))")
    context, page = await _create_browser_context(block_images=False)

    async with context:
        for i, url in enumerate(product_urls, 1):
            if url in seen_urls:
                print(f"   ⏭️  [{i}/{len(product_urls)}] Already collected — skip.")
                continue

            print(f"   ⏳ {i}/{len(product_urls)} → {url}")
            try:
                row = await asyncio.wait_for(
                    _scrape_product_detail(page, {"product_url": url}), timeout=PRODUCT_DETAIL_TIMEOUT
                )
            except asyncio.TimeoutError:
                print(f"      ⏱️  Timed out after {PRODUCT_DETAIL_TIMEOUT}s — skipping.")
                row = None

            if row:
                all_rows.append(row)
                seen_urls.add(url)

                if len(all_rows) % CHECKPOINT_EVERY_PRODUCTS == 0:
                    _save_checkpoint(all_rows, "temp_konvy_detail", DETAIL_DEDUPE_COLS, "auto")

            await _human_delay()

    _save_checkpoint(all_rows, "temp_konvy_detail", DETAIL_DEDUPE_COLS, "final")
    return _save_final(all_rows, "konvy_detail", DETAIL_DEDUPE_COLS)


# ════════════════════════════════════════════════════════════════
# All-brands mode — walk every brand on Konvy's brand index
# ════════════════════════════════════════════════════════════════
# How it works: read the brand index page once (cached for a week in
# data/brand_links/), then for each brand run the same title search that
# run_search_keyword uses and keep only the products whose URL sits under the
# brand's own slug (konvy.com/<brand-slug>/<product>-<id>.html) — a title
# search alone also returns other brands' products that merely mention the
# name.
#
# NOTE: konvy.com's robots.txt disallows ClaudeBot, so this mode was written
# and tested offline only (fake pages) — the brand-index parsing in
# particular has NOT been checked against the live page. Known from the
# user's own links: brand pages are konvy.com/brand/<slug>, where the slug may
# carry a -2 / -1 / -new suffix that product URLs lack (brand/mistine-2 →
# konvy.com/mistine/…), see _slug_candidates. If the index can't be read, the
# run raises with a HTML/screenshot dump in data/konvy/log/; the escape hatch
# is `brands_file` — a csv/xlsx with a `name` column (and optionally `slug`).
BRAND_INDEX_URL         = "https://www.konvy.com/brand.php"   # the user's all-brands page (2026-11-17); link shape still unchecked
BRAND_LIST_DIR          = Path("./data/brand_links")   # same cache folder find_brand_links.py / Watsons use
BRAND_LIST_MAX_AGE_DAYS = 7
BRAND_LIST_MIN_BRANDS   = 30                           # fewer than this = page changed / not loaded

ALL_BRANDS_DELAY      = (4.0, 8.0)   # seconds between brands (on top of the per-product delay)
BRAND_PAGE_URL        = "https://www.konvy.com/brand/{slug}/"   # lists the brand's products, 40 a page,
                                                                 # with "(N)" = total above the page links
SHORTFALL_MIN         = 3            # a brand is "short" when it has this many (or 1 %) fewer products
SHORTFALL_FRACTION    = 0.01         # than the "(N)" Konvy lists for it
ALL_BRANDS_MAX_STALLS = 5            # this many failed/empty brands in a row = stop (blocked?)

# Single-segment konvy.com paths that are site sections, not brands — only
# consulted by the fallback that reads brands from "konvy.com/<slug>/" links.
_NON_BRAND_SEGMENTS = {
    "list", "brand", "brands", "cart", "login", "register", "search", "help", "sale", "new",
    "promotion", "promotions", "member", "order", "orders", "user", "about", "category",
    "categories", "team", "home", "blog", "news", "coupon", "coupons", "faq", "contact",
    "terms", "privacy", "app", "download", "flashsale", "flash-sale", "wishlist", "account",
}


def _slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


def _slug_from_url(url: str) -> str:
    """First path segment of a Konvy product URL = the brand slug (lower-cased)."""
    try:
        path = unquote(urlparse(url).path).strip("/")
        return path.split("/")[0].lower() if path else ""
    except Exception:
        return ""


def _slug_from_brand_url(url: str) -> str:
    """Brand slug out of a brand-page URL: /brand/<slug>/… or /<slug>/."""
    try:
        segs = [unquote(s) for s in urlparse(url).path.split("/") if s]
    except Exception:
        return ""
    if not segs:
        return ""
    if segs[0].lower() == "brand" and len(segs) > 1:
        slug = segs[1]
        if slug.isdigit() and len(segs) > 2:     # /brand/<id>/<slug>
            slug = segs[2]
        return slug.lower()
    return segs[0].lower()


def _base_slug(slug: str) -> str:
    """Brand-page slugs carry a de-duplication suffix (/brand/mistine-2, /brand/faris-by-naris-2,
    /brand/exfac-new, /brand/clear-nose-1) that the product URLs don't (/mistine/<product>-<id>.html)."""
    return re.sub(r"-(?:\d+|new)$", "", slug.lower())


def _slug_candidates(slug: str) -> set:
    """First-path-segment values a product of this brand may have: the brand-page slug and its base."""
    return {s for s in (slug.lower(), _base_slug(slug)) if s}


def _brand_keyword(name: str, slug: str) -> str:
    """Text to type into Konvy's title search for a brand. Konvy's search breaks
    on apostrophes; a non-Latin display name falls back to the slug's words."""
    cleaned = re.sub(r"[’'`\"]", "", name).strip()
    if cleaned and cleaned.isascii():
        return cleaned
    return _base_slug(slug).replace("-", " ") if slug else cleaned


def _parse_brand_links(anchors: List[dict]) -> List[tuple]:
    """
    Turn every <a> on the brand index page ({text, href, alt}) into
    [(name, slug, url)]. Two link shapes are recognised and the one that
    yields more brands wins: /brand/<slug>… and bare /<slug>/.
    """
    base = "https://www.konvy.com"
    with_prefix: Dict[str, dict] = {}
    bare: Dict[str, dict] = {}

    for a in anchors:
        href = (a.get("href") or "").strip()
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue
        if href.startswith("//"):
            href = "https:" + href
        elif href.startswith("/"):
            href = base + href
        parsed = urlparse(href)
        if parsed.netloc and not parsed.netloc.lower().endswith("konvy.com"):
            continue
        segs = [unquote(s) for s in parsed.path.split("/") if s]
        if not segs or segs[-1].lower().endswith((".html", ".php")):
            continue                                  # product / script URLs

        name = " ".join((a.get("text") or a.get("alt") or "").split())
        if segs[0].lower() == "brand":
            if len(segs) < 2:
                continue
            bucket = with_prefix
        elif len(segs) == 1 and re.fullmatch(r"[a-z0-9][a-z0-9-]*", segs[0].lower()) \
                and segs[0].lower() not in _NON_BRAND_SEGMENTS:
            bucket = bare
        else:
            continue

        slug = _slug_from_brand_url(href)
        if not slug:
            continue
        entry = bucket.setdefault(slug, {"name": "", "url": href})
        if name and not entry["name"]:
            entry["name"] = name

    chosen = with_prefix if len(with_prefix) >= len(bare) else bare
    return [
        (e["name"] or " ".join(w.capitalize() for w in slug.split("-")), slug, e["url"])
        for slug, e in chosen.items()
    ]


async def _dump_page_debug(page, tag: str) -> None:
    """HTML + screenshot of the current page into LOG_DIR (self-diagnosis when a page isn't what the code expects)."""
    base = Path(LOG_DIR) / f"{tag}_debug_{_make_timestamp()}"
    try:
        base.with_suffix(".html").write_text(await page.content(), encoding="utf-8")
    except Exception as e:
        print(f"   ⚠️  [DEBUG] Could not save HTML: {e}")
    try:
        await page.screenshot(path=str(base.with_suffix(".png")), full_page=True)
    except Exception as e:
        print(f"   ⚠️  [DEBUG] Could not save screenshot: {e}")
    print(f"   🧪 [DEBUG] Page dumped → {base}.{{html,png}}")


# Every <a> on a page as {text, href, alt} — the input _parse_brand_links expects.
ANCHORS_JS = (
    "els => els.map(e => { const img = e.querySelector('img'); "
    "return {text: (e.textContent || '').trim(), href: e.getAttribute('href') || '', "
    "alt: img ? (img.alt || img.title || '') : ''}; })"
)


async def _read_brand_index() -> List[tuple]:
    """Open Konvy's brand index and return [(name, slug, url)]."""
    context, page = await _create_browser_context(block_images=True)
    async with context:
        await page.goto(BRAND_INDEX_URL, wait_until="domcontentloaded", timeout=60_000)
        await _accept_cookie(page)
        await asyncio.sleep(3)

        # lazy-loaded / tabbed index: scroll until the link count stops growing
        prev, stable = -1, 0
        for _ in range(15):
            try:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            except Exception:
                break
            await asyncio.sleep(1.0)
            try:
                n = await page.evaluate("document.querySelectorAll('a[href]').length")
            except Exception:
                n = prev
            if n == prev:
                stable += 1
                if stable >= 2:
                    break
            else:
                stable = 0
            prev = n

        anchors = await page.eval_on_selector_all("a[href]", ANCHORS_JS)
        entries = _parse_brand_links(anchors)
        if len(entries) < BRAND_LIST_MIN_BRANDS:
            await _dump_page_debug(page, "brand_index")
    return entries


def _read_brand_file(path: Path) -> List[tuple]:
    """(name, slug) pairs from a csv/xlsx with a name/brand column and optional slug / url columns."""
    path = Path(path)
    if path.suffix.lower() in (".xlsx", ".xls"):
        df = pd.read_excel(path, dtype=str)
    else:
        df = pd.read_csv(path, dtype=str, encoding="utf-8-sig")
    df = df.fillna("")
    cols = {str(c).strip().lower(): c for c in df.columns}
    name_col = next((cols[c] for c in ("name", "brand", "brand_name") if c in cols), df.columns[0])
    slug_col, url_col = cols.get("slug"), cols.get("url")

    entries: List[tuple] = []
    for _, r in df.iterrows():
        name = str(r[name_col]).strip()
        if not name:
            continue
        slug = str(r[slug_col]).strip().lower() if slug_col else ""
        if not slug and url_col:
            slug = _slug_from_brand_url(str(r[url_col]).strip())
        entries.append((name, slug or _slugify(name)))
    return entries


def _latest_cached_brand_list() -> Optional[Path]:
    files = sorted(BRAND_LIST_DIR.glob("konvy_brandlist_*.csv"))
    if not files:
        return None
    latest = files[-1]
    age_days = (time.time() - latest.stat().st_mtime) / 86400
    return latest if age_days <= BRAND_LIST_MAX_AGE_DAYS else None


async def fetch_brand_list(refresh: bool = False, brands_file: Optional[str] = None) -> Dict[str, str]:
    """
    {brand name: slug} for every brand on Konvy's brand index.

    `brands_file` (csv/xlsx) replaces the index entirely. Otherwise the cached
    copy in data/brand_links/ is used when under a week old, else the index
    page is read once and cached.
    """
    entries: List[tuple] = []
    if brands_file:
        entries = _read_brand_file(Path(str(brands_file).strip().strip('"')))
        if not entries:
            raise RuntimeError(f"No brands found in {brands_file}")
        print(f"📋 Brand list from file: {brands_file}  ({len(entries)} brands)")
    else:
        cached = None if refresh else _latest_cached_brand_list()
        if cached:
            entries = _read_brand_file(cached)
            if len(entries) >= BRAND_LIST_MIN_BRANDS:
                print(f"📋 Brand list from cache: {cached}  ({len(entries)} brands)")
            else:
                entries = []

        if not entries:
            print(f"🌐 Reading the brand index: {BRAND_INDEX_URL}")
            triples = await _read_brand_index()
            if len(triples) < BRAND_LIST_MIN_BRANDS:
                raise RuntimeError(
                    f"Only {len(triples)} brands could be read from {BRAND_INDEX_URL} — the page "
                    f"changed or did not finish loading (HTML dump saved in {LOG_DIR}). Nothing saved. "
                    "Workaround: pass a brands_file (csv/xlsx with a 'name' column, optional 'slug')."
                )
            BRAND_LIST_DIR.mkdir(parents=True, exist_ok=True)
            out = BRAND_LIST_DIR / f"konvy_brandlist_{datetime.now(TZ_THAI):%Y%m%d}.csv"
            with open(out, "w", encoding="utf-8-sig", newline="") as fh:
                w = csv.writer(fh)
                w.writerow(["name", "slug", "url"])
                w.writerows(triples)
            print(f"💾 {len(triples)} Konvy brands cached → {out}")
            entries = [(n, s) for n, s, _ in triples]

    brand_map: Dict[str, str] = {}
    for name, slug in entries:
        key = name
        if key in brand_map and brand_map[key] != slug:     # two brands with the same display name
            key = f"{name} [{slug}]"
        brand_map[key] = slug
    return brand_map


def _filter_brands(brand_map: Dict[str, str], only: List[str]) -> Dict[str, str]:
    """
    Keep the brands matching *only* (case-insensitive, by name or slug). A term
    that equals a brand picks just that brand; otherwise it falls back to
    "contained in" and the matches are printed so a wrong hit is visible.
    """
    picked: Dict[str, str] = {}
    for term in only:
        t = term.strip().lower()
        if not t:
            continue
        ts = _slugify(term)
        exact = [n for n, s in brand_map.items() if n.lower() == t or (ts and s == ts)]
        hits = exact or [n for n, s in brand_map.items() if t in n.lower() or (ts and ts in s)]
        if not hits:
            print(f"   ⚠️  '{term}' is not on the Konvy brand list.")
            continue
        if not exact:
            print(f"   ↪ '{term}' matched: {', '.join(hits[:8])}{' …' if len(hits) > 8 else ''}")
        for n in hits:
            picked[n] = brand_map[n]
    return picked


def _all_brands_prefix(detail: bool) -> str:
    return "temp_konvy_all_brands" if detail else "temp_konvy_all_brands_listing"


def _all_brands_state_path(detail: bool) -> Path:
    return Path(TEMP_DIR) / f"{_all_brands_prefix(detail)}_state.json"


def _load_all_brands_state(detail: bool) -> tuple:
    """(done, empty_seen, short_seen): finished brands, and in how many separate runs a
    brand came up empty / came out shorter than the count Konvy lists for it."""
    path = _all_brands_state_path(detail)
    if not path.exists():
        return {}, {}, {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data.get("done", {}), data.get("empty_seen", {}), data.get("short_seen", {})
    except Exception as e:
        print(f"⚠️  Could not read {path}: {e}")
        return {}, {}, {}


def _save_all_brands_state(detail: bool, done: dict, empty_seen: dict, short_seen: dict) -> None:
    payload = {
        "updated": datetime.now(TZ_THAI).strftime("%Y-%m-%d %H:%M:%S"),
        "done": done,
        "empty_seen": empty_seen,
        "short_seen": short_seen,
    }
    _all_brands_state_path(detail).write_text(
        json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8"
    )


def all_brands_checkpoint_summary(detail: bool = True) -> Optional[dict]:
    """What an earlier all-brands run left behind, or None (used by the menu)."""
    path = _all_brands_state_path(detail)
    done, _, _ = _load_all_brands_state(detail)
    if not done:
        return None
    return {
        "brands_done": len(done),
        "products": sum(int(v.get("products", 0)) for v in done.values()),
        "last_collected": datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M"),
    }


def _is_short(collected: int, expected: Optional[int]) -> bool:
    """True when a brand came out clearly smaller than the "(N)" Konvy lists for it
    (a failed page leaves ~40 missing; a couple of items shifting between pages doesn't count)."""
    if not expected:
        return False
    return expected - collected > max(SHORTFALL_MIN, int(expected * SHORTFALL_FRACTION))


async def _collect_brand(page, name: str, slug: str, source: str, slug_filter: bool,
                         cookie_dismissed: bool) -> tuple:
    """
    One brand's product links → (links, expected_total_or_None, cookie_dismissed).

    source "brand_page": open konvy.com/brand/<slug>/ and page through it (complete,
    and Konvy prints the product count there). If that page lists nothing (a guessed
    slug, a renamed brand) it falls back to the title search below.
    source "search": title search on the brand name, keeping only products whose URL
    is under the brand's slug.
    """
    links: List[dict] = []
    expected: Optional[int] = None
    if source == "brand_page":
        url = BRAND_PAGE_URL.format(slug=slug)
        print(f"   🔗 {url}")
        links, expected, cookie_dismissed = await _collect_listing(page, url, name, cookie_dismissed)
        if not links:
            print("   ↪ the brand page listed nothing — trying the title search instead")
            expected = None

    if not links:
        keyword = _brand_keyword(name, slug)
        print(f"   🔎 {LISTING_BASE}?title={quote(keyword)}")
        found, cookie_dismissed = await _collect_links_from_url(page, keyword, cookie_dismissed)
        wanted = _slug_candidates(slug)
        links = (
            [l for l in found if _slug_from_url(l["product_url"]) in wanted]
            if slug_filter else found
        )
        if found and not links:
            print(f"   ⚠️  Search found {len(found)} product(s), none under "
                  f"{' or '.join('/' + w + '/' for w in sorted(wanted))} (other brands that "
                  "mention the name).")

    for l in links:
        l["keyword_searched"] = name
    return links, expected, cookie_dismissed


async def run_all_brands(
    detail: bool = True,
    only: Optional[List[str]] = None,
    max_brands: Optional[int] = None,
    resume: bool = True,
    refresh_list: bool = False,
    brands_file: Optional[str] = None,
    source: str = "brand_page",
    slug_filter: bool = True,
) -> pd.DataFrame:
    """
    Scrape EVERY brand on Konvy's brand index (brand.php, ~1,200 brands), one
    brand after another.

    Parameters
    ----------
    detail : bool
        True  → full product detail per product (same columns as run_search_keyword).
        False → listing only (brand, product_name, product_url, image_url) — far
                faster; run action 3 / run_product_detail on the result afterwards.
    only : list[str], optional
        Restrict to these brands (name or slug; exact, else "contained in").
        If the brand index can't be read, the names typed here are used with a
        slug guessed from the name.
    max_brands : int, optional
        Brands to scrape in THIS run — lets a long job span several sessions.
    resume : bool
        Skip brands finished in an earlier run (progress is saved after every
        brand in data/konvy/collected_data/temp/).
    refresh_list : bool
        Re-read the brand index even if a cached copy is under a week old.
    brands_file : str, optional
        csv/xlsx with a `name` column (+ optional `slug`/`url`) used instead of the index.
    source : "brand_page" | "search"
        "brand_page" (default) pages through konvy.com/brand/<slug>/ — every product of
        the brand, checked against the product count Konvy prints there. "search" uses
        the title search on the brand name (misses products whose title lacks the name).
    slug_filter : bool
        Only for the title search (also the brand-page fallback): keep products whose URL
        is under the brand's slug (the brand-page slug, or it without a -2 / -1 / -new
        suffix: /brand/mistine-2 → konvy.com/mistine/…).

    A brand is only marked finished when all of its products were collected, so a
    brand that timed out, loaded empty or came out shorter than Konvy's own count
    is retried on the next run (a count that is still short after a second run is
    accepted and flagged in the summary). The run stops early after
    ALL_BRANDS_MAX_STALLS failed/empty/short brands in a row (usually the WAF / a
    CAPTCHA — solve it in the browser, then resume).

    Returns
    -------
    pd.DataFrame — one row per product with `keyword_searched` = brand name.
    A per-brand summary CSV (status, products expected vs collected) is written
    next to the final file.
    """
    if source not in ("brand_page", "search"):
        raise ValueError("source must be 'brand_page' or 'search'")

    try:
        brand_map = await fetch_brand_list(refresh=refresh_list, brands_file=brands_file)
    except Exception as exc:
        if not only:
            raise
        print(f"⚠️  Brand list unavailable ({exc}) — using the names given, slug guessed from each name.")
        brand_map = {n.strip(): _slugify(n) for n in only if n.strip()}
        only = None

    if only:
        brand_map = _filter_brands(brand_map, only)
    if not brand_map:
        print("❌ None of the requested brands were found on the Konvy brand list.")
        return pd.DataFrame()

    prefix = _all_brands_prefix(detail)
    all_rows: List[dict] = []
    done: dict = {}
    empty_seen: dict = {}
    short_seen: dict = {}
    if resume:
        done, empty_seen, short_seen = _load_all_brands_state(detail)
        if done:
            all_rows = _load_backup(prefix, DETAIL_DEDUPE_COLS)
    seen_urls = {r["product_url"] for r in all_rows}

    todo = [(n, s) for n, s in brand_map.items() if n not in done]
    already = len(brand_map) - len(todo)
    if max_brands:
        todo = todo[:max_brands]

    mode = "full detail" if detail else "listing only"
    print(f"\n🏷️  Konvy — ALL brands ({mode}, via {source.replace('_', ' ')}): {len(brand_map)} brand(s) in scope, "
          f"{already} already done, {len(todo)} to scrape in this run.")
    if not todo:
        print("✅ Nothing left to do for these brands.")

    context = None
    outcome: Dict[str, str] = {}
    expected_by_brand: Dict[str, Optional[int]] = {}
    stalled = 0                      # consecutive failed / empty / short brands this run
    pending_empty: List[str] = []    # empty brands waiting for a later brand to prove the site works
    if todo:
        context, page = await _create_browser_context(block_images=False)
    cookie_dismissed = False

    try:
        if context is not None:
            async with context:
                for i, (name, slug) in enumerate(todo, 1):
                    print(f"\n{'─' * 60}\n[{i}/{len(todo)}] 🏷️  {name}  (slug '{slug}')")
                    try:
                        links, expected, cookie_dismissed = await _collect_brand(
                            page, name, slug, source, slug_filter, cookie_dismissed
                        )
                        expected_by_brand[name] = expected

                        if not links:
                            # A genuinely empty brand and a block look the same. It is
                            # confirmed empty when a later brand in this run loads fine,
                            # or when it comes up empty again in a separate run.
                            empty_seen[name] = empty_seen.get(name, 0) + 1
                            print("   ⚠️  No products collected for this brand.")
                            if empty_seen[name] >= 2:
                                outcome[name] = "empty"
                                done[name] = {"products": 0, "at": _collected_at()}
                                print("      (empty in two separate runs — treating as an empty brand)")
                            else:
                                outcome[name] = "empty (unconfirmed)"
                                pending_empty.append(name)
                                stalled += 1
                        else:
                            if detail:
                                for j, item in enumerate(links, 1):
                                    url = item["product_url"]
                                    if url in seen_urls:
                                        continue
                                    print(f"   ⏳ [{name}] {j}/{len(links)} → {url}")
                                    try:
                                        row = await asyncio.wait_for(
                                            _scrape_product_detail(page, item), timeout=PRODUCT_DETAIL_TIMEOUT
                                        )
                                    except asyncio.TimeoutError:
                                        print(f"      ⏱️  Timed out after {PRODUCT_DETAIL_TIMEOUT}s — skipping.")
                                        row = None
                                    if row:
                                        all_rows.append(row)
                                        seen_urls.add(url)
                                        if len(all_rows) % CHECKPOINT_EVERY_PRODUCTS == 0:
                                            _save_checkpoint(all_rows, prefix, DETAIL_DEDUPE_COLS, "auto")
                                    await _human_delay()
                            else:
                                now = _collected_at()
                                for it in links:
                                    if it["product_url"] in seen_urls:
                                        continue
                                    seen_urls.add(it["product_url"])
                                    all_rows.append({
                                        "brand":            name,
                                        "brand_slug":       slug,
                                        "product_name":     it["product_name_listing"],
                                        "product_url":      it["product_url"],
                                        "image_url":        it["image_url"],
                                        "keyword_searched": name,
                                        "source":           "Konvy",
                                        "collected_at":     now,
                                    })

                            missing = sum(1 for it in links if it["product_url"] not in seen_urls)
                            short = _is_short(len(links), expected)
                            if short:
                                short_seen[name] = short_seen.get(name, 0) + 1
                                print(f"   ⚠️  Collected {len(links)} of the {expected} product(s) Konvy lists "
                                      "for this brand.")

                            if missing:
                                outcome[name] = f"partial ({missing} of {len(links)} not collected)"
                                print(f"   ⚠️  {missing}/{len(links)} product page(s) failed — "
                                      "this brand will be retried on the next run.")
                            elif short and short_seen[name] < 2:
                                outcome[name] = f"partial (collected {len(links)} of {expected} listed)"
                                stalled += 1
                                print("      This brand will be retried on the next run.")
                            else:
                                outcome[name] = "done"
                                done[name] = {"products": len(links), "expected": expected, "at": _collected_at()}
                                if short:
                                    done[name]["short"] = True
                                    print("      (still short after two runs — accepting it, flagged in the summary)")
                                # a brand that worked proves the site isn't blocking, so the
                                # brands that came up empty just before it really are empty
                                for empty_name in pending_empty:
                                    outcome[empty_name] = "empty"
                                    done[empty_name] = {"products": 0, "at": done[name]["at"]}
                                pending_empty.clear()
                                stalled = 0

                    except Exception as exc:
                        outcome[name] = f"failed: {exc}"
                        stalled += 1
                        print(f"   ❌ Error on brand '{name}': {exc}")

                    _save_all_brands_state(detail, done, empty_seen, short_seen)
                    _save_checkpoint(all_rows, prefix, DETAIL_DEDUPE_COLS, f"brand {i}/{len(todo)}")

                    if stalled >= ALL_BRANDS_MAX_STALLS:
                        print(f"\n🛑 {stalled} brands in a row failed, came up empty or short — "
                              "probably blocked / CAPTCHA. Stopping; solve it in the browser, "
                              "then run again with resume.")
                        break

                    if i < len(todo):
                        await _human_delay(*ALL_BRANDS_DELAY)
    finally:
        _save_all_brands_state(detail, done, empty_seen, short_seen)

    _save_checkpoint(all_rows, prefix, DETAIL_DEDUPE_COLS, "final")
    df = _save_final(all_rows, "konvy_all_brands" if detail else "konvy_all_brands_listing",
                     DETAIL_DEDUPE_COLS)

    # Per-brand summary: what each brand in scope ended as, how many products Konvy lists vs collected
    counts = (
        df["keyword_searched"].value_counts().to_dict()
        if not df.empty and "keyword_searched" in df.columns else {}
    )
    summary = []
    for name, slug in brand_map.items():
        expected = expected_by_brand.get(name) or (done.get(name) or {}).get("expected")
        if name in done:
            if not done[name].get("products"):
                status = "done (0 products)"
            elif done[name].get("short"):
                status = "done (short — fewer than Konvy lists)"
            else:
                status = "done"
        else:
            status = outcome.get(name, "not run yet")
        summary.append({"brand": name, "slug": slug, "status": status,
                        "products_expected": expected, "products_collected": counts.get(name, 0)})
    sum_path = Path(OUTPUT_DIR) / f"konvy_all_brands_summary_{_make_timestamp()}.csv"
    pd.DataFrame(summary).to_csv(sum_path, index=False, encoding="utf-8-sig")
    left = sum(1 for s in summary if not s["status"].startswith("done"))
    print(f"📋 Brand summary: {sum_path}")
    print(f"   {len(brand_map) - left}/{len(brand_map)} brand(s) finished"
          + (f" — {left} left; run again with resume to continue." if left else "."))
    return df


# ════════════════════════════════════════════════════════════════
# Public API: Product Reviews
# ════════════════════════════════════════════════════════════════
async def _fetch_comments_page(page, product_id: str, page_num: int) -> dict:
    """Fetch one page of ajax_comment.php via the browser's `page.request`
    (not the standalone `requests` library) so the call carries the
    session's cookies. Konvy sits behind an Aliyun WAF that returns a JS
    challenge page (not JSON) to bare/cookie-less HTTP requests — a plain
    `requests.get()` gets blocked 100% of the time; routing through the
    already-warmed-up browser context is what actually gets past it."""
    url = (
        "https://www.konvy.com/team/ajax_comment.php"
        f"?action=comment_show_json_new&page={page_num}&team_id={product_id}"
        "&ctype=default&score=0&onlyhaveimg=0&imgbtnclicked=0"
    )
    resp = await page.request.get(url, headers={"Referer": "https://www.konvy.com/", "Accept": "application/json"})
    raw = await resp.text()
    return json.loads(raw)


async def run_product_reviews(
    product_ids: Optional[List[str]] = None,
    df_product: Optional[pd.DataFrame] = None,
    max_pages_per_product: Optional[int] = None,
) -> pd.DataFrame:
    """
    Scrape all customer reviews for the given Konvy product_id(s) via the
    internal ajax_comment.php JSON API, called through the browser session
    (needed to carry WAF-passing cookies — a bare HTTP request gets a JS
    challenge page instead of JSON). `product_id` must come from a prior
    `run_search_keyword` / `run_product_detail` call (it's read off the
    product page's attribute-box, not derivable from the product URL alone).

    Each review includes an `option` column (from the API's `spec_title`)
    identifying which variant/spec (size, pump vs. refill, formula, …) the
    reviewer purchased — this is per-review, not per-product, so multiple
    reviews on the same product_id can carry different option values.

    Parameters
    ----------
    product_ids : list[str], optional
    df_product  : pd.DataFrame, optional   (must contain a 'product_id' column)
    max_pages_per_product : int, optional
        จำกัดจำนวนหน้ารีวิวต่อสินค้า (10 รีวิว/หน้า) — สินค้ายอดนิยมมีรีวิวหลักพัน
        (100+ หน้า) ถ้าต้องการแค่ตัวอย่างล่าสุดสำหรับวิเคราะห์ให้ตั้งค่านี้ เช่น 10
        (= สูงสุด 100 รีวิวล่าสุดต่อสินค้า) ค่า default None = ดึงจนครบทุกหน้าเหมือนเดิม
    """
    if product_ids is None:
        product_ids = []
    if df_product is not None and not df_product.empty and "product_id" in df_product.columns:
        extra = (
            df_product["product_id"].dropna().astype(str)
            .loc[lambda s: s.str.strip() != ""].unique().tolist()
        )
        product_ids = list(dict.fromkeys([str(p) for p in product_ids] + extra))

    if not product_ids:
        print("❌ run_product_reviews: no product_id(s) provided (run a detail scrape first).")
        return pd.DataFrame()

    all_reviews: List[dict] = (
        _load_backup("temp_konvy_reviews", REVIEW_DEDUPE_COLS)
        if RESUME_FROM_BACKUP else []
    )
    seen_ids = {str(r.get("review_id")) for r in all_reviews}
    failed: List[dict] = []

    print(f"\n💬 Konvy — Product Reviews  ({len(product_ids)} product id(s))")

    context, page = await _create_browser_context(block_images=True)

    async def _warmup():
        # Warm up WAF-passing cookies once by visiting the site — without
        # this, ajax_comment.php returns a WAF challenge page instead of
        # JSON on every single request.
        try:
            await page.goto("https://www.konvy.com/", wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(random.uniform(1.0, 2.0))
        except Exception as e:
            print(f"   ⚠️  Failed to warm up session: {e}")

    await _warmup()

    MAX_CONTEXT_RESTARTS = 5
    restarts = 0

    async def _fetch_with_reconnect(pid, page_num):
        # Long review runs (hundreds of pages) have been observed to have
        # the browser/context close out from under them mid-run (Playwright
        # raises "Target page, context or browser has been closed") for
        # reasons outside this code (OS/renderer-level). Without recovery,
        # every remaining page for every remaining product fails instantly,
        # silently producing near-zero new data despite the run "succeeding"
        # (no uncaught exception). Reopen the browser and retry once instead.
        nonlocal context, page, restarts
        try:
            return await _fetch_comments_page(page, pid, page_num)
        except Exception as e:
            if restarts >= MAX_CONTEXT_RESTARTS or "closed" not in str(e).lower():
                raise
            restarts += 1
            print(f"   🔁 Browser session closed unexpectedly — reopening (attempt {restarts}/{MAX_CONTEXT_RESTARTS})...")
            try:
                await context.close()
            except Exception:
                pass
            context, page = await _create_browser_context(block_images=True)
            await _warmup()
            return await _fetch_comments_page(page, pid, page_num)

    try:
        for idx, pid in enumerate(product_ids, 1):
            print(f"\n{'─'*60}")
            print(f"[{idx}/{len(product_ids)}] Reviews for product_id: {pid}")

            page_num = 1
            while True:
                # Transient errors (DNS blips, connection resets) shouldn't
                # cost the whole product its reviews just because they hit
                # on page 1 — _fetch_with_reconnect already handles the
                # unrecoverable "browser closed" case; this retries anything
                # else (e.g. "getaddrinfo ENOTFOUND") a few times first.
                data = None
                last_error = None
                for attempt in range(1, 4):
                    try:
                        data = await _fetch_with_reconnect(pid, page_num)
                        break
                    except Exception as e:
                        last_error = e
                        print(f"   ⚠️  Request error on page {page_num} (attempt {attempt}/3): {e}")
                        await asyncio.sleep(random.uniform(3.0, 6.0))
                if data is None:
                    failed.append({"product_id": pid, "page": page_num, "error": str(last_error)})
                    break

                comments = data.get("comments") or []
                if not comments:
                    if page_num == 1:
                        print("   ⚠️  No reviews found.")
                    break

                new_count = 0
                for c in comments:
                    review_id = str(c.get("id"))
                    if review_id in seen_ids:
                        continue
                    seen_ids.add(review_id)
                    user = c.get("user") or {}
                    team = c.get("team") or {}
                    all_reviews.append({
                        "product_id": str(pid),
                        "review_id": review_id,
                        "option": c.get("spec_title", ""),
                        "user_id": str(user.get("id", "")),
                        "username": user.get("username", ""),
                        "skin_type": user.get("skintype", ""),
                        "age": user.get("birth_age", ""),
                        "review_content": c.get("content", ""),
                        "score": c.get("score", 0),
                        "review_date": c.get("create_time", ""),
                        "product_name": team.get("product", ""),
                        "brand_name": team.get("brand_name", ""),
                        "category": c.get("cate_id", ""),
                        "price": team.get("team_price", ""),
                        "images": json.dumps(c.get("images") or [], ensure_ascii=False),
                        "source": "Konvy",
                        "collected_at": _collected_at(),
                    })
                    new_count += 1

                print(f"   ✅ Page {page_num}: +{new_count} review(s) | Total: {len(all_reviews):,}")

                if len(all_reviews) % CHECKPOINT_EVERY_PRODUCTS == 0:
                    _save_checkpoint(all_reviews, "temp_konvy_reviews", REVIEW_DEDUPE_COLS, "auto")

                if max_pages_per_product is not None and page_num >= max_pages_per_product:
                    print(f"   > Reached max_pages_per_product cap ({max_pages_per_product}). Stop.")
                    break

                page_num += 1
                await asyncio.sleep(random.uniform(1.5, 3.0))
    finally:
        try:
            await context.close()
        except Exception:
            pass

    if failed:
        log_path = Path(LOG_DIR) / f"konvy_reviews_failed_{_make_timestamp()}.json"
        log_path.write_text(json.dumps(failed, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"⚠️  Failed page(s) logged: {log_path}")

    _save_checkpoint(all_reviews, "temp_konvy_reviews", REVIEW_DEDUPE_COLS, "final")
    return _save_final(all_reviews, "konvy_reviews", REVIEW_DEDUPE_COLS)
