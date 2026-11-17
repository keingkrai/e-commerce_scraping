"""
cloak_watsons.py
────────────────────────────────────────────────────────────────
Watsons Thailand scraper — mirrors cloak_shopee_AI_bot.py structure.

Key differences from the original notebook:
  • Uses CloakBrowser (Playwright) instead of plain Selenium
  • Fully async throughout  
  • Adds run_product_reviews() to scrape customer feedback
  • Checkpoint / resume pattern matches Shopee/Lazada conventions

Public entry-points:
  run_search_keyword(keywords)       → pd.DataFrame   full detail rows (brand/price/sold/rating/…)
  run_brand_url(brand_map)           → pd.DataFrame   full detail rows
  run_all_brands(...)                → pd.DataFrame   EVERY brand on Watsons' brand list
                                       (resumable across days; detail or listing-only)
  run_product_detail(product_urls)   → pd.DataFrame   detail rows
  run_product_reviews(product_urls)  → pd.DataFrame   review rows
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

import pandas as pd
from pydantic import BaseModel, Field
from cloakbrowser import launch_persistent_context_async

# ════════════════════════════════════════════════════════════════
# Directories & Config
# ════════════════════════════════════════════════════════════════

OUTPUT_DIR = "./data/watsons/collected_data"
TEMP_DIR   = "./data/watsons/collected_data/temp"
LOG_DIR    = "./data/watsons/log"

REQUEST_DELAY_MIN = 1.5
REQUEST_DELAY_MAX = 4.0

CHECKPOINT_EVERY_PRODUCTS = 10
CHECKPOINT_EVERY_PAGES    = 20

# Product-detail pages used to be fetched one at a time on a single tab,
# plus a fixed 1.5-4s "human" delay serialized after every single one —
# with hundreds of products that delay alone adds up to tens of minutes.
# Fetching several detail pages concurrently (separate tabs, same
# browser context) cuts total wall-clock time by roughly this factor.
# Lower it if Watsons starts throwing CAPTCHAs/blocks; raise it if it
# stays stable and you want it faster.
DETAIL_CONCURRENCY = 5

# Detail pages only need each image's `src` URL (read from the DOM),
# never the actual pixels — blocking the image requests skips a lot of
# network transfer per product page with no effect on extracted data.
DETAIL_BLOCK_IMAGES = True

# Hard ceiling per product-detail page. Playwright's own per-step timeouts
# don't always fire if the browser/page itself stalls (e.g. an anti-bot
# challenge page that never finishes loading) — this guarantees the loop
# always moves on instead of hanging forever on one URL.
PRODUCT_DETAIL_TIMEOUT = 90

RESUME_FROM_BACKUP = False

for _folder in [OUTPUT_DIR, TEMP_DIR, LOG_DIR]:
    Path(_folder).mkdir(parents=True, exist_ok=True)

print(f"📂 Output folder          : {OUTPUT_DIR}")
print(f"💾 Temp checkpoint folder : {TEMP_DIR}")

TZ_THAI = timezone(timedelta(hours=7))

DETAIL_DEDUPE_COLS = ["product_url"]
REVIEW_DEDUPE_COLS = ["product_url", "user_name", "comment_date", "comment"]

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
# Pydantic Models
# ════════════════════════════════════════════════════════════════

class ProductListingModel(BaseModel):
    product_name: str = ""
    product_url:  str = ""
    source:       str = "Watsons"
    collected_at: str = Field(
        default_factory=lambda: datetime.now(TZ_THAI).strftime("%Y-%m-%d %H:%M:%S")
    )


class ProductDetailModel(BaseModel):
    scraped_date:        str = ""
    brand:               str = ""
    product_name:        str = ""
    sale_price_thb:      str = ""
    original_price_thb:  str = ""
    discount:            str = ""
    rating_stars:        int = 0
    reviews:             str = ""
    sold:                str = ""
    product_code:        str = ""
    coupons:             str = ""
    free_gift:           str = ""
    main_image:          str = ""
    registration_number: str = ""
    ingredients:         str = ""
    product_content:     str = ""
    detail_images:       str = ""   # JSON list stored as string
    category:            str = ""
    sub_category:        str = ""
    product_url:         str = ""
    keyword_searched:    str = ""
    source:              str = "Watsons"
    collected_at:        str = Field(
        default_factory=lambda: datetime.now(TZ_THAI).strftime("%Y-%m-%d %H:%M:%S")
    )


class ReviewModel(BaseModel):
    product_url:  str = ""
    product_name: str = ""   # <-- Add this line
    user_name:    str = ""
    comment_date: str = ""
    rating_star:  int = 0
    comment:      str = ""
    verified:     str = ""   
    source:       str = "Watsons"
    collected_at: str = Field(
        default_factory=lambda: datetime.now(TZ_THAI).strftime("%Y-%m-%d %H:%M:%S")
    )

# ════════════════════════════════════════════════════════════════
# General Helpers
# ════════════════════════════════════════════════════════════════
def _make_timestamp() -> str:
    return datetime.now(TZ_THAI).strftime("%Y%m%d_%H%M")

def _scraped_date() -> str:
    return datetime.now(TZ_THAI).strftime("%d/%m/%Y")


def _clean_sold(raw: str) -> str:
    """
    Strip Thai boilerplate from sold-count text and return a plain number string.

    Examples
    --------
    'ขายไปแล้ว 6000+ ชิ้น'  →  '6000'
    'ขายไปแล้ว 1.2K+ ชิ้น'  →  '1200'
    'ขายไปแล้ว 2.5M ชิ้น'   →  '2500000'
    '6000+'                  →  '6000'
    ''                       →  ''
    """
    if not raw or not raw.strip():
        return ""

    # Remove Thai words and unit words, keep digits / decimals / K / M / +
    cleaned = raw.strip()
    for word in ["ขายไปแล้ว", "ชิ้น", "ขาย", "แล้ว"]:
        cleaned = cleaned.replace(word, "")
    cleaned = cleaned.replace("+", "").strip()

    # Handle K / M suffixes (case-insensitive)
    lower = cleaned.lower()
    try:
        if lower.endswith("m"):
            value = float(lower[:-1]) * 1_000_000
        elif lower.endswith("k"):
            value = float(lower[:-1]) * 1_000
        else:
            value = float(cleaned)
        # Return as integer string when the value is whole, e.g. '6000' not '6000.0'
        return str(int(value)) if value == int(value) else str(value)
    except ValueError:
        # Fallback: return whatever digits we can extract
        digits = re.sub(r"[^\d]", "", cleaned)
        return digits if digits else raw.strip()

async def _human_delay(
    min_d: float = REQUEST_DELAY_MIN,
    max_d: float = REQUEST_DELAY_MAX,
):
    await asyncio.sleep(random.uniform(min_d, max_d))

def _save_checkpoint(
    rows: list,
    data_prefix: str,
    dedupe_cols: Optional[List[str]] = None,
    reason: str = "checkpoint",
):
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

def _save_final(
    rows: list,
    final_prefix: str,
    dedupe_cols: Optional[List[str]] = None,
) -> pd.DataFrame:
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
    csv  = Path(OUTPUT_DIR) / f"{final_prefix}_{ts}.csv"

    df.to_excel(xlsx, index=False)
    df.to_csv(csv, index=False, encoding="utf-8-sig")

    print("\n" + "=" * 60)
    print(f"✅ FINAL FILE SAVED : {final_prefix}")
    print(f"📊 Total Rows       : {len(df):,}")
    print(f"📋 Columns          : {df.columns.tolist()}")
    print(f"💾 XLSX             : {xlsx}")
    print(f"💾 CSV              : {csv}")
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
    """
    Launch a CloakBrowser persistent context.
    Watsons is a public site so no saved session directory is needed;
    we still use CloakBrowser for stealth / human-like fingerprinting.
    """
    context = await launch_persistent_context_async(
        "./data/watsons/browser_session",
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

# ════════════════════════════════════════════════════════════════
# Anti-bot / cookie helpers
# ════════════════════════════════════════════════════════════════
async def _clear_onetrust_overlay(page):
    """
    OneTrust's cookie-preference-center backdrop (`.onetrust-pc-dark-filter`)
    can stay attached to the DOM — invisible, but still at a near-max
    z-index — even after the visible cookie banner is dismissed or never
    shown at all. It silently swallows every click on the page underneath
    it, including pagination "next" clicks: Playwright reports the click
    as successful, but it actually lands on this dead overlay div instead
    of the real button, so nothing ever happens and no request fires.
    Confirmed by checking document.elementFromPoint() at the next-button's
    coordinates — it resolved to this overlay, not the button.
    """
    try:
        await page.evaluate(
            "document.querySelectorAll('.onetrust-pc-dark-filter, #onetrust-consent-sdk')"
            ".forEach(el => el.remove())"
        )
    except Exception:
        pass

async def _accept_cookie(page) -> bool:
    """Dismiss OneTrust / cookie consent banner if present."""
    selectors = [
        "#onetrust-accept-btn-handler",
        "button:has-text('Accept All')",
        "button:has-text('ยอมรับทั้งหมด')",
        "button:has-text('ยอมรับ')",
    ]
    clicked = False
    for sel in selectors:
        try:
            btn = page.locator(sel).first
            if await btn.count() > 0 and await btn.is_visible(timeout=4000):
                await btn.click()
                await asyncio.sleep(0.5)
                clicked = True
                break
        except Exception:
            continue

    await _clear_onetrust_overlay(page)
    return clicked

async def _human_scroll(page, amount: int = 600):
    await page.mouse.wheel(0, amount)
    await asyncio.sleep(random.uniform(0.3, 0.8))

# ════════════════════════════════════════════════════════════════
# Low-level: paginate a listing page and collect product links
# ════════════════════════════════════════════════════════════════
async def _page_signature(page) -> list:
    """[tile count, first tile's link, current page number] — changes when the listing page changes."""
    try:
        return await page.evaluate(
            """() => {
                const t = document.querySelectorAll('e2-product-tile');
                const a = t.length ? t[0].querySelector('h2 a, a') : null;
                const cur = document.querySelector('.page-current');
                return [t.length, a ? (a.getAttribute('href') || a.textContent.trim()) : '',
                        cur ? cur.innerText : ''];
            }"""
        )
    except Exception:
        return []


async def _wait_for_page_change(page, before: list, timeout: float = 15.0) -> bool:
    """Poll until the listing differs from *before* (the app swaps tiles a moment after a click)."""
    waited = 0.0
    while waited < timeout:
        await asyncio.sleep(0.5)
        waited += 0.5
        now = await _page_signature(page)
        if now and now != before:
            await asyncio.sleep(1.0)       # let the rest of the tiles render
            return True
    return False


async def _go_to_next_page(page) -> bool:
    """
    Try to advance the Watsons pagination bar to the next page.

    Returns True if a next-page control was found, visible, enabled, and
    clicked; False if we're already on the last page. After the click it
    waits for the listing to actually change (a fixed 2 s sleep was too
    short: the next read could still see the previous page's tiles).
    """
    await _clear_onetrust_overlay(page)
    before = await _page_signature(page)

    next_selectors = [
        "//e2-pagination//a[contains(@aria-label,'Next') or contains(@aria-label,'next')]",
        "//e2-pagination//li[contains(@class,'next')]/a",
        # Brand list pages (Thai UI) build the pager as e2-pagination > div > ul >
        # li — a "1/2" fractional pager (prev arrow, 1/N, next arrow) at the top and
        # a numbered one at the bottom, with no aria-label/"next" class. The
        # next arrow is the last <li> of the first one. `//ul`, not `/ul`: the
        # old direct-child path never matched, so only page 1 was ever read.
        "//e2-pagination//ul/li[last()]/a",
    ]
    for sel in next_selectors:
        try:
            btn = page.locator(sel).first
            if await btn.count() == 0 or not await btn.is_visible():
                continue

            parent_li = btn.locator("xpath=..")
            parent_class = (await parent_li.get_attribute("class")) or ""
            btn_class = (await btn.get_attribute("class")) or ""
            aria_disabled = (await btn.get_attribute("aria-disabled")) or ""
            if "disabled" in parent_class or "disabled" in btn_class or aria_disabled == "true":
                continue

            await btn.scroll_into_view_if_needed()
            await btn.click()
            if await _wait_for_page_change(page, before):
                return True
            print("   ⏳ Page did not change after the click — clicking once more…")
            await _clear_onetrust_overlay(page)
            await btn.click()
            await _wait_for_page_change(page, before)
            return True        # if it still looks the same, the repeated-page check in the caller decides
        except Exception:
            continue
    return False


def _brand_list_url(url: str) -> str:
    """
    A brand's /all-brands/b/<id>/<name> URL is its brand-STORE page for brands
    that have one ("SHOP BY RANGE" + story, no product tiles at all — e.g.
    Garnier). The products are at /all-brands/list/<id>/<name>. Brands without
    a store page redirect there on their own, which hides the difference.
    """
    return url.replace("/all-brands/b/", "/all-brands/list/")


# Set by _collect_links_from_url for brand/list pages: the "N items" figure the
# page itself shows, so callers can tell a complete listing from a short one.
_last_listing_info: dict = {"reported_total": None}


async def _reported_total(page) -> Optional[int]:
    """The 'N items' count Watsons prints on brand list pages, or None."""
    try:
        text = await page.evaluate("document.body.innerText")
    except Exception:
        return None
    # English UI: "50 items".  Thai UI: "50 Garnier รายการ" — the "N … รายการ" header
    # (the per-page choices read "ดูตาม 32 รายการต่อหน้า", where รายการ is glued to ต่อ).
    m = (re.search(r"([\d,]+)\s+items?\b", text, re.I)
         or re.search(r"([\d,]+)\s+[^\d\n]{0,60}?รายการ(?=\s)", text))
    return int(m.group(1).replace(",", "")) if m else None


async def _scroll_load_all_tiles(page, max_rounds: int = 80) -> int:
    """
    Fallback for brand list pages that render WITHOUT a pagination bar: seen
    in the English UI, which shows the first batch of tiles (32) and loads the
    rest on scroll (Garnier: 32, then 50 after scrolling — the page's "50
    items"). Scroll to the bottom until the tile count stops growing for two
    rounds in a row. Returns the final tile count. (The Thai UI the scraper
    runs in has a real pager instead and never gets here.)
    """
    prev, stable = -1, 0
    for _ in range(max_rounds):
        try:
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        except Exception:
            break
        await asyncio.sleep(2.0)
        count = await page.locator("//e2-product-tile").count()
        stable = stable + 1 if count == prev else 0
        prev = count
        if stable >= 2:
            return count
    print(f"   ⚠️  Still loading tiles after {max_rounds} scrolls — stopping at {prev}.")
    return prev


async def _read_tile_items(tiles) -> List[dict]:
    """{product_name, product_url} for each product tile element."""
    items: List[dict] = []
    for tile in tiles:
        try:
            a_tag = await tile.query_selector("h2 a")
            if not a_tag:
                a_tag = await tile.query_selector("a")
            if not a_tag:
                continue
            name = (await a_tag.inner_text()).strip()
            href = await a_tag.get_attribute("href")
            if href:
                # Fix relative URLs leading to Protocol Error
                if href.startswith("/"):
                    href = "https://www.watsons.co.th" + href
                items.append({"product_name": name, "product_url": href})
        except Exception:
            continue
    return items


async def _collect_links_from_url(
    page,
    start_url: str,
    cookie_dismissed: bool = False,
) -> List[dict]:
    """
    Navigate to *start_url* (search or brand page), paginate through
    all pages, and return a list of {product_name, product_url} dicts.

    Pages are walked by clicking "next" until either the pagination bar
    has no next control left, or a page yields the same product URLs as
    the page before it (guards against a click that doesn't actually
    advance). Previously the loop trusted a single "total pages" number
    scraped off the pagination widget (`li[2]/span[2]`), which actually
    reads the *second page number shown*, not the true last page — so it
    silently capped collection early (e.g. stopping after 2 pages of 32
    tiles = 64 products) whenever a brand had more than a couple of pages.
    """
    collected: List[dict] = []
    MAX_PAGES = 200  # safety cap against a runaway loop
    prev_page_urls: set = set()

    start_url = _brand_list_url(start_url)
    brand_list_page = "/all-brands/list/" in start_url   # scroll-loaded, not paginated
    _last_listing_info["reported_total"] = None

    await page.goto(start_url, wait_until="domcontentloaded", timeout=60_000)
    await page.wait_for_selector("body", timeout=15_000)

    if not cookie_dismissed:
        await _accept_cookie(page)
        cookie_dismissed = True

    pg = 1
    while pg <= MAX_PAGES:
        print(f"   📄 Listing page {pg} …")

        # A single failed tile-wait used to end the *entire* pagination run
        # (treated as "this was the last page"), but an occasional slow
        # network/render tick can trip this on a page that's actually fine —
        # observed live: one run failed here on page 3, an immediate retry
        # of the same page succeeded cleanly. Retry a couple of times before
        # concluding there's really nothing more to collect.
        tiles_found = False
        for attempt in range(3):
            try:
                await page.wait_for_selector("//e2-product-tile", timeout=15_000)
                tiles_found = True
                break
            except Exception:
                if attempt < 2:
                    print(f"   ⏳ No tiles yet on page {pg} (attempt {attempt + 1}/3) — retrying…")
                    await asyncio.sleep(3)
        if not tiles_found:
            print(f"   ⚠️  No product tiles on page {pg} after retrying — stopping.")
            break

        await asyncio.sleep(1.5)

        if brand_list_page:
            if await page.locator("//e2-pagination").count() == 0:
                await _scroll_load_all_tiles(page)      # no pager → tiles load on scroll
            if pg == 1:
                _last_listing_info["reported_total"] = await _reported_total(page)

        tiles = await page.query_selector_all("//e2-product-tile")
        page_items = await _read_tile_items(tiles)
        this_page_urls = {it["product_url"] for it in page_items}

        # Right after a "next" click the app may not have swapped the tiles yet, so an
        # identical page is read once more before it is taken as "no further pages".
        if pg > 1 and this_page_urls and this_page_urls == prev_page_urls:
            await asyncio.sleep(4)
            tiles = await page.query_selector_all("//e2-product-tile")
            page_items = await _read_tile_items(tiles)
            this_page_urls = {it["product_url"] for it in page_items}

        collected.extend(page_items)
        print(f"   ✅ {len(tiles)} tile(s) on page {pg} ({len(collected)} total so far).")

        if pg > 1 and this_page_urls and this_page_urls == prev_page_urls:
            print(f"   🏁 Page {pg} repeated the previous page's products — stopping.")
            break
        prev_page_urls = this_page_urls

        advanced = await _go_to_next_page(page)
        if not advanced:
            print(f"   🏁 No further pages — stopped after page {pg}.")
            break
        pg += 1

    if pg > MAX_PAGES:
        print(f"   ⚠️  Hit safety cap of {MAX_PAGES} pages — stopping.")

    # Deduplicate while preserving order
    seen_urls: set = set()
    deduped: List[dict] = []
    for item in collected:
        if item["product_url"] not in seen_urls:
            seen_urls.add(item["product_url"])
            deduped.append(item)

    reported = _last_listing_info["reported_total"]
    if reported is not None:
        flag = "✅" if len(deduped) >= reported else "⚠️ "
        print(f"   {flag} Collected {len(deduped)} of the {reported} item(s) the page reports.")
    return deduped

# ════════════════════════════════════════════════════════════════
# Low-level: scrape one product detail page
# ════════════════════════════════════════════════════════════════
async def _scrape_product_detail(page, url: str) -> Optional[dict]:
    """
    Navigate to a single Watsons product page and extract all fields.
    Returns a dict or None on error.
    """
    try:
        print("      🌐 navigating…")
        await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        await page.wait_for_selector("body", timeout=15_000)
        await asyncio.sleep(1)
        print("      📄 page loaded, extracting fields…")

        # Dismiss cookie if it appears
        await _accept_cookie(page)

        # Click "Product Details" tab to reveal ingredients / registration number
        tab_clicked = False
        detail_tab_xpaths = [
            "//wtc-pdp-tab//*[contains(text(),'รายละเอียด') or contains(text(),'ข้อมูลสินค้า')]",
            "//*[contains(text(),'PRODUCT DETAILS') or contains(text(),'DETAILS')]",
        ]
        for tx in detail_tab_xpaths:
            try:
                tab = page.locator(tx).first
                if await tab.count() > 0:
                    await tab.scroll_into_view_if_needed()
                    await tab.click()
                    tab_clicked = True
                    await asyncio.sleep(1)
                    break
            except Exception:
                continue

        # Extract main product-content text block
        product_content_text = ""
        content_selectors = [
            "wtc-product-details .product-details-container",
            "wtc-product-details",
            "div.product-details-container",
            "wtc-pdp-tab",
        ]
        for sel in content_selectors:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    await el.wait_for(state="attached", timeout=8_000)
                    txt = (await el.inner_text()).strip()
                    if len(txt) > 30:
                        product_content_text = txt
                        break
            except Exception:
                continue

        # BeautifulSoup parse of full page source
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(await page.content(), "lxml")

        # Breadcrumbs
        crumbs   = soup.select("div.breadcrumb-container span[itemprop='name']")
        category     = crumbs[1].text.strip() if len(crumbs) > 1 else ""
        sub_category = crumbs[2].text.strip() if len(crumbs) > 2 else ""

        # Main product image — try multiple XPaths
        product_img_url = ""
        for ix in [
            "//wtc-product-images//swiper//e2-media/img",
            "//wtc-product-images//img",
            "//e2-media/img",
        ]:
            try:
                img_el = page.locator(ix).first
                if await img_el.count() > 0:
                    src = await img_el.get_attribute("src")
                    if src:
                        product_img_url = src
                        break
            except Exception:
                pass

        # Registration number
        registration_number = ""
        for rx in [
            "//wtc-product-details//*[contains(text(),'เลขที่จดแจ้ง')]/following::*[1]",
        ]:
            try:
                el = page.locator(rx).first
                if await el.count() > 0:
                    registration_number = (await el.inner_text()).strip()
                    if registration_number:
                        break
            except Exception:
                pass

        # Ingredients
        ingredients = ""
        for ix in [
            "//wtc-product-details//*[contains(text(),'ส่วนประกอบ') or contains(text(),'Ingredients')]/following::*[1]",
        ]:
            try:
                el = page.locator(ix).first
                if await el.count() > 0:
                    ingredients = (await el.inner_text()).strip()
                    if ingredients:
                        break
            except Exception:
                pass

        # BS4 fields - Updated based on the HTML layout
        brand_el        = soup.select_one("h2.product-brand a, .product-brand")
        product_name_el = soup.select_one("h1.product-name, div.product-name")
        price_el        = soup.select_one("div.display-price span.price")
        orig_price_el   = soup.select_one("span.retail-price")
        discount_el     = soup.select_one("span.discount")
        code_el         = soup.select_one("div.extra-info-container .info .value")
        
        # Count FILLED stars only: <i class="icon-star"> (no "off" class)
        stars = len([
            i for i in soup.select("div.reviewRatingStar i")
            if "icon-star" in (i.get("class") or []) and "off" not in (i.get("class") or [])
        ])
        
        review_el       = soup.select_one("wtc-review-summary span")
        
        # Target the sold count text specifically if present
        sold_el         = soup.select_one("div.social-proof .sold-count")
        if not sold_el:
             sold_el = soup.select_one("div.social-proof")
             
        coupons         = [s.text.strip() for s in soup.select("e2-ecoupon-item div.des span")]
        free_gift_el    = soup.select_one("wtc-product-free-gift-info .name")
        detail_images   = [
            img.get("src")
            for img in soup.select("div.product-details-container img")
            if img.get("src") and img.get("src").startswith("http")
        ]

        # Clean up duplicated ingredient text inside product_content_text
        if product_content_text and ingredients:
            product_content_text = product_content_text.replace(ingredients, "").strip()
            for word in ["ส่วนประกอบ", "Ingredients", "เลขที่จดแจ้ง", "ประเทศไทย"]:
                product_content_text = product_content_text.split(word)[0].strip()

        return {
            "scraped_date":        _scraped_date(),
            "brand":               brand_el.text.strip()        if brand_el        else "",
            "product_name":        product_name_el.text.strip() if product_name_el else "",
            "sale_price_thb":      price_el.text.strip()        if price_el        else "",
            "original_price_thb":  orig_price_el.text.strip()   if orig_price_el   else "",
            "discount":            discount_el.text.strip()      if discount_el     else "",
            "rating_stars":        stars,
            "reviews":             review_el.text.strip()        if review_el       else "",
            "sold":                _clean_sold(sold_el.text.strip()) if sold_el else "",
            "product_code":        code_el.text.strip()          if code_el         else "",
            "coupons":             ", ".join(coupons),
            "free_gift":           free_gift_el.text.strip()     if free_gift_el    else "",
            "main_image":          product_img_url,
            "registration_number": registration_number,
            "ingredients":         ingredients,
            "product_content":     product_content_text,
            "detail_images":       json.dumps(detail_images, ensure_ascii=False),
            "category":            category,
            "sub_category":        sub_category,
            "product_url":         url,
            "tab_clicked":         tab_clicked,
            "source":              "Watsons",
            "collected_at":        datetime.now(TZ_THAI).strftime("%Y-%m-%d %H:%M:%S"),
        }

    except Exception as e:
        print(f"   ❌ Error scraping detail {url}: {e}")
        return None

# ════════════════════════════════════════════════════════════════
# Concurrent detail-page fetching
# ════════════════════════════════════════════════════════════════
async def _new_detail_page(context):
    page = await context.new_page()
    if DETAIL_BLOCK_IMAGES:
        async def _block(route):
            await route.abort()
        await page.route("**/*.{png,jpg,jpeg,webp,gif,svg}", _block)
    return page


def _brand_from_repeated_prefix(name: str) -> str:
    """
    On the handful of product pages where both the brand selector and the
    product-name selector come up empty (a different page template), the
    listing-tile name we fall back to still carries the brand — repeated
    as an ALL-CAPS prefix before the properly-cased title, e.g.
    'BENICE Benice Shower Cream Beautiful Whitening 400 Ml.'.
    Recovers that prefix as the brand; returns "" if the pattern isn't there.
    """
    if not name:
        return ""
    words = name.split()
    caps_run = []
    for w in words:
        if w.isupper() and any(c.isalpha() for c in w):
            caps_run.append(w)
        else:
            break
    if not caps_run:
        return ""
    n = len(caps_run)
    rest = words[n:n + n]
    if len(rest) == n and all(a.lower() == b.lower() for a, b in zip(caps_run, rest)):
        return " ".join(rest)
    return ""


async def _scrape_one_detail(context, item: dict, sem: asyncio.Semaphore) -> tuple:
    """
    Returns (url, row_or_None). The url is returned alongside the row
    (rather than looked up via a dict keyed by the Task object) because
    asyncio.as_completed() yields its own internal wrapper objects, not
    the original Task instances — a dict keyed by Task identity never
    matches what as_completed() actually hands back.
    """
    url = item["product_url"]
    async with sem:
        page = await _new_detail_page(context)
        try:
            await _human_delay(0.3, 1.2)
            row = await asyncio.wait_for(
                _scrape_product_detail(page, url), timeout=PRODUCT_DETAIL_TIMEOUT
            )
        except asyncio.TimeoutError:
            print(f"      ⏱️  Timed out after {PRODUCT_DETAIL_TIMEOUT}s — skipping: {url}")
            row = None
        finally:
            await page.close()

    if row and not row.get("product_name"):
        row["product_name"] = item.get("product_name", "")
    if row and not row.get("brand"):
        row["brand"] = _brand_from_repeated_prefix(row.get("product_name", ""))
    return url, row


async def _scrape_details_concurrent(
    context,
    items: List[dict],
    seen_urls: set,
    all_rows: List[dict],
    data_prefix: str,
    dedupe_cols: List[str],
    label: str = "",
    label_key: str = "keyword_searched",
) -> None:
    """
    Fetch detail pages for *items* using up to DETAIL_CONCURRENCY tabs
    at once (instead of one page, one at a time), appending finished
    rows into *all_rows*/*seen_urls* in place with the usual periodic
    checkpoint saves.
    """
    todo = [it for it in items if it["product_url"] not in seen_urls]
    skipped = len(items) - len(todo)
    if skipped:
        print(f"   ⏭️  Skipping {skipped} already-collected product(s).")
    if not todo:
        return

    print(f"   🚀 Fetching {len(todo)} product page(s) — {DETAIL_CONCURRENCY} tab(s) in parallel…")
    sem = asyncio.Semaphore(DETAIL_CONCURRENCY)
    tasks = [asyncio.create_task(_scrape_one_detail(context, it, sem)) for it in todo]

    done = 0
    for coro in asyncio.as_completed(tasks):
        url, row = await coro
        done += 1
        if row:
            if label:
                row[label_key] = label
            all_rows.append(row)
            seen_urls.add(url)

            if len(all_rows) % CHECKPOINT_EVERY_PRODUCTS == 0:
                _save_checkpoint(all_rows, data_prefix, dedupe_cols, "auto")

        print(f"   ✅ [{done}/{len(todo)}] → {url}")

# ════════════════════════════════════════════════════════════════
# Public API
# ════════════════════════════════════════════════════════════════
async def run_search_keyword(keywords: List[str]) -> pd.DataFrame:
    """
    Search Watsons by keyword(s) and scrape full product detail (brand,
    price, discount, sold, rating, review count, ingredients, …) for every
    listing found — one call, one merged dataframe per row, matching the
    Shopee/Lazada shop-listing behaviour (no separate "detail" pass needed).

    Parameters
    ----------
    keywords : list[str]
        e.g. ["toner pad", "eye shadow"]

    Returns
    -------
    pd.DataFrame  — one row per product, full detail columns (see
    ProductDetailModel) plus `keyword_searched`.
    """
    all_rows: List[dict] = (
        _load_backup("temp_watsons_search", DETAIL_DEDUPE_COLS)
        if RESUME_FROM_BACKUP else []
    )
    seen_urls = {r["product_url"] for r in all_rows}

    print(f"\n🔍 Watsons — Search by keyword  ({len(keywords)} keyword(s))")
    context, page = await _create_browser_context(block_images=False)
    cookie_dismissed = False

    async with context:
        for kw in keywords:
            search_url = (
                "https://www.watsons.co.th/en/search?text="
                + kw.replace(" ", "+")
            )
            print(f"\n🔑 Keyword: '{kw}'  →  {search_url}")

            # Collect the basic links/names from the pagination pages first
            links = await _collect_links_from_url(page, search_url, cookie_dismissed)
            cookie_dismissed = True

            if not links:
                print(f"   ⚠️  No products found for '{kw}'.")
                continue

            print(f"   ✅ {len(links)} product link(s) collected — fetching detail for each…")

            await _scrape_details_concurrent(
                context, links, seen_urls, all_rows,
                "temp_watsons_search", DETAIL_DEDUPE_COLS,
                label=kw, label_key="keyword_searched",
            )

    _save_checkpoint(all_rows, "temp_watsons_search", DETAIL_DEDUPE_COLS, "final")
    return _save_final(all_rows, "watsons_search_list", DETAIL_DEDUPE_COLS)

async def run_brand_url(brand_map: Dict[str, str]) -> pd.DataFrame:
    """
    Scrape all products from explicit Watsons brand pages.

    Parameters
    ----------
    brand_map : dict[str, str]
        { "Innisfree": "https://www.watsons.co.th/en/all-brands/list/160157/innisfree", … }

    Returns
    -------
    pd.DataFrame  — one row per product, detail columns.
    """
    all_rows: List[dict] = (
        _load_backup("temp_watsons_brand", DETAIL_DEDUPE_COLS)
        if RESUME_FROM_BACKUP else []
    )
    seen_urls = {r["product_url"] for r in all_rows}

    print(f"\n🏷️  Watsons — Scrape by brand URL  ({len(brand_map)} brand(s))")
    context, page = await _create_browser_context(block_images=False)
    cookie_dismissed = False

    async with context:
        for brand_name, brand_url in brand_map.items():
            print(f"\n🔍 Brand: '{brand_name}'  →  {brand_url}")

            links = await _collect_links_from_url(page, brand_url, cookie_dismissed)
            cookie_dismissed = True

            if not links:
                print(f"   ⚠️  No products found for brand '{brand_name}'.")
                continue

            print(f"   ✅ {len(links)} product link(s) collected.")

            await _scrape_details_concurrent(
                context, links, seen_urls, all_rows,
                "temp_watsons_brand", DETAIL_DEDUPE_COLS,
                label=brand_name, label_key="keyword_searched",
            )

    _save_checkpoint(all_rows, "temp_watsons_brand", DETAIL_DEDUPE_COLS, "final")
    return _save_final(all_rows, "watsons_brand", DETAIL_DEDUPE_COLS)

# ════════════════════════════════════════════════════════════════
# All-brands mode — walk Watsons' whole brand list
# ════════════════════════════════════════════════════════════════
# The brand list page (/th/b/brandlist, ~755 brands on one page) is allowed
# by robots.txt. robots.txt also asks for Crawl-delay 10 and lists visit
# hours 04:00–08:45 UTC (11:00–15:45 Thai); the visit hours are advisory, so
# a run outside them only prints a warning.
BRAND_LIST_URL          = "https://www.watsons.co.th/th/b/brandlist"
BRAND_LIST_DIR          = Path("./data/brand_links")   # same cache find_brand_links.py writes
BRAND_LIST_MAX_AGE_DAYS = 7
BRAND_LIST_MIN_BRANDS   = 100                          # fewer than this = page changed / not loaded

ALL_BRANDS_DELAY          = (10.0, 15.0)   # seconds between brands (robots Crawl-delay: 10)
ALL_BRANDS_MAX_STALLS     = 5              # this many failed/empty brands in a row = stop (blocked?)
VISIT_WINDOW_UTC          = (4 * 60, 8 * 60 + 45)   # minutes after 00:00 UTC


def _all_brands_prefix(detail: bool) -> str:
    return "temp_watsons_all_brands" if detail else "temp_watsons_all_brands_listing"


def _all_brands_state_path(detail: bool) -> Path:
    return Path(TEMP_DIR) / f"{_all_brands_prefix(detail)}_state.json"


def _load_all_brands_state(detail: bool) -> tuple:
    """(done, empty_seen): finished brands, and how many separate runs a brand came up empty in."""
    path = _all_brands_state_path(detail)
    if not path.exists():
        return {}, {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data.get("done", {}), data.get("empty_seen", {})
    except Exception as e:
        print(f"⚠️  Could not read {path}: {e}")
        return {}, {}


def _save_all_brands_state(detail: bool, done: dict, empty_seen: dict) -> None:
    payload = {
        "updated": datetime.now(TZ_THAI).strftime("%Y-%m-%d %H:%M:%S"),
        "done": done,
        "empty_seen": empty_seen,
    }
    _all_brands_state_path(detail).write_text(
        json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8"
    )


def all_brands_checkpoint_summary(detail: bool = True) -> Optional[dict]:
    """What an earlier all-brands run left behind, or None (used by the menu)."""
    path = _all_brands_state_path(detail)
    done, _ = _load_all_brands_state(detail)
    if not done:
        return None
    return {
        "brands_done": len(done),
        "products": sum(int(v.get("products", 0)) for v in done.values()),
        "last_collected": datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M"),
    }


def _read_brand_list_csv(path: Path) -> List[tuple]:
    with open(path, encoding="utf-8-sig", newline="") as fh:   # written with a BOM for Excel
        return [(r["name"], r["url"]) for r in csv.DictReader(fh) if r.get("name") and r.get("url")]


def _latest_cached_brand_list() -> Optional[Path]:
    files = sorted(BRAND_LIST_DIR.glob("watsons_brandlist_*.csv"))
    if not files:
        return None
    latest = files[-1]
    age_days = (time.time() - latest.stat().st_mtime) / 86400
    return latest if age_days <= BRAND_LIST_MAX_AGE_DAYS else None


async def fetch_brand_list(refresh: bool = False) -> Dict[str, str]:
    """
    {brand name: brand page URL} for every brand on Watsons' brand list.

    Uses the cached copy in data/brand_links/ (shared with
    scripts/find_brand_links.py) when it is under a week old; otherwise
    reads /th/b/brandlist once and caches it.
    """
    entries: List[tuple] = []
    cached = None if refresh else _latest_cached_brand_list()
    if cached:
        entries = _read_brand_list_csv(cached)
        if len(entries) >= BRAND_LIST_MIN_BRANDS:
            print(f"📋 Brand list from cache: {cached}  ({len(entries)} brands)")
        else:
            entries = []

    if not entries:
        print(f"🌐 Reading the brand list: {BRAND_LIST_URL}")
        context, page = await _create_browser_context(block_images=True)
        async with context:
            await page.goto(BRAND_LIST_URL, wait_until="domcontentloaded", timeout=60_000)
            await _accept_cookie(page)
            await asyncio.sleep(4)
            pairs = await page.eval_on_selector_all(
                'a[href*="/all-brands/"]',
                "els => els.map(e => [e.textContent.trim(), e.getAttribute('href')])",
            )
        seen: set = set()
        for name, href in pairs:
            url = href if href.startswith("http") else "https://www.watsons.co.th" + href
            if name and url not in seen:
                seen.add(url)
                entries.append((name, url))
        if len(entries) < BRAND_LIST_MIN_BRANDS:
            raise RuntimeError(
                f"Only {len(entries)} brands read from the Watsons brand list — "
                "page changed or did not finish loading. Nothing saved."
            )
        BRAND_LIST_DIR.mkdir(parents=True, exist_ok=True)
        out = BRAND_LIST_DIR / f"watsons_brandlist_{datetime.now(TZ_THAI):%Y%m%d}.csv"
        with open(out, "w", encoding="utf-8-sig", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["name", "url"])
            w.writerows(entries)
        print(f"💾 {len(entries)} Watsons brands cached → {out}")

    brand_map: Dict[str, str] = {}
    for name, url in entries:
        key = name
        if key in brand_map:                      # two brands with the same display name
            key = f"{name} [{url.rstrip('/').split('/')[-2]}]"
        brand_map[key] = url
    return brand_map


def _filter_brands(brand_map: Dict[str, str], only: List[str]) -> Dict[str, str]:
    """
    Keep the brands matching *only* (case-insensitive). A term that equals a
    brand name picks just that brand; otherwise it falls back to "contained
    in the brand name" and the matches are printed so a wrong hit is visible.
    """
    picked: Dict[str, str] = {}
    for term in only:
        t = term.strip().lower()
        if not t:
            continue
        exact = [n for n in brand_map if n.lower() == t]
        hits = exact or [n for n in brand_map if t in n.lower()]
        if not hits:
            print(f"   ⚠️  '{term}' is not on the Watsons brand list.")
            continue
        if not exact:
            print(f"   ↪ '{term}' matched: {', '.join(hits[:8])}{' …' if len(hits) > 8 else ''}")
        for n in hits:
            picked[n] = brand_map[n]
    return picked


def _warn_outside_visit_window() -> None:
    now = datetime.now(timezone.utc)
    minutes = now.hour * 60 + now.minute
    lo, hi = VISIT_WINDOW_UTC
    if not (lo <= minutes <= hi):
        print(
            "⚠️  robots.txt asks bots to visit between 04:00 and 08:45 UTC "
            "(11:00–15:45 Thai time); it is "
            f"{now.astimezone(TZ_THAI):%H:%M} Thai time now. Continuing — the window is advisory."
        )


async def run_all_brands(
    detail: bool = True,
    only: Optional[List[str]] = None,
    max_brands: Optional[int] = None,
    resume: bool = True,
    refresh_list: bool = False,
) -> pd.DataFrame:
    """
    Scrape EVERY brand on Watsons' brand list (~755), one brand page after
    another, paced at the robots.txt Crawl-delay between brands.

    Parameters
    ----------
    detail : bool
        True  → full product detail per product (same columns as run_brand_url).
        False → listing only (brand, product_name, product_url) — far faster;
                run action 3 / run_product_detail on the result afterwards.
    only : list[str], optional
        Restrict to these brand names (exact name, else "contained in").
    max_brands : int, optional
        Brands to scrape in THIS run — lets a long job span several days.
    resume : bool
        Skip brands finished in an earlier run (progress is saved after
        every brand in data/watsons/collected_data/temp/).
    refresh_list : bool
        Re-read the brand list even if a cached copy is under a week old.

    A brand is only marked finished when all of its products were collected,
    so a brand that timed out or loaded empty is retried on the next run. The
    run stops early after ALL_BRANDS_MAX_STALLS failed/empty brands in a row
    (usually a block or CAPTCHA — solve it in the browser, then resume).

    Returns
    -------
    pd.DataFrame — one row per product with `keyword_searched` = brand name.
    A per-brand summary CSV is written next to the final file.
    """
    brand_map = await fetch_brand_list(refresh=refresh_list)
    if only:
        brand_map = _filter_brands(brand_map, only)
        if not brand_map:
            print("❌ None of the requested brands were found on the Watsons brand list.")
            return pd.DataFrame()

    prefix = _all_brands_prefix(detail)
    all_rows: List[dict] = []
    done: dict = {}
    empty_seen: dict = {}
    if resume:
        done, empty_seen = _load_all_brands_state(detail)
        if done:
            all_rows = _load_backup(prefix, DETAIL_DEDUPE_COLS)
    seen_urls = {r["product_url"] for r in all_rows}

    todo = [(n, u) for n, u in brand_map.items() if n not in done]
    already = len(brand_map) - len(todo)
    if max_brands:
        todo = todo[:max_brands]

    mode = "full detail" if detail else "listing only"
    print(f"\n🏷️  Watsons — ALL brands ({mode}): {len(brand_map)} brand(s) in scope, "
          f"{already} already done, {len(todo)} to scrape in this run.")
    if not todo:
        print("✅ Nothing left to do for these brands.")
    else:
        _warn_outside_visit_window()

    context = None
    outcome: Dict[str, str] = {}
    stalled = 0                      # consecutive failed / unconfirmed-empty brands this run
    pending_empty: List[str] = []    # empty brands waiting for a later brand to prove the site works
    if todo:
        context, page = await _create_browser_context(block_images=False)
    cookie_dismissed = False

    try:
        if context is not None:
            async with context:
                for i, (name, url) in enumerate(todo, 1):
                    print(f"\n{'─' * 60}\n[{i}/{len(todo)}] 🏷️  {name}  →  {url}")
                    try:
                        links = await _collect_links_from_url(page, url, cookie_dismissed)
                        cookie_dismissed = True
                        reported = _last_listing_info["reported_total"]   # the page's own "N items"

                        if not links:
                            # A genuinely empty brand and a block look the same. It is
                            # confirmed empty when a later brand in this run loads fine,
                            # or when it comes up empty again in a separate run.
                            empty_seen[name] = empty_seen.get(name, 0) + 1
                            print("   ⚠️  No products listed for this brand.")
                            if empty_seen[name] >= 2:
                                outcome[name] = "empty"
                                done[name] = {"products": 0,
                                              "at": datetime.now(TZ_THAI).strftime("%Y-%m-%d %H:%M:%S")}
                                print("      (empty in two separate runs — treating as an empty brand)")
                            else:
                                outcome[name] = "empty (unconfirmed)"
                                pending_empty.append(name)
                                stalled += 1
                        else:
                            if detail:
                                await _scrape_details_concurrent(
                                    context, links, seen_urls, all_rows, prefix,
                                    DETAIL_DEDUPE_COLS, label=name, label_key="keyword_searched",
                                )
                            else:
                                now = datetime.now(TZ_THAI).strftime("%Y-%m-%d %H:%M:%S")
                                for it in links:
                                    if it["product_url"] in seen_urls:
                                        continue
                                    seen_urls.add(it["product_url"])
                                    all_rows.append({
                                        "brand":            name,
                                        "product_name":     it["product_name"],
                                        "product_url":      it["product_url"],
                                        "brand_url":        url,
                                        "keyword_searched": name,
                                        "source":           "Watsons",
                                        "collected_at":     now,
                                    })

                            missing = sum(1 for it in links if it["product_url"] not in seen_urls)
                            if missing:
                                outcome[name] = f"partial ({missing} of {len(links)} not collected)"
                                print(f"   ⚠️  {missing}/{len(links)} product page(s) failed — "
                                      "this brand will be retried on the next run.")
                            else:
                                outcome[name] = "done"
                                done[name] = {
                                    "products": len(links),
                                    "reported": reported,
                                    "at": datetime.now(TZ_THAI).strftime("%Y-%m-%d %H:%M:%S"),
                                }
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

                    _save_all_brands_state(detail, done, empty_seen)
                    _save_checkpoint(all_rows, prefix, DETAIL_DEDUPE_COLS, f"brand {i}/{len(todo)}")

                    if stalled >= ALL_BRANDS_MAX_STALLS:
                        print(f"\n🛑 {stalled} brands in a row failed or came up empty — "
                              "probably blocked / CAPTCHA. Stopping; solve it in the browser, "
                              "then run again with resume.")
                        break

                    if i < len(todo):
                        await _human_delay(*ALL_BRANDS_DELAY)
    finally:
        _save_all_brands_state(detail, done, empty_seen)

    _save_checkpoint(all_rows, prefix, DETAIL_DEDUPE_COLS, "final")
    df = _save_final(all_rows, "watsons_all_brands" if detail else "watsons_all_brands_listing",
                     DETAIL_DEDUPE_COLS)

    # Per-brand summary: what each brand in scope ended as, and how many products it has
    counts = (
        df["keyword_searched"].value_counts().to_dict()
        if not df.empty and "keyword_searched" in df.columns else {}
    )
    summary = []
    for name, url in brand_map.items():
        if name in done:
            got, site = done[name].get("products", 0), done[name].get("reported")
            if not got:
                status = "done (0 products)"
            elif site and got < site:
                status = f"done, but SHORT: page says {site} items, got {got}"
            else:
                status = "done"
        else:
            status = outcome.get(name, "not run yet")
        summary.append({"brand": name, "brand_url": url, "status": status,
                        "products_collected": counts.get(name, 0)})
    sum_path = Path(OUTPUT_DIR) / f"watsons_all_brands_summary_{_make_timestamp()}.csv"
    pd.DataFrame(summary).to_csv(sum_path, index=False, encoding="utf-8-sig")
    left = sum(1 for s in summary if not s["status"].startswith("done"))
    short = [s["brand"] for s in summary if "SHORT" in s["status"]]
    print(f"📋 Brand summary: {sum_path}")
    print(f"   {len(brand_map) - left}/{len(brand_map)} brand(s) finished"
          + (f" — {left} left; run again with resume to continue." if left else "."))
    if short:
        print(f"   ⚠️  {len(short)} brand(s) returned fewer products than the page reports: "
              f"{', '.join(short[:10])}{' …' if len(short) > 10 else ''}")
    return df

async def run_product_detail(
    product_urls: Optional[List[str]] = None,
    df_product: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Scrape detail pages for an explicit list of Watsons product URLs.

    Parameters
    ----------
    product_urls : list[str], optional
    df_product   : pd.DataFrame, optional   (must contain 'product_url' column)

    Returns
    -------
    pd.DataFrame  — one row per product, detail columns.
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
        _load_backup("temp_watsons_detail", DETAIL_DEDUPE_COLS)
        if RESUME_FROM_BACKUP else []
    )
    seen_urls = {r["product_url"] for r in all_rows}

    print(f"\n📝 Watsons — Product Detail  ({len(product_urls)} URL(s))")
    context, page = await _create_browser_context(block_images=False)

    async with context:
        items = [{"product_url": u} for u in product_urls]
        await _scrape_details_concurrent(
            context, items, seen_urls, all_rows,
            "temp_watsons_detail", DETAIL_DEDUPE_COLS,
        )

    _save_checkpoint(all_rows, "temp_watsons_detail", DETAIL_DEDUPE_COLS, "final")
    return _save_final(all_rows, "watsons_detail", DETAIL_DEDUPE_COLS)

# ════════════════════════════════════════════════════════════════
# Low-level: parse review cards on current page
# ════════════════════════════════════════════════════════════════
async def _parse_reviews_on_page(page, product_url: str, product_name: str = "") -> List[dict]:
    """
    Extract all visible review cards from the current page state.
    """
    reviews: List[dict] = []

    cards = await page.query_selector_all(".reviewComment.ng-star-inserted")

    if not cards:
        cards = await page.query_selector_all(
            ".review-comment-container .reviewComment, cx-review-details, .reviewContainer.ng-star-inserted"
        )

    if not cards:
        print("   ⚠️  No review cards found on this page.")
        return reviews

    collected_at = datetime.now(TZ_THAI).strftime("%Y-%m-%d %H:%M:%S")

    for card in cards:
        try:
            # ── Comment text ────────────────────────────────────
            comment = ""

            # Target the section requested from the HTML layout
            el = await card.query_selector(".text.ng-star-inserted")
            if el:
                comment = (await el.inner_text()).strip()

            # Fallback selectors
            if not comment:
                for csel in [".text", ".review-content", ".review-comment", "p.reviewText"]:
                    el_fallback = await card.query_selector(csel)
                    if el_fallback:
                        comment = (await el_fallback.inner_text()).strip()
                        if comment:
                            break

            if not comment:
                continue

            # ── Rating (stars) ──────────────────────────────────
            rating_star = 0
            star_container = await card.query_selector("#reviewRatingStar, .reviewRatingStar")
            if star_container:
                all_stars = await star_container.query_selector_all("i")
                for star in all_stars:
                    cls = (await star.get_attribute("class")) or ""
                    if "icon-star" in cls and "off" not in cls:
                        rating_star += 1
            else:
                star_el = await card.query_selector("cx-star-rating[aria-label], [aria-label*='out of']")
                if star_el:
                    aria = (await star_el.get_attribute("aria-label")) or ""
                    m = re.search(r"(\d+)\s*(?:out of|\/)", aria)
                    if m:
                        rating_star = int(m.group(1))

            # ── Username ────────────────────────────────────────
            user_name = "Anonymous"
            el_name = await card.query_selector(".reviewerName .name")
            if el_name:
                t = (await el_name.inner_text()).strip()
                if t:
                    user_name = t
            else:
                for name_sel in [".name", ".user-name", ".reviewer-name"]:
                    el_name_fb = await card.query_selector(name_sel)
                    if el_name_fb:
                        t = (await el_name_fb.inner_text()).strip()
                        if t:
                            user_name = t
                            break

            # ── Date ────────────────────────────────────────────
            comment_date = ""
            el_date = await card.query_selector(".date.ng-star-inserted")
            if el_date:
                comment_date = (await el_date.inner_text()).strip()
            else:
                for date_sel in [".date", ".review-date", "time", "[class*='date']"]:
                    el_date_fb = await card.query_selector(date_sel)
                    if el_date_fb:
                        t = (await el_date_fb.inner_text()).strip()
                        if t:
                            comment_date = t
                            break

            reviews.append({
                "product_url":  product_url,
                "product_name": product_name,  # <-- Added to dictionary
                "user_name":    user_name,
                "comment_date": comment_date,
                "rating_star":  rating_star,
                "comment":      comment,
                "verified":     "",
                "source":       "Watsons",
                "collected_at": collected_at,
            })

        except Exception as e:
            continue

    return reviews

async def _scrape_all_reviews_for_product(
    page,
    product_url: str,
    seen: set,
) -> List[dict]:
    """
    Open *product_url*, scroll to the review section, click "ดูเพิ่มเติม" 
    repeatedly until all comments are fully loaded.
    """
    product_reviews: List[dict] = []

    await page.goto(product_url, wait_until="domcontentloaded", timeout=60_000)
    await page.wait_for_selector("body", timeout=15_000)
    await asyncio.sleep(1.5)
    await _accept_cookie(page)

    # ── Extract Product Name ──────────────────────────────────
    product_name = ""
    try:
        el_pn = await page.query_selector(".product-name")
        if el_pn:
            product_name = (await el_pn.inner_text()).strip()
    except Exception:
        pass
    # ──────────────────────────────────────────────────────────

    for _ in range(6):
        await _human_scroll(page, random.randint(500, 800))

    try:
        review_comp = page.locator("wtc-product-reviews").first
        if await review_comp.count() > 0:
            await review_comp.scroll_into_view_if_needed()
            await asyncio.sleep(1.5)
    except Exception:
        pass

    review_present = False
    for sel in [
        "wtc-product-reviews .reviewContainer .reviewComment.ng-star-inserted",
        "wtc-product-reviews .reviewContainer .review-comment-container",
        "wtc-product-reviews .reviewComment.ng-star-inserted",
    ]:
        try:
            await page.wait_for_selector(sel, timeout=10_000)
            review_present = True
            break
        except Exception:
            continue

    if not review_present:
        print("   ⚠️  No review section found.")
        return product_reviews

    SHOW_MORE_SEL_PRIMARY  = ".reviewContainer > div.show-more.ng-star-inserted:not(.reverse)"
    SHOW_MORE_SEL_FALLBACK = "wtc-product-reviews .reviewContainer div.show-more.ng-star-inserted:not(.reverse)"

    click_count = 0
    previous_card_count = 0

    while True:
        try:
            cards = await page.query_selector_all(
                "wtc-product-reviews .reviewComment.ng-star-inserted"
            )
            current_card_count = len(cards)

            candidate = None
            for sel in [SHOW_MORE_SEL_PRIMARY, SHOW_MORE_SEL_FALLBACK]:
                loc = page.locator(sel)
                if await loc.count() > 0:
                    candidate_el = await page.query_selector(sel)
                    if candidate_el:
                        is_decoy = await page.evaluate(
                            """(el) => {
                                let node = el;
                                while (node) {
                                    if (node.tagName && node.tagName.toLowerCase() === 'wtc-product-why-not-try') {
                                        return true;
                                    }
                                    node = node.parentElement;
                                }
                                return false;
                            }""",
                            candidate_el,
                        )
                        if not is_decoy:
                            candidate = loc.first
                            break

            btn_visible = candidate is not None and (
                await candidate.count() > 0
            ) and await candidate.is_visible()

            if not btn_visible:
                print("   ✅ 'ดูเพิ่มเติม' button no longer visible — all reviews loaded.")
                break

            if click_count > 0 and current_card_count == previous_card_count:
                print(
                    f"   ✅ No new cards after click "
                    f"(Total {current_card_count} on screen). Stopping expansion."
                )
                break

            previous_card_count = current_card_count

            await candidate.scroll_into_view_if_needed()
            await asyncio.sleep(0.8)
            await candidate.click()
            click_count += 1
            print(
                f"   🖱️  Clicked 'ดูเพิ่มเติม' (Load More) "
                f"{click_count} time(s) | cards before click: {current_card_count}…"
            )
            await asyncio.sleep(3.0)

        except Exception as e:
            print(f"   ⚠️  Exception during show-more loop: {e}")
            break

        if click_count >= 100:
            print("   ⚠️  Reached 100-click limit. Stopping expansion.")
            break

    print("   📜 Scrolling each review card into view to trigger text rendering…")

    all_card_handles = await page.query_selector_all(
        "wtc-product-reviews .reviewComment.ng-star-inserted"
    )
    total_cards = len(all_card_handles)
    text_loaded = 0

    for idx, card_handle in enumerate(all_card_handles, 1):
        try:
            await card_handle.scroll_into_view_if_needed()
            
            for _ in range(12):          
                text_el = await card_handle.query_selector(".text.ng-star-inserted")
                if not text_el:
                    text_el = await card_handle.query_selector(".text")

                if text_el:
                    txt = (await text_el.inner_text()).strip()
                    if txt:
                        text_loaded += 1
                        break 
                        
                await asyncio.sleep(0.25)

        except Exception:
            continue

    print(f"   📊 {text_loaded}/{total_cards} cards have text rendered.")
    await asyncio.sleep(1.0)

    # ── Pass product_name down to parser ──────────────────────
    page_revs = await _parse_reviews_on_page(page, product_url, product_name)

    new_count = 0
    for r in page_revs:
        key = (r["product_url"], r["user_name"], r["comment_date"], r["comment"])
        if key not in seen:
            product_reviews.append(r)
            seen.add(key)
            new_count += 1

    if new_count > 0:
        print(f"   ✅ {new_count} text review(s) successfully scraped.")

    return product_reviews

# ════════════════════════════════════════════════════════════════
# Public API: Product Reviews
# ════════════════════════════════════════════════════════════════

async def run_product_reviews(
    product_urls: Optional[List[str]] = None,
    df_product:   Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Scrape all customer reviews for the given Watsons product URLs.

    Parameters
    ----------
    product_urls : list[str], optional
    df_product   : pd.DataFrame, optional   (must contain 'product_url' column)

    Returns
    -------
    pd.DataFrame  columns: product_url, user_name, comment_date,
                           rating_star, comment, verified,
                           source, collected_at
    """
    if product_urls is None:
        product_urls = []
    if df_product is not None and not df_product.empty and "product_url" in df_product.columns:
        extra = df_product["product_url"].dropna().unique().tolist()
        product_urls = list(dict.fromkeys(product_urls + extra))

    if not product_urls:
        print("❌ run_product_reviews: no URLs provided.")
        return pd.DataFrame()

    all_reviews: List[dict] = (
        _load_backup("temp_watsons_reviews", REVIEW_DEDUPE_COLS)
        if RESUME_FROM_BACKUP else []
    )
    seen = {
        (r["product_url"], r["user_name"], r["comment_date"], r["comment"])
        for r in all_reviews
    }
    failed_urls: List[dict] = []

    print(f"\n💬 Watsons — Product Reviews  ({len(product_urls)} URL(s))")
    context, page = await _create_browser_context(block_images=False)

    async with context:
        for idx, url in enumerate(product_urls, 1):
            print(f"\n{'─'*60}")
            print(f"[{idx}/{len(product_urls)}] Reviews for: {url}")
            try:
                product_reviews = await _scrape_all_reviews_for_product(
                    page, url, seen
                )
                all_reviews.extend(product_reviews)

                if len(all_reviews) % CHECKPOINT_EVERY_PRODUCTS == 0:
                    _save_checkpoint(
                        all_reviews, "temp_watsons_reviews",
                        REVIEW_DEDUPE_COLS, "auto"
                    )

            except Exception as e:
                print(f"   ❌ [ERROR] {e}")
                failed_urls.append({"product_url": url, "error": str(e)})
                _save_checkpoint(
                    all_reviews, "temp_watsons_reviews",
                    REVIEW_DEDUPE_COLS, "error-recovery"
                )

            await _human_delay()

    # Log failed URLs
    if failed_urls:
        log_path = Path(LOG_DIR) / f"watsons_reviews_failed_{_make_timestamp()}.json"
        log_path.write_text(
            json.dumps(failed_urls, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"⚠️  Failed URLs logged: {log_path}")

    _save_checkpoint(all_reviews, "temp_watsons_reviews", REVIEW_DEDUPE_COLS, "final")
    return _save_final(all_reviews, "watsons_reviews", REVIEW_DEDUPE_COLS)