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
  run_product_reviews(product_ids)    → pd.DataFrame   review rows (needs product_id from a detail scrape)
"""

import asyncio
import json
import random
import re
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Optional
from urllib.parse import quote, urlparse

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


# ════════════════════════════════════════════════════════════════
# Low-level: paginate a listing page and collect product links
# ════════════════════════════════════════════════════════════════
async def _collect_links_from_url(page, keyword: str, cookie_dismissed: bool = False) -> tuple:
    """
    Navigate to Konvy's title-search listing for *keyword*, paginate through
    all pages, and return (list of {product_name_listing, product_url,
    image_url, keyword_searched}, cookie_dismissed).
    """
    collected: List[dict] = []
    url = f"{LISTING_BASE}?title={quote(keyword)}"

    await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
    await page.wait_for_selector("body", timeout=15_000)

    if not cookie_dismissed:
        await _accept_cookie(page)
        cookie_dismissed = True

    await asyncio.sleep(2)

    max_page = 1
    try:
        page_nums = await page.eval_on_selector_all(
            "div.sc_paging a",
            "els => els.map(e => e.innerText.trim()).filter(t => /^\\d+$/.test(t)).map(Number)",
        )
        if page_nums:
            max_page = max(page_nums)
    except Exception:
        pass

    for pg in range(1, max_page + 1):
        print(f"   📄 Listing page {pg}/{max_page} …")
        if pg > 1:
            try:
                await page.goto(f"{url}&page={pg}", wait_until="domcontentloaded", timeout=60_000)
            except Exception as e:
                print(f"   ⚠️  Page {pg} navigation failed: {e}")
                break
            await asyncio.sleep(1.5)

        tile_count = await _scroll_until_stable(page)
        if tile_count == 0:
            print(f"   ⚠️  Page {pg}: 0 product tiles found (JS selector check).")
            await _dump_listing_debug(page, keyword, pg)
            continue

        html = await page.content()
        soup = BeautifulSoup(html, "lxml")
        product_names = soup.find_all("p", class_="product-name")

        page_count = 0
        for name_tag in product_names:
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

            collected.append({
                "product_name_listing": name_tag.get_text(strip=True),
                "product_url": full_link,
                "image_url": img_url,
                "keyword_searched": keyword,
            })
            page_count += 1

        print(f"   ✅ Page {pg}: {page_count} product(s) ({len(collected)} total so far).")

        # Tiles were detected (tile_count > 0) but link/href extraction still
        # came back empty — the DOM nesting assumption doesn't match Konvy's
        # current markup. Dump the real HTML so the actual structure can be
        # inspected instead of guessing at another selector blind.
        if page_count == 0:
            print(f"   ⚠️  Page {pg}: tiles found but 0 could be mapped to a product URL.")
            await _dump_listing_debug(page, keyword, pg)

    seen_urls: set = set()
    deduped: List[dict] = []
    for item in collected:
        if item["product_url"] not in seen_urls:
            seen_urls.add(item["product_url"])
            deduped.append(item)
    return deduped, cookie_dismissed


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
