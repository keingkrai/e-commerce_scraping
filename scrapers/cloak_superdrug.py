"""
cloak_superdrug.py
────────────────────────────────────────────────────────────────
Superdrug (www.superdrug.com, UK) scraper — brand / category listings,
product details and customer reviews.

Confirmed on the live site (2026-11-12):
  • The site is SAP Commerce (Spartacus, Angular). Every listing and product
    page is filled from a JSON API on api.superdrug.com — the same calls the
    page itself makes, so the data is read from there instead of the DOM.
      listing : /api/v2/sd/search?fields=PLP&categoryCode=<code>&currentPage=N&pageSize=100
                (brand code = the number in /cerave/b/403790, category code =
                the part after /c/, e.g. /skin/korean-japanese-beauty/c/k-beauty)
      detail  : /api/v2/sd/products/<code>?fields=FULL
  • The search call needs the Akamai cookies the page sets (fetch with
    credentials: 'include'); without them it answers "Access Denied" (403).
    Unknown `fields=` values are refused the same way, so only the two field
    sets the site itself uses are requested.
  • Reviews come from PowerReviews (display.powerreviews.com), 25 per call.
    Its merchant id and public display key are read at runtime from the
    request the review widget makes on a product page — nothing is hard-coded.
  • A listing row already carries name, brand, EAN, price, was-price,
    discount %, unit price, rating, review count, stock status, category and
    promotions — the detail pass adds ingredients, how-to-use, features,
    warnings, stock level and images.

Not done on purpose:
  • No keyword search: robots.txt disallows /search on www.superdrug.com.
    Use brand / category pages instead (they are allowed).
  • Nothing tries to get around an "Access Denied" block: the run backs off,
    reloads the site once, and stops (saving what it has) if blocks repeat.

Public entry-points:
  run_brand_products(targets)          →  pd.DataFrame  listing rows (brand/category URLs or brand names)
  run_product_detail(product_urls)     →  pd.DataFrame  detail rows
  run_product_reviews(product_urls)    →  pd.DataFrame  review rows
  checkpoint_summary(kind)             →  what a checkpoint holds ("listing" / "detail" / "review")
"""

import asyncio
import html
import json
import random
import re
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import parse_qs, quote, urlparse

import pandas as pd
from cloakbrowser import launch_persistent_context_async

# ════════════════════════════════════════════════════════════════
# Directories & Config
# ════════════════════════════════════════════════════════════════

USER_DATA_DIR = "./data/superdrug/browser_session"
OUTPUT_DIR    = "./data/superdrug/collected_data"
TEMP_DIR      = "./data/superdrug/collected_data/temp"
LOG_DIR       = "./data/superdrug/log"

SITE          = "https://www.superdrug.com"
API           = "https://api.superdrug.com/api/v2/sd"
API_PARAMS    = "lang=en_GB&curr=GBP"
BRANDS_AZ_URL = f"{SITE}/a-z-brands"
MEDIA_HOST    = "https://media.superdrug.com"

LISTING_PAGE_SIZE = 100                  # the site offers 60 / 80 / 100
LISTING_SORTS = ("ranking", "newest", "price-asc", "price-desc", "name-asc", "name-desc")
DEFAULT_MAX_PRODUCTS_PER_TARGET = None   # None = the whole brand / category
DEFAULT_MAX_REVIEWS_PER_PRODUCT = 500    # None = every review
REVIEW_PAGE_SIZE = 25                    # PowerReviews' own page size

LISTING_DELAY_MIN, LISTING_DELAY_MAX = 3.0, 6.0     # seconds between listing pages
DETAIL_DELAY_MIN,  DETAIL_DELAY_MAX  = 1.5, 4.0     # seconds between products
REVIEW_DELAY_MIN,  REVIEW_DELAY_MAX  = 1.5, 3.5     # seconds between review pages
TARGET_PAUSE_MIN,  TARGET_PAUSE_MAX  = 10, 25       # seconds between brands / categories

BLOCK_BACKOFF_MIN, BLOCK_BACKOFF_MAX = 60, 120      # wait after an "Access Denied"
MAX_BLOCKS_PER_RUN = 3                              # then stop instead of pushing on
API_RETRIES = 2                                     # retries for network / 5xx errors

REVIEW_WIDGET_SCROLLS = 15     # scroll steps (1.5 s each) waiting for the review widget
CHECKPOINT_EVERY = 10          # products between checkpoints
PAGE_TIMEOUT_MS  = 60000

RESUME_FROM_BACKUP = False

LISTING_DEDUPE_COLS = ["target_code", "product_code"]
DETAIL_DEDUPE_COLS  = ["product_url"]
REVIEW_DEDUPE_COLS  = ["review_id"]

# Columns that must stay text (leading zeros in EAN/GTIN, ids that look numeric)
_TEXT_COLS = {c: str for c in ("product_code", "base_product", "ean", "target_code",
                               "review_id", "variant_code", "gtin")}

_TARGET_RE     = re.compile(r"/(b|c)/([^/?#]+)")
_PRODUCT_RE    = re.compile(r"/p/([^/?#]+)")
_PR_REVIEWS_RE = re.compile(r"display\.powerreviews\.com/m/(\d+)/l/([^/]+)/product/([^/?]+)/reviews")
_BR_RE         = re.compile(r"<\s*br\s*/?\s*>|</\s*(p|li|div)\s*>", re.I)
_TAG_RE        = re.compile(r"<[^>]+>")

for _folder in [OUTPUT_DIR, TEMP_DIR, LOG_DIR]:
    Path(_folder).mkdir(parents=True, exist_ok=True)

TZ_THAI = timezone(timedelta(hours=7))

_blocks = 0
_reviews_api: Optional[dict] = None   # {merchant_id, locale, apikey} read from the widget

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


class _RunStopped(RuntimeError):
    """The run can't continue safely. Rows collected so far are still saved."""


class _Blocked(RuntimeError):
    """Akamai answered "Access Denied"."""


# ════════════════════════════════════════════════════════════════
# In-page fetch (uses the browser's own cookies and fingerprint)
# ════════════════════════════════════════════════════════════════

_JS_FETCH_JSON = r"""async ([url, withCookies]) => {
  try {
    const r = await fetch(url, { credentials: withCookies ? 'include' : 'omit' });
    const text = await r.text();
    let data = null;
    try { data = JSON.parse(text); } catch (e) {}
    return { status: r.status, data, head: data ? '' : text.slice(0, 300) };
  } catch (e) {
    return { status: 0, data: null, head: String(e) };
  }
}"""

_JS_BRAND_LINKS = r"""() => [...document.querySelectorAll('a[href*="/b/"]')]
  .map(a => a.getAttribute('href'))
  .filter((h, i, all) => h && all.indexOf(h) === i)"""


# ════════════════════════════════════════════════════════════════
# General Helpers
# ════════════════════════════════════════════════════════════════
def _make_timestamp() -> str:
    return datetime.now(TZ_THAI).strftime("%Y%m%d_%H%M")


def _scraped_date() -> str:
    return datetime.now(TZ_THAI).strftime("%d/%m/%Y")


def _collected_at() -> str:
    return datetime.now(TZ_THAI).strftime("%Y-%m-%d %H:%M:%S")


async def _human_delay(min_d: float, max_d: float):
    await asyncio.sleep(random.uniform(min_d, max_d))


def _where(e: Exception) -> str:
    """'ValueError: … (cloak_superdrug.py:123 in _listing_row)' for unexpected-error messages."""
    import traceback
    frames = traceback.extract_tb(e.__traceback__)
    last = frames[-1] if frames else None
    at = f" ({Path(last.filename).name}:{last.lineno} in {last.name})" if last else ""
    return f"{type(e).__name__}: {e}{at}"


def _clean_text(value) -> str:
    """HTML fragment → plain text (line breaks kept). The API writes 'undefined' for empty fields."""
    if value is None:
        return ""
    text = str(value)
    if text.strip().lower() in ("undefined", "null", "none"):
        return ""
    text = _TAG_RE.sub("", _BR_RE.sub("\n", text))
    lines = [re.sub(r"[ \t\xa0]+", " ", line).strip() for line in html.unescape(text).splitlines()]
    return "\n".join(line for line in lines if line)


def _slug(text: str) -> str:
    """'L'Oreal Paris' → 'loreal-paris' (matches Superdrug's brand URL slugs)."""
    text = re.sub(r"['’`]", "", str(text).lower())
    return re.sub(r"[^a-z0-9]+", "-", text).strip("-")


def _abs_url(path: str, host: str = SITE) -> str:
    if not path:
        return ""
    if path.startswith("http"):
        return path
    if path.startswith("//"):
        return "https:" + path
    return host + (path if path.startswith("/") else "/" + path)


def _product_code(url: str) -> Optional[str]:
    match = _PRODUCT_RE.search(str(url or ""))
    return match.group(1) if match else None


def _category_path(raw: Optional[str]) -> str:
    """'Skin/Moisturisers/Day Creams/undefined' → 'Skin > Moisturisers > Day Creams'."""
    parts = [p.strip() for p in str(raw or "").split("/")]
    return " > ".join(p for p in parts if p and p.lower() != "undefined")


def _promo_text(promotions) -> str:
    tags = []
    for promo in promotions or []:
        if not isinstance(promo, dict):
            continue
        tag = _clean_text(promo.get("simplifiedTag") or promo.get("tag") or promo.get("description"))
        if tag and tag not in tags:
            tags.append(tag)
    return " | ".join(tags)


def _price(product: dict) -> dict:
    price = product.get("price") or {}
    return {
        "price_gbp": price.get("value"),
        "original_price_gbp": price.get("oldValue"),
        "discount_pct": product.get("discountRoundel"),
        "unit_price": product.get("contentUnitPrice", ""),
    }


def _sanitize_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    """Strip control characters openpyxl refuses to write into a cell."""
    from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].map(lambda v: ILLEGAL_CHARACTERS_RE.sub("", v) if isinstance(v, str) else v)
    return df


def _save_checkpoint(rows: list, data_prefix: str, dedupe_cols: List[str], reason: str = "checkpoint"):
    if not rows:
        return
    df = pd.DataFrame(rows).drop_duplicates(subset=dedupe_cols)
    path = Path(TEMP_DIR) / f"{data_prefix}.csv"
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"   💾 Checkpoint [{reason}]: {path}  ({len(df):,} rows)")


def _save_final(rows: list, final_prefix: str, dedupe_cols: List[str]) -> pd.DataFrame:
    if not rows:
        print(f"   ⚠️  No data to save: {final_prefix}")
        return pd.DataFrame()
    df = _sanitize_for_excel(pd.DataFrame(rows).drop_duplicates(subset=dedupe_cols))
    xlsx = Path(OUTPUT_DIR) / f"{final_prefix}_{_make_timestamp()}.xlsx"
    df.to_excel(xlsx, index=False)
    print("\n" + "=" * 60)
    print(f"✅ FINAL FILE SAVED : {final_prefix}")
    print(f"📊 Total Rows       : {len(df):,}")
    print(f"📋 Columns          : {df.columns.tolist()}")
    print(f"💾 XLSX             : {xlsx}")
    print("=" * 60)
    return df


def _load_backup(data_prefix: str, enabled: Optional[bool] = None) -> list:
    if not (RESUME_FROM_BACKUP if enabled is None else enabled):
        return []
    path = Path(TEMP_DIR) / f"{data_prefix}.csv"
    if not path.exists():
        print(f"ℹ️  No backup found: {path}")
        return []
    try:
        df = pd.read_csv(path, dtype=_TEXT_COLS)
        rows = df.astype(object).where(df.notna(), None).to_dict(orient="records")
        print(f"🔁 Backup loaded: {path}  ({len(rows):,} rows)")
        return rows
    except Exception as e:
        print(f"⚠️  Failed to load backup {path}: {e}")
        return []


_CHECKPOINT_FILES = {
    "listing": "temp_superdrug_listing",
    "detail": "temp_superdrug_detail",
    "review": "temp_superdrug_review",
}


def checkpoint_summary(kind: str) -> Optional[dict]:
    """What the 'listing' / 'detail' / 'review' checkpoint holds, so a caller can offer to resume."""
    path = Path(TEMP_DIR) / f"{_CHECKPOINT_FILES[kind]}.csv"
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, dtype=str)
    except Exception:
        return None
    if df.empty:
        return None
    return {
        "rows": len(df),
        "products": df["product_url"].nunique() if "product_url" in df.columns else len(df),
        "last_collected": df["collected_at"].max() if "collected_at" in df.columns else "",
    }


# ════════════════════════════════════════════════════════════════
# Browser session
# ════════════════════════════════════════════════════════════════
async def _create_browser_context():
    context = await launch_persistent_context_async(
        USER_DATA_DIR,
        headless=False,
        viewport={"width": 1366, "height": 900},
        locale="en-GB",
        timezone_id="Asia/Bangkok",   # matches the IP the requests come from
        humanize=True,
    )
    page = context.pages[0] if context.pages else await context.new_page()
    return context, page


async def _dump_debug(page, label: str) -> None:
    safe = re.sub(r"[^\w-]+", "_", label, flags=re.UNICODE)[:50]
    base = Path(LOG_DIR) / f"{datetime.now(TZ_THAI).strftime('%Y%m%d_%H%M%S')}_{safe}"
    try:
        base.with_suffix(".html").write_text(await page.content(), encoding="utf-8")
        await page.screenshot(path=str(base.with_suffix(".png")))
        print(f"   📸 Debug dump: {base}.{{html,png}}")
    except Exception as e:
        print(f"   ⚠️  Could not save debug dump: {e}")


async def _dismiss_cookies(page) -> None:
    """Reject non-essential cookies on the OneTrust banner if it is showing."""
    for selector in ("#onetrust-reject-all-handler", "button:has-text('Reject All')"):
        try:
            button = page.locator(selector).first
            if await button.count() and await button.is_visible(timeout=3000):
                await button.click()
                await asyncio.sleep(0.5)
                return
        except Exception:
            continue


async def _goto(page, url: str) -> None:
    """Open a www page like a visitor would. Raises _Blocked on an Akamai denial page."""
    await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
    await asyncio.sleep(random.uniform(3.0, 5.0))
    if "access denied" in (await page.title()).lower():
        raise _Blocked(f"page {url}")
    await _dismiss_cookies(page)


async def _handle_block(page, what: str) -> None:
    """Back off after an "Access Denied", reload the site once, or stop the run."""
    global _blocks
    _blocks += 1
    await _dump_debug(page, f"blocked_{what}")
    if _blocks > MAX_BLOCKS_PER_RUN:
        raise _RunStopped(
            f"Superdrug answered 'Access Denied' {_blocks} times in this run. Stopping — "
            "wait an hour or two, then run again and continue from the checkpoint."
        )
    pause = random.uniform(BLOCK_BACKOFF_MIN, BLOCK_BACKOFF_MAX)
    print(f"   🚧 'Access Denied' ({what}) — block {_blocks}/{MAX_BLOCKS_PER_RUN}. "
          f"Backing off {pause:.0f}s, then reloading the site…")
    await asyncio.sleep(pause)
    try:
        await _goto(page, SITE + "/")
    except _Blocked:
        raise _RunStopped("The Superdrug home page itself is blocked for this browser / IP. Stopping.")


async def _goto_checked(page, url: str) -> None:
    """_goto with one back-off-and-retry on a block; a second block stops the run."""
    try:
        await _goto(page, url)
    except _Blocked:
        await _handle_block(page, "page")
        await _goto(page, url)


async def _fetch_json(page, url: str, with_cookies: bool = True, what: str = "api") -> Optional[dict]:
    """GET a JSON endpoint from inside the page. None for 404 / repeated errors."""
    for attempt in range(API_RETRIES + 2):   # +1 for a retry after a block
        result = await page.evaluate(_JS_FETCH_JSON, [url, with_cookies])
        status, data = result["status"], result["data"]
        if status == 200 and data is not None:
            return data
        if status == 404:
            return None
        if status == 403 or "access denied" in result["head"].lower():
            await _handle_block(page, what)
            continue
        print(f"   ⚠️  {what}: HTTP {status} (attempt {attempt + 1}) {result['head'][:120]}")
        await asyncio.sleep(5 * (attempt + 1))
    return None


# ════════════════════════════════════════════════════════════════
# Targets: brand / category URLs or brand names
# ════════════════════════════════════════════════════════════════
def _target_from_url(raw: str) -> Optional[dict]:
    path = urlparse(raw if raw.startswith("http") else _abs_url(raw)).path
    match = _TARGET_RE.search(path)
    if not match:
        return None
    segments = [s for s in path[:match.start()].split("/") if s]
    return {
        "label": segments[-1] if segments else match.group(2),
        "kind": "brand" if match.group(1) == "b" else "category",
        "code": match.group(2),
        "url": SITE + path,
    }


async def _resolve_targets(page, inputs: List[str]) -> List[dict]:
    """URLs are parsed directly; brand names are looked up on the A–Z brands page."""
    targets: List[dict] = []
    names: List[str] = []
    for raw in (s.strip() for s in inputs):
        if not raw:
            continue
        if "/b/" in raw or "/c/" in raw:
            target = _target_from_url(raw)
            if target:
                targets.append(target)
            else:
                print(f"   ⚠️  Not a Superdrug brand/category URL, skipped: {raw}")
        else:
            names.append(raw)

    if names:
        print(f"🔎 Looking up {len(names)} brand name(s) on {BRANDS_AZ_URL} …")
        await _goto_checked(page, BRANDS_AZ_URL)
        by_slug: Dict[str, str] = {}
        for href in await page.evaluate(_JS_BRAND_LINKS):
            target = _target_from_url(href)
            if target and target["kind"] == "brand":
                by_slug.setdefault(target["label"], href)
        for name in names:
            wanted = _slug(name)
            href = by_slug.get(wanted)
            if href:
                target = _target_from_url(href)
                target["label"] = name
                targets.append(target)
                print(f"   ✅ '{name}' → {target['url']}")
                continue
            close = [s for s in by_slug if wanted and (s.startswith(wanted) or wanted in s)][:8]
            hint = f"  Did you mean: {', '.join(close)}" if close else ""
            print(f"   ⚠️  Brand '{name}' not found on the A–Z page.{hint}")
    return targets


# ════════════════════════════════════════════════════════════════
# Rows
# ════════════════════════════════════════════════════════════════
def _primary_image(p: dict) -> str:
    """The listing API gives a list of {imageType, format, url}; the page state a dict by type."""
    images = p.get("images") or []
    if isinstance(images, dict):
        images = [img for by_format in images.values() if isinstance(by_format, dict)
                  for img in by_format.values() if isinstance(img, dict)]
    primary = [img for img in images if isinstance(img, dict) and img.get("imageType") == "PRIMARY"]
    for img in primary or images:
        if isinstance(img, dict) and img.get("url"):
            return img["url"]
    return ""


def _listing_row(target: dict, rank: int, p: dict) -> dict:
    thumb = _primary_image(p)
    return {
        "scraped_date": _scraped_date(),
        "target": target["label"],
        "target_type": target["kind"],
        "target_code": target["code"],
        "list_rank": rank,
        "product_code": str(p.get("code", "")),
        "base_product": p.get("baseProduct", ""),
        "product_name": _clean_text(p.get("name")),
        "brand": (p.get("masterBrand") or {}).get("name", ""),
        "ean": str(p.get("ean") or ""),
        **_price(p),
        "rating": p.get("averageRating"),
        "review_count": p.get("numberOfReviews"),
        "stock_status": (p.get("stock") or {}).get("stockLevelStatus", ""),
        "category_path": _category_path(p.get("categoryNameHierarchy")),
        "promotions": _promo_text(p.get("promotions")),
        "variants_count": p.get("variantsNumber"),
        "is_new": bool(p.get("newIn")),
        "is_marketplace": bool(p.get("marketplaceProduct")),
        "image_url": _abs_url(thumb, MEDIA_HOST),
        "product_url": _abs_url(p.get("url", "")),
        "source": "Superdrug",
        "collected_at": _collected_at(),
    }


def _info_map(d: dict) -> Dict[str, str]:
    """extendedAttributes + productInformation as {key: plain text}."""
    info: Dict[str, str] = {}
    for entry in d.get("extendedAttributes") or []:
        info[entry.get("code", "")] = _clean_text(entry.get("value"))
    for entry in (d.get("productInformation") or {}).get("entry") or []:
        value = _clean_text(entry.get("value"))
        if value:
            info[entry.get("key", "")] = value
    return info


def _gallery(d: dict) -> List[str]:
    images = d.get("galleryImages") or d.get("images") or []
    formats = [img.get("format") for img in images]
    best = next((f for f in ("superZoom", "zoom", "product") if f in formats), None)
    urls = [_abs_url(img.get("url", ""), MEDIA_HOST) for img in images
            if img.get("url") and (best is None or img.get("format") == best)]
    return list(dict.fromkeys(urls))


def _detail_row(d: dict, product_url: str) -> dict:
    info = _info_map(d)
    hierarchy = (d.get("categoriesHierarchy") or [{}])[0].get("categories") or []
    images = _gallery(d)
    stock = d.get("stock") or {}
    return {
        "scraped_date": _scraped_date(),
        "product_code": str(d.get("code", "")),
        "base_product": d.get("baseProduct", ""),
        "product_name": _clean_text(d.get("name")),
        "brand": (d.get("masterBrand") or {}).get("name", ""),
        "ean": str(d.get("ean") or ""),
        **_price(d),
        "stock_status": stock.get("stockLevelStatus", ""),
        "stock_level": stock.get("stockLevel"),
        "rating": d.get("averageRating"),
        "review_count": d.get("numberOfReviews"),
        "category_path": " > ".join(c.get("name", "") for c in hierarchy if c.get("name"))
                         or _category_path(d.get("categoryNameHierarchy")),
        "product_category": (d.get("productCategory") or {}).get("name", ""),
        "short_description": _clean_text(d.get("shortDescription")),
        "description": _clean_text(d.get("description")),
        "key_features": info.get("otherFeatures", ""),
        "how_to_use": info.get("uses", ""),
        "ingredients": info.get("ingredients") or info.get("unbound.ingridients", ""),
        "warnings": info.get("warnings", ""),
        "storage": info.get("storage", ""),
        "pack_size": info.get("packSize", ""),
        "content_unit": info.get("section.product.ids.contentUnit", ""),
        "promotions": _promo_text(d.get("promotions")),
        "variants_count": d.get("variantsNumber"),
        "main_image": images[0] if images else "",
        "detail_images": json.dumps(images, ensure_ascii=False),
        "product_url": _abs_url(d.get("url", "")) or product_url,
        "source": "Superdrug",
        "collected_at": _collected_at(),
    }


def _review_row(review: dict, product_url: str, product_code: str, product_name: str,
                base_product: str = "") -> dict:
    details = review.get("details") or {}
    metrics = review.get("metrics") or {}
    badges = review.get("badges") or {}
    created = details.get("created_date")
    return {
        "product_url": product_url,
        "product_code": product_code,
        "product_name": product_name or details.get("product_name", ""),
        "base_product": base_product,
        "reviewed_product_name": details.get("product_name", ""),
        "review_id": str(review.get("review_id") or review.get("ugc_id") or ""),
        "rating_star": metrics.get("rating"),
        "headline": _clean_text(details.get("headline")),
        "comment": _clean_text(details.get("comments")),
        "user_name": details.get("nickname", ""),
        "location": details.get("location", ""),
        "comment_date": (datetime.fromtimestamp(created / 1000, timezone.utc).strftime("%Y-%m-%d")
                         if isinstance(created, (int, float)) else ""),
        "verified_buyer": bool(badges.get("is_verified_buyer")),
        "recommended": details.get("bottom_line", ""),
        "helpful_votes": metrics.get("helpful_votes"),
        "not_helpful_votes": metrics.get("not_helpful_votes"),
        "has_media": bool(review.get("media")),
        "variant_code": str(details.get("product_variant") or ""),
        "gtin": str(details.get("gtin") or ""),
        "source": "Superdrug",
        "collected_at": _collected_at(),
    }


def _targets_from_input(product_urls: Optional[List[str]],
                        df_product: Optional[pd.DataFrame]) -> Dict[str, dict]:
    """Merge URLs + a dataframe into {product_url: row-or-empty}, in first-seen order."""
    targets: Dict[str, dict] = {}
    if df_product is not None and not df_product.empty and "product_url" in df_product.columns:
        for row in df_product.to_dict(orient="records"):
            url = row.get("product_url")
            if isinstance(url, str) and _product_code(url) and url not in targets:
                targets[url] = row
    for url in product_urls or []:
        if isinstance(url, str) and _product_code(url):
            targets.setdefault(_abs_url(url.strip()), {})
        else:
            print(f"   ⚠️  Skipping URL with no /p/<code>: {url}")
    return targets


# ════════════════════════════════════════════════════════════════
# Public API: brand / category listings
# ════════════════════════════════════════════════════════════════
async def run_brand_products(
    targets: List[str],
    max_products_per_target: Optional[int] = DEFAULT_MAX_PRODUCTS_PER_TARGET,
    sort: str = "ranking",
    resume: Optional[bool] = None,
) -> pd.DataFrame:
    """
    Every product of each Superdrug brand or category.

    targets                 : brand/category URLs (".../cerave/b/403790",
                              ".../korean-japanese-beauty/c/k-beauty") or brand
                              names ("CeraVe", "L'Oreal Paris")
    max_products_per_target : None = all
    sort                    : one of LISTING_SORTS ("ranking" = the site default)
    resume                  : True = start from the checkpoint and skip targets in it

    Rows: target, product_code, base_product, product_name, brand, ean,
    price_gbp, original_price_gbp, discount_pct, unit_price, rating,
    review_count, stock_status, category_path, promotions, variants_count,
    is_new, is_marketplace, image_url, product_url, …
    """
    global _blocks
    _blocks = 0
    if sort not in LISTING_SORTS:
        print(f"⚠️  Unknown sort '{sort}' — using 'ranking'.")
        sort = "ranking"

    prefix = _CHECKPOINT_FILES["listing"]
    all_rows: List[dict] = _load_backup(prefix, enabled=resume)
    done_codes = {str(r.get("target_code")) for r in all_rows}

    print(f"\n🛍️  Superdrug — Brand / Category products  ({len(targets)} target(s))")
    try:
        context, page = await _create_browser_context()
        async with context:
            resolved = await _resolve_targets(page, targets)
            for i, target in enumerate(resolved, 1):
                if target["code"] in done_codes:
                    print(f"\n⏭️  [{i}/{len(resolved)}] '{target['label']}' — already collected, skip.")
                    continue
                print(f"\n🏷️  [{i}/{len(resolved)}] {target['kind']} '{target['label']}'  →  {target['url']}")
                await _goto_checked(page, target["url"])

                rows_t: List[dict] = []
                page_no, total_pages = 0, 1
                while page_no < total_pages:
                    url = (f"{API}/search?fields=PLP&categoryCode={quote(target['code'])}&{API_PARAMS}"
                           f"&currentPage={page_no}&pageSize={LISTING_PAGE_SIZE}"
                           + (f"&sort={sort}" if sort != "ranking" else ""))
                    data = await _fetch_json(page, url, what=f"listing_{target['code']}")
                    if not data:
                        print(f"   ⚠️  No listing data for page {page_no + 1} — stopping this target.")
                        break
                    pagination = data.get("pagination") or {}
                    total_pages = pagination.get("totalPages") or 0
                    products = data.get("products") or []
                    for p in products:
                        rows_t.append(_listing_row(target, len(rows_t) + 1, p))
                    print(f"   📄 Page {page_no + 1}/{max(total_pages, 1)}: {len(products)} product(s)  "
                          f"(total {len(rows_t)} of {pagination.get('totalResults', '?')})")
                    if not products:
                        break
                    if max_products_per_target and len(rows_t) >= max_products_per_target:
                        rows_t = rows_t[:max_products_per_target]
                        print(f"   ⏹️  Reached the limit of {max_products_per_target} product(s).")
                        break
                    page_no += 1
                    if page_no < total_pages:
                        await _human_delay(LISTING_DELAY_MIN, LISTING_DELAY_MAX)

                if not rows_t:
                    await _dump_debug(page, f"empty_{target['code']}")
                all_rows.extend(rows_t)
                done_codes.add(target["code"])
                _save_checkpoint(all_rows, prefix, LISTING_DEDUPE_COLS, "auto")
                if i < len(resolved):
                    await _human_delay(TARGET_PAUSE_MIN, TARGET_PAUSE_MAX)
    except (_RunStopped, _Blocked) as e:
        print(f"\n🛑 Stopped: {e}")
    except Exception as e:
        print(f"\n🛑 Stopped by unexpected error — saving what was collected: {_where(e)}")

    _save_checkpoint(all_rows, prefix, LISTING_DEDUPE_COLS, "final")
    return _save_final(all_rows, "superdrug_brand_list", LISTING_DEDUPE_COLS)


# ════════════════════════════════════════════════════════════════
# Public API: product details
# ════════════════════════════════════════════════════════════════
async def run_product_detail(
    product_urls: Optional[List[str]] = None,
    df_product: Optional[pd.DataFrame] = None,
    resume: Optional[bool] = None,
) -> pd.DataFrame:
    """
    Full detail for each product (ingredients, how to use, features,
    warnings, stock level, images, …) from the product API.

    product_urls / df_product : either or both (df needs a 'product_url' column)
    resume                    : True = start from the checkpoint and skip products in it
    """
    targets = _targets_from_input(product_urls, df_product)
    if not targets:
        print("❌ run_product_detail: no Superdrug product URLs provided.")
        return pd.DataFrame()

    global _blocks
    _blocks = 0
    prefix = _CHECKPOINT_FILES["detail"]
    all_rows: List[dict] = _load_backup(prefix, enabled=resume)
    done_urls = {r.get("product_url") for r in all_rows}
    todo = [u for u in targets if u not in done_urls]

    print(f"\n📝 Superdrug — Product Detail  ({len(todo)} to fetch, {len(targets) - len(todo)} already done)")
    try:
        context, page = await _create_browser_context()
        async with context:
            await _goto_checked(page, SITE + "/")
            for i, url in enumerate(todo, 1):
                code = _product_code(url)
                data = await _fetch_json(
                    page, f"{API}/products/{quote(code)}?fields=FULL&{API_PARAMS}", what=f"detail_{code}"
                )
                if not data:
                    print(f"   ⚠️  [{i}/{len(todo)}] No data for {code} (removed or not found).")
                else:
                    row = _detail_row(data, url)
                    row["product_url"] = url   # keep the caller's URL so merges on product_url line up
                    all_rows.append(row)
                    print(f"   ✅ [{i}/{len(todo)}] {row['product_name'][:70]}  £{row['price_gbp']}")
                if i % CHECKPOINT_EVERY == 0:
                    _save_checkpoint(all_rows, prefix, DETAIL_DEDUPE_COLS, "auto")
                if i < len(todo):
                    await _human_delay(DETAIL_DELAY_MIN, DETAIL_DELAY_MAX)
    except (_RunStopped, _Blocked) as e:
        print(f"\n🛑 Stopped: {e}")
    except Exception as e:
        print(f"\n🛑 Stopped by unexpected error — saving what was collected: {_where(e)}")

    _save_checkpoint(all_rows, prefix, DETAIL_DEDUPE_COLS, "final")
    return _save_final(all_rows, "superdrug_detail", DETAIL_DEDUPE_COLS)


# ════════════════════════════════════════════════════════════════
# Public API: reviews (PowerReviews)
# ════════════════════════════════════════════════════════════════
async def _discover_reviews_api(page, product_url: str) -> Optional[dict]:
    """Open a product page and read merchant id / locale / public key from the
    request its review widget makes. Sends nothing itself."""
    found: dict = {}

    def on_request(request):
        match = _PR_REVIEWS_RE.search(request.url)
        if match and not found:
            key = parse_qs(urlparse(request.url).query).get("apikey", [""])[0]
            if key:
                found.update(merchant_id=match.group(1), locale=match.group(2), apikey=key)

    page.on("request", on_request)
    try:
        await _goto_checked(page, product_url)
        for _ in range(REVIEW_WIDGET_SCROLLS):
            if found:
                break
            await page.evaluate("() => window.scrollBy(0, Math.round(window.innerHeight * 0.8))")
            await asyncio.sleep(1.5)
    finally:
        page.remove_listener("request", on_request)
    return found or None


async def _base_product(page, code: str) -> str:
    data = await _fetch_json(page, f"{API}/products/{quote(code)}?fields=FULL&{API_PARAMS}", what=f"detail_{code}")
    return (data or {}).get("baseProduct") or f"BP_{code}"


async def run_product_reviews(
    product_urls: Optional[List[str]] = None,
    df_product: Optional[pd.DataFrame] = None,
    max_reviews_per_product: Optional[int] = DEFAULT_MAX_REVIEWS_PER_PRODUCT,
    resume: Optional[bool] = None,
) -> pd.DataFrame:
    """
    Customer reviews (newest first) for each product.

    Shades / sizes of one product share a single review stream on Superdrug
    (PowerReviews page = base product), so it is fetched once per base product.
    Each review is filed under the input product the reviewer actually bought
    (variant_code) when that product is in the input, otherwise under the first
    input product of that base; reviewed_product_name keeps what was reviewed.

    product_urls / df_product : either or both. A df from run_brand_products /
                                run_product_detail supplies base_product,
                                product_name and review_count, which saves
                                requests and skips products with no reviews.
    max_reviews_per_product   : per base product; None = every review
    resume                    : True = start from the checkpoint and skip base products in it
    """
    global _blocks, _reviews_api
    targets = _targets_from_input(product_urls, df_product)
    if not targets:
        print("❌ run_product_reviews: no Superdrug product URLs provided.")
        return pd.DataFrame()

    _blocks = 0
    prefix = _CHECKPOINT_FILES["review"]
    all_rows: List[dict] = _load_backup(prefix, enabled=resume)
    done_bases = {r.get("base_product") for r in all_rows if r.get("base_product")}

    def _count(row: dict) -> Optional[int]:
        try:
            return int(float(row.get("review_count")))
        except (TypeError, ValueError):
            return None

    def _text(row: dict, key: str) -> str:
        value = row.get(key)
        return value if isinstance(value, str) else ""

    no_reviews = [u for u in targets if _count(targets[u]) == 0]
    todo = [u for u in targets if u not in no_reviews]
    cap = max_reviews_per_product or None

    print(f"\n💬 Superdrug — Reviews  ({len(todo)} product(s), {len(no_reviews)} skipped with 0 reviews, "
          f"up to {cap or 'all'} per base product)")
    if not todo:
        return _save_final(all_rows, "superdrug_reviews", REVIEW_DEDUPE_COLS)

    try:
        context, page = await _create_browser_context()
        async with context:
            if not _reviews_api:
                for url in todo[:3]:
                    print(f"   🔎 Reading the review widget's settings from {url}")
                    _reviews_api = await _discover_reviews_api(page, url)
                    if _reviews_api:
                        break
                if not _reviews_api:
                    await _dump_debug(page, "no_review_widget")
                    raise _RunStopped("The PowerReviews widget never loaded on the product page(s) tried.")
            else:
                await _goto_checked(page, todo[0])
            api = _reviews_api

            groups: Dict[str, List[str]] = {}
            for url in todo:
                base = _text(targets[url], "base_product") or await _base_product(page, _product_code(url))
                groups.setdefault(base, []).append(url)
            skipped = [b for b in groups if b in done_bases]
            groups = {b: urls for b, urls in groups.items() if b not in done_bases}
            if skipped:
                print(f"   ⏭️  {len(skipped)} base product(s) already in the checkpoint — skipped.")

            for i, (base, urls) in enumerate(groups.items(), 1):
                url_by_code = {_product_code(u): u for u in urls}
                first = urls[0]
                page_id = f"SD{base}"

                rows_p: List[dict] = []
                offset, total = 0, None
                while total is None or offset < total:
                    review_url = (
                        f"https://display.powerreviews.com/m/{api['merchant_id']}/l/{api['locale']}"
                        f"/product/{quote(page_id)}/reviews?apikey={api['apikey']}"
                        f"&paging.from={offset}&paging.size={REVIEW_PAGE_SIZE}&filters=&search="
                        f"&sort=Newest&image_only=false&page_locale={api['locale']}"
                    )
                    data = await _fetch_json(page, review_url, with_cookies=False, what=f"reviews_{base}")
                    if not data:
                        break
                    total = (data.get("paging") or {}).get("total_results") or 0
                    reviews = ((data.get("results") or [{}])[0] or {}).get("reviews") or []
                    for review in reviews:
                        variant = str((review.get("details") or {}).get("product_variant") or "")
                        url = url_by_code.get(variant, first)
                        rows_p.append(_review_row(review, url, _product_code(url),
                                                  _text(targets[url], "product_name"), base))
                    if not reviews:
                        break
                    offset += len(reviews)
                    if cap and len(rows_p) >= cap:
                        rows_p = rows_p[:cap]
                        break
                    await _human_delay(REVIEW_DELAY_MIN, REVIEW_DELAY_MAX)

                all_rows.extend(rows_p)
                shades = f"  (+{len(urls) - 1} more shade/size in the input share these)" if len(urls) > 1 else ""
                label = _text(targets[first], "product_name")[:60] or _product_code(first)
                print(f"   ✅ [{i}/{len(groups)}] {len(rows_p)} of {total if total is not None else '?'} "
                      f"review(s) — {label}{shades}")
                if i % CHECKPOINT_EVERY == 0:
                    _save_checkpoint(all_rows, prefix, REVIEW_DEDUPE_COLS, "auto")
                if i < len(groups):
                    await _human_delay(DETAIL_DELAY_MIN, DETAIL_DELAY_MAX)
    except (_RunStopped, _Blocked) as e:
        print(f"\n🛑 Stopped: {e}")
    except Exception as e:
        print(f"\n🛑 Stopped by unexpected error — saving what was collected: {_where(e)}")

    _save_checkpoint(all_rows, prefix, REVIEW_DEDUPE_COLS, "final")
    return _save_final(all_rows, "superdrug_reviews", REVIEW_DEDUPE_COLS)
