"""
cloak_tesco.py
────────────────────────────────────────────────────────────────
Tesco (www.tesco.com, UK groceries & marketplace) scraper — search /
category listings, product details and customer reviews.

Confirmed on the live site (2026-11-12):
  • Every search, category and product page is rendered on the server and
    ships its GraphQL (Apollo) cache inside
    <script type="application/discover+json"> → mfe-orchestrator.props.apolloCache.
    Data is read from that cache, not from the DOM:
      ROOT_QUERY.search({...}) / ROOT_QUERY.category({...}) → info (total,
      page, pageSize) + ordered results → ProductType:<id> / MPProduct:{…}
      (marketplace) → title, brandName, gtin, tpnb/tpnc, price {actual,
      unitPrice, unitOfMeasure}, promotions → PromotionType (Clubcard text),
      reviews.stats, department / aisle / shelf names, adId (= sponsored).
  • Listing URLs page with ?page=N&count=48 (search and /browse/… category
    pages alike). robots.txt only forbids &offset=, which is never used.
  • A product page's ProductType adds details (ingredients, packSize,
    netContents, preparationAndUsage, productMarketing, features, warnings,
    storage), all images, rating distribution and the first 10 reviews.
  • More reviews load 10 at a time when "Show 10 more reviews" is clicked,
    from xapi.tesco.com (GraphQL). The run clicks that button like a visitor
    and reads the responses the browser receives — it sends no API calls itself.
  • Akamai Bot Manager guards the site. A verification page pauses the run
    for a person; "Access Denied" backs off and stops if it keeps happening.
    Nothing tries to solve or get around either.

Public entry-points:
  run_search_keyword(keywords_or_urls)  →  pd.DataFrame  listing rows (keywords, search or /browse/ URLs)
  run_product_detail(product_urls)      →  pd.DataFrame  detail rows
  run_product_reviews(product_urls)     →  pd.DataFrame  review rows
  checkpoint_summary(kind)              →  what a checkpoint holds ("listing" / "detail" / "review")
"""

import asyncio
import json
import random
import re
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, quote, urlencode, urlparse, urlunparse

import pandas as pd
from cloakbrowser import launch_persistent_context_async

# ════════════════════════════════════════════════════════════════
# Directories & Config
# ════════════════════════════════════════════════════════════════

USER_DATA_DIR = "./data/tesco/browser_session"
OUTPUT_DIR    = "./data/tesco/collected_data"
TEMP_DIR      = "./data/tesco/collected_data/temp"
LOG_DIR       = "./data/tesco/log"

SITE        = "https://www.tesco.com"
SEARCH_URL  = f"{SITE}/shop/en-GB/search"
PRODUCT_URL = f"{SITE}/shop/en-GB/products/{{}}"

LISTING_PAGE_SIZE = 48                   # the site offers 24 / 48
DEFAULT_MAX_PAGES_PER_KEYWORD = 10       # 48 items per page; None = every page
DEFAULT_MAX_REVIEWS_PER_PRODUCT = 200    # None = every review (10 per click)
MAX_PAGES_HARD = 200

PAGE_DELAY_MIN,    PAGE_DELAY_MAX    = 6, 12     # seconds between listing pages
KEYWORD_PAUSE_MIN, KEYWORD_PAUSE_MAX = 20, 45    # seconds between keywords / URLs
DETAIL_DELAY_MIN,  DETAIL_DELAY_MAX  = 6, 12     # seconds between product pages
CLICK_DELAY_MIN,   CLICK_DELAY_MAX   = 2.5, 5    # seconds between "Show more reviews" clicks

PAGE_TIMEOUT_MS     = 60000
INTERSTITIAL_POLLS  = 10     # polls of 2 s for a page that hasn't shown its data yet
REVIEW_WAIT_POLLS   = 10     # polls of 1 s for reviews after a click
MAX_STALLED_CLICKS  = 2      # clicks that bring nothing new before giving up on a product

HUMAN_WAIT_TIMEOUT      = 900   # seconds to wait for a person at a verification page; None = no limit
HUMAN_WAIT_REMINDER     = 120
HUMAN_WAIT_COOLDOWN     = 20
MAX_HUMAN_WAITS_PER_RUN = 3
BLOCK_BACKOFF_MIN, BLOCK_BACKOFF_MAX = 90, 180
MAX_BLOCKS_PER_RUN      = 3

CHECKPOINT_EVERY = 10
RESUME_FROM_BACKUP = False

LISTING_DEDUPE_COLS = ["keyword_searched", "product_id"]
DETAIL_DEDUPE_COLS  = ["product_url"]
REVIEW_DEDUPE_COLS  = ["product_url", "review_id"]

_TEXT_COLS = {c: str for c in ("product_id", "tpnb", "base_product_id", "gtin", "review_id")}

_PRODUCT_RE = re.compile(r"/products/(\d+)")
_CLUBCARD_RE = re.compile(r"^\s*(?:£\s?(\d[\d,]*(?:\.\d{1,2})?)|(\d+)p)\b.*clubcard price", re.I)
_MORE_REVIEWS_RE = re.compile(r"^\s*Show \d+ more reviews?\s*$", re.I)
_HUMAN_CHECK_PHRASES = ("verify you are human", "verify you are a human", "are you a robot",
                        "press & hold", "press and hold")
# Tesco's Akamai block page: title "Error", "…your browser has failed some security checks."
_BLOCK_PHRASES = ("access denied", "failed some security checks", "something is not right")

for _folder in [OUTPUT_DIR, TEMP_DIR, LOG_DIR]:
    Path(_folder).mkdir(parents=True, exist_ok=True)

TZ_THAI = timezone(timedelta(hours=7))

_blocks = 0
_human_waits = 0

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


# ════════════════════════════════════════════════════════════════
# Page-data extraction (run inside the browser)
# ════════════════════════════════════════════════════════════════

_JS_PAGE_STATE = r"""() => ({
  data: [...document.scripts].some(s => s.type === 'application/discover+json'),
  title: document.title || '',
  text: document.body ? document.body.innerText.slice(0, 2000) : '',
})"""

_JS_APOLLO_CACHE = r"""() => {
  const s = [...document.scripts].find(s => s.type === 'application/discover+json');
  if (!s) return null;
  let root;
  try { root = JSON.parse(s.textContent); } catch (e) { return null; }
  const orch = root['mfe-orchestrator'];
  if (orch && orch.props && orch.props.apolloCache) return orch.props.apolloCache;
  for (const mfe of Object.values(root)) {
    if (mfe && mfe.props && mfe.props.apolloCache) return mfe.props.apolloCache;
  }
  return null;
}"""


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
    """'ValueError: … (cloak_tesco.py:123 in _listing_row)' for unexpected-error messages."""
    import traceback
    frames = traceback.extract_tb(e.__traceback__)
    last = frames[-1] if frames else None
    at = f" ({Path(last.filename).name}:{last.lineno} in {last.name})" if last else ""
    return f"{type(e).__name__}: {e}{at}"


def _join(value, sep: str = "\n") -> str:
    """Tesco text fields are strings, lists of strings, or null."""
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        parts = [_join(v, sep) for v in value]
        return sep.join(p for p in parts if p)
    if isinstance(value, dict):
        return _join([v for k, v in value.items() if k != "__typename"], sep)
    return re.sub(r"[ \t\xa0]+", " ", str(value)).strip()


def _product_id(url: str) -> Optional[str]:
    match = _PRODUCT_RE.search(str(url or ""))
    return match.group(1) if match else None


def _clubcard_price(promotions: List[dict]) -> Optional[float]:
    """'£11.30 Clubcard Price' → 11.30, '75p Clubcard Price' → 0.75; multi-buy offers → None."""
    for promo in promotions:
        match = _CLUBCARD_RE.match(promo.get("description") or "")
        if match:
            return float(match.group(1).replace(",", "")) if match.group(1) else int(match.group(2)) / 100
    return None


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
    "listing": "temp_tesco_listing",
    "detail": "temp_tesco_detail",
    "review": "temp_tesco_review",
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
        "keywords": list(dict.fromkeys(df["keyword_searched"].fillna(""))) if "keyword_searched" in df.columns else [],
        "last_collected": df["collected_at"].max() if "collected_at" in df.columns else "",
    }


# ════════════════════════════════════════════════════════════════
# Apollo cache helpers
# ════════════════════════════════════════════════════════════════
def _resolve(cache: dict, obj):
    """Follow an Apollo {"__ref": key} (the inline fields win over the stored ones)."""
    if isinstance(obj, dict) and "__ref" in obj:
        stored = cache.get(obj["__ref"]) or {}
        return {**stored, **{k: v for k, v in obj.items() if k != "__ref"}}
    return obj


def _field(obj: dict, name: str):
    """Apollo stores fields with arguments as 'name({...})' — take the first match."""
    if name in obj:
        return obj[name]
    for key, value in obj.items():
        if key.startswith(name + "("):
            return value
    return None


def _promotions(cache: dict, product: dict) -> List[dict]:
    promos = []
    for promo in product.get("promotions") or []:
        promo = _resolve(cache, promo)
        if isinstance(promo, dict) and promo.get("description"):
            promos.append(promo)
    return promos


def _listing_results(cache: dict) -> Tuple[dict, List[dict]]:
    """The page's main product list (search or category) → (info, [product, …])."""
    best_info, best_items = {}, []
    for key, value in (cache.get("ROOT_QUERY") or {}).items():
        if not isinstance(value, dict) or not isinstance(value.get("results"), list):
            continue
        if '"mode":"EXPLORE"' in key:   # the 5-item suggestion strip
            continue
        items = []
        for result in value["results"]:
            node = _resolve(cache, (result or {}).get("node") or result)
            if isinstance(node, dict) and node.get("__typename") in ("ProductType", "MPProduct") and node.get("id"):
                items.append(node)
        if len(items) > len(best_items):
            best_info, best_items = value.get("info") or {}, items
    return best_info, best_items


def _product_entry(cache: dict, product_id: str) -> Optional[dict]:
    """The richest cache entry for one product id."""
    candidates = [v for v in cache.values() if isinstance(v, dict)
                  and v.get("__typename") in ("ProductType", "MPProduct") and str(v.get("id")) == product_id]
    return max(candidates, key=len) if candidates else None


def _review_stats(product: dict) -> dict:
    reviews = _field(product, "reviews") or {}
    return reviews.get("stats") or {}


def _seller_name(cache: dict, product: dict) -> str:
    seller = _resolve(cache, product.get("seller"))
    if isinstance(seller, dict):
        return seller.get("name") or seller.get("displayName") or ""
    return ""


# ════════════════════════════════════════════════════════════════
# Rows
# ════════════════════════════════════════════════════════════════
def _common_fields(cache: dict, p: dict) -> dict:
    price = p.get("price") or {}
    promos = _promotions(cache, p)
    stats = _review_stats(p)
    return {
        "product_id": str(p.get("id", "")),
        "tpnb": str(p.get("tpnb") or ""),
        "gtin": str(p.get("gtin") or ""),
        "product_name": _join(p.get("title")),
        "brand": p.get("brandName") or "",
        "price_gbp": price.get("actual"),
        "unit_price_gbp": price.get("unitPrice"),
        "unit_of_measure": price.get("unitOfMeasure") or "",
        "clubcard_price_gbp": _clubcard_price(promos),
        "promotions": " | ".join(dict.fromkeys(_join(pr.get("description")) for pr in promos)),
        "promotion_end": max((str(pr.get("endDate"))[:10] for pr in promos if pr.get("endDate")), default=""),
        "rating": stats.get("overallRating"),
        "review_count": stats.get("noOfReviews"),
        "super_department": p.get("superDepartmentName") or "",
        "department": p.get("departmentName") or "",
        "aisle": p.get("aisleName") or "",
        "shelf": p.get("shelfName") or "",
        "status": p.get("status") or "",
        "is_for_sale": bool(p.get("isForSale")),
        "is_marketplace": p.get("__typename") == "MPProduct",
        "seller_name": _seller_name(cache, p),
        "is_new": bool(p.get("isNew")),
    }


def _listing_row(cache: dict, label: str, page_no: int, rank: int, p: dict) -> dict:
    image = ((p.get("media") or {}).get("defaultImage") or {}).get("url") or p.get("defaultImageUrl") or ""
    return {
        "scraped_date": _scraped_date(),
        "keyword_searched": label,
        "page": page_no,
        "search_rank": rank,
        **_common_fields(cache, p),
        "is_sponsored": bool(p.get("adId")),
        "image_url": image.split("?")[0],
        "product_url": PRODUCT_URL.format(p.get("id")),
        "source": "Tesco",
        "collected_at": _collected_at(),
    }


def _detail_row(cache: dict, p: dict, product_url: str) -> dict:
    details = p.get("details") or {}
    stats = _review_stats(p)
    distribution = {
        str(item.get("name", "")).replace(".0", ""): int(float(item.get("value") or 0))
        for item in stats.get("ratingsDistribution") or [] if isinstance(item, dict)
    }
    images = [img.get("url") for img in (p.get("media") or {}).get("images") or [] if isinstance(img, dict)]
    images = [u for u in dict.fromkeys(images) if u]
    pack = [f"{ps.get('value', '')} {ps.get('units', '')}".strip()
            for ps in details.get("packSize") or [] if isinstance(ps, dict)]
    return {
        "scraped_date": _scraped_date(),
        **_common_fields(cache, p),
        "base_product_id": str(p.get("baseProductId") or ""),
        "rating_distribution": json.dumps(distribution, ensure_ascii=False) if distribution else "",
        "description": _join(p.get("description")),
        "product_marketing": _join(details.get("productMarketing")),
        "brand_marketing": _join(details.get("brandMarketing")),
        "features": _join(details.get("features")),
        "ingredients": _join(details.get("ingredients"), ", "),
        "allergens": _join(details.get("allergens"), ", "),
        "how_to_use": _join([details.get("preparationAndUsage"), details.get("directions"),
                             details.get("dosage")]),
        "warnings": _join([details.get("warnings"), details.get("safetyWarning"), details.get("hazardInfo")]),
        "storage": _join(details.get("storage")),
        "net_contents": _join(details.get("netContents")),
        "pack_size": ", ".join(pack),
        "manufacturer": _join(p.get("manufacturer"), ", "),
        "main_image": images[0] if images else "",
        "detail_images": json.dumps(images, ensure_ascii=False),
        "product_url": product_url,
        "source": "Tesco",
        "collected_at": _collected_at(),
    }


def _review_row(review: dict, product_url: str, product_id: str, product_name: str) -> dict:
    rating = review.get("rating")
    author = review.get("author") or {}
    synd = review.get("syndicationSource") or {}
    return {
        "product_url": product_url,
        "product_id": product_id,
        "product_name": product_name,
        "review_id": str(review.get("reviewId") or review.get("id") or ""),
        "rating_star": rating.get("value") if isinstance(rating, dict) else rating,
        "headline": _join(review.get("summary")),
        "comment": _join(review.get("text")),
        "user_name": author.get("nickname") or "" if isinstance(author, dict) else "",
        "comment_date": str(review.get("submissionDateTime") or "")[:10],
        "verified_buyer": bool(review.get("verifiedBuyer")),
        "syndicated": bool(review.get("syndicated")),
        "syndication_source": synd.get("name") or "" if isinstance(synd, dict) else "",
        "promotional_review": bool(review.get("promotionalReview")),
        "sampled_review": bool(review.get("sampledReview")),
        "source": "Tesco",
        "collected_at": _collected_at(),
    }


def _find_reviews(payload) -> List[dict]:
    """Every object that looks like a review anywhere in a GraphQL response."""
    found, stack = [], [payload]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            if node.get("reviewId") and ("text" in node or "rating" in node):
                found.append(node)
                continue
            stack.extend(node.values())
        elif isinstance(node, list):
            stack.extend(node)
    return found


def _targets_from_input(product_urls: Optional[List[str]],
                        df_product: Optional[pd.DataFrame]) -> Dict[str, dict]:
    """Merge URLs + a dataframe into {product_url: row-or-empty}, in first-seen order."""
    targets: Dict[str, dict] = {}
    if df_product is not None and not df_product.empty and "product_url" in df_product.columns:
        for row in df_product.to_dict(orient="records"):
            pid = _product_id(row.get("product_url"))
            if pid:
                targets.setdefault(PRODUCT_URL.format(pid), row)
    for url in product_urls or []:
        pid = _product_id(url)
        if pid:
            targets.setdefault(PRODUCT_URL.format(pid), {})
        else:
            print(f"   ⚠️  Skipping URL with no /products/<id>: {url}")
    return targets


def _listing_url(entry: str, page_no: int) -> Tuple[str, str]:
    """(label, url) for a keyword or a pasted search / category URL at page N."""
    entry = entry.strip()
    if entry.startswith("http"):
        parts = urlparse(entry)
        query = {k: v[0] for k, v in parse_qs(parts.query).items() if k not in ("page", "count", "offset")}
        query.update(page=str(page_no), count=str(LISTING_PAGE_SIZE))
        label = query.get("query") or [s for s in parts.path.split("/") if s and s != "all"][-1]
        return label, urlunparse(parts._replace(query=urlencode(query)))
    return entry, f"{SEARCH_URL}?query={quote(entry)}&page={page_no}&count={LISTING_PAGE_SIZE}"


# ════════════════════════════════════════════════════════════════
# Browser session & page loading
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


async def _evaluate(page, script: str):
    """page.evaluate that survives one in-page navigation during the call."""
    try:
        return await page.evaluate(script)
    except Exception as e:
        if "context was destroyed" not in str(e).lower():
            raise
        await asyncio.sleep(2)
        return await page.evaluate(script)


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
    """Reject non-essential cookies on the consent banner if it is showing."""
    for selector in ("#onetrust-reject-all-handler", "button:has-text('Reject all')",
                     "button:has-text('Reject All')"):
        try:
            button = page.locator(selector).first
            if await button.count() and await button.is_visible(timeout=2000):
                await button.click()
                await asyncio.sleep(0.8)
                return
        except Exception:
            continue


async def _page_state(page) -> str:
    """'ok', 'blocked', 'human_check' or 'no_data'."""
    state = await _evaluate(page, _JS_PAGE_STATE)
    if state["data"]:
        return "ok"
    text = f"{state['title']} {state['text']}".lower()
    if any(phrase in text for phrase in _BLOCK_PHRASES):
        return "blocked"
    if any(phrase in text for phrase in _HUMAN_CHECK_PHRASES):
        return "human_check"
    return "no_data"


async def _wait_for_human(page) -> None:
    """Pause until a person clears the verification page in the browser window.
    Nothing here attempts to solve or get around it."""
    global _human_waits
    _human_waits += 1
    if _human_waits > MAX_HUMAN_WAITS_PER_RUN:
        raise _RunStopped(f"Tesco asked for verification more than {MAX_HUMAN_WAITS_PER_RUN} times "
                          "in this run. Stopping — try again later.")
    await _dump_debug(page, "human_check")
    how_long = f"up to {HUMAN_WAIT_TIMEOUT / 60:.0f} minutes" if HUMAN_WAIT_TIMEOUT else "until you're done"
    print(f"\n⏸️  Tesco is showing a verification page — please complete it in the browser window. "
          f"Waiting {how_long} (Ctrl+C to stop)…")
    loop = asyncio.get_running_loop()
    started = loop.time()
    next_reminder = started + HUMAN_WAIT_REMINDER
    while HUMAN_WAIT_TIMEOUT is None or loop.time() - started < HUMAN_WAIT_TIMEOUT:
        await asyncio.sleep(5)
        if await _page_state(page) == "ok":
            print(f"▶️  Cleared. Resuming after {HUMAN_WAIT_COOLDOWN}s…")
            await asyncio.sleep(HUMAN_WAIT_COOLDOWN)
            return
        if loop.time() >= next_reminder:
            print(f"   ⏳ Still waiting ({(loop.time() - started) / 60:.0f} min so far)…")
            next_reminder += HUMAN_WAIT_REMINDER
    raise _RunStopped("Nobody cleared the verification page in time.")


async def _back_off(page, state: str) -> None:
    global _blocks
    _blocks += 1
    await _dump_debug(page, state)
    if _blocks > MAX_BLOCKS_PER_RUN:
        raise _RunStopped(f"Tesco refused pages {_blocks} times in this run ({state}). Stopping — "
                          "wait a few hours, then run again and continue from the checkpoint.")
    pause = random.uniform(BLOCK_BACKOFF_MIN, BLOCK_BACKOFF_MAX)
    print(f"   🚧 Page refused ({state}) — {_blocks}/{MAX_BLOCKS_PER_RUN}. Backing off {pause:.0f}s…")
    await asyncio.sleep(pause)


async def _goto_checked(page, url: str) -> None:
    """Open a page and make sure it carries its data; pauses / backs off otherwise."""
    for _ in range(MAX_BLOCKS_PER_RUN + MAX_HUMAN_WAITS_PER_RUN + 1):
        await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
        await asyncio.sleep(random.uniform(2.5, 4.5))
        state = await _page_state(page)
        for _ in range(INTERSTITIAL_POLLS):   # a bot-check interstitial may still be resolving
            if state != "no_data":
                break
            await asyncio.sleep(2)
            state = await _page_state(page)
        if state == "ok":
            await _dismiss_cookies(page)
            return
        if state == "human_check":
            await _wait_for_human(page)
            if await _page_state(page) == "ok":
                return
            continue
        await _back_off(page, state)
    raise _RunStopped(f"Could not load {url}")


async def _read_cache(page) -> dict:
    return await _evaluate(page, _JS_APOLLO_CACHE) or {}


class _ReviewCapture:
    """Collects reviews from the GraphQL responses the page receives after a
    'Show more reviews' click. Passive: it reads responses, sends nothing."""

    def __init__(self):
        self.reviews: Dict[str, dict] = {}
        self.responses = 0
        self._tasks: set = set()

    def attach(self, page) -> None:
        self._handler = lambda response: self._spawn(self._read(response))
        page.on("response", self._handler)

    def detach(self, page) -> None:
        page.remove_listener("response", self._handler)

    def _spawn(self, coroutine) -> None:
        task = asyncio.ensure_future(coroutine)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def drain(self) -> None:
        if self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)

    async def _read(self, response) -> None:
        if "xapi.tesco.com" not in response.url or response.status != 200:
            return
        try:
            payload = await response.json()
        except Exception:
            return
        self.responses += 1
        for review in _find_reviews(payload):
            self.reviews.setdefault(str(review["reviewId"]), review)


# ════════════════════════════════════════════════════════════════
# Public API: search / category listings
# ════════════════════════════════════════════════════════════════
async def run_search_keyword(
    keywords: List[str],
    max_pages_per_keyword: Optional[int] = DEFAULT_MAX_PAGES_PER_KEYWORD,
    resume: Optional[bool] = None,
) -> pd.DataFrame:
    """
    Search each keyword (or open each pasted search / /browse/ category URL)
    and page through the results, 48 per page.

    keywords              : ["cerave", "nivea creme", "https://www.tesco.com/shop/en-GB/browse/…/all"]
    max_pages_per_keyword : None = every page
    resume                : True = start from the checkpoint and skip keywords already in it

    Rows: product_id (= tpnc), tpnb, gtin, product_name, brand, price_gbp,
    unit_price_gbp, unit_of_measure, clubcard_price_gbp, promotions,
    promotion_end, rating, review_count, department / aisle / shelf,
    status, is_marketplace, is_sponsored, product_url, plus keyword/page/rank.
    """
    global _blocks, _human_waits
    _blocks = _human_waits = 0
    page_limit = min(max_pages_per_keyword or MAX_PAGES_HARD, MAX_PAGES_HARD)
    prefix = _CHECKPOINT_FILES["listing"]
    all_rows: List[dict] = _load_backup(prefix, enabled=resume)
    done = {str(r.get("keyword_searched")) for r in all_rows}

    print(f"\n🔍 Tesco — Search / Category  ({len(keywords)} keyword(s)/URL(s), up to {page_limit} page(s) each)")
    try:
        context, page = await _create_browser_context()
        async with context:
            for i, entry in enumerate(keywords, 1):
                label, url = _listing_url(entry, 1)
                if label in done:
                    print(f"\n⏭️  [{i}/{len(keywords)}] '{label}' — already collected, skip.")
                    continue
                print(f"\n🔑 [{i}/{len(keywords)}] '{label}'  →  {url}")

                seen: set = set()
                rows_kw: List[dict] = []
                page_no, total_pages = 1, 1
                while page_no <= min(total_pages, page_limit):
                    _, url = _listing_url(entry, page_no)
                    await _goto_checked(page, url)
                    cache = await _read_cache(page)
                    info, items = _listing_results(cache)
                    if page_no == 1:
                        size = info.get("pageSize") or LISTING_PAGE_SIZE
                        total_pages = max(1, -(-int(info.get("total") or 0) // size))
                        print(f"   📊 {info.get('total', 0):,} result(s) → {total_pages} page(s)")
                    new = [p for p in items if str(p["id"]) not in seen]
                    for p in new:
                        seen.add(str(p["id"]))
                        rows_kw.append(_listing_row(cache, label, page_no, len(rows_kw) + 1, p))
                    print(f"   📄 Page {page_no}/{total_pages}: {len(new)} new item(s)  (total {len(rows_kw)})")
                    if not new:
                        if page_no == 1:
                            await _dump_debug(page, f"empty_{label}")
                        break
                    page_no += 1
                    if page_no <= min(total_pages, page_limit):
                        await _human_delay(PAGE_DELAY_MIN, PAGE_DELAY_MAX)

                all_rows.extend(rows_kw)
                done.add(label)
                _save_checkpoint(all_rows, prefix, LISTING_DEDUPE_COLS, "auto")
                if i < len(keywords):
                    pause = random.uniform(KEYWORD_PAUSE_MIN, KEYWORD_PAUSE_MAX)
                    print(f"   ⏳ Waiting {pause:.0f}s before the next keyword…")
                    await asyncio.sleep(pause)
    except _RunStopped as e:
        print(f"\n🛑 Stopped: {e}")
    except Exception as e:
        print(f"\n🛑 Stopped by unexpected error — saving what was collected: {_where(e)}")

    _save_checkpoint(all_rows, prefix, LISTING_DEDUPE_COLS, "final")
    return _save_final(all_rows, "tesco_search_list", LISTING_DEDUPE_COLS)


# ════════════════════════════════════════════════════════════════
# Public API: product details
# ════════════════════════════════════════════════════════════════
async def run_product_detail(
    product_urls: Optional[List[str]] = None,
    df_product: Optional[pd.DataFrame] = None,
    resume: Optional[bool] = None,
) -> pd.DataFrame:
    """
    Full detail for each product page: ingredients, pack size, net contents,
    how to use, marketing copy, features, warnings, images, rating
    distribution — plus the listing fields (price, Clubcard price, …).

    product_urls / df_product : either or both (df needs a 'product_url' column)
    resume                    : True = start from the checkpoint and skip products in it
    """
    targets = _targets_from_input(product_urls, df_product)
    if not targets:
        print("❌ run_product_detail: no Tesco product URLs provided.")
        return pd.DataFrame()

    global _blocks, _human_waits
    _blocks = _human_waits = 0
    prefix = _CHECKPOINT_FILES["detail"]
    all_rows: List[dict] = _load_backup(prefix, enabled=resume)
    done_urls = {r.get("product_url") for r in all_rows}
    todo = [u for u in targets if u not in done_urls]

    print(f"\n📝 Tesco — Product Detail  ({len(todo)} to fetch, {len(targets) - len(todo)} already done)")
    try:
        context, page = await _create_browser_context()
        async with context:
            for i, url in enumerate(todo, 1):
                await _goto_checked(page, url)
                cache = await _read_cache(page)
                product = _product_entry(cache, _product_id(url))
                if not product:
                    print(f"   ⚠️  [{i}/{len(todo)}] No product data on {url} (removed or not found).")
                else:
                    row = _detail_row(cache, product, url)
                    all_rows.append(row)
                    print(f"   ✅ [{i}/{len(todo)}] {row['product_name'][:70]}  £{row['price_gbp']}")
                if i % CHECKPOINT_EVERY == 0:
                    _save_checkpoint(all_rows, prefix, DETAIL_DEDUPE_COLS, "auto")
                if i < len(todo):
                    await _human_delay(DETAIL_DELAY_MIN, DETAIL_DELAY_MAX)
    except _RunStopped as e:
        print(f"\n🛑 Stopped: {e}")
    except Exception as e:
        print(f"\n🛑 Stopped by unexpected error — saving what was collected: {_where(e)}")

    _save_checkpoint(all_rows, prefix, DETAIL_DEDUPE_COLS, "final")
    return _save_final(all_rows, "tesco_detail", DETAIL_DEDUPE_COLS)


# ════════════════════════════════════════════════════════════════
# Public API: reviews
# ════════════════════════════════════════════════════════════════
REVIEW_SECTION_SCROLLS = 15   # wheel steps looking for the review section (it renders on scroll)


async def _click_more_reviews(page) -> bool:
    # The text sits in a <span> inside an <a class="…textButton">; the link is what takes the click.
    button = page.locator("a, button").filter(has_text=_MORE_REVIEWS_RE).first
    try:
        for _ in range(REVIEW_SECTION_SCROLLS):
            if await button.count():
                break
            await page.mouse.wheel(0, random.randint(500, 800))
            await asyncio.sleep(random.uniform(0.6, 1.2))
        if not await button.count():
            return False
        await button.scroll_into_view_if_needed(timeout=5000)
        await button.click(timeout=8000)
        return True
    except Exception as e:
        print(f"   ⚠️  Could not click 'Show more reviews': {str(e).splitlines()[0][:160]}")
        return False


async def run_product_reviews(
    product_urls: Optional[List[str]] = None,
    df_product: Optional[pd.DataFrame] = None,
    max_reviews_per_product: Optional[int] = DEFAULT_MAX_REVIEWS_PER_PRODUCT,
    resume: Optional[bool] = None,
) -> pd.DataFrame:
    """
    Customer reviews for each product, in the order the product page shows
    them. The first 10 come with the page; more are loaded by clicking
    "Show 10 more reviews" (10 per click, paced like a visitor).

    Some brands syndicate reviews from their own site (syndicated=True,
    syndication_source) — those can be shared by every size of a product.

    product_urls / df_product : either or both. A df with review_count skips
                                products that have none.
    max_reviews_per_product   : None = every review
    resume                    : True = start from the checkpoint and skip products in it
    """
    targets = _targets_from_input(product_urls, df_product)
    if not targets:
        print("❌ run_product_reviews: no Tesco product URLs provided.")
        return pd.DataFrame()

    global _blocks, _human_waits
    _blocks = _human_waits = 0
    prefix = _CHECKPOINT_FILES["review"]
    all_rows: List[dict] = _load_backup(prefix, enabled=resume)
    done_urls = {r.get("product_url") for r in all_rows}

    def _count(row: dict) -> Optional[int]:
        """Tesco leaves the review stats empty (null) for products without reviews,
        so an empty review_count from a listing/detail df means 0. Unknown → None."""
        if "review_count" not in row:
            return None
        try:
            value = float(row.get("review_count"))
        except (TypeError, ValueError):
            return 0
        return 0 if value != value else int(value)   # NaN → 0

    no_reviews = [u for u in targets if _count(targets[u]) == 0]
    todo = [u for u in targets if u not in done_urls and u not in no_reviews]
    cap = max_reviews_per_product or None
    print(f"\n💬 Tesco — Reviews  ({len(todo)} product(s), {len(no_reviews)} skipped with 0 reviews, "
          f"up to {cap or 'all'} each)")
    if not todo:
        return _save_final(all_rows, "tesco_reviews", REVIEW_DEDUPE_COLS)

    try:
        context, page = await _create_browser_context()
        async with context:
            capture = _ReviewCapture()
            capture.attach(page)
            for i, url in enumerate(todo, 1):
                pid = _product_id(url)
                capture.reviews.clear()
                await _goto_checked(page, url)
                cache = await _read_cache(page)
                product = _product_entry(cache, pid) or {}
                name = _join(product.get("title")) or (targets[url].get("product_name") or "")
                reviews_field = _field(product, "reviews") or {}
                total = (reviews_field.get("info") or {}).get("total") or _review_stats(product).get("noOfReviews") or 0

                collected: Dict[str, dict] = {}
                for entry in reviews_field.get("entries") or []:
                    review = _resolve(cache, entry)
                    if isinstance(review, dict) and review.get("reviewId"):
                        collected.setdefault(str(review["reviewId"]), review)

                stalled = 0
                while (not cap or len(collected) < cap) and len(collected) < total and stalled < MAX_STALLED_CLICKS:
                    if not await _click_more_reviews(page):
                        break
                    before = len(collected)
                    for _ in range(REVIEW_WAIT_POLLS):
                        await asyncio.sleep(1)
                        await capture.drain()
                        for rid, review in capture.reviews.items():
                            collected.setdefault(rid, review)
                        if len(collected) > before:
                            break
                    stalled = stalled + 1 if len(collected) == before else 0
                    if stalled == 1 and capture.responses == 0:
                        print("   ⚠️  The click brought no GraphQL response — check the debug dump.")
                        await _dump_debug(page, f"no_more_reviews_{pid}")
                    await _human_delay(CLICK_DELAY_MIN, CLICK_DELAY_MAX)

                reviews = list(collected.values())[:cap] if cap else list(collected.values())
                all_rows.extend(_review_row(r, url, pid, name) for r in reviews)
                print(f"   ✅ [{i}/{len(todo)}] {len(reviews)} of {total} review(s) — {name[:60] or pid}")
                if i % CHECKPOINT_EVERY == 0:
                    _save_checkpoint(all_rows, prefix, REVIEW_DEDUPE_COLS, "auto")
                if i < len(todo):
                    await _human_delay(DETAIL_DELAY_MIN, DETAIL_DELAY_MAX)
            capture.detach(page)
    except _RunStopped as e:
        print(f"\n🛑 Stopped: {e}")
    except Exception as e:
        print(f"\n🛑 Stopped by unexpected error — saving what was collected: {_where(e)}")

    _save_checkpoint(all_rows, prefix, REVIEW_DEDUPE_COLS, "final")
    return _save_final(all_rows, "tesco_reviews", REVIEW_DEDUPE_COLS)
