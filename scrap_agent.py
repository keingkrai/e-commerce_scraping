"""
scrap_agent.py — E-Commerce Scraper Agent
══════════════════════════════════════════════════════════════════════

Auto-loads shop / brand URLs from a central memory JSON file
(D:\\workspace\\memory\\test_financial.json) and runs the correct
scraping pipeline for each platform.

Supported platforms  (from the JSON memory file):
  • Shopee   → SHOPEE_LINK   : list of shop URLs      → action 2 (+3)
  • Lazada   → LAZADA_LINK   : list of shop URLs      → action 2 (+3)
  • Watsons  → WATSONS_LINK  : list of brand URLs     → action 2 (+3)
                                  (put "ALL" in the list = every brand on Watsons'
                                  brand list; in manual action 2 pick option 2)
  • Konvy    → KONVY_LINK    : list of brand/keywords → action 1 (full detail in one pass)
                                  (put "ALL" in the list = every brand on Konvy's brand
                                  index; in manual action 1 pick option 2)
  • Pinduoduo → PINDUODUO_LINK : list of keywords     → action 1 (listing only;
                                  needs a logged-in session — run action 0 first;
                                  whole categories via action 6 and whole shops
                                  via action 2 in manual mode)
  • Taobao    → TAOBAO_LINK    : list of keywords     → action 1 (search data incl.
                                  sales; needs a logged-in session — action 0 first).
                                  Product details = action 3 in manual mode, best
                                  sellers only, ~40 pages/day (spans several days)
  • Superdrug → SUPERDRUG_LINK : brand/category URLs or brand names → action 2 (+3)
                                  (UK; listing already has price/EAN/rating/stock —
                                  action 3 adds ingredients etc., action 4 reviews)
  • Tesco     → TESCO_LINK     : keywords or search/category URLs → action 1 (+3)
                                  (UK; Akamai-protected — paced slowly, stops if blocked)
  • Amazon UK → AMAZON_UK_LINK : keywords or search URLs → action 1 (+3)
                                  (run action 0 once first to set a UK delivery postcode;
                                  reviews = only those shown on the product page;
                                  a whole category's top products via action 6)

STEPS IN THE PIPELINE
──────────────────────
For Shopee / Lazada:
  Action 2  — Collect Shop Products   (all products from a shop URL)
  Action 3  — Collect Product Details (specs + description per URL)

For Watsons:
  Action 2  — Collect Brand Products  (all products from brand URL,
               includes product details & description in one pass)

For Konvy:
  Action 1  — Search by Brand/Keyword (full product detail in one pass;
               reviews need a separate action 4 pass using product_id)

After all platforms are scraped the collected files are ready to be
passed into product_agent.py (options 1-5) to clean names, identify
product info, and build the Omnichannel Master.

USAGE
──────
python scrap_agent.py
  → interactive menu; choose Auto (A) to load from JSON, or Manual (M)
    to enter URLs at the keyboard as before.
"""

import asyncio
import json
import re
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

# ── Platform scrapers ─────────────────────────────────────────────────────────
from scrapers import cloak_shopee_AI_bot as shopee
from scrapers import cloak_lazada         as lazada
from scrapers import cloak_watsons        as watsons
from scrapers import cloak_konvy          as konvy
from scrapers import cloak_pinduoduo      as pinduoduo
from scrapers import cloak_taobao         as taobao
from scrapers import cloak_superdrug      as superdrug
from scrapers import cloak_tesco          as tesco
from scrapers import cloak_amazon_uk      as amazon_uk

# ════════════════════════════════════════════════════════════════════════════════
# Config
# ════════════════════════════════════════════════════════════════════════════════

MEMORY_FILE = Path(r"D:\workspace\memory\test_financial.json")

COMBINED_OUTPUT_DIR = "./data/combined_output"
Path(COMBINED_OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

TZ_THAI = timezone(timedelta(hours=7))

# Keys inside the JSON memory file that hold shop / brand URLs
PLATFORM_URL_KEYS: Dict[str, str] = {
    "shopee":  "SHOPEE_LINK",
    "lazada":  "LAZADA_LINK",
    "watsons": "WATSONS_LINK",
    "konvy":   "KONVY_LINK",
    "pinduoduo": "PINDUODUO_LINK",
    "taobao":  "TAOBAO_LINK",
    "superdrug": "SUPERDRUG_LINK",
    "tesco":   "TESCO_LINK",
    "amazon_uk": "AMAZON_UK_LINK",
}

# ════════════════════════════════════════════════════════════════════════════════
# Step catalogue  (mirrors original scrap_agent.py)
# ════════════════════════════════════════════════════════════════════════════════

STEPS = {
    0: {
        "name": "Create / Refresh Session",
        "desc": (
            "Login to platform and save browser session\n"
            "      Amazon UK: no login — sets a UK delivery postcode (UK prices/stock)"
        ),
        "emoji": "🔐",
        "platforms": ["shopee", "lazada", "pinduoduo", "taobao", "amazon_uk"],
    },
    1: {
        "name": "Search by Keyword",
        "desc": (
            "Scrape product listings from search results\n"
            "      Konvy: keyword = brand/title text; full detail\n"
            "             (brand/price/product_id) included in one pass;\n"
            "             option 2 = EVERY brand on the brand index (resumable)\n"
            "      Taobao: search results incl. sales (X人付款) — details via action 3\n"
            "      Tesco: keywords, or pasted search / category (/browse/…) URLs —\n"
            "             price, Clubcard price, GTIN, rating, aisle (48 per page)\n"
            "      Amazon UK: keywords or search URLs — price, rating, 'bought in past\n"
            "             month', sponsored (keep pages low: default 3)"
        ),
        "emoji": "🔍",
        "platforms": ["shopee", "lazada", "watsons", "konvy", "pinduoduo", "taobao", "tesco", "amazon_uk"],
    },
    2: {
        "name": "Collect Shop / Brand Products",
        "desc": (
            "Shopee/Lazada: scrape all products from a shop URL\n"
            "      Watsons: scrape all products from brand URL(s), or option 2 = EVERY\n"
            "               brand on the brand list (~755; resumable, details optional)\n"
            "               (includes details + description in one pass)\n"
            "      Pinduoduo: products from mall_id(s) or shop link(s); uses the saved\n"
            "                 session (run action 0 first if it has expired)\n"
            "      Superdrug: brand/category URL(s) or brand name(s) — price, EAN,\n"
            "                 rating, review count, stock in one pass (fast)"
        ),
        "emoji": "🛍️",
        "platforms": ["shopee", "lazada", "watsons", "pinduoduo", "superdrug"],
    },
    3: {
        "name": "Collect Product Details",
        "desc": (
            "Get specs and description for products (requires product URLs)\n"
            "      [Watsons/Konvy already collect details in action 1 —\n"
            "       this is only for a standalone re-scrape]\n"
            "      Superdrug: ingredients, how to use, features, stock level, images\n"
            "      Tesco: ingredients, pack size, how to use, marketing copy, images\n"
            "      Amazon UK: BSR, bullets, ingredients, rating histogram, details tables\n"
            "      Taobao: brand, net content, origin, SPF, 备案号, SKUs/prices — best\n"
            "              sellers from a search file only, ~40 pages/day, resumes daily"
        ),
        "emoji": "📝",
        "platforms": ["shopee", "lazada", "konvy", "pinduoduo", "taobao", "superdrug", "tesco", "amazon_uk"],
    },
    4: {
        "name": "Collect Reviews — With Comments",
        "desc": (
            "Shopee: scrape reviews with text comments (fast, up to ~3 000)\n"
            "      Lazada: standard review scraper\n"
            "      Watsons: scrape all customer feedback (paginated)\n"
            "      Konvy: scrape reviews via internal API (needs product_id\n"
            "             from a prior action 1/3 detail scrape)\n"
            "      Pinduoduo: scrape reviews by scrolling (capped per product)\n"
            "      Superdrug: PowerReviews, newest first (capped per product)\n"
            "      Tesco: clicks 'Show more reviews', 10 per click (capped per product)\n"
            "      Amazon UK: only the reviews on the product page (~8–13, no sign-in)"
        ),
        "emoji": "💬",
        "platforms": ["shopee", "lazada", "watsons", "konvy", "pinduoduo", "superdrug", "tesco", "amazon_uk"],
    },
    5: {
        "name": "Collect Reviews — By Stars",
        "desc": "Scrape all reviews grouped by star rating (slower, more complete) [Shopee/Lazada only]",
        "emoji": "⭐",
        "platforms": ["shopee", "lazada"],
    },
    6: {
        "name": "Collect Category Products",
        "desc": (
            "Scrape listings from the site's own category pages [Pinduoduo / Amazon UK]\n"
            "      Pinduoduo: 美妆 = every sub-category, 美妆/面膜 = one sub-category\n"
            "      Amazon UK: paste a category URL — top products of it and every\n"
            "                 sub-category (Best Sellers top 100 + search pages)"
        ),
        "emoji": "📂",
        "platforms": ["pinduoduo", "amazon_uk"],
    },
}


# ════════════════════════════════════════════════════════════════════════════════
# Memory-file helpers
# ════════════════════════════════════════════════════════════════════════════════

def load_memory_file(path: Path = MEMORY_FILE) -> Optional[dict]:
    """
    Load the JSON memory file.  Returns None and prints a warning on failure.
    Accepts a raw path override for testing.
    """
    if not path.exists():
        print(f"⚠️  Memory file not found: {path}")
        return None
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        print(f"✅ Memory file loaded: {path}")
        return data
    except Exception as exc:
        print(f"❌ Could not parse memory file: {exc}")
        return None


def extract_urls_for_platform(data: dict, platform: str) -> List[str]:
    """
    Pull the list of URLs for *platform* from the memory dict.

    The JSON value may be:
      • a list of strings  → used as-is
      • a single string    → wrapped in a list
      • missing / null     → empty list
    """
    key = PLATFORM_URL_KEYS.get(platform, "")
    raw = data.get(key)
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(u).strip() for u in raw if u]
    if isinstance(raw, str) and raw.strip():
        return [raw.strip()]
    return []


def _build_watsons_brand_map(urls: List[str]) -> Dict[str, str]:
    """
    Convert a list of Watsons brand URLs into the brand_map dict that
    run_brand_url() expects: { brand_name: brand_url }.

    The brand name is inferred from the last meaningful URL path segment
    (e.g. ".../innisfree" → "innisfree").  Users can override at runtime.
    """
    brand_map: Dict[str, str] = {}
    for url in urls:
        parts = [p for p in url.rstrip("/").split("/") if p and "http" not in p]
        name = parts[-1].replace("-", " ").title() if parts else url
        brand_map[name] = url
    return brand_map


# ════════════════════════════════════════════════════════════════════════════════
# General helpers  (carried over from original scrap_agent.py)
# ════════════════════════════════════════════════════════════════════════════════

def print_header(title: str) -> None:
    print(f"\n{'='*60}")
    print(f"{title.center(60)}")
    print(f"{'='*60}")


def safe_input(prompt: str, default: str = "") -> str:
    """input() that falls back to `default` instead of crashing when stdin hits EOF
    (e.g. no interactive terminal attached)."""
    try:
        return input(prompt)
    except EOFError:
        print(f"\n⚠️  No input available (EOF) — using default: '{default}'")
        return default


def get_list_input(prompt_text: str) -> List[str]:
    print(f"\n{prompt_text}")
    print("(Enter multiple items separated by commas)")
    user_input = safe_input(">> ").strip()
    if not user_input:
        return []
    return [item.strip() for item in user_input.split(",") if item.strip()]


def load_df_from_file(file_path: str) -> Optional[pd.DataFrame]:
    path = Path(file_path.strip().strip('"').strip("'"))
    if not path.exists():
        print(f"❌ File not found: {path}")
        return None
    try:
        df = (
            pd.read_excel(path)
            if path.suffix.lower() in (".xlsx", ".xls")
            else pd.read_csv(path)
        )
    except Exception as exc:
        print(f"❌ Failed to read '{path}': {exc}")
        return None
    if "product_url" not in df.columns:
        print(f"❌ '{path.name}' has no 'product_url' column.  Found: {df.columns.tolist()}")
        return None
    if df.empty:
        print(f"❌ '{path.name}' is empty.")
        return None
    print(f"✅ Loaded {len(df):,} row(s) from '{path.name}'")
    return df


def get_product_urls_or_file(prompt_text: str):
    print(f"\n{prompt_text}")
    print("How do you want to provide product URLs?")
    print("  1. Paste URL(s) directly (comma separated)")
    print("  2. Load from a file path (.xlsx / .xls / .csv with a 'product_url' column)")
    choice = safe_input("Enter choice (1 or 2) [default: 1]: ", default="1").strip()

    if choice == "2":
        while True:
            file_path = safe_input("Enter full path to file: ").strip()
            if not file_path:
                print("❌ No path entered. Falling back to manual URL entry.")
                break
            df_loaded = load_df_from_file(file_path)
            if df_loaded is not None:
                urls = df_loaded["product_url"].dropna().unique().tolist()
                return urls, df_loaded
            retry = safe_input("Try another path? (y/n) [default: y]: ", default="y").strip().lower()
            if retry in ("n", "no"):
                break

    urls = get_list_input("Enter product URL(s):")
    return urls, None


def get_brand_map_input() -> dict:
    print("\nEnter Watsons brand name(s) and URL(s).")
    print('Format per line:  "BrandName | https://www.watsons.co.th/en/all-brands/list/…"')
    print("Enter a blank line when done.\n")
    brand_map: Dict[str, str] = {}
    while True:
        entry = safe_input(">> ").strip()
        if not entry:
            break
        if "|" not in entry:
            print("⚠️  Skipping — expected format:  BrandName | URL")
            continue
        name, url = entry.split("|", 1)
        name, url = name.strip(), url.strip()
        if name and url:
            brand_map[name] = url
    return brand_map


def _ask_int(prompt: str, default: int, lo: int, hi: int) -> int:
    """Whole number between lo and hi; Enter or anything else keeps the default."""
    raw = safe_input(f"{prompt} [default: {default}]: ").strip()
    if raw.isdigit() and lo <= int(raw) <= hi:
        return int(raw)
    if raw:
        print(f"   ↳ not a number from {lo} to {hi} — using {default}")
    return default


def _is_all_brands_request(urls: List[str]) -> bool:
    """WATSONS_LINK / KONVY_LINK containing ALL (or *) means: every brand on that site's brand list."""
    return any(u.strip().upper() in ("ALL", "*") for u in urls)


async def _run_konvy_all_brands() -> Optional[pd.DataFrame]:
    """Ask the all-brands options, then run konvy.run_all_brands()."""
    print("\nKonvy ALL-brands mode: reads Konvy's brand list (brand.php, ~1,200 brands), then pages through")
    print("each brand's own page (konvy.com/brand/<name>/, 40 products a page) and checks the count against")
    print("the number Konvy prints there. Progress is saved after every brand, so it can be stopped and")
    print("continued. Listing only is quick; with full details it is one page load per product (days).")

    detail = safe_input(
        "\nCollect full product details too? (y/n) [default: y]\n"
        "  n = brand + product name + URL only (much faster; details later via action 3): ",
        default="y",
    ).strip().lower() not in ("n", "no")

    only = get_list_input("Only these brand names (Enter = every brand):")
    max_brands = _ask_int("Brands to scrape in this run (0 = all remaining)", 0, 0, 5000) or None

    brands_file = safe_input(
        "\nBrand list file (.csv/.xlsx with a 'name' column, optional 'slug') — Enter = read Konvy's brand index: "
    ).strip().strip('"') or None

    resume = False
    saved = konvy.all_brands_checkpoint_summary(detail)
    if saved:
        print(f"\n💾 Earlier run from {saved['last_collected']}: {saved['brands_done']:,} brand(s) done, "
              f"{saved['products']:,} product(s)")
        resume = safe_input(
            "Continue from it and skip those brands? (y/n) [default: y]: ", default="y"
        ).strip().lower() in ("", "y", "yes")

    return await konvy.run_all_brands(
        detail=detail, only=only or None, max_brands=max_brands, resume=resume,
        brands_file=brands_file,
    )


async def _run_watsons_all_brands() -> Optional[pd.DataFrame]:
    """Ask the all-brands options, then run watsons.run_all_brands()."""
    print("\nWatsons ALL-brands mode: walks Watsons' whole brand list (~755 brands), one brand page")
    print("after another, ~10-15 s apart (robots.txt Crawl-delay 10). Progress is saved after every")
    print("brand, so it can be stopped and continued — a full run with details spans several days.")
    print("robots.txt also asks for visits 04:00–08:45 UTC (11:00–15:45 Thai time).")

    detail = safe_input(
        "\nCollect full product details too? (y/n) [default: y]\n"
        "  n = brand + product name + URL only (much faster; details later via action 3): ",
        default="y",
    ).strip().lower() not in ("n", "no")

    only = get_list_input("Only these brand names (Enter = every brand):")
    max_brands = _ask_int("Brands to scrape in this run (0 = all remaining)", 0, 0, 5000) or None

    resume = False
    saved = watsons.all_brands_checkpoint_summary(detail)
    if saved:
        print(f"\n💾 Earlier run from {saved['last_collected']}: {saved['brands_done']:,} brand(s) done, "
              f"{saved['products']:,} product(s)")
        resume = safe_input(
            "Continue from it and skip those brands? (y/n) [default: y]: ", default="y"
        ).strip().lower() in ("", "y", "yes")

    return await watsons.run_all_brands(
        detail=detail, only=only or None, max_brands=max_brands, resume=resume,
    )


def _ask_resume(scraper, site: str, kind: str) -> bool:
    """Offer to continue a Superdrug/Tesco listing/detail/review run from its checkpoint."""
    saved = scraper.checkpoint_summary(kind)
    if not saved:
        return False
    what = (f"keyword(s): {', '.join(saved['keywords'])}" if kind == "listing" and saved.get("keywords")
            else f"{saved['products']:,} product(s)")
    print(f"\n💾 {site} {kind} checkpoint from {saved['last_collected']}: {saved['rows']:,} rows for {what}")
    skip = "keywords" if kind == "listing" else "products"
    answer = safe_input(f"Continue from it and skip those {skip}? (y/n) [default: y]: ", default="y")
    return answer.strip().lower() in ("", "y", "yes")


def _make_timestamp() -> str:
    return datetime.now(TZ_THAI).strftime("%Y%m%d_%H%M")


def _slugify(text: str, max_len: int = 30) -> str:
    text = re.sub(r"[^\w\s-]", "", text, flags=re.UNICODE)
    text = re.sub(r"[\s_-]+", "_", text).strip("_")
    return text[:max_len]


def _build_filename(platform: str, actions: List[int], context_label: str = "") -> str:
    action_tag = "act" + "".join(str(a) for a in sorted(actions))
    parts = [platform, action_tag]
    if context_label:
        parts.append(_slugify(context_label))
    parts.append(_make_timestamp())
    return "_".join(parts)


def _label_from_brand_map(brand_map: dict) -> str:
    names = list(brand_map.keys())
    if len(names) == 1:
        return names[0]
    if len(names) <= 3:
        return "_".join(names)
    return f"{names[0]}_and_{len(names)-1}_more"


def _label_from_urls(urls: List[str]) -> str:
    if not urls:
        return ""
    parts = [p for p in urls[0].rstrip("/").split("/") if p and "http" not in p]
    fragment = parts[-1] if parts else urls[0]
    return _slugify(fragment)


def _label_from_keywords(keywords: List[str]) -> str:
    return _slugify("_".join(keywords[:2])) if keywords else ""


def _label_from_superdrug(targets: List[str]) -> str:
    """'…/cerave/b/403790' → 'cerave'; a brand name is used as-is."""
    names = []
    for t in targets[:2]:
        match = re.search(r"/([^/]+)/(?:b|c)/", t)
        names.append(match.group(1) if match else t)
    return _label_from_keywords(names)


# ════════════════════════════════════════════════════════════════════════════════
# Combined-output writer  (unchanged from original)
# ════════════════════════════════════════════════════════════════════════════════

def _save_combined_excel(
    platform: str,
    selected_actions: List[int],
    context_label: str,
    df_shop:   Optional[pd.DataFrame],
    df_detail: Optional[pd.DataFrame],
    df_review: Optional[pd.DataFrame],
) -> None:
    has_shop   = df_shop   is not None and not df_shop.empty
    has_detail = df_detail is not None and not df_detail.empty
    has_review = df_review is not None and not df_review.empty

    if not any([has_shop, has_detail, has_review]):
        print("ℹ️  No DataFrames available — skipping combined output.")
        return
    if sum([has_shop, has_detail, has_review]) < 2:
        print("ℹ️  Only one data source collected — no combined file needed.")
        return

    base     = _build_filename(platform, selected_actions, context_label)
    out_path = Path(COMBINED_OUTPUT_DIR) / f"{base}_COMBINED.xlsx"
    sheets: Dict[str, pd.DataFrame] = {}

    if has_shop:   sheets["shop_products"]  = df_shop.copy()
    if has_detail: sheets["product_detail"] = df_detail.copy()
    if has_review: sheets["reviews"]        = df_review.copy()

    if has_shop and has_detail:
        detail_cols = [c for c in df_detail.columns if c not in df_shop.columns or c == "product_url"]
        sheets["final_products"] = pd.merge(
            df_shop, df_detail[detail_cols], on="product_url", how="left", suffixes=("", "_detail"),
        )
    elif has_detail and not has_shop:
        sheets["final_products"] = df_detail.copy()

    if has_review:
        df_rev = df_review.copy()
        name_source = None
        if has_shop   and "product_url" in df_shop.columns   and "product_name" in df_shop.columns:
            name_source = df_shop[["product_url", "product_name"]].drop_duplicates("product_url")
        elif has_detail and "product_url" in df_detail.columns and "product_name" in df_detail.columns:
            name_source = df_detail[["product_url", "product_name"]].drop_duplicates("product_url")

        if name_source is not None and "product_url" in df_rev.columns:
            if "product_name" not in df_rev.columns:
                df_rev = pd.merge(
                    df_rev,
                    name_source.rename(columns={"product_name": "product_name_mapped"}),
                    on="product_url", how="left",
                )
                df_rev.insert(1, "product_name", df_rev.pop("product_name_mapped"))
            else:
                df_rev = pd.merge(
                    df_rev,
                    name_source.rename(columns={"product_name": "_pn_fill"}),
                    on="product_url", how="left",
                )
                mask = df_rev["product_name"].isna() | (df_rev["product_name"].str.strip() == "")
                df_rev.loc[mask, "product_name"] = df_rev.loc[mask, "_pn_fill"]
                df_rev.drop(columns=["_pn_fill"], inplace=True)
        sheets["final_reviews"] = df_rev

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        for sheet_name, df_s in sheets.items():
            df_s.to_excel(writer, sheet_name=sheet_name, index=False)

    print("\n" + "=" * 60)
    print("📦 COMBINED OUTPUT SAVED")
    print(f"📁 File  : {out_path}")
    print(f"📋 Sheets: {list(sheets.keys())}")
    for name, df_s in sheets.items():
        print(f"   • {name:<20} {len(df_s):>6,} rows  |  {df_s.shape[1]} cols")
    print("=" * 60)


# ════════════════════════════════════════════════════════════════════════════════
# NEW: Auto-pipeline  — reads JSON memory file and runs actions 2 (+3)
# ════════════════════════════════════════════════════════════════════════════════

async def run_auto_pipeline(memory_path: Path = MEMORY_FILE) -> None:
    """
    Automatically scrape all platforms whose URLs are defined in the
    memory JSON file.

    Pipeline per platform:
      Shopee / Lazada  → action 2 (shop products)  then action 3 (details)
      Watsons          → action 2 (brand products + details in one pass)

    Each platform runs independently; a failure on one platform does not
    stop the others.
    """
    print_header("🤖 Auto-Pipeline  (Memory File Mode)")

    data = load_memory_file(memory_path)
    if data is None:
        print("❌ Cannot proceed without memory file.  Falling back to manual mode.\n")
        await run_manual_pipeline()
        return

    # ── Show what URLs were found ─────────────────────────────────────────────
    print("\n📋 URLs found in memory file:")
    platform_urls: Dict[str, List[str]] = {}
    for platform in PLATFORM_URL_KEYS:
        urls = extract_urls_for_platform(data, platform)
        platform_urls[platform] = urls
        key = PLATFORM_URL_KEYS[platform]
        status = f"{len(urls)} URL(s)" if urls else "— not found / empty"
        print(f"   {platform.capitalize():<10}  [{key}]  {status}")
        for u in urls:
            print(f"               {u}")

    active = [p for p, urls in platform_urls.items() if urls]
    if not active:
        print("\n⚠️  No URLs found for any platform.  Nothing to scrape.")
        return

    print(f"\n▶  Will scrape: {', '.join(p.capitalize() for p in active)}")
    confirm = safe_input("\nProceed? (y/n) [default: y]: ", default="y").strip().lower()
    if confirm in ("n", "no"):
        print("Aborted.")
        return

    # ── Shopee ────────────────────────────────────────────────────────────────
    if "shopee" in active:
        print_header("🟧 Shopee — Action 2: Shop Products")
        shop_urls = platform_urls["shopee"]
        context_label = _label_from_urls(shop_urls)
        df_shop: Optional[pd.DataFrame] = None
        df_detail: Optional[pd.DataFrame] = None

        try:
            df_shop = await shopee.run_shop_products(shop_urls)
        except Exception as exc:
            print(f"❌ Shopee action 2 error: {exc}")

        if df_shop is not None and not df_shop.empty and "product_url" in df_shop.columns:
            product_urls = df_shop["product_url"].dropna().unique().tolist()
            print(f"\n✅ {len(product_urls)} product URL(s) collected from Shopee shops.")
            print_header("🟧 Shopee — Action 3: Product Details")
            try:
                df_detail = await shopee.run_product_detail(product_urls)
            except Exception as exc:
                print(f"❌ Shopee action 3 error: {exc}")

        _save_combined_excel(
            platform="shopee",
            selected_actions=[2, 3],
            context_label=context_label,
            df_shop=df_shop,
            df_detail=df_detail,
            df_review=None,
        )

    # ── Lazada ────────────────────────────────────────────────────────────────
    if "lazada" in active:
        print_header("🟦 Lazada — Action 2: Shop Products")
        shop_urls = platform_urls["lazada"]
        context_label = _label_from_urls(shop_urls)
        df_shop_lz: Optional[pd.DataFrame] = None
        df_detail_lz: Optional[pd.DataFrame] = None

        try:
            df_shop_lz = await lazada.run_product_by_shop(shop_urls)
        except Exception as exc:
            print(f"❌ Lazada action 2 error: {exc}")

        if df_shop_lz is not None and not df_shop_lz.empty:
            print(f"\n✅ {len(df_shop_lz):,} product(s) collected from Lazada shops.")
            print_header("🟦 Lazada — Action 3: Product Details")
            try:
                df_detail_lz = await lazada.run_product_detail(df_product=df_shop_lz)
            except Exception as exc:
                print(f"❌ Lazada action 3 error: {exc}")

        _save_combined_excel(
            platform="lazada",
            selected_actions=[2, 3],
            context_label=context_label,
            df_shop=df_shop_lz,
            df_detail=df_detail_lz,
            df_review=None,
        )

    # ── Watsons ───────────────────────────────────────────────────────────────
    if "watsons" in active:
        df_watsons: Optional[pd.DataFrame] = None

        if _is_all_brands_request(platform_urls["watsons"]):
            # WATSONS_LINK = ["ALL"] → every brand on Watsons' brand list
            print_header("🟩 Watsons — ALL brands")
            try:
                df_watsons = await _run_watsons_all_brands()
            except Exception as exc:
                print(f"❌ Watsons all-brands error: {exc}")
        else:
            print_header("🟩 Watsons — Action 2: Brand Products + Details")
            brand_map = _build_watsons_brand_map(platform_urls["watsons"])

            print("\n  Brand map inferred from URLs:")
            for name, url in brand_map.items():
                print(f"    • {name:<30} {url}")

            # Allow the user to rename brands before scraping
            edit = safe_input("\n  Edit brand names? (y/n) [default: n]: ", default="n").strip().lower()
            if edit in ("y", "yes"):
                new_map: Dict[str, str] = {}
                for name, url in brand_map.items():
                    new_name = safe_input(f"    Rename '{name}' (leave blank to keep): ").strip()
                    new_map[new_name if new_name else name] = url
                brand_map = new_map

            context_label = _label_from_brand_map(brand_map)

            try:
                # Watsons action 2 (run_brand_url) already collects full product
                # details + description in a single pass — no separate action 3 needed.
                df_watsons = await watsons.run_brand_url(brand_map)
            except Exception as exc:
                print(f"❌ Watsons action 2 error: {exc}")

        if df_watsons is not None and not df_watsons.empty:
            print(f"\n✅ {len(df_watsons):,} product(s) collected from Watsons.")
        else:
            print("\n⚠️  No Watsons products were collected.")

    # ── Konvy ─────────────────────────────────────────────────────────────────
    if "konvy" in active:
        print_header("🩷 Konvy — Search by Brand/Keyword (full detail)")
        keywords = platform_urls["konvy"]
        df_konvy: Optional[pd.DataFrame] = None

        try:
            if _is_all_brands_request(keywords):
                # KONVY_LINK = ["ALL"] → every brand on Konvy's brand index
                df_konvy = await _run_konvy_all_brands()
            else:
                # Konvy's search already scrapes full product detail (brand,
                # price, product_id, …) in a single pass — no separate action
                # needed here. Reviews (action 4) need product_id and are only
                # available via the manual pipeline for now.
                df_konvy = await konvy.run_search_keyword(keywords)
        except Exception as exc:
            print(f"❌ Konvy error: {exc}")

        if df_konvy is not None and not df_konvy.empty:
            print(f"\n✅ {len(df_konvy):,} product(s) collected from Konvy.")
        else:
            print("\n⚠️  No Konvy products were collected.")

    # ── Pinduoduo ─────────────────────────────────────────────────────────────
    if "pinduoduo" in active:
        print_header("🟥 Pinduoduo — Search by Keyword (listing only)")
        df_pinduoduo: Optional[pd.DataFrame] = None

        try:
            df_pinduoduo = await pinduoduo.run_search_keyword(platform_urls["pinduoduo"])
        except Exception as exc:
            print(f"❌ Pinduoduo error: {exc}")

        if df_pinduoduo is not None and not df_pinduoduo.empty:
            print(f"\n✅ {len(df_pinduoduo):,} product(s) collected from Pinduoduo.")
        else:
            print("\n⚠️  No Pinduoduo products were collected.")

    # ── Taobao ────────────────────────────────────────────────────────────────
    if "taobao" in active:
        print_header("🟠 Taobao — Search by Keyword (incl. sales)")
        df_taobao: Optional[pd.DataFrame] = None

        try:
            df_taobao = await taobao.run_search_keyword(platform_urls["taobao"])
        except Exception as exc:
            print(f"❌ Taobao error: {exc}")

        if df_taobao is not None and not df_taobao.empty:
            print(f"\n✅ {len(df_taobao):,} product(s) collected from Taobao.")
        else:
            print("\n⚠️  No Taobao products were collected.")

    # ── Superdrug ─────────────────────────────────────────────────────────────
    if "superdrug" in active:
        print_header("🟪 Superdrug — Action 2: Brand / Category Products")
        targets = platform_urls["superdrug"]
        context_label = _label_from_superdrug(targets)
        df_shop_sd: Optional[pd.DataFrame] = None
        df_detail_sd: Optional[pd.DataFrame] = None

        try:
            df_shop_sd = await superdrug.run_brand_products(targets)
        except Exception as exc:
            print(f"❌ Superdrug action 2 error: {exc}")

        if df_shop_sd is not None and not df_shop_sd.empty:
            print(f"\n✅ {len(df_shop_sd):,} product(s) collected from Superdrug.")
            print_header("🟪 Superdrug — Action 3: Product Details")
            try:
                df_detail_sd = await superdrug.run_product_detail(df_product=df_shop_sd)
            except Exception as exc:
                print(f"❌ Superdrug action 3 error: {exc}")
        else:
            print("\n⚠️  No Superdrug products were collected.")

        _save_combined_excel(
            platform="superdrug",
            selected_actions=[2, 3],
            context_label=context_label,
            df_shop=df_shop_sd,
            df_detail=df_detail_sd,
            df_review=None,
        )

    # ── Tesco ─────────────────────────────────────────────────────────────────
    if "tesco" in active:
        print_header("🔵 Tesco — Action 1: Search / Category Listing")
        entries = platform_urls["tesco"]
        context_label = _label_from_keywords([tesco._listing_url(e, 1)[0] for e in entries])
        df_list_ts: Optional[pd.DataFrame] = None
        df_detail_ts: Optional[pd.DataFrame] = None

        try:
            df_list_ts = await tesco.run_search_keyword(entries)
        except Exception as exc:
            print(f"❌ Tesco action 1 error: {exc}")

        if df_list_ts is not None and not df_list_ts.empty:
            print(f"\n✅ {len(df_list_ts):,} product(s) collected from Tesco.")
            print_header("🔵 Tesco — Action 3: Product Details")
            try:
                df_detail_ts = await tesco.run_product_detail(df_product=df_list_ts)
            except Exception as exc:
                print(f"❌ Tesco action 3 error: {exc}")
        else:
            print("\n⚠️  No Tesco products were collected.")

        _save_combined_excel(
            platform="tesco",
            selected_actions=[1, 3],
            context_label=context_label,
            df_shop=df_list_ts,
            df_detail=df_detail_ts,
            df_review=None,
        )

    # ── Amazon UK ─────────────────────────────────────────────────────────────
    if "amazon_uk" in active:
        print_header("🟨 Amazon UK — Action 1: Search")
        entries = platform_urls["amazon_uk"]
        context_label = _label_from_keywords([amazon_uk._search_url(e, 1)[0] for e in entries])
        df_list_az: Optional[pd.DataFrame] = None
        df_detail_az: Optional[pd.DataFrame] = None

        try:
            df_list_az = await amazon_uk.run_search_keyword(entries)
        except Exception as exc:
            print(f"❌ Amazon UK action 1 error: {exc}")

        if df_list_az is not None and not df_list_az.empty:
            print(f"\n✅ {len(df_list_az):,} product(s) collected from Amazon UK.")
            print_header("🟨 Amazon UK — Action 3: Product Details")
            try:
                df_detail_az = await amazon_uk.run_product_detail(df_product=df_list_az)
            except Exception as exc:
                print(f"❌ Amazon UK action 3 error: {exc}")
        else:
            print("\n⚠️  No Amazon UK products were collected.")

        _save_combined_excel(
            platform="amazon_uk",
            selected_actions=[1, 3],
            context_label=context_label,
            df_shop=df_list_az,
            df_detail=df_detail_az,
            df_review=None,
        )

    print_header("✅ Auto-Pipeline Complete")
    print(
        "\nNext steps in product_agent.py:\n"
        "  [2]  Identify Product Information  (AI insight columns)\n"
        "  [3]  Find Winning Products\n"
        "  [4]  Find Product Segment\n"
        "  [5]  Combine Platforms  (Omnichannel Master)\n"
    )


# ════════════════════════════════════════════════════════════════════════════════
# Manual pipeline  (original interactive flow, preserved intact)
# ════════════════════════════════════════════════════════════════════════════════

async def run_manual_pipeline() -> None:
    print_header("🤖 E-Commerce Scraper Agent  (Manual Mode)")

    # ── 1. Select Platform ────────────────────────────────────────────────────
    print("\nSelect Platform:")
    print("1. Shopee   🟧")
    print("2. Lazada   🟦")
    print("3. Watsons  🟩")
    print("4. Konvy    🩷")
    print("5. Pinduoduo 🟥")
    print("6. Taobao   🟠")
    print("7. Superdrug 🟪  (UK)")
    print("8. Tesco     🔵  (UK)")
    print("9. Amazon UK 🟨  (run action 0 once first)")

    platform_choice = safe_input("\nEnter choice (1-9): ").strip()
    platform = (
        "shopee"  if platform_choice == "1" else
        "lazada"  if platform_choice == "2" else
        "watsons" if platform_choice == "3" else
        "konvy"   if platform_choice == "4" else
        "pinduoduo" if platform_choice == "5" else
        "taobao"  if platform_choice == "6" else
        "superdrug" if platform_choice == "7" else
        "tesco"   if platform_choice == "8" else
        "amazon_uk" if platform_choice == "9" else
        None
    )

    if not platform:
        print("❌ Invalid platform choice. Exiting.")
        return

    # ── 2. Select Action(s) ───────────────────────────────────────────────────
    print_header("📋 Available Actions")
    available_steps = {k: v for k, v in STEPS.items() if platform in v["platforms"]}

    for key, step in available_steps.items():
        print(f"[{key}] {step['emoji']} {step['name']}")
        print(f"    └ {step['desc']}")

    action_str = safe_input("\nEnter action number(s) separated by commas (e.g., 2,3): ").strip()
    selected_actions: List[int] = []
    for part in action_str.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            action = int(part)
            if action not in available_steps:
                print(f"⚠️  Skipping invalid / unsupported action: {part}")
                continue
            if action not in selected_actions:
                selected_actions.append(action)
        except ValueError:
            print(f"⚠️  Skipping non-numeric input: '{part}'")

    if not selected_actions:
        print("❌ No valid actions selected. Exiting.")
        return

    # ── 3. Execute Actions Sequentially ──────────────────────────────────────
    collected_df: Optional[pd.DataFrame] = None
    df_shop:      Optional[pd.DataFrame] = None
    df_detail:    Optional[pd.DataFrame] = None
    df_review:    Optional[pd.DataFrame] = None
    context_label: str = ""

    for action in selected_actions:
        print_header(f"🚀 Running: [{action}] {STEPS[action]['name']} on {platform.title()}")

        # ── Action 0 : Session ────────────────────────────────────────────────
        if action == 0:
            if platform == "shopee":
                await shopee.create_shopee_session()
            elif platform == "lazada":
                await lazada.create_lazada_session()
            elif platform == "pinduoduo":
                await pinduoduo.create_pinduoduo_session()
            elif platform == "taobao":
                await taobao.create_taobao_session()
            elif platform == "amazon_uk":
                await amazon_uk.create_amazon_session()

        # ── Action 1 : Search by Keyword ──────────────────────────────────────
        elif action == 1:
            if platform == "tesco":
                print("\nTesco: type keywords, or paste search / category URLs, e.g.")
                print("  cerave , https://www.tesco.com/shop/en-GB/browse/health-and-beauty/face-skincare/all")
            elif platform == "amazon_uk":
                print("\nAmazon UK: type keywords, or paste amazon.co.uk search URLs (filters are kept).")
            konvy_all_brands = False
            if platform == "konvy":
                print("\nKonvy:  [1] specific brands / keywords")
                print("        [2] ALL brands on Konvy's brand index (resumable)")
                konvy_all_brands = safe_input("Choose (1/2) [default: 1]: ", default="1").strip() == "2"

            keywords = (
                [] if konvy_all_brands
                else [k.strip() for k in safe_input("Enter keywords (comma separated): ").split(",") if k.strip()]
            )
            if not context_label:
                context_label = "all_brands" if konvy_all_brands else _label_from_keywords(keywords)

            df_search: Optional[pd.DataFrame] = None
            if platform == "shopee":
                df_search = await shopee.run_search_keyword(keywords)
            elif platform == "lazada":
                df_search = await lazada.run_search_keyword(keywords)
            elif platform == "watsons":
                # Watsons' search already scrapes full product detail (brand,
                # price, sold, rating, reviews, …) per listing in one pass —
                # unlike Shopee/Lazada, there's no separate detail step to offer.
                df_search = await watsons.run_search_keyword(keywords)
                collected_df = df_search
            elif platform == "konvy":
                # Konvy's search (keyword = brand/title text) also scrapes
                # full detail per listing in one pass — same reasoning as Watsons.
                df_search = (
                    await _run_konvy_all_brands() if konvy_all_brands
                    else await konvy.run_search_keyword(keywords)
                )
                collected_df = df_search
            elif platform == "pinduoduo":
                # Kept so a later action 3/4 inherits the listing's price and
                # sales columns — product pages can mask the price.
                df_search = await pinduoduo.run_search_keyword(keywords)
                collected_df = df_search
            elif platform == "taobao":
                default_pages = taobao.DEFAULT_MAX_PAGES_PER_KEYWORD
                pages_str = safe_input(
                    f"Max pages per keyword (48 items each) [default: {default_pages}, "
                    "0 = all, up to 100 — large runs get rate-limited]: "
                ).strip()
                max_pages = (int(pages_str) or None) if pages_str.isdigit() else default_pages
                sort_by_sales = safe_input(
                    "Sort by sales volume? (y/n) [default: n]: ", default="n"
                ).strip().lower() in ("y", "yes")

                resume = False
                saved = taobao.search_checkpoint_summary()
                if saved:
                    print(f"\n💾 Checkpoint from {saved['last_collected']}: {saved['rows']:,} rows for "
                          f"{', '.join(saved['keywords'])}")
                    answer = safe_input(
                        "Continue from it and skip those keywords? (y/n) [default: y]: ", default="y"
                    ).strip().lower()
                    resume = answer in ("", "y", "yes")

                df_search = await taobao.run_search_keyword(
                    keywords, max_pages_per_keyword=max_pages,
                    sort_by_sales=sort_by_sales, resume=resume,
                )
                collected_df = df_search
                df_shop = df_search   # so the combined file can join search + detail
            elif platform == "tesco":
                default_pages = tesco.DEFAULT_MAX_PAGES_PER_KEYWORD
                pages_str = safe_input(
                    f"Max pages per keyword/URL (48 items each) [default: {default_pages}, 0 = all]: "
                ).strip()
                max_pages = (int(pages_str) or None) if pages_str.isdigit() else default_pages
                resume = _ask_resume(tesco, "Tesco", "listing")
                df_search = await tesco.run_search_keyword(
                    keywords, max_pages_per_keyword=max_pages, resume=resume,
                )
                collected_df = df_search
                df_shop = df_search   # so the combined file can join listing + detail
            elif platform == "amazon_uk":
                default_pages = amazon_uk.DEFAULT_MAX_PAGES_PER_KEYWORD
                pages_str = safe_input(
                    f"Max pages per keyword [default: {default_pages}, 0 = all (up to "
                    f"{amazon_uk.MAX_PAGES_HARD}) — keep it low]: "
                ).strip()
                max_pages = (int(pages_str) or None) if pages_str.isdigit() else default_pages
                resume = _ask_resume(amazon_uk, "Amazon UK", "listing")
                df_search = await amazon_uk.run_search_keyword(
                    keywords, max_pages_per_keyword=max_pages, resume=resume,
                )
                collected_df = df_search
                df_shop = df_search

            product_urls: List[str] = (
                df_search["product_url"].dropna().unique().tolist()
                if df_search is not None and not df_search.empty and "product_url" in df_search.columns
                else []
            )

            if product_urls and platform == "konvy" and "sale_price" not in df_search.columns:
                print(f"\n✅ {len(product_urls)} product(s) found — listing only (no price/product_id yet); "
                      "run action 3 with this file for details.")
            elif product_urls and platform in ("watsons", "konvy"):
                print(f"\n✅ {len(product_urls)} product(s) found — full detail already included (brand/price/…).")
            elif product_urls and platform == "taobao":
                print(f"\n✅ {len(product_urls)} product(s) found — "
                      + ("details for the best sellers follow in action 3." if 3 in selected_actions
                         else "for product details run action 3 with this search file."))
            elif product_urls and platform in ("tesco", "amazon_uk") and 3 in selected_actions:
                print(f"\n✅ {len(product_urls)} product(s) found — details follow in action 3.")
            elif product_urls:
                print(f"\n✅ {len(product_urls)} product(s) found.")
                want = safe_input("Also scrape detailed description? (y/n) [default: y]: ", default="y").strip().lower()
                if want in ("y", "yes", ""):
                    if platform == "shopee":
                        df_detail = await shopee.run_product_detail(product_urls)
                    elif platform == "lazada":
                        df_detail = await lazada.run_product_detail(product_urls=product_urls)
                    elif platform == "pinduoduo":
                        df_detail = await pinduoduo.run_product_detail(df_product=df_search)
                    elif platform == "tesco":
                        df_detail = await tesco.run_product_detail(df_product=df_search)
                    elif platform == "amazon_uk":
                        df_detail = await amazon_uk.run_product_detail(df_product=df_search)

        # ── Action 2 : Shop / Brand Products ──────────────────────────────────
        elif action == 2:
            if platform == "watsons":
                print("\nWatsons:  [1] specific brands (name | URL)")
                print("          [2] ALL brands on Watsons' brand list (~755, resumable)")
                scope = safe_input("Choose (1/2) [default: 1]: ", default="1").strip()
                if scope == "2":
                    collected_df = await _run_watsons_all_brands()
                    if collected_df is None or collected_df.empty:
                        print("❌ No Watsons products collected. Skipping.")
                        continue
                    if not context_label:
                        context_label = "all_brands"
                    df_shop = collected_df
                else:
                    brand_map = get_brand_map_input()
                    if not brand_map:
                        print("❌ No brand entries provided. Skipping.")
                        continue
                    if not context_label:
                        context_label = _label_from_brand_map(brand_map)
                    collected_df = await watsons.run_brand_url(brand_map)
                    df_shop = collected_df
            elif platform == "pinduoduo":
                shops = get_list_input("Enter mall_id(s) or full shop link(s), copied whole from the address bar:")
                if not shops:
                    print("❌ No shops provided. Skipping.")
                    continue
                default_limit = pinduoduo.DEFAULT_MAX_PRODUCTS_PER_SHOP
                limit_str = safe_input(
                    f"Max products per shop [default: {default_limit}, "
                    "0 = no limit — large runs get rate-limited]: "
                ).strip()
                limit = (int(limit_str) or None) if limit_str.isdigit() else default_limit

                resume = False
                saved = pinduoduo.shop_checkpoint_summary()
                if saved:
                    print(f"\n💾 Checkpoint from {saved['last_collected']}: {saved['rows']:,} rows in "
                          f"{len(saved['shops'])} shop(s): {', '.join(saved['shops'])}")
                    answer = safe_input(
                        "Continue from it and skip those shops? (y/n) [default: y]: ", default="y"
                    ).strip().lower()
                    resume = answer in ("", "y", "yes")

                collected_df = await pinduoduo.run_shop_products(
                    shops, max_products_per_shop=limit, resume=resume
                )
                df_shop = collected_df
                if not context_label and not collected_df.empty:
                    context_label = _label_from_keywords(collected_df["shop_key"].astype(str).unique().tolist())
            elif platform == "superdrug":
                print("\nPaste Superdrug brand/category URL(s), or type brand name(s) as on the A–Z page.")
                print("  e.g. https://www.superdrug.com/cerave/b/403790 , "
                      "https://www.superdrug.com/skin/korean-japanese-beauty/c/k-beauty , L'Oreal Paris")
                targets = get_list_input("Brand / category URL(s) or brand name(s):")
                if not targets:
                    print("❌ No brands / categories provided. Skipping.")
                    continue
                limit_str = safe_input(
                    "Max products per brand/category [default: 0 = all]: ", default="0"
                ).strip()
                limit = (int(limit_str) or None) if limit_str.isdigit() else None
                if not context_label:
                    context_label = _label_from_superdrug(targets)
                collected_df = await superdrug.run_brand_products(targets, max_products_per_target=limit)
                df_shop = collected_df
            else:
                shop_urls = get_list_input("Enter shop URL(s):")
                if not shop_urls:
                    print("❌ No shop URLs provided. Skipping.")
                    continue
                if not context_label:
                    context_label = _label_from_urls(shop_urls)
                if platform == "shopee":
                    collected_df = await shopee.run_shop_products(shop_urls)
                else:
                    collected_df = await lazada.run_product_by_shop(shop_urls)
                df_shop = collected_df

        # ── Action 6 : Category Products (Amazon UK) ──────────────────────────
        elif action == 6 and platform == "amazon_uk":
            print("\nAmazon UK: paste a category URL from amazon.co.uk — a /b?node=… page, a search URL")
            print("with rh=n:…, or a Best Sellers page — or just the node number.")
            print("This collects the TOP of the category and each sub-category, not every product.")
            start = safe_input("Category URL / node: ").strip().strip('"')
            if not start:
                print("❌ No category given. Skipping.")
                continue
            depth = _ask_int("Sub-category levels to follow", amazon_uk.CATEGORY_MAX_DEPTH, 0, 6)
            print("\nEvery-product mode pages through each lowest sub-category to the end and splits big")
            print("ones by price (£0–3, £3–5, …) so Amazon's ~7-page limit doesn't cut them off.")
            print("Far more pages: plan on many runs. Items with no price (unavailable) are still missed.")
            every = safe_input("Collect EVERY product? (y/n) [default: n]: ", default="n").strip().lower() in ("y", "yes")
            pages = amazon_uk.CATEGORY_PAGES_PER_NODE
            if not every:
                pages = _ask_int("Search pages per lowest sub-category (~48 items each)",
                                 amazon_uk.CATEGORY_PAGES_PER_NODE, 1, amazon_uk.MAX_PAGES_HARD)
            bs_pages = _ask_int("Best Sellers pages per category (50 each, 0 = skip)",
                                amazon_uk.CATEGORY_BESTSELLER_PAGES, 0, 2)
            budget = _ask_int("Page loads this run (0 = no limit — long runs get blocked)",
                              amazon_uk.CATEGORY_PAGES_PER_RUN, 0, 100000)

            resume = False
            saved = amazon_uk.category_checkpoint_summary()
            if saved:
                print(f"\n💾 Checkpoint from {saved['last_collected']}: '{saved['root_name']}' (node "
                      f"{saved['root_node']}) — {saved['visited']}/{saved['categories']} categories opened, "
                      f"{saved['products']:,} products")
                answer = safe_input("Continue from it if it is the same category? (y/n) [default: y]: ",
                                    default="y").strip().lower()
                resume = answer in ("", "y", "yes")

            collected_df = await amazon_uk.run_category_crawl(
                start, max_depth=depth, pages_per_node=pages, bestseller_pages=bs_pages,
                page_budget=budget or None, resume=resume, exhaustive=every,
            )
            df_shop = collected_df
            if not context_label and not collected_df.empty:
                context_label = _label_from_keywords([str(collected_df["best_rank_category"].iloc[0]).split(" > ")[0]])

        # ── Action 6 : Category Products (Pinduoduo) ──────────────────────────
        elif action == 6:
            print("\nPress Enter to pick from Pinduoduo's current category list, or type")
            print("Chinese names (美妆 = every sub-category, 美妆/面膜 = one) or paste URLs.")
            categories = get_list_input("Category name(s) / URL(s) [Enter = pick from list]:")
            default_limit = pinduoduo.DEFAULT_MAX_PRODUCTS_PER_CATEGORY
            limit_str = safe_input(
                f"Max products per sub-category [default: {default_limit}, "
                "0 = no limit — large runs get rate-limited]: "
            ).strip()
            limit = (int(limit_str) or None) if limit_str.isdigit() else default_limit

            resume = False
            saved = pinduoduo.category_checkpoint_summary()
            if saved:
                print(f"\n💾 Checkpoint from {saved['last_collected']}: {saved['rows']:,} rows in "
                      f"{', '.join(saved['categories'])}")
                answer = safe_input(
                    "Continue from it and skip those categories? (y/n) [default: y]: ", default="y"
                ).strip().lower()
                resume = answer in ("", "y", "yes")

            collected_df = await pinduoduo.run_category_products(
                categories,
                max_products_per_category=limit,
                ask=lambda prompt: asyncio.to_thread(safe_input, prompt),
                resume=resume,
            )
            df_shop = collected_df
            if not context_label and not collected_df.empty:
                context_label = _label_from_keywords(collected_df["category_l2"].unique().tolist())

        # ── Actions 3 / 4 / 5 ────────────────────────────────────────────────
        elif action in (3, 4, 5):
            product_urls = []
            if (
                collected_df is not None
                and not collected_df.empty
                and "product_url" in collected_df.columns
            ):
                product_urls = collected_df["product_url"].dropna().unique().tolist()
                print(f"🔗 Auto-feeding {len(product_urls)} URL(s) from previous step …")
            else:
                prompt = "Enter product URL(s):" if action == 3 else "Enter product URL(s) for reviews:"
                product_urls, df_loaded = get_product_urls_or_file(prompt)
                if df_loaded is not None:
                    collected_df = df_loaded
                if not context_label:
                    context_label = _label_from_urls(product_urls)

            if not product_urls:
                print("❌ No product URLs provided. Skipping.")
                continue

            if action == 3:
                if platform == "shopee":
                    df_detail = await shopee.run_product_detail(product_urls)
                elif platform == "lazada":
                    df_detail = (
                        await lazada.run_product_detail(df_product=collected_df)
                        if collected_df is not None and not collected_df.empty
                        else await lazada.run_product_detail(product_urls=product_urls)
                    )
                    collected_df = df_detail
                elif platform == "watsons":
                    result = await watsons.run_product_detail(product_urls=product_urls, df_product=collected_df)
                    collected_df = result
                    df_detail = result
                elif platform == "konvy":
                    result = await konvy.run_product_detail(product_urls=product_urls, df_product=collected_df)
                    collected_df = result
                    df_detail = result
                elif platform == "pinduoduo":
                    df_detail = await pinduoduo.run_product_detail(product_urls=product_urls, df_product=collected_df)
                elif platform == "taobao":
                    coverage = taobao.DEFAULT_SALES_COVERAGE
                    if collected_df is not None and not collected_df.empty and "sold_count" in collected_df.columns:
                        default_pct = int(taobao.DEFAULT_SALES_COVERAGE * 100)
                        pct_str = safe_input(
                            "Open product pages for the best sellers making up what % of sales? "
                            f"[default: {default_pct}, 100 = every item]: "
                        ).strip()
                        pct = int(pct_str) if pct_str.isdigit() and 0 < int(pct_str) <= 100 else default_pct
                        coverage = None if pct >= 100 else pct / 100
                        picked = len(taobao.select_best_sellers(collected_df, coverage))
                        limit = taobao.DAILY_DETAIL_LIMIT
                        print(f"   → {picked:,} item(s)" + (
                            f"; budget {limit} pages/day ({taobao.detail_quota_left()} left today) "
                            f"→ about {-(-picked // limit)} day(s) in total" if limit else ""))
                    resume = _ask_resume(taobao, "Taobao", "detail")
                    df_detail = await taobao.run_product_detail(
                        product_urls=product_urls, df_product=collected_df,
                        sales_coverage=coverage, resume=resume,
                    )
                    if collected_df is None or collected_df.empty:
                        collected_df = df_detail
                elif platform == "superdrug":
                    resume = _ask_resume(superdrug, "Superdrug", "detail")
                    df_detail = await superdrug.run_product_detail(
                        product_urls=product_urls, df_product=collected_df, resume=resume
                    )
                    if collected_df is None or collected_df.empty:
                        collected_df = df_detail
                elif platform == "tesco":
                    resume = _ask_resume(tesco, "Tesco", "detail")
                    df_detail = await tesco.run_product_detail(
                        product_urls=product_urls, df_product=collected_df, resume=resume
                    )
                    if collected_df is None or collected_df.empty:
                        collected_df = df_detail
                elif platform == "amazon_uk":
                    df_for_detail = collected_df
                    if (6 in selected_actions and collected_df is not None
                            and "best_rank" in collected_df.columns and len(product_urls) > 100):
                        # a category crawl finds thousands of products — product pages are the slow part
                        n_open = _ask_int(f"{len(product_urls):,} products found. Open product pages for how "
                                          "many (best ranked first)", 100, 1, len(product_urls))
                        df_for_detail = collected_df.head(n_open)
                        product_urls = df_for_detail["product_url"].dropna().tolist()
                    resume = _ask_resume(amazon_uk, "Amazon UK", "detail")
                    df_detail = await amazon_uk.run_product_detail(
                        product_urls=product_urls, df_product=df_for_detail, resume=resume
                    )
                    if collected_df is None or collected_df.empty:
                        collected_df = df_detail

            elif action == 4:
                if platform == "shopee":
                    df_review = await shopee.run_product_reviews_with_comments(product_urls)
                elif platform == "lazada":
                    df_review = await lazada.run_reviews(product_urls)
                elif platform == "watsons":
                    df_review = await watsons.run_product_reviews(product_urls=product_urls, df_product=collected_df)
                elif platform == "konvy":
                    # Konvy's review API is keyed by product_id (not product_url) —
                    # pulled from the 'product_id' column collected_df already has
                    # from a prior action 1/3 detail scrape.
                    df_review = await konvy.run_product_reviews(df_product=collected_df)
                elif platform == "pinduoduo":
                    df_review = await pinduoduo.run_product_reviews(product_urls=product_urls, df_product=collected_df)
                elif platform == "superdrug":
                    default_cap = superdrug.DEFAULT_MAX_REVIEWS_PER_PRODUCT
                    cap_str = safe_input(
                        f"Max reviews per product [default: {default_cap}, 0 = all]: "
                    ).strip()
                    cap = (int(cap_str) or None) if cap_str.isdigit() else default_cap
                    resume = _ask_resume(superdrug, "Superdrug", "review")
                    df_review = await superdrug.run_product_reviews(
                        product_urls=product_urls, df_product=collected_df,
                        max_reviews_per_product=cap, resume=resume,
                    )
                elif platform == "tesco":
                    default_cap = tesco.DEFAULT_MAX_REVIEWS_PER_PRODUCT
                    cap_str = safe_input(
                        f"Max reviews per product [default: {default_cap}, 0 = all — 10 per click, slow]: "
                    ).strip()
                    cap = (int(cap_str) or None) if cap_str.isdigit() else default_cap
                    resume = _ask_resume(tesco, "Tesco", "review")
                    df_review = await tesco.run_product_reviews(
                        product_urls=product_urls, df_product=collected_df,
                        max_reviews_per_product=cap, resume=resume,
                    )
                elif platform == "amazon_uk":
                    # Only the reviews printed on each product page — pages already
                    # opened by action 3 in this run are reused, not reloaded.
                    resume = _ask_resume(amazon_uk, "Amazon UK", "review")
                    df_review = await amazon_uk.run_product_reviews(
                        product_urls=product_urls, df_product=collected_df, resume=resume,
                    )

            elif action == 5:
                if platform == "shopee":
                    df_review = await shopee.run_product_reviews_by_stars(product_urls)
                else:
                    df_review = await lazada.run_reviews(product_urls)

    # ── 4. Combined output ────────────────────────────────────────────────────
    # Tesco / Amazon UK / Taobao listings come from action 1 (search), so it counts as the "shop" sheet
    ran_shop   = bool({2, 6} & set(selected_actions)) or (
        platform in ("tesco", "amazon_uk", "taobao") and 1 in selected_actions)
    ran_detail = 3 in selected_actions
    ran_review = bool({4, 5} & set(selected_actions))

    if sum([ran_shop, ran_detail, ran_review]) >= 2:
        print_header("📦 Building Combined Output File")
        _save_combined_excel(
            platform=platform,
            selected_actions=selected_actions,
            context_label=context_label,
            df_shop=df_shop,
            df_detail=df_detail,
            df_review=df_review,
        )

    print_header("✅ All Selected Tasks Complete!")


# ════════════════════════════════════════════════════════════════════════════════
# Entry-point menu
# ════════════════════════════════════════════════════════════════════════════════

async def main() -> None:
    print_header("🤖 E-Commerce Scraper Agent")
    print(
        "\n  [A]  Auto-Pipeline  — load shop/brand URLs from memory file\n"
        f"       ({MEMORY_FILE})\n\n"
        "  [M]  Manual Mode   — select platform & actions interactively\n"
    )
    mode = safe_input("Choose mode (A / M) [default: A]: ", default="A").strip().upper() or "A"

    if mode == "A":
        # Optionally override the memory file path
        custom = safe_input(
            f"Memory file path [press Enter for default:\n  {MEMORY_FILE}]: "
        ).strip().strip('"').strip("'")
        mem_path = Path(custom) if custom else MEMORY_FILE
        await run_auto_pipeline(mem_path)
    else:
        await run_manual_pipeline()


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n🛑 Interrupted by user. Exiting gracefully.")
        sys.exit(0)