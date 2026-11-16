"""
scrape_category_search.py — Product-overview (search by keyword) scrape
across Shopee, Lazada, and Konvy for a fixed list of CATEGORY keywords
(Thai + English name per category) — same idea as scrape_brand_search.py
but for generic category terms (shampoo, conditioner, …) instead of brand
names.

For each category, each platform is searched with BOTH the English and Thai
term in one call (so results are tagged by which keyword matched), and
saved as ONE .xlsx per (platform, category) — no reviews/comments.

Output: N categories x 3 platforms files under ./category_search_output/
    {platform}_search_{category_slug}_TIMESTAMP.xlsx

NOTE — category terms return far more results than a brand name does
(hundreds of products across many brands, vs. a narrow brand-name match).
Konvy in particular does a full per-product detail-page visit for every
result (see _run_konvy_categories docstring), so a broad category can take
substantially longer to run than a brand ever did — budget accordingly.

USAGE
------
python scripts/scrape_category_search.py
python scripts/scrape_category_search.py --platforms shopee,lazada
python scripts/scrape_category_search.py --categories shampoo,conditioner
"""

import asyncio
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import pandas as pd

# Let `python scripts/<name>.py` (run from the project root) import scrapers/
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scrapers import cloak_shopee_AI_bot as shopee
from scrapers import cloak_lazada         as lazada
from scrapers import cloak_konvy          as konvy

# Resume-from-backup so an interrupted run of this category list can pick
# back up instead of re-scraping categories already saved to temp checkpoints.
shopee.RESUME_FROM_BACKUP = True
lazada.RESUME_FROM_BACKUP = True
konvy.RESUME_FROM_BACKUP  = True

OUTPUT_DIR = Path("./data/category_search_output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# (slug, english_term, thai_term) — slug used only for the output filename.
CATEGORIES = [
    ("shampoo",           "shampoo",           "แชมพู"),
    ("shampoo1",          "ยาสระผม",           "ผลิตภัณฑ์สระผม"),
    ("conditioner",       "Conditioner",       "คอนดิชันเนอร์"),
    ("hair_care",         "Hair Care",         "ผลิตภัณฑ์บำรุงผม"),
    ("hair_treatment",    "Hair Treatment",    "ทรีทเม้นท์ผม"),
    ("keratin_treatment", "Keratin Treatment", "ทรีทเม้นท์เคราติน"),
    ("body_lotion",       "Body Lotion",       "โลชั่นบำรุงผิว"),
    ("soap_bar",          "Soap Bar",          "สบู่ก้อน"),
    ("liquid_blush",      "Liquid Blush",      "บลัชออนเนื้อน้ำ"),
    ("body_cream",        "โลชั่น",              "ครีมทาตัว"),
    ("lip_stick",         "Lip Stick",         "ลิปสติก"),
    ("lip_matte",         "Lip Matte",         "ลิปแมตต์"),
    ("lip_oil",           "Lip Oil",           "ลิปออยล์"),
    ("lip_balm",          "Lip Balm",          "ลิปบาล์ม"),
    ("sunscreen",         "Sun Screen",        "ครีมกันแดด"),
    ("toneup",            "Tone Up Cream",     "โทนอัพครีม"),
    ("soapbar",           "Soap Bar",          "สบู่ก้อน"),
    ("exfac1",            "EXFAC",             "เอ็กแฟค"),
    ("exfac2",            "Exfac",             "เอ็กซ์แฟค"),
    ("beautiibe1",        "Beautii Be",        "บิวตี้บี"),
    ("beautiibe2",        "BEAUTII BE",        "บิวตี้ บี"),
    ("biox1",             "BioX",              "ไบโอเอ็กซ์"),
    ("biox2",             "Bio X",             "ไบโอเอ็กซ์ทำความสะอาด")
]

# Konvy-only keyword overrides, same mechanism as scrape_brand_search.py —
# add an entry here if a category term needs a different spelling on Konvy.
KONVY_KEYWORD_OVERRIDES = {}

# Brand-name searches cap at 40 pages on Konvy (a real brand only ever fills
# a handful of pages, so 40+ means the query broke and fell back to a whole-
# catalog dump — see scrape_brand_search.py). A genuine category term is
# supposed to return many pages (shampoo/conditioner are broad, multi-brand
# categories), so that cap would misfire here — set much higher as a loose
# safety net against an actual catalog-dump failure, not a real limit on
# legitimate category size.
KONVY_MAX_SANE_PAGES = 300

# Konvy has no observed anti-bot/rate-limit wall on product pages (unlike
# Shopee/Lazada), so the detail-fetch stage — the actual bottleneck, since
# a broad category needs one full page visit per product — can be fanned
# out across several tabs in the same browser context instead of visiting
# pages one at a time. Raise/lower this to trade speed against server load;
# each tab still keeps its own _human_delay() between requests.
KONVY_DETAIL_TABS = 5


def _timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _sanitize_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    """Strip control characters openpyxl refuses to write into a cell
    (raises IllegalCharacterError otherwise) — real artifacts occasionally
    present in scraped shop/product text, not a scraping bug."""
    from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
    str_cols = df.select_dtypes(include="object").columns
    for col in str_cols:
        df[col] = df[col].map(lambda v: ILLEGAL_CHARACTERS_RE.sub("", v) if isinstance(v, str) else v)
    return df


def _save(df: pd.DataFrame, platform: str, category_slug: str) -> None:
    if df is None or df.empty:
        print(f"⚠️  {platform}/{category_slug}: nothing to save (empty result).")
        return
    df = _sanitize_for_excel(df)
    path = OUTPUT_DIR / f"{platform}_search_{category_slug}_{_timestamp()}.xlsx"
    df.to_excel(path, index=False)
    print(f"💾 Saved: {path}  ({len(df):,} rows)")


async def _run_shopee_categories(categories) -> None:
    for slug, en, th in categories:
        print(f"\n{'='*60}\n🟧 SHOPEE — {en} / {th}\n{'='*60}")
        try:
            df = await shopee.run_search_keyword([en, th], max_pages=None)
            _save(df, "shopee", slug)
        except Exception as e:
            print(f"❌ Shopee {en}/{th} failed: {e}")


async def _run_lazada_categories(categories) -> None:
    for slug, en, th in categories:
        print(f"\n{'='*60}\n🟦 LAZADA — {en} / {th}\n{'='*60}")
        try:
            df = await lazada.run_search_keyword([en, th])
            _save(df, "lazada", slug)
        except Exception as e:
            print(f"❌ Lazada {en}/{th} failed: {e}")


async def _konvy_page_count(page, keyword) -> int:
    """Cheap precheck of how many listing pages a keyword has, without
    scraping any of them — used to catch a genuine broken-query/whole-
    catalog fallback before wasting time (and mislabeling data) on it."""
    await page.goto(f"https://www.konvy.com/list/?title={quote(keyword)}", wait_until="domcontentloaded", timeout=60_000)
    await asyncio.sleep(1.5)
    try:
        page_nums = await page.eval_on_selector_all(
            "div.sc_paging a",
            "els => els.map(e => e.innerText.trim()).filter(t => /^\\d+$/.test(t)).map(Number)",
        )
        return max(page_nums) if page_nums else 1
    except Exception:
        return 1


async def _fetch_konvy_details_concurrent(context, items: list, n_tabs: int) -> list:
    """Fan *items* (each a dict with at least 'product_url') out across
    *n_tabs* browser tabs in the same context, each pulling from a shared
    queue so a few slow product pages don't leave other tabs idle. Returns
    the collected detail rows (order not guaranteed — order doesn't matter
    for the final per-category dataframe)."""
    if not items:
        return []

    queue: asyncio.Queue = asyncio.Queue()
    for item in items:
        queue.put_nowait(item)

    rows: list = []
    done = 0
    total = len(items)

    async def worker(tab_id: int):
        nonlocal done
        tab_page = await context.new_page()
        try:
            while True:
                try:
                    item = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    row = await konvy._scrape_product_detail(tab_page, item)
                except Exception as e:
                    print(f"❌ [tab{tab_id}] detail fetch failed for {item['product_url']}: {e}")
                    row = None
                done += 1
                status = "✅" if row else "⚠️"
                print(f"   {status} [tab{tab_id}] {done}/{total} → {item['product_url']}")
                if row:
                    rows.append(row)
                await konvy._human_delay()
        finally:
            await tab_page.close()

    await asyncio.gather(*(worker(i + 1) for i in range(min(n_tabs, total))))
    return rows


async def _run_konvy_categories(categories) -> None:
    """Full per-product detail scrape (product_id, price, category, stock,
    sold, rating, other_attributes, …) — same flow as
    scrape_brand_search.py's _run_konvy_brands, reused here for category
    terms. A broad category can return hundreds of listings, each needing
    its own detail-page visit, so this can take much longer per category
    than any single brand did."""
    print(f"\n📂 [SESSION] Loading: './data/konvy/browser_session'")
    # Detail pages are parsed from HTML (BeautifulSoup), not screenshotted —
    # image_url is already captured off the listing page's <img> tag, so
    # blocking images here only cuts page-load time, no data lost.
    context, page = await konvy._create_browser_context(block_images=True)
    cookie_dismissed = False
    async with context:
        for slug, en, th in categories:
            print(f"\n{'='*60}\n🩷 KONVY — {en} / {th}\n{'='*60}")
            k_en, k_th = KONVY_KEYWORD_OVERRIDES.get(slug, (en, th))
            if (k_en, k_th) != (en, th):
                print(f"   ℹ️  Konvy override: '{en}'/'{th}' -> '{k_en}'/'{k_th}'")

            # Collect + dedupe links from BOTH keywords first, so a product
            # matching both the EN and TH term only gets its detail page
            # fetched once instead of twice (the two keywords overlap
            # heavily for a category term — this was the main time sink).
            all_links = []
            seen_urls = set()
            for kw in (k_en, k_th):
                try:
                    n_pages = await _konvy_page_count(page, kw)
                    if n_pages > KONVY_MAX_SANE_PAGES:
                        print(f"   ⚠️  '{kw}' -> {n_pages} pages (> {KONVY_MAX_SANE_PAGES}) — looks like Konvy's "
                              f"broken-query catalog-dump fallback, not a genuine category match. Skipping.")
                        continue
                    print(f"🔑 Keyword: '{kw}'  ({n_pages} page(s))")
                    links, cookie_dismissed = await konvy._collect_links_from_url(page, kw, cookie_dismissed)
                    new_links = [l for l in links if l["product_url"] not in seen_urls]
                    seen_urls.update(l["product_url"] for l in new_links)
                    all_links.extend(new_links)
                    print(f"   ✅ {len(links)} listing(s) found for '{kw}' "
                          f"({len(new_links)} new, {len(links) - len(new_links)} already seen from the other keyword).")
                except Exception as e:
                    print(f"❌ Konvy keyword '{kw}' failed: {e}")

            n_tabs = min(KONVY_DETAIL_TABS, len(all_links)) or 1
            print(f"   ⏳ Fetching detail for {len(all_links)} unique product(s) across {n_tabs} tab(s)…")
            rows = await _fetch_konvy_details_concurrent(context, all_links, KONVY_DETAIL_TABS)

            df = pd.DataFrame(rows)
            if not df.empty:
                df = df.drop_duplicates(subset=["product_url"])
            _save(df, "konvy", slug)


async def main(platforms=None, category_slugs=None) -> None:
    platforms = platforms or ["shopee", "lazada", "konvy"]
    categories = CATEGORIES if not category_slugs else [c for c in CATEGORIES if c[0] in category_slugs]
    if not categories:
        print("⚠️  No matching categories for --categories filter.")
        return

    tasks = []
    if "shopee" in platforms:
        tasks.append(_run_shopee_categories(categories))
    if "lazada" in platforms:
        tasks.append(_run_lazada_categories(categories))
    if "konvy" in platforms:
        tasks.append(_run_konvy_categories(categories))

    # Each platform uses its own persistent browser profile dir, so running
    # them concurrently is safe and cuts total wall-clock time roughly 3x.
    await asyncio.gather(*tasks)
    print("\n✅ All category searches done.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--platforms", default="shopee,lazada,konvy",
                         help="Comma-separated subset to run, e.g. --platforms konvy")
    parser.add_argument("--categories", default="",
                         help="Comma-separated subset of category slugs, e.g. --categories shampoo (default: all)")
    args = parser.parse_args()

    platforms = [p.strip() for p in args.platforms.split(",") if p.strip()]
    category_slugs = [c.strip() for c in args.categories.split(",") if c.strip()] or None

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main(platforms=platforms, category_slugs=category_slugs))
