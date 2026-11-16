"""
scrape_brand_search.py — Product-overview (search by keyword) scrape across
Shopee, Lazada, and Konvy for a fixed list of brand keywords (Thai + English
name per brand).

For each brand, each platform is searched with BOTH the English and Thai
name in one call (so results are tagged by which keyword matched), and
saved as ONE .xlsx per (platform, brand) — no reviews/comments. Columns
match the EF Project's own reference files under
inputs/raw/exfac/shower_cream/: shopee_search_Shokubutsu.xlsx (itemid,
shopid, name, brand, price, sold, rating_star, rating_1star..5star, …),
lazada_search_Shokubutsu.xlsx (product_id, sku_id, discount_price, …), and
konvy_search_BeNice.xlsx (scraped_date, product_id, sale_price, category,
sub_category, leaf_category, stock, other_attributes, …) — Konvy's schema
needs a full per-product detail-page visit to fill in (product_id and
other_attributes in particular aren't on the listing page at all), so
unlike Shopee/Lazada this one isn't a lightweight search-API-only scrape.

Output: N brands x 3 platforms files under ./brand_search_output/
    {platform}_search_{brand_slug}_TIMESTAMP.xlsx

USAGE
------
python scripts/scrape_brand_search.py
python scripts/scrape_brand_search.py --platforms shopee,lazada
python scripts/scrape_brand_search.py --brands dove,sunsilk
"""

import asyncio
import re
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

# Resume-from-backup so an interrupted run of this brand list can pick back
# up instead of re-scraping brands already saved to temp checkpoints.
shopee.RESUME_FROM_BACKUP = True
lazada.RESUME_FROM_BACKUP = True
konvy.RESUME_FROM_BACKUP  = True

OUTPUT_DIR = Path("./data/brand_search_output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# (slug, english_name, thai_name) — slug used only for the output filename.
BRANDS = [
    ("dove",         "Dove",           "โดฟ"),
    ("watsons",      "Watsons",        "วัตสัน"),
    ("feather",      "Feather",        "แฟซ่า"),
    ("carebeau",     "Carebeau",       "แคร์บิว"),
    ("boya",         "Boya",           "โบย่า"),
    ("jmella",       "Jmella",         "เจเมล่า"),
    ("cokki",        "cokki",          "ค๊อกกี้"),
    ("sunsilk",      "Sunsilk",        "ซันซิล"),
    ("loreal_paris", "L'Oreal Paris",  "ลอรีอัล ปารีส"),
    ("boots",        "Boots",          "บู๊ทส์"),
    ("shokubutsu",   "Shokubutsu",     "โชกุบุสซึ"),
    ("benice",       "BeNice",         "บีไนซ์"),
    ("skat",         "SKAT",           "SKAT"),
    ("vaseline",     "Vaseline",       "วาสลีน"),
    ("jabs",         "Jabs",           "แจ๊บส์"),
    ("civic",        "Civic",          "ซีวิค"),
    ("a_bonne",      "A BONNE'",       "เอบอนเน่"),
    ("plantae",      "Plantae",        "แพลนเต้"),
    ("in2it",        "IN2IT",          "อินทูอิท"),
    ("srichand",     "Srichand",       "ศรีจันทร์"),
    ("sasi",         "Sasi",           "ศศิ"),
    ("revlon",       "Revlon",         "เรฟลอน"),
    ("charmiss",     "Charmiss",       "ชาร์มิส"),
    ("cathy_doll",   "Cathy Doll",     "เคที่ดอลล์"),
    ("4u2",          "4U2",            "โฟร์ยูทู"),
    ("varivida",     "Varivida",       "วาริวีดา"),
    ("love_potion",  "Love Potion",    "เลิฟโพชั่น"),
    ("odbo",         "ODBO",           "โอดีบีโอ"),
]

# Konvy-only keyword overrides. Confirmed live: Konvy's listing search
# silently breaks on an apostrophe+space combo and falls back to paging
# through its entire catalog (searching "L'Oreal Paris" returned 218
# pages / ~7,000 unrelated products, vs. 6 sane pages for "Loreal Paris"
# without the apostrophe — Shopee/Lazada's own search handles the
# apostrophe fine, so this override is Konvy-specific only).
KONVY_KEYWORD_OVERRIDES = {
    "loreal_paris": ("Loreal Paris", "ลอรีอัล ปารีส"),
    "a_bonne":      ("Abonne",       "เอบอนเน่"),
}

# Anomalously high Konvy page counts (way beyond any genuine brand-title
# match seen across this brand list) are the same "broken query fell back
# to the whole catalog" failure mode as the apostrophe bug above — bail
# out on a keyword rather than silently saving thousands of unrelated
# rows mislabeled as this brand's data.
KONVY_MAX_SANE_PAGES = 40


def _timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _sanitize_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    """Strip control characters openpyxl refuses to write into a cell
    (raises IllegalCharacterError otherwise). Seen live: a scraped Lazada
    shop name containing a literal backspace char (\\x08) embedded in the
    text — a real artifact of the source data, not a scraping bug — which
    aborted the whole .xlsx save with no file written."""
    from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
    str_cols = df.select_dtypes(include="object").columns
    for col in str_cols:
        df[col] = df[col].map(lambda v: ILLEGAL_CHARACTERS_RE.sub("", v) if isinstance(v, str) else v)
    return df


def _save(df: pd.DataFrame, platform: str, brand_slug: str) -> None:
    if df is None or df.empty:
        print(f"⚠️  {platform}/{brand_slug}: nothing to save (empty result).")
        return
    df = _sanitize_for_excel(df)
    path = OUTPUT_DIR / f"{platform}_search_{brand_slug}_{_timestamp()}.xlsx"
    df.to_excel(path, index=False)
    print(f"💾 Saved: {path}  ({len(df):,} rows)")


async def _run_shopee_brands(brands) -> None:
    for slug, en, th in brands:
        print(f"\n{'='*60}\n🟧 SHOPEE — {en} / {th}\n{'='*60}")
        try:
            # run_search_keyword already returns overview-only rows (search
            # API response — no product-detail-page visit) and already
            # filters its return value down to just this call's keywords.
            df = await shopee.run_search_keyword([en, th], max_pages=None)
            _save(df, "shopee", slug)
        except Exception as e:
            print(f"❌ Shopee {en}/{th} failed: {e}")


async def _run_lazada_brands(brands) -> None:
    for slug, en, th in brands:
        print(f"\n{'='*60}\n🟦 LAZADA — {en} / {th}\n{'='*60}")
        try:
            # Same story as Shopee: search-API overview rows only, already
            # filtered to this call's keywords.
            df = await lazada.run_search_keyword([en, th])
            _save(df, "lazada", slug)
        except Exception as e:
            print(f"❌ Lazada {en}/{th} failed: {e}")


async def _konvy_page_count(page, keyword) -> int:
    """Cheap precheck of how many listing pages a keyword has, without
    scraping any of them — used to catch the broken-query/whole-catalog
    fallback before wasting time (and mislabeling data) on it."""
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


async def _run_konvy_brands(brands) -> None:
    """Full per-product detail scrape (product_id, price, category, stock,
    sold, rating, other_attributes, …) — matches the 20-column schema the
    EF Project's dd_sku_tagging.py pipeline expects (confirmed against its
    own reference file, inputs/raw/exfac/shower_cream/konvy_search_BeNice.xlsx).
    A listing-only pass (title/url/image, no detail visit) was tried first
    per an earlier "overview only" instruction, but that schema turned out
    too thin for the downstream enrichment join — this inlines the same
    collect-then-detail-scrape flow as konvy.run_search_keyword() (one
    shared browser context across all brands/keywords, instead of that
    function's own fresh-context-per-call), while keeping the page-count
    guard and keyword overrides from the listing-only version, and — unlike
    konvy.run_search_keyword(), which returns its whole cumulative backup
    regardless of what was requested — building each brand's own rows list
    directly so the saved file only ever contains this brand's own data."""
    print(f"\n📂 [SESSION] Loading: './data/konvy/browser_session'")
    context, page = await konvy._create_browser_context(block_images=False)
    cookie_dismissed = False
    async with context:
        for slug, en, th in brands:
            print(f"\n{'='*60}\n🩷 KONVY — {en} / {th}\n{'='*60}")
            k_en, k_th = KONVY_KEYWORD_OVERRIDES.get(slug, (en, th))
            if (k_en, k_th) != (en, th):
                print(f"   ℹ️  Konvy override: '{en}'/'{th}' -> '{k_en}'/'{k_th}'")
            rows = []
            for kw in (k_en, k_th):
                try:
                    n_pages = await _konvy_page_count(page, kw)
                    if n_pages > KONVY_MAX_SANE_PAGES:
                        print(f"   ⚠️  '{kw}' -> {n_pages} pages (> {KONVY_MAX_SANE_PAGES}) — looks like Konvy's "
                              f"broken-query catalog-dump fallback, not a genuine brand match. Skipping.")
                        continue
                    print(f"🔑 Keyword: '{kw}'")
                    links, cookie_dismissed = await konvy._collect_links_from_url(page, kw, cookie_dismissed)
                    print(f"   ✅ {len(links)} listing(s) found for '{kw}' — fetching detail for each…")
                    for i, item in enumerate(links, 1):
                        print(f"   ⏳ [{kw}] {i}/{len(links)} → {item['product_url']}")
                        row = await konvy._scrape_product_detail(page, item)
                        if row:
                            rows.append(row)
                        await konvy._human_delay()
                except Exception as e:
                    print(f"❌ Konvy keyword '{kw}' failed: {e}")
            df = pd.DataFrame(rows)
            if not df.empty:
                df = df.drop_duplicates(subset=["product_url"])
            _save(df, "konvy", slug)


async def main(platforms=None, brand_slugs=None) -> None:
    platforms = platforms or ["shopee", "lazada", "konvy"]
    brands = BRANDS if not brand_slugs else [b for b in BRANDS if b[0] in brand_slugs]
    if not brands:
        print("⚠️  No matching brands for --brands filter.")
        return

    tasks = []
    if "shopee" in platforms:
        tasks.append(_run_shopee_brands(brands))
    if "lazada" in platforms:
        tasks.append(_run_lazada_brands(brands))
    if "konvy" in platforms:
        tasks.append(_run_konvy_brands(brands))

    # Each platform uses its own persistent browser profile dir, so running
    # them concurrently is safe and cuts total wall-clock time roughly 3x.
    await asyncio.gather(*tasks)
    print("\n✅ All brand searches done.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--platforms", default="shopee,lazada,konvy",
                         help="Comma-separated subset to run, e.g. --platforms konvy")
    parser.add_argument("--brands", default="",
                         help="Comma-separated subset of brand slugs, e.g. --brands dove,sunsilk (default: all)")
    args = parser.parse_args()

    platforms = [p.strip() for p in args.platforms.split(",") if p.strip()]
    brand_slugs = [b.strip() for b in args.brands.split(",") if b.strip()] or None

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main(platforms=platforms, brand_slugs=brand_slugs))
