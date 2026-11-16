"""
scrape_target_skus.py — Scrape product detail + reviews for a pre-filtered
"target SKUs" file produced by the drill-down/branding pipeline
(e.g. dd_target_skus_YYYYMMDD_REVISED.xlsx).

The file is expected to have one sheet per platform (shopee / lazada / konvy),
each with boolean columns:
    selected_for_detail  — True  → scrape product_url for spec/description
    selected_for_review  — True  → scrape product_url (or product_id for
                                    Konvy) for customer reviews/comments

For each platform, the freshly scraped columns are merged back onto the
original selection-metadata row (brand, tier, canonical group, scores, …)
and saved as ONE Excel file per platform under ./target_skus_output —
{platform}_target_combined_TIMESTAMP.xlsx — with two sheets:
"product_detail" and "reviews_comments".

USAGE
------
python scripts/scrape_target_skus.py "D:\\path\\to\\dd_target_skus_20260721_REVISED.xlsx"
"""

import asyncio
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# Let `python scripts/<name>.py` (run from the project root) import scrapers/
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scrapers import cloak_shopee_AI_bot as shopee
from scrapers import cloak_lazada         as lazada
from scrapers import cloak_konvy          as konvy

# This script is meant to be re-run whenever the target-SKU file changes
# (e.g. a brand added/removed) — force resume-from-temp-backup for every
# platform so a re-run skips products already scraped in a prior run
# instead of overwriting their temp checkpoint and re-scraping from
# scratch. Set here (not in the cloak_*.py files) so it only affects this
# script, not scrap_agent.py's normal interactive runs.
shopee.RESUME_FROM_BACKUP = True
lazada.RESUME_FROM_BACKUP = True
konvy.RESUME_FROM_BACKUP  = True

OUTPUT_DIR = Path("./data/target_skus_output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

PLATFORM_SHEETS = ["shopee", "lazada", "konvy"]

# Cap on review pages scraped per product. None = no cap.
LAZADA_REVIEW_PAGE_CAP = 10  # 50 reviews/page
KONVY_REVIEW_PAGE_CAP  = 10  # 10 reviews/page


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


def _save(df: pd.DataFrame, name: str) -> None:
    if df is None or df.empty:
        print(f"⚠️  Nothing to save for {name} (empty result).")
        return
    df = _sanitize_for_excel(df)
    path = OUTPUT_DIR / f"{name}_{_timestamp()}.xlsx"
    df.to_excel(path, index=False)
    print(f"💾 Saved: {path}  ({len(df):,} rows)")


def _save_combined(platform: str, category: str, detail_df: pd.DataFrame, review_df: pd.DataFrame) -> None:
    """Two separate .xlsx files per platform — detail and reviews/comments —
    instead of one combined file with two sheets, named
    {platform}_detail_{category}_TIMESTAMP.xlsx /
    {platform}_reviews_{category}_TIMESTAMP.xlsx."""
    has_detail = detail_df is not None and not detail_df.empty
    has_review = review_df is not None and not review_df.empty
    if not has_detail and not has_review:
        print(f"⚠️  Nothing to save for {platform} (both detail and reviews empty).")
        return
    ts = _timestamp()
    if has_detail:
        detail_df = _sanitize_for_excel(detail_df)
        path = OUTPUT_DIR / f"{platform}_detail_{category}_{ts}.xlsx"
        detail_df.to_excel(path, index=False)
        print(f"💾 Saved: {path}  (detail={len(detail_df):,} rows)")
    if has_review:
        review_df = _sanitize_for_excel(review_df)
        path = OUTPUT_DIR / f"{platform}_reviews_{category}_{ts}.xlsx"
        review_df.to_excel(path, index=False)
        print(f"💾 Saved: {path}  (reviews={len(review_df):,} rows)")


def _merge_scraped(scraped: pd.DataFrame, meta: pd.DataFrame, left_on: str, right_on: str) -> pd.DataFrame:
    """Left-join scraped results onto their selection metadata, with META as
    the left side of the join. Scrapers with RESUME_FROM_BACKUP=True return
    their ENTIRE accumulated backup — every product ever scraped in any past
    run of that module, not just the ones requested this call — so joining
    scraped.merge(meta) would leak unrelated historical products (and, for
    platforms with no RESUME_FROM_BACKUP, an empty-columns DataFrame()
    raises KeyError on the join column). Meta-as-left keeps the result
    scoped to exactly the current target selection, with 1 metadata row
    expanding to N rows per matching scraped review/detail.
    `left_on`/`right_on` name the join column in `scraped`/`meta` respectively."""
    if scraped is None or scraped.empty:
        return pd.DataFrame()
    return meta.merge(scraped, left_on=right_on, right_on=left_on, how="left", suffixes=("_meta", ""))


def _existing_review_coverage(platform: str, url_col: str, content_col: str) -> set:
    """Union of product ids/urls that already have real review data across
    ALL historical output for this platform — old separate
    *_target_reviews*.xlsx files and *_target_combined_*.xlsx
    (reviews_comments sheet) from before the per-category naming format,
    plus every *_reviews_<category>_*.xlsx since. Deliberately unions every
    file instead of trusting just the most recently modified one: a later
    run that only retried a handful of products (a small partial file)
    would otherwise shadow an earlier, more complete result and make
    already-scraped products look missing again."""
    have = set()
    for f in OUTPUT_DIR.glob(f"{platform}_target_reviews*.xlsx"):
        try:
            prev = pd.read_excel(f)
            if url_col in prev.columns and content_col in prev.columns:
                have |= set(prev.loc[prev[content_col].notna(), url_col].astype(str))
        except Exception as e:
            print(f"   ⚠️  Could not read {f.name}: {e}")
    for f in OUTPUT_DIR.glob(f"{platform}_target_combined_*.xlsx"):
        try:
            prev = pd.read_excel(f, sheet_name="reviews_comments")
            if url_col in prev.columns and content_col in prev.columns:
                have |= set(prev.loc[prev[content_col].notna(), url_col].astype(str))
        except Exception:
            pass  # file may have no reviews_comments sheet yet
    for f in OUTPUT_DIR.glob(f"{platform}_reviews_*.xlsx"):
        try:
            prev = pd.read_excel(f)
            if url_col in prev.columns and content_col in prev.columns:
                have |= set(prev.loc[prev[content_col].notna(), url_col].astype(str))
        except Exception as e:
            print(f"   ⚠️  Could not read {f.name}: {e}")
    return have


def _filter_missing_reviews(review_meta: pd.DataFrame, platform: str, url_col: str, content_col: str) -> pd.DataFrame:
    """Drop rows that already have real review data in any prior saved
    output for this platform (see _existing_review_coverage). Used for
    Lazada/Konvy, whose scrapers — unlike Shopee's — don't skip products
    they already fully reviewed in an earlier run on their own."""
    have_data = _existing_review_coverage(platform, url_col, content_col)
    missing = review_meta[~review_meta[url_col].astype(str).isin(have_data)]
    print(f"   ℹ️  {len(have_data)} product(s) already have reviews on disk — {len(missing)}/{len(review_meta)} still missing.")
    return missing


async def _run_shopee(df: pd.DataFrame, category: str, skip_detail=False, skip_review=False) -> None:
    detail_out = pd.DataFrame()
    review_out = pd.DataFrame()

    detail_meta = df[df["selected_for_detail"] == True]
    if not skip_detail and not detail_meta.empty:
        urls = detail_meta["product_url"].dropna().unique().tolist()
        print(f"\n🟧 Shopee — Product Detail ({len(urls)} URL(s))")
        detail = await shopee.run_product_detail(urls)
        detail_out = _merge_scraped(detail, detail_meta, "product_url", "product_url")

    review_meta = df[df["selected_for_review"] == True]
    # Unlike Lazada/Konvy, Shopee's own run_product_reviews_with_comments
    # already skips URLs it fully scraped in a previous run (done_urls
    # check keyed off temp_shopee_reviews_comments) — no extra
    # missing-only filtering needed here.
    if not skip_review and not review_meta.empty:
        urls = review_meta["product_url"].dropna().unique().tolist()
        print(f"\n🟧 Shopee — Product Reviews ({len(urls)} URL(s))")
        reviews = await shopee.run_product_reviews_with_comments(urls)

        # run_product_reviews_with_comments hard-caps at Shopee's own
        # "with comments" UI limit (~3,000 reviews) — anything over that
        # gets marked done with ZERO rows saved for it (not retried on a
        # later run), silently leaving real, obtainable review data
        # uncollected. Any requested URL that still has no rows after this
        # pass — whether from that cap, a genuinely 0-review product, or a
        # dead/delisted URL — gets a second pass through
        # run_product_reviews_by_stars(), which has no such cap (walks
        # every star bucket to its real last page) and is the only way to
        # reach reviews past 3,000. (Delisted/0-review URLs just fail fast
        # here too — the wasted retry is a small, acceptable cost for not
        # silently losing every >3,000-review product.)
        got_urls = set(reviews["product_url"].astype(str)) if not reviews.empty else set()
        missing_urls = [u for u in urls if u not in got_urls]
        if missing_urls:
            print(f"\n🟧 Shopee — Product Reviews (by-stars fallback, {len(missing_urls)} URL(s) with 0 rows from 'with comments')")
            try:
                by_stars = await shopee.run_product_reviews_by_stars(missing_urls)
                if not by_stars.empty:
                    reviews = pd.concat([reviews, by_stars], ignore_index=True) if not reviews.empty else by_stars
            except Exception as e:
                # A single product that runs into an unsolvable captcha (AI
                # solver exhausted its attempts, manual fallback needs a TTY
                # this background run doesn't have) used to take the whole
                # Shopee save down with it — every other product's already-
                # scraped reviews lost along with it. run_product_reviews_by_stars
                # checkpoints its own progress to temp backup as it goes, so
                # falling back to whatever it already saved there and
                # continuing to the final save is strictly better than losing
                # everything.
                print(f"   ⚠️  by-stars fallback failed ({e}) — falling back to its checkpointed backup so this doesn't lose the rest of the run's data.")
                backup_rows = shopee.load_existing_backup("temp_shopee_reviews_by_stars", shopee.REVIEW_DEDUPE_COLS)
                by_stars = pd.DataFrame(backup_rows)
                if not by_stars.empty:
                    reviews = pd.concat([reviews, by_stars], ignore_index=True) if not reviews.empty else by_stars

        review_out = _merge_scraped(reviews, review_meta, "product_url", "product_url")

    _save_combined("shopee", category, detail_out, review_out)


async def _run_lazada(df: pd.DataFrame, category: str, review_cap=LAZADA_REVIEW_PAGE_CAP, skip_detail=False, skip_review=False) -> None:
    detail_out = pd.DataFrame()
    review_out = pd.DataFrame()

    detail_meta = df[df["selected_for_detail"] == True]
    if not skip_detail and not detail_meta.empty:
        print(f"\n🟦 Lazada — Product Detail ({len(detail_meta)} product(s))")
        # lazada.run_product_detail merges the scraped spec/description
        # columns back onto df_product internally (left-join on product_url).
        detail_out = await lazada.run_product_detail(df_product=detail_meta)

    review_meta = df[df["selected_for_review"] == True]
    if not skip_review and not review_meta.empty:
        # Lazada's run_reviews has no built-in "already fully scraped, skip"
        # check — only scrape products still missing real review data, but
        # merge the result against the FULL review_meta (not just the
        # missing subset): run_reviews() returns its ENTIRE accumulated
        # backup regardless of which URLs were requested this call, so this
        # still picks up products that were successfully scraped in an
        # earlier run and weren't part of this call's scrape list.
        to_scrape = _filter_missing_reviews(review_meta, "lazada", "product_url", "comment")
        if not to_scrape.empty:
            urls = to_scrape["product_url"].dropna().unique().tolist()
            print(f"\n🟦 Lazada — Reviews ({len(urls)} URL(s) to scrape, {len(review_meta)} selected total)")
            # Some target products have 10,000+ reviews (247+ API pages each at
            # ~3-5s/page) — capped by default so this finishes in minutes, not
            # the better part of an hour. Pass review_cap=None for full coverage.
            reviews = await lazada.run_reviews(urls, max_pages_per_product=review_cap)
        else:
            # Nothing left to scrape — just read the accumulated backup off
            # disk instead of calling run_reviews([]), which would still
            # launch a whole browser for zero URLs.
            print("\n🟦 Lazada — Reviews: nothing left to scrape, loading accumulated backup for the merge.")
            backup_rows = lazada.load_existing_backup("temp_lazada_reviews", lazada.REVIEW_DEDUPE_COLS)
            reviews = pd.DataFrame(backup_rows)
        # run_reviews() returns the product link under 'comment_url' (not
        # 'product_url' — that column gets dropped by its own output-column
        # filter), so join on that instead.
        review_out = _merge_scraped(reviews, review_meta, "comment_url", "product_url")

    _save_combined("lazada", category, detail_out, review_out)


async def _run_konvy(df: pd.DataFrame, category: str, review_cap=KONVY_REVIEW_PAGE_CAP, skip_detail=False, skip_review=False) -> None:
    detail_out = pd.DataFrame()
    review_out = pd.DataFrame()

    detail_meta = df[df["selected_for_detail"] == True]
    if not skip_detail and not detail_meta.empty:
        urls = detail_meta["product_url"].dropna().unique().tolist()
        print(f"\n🩷 Konvy — Product Detail ({len(urls)} URL(s))")
        detail = await konvy.run_product_detail(product_urls=urls)
        detail_out = _merge_scraped(detail, detail_meta, "product_url", "product_url")

    review_meta = df[df["selected_for_review"] == True].copy()
    if not review_meta.empty:
        review_meta["product_id"] = review_meta["product_id"].astype(str)
    if not skip_review and not review_meta.empty:
        # Same reasoning as Lazada above: konvy.run_product_reviews has no
        # per-product skip-if-done check, so only feed it products still
        # missing data, but merge against the FULL review_meta since it
        # returns its whole accumulated backup regardless of input.
        to_scrape = _filter_missing_reviews(review_meta, "konvy", "product_id", "review_content")
        if not to_scrape.empty:
            print(f"\n🩷 Konvy — Reviews ({len(to_scrape)} product(s) to scrape, {len(review_meta)} selected total)")
            reviews = await konvy.run_product_reviews(df_product=to_scrape, max_pages_per_product=review_cap)
        else:
            # Nothing left to scrape — read the accumulated backup off disk
            # instead of calling run_product_reviews with an empty
            # df_product, which would bail out before even launching a
            # browser but still needs a valid non-empty input to do so.
            print("\n🩷 Konvy — Reviews: nothing left to scrape, loading accumulated backup for the merge.")
            backup_rows = konvy._load_backup("temp_konvy_reviews", konvy.REVIEW_DEDUPE_COLS)
            reviews = pd.DataFrame(backup_rows)
        if not reviews.empty:
            reviews["product_id"] = reviews["product_id"].astype(str)
        review_out = _merge_scraped(reviews, review_meta, "product_id", "product_id")

    _save_combined("konvy", category, detail_out, review_out)


async def main(path: str, category: str = None, platforms=None, lazada_review_cap=LAZADA_REVIEW_PAGE_CAP, konvy_review_cap=KONVY_REVIEW_PAGE_CAP, skip_detail=False, skip_review=False) -> None:
    # Default category = parent folder name of the input file, matching the
    # EF Project's own inputs/raw/exfac/<category>/ layout (e.g.
    # ".../outputs/exfac/shower_cream/dd_target_skus_*.xlsx" -> "shower_cream").
    category = category or Path(path).parent.name
    platforms = platforms or PLATFORM_SHEETS
    xl = pd.ExcelFile(path)
    for platform in platforms:
        if platform not in xl.sheet_names:
            print(f"⚠️  Sheet '{platform}' not found in file — skipping.")
            continue
        pf_df = xl.parse(platform)
        if "selected_for_detail" not in pf_df.columns or "selected_for_review" not in pf_df.columns:
            print(f"⚠️  Sheet '{platform}' missing selected_for_detail/selected_for_review — skipping.")
            continue

        n_detail = int((pf_df["selected_for_detail"] == True).sum())
        n_review = int((pf_df["selected_for_review"] == True).sum())
        print(f"\n{'='*60}\n{platform.upper()}  — detail: {n_detail}, review: {n_review}\n{'='*60}")
        if n_detail == 0 and n_review == 0:
            continue

        try:
            if platform == "shopee":
                await _run_shopee(pf_df, category, skip_detail=skip_detail, skip_review=skip_review)
            elif platform == "lazada":
                await _run_lazada(pf_df, category, review_cap=lazada_review_cap, skip_detail=skip_detail, skip_review=skip_review)
            elif platform == "konvy":
                await _run_konvy(pf_df, category, review_cap=konvy_review_cap, skip_detail=skip_detail, skip_review=skip_review)
        except Exception as e:
            # Don't let one platform's failure take the rest of the run down
            # with it — e.g. a crash mid-Shopee previously meant Lazada and
            # Konvy silently never ran at all.
            print(f"❌ {platform.upper()} failed: {e}")

    print("\n✅ All done.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("path", nargs="?", help="Path to target SKUs xlsx")
    parser.add_argument("--category", default=None,
                         help="Category tag for output filenames, e.g. --category shower_cream "
                              "(default: input file's parent folder name)")
    parser.add_argument("--platforms", default="shopee,lazada,konvy",
                         help="Comma-separated subset to run, e.g. --platforms konvy")
    parser.add_argument("--no-review-cap", action="store_true",
                         help="Scrape every review page for every selected product (no sampling cap)")
    parser.add_argument("--skip-detail", action="store_true", help="Don't scrape product detail, only reviews.")
    parser.add_argument("--skip-review", action="store_true", help="Don't scrape reviews, only product detail.")
    args = parser.parse_args()

    file_path = args.path or input("Path to target SKUs xlsx: ").strip().strip('"').strip("'")
    platforms = [p.strip() for p in args.platforms.split(",") if p.strip()]
    lazada_cap = None if args.no_review_cap else LAZADA_REVIEW_PAGE_CAP
    konvy_cap  = None if args.no_review_cap else KONVY_REVIEW_PAGE_CAP

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main(
        file_path, category=args.category, platforms=platforms, lazada_review_cap=lazada_cap, konvy_review_cap=konvy_cap,
        skip_detail=args.skip_detail, skip_review=args.skip_review,
    ))
