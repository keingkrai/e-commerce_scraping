"""
scrape_lemon8_search.py — Scrape Lemon8 post content (the "review") + comments
for a keyword, given a pre-discovered list of post URLs.

Lemon8's own keyword-search backend (/search/general/<keyword>) rejects
direct HTTP/browser requests with a 502 (confirmed via both curl and a real
Playwright browser — it's gated the same way TikTok's other internal APIs
are), and free search engines (DuckDuckGo, Bing) block automated queries
without an API key. So unlike scrape_brand_search.py (which calls each
platform's own search API directly), this script does NOT search Lemon8
itself — it expects the caller to already have a list of candidate post
URLs for the keyword (e.g. found via a web search for
site:lemon8-app.com "<keyword>"), and only handles extraction + saving.

Each URL's post page embeds its full data (title, content, stats, comments)
in a Remix loader payload server-side — same technique as lemon8_scraper.py,
reused here via its parse_post().

Output: ONE .xlsx per keyword under ./lemon8_search_output/
    lemon8_search_{keyword_slug}_{timestamp}.xlsx
with two sheets:
    "posts"    — one row per post (title, review content, author, stats,
                 publish datetime, scraped_at) — the product-detail-style sheet
    "comments" — one row per comment across all posts for this keyword

USAGE
------
python scripts/scrape_lemon8_search.py --keyword "ครีมอาบน้ำ Exfac" --urls url1 url2 url3 ...
python scripts/scrape_lemon8_search.py --keyword "ครีมอาบน้ำ Exfac" --urls-file urls.txt
"""

import argparse
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from lemon8_scraper import parse_post

OUTPUT_DIR = Path("./data/lemon8_search_output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

POST_ID_RE = re.compile(r"/@([^/]+)/(\d+)")


def _timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _slugify(keyword: str) -> str:
    slug = re.sub(r"[^\w]+", "_", keyword.strip(), flags=re.UNICODE).strip("_")
    return slug or "keyword"


def _epoch_to_iso(ts) -> str:
    if not ts:
        return ""
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return ""


def scrape_keyword(keyword: str, urls: list, delay: float = 2.0) -> tuple:
    post_rows = []
    comment_rows = []
    seen_ids = set()

    for i, url in enumerate(urls):
        m = POST_ID_RE.search(url)
        post_id = m.group(2) if m else None
        if post_id and post_id in seen_ids:
            continue  # same post surfaced twice by the search results
        if post_id:
            seen_ids.add(post_id)

        print(f"[{i + 1}/{len(urls)}] Fetching {url}")
        scraped_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            post = parse_post(url)
        except Exception as e:
            print(f"   ERROR: {e}")
            post_rows.append({
                "keyword": keyword,
                "url": url,
                "post_id": post_id,
                "scraped_at": scraped_at,
                "error": str(e),
            })
            if i < len(urls) - 1:
                time.sleep(delay)
            continue

        post_rows.append({
            "keyword": keyword,
            "url": post["url"],
            "post_id": post_id,
            "title": post["title"],
            "review_content": post["content_text"],
            "author_nickname": post["author_nickname"],
            "author_linkName": post["author_linkName"],
            "publish_datetime": _epoch_to_iso(post["publishTime"]),
            "readCount": post["readCount"],
            "diggCount": post["diggCount"],
            "favoriteCount": post["favoriteCount"],
            "commentCount": post["commentCount"],
            "comments_fetched": post["comments_fetched"],
            "scraped_at": scraped_at,
            "error": "",
        })

        for c in post["comments"]:
            comment_rows.append({
                "keyword": keyword,
                "post_id": post_id,
                "post_url": post["url"],
                "commentId": c["commentId"],
                "userNickname": c["userNickname"],
                "comment_content": c["content"],
                "diggCount": c["diggCount"],
                "comment_datetime": _epoch_to_iso(c["createTime"]),
                "scraped_at": scraped_at,
            })

        if i < len(urls) - 1:
            time.sleep(delay)

    return pd.DataFrame(post_rows), pd.DataFrame(comment_rows)


def save(keyword: str, posts_df: pd.DataFrame, comments_df: pd.DataFrame) -> Path:
    path = OUTPUT_DIR / f"lemon8_search_{_slugify(keyword)}_{_timestamp()}.xlsx"
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        posts_df.to_excel(writer, sheet_name="posts", index=False)
        if not comments_df.empty:
            comments_df.to_excel(writer, sheet_name="comments", index=False)
    print(f"Saved: {path}  (posts={len(posts_df):,} rows, comments={len(comments_df):,} rows)")
    return path


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--keyword", required=True, help="Search keyword this URL list was found for")
    parser.add_argument("--urls", nargs="*", default=[], help="Lemon8 post URL(s)")
    parser.add_argument("--urls-file", default=None, help="Path to a text file with one URL per line")
    parser.add_argument("--delay", type=float, default=2.0, help="Seconds between requests (default 2)")
    args = parser.parse_args()

    urls = list(args.urls)
    if args.urls_file:
        with open(args.urls_file, encoding="utf-8") as f:
            urls += [line.strip() for line in f if line.strip()]

    if not urls:
        parser.error("No URLs given — pass --urls or --urls-file")

    posts_df, comments_df = scrape_keyword(args.keyword, urls, delay=args.delay)
    save(args.keyword, posts_df, comments_df)


if __name__ == "__main__":
    main()
