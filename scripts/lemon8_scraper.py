"""
Lemon8 post scraper.

Extracts post content, stats, and comments from a Lemon8 post URL for
market-research purposes (e.g. monitoring reviews of your own products).

Usage:
    python scripts/lemon8_scraper.py <url> [<url> ...] [--out results.json] [--csv reviews.csv]

Example:
    python scripts/lemon8_scraper.py "https://www.lemon8-app.com/@sktl.som/7416339209004466689?region=th"

How it works:
    Lemon8's post pages are server-rendered by Remix. The full post data
    (title, HTML content, stats, comments) is embedded in the page as a
    URL-encoded JSON blob inside:
        <script type="application/json" data-ttark="__remixContext">...</script>
    This script fetches the page, decodes that blob, and pulls out the
    fields under the "$ArticleDetail+<id>", "$UserDetail+<userId>", and
    "$ArticleComments+<id>" keys.

Notes:
    - Only paginates comments that Lemon8 already serves in the initial
      page load (`hasMore`/`cursor` are reported but not followed via a
      private API, since that endpoint isn't public and may require auth).
    - Respect Lemon8's Terms of Service and robots.txt, and keep request
      volume reasonable (built-in delay between URLs).
"""

import argparse
import csv
import html
import json
import re
import sys
import time
import urllib.parse
import urllib.request

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

REMIX_CONTEXT_RE = re.compile(
    r'<script type="application/json" data-ttark="__remixContext">([^<]+)</script>'
)


def fetch_html(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=20) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def strip_html_tags(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def parse_post(url: str) -> dict:
    page_html = fetch_html(url)
    m = REMIX_CONTEXT_RE.search(page_html)
    if not m:
        raise ValueError(f"Could not find embedded post data on page: {url}")

    decoded = urllib.parse.unquote(m.group(1))
    data = json.loads(decoded)

    loader_data = data["state"]["loaderData"]
    route_key = next(
        (k for k in loader_data if k.startswith("routes/$user_link_name_")), None
    )
    if route_key is None:
        raise ValueError(f"Unexpected page structure (not a post page?): {url}")

    route = loader_data[route_key]

    article_key = next(k for k in route if k.startswith("$ArticleDetail+"))
    article = route[article_key]

    user_key = next((k for k in route if k.startswith("$UserDetail+")), None)
    author = route.get(user_key, {}) if user_key else {}

    comments_key = next((k for k in route if k.startswith("$ArticleComments+")), None)
    comments_block = route.get(comments_key, {}) if comments_key else {}
    comments = []
    for c in comments_block.get("items", []):
        comments.append(
            {
                "commentId": c.get("commentId") or c.get("id"),
                "userNickname": (c.get("user") or {}).get("nickName"),
                "content": c.get("content"),
                "diggCount": c.get("diggCount"),
                "createTime": c.get("createTime"),
            }
        )

    return {
        "url": url,
        "title": article.get("title"),
        "content_html": article.get("content"),
        "content_text": strip_html_tags(article.get("content") or ""),
        "author_nickname": author.get("nickName"),
        "author_linkName": author.get("linkName"),
        "publishTime": article.get("publishTime"),
        "readCount": article.get("readCount"),
        "diggCount": article.get("diggCount"),
        "favoriteCount": article.get("favoriteCount"),
        "commentCount": article.get("commentCount"),
        "comments_fetched": len(comments),
        "comments_has_more": comments_block.get("hasMore", False),
        "comments": comments,
    }


def write_csv(results: list, path: str) -> None:
    fieldnames = [
        "url",
        "title",
        "author_nickname",
        "publishTime",
        "readCount",
        "diggCount",
        "favoriteCount",
        "commentCount",
        "content_text",
    ]
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in results:
            writer.writerow(r)


def main():
    parser = argparse.ArgumentParser(description="Scrape Lemon8 post content & comments")
    parser.add_argument("urls", nargs="+", help="Lemon8 post URL(s)")
    parser.add_argument("--out", default=None, help="Write full JSON results to this file")
    parser.add_argument("--csv", default=None, help="Write a flat CSV summary to this file")
    parser.add_argument(
        "--delay", type=float, default=2.0, help="Seconds to wait between requests (default 2)"
    )
    args = parser.parse_args()

    results = []
    for i, url in enumerate(args.urls):
        print(f"[{i + 1}/{len(args.urls)}] Fetching {url}", file=sys.stderr)
        try:
            results.append(parse_post(url))
        except Exception as e:
            print(f"  ERROR: {e}", file=sys.stderr)
        if i < len(args.urls) - 1:
            time.sleep(args.delay)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"Wrote {len(results)} posts to {args.out}", file=sys.stderr)

    if args.csv:
        write_csv(results, args.csv)
        print(f"Wrote CSV summary to {args.csv}", file=sys.stderr)

    if not args.out and not args.csv:
        print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
