"""
dump_konvy_pages.py — save what Konvy's brand pages really look like (run by the USER).

Konvy's robots.txt keeps ClaudeBot out, so Claude can't open konvy.com to check
the all-brands mode. Run this once yourself; it saves HTML + a screenshot of:
  1. the brand index  (cloak_konvy.BRAND_INDEX_URL)
  2. a few brand pages — the first links the index parser finds, or --brand-url

into data/konvy/log/  and prints a short report (brands parsed, paging links,
product tiles, anything that looks like a "N products" count). Send / point
Claude at those files; reading local files is fine.

    python scripts/dump_konvy_pages.py
    python scripts/dump_konvy_pages.py --brand-url https://www.konvy.com/brand/mistine-2 --brands 0
"""
import argparse
import asyncio
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))   # project root (run from there too: ./data paths)

from scrapers import cloak_konvy as k  # noqa: E402


async def _scroll(page, rounds: int = 6) -> None:
    for _ in range(rounds):
        try:
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        except Exception:
            return
        await asyncio.sleep(1.0)


async def _report_brand_page(page, url: str, n: int) -> None:
    await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
    await k._accept_cookie(page)
    await asyncio.sleep(3)
    await _scroll(page)
    tiles = await page.evaluate("document.querySelectorAll('p.product-name').length")
    paging = await page.eval_on_selector_all(
        "div.sc_paging a",
        "els => els.map(e => [e.innerText.trim(), e.getAttribute('href')]).filter(p => p[0])",
    )
    print(f"   final URL: {page.url}")
    body_text = await page.evaluate("document.body.innerText")
    counts = re.findall(r"[^\n]{0,30}\d[\d,]*\s*(?:items?|products?|สินค้า|รายการ|ชิ้น)[^\n]{0,20}", body_text, re.I)
    print(f"   product tiles (p.product-name): {tiles}")
    print(f"   paging links (div.sc_paging a): {paging[:20]}")
    print(f"   count-like text: {counts[:5]}")
    await k._dump_page_debug(page, f"brand_page{n}")   # numbered: the file name only has minute precision


async def main(args) -> None:
    context, page = await k._create_browser_context(block_images=False)
    async with context:
        print(f"🌐 Brand index: {k.BRAND_INDEX_URL}")
        await page.goto(k.BRAND_INDEX_URL, wait_until="domcontentloaded", timeout=60_000)
        await k._accept_cookie(page)
        await asyncio.sleep(3)
        await _scroll(page, 10)
        anchors = await page.eval_on_selector_all("a[href]", k.ANCHORS_JS)
        entries = k._parse_brand_links(anchors)
        print(f"   {len(anchors)} links on the page, {len(entries)} brand(s) parsed "
              f"(the all-brands mode needs ≥ {k.BRAND_LIST_MIN_BRANDS})")
        for name, slug, url in entries[:10]:
            print(f"     {name!r:30} {slug:25} {url}")
        await k._dump_page_debug(page, "brand_index")

        urls = list(args.brand_url) + [u for _, _, u in entries[: args.brands]]
        for n, url in enumerate(urls, 1):
            print(f"\n🏷️  Brand page {n}: {url}")
            try:
                await _report_brand_page(page, url, n)
            except Exception as exc:
                print(f"   ❌ {exc}")
            await asyncio.sleep(3)

    print(f"\n✅ Done — files are in {k.LOG_DIR}/  (…_debug_*.html / .png)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--brand-url", action="append", default=[], help="a brand page URL to dump (repeatable)")
    ap.add_argument("--brands", type=int, default=2, help="also dump this many brand pages found on the index (default 2)")
    asyncio.run(main(ap.parse_args()))
