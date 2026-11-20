"""
cloak_tiktok_hashtags.py
────────────────────────────────────────────────────────────────
TikTok trending hashtags (Creative Center → Trends → Hashtag, now served by
TikTok One at ads.tiktok.com/creative/creativeCenter/trends/hashtag). The top
100 hashtags for a country / period, optionally per industry.

Things specific to this page, confirmed on the live page (2026-11-20, logged in):
  • Same login as Top Ads: this module uses the Top Ads browser profile, so
    action 0 of the TikTok menu covers both.
  • The list comes from POST /CreativeOne/KnowledgeAPI/GetHashtagList with a
    JSON body {"timeRange": 30, "countryCode": "TH", "industryID": "…",
    "page": N, "limit": 20}; the answer is {items: […], pagination:
    {hasMore, page, limit, totalCount: 100}}. This module never calls it: it
    reads the answers the page itself received (passive).
  • Page 1 is rendered into the HTML on the server, so it never shows up as a
    request on a plain load. Picking a period in the page's dropdown that
    differs from the one in the URL makes the page fetch page 1 (switching back
    to a period or industry already shown uses its cache and fetches nothing).
    So every list is opened with ANOTHER period in the URL, then the wanted
    period — and the industry, if any — is picked in the dropdowns.
  • The URL accepts region= and period= (7, 30 or 90), not the industry.
  • Pages 2–5 load when the page is scrolled up a little and then to the
    bottom (one page per scroll). The page must be drawn on screen for that.
  • Industry ids are the Top Ads ones; the dropdown lists the 15 top-level
    industries below (same order as the page's filter config).
  • Per hashtag: rank (position on the page, 1–100), TikTok's own rankIndex
    (which skips numbers, so it is kept as rank_index_site), name, industries, posts (publishCnt), views (vv), a
    daily popularity curve (0–100, one point per day), and the top 3 creators
    (handle, nickname, followers). Avatar URLs and internal user ids are not
    kept. The page's embedded data also holds the signed-in account's info —
    this module never reads it.

Public entry-point:
  run_trending_hashtags(country, periods, industries)  →  pd.DataFrame  one row per hashtag
"""

import asyncio
import json
import random
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlencode, urlparse

import pandas as pd

from scrapers.cloak_tiktok_topads import (
    DEFAULT_COUNTRY, TZ_THAI, _RunStopped, _collected_at, _create_browser_context,
    _dismiss_popups, _make_timestamp, _sanitize_for_excel,
)

# ════════════════════════════════════════════════════════════════
# Directories & Config
# ════════════════════════════════════════════════════════════════

OUTPUT_DIR = "./data/tiktok_hashtags/collected_data"
TEMP_DIR   = "./data/tiktok_hashtags/collected_data/temp"
LOG_DIR    = "./data/tiktok_hashtags/log"

PAGE_URL      = "https://ads.tiktok.com/creative/creativeCenter/trends/hashtag"
LIST_API_PATH = "/CreativeOne/KnowledgeAPI/GetHashtagList"

PERIODS        = (7, 30, 90)
DEFAULT_PERIOD = 30

# Top-level industries in the page's dropdown (ids from its filter config, in
# the same order as the names shown).
INDUSTRIES: Dict[str, str] = {
    "10000000000": "Education",
    "11000000000": "Vehicle & Transportation",
    "12000000000": "Baby, Kids & Maternity",
    "14000000000": "Beauty & Personal Care",
    "15000000000": "Tech & Electronics",
    "17000000000": "Travel",
    "18000000000": "Household Products",
    "19000000000": "Pets",
    "21000000000": "Home Improvement",
    "22000000000": "Apparel & Accessories",
    "23000000000": "News & Entertainment",
    "25000000000": "Games",
    "27000000000": "Food & Beverage",
    "28000000000": "Sports & Outdoor",
    "29000000000": "Health",
}
ALL_LABEL = "All"

SCROLL_DELAY_MIN    = 2.5
SCROLL_DELAY_MAX    = 5.0
COMBO_PAUSE_MIN     = 8
COMBO_PAUSE_MAX     = 20
MAX_STALLED_SCROLLS = 4
MAX_SCROLL_ROUNDS   = 15        # 100 hashtags / 20 per page = 5 pages
FIRST_PAGE_WAIT_S   = 20
# The table arrives with the HTML, but the filter bar is a grey placeholder
# until the page's scripts finish (seen live: still loading ~4s in), so wait
# for the dropdowns rather than looking once.
FILTERS_WAIT_S      = 30
MAX_LOGIN_PROMPTS   = 2
PAGE_TIMEOUT_MS     = 60000
CLICK_TIMEOUT_MS    = 10000

DEDUPE_COLS = ["country", "period_days", "industry_filter_id", "hashtag_id"]

# Marks the period or industry dropdown so Playwright can click it for real.
# The two are told apart by the value in their input box ("Last 30 days" /
# "All" or an industry name); the country picker has an empty input.
_JS_MARK_FILTER = r"""({kind, industryNames}) => {
    document.querySelectorAll('[data-cc-ht]').forEach(e => e.removeAttribute('data-cc-ht'));
    const shown = (e) => e.getBoundingClientRect().height > 0;
    const value = (s) => [...s.querySelectorAll('input')].map(i => i.value).join('').trim();
    const sel = [...document.querySelectorAll('.byted-select')].filter(shown).find(s => {
        const v = value(s);
        return kind === 'period' ? /^Last \d+ days$/.test(v) : (v === 'All' || industryNames.includes(v));
    });
    if (!sel) return null;
    (sel.querySelector('.byted-popper-trigger') || sel).setAttribute('data-cc-ht', 'select');
    return value(sel);
}"""
_JS_MARK_OPTION = """(label) => {
    document.querySelectorAll('[data-cc-ht="option"]').forEach(e => e.removeAttribute('data-cc-ht'));
    const panels = [...document.querySelectorAll('.byted-select-popover-panel')]
        .filter(p => p.getBoundingClientRect().height > 0);
    for (const p of panels) {
        const opt = [...p.querySelectorAll('.byted-select-option-inner-wrapper')]
            .find(o => o.textContent.trim() === label);
        if (opt) { opt.setAttribute('data-cc-ht', 'option'); return true; }
    }
    return false;
}"""
_JS_SCROLL_UP = "() => window.scrollBy(0, -800)"
_JS_SCROLL_BOTTOM = "() => window.scrollTo(0, document.documentElement.scrollHeight)"

for _folder in [OUTPUT_DIR, TEMP_DIR, LOG_DIR]:
    Path(_folder).mkdir(parents=True, exist_ok=True)


# ════════════════════════════════════════════════════════════════
# Helpers
# ════════════════════════════════════════════════════════════════
def page_url(country: str, period: int) -> str:
    return f"{PAGE_URL}?{urlencode({'region': country, 'period': period})}"


def period_label(period: int) -> str:
    return f"Last {period} days"


def resolve_industry(text: str) -> Optional[str]:
    """Industry id from an id or an English name (case-insensitive; a unique
    'contains' match also works, e.g. 'beauty')."""
    text = text.strip()
    if text in INDUSTRIES:
        return text
    low = text.lower()
    for iid, name in INDUSTRIES.items():
        if name.lower() == low:
            return iid
    hits = [iid for iid, name in INDUSTRIES.items() if low in name.lower()]
    return hits[0] if len(hits) == 1 else None


def _to_int(value) -> Optional[int]:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _curve_stats(curve: List[dict]) -> dict:
    points = []
    for p in curve or []:
        ts, val = _to_int(p.get("timestamp")), p.get("value")
        if ts is None or not isinstance(val, (int, float)):
            continue
        points.append((datetime.fromtimestamp(ts, TZ_THAI).strftime("%Y-%m-%d"), float(val)))
    if not points:
        return {"trend_peak_date": "", "trend_latest": None, "trend_curve": ""}
    peak = max(points, key=lambda p: p[1])
    return {
        "trend_peak_date": peak[0],
        "trend_latest": round(points[-1][1]),
        "trend_curve": "; ".join(f"{d}={round(v)}" for d, v in points),
    }


def _hashtag_to_row(item: dict, rank: int, country: str, period: int,
                    industry_filter_id: Optional[str]) -> dict:
    name = item.get("hashtagName") or ""
    ids = [str(i) for i in item.get("industryIDs") or []]
    row = {
        "country": country,
        "period_days": period,
        "industry_filter_id": industry_filter_id or "",
        "industry_filter": INDUSTRIES.get(industry_filter_id or "", industry_filter_id or ""),
        "rank": rank,
        "rank_index_site": _to_int(item.get("rankIndex")),
        "hashtag": f"#{name}" if name else "",
        "hashtag_id": str(item.get("hashtagID") or ""),
        "industries": ", ".join(INDUSTRIES.get(i, i) for i in ids),
        "posts": _to_int(item.get("publishCnt")),
        "views": _to_int(item.get("vv")),
        **_curve_stats(item.get("popularityCurve")),
        "hashtag_url": f"https://www.tiktok.com/tag/{name}" if name else "",
    }
    creators = sorted(item.get("topCreators") or [], key=lambda c: _to_int(c.get("creatorRank")) or 99)
    for n in range(1, 4):
        c = creators[n - 1] if len(creators) >= n else {}
        handle = c.get("handleName") or ""
        row[f"creator_{n}_handle"] = f"@{handle}" if handle else ""
        row[f"creator_{n}_nickname"] = c.get("nickname") or ""
        row[f"creator_{n}_followers"] = _to_int(c.get("followedCnt"))
    row["collected_at"] = _collected_at()
    return row


def _save(rows: List[dict], prefix: str, final: bool) -> pd.DataFrame:
    if not rows:
        if final:
            print(f"   ⚠️  No data to save: {prefix}")
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=[c for c in DEDUPE_COLS if c in df.columns])
    if not final:
        path = Path(TEMP_DIR) / f"{prefix}.csv"
        df.to_csv(path, index=False, encoding="utf-8-sig")
        print(f"   💾 Checkpoint: {path}  ({len(df):,} rows)")
        return df
    xlsx = Path(OUTPUT_DIR) / f"{prefix}_{_make_timestamp()}.xlsx"
    _sanitize_for_excel(df).to_excel(xlsx, index=False)
    print("\n" + "=" * 60)
    print(f"✅ FINAL FILE SAVED : {prefix}")
    print(f"📊 Total Rows       : {len(df):,}")
    print(f"💾 XLSX             : {xlsx}")
    print("=" * 60)
    return df


# ════════════════════════════════════════════════════════════════
# Response capture (passive)
# ════════════════════════════════════════════════════════════════
class _HashtagCapture:
    """Keeps the list pages the page itself fetched for the expected
    period / industry. Sends no requests of its own."""

    def __init__(self):
        self._tasks: set = set()
        self.expect(None, None)

    def expect(self, period: Optional[int], industry_id: Optional[str]) -> None:
        """Forget the list; keep only answers for this period + industry
        (period None = keep nothing, e.g. while the page loads)."""
        self.period = period
        self.industry_id = industry_id or ""
        self.items: Dict[str, dict] = {}       # hashtag id → item + _rank
        self.pages: Dict[int, dict] = {}       # page → pagination
        self.errors = 0

    def attach(self, page) -> None:
        page.on("response", lambda response: self._spawn(self._read(response)))

    def _spawn(self, coroutine) -> None:
        task = asyncio.ensure_future(coroutine)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def drain(self) -> None:
        if self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)

    async def _read(self, response) -> None:
        if urlparse(response.url).path != LIST_API_PATH or response.status != 200:
            return
        try:
            body = json.loads(response.request.post_data or "{}")
            payload = await response.json()
        except Exception:
            self.errors += 1
            return
        self.ingest(body, payload)

    # Split out so it can be tested without a browser.
    def ingest(self, body: dict, payload: dict) -> None:
        if self.period is None or not isinstance(body, dict) or not isinstance(payload, dict):
            return
        if (payload.get("BaseResp") or {}).get("StatusCode", 0) != 0:
            return
        if _to_int(body.get("timeRange")) != self.period:
            return
        if str(body.get("industryID") or "") != self.industry_id:
            return
        page_no = _to_int(body.get("page")) or 1
        limit = _to_int(body.get("limit")) or 20
        self.pages[page_no] = payload.get("pagination") or {}
        for idx, item in enumerate(payload.get("items") or []):
            hid = str(item.get("hashtagID") or item.get("hashtagName") or "")
            if not hid or hid in self.items:
                continue
            # The page numbers rows by position (1–100). TikTok's rankIndex
            # skips numbers (seen live: 10, 14, 15, 31, 56 missing in TH/30d),
            # so it is kept separately as rank_index_site.
            self.items[hid] = {**item, "_rank": (page_no - 1) * limit + idx + 1}

    @property
    def has_more(self) -> bool:
        if not self.pages:
            return True
        return bool(self.pages[max(self.pages)].get("hasMore"))

    @property
    def total_count(self) -> Optional[int]:
        if not self.pages:
            return None
        return _to_int(self.pages[max(self.pages)].get("totalCount"))


# ════════════════════════════════════════════════════════════════
# Page actions
# ════════════════════════════════════════════════════════════════
async def _dump_debug(page, label: str) -> None:
    safe = re.sub(r"[^\w-]+", "_", label, flags=re.UNICODE)[:50]
    base = Path(LOG_DIR) / f"{datetime.now(TZ_THAI).strftime('%Y%m%d_%H%M%S')}_{safe}"
    try:
        # Screenshot only: the page's HTML embeds the signed-in account's info.
        await page.screenshot(path=str(base.with_suffix(".png")))
        print(f"   📸 Debug screenshot: {base}.png")
    except Exception as e:
        print(f"   ⚠️  Could not save debug screenshot: {e}")


async def _mark_filter(page, kind: str, wait_s: float = FILTERS_WAIT_S) -> Optional[str]:
    """Mark the period / industry dropdown, waiting for it to finish loading.
    Returns its current value, or None if it never appeared."""
    loop = asyncio.get_running_loop()
    end = loop.time() + wait_s
    while True:
        current = await page.evaluate(_JS_MARK_FILTER,
                                      {"kind": kind, "industryNames": list(INDUSTRIES.values())})
        if current is not None or loop.time() >= end:
            return current
        await asyncio.sleep(1)


async def _pick(page, kind: str, label: str) -> None:
    """Open the period / industry dropdown and click `label` (real clicks)."""
    current = await _mark_filter(page, kind)
    if current is None:
        raise _RunStopped(f"The {kind} dropdown did not appear within {FILTERS_WAIT_S}s "
                          "(page still loading, or its layout changed).")
    await _dismiss_popups(page)
    await page.click('[data-cc-ht="select"]', timeout=CLICK_TIMEOUT_MS)
    await asyncio.sleep(random.uniform(0.6, 1.2))
    if not await page.evaluate(_JS_MARK_OPTION, label):
        raise _RunStopped(f"'{label}' not found in the {kind} dropdown.")
    await page.click('[data-cc-ht="option"]', timeout=CLICK_TIMEOUT_MS)


async def _wait_for_page_one(capture: _HashtagCapture) -> bool:
    loop = asyncio.get_running_loop()
    end = loop.time() + FIRST_PAGE_WAIT_S
    while loop.time() < end:
        await capture.drain()
        if capture.pages:
            return True
        await asyncio.sleep(1)
    return False


async def _open_list(page, capture: _HashtagCapture, country: str, period: int,
                     industry_id: Optional[str], ask_login) -> None:
    """Load the page with another period in the URL, then pick the wanted
    period (and industry) so page 1 arrives as a request."""
    other = next(p for p in PERIODS if p != period)
    for attempt in range(MAX_LOGIN_PROMPTS + 1):
        capture.expect(None, None)
        await page.goto(page_url(country, other), wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
        await asyncio.sleep(random.uniform(3.0, 4.5))
        if "login" in page.url.lower():
            if attempt == MAX_LOGIN_PROMPTS:
                break
            print("\n⏸️  TikTok asks to log in. Log in in the browser window, then come back here.")
            await ask_login("   Press Enter once you're logged in… ")
            continue
        await _dismiss_popups(page)
        capture.expect(period, industry_id)
        try:
            await _pick(page, "period", period_label(period))
            if industry_id:
                await asyncio.sleep(random.uniform(2.0, 3.5))
                await _pick(page, "industry", INDUSTRIES[industry_id])
        except _RunStopped:
            await _dump_debug(page, "no_filter_dropdown")
            raise
        except Exception as e:   # click timed out
            await _dump_debug(page, "filter_click_failed")
            raise _RunStopped(f"Could not pick the filters: {e}")
        if await _wait_for_page_one(capture):
            return
        await _dump_debug(page, "no_list")
        raise _RunStopped("The hashtag list did not load after picking the filters "
                          "(signed out, or the page changed). See the screenshot in LOG_DIR.")
    raise _RunStopped("Still asked to log in — run action 0 to log in, then try again.")


async def _scroll_to_end(page, capture: _HashtagCapture, max_items: Optional[int]) -> None:
    stalled = 0
    for _ in range(MAX_SCROLL_ROUNDS):
        await capture.drain()
        if not capture.has_more or (max_items and len(capture.items) >= max_items):
            return
        before = len(capture.items)
        # Seen live: one page loads per "scroll up a little, then to the bottom".
        await page.evaluate(_JS_SCROLL_UP)
        await asyncio.sleep(random.uniform(0.5, 0.9))
        await page.evaluate(_JS_SCROLL_BOTTOM)
        await asyncio.sleep(random.uniform(SCROLL_DELAY_MIN, SCROLL_DELAY_MAX))
        await capture.drain()
        if len(capture.items) > before:
            stalled = 0
        else:
            stalled += 1
            if stalled >= MAX_STALLED_SCROLLS:
                print(f"   ⚠️  Scrolling stopped loading new hashtags ({stalled} tries). "
                      "Keep the browser window open on screen (not minimised) while it runs.")
                return


# ════════════════════════════════════════════════════════════════
# Public entry-point
# ════════════════════════════════════════════════════════════════
async def run_trending_hashtags(
    country: str = DEFAULT_COUNTRY,
    periods: Optional[List[int]] = None,
    industries: Optional[List[str]] = None,
    max_items_per_list: Optional[int] = None,
    ask=None,
) -> pd.DataFrame:
    """Collect the top hashtags for each period (and each industry, if given).

    periods:    7, 30 and/or 90 (days).
    industries: ids or English names from INDUSTRIES; empty = all industries.
    ask:        async callable(prompt) -> str for terminal input (defaults to input()).
    """
    country = (country or DEFAULT_COUNTRY).strip().upper()
    periods = periods or [DEFAULT_PERIOD]
    bad = [p for p in periods if p not in PERIODS]
    if bad:
        raise ValueError(f"periods must be from {PERIODS}, got {bad}")
    if ask is None:
        async def ask(prompt: str) -> str:
            return await asyncio.to_thread(input, prompt)

    industry_ids: List[Optional[str]] = [None]
    if industries:
        industry_ids = []
        for text in industries:
            iid = resolve_industry(text)
            if iid:
                if iid not in industry_ids:   # "Baby, Kids & Maternity" split on its comma
                    industry_ids.append(iid)
            else:
                print(f"   ⚠️  Unknown or ambiguous industry '{text}' — skipped. "
                      f"Choose from: {', '.join(INDUSTRIES.values())}")
        if not industry_ids:
            print("🛑 No usable industry given.")
            return pd.DataFrame()

    prefix = f"tiktok_hashtags_{country}_{'-'.join(str(p) for p in periods)}d"
    rows: List[dict] = []
    capture = _HashtagCapture()
    context, page = await _create_browser_context()
    capture.attach(page)
    print("ℹ️  Keep the browser window on screen (not minimised): the list only loads more "
          "while the page is being drawn.")
    combos = [(p, iid) for p in periods for iid in industry_ids]
    try:
        for n, (period, iid) in enumerate(combos, 1):
            print(f"\n#️⃣  [{n}/{len(combos)}] {country} · {period} days · "
                  f"{INDUSTRIES.get(iid or '', 'all industries')}")
            await _open_list(page, capture, country, period, iid, ask)
            await _scroll_to_end(page, capture, max_items_per_list)
            await capture.drain()
            got = sorted(capture.items.values(), key=lambda h: h["_rank"])
            if max_items_per_list:
                got = got[:max_items_per_list]
            rows.extend(_hashtag_to_row(h, h["_rank"], country, period, iid) for h in got)
            total = capture.total_count
            note = "" if total is None or len(got) >= total or max_items_per_list else f"  ⚠️ site says {total}"
            print(f"   ✅ {len(got)} hashtags (site total: {total}){note}")
            _save(rows, prefix, final=False)
            if n < len(combos):
                await asyncio.sleep(random.uniform(COMBO_PAUSE_MIN, COMBO_PAUSE_MAX))
    except _RunStopped as e:
        print(f"\n🛑 {e}")
    finally:
        try:
            await context.close()
        except Exception:
            pass
    return _save(rows, prefix, final=True)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="TikTok trending hashtags (Creative Center / TikTok One)")
    ap.add_argument("--country", default=DEFAULT_COUNTRY)
    ap.add_argument("--periods", default=str(DEFAULT_PERIOD), help="comma separated: 7,30,90 — or all")
    ap.add_argument("--industries", default="", help='comma separated ids or names, e.g. "Beauty & Personal Care"')
    ap.add_argument("--max", type=int, default=0, help="max hashtags per list (0 = all)")
    args = ap.parse_args()
    asyncio.run(run_trending_hashtags(
        country=args.country,
        periods=(list(PERIODS) if args.periods.strip().lower() == "all"
                 else [int(p) for p in args.periods.split(",") if p.strip()]),
        industries=[s.strip() for s in args.industries.split(",") if s.strip()],
        max_items_per_list=args.max or None,
    ))
