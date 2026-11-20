"""
cloak_tiktok_topads.py
────────────────────────────────────────────────────────────────
TikTok Creative Center — Top Ads (ads.tiktok.com/business/creativecenter/
inspiration/topads/…). Collects the list of high-performing auction ads for a
country / period, optionally per industry.

Things specific to this site, confirmed on the live pages (2026-11-20):
  • robots.txt allows everything (User-agent: * / Allow: /).
  • Without a login the page shows only 3 ads ("เข้าสู่ระบบเพื่อเข้าถึงโฆษณา
    ยอดนิยมทั้งหมด"), so a logged-in session is required (action 0). Any
    TikTok account works; the login lives in USER_DATA_DIR.
  • The page loads its data from /creative_radar_api/v1/top_ads/v2/list. Logged
    in, the first call is page=1&limit=20 and scrolling to the bottom loads
    page=2, 3, … until pagination.has_more is false. The calls carry signed
    headers (user-sign, timestamp, web-id), so this module never calls the API
    itself: it reads the responses the page already received while it is
    scrolled (passive), just like the Pinduoduo shop capture.
  • The URL accepts period=, region= and industry= (industry id, e.g.
    14000000000 = Beauty & Personal Care). Sort order is NOT read from the URL.
  • A fresh page load can fetch page 1 with limit=3 only, even when logged in
    (seen on the narrow layout); scrolling then jumps to page=2&limit=20, so
    ads 4–20 never show. Picking a sort order in the page's own sort control
    (tabs + "Others" menu on a wide window, a dropdown on a narrow one)
    re-fetches page=1&limit=20, so every list is opened that way (for the
    order already selected, another one is picked first).
    Signed out, the same pick re-fetches with limit=3 — that is how a missing
    login is detected. The rank column is the order of the chosen sort.
  • Totals seen for TH: 63 ads for 30 days, 400 for 180 days (400 looks like a
    cap), 9 for Beauty & Personal Care over 30 days. Only ads the advertiser
    allowed are listed, so the pool is small.
  • Field meanings (checked against the cards on the PC layout):
      like  → likes count
      ctr   → a ranking, not a rate: 0.80 is shown as "Top 80% CTR"
      cost  → budget level: 0 = low (น้อย), 1 = medium (ปานกลาง), 2 = high
    brand_name is often empty; the brand usually only appears in ad_title's
    hashtags.
  • Cover / video URLs are signed CDN links that expire after a few hours.
  • Opening an ad's analytics page directly (/creativecenter/topads/<id>/…)
    redirected back to the list in testing, so no detail URL is stored.

Public entry-points:
  create_tiktok_session()                      log in by hand and save the session
  run_top_ads(country, periods, industries)  → pd.DataFrame  one row per ad
"""

import asyncio
import random
import re
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlencode, urlparse

import pandas as pd

# ════════════════════════════════════════════════════════════════
# Directories & Config
# ════════════════════════════════════════════════════════════════

USER_DATA_DIR = "./data/tiktok_topads/my_session"
OUTPUT_DIR    = "./data/tiktok_topads/collected_data"
TEMP_DIR      = "./data/tiktok_topads/collected_data/temp"
LOG_DIR       = "./data/tiktok_topads/log"

BASE_URL  = "https://ads.tiktok.com/business/creativecenter"
LOGIN_URL = f"{BASE_URL}/login"
LIST_API_PATH    = "/creative_radar_api/v1/top_ads/v2/list"
FILTERS_API_PATH = "/creative_radar_api/v1/top_ads/v2/filters"

DEFAULT_COUNTRY = "TH"
PERIODS         = (7, 30, 180) # days offered by the period dropdown
DEFAULT_PERIOD  = 30
DEFAULT_ORDER   = "for_you"
# English page: the sort control is found by these labels. The data is the
# same in every page language.
PAGE_LANG       = "en"

# order_by value in the API → label in the sort control (English page)
SORT_OPTIONS = {
    "for_you":      "For You",
    "impression":   "Reach",
    "ctr":          "CTR",
    "play_2s_rate": "2s Views Rate",
    "play_6s_rate": "6s Views Rate",
    "cvr":          "CVR",
    "like":         "Likes",
}

SCROLL_DELAY_MIN   = 2.5       # seconds after each scroll (min/max)
SCROLL_DELAY_MAX   = 5.0
COMBO_PAUSE_MIN    = 8         # seconds between country/period/industry lists
COMBO_PAUSE_MAX    = 20
MAX_STALLED_SCROLLS = 4        # stop after this many scrolls that load nothing new
MAX_SCROLL_ROUNDS   = 40       # hard ceiling per list (400 ads / 20 per page = 20)
FIRST_PAGE_WAIT_S   = 20       # wait this long for page 1 after picking the sort order
MAX_LOGIN_PROMPTS   = 2

PAGE_TIMEOUT_MS  = 60000
CLICK_TIMEOUT_MS = 10000

DEDUPE_COLS = ["country", "period_days", "order_by", "industry_filter_id", "ad_id"]

BUDGET_LEVELS = {0: "low", 1: "medium", 2: "high"}

# The sort control has two layouts, both checked live on 2026-11-20:
#   • "tabs" (wide window, PC layout): "Sort by: For You | Reach | CTR | Others ▾".
#     The active item's label is red (inline color rgb(254, 44, 85)); the
#     "Others" menu holds 2s/6s Views Rate, CVR and Likes and shows the picked
#     one in red in place of "Others".
#   • "dropdown" (narrow window, tablet layout): one single-select whose text
#     is the current sort label. The "Likes" range filter is a multi-select and
#     the period dropdown has other text, so neither matches.
# The JS below only reads the page and marks the element to click with
# data-cc-sort, so Playwright can click it for real (a JS .click() does not
# open these dropdowns).
_JS_SORT_COMMON = """
    const RED = 'rgb(254, 44, 85)';
    const shown = (e) => !!e && e.getBoundingClientRect().height > 0;
    const sortBar = () => [...document.querySelectorAll('[class*="TopadsListFilter_sortContainer"]')]
        .find(shown) || null;
    const tabLabels = (sc) => [...sc.querySelectorAll('[class*="SortItem_sortItemLabel"]')].filter(shown);
    const othersButton = (sc) => sc.querySelector('.byted-select [role="button"]') || sc.querySelector('.byted-select');
    const sortDropdown = (labels) => {
        const bar = document.querySelector('[class*="TopadsListFilter_container"]');
        if (!bar) return null;
        return [...bar.querySelectorAll('.byted-select')].find(s => shown(s) &&
            /CcSelect_ccSelector/.test(s.className) && labels.includes(s.innerText.trim())) || null;
    };
"""
# → {layout: 'tabs' | 'dropdown' | null, current: label or null}
_JS_SORT_STATE = """(labels) => {""" + _JS_SORT_COMMON + """
    const sc = sortBar();
    if (sc) {
        let current = null;
        for (const l of tabLabels(sc)) if (l.style.color === RED) current = l.innerText.trim();
        const other = othersButton(sc);
        if (!current && other && other.style.color === RED) current = other.innerText.trim();
        return {layout: 'tabs', current};
    }
    const sel = sortDropdown(labels);
    return sel ? {layout: 'dropdown', current: sel.innerText.trim()} : {layout: null, current: null};
}"""
# Marks what to click first → 'direct' (a tab: one click), 'menu' (opens a
# menu; the option is clicked next) or null (no sort control found).
_JS_MARK_SORT_TARGET = """({label, labels}) => {""" + _JS_SORT_COMMON + """
    document.querySelectorAll('[data-cc-sort]').forEach(e => e.removeAttribute('data-cc-sort'));
    const sc = sortBar();
    if (sc) {
        const tab = tabLabels(sc).find(l => l.innerText.trim() === label);
        if (tab) { tab.setAttribute('data-cc-sort', 'target'); return 'direct'; }
        const other = othersButton(sc);
        if (!other) return null;
        other.setAttribute('data-cc-sort', 'target');
        return 'menu';
    }
    const sel = sortDropdown(labels);
    if (!sel) return null;
    sel.setAttribute('data-cc-sort', 'target');
    return 'menu';
}"""
_JS_SCROLL_TO_SENTINEL = """() => {
    const s = document.querySelector('[class*="ScrollMoreLoading_wrapper"]');
    if (s) { s.scrollIntoView({block: 'center'}); return true; }
    window.scrollTo(0, document.documentElement.scrollHeight);
    return false;
}"""
# Marks the option `label` in the open menu (either layout). Options can hold
# a help icon next to the text, so they are matched on their own text nodes.
_JS_MARK_SORT_OPTION = """(label) => {
    document.querySelectorAll('[data-cc-sort="option"]').forEach(e => e.removeAttribute('data-cc-sort'));
    const ownText = (e) => [...e.childNodes].filter(n => n.nodeType === 3)
        .map(n => n.textContent).join('').trim();
    const pops = [...document.querySelectorAll(
        '[class*="CcSelect_ccSelectPopover"], [class*="CcRimlessSelect_ccRimSelectPopover"]')]
        .filter(p => p.getBoundingClientRect().height > 0);
    for (const pop of pops) {
        const opt = [...pop.querySelectorAll('*')].find(e => ownText(e) === label);
        if (opt) { opt.setAttribute('data-cc-sort', 'option'); return true; }
    }
    return false;
}"""

for _folder in [OUTPUT_DIR, TEMP_DIR, LOG_DIR]:
    Path(_folder).mkdir(parents=True, exist_ok=True)

TZ_THAI = timezone(timedelta(hours=7))

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
    """The run can't continue. Rows collected so far are still saved."""


# ════════════════════════════════════════════════════════════════
# General Helpers
# ════════════════════════════════════════════════════════════════
def _make_timestamp() -> str:
    return datetime.now(TZ_THAI).strftime("%Y%m%d_%H%M")


def _collected_at() -> str:
    return datetime.now(TZ_THAI).strftime("%Y-%m-%d %H:%M:%S")


def list_page_url(country: str, period: int, industry_id: Optional[str] = None,
                  lang: str = PAGE_LANG) -> str:
    params = {"period": period, "region": country}
    if industry_id:
        params["industry"] = industry_id
    return f"{BASE_URL}/inspiration/topads/pc/{lang}?{urlencode(params)}"


def _industry_id_from_key(key: Optional[str]) -> Optional[str]:
    """'label_14106000000' → '14106000000'."""
    m = re.search(r"(\d{6,})", key or "")
    return m.group(1) if m else None


def _best_video_url(video_url: Optional[dict]) -> Tuple[str, str]:
    """Highest resolution in {'720p': url, '360p': url, …} → (url, '720p')."""
    if not isinstance(video_url, dict) or not video_url:
        return "", ""
    def res(k: str) -> int:
        m = re.match(r"(\d+)", k)
        return int(m.group(1)) if m else 0
    key = max(video_url, key=res)
    return video_url.get(key) or "", key


def _sanitize_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    """Strip control characters openpyxl refuses to write into a cell."""
    from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].map(lambda v: ILLEGAL_CHARACTERS_RE.sub("", v) if isinstance(v, str) else v)
    return df


def _save_checkpoint(rows: List[dict], prefix: str) -> None:
    if not rows:
        return
    df = pd.DataFrame(rows).drop_duplicates(subset=[c for c in DEDUPE_COLS if c in rows[0]])
    path = Path(TEMP_DIR) / f"{prefix}.csv"
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"   💾 Checkpoint: {path}  ({len(df):,} rows)")


def _save_final(rows: List[dict], prefix: str) -> pd.DataFrame:
    if not rows:
        print(f"   ⚠️  No data to save: {prefix}")
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=[c for c in DEDUPE_COLS if c in df.columns])
    xlsx = Path(OUTPUT_DIR) / f"{prefix}_{_make_timestamp()}.xlsx"
    _sanitize_for_excel(df).to_excel(xlsx, index=False)
    print("\n" + "=" * 60)
    print(f"✅ FINAL FILE SAVED : {prefix}")
    print(f"📊 Total Rows       : {len(df):,}")
    print(f"💾 XLSX             : {xlsx}")
    print("=" * 60)
    return df


# ════════════════════════════════════════════════════════════════
# Response capture (passive — reads what the page itself loaded)
# ════════════════════════════════════════════════════════════════
class _TopAdsCapture:
    """Collects list pages and the filter catalogue from responses the browser
    already received. It sends no requests of its own."""

    def __init__(self):
        self.industries: Dict[str, dict] = {}   # id → {"name", "parent_id"}
        self.reset()
        self._tasks: set = set()

    def reset(self, order_by: Optional[str] = None) -> None:
        """Forget the list. Only list responses for `order_by` are kept until
        the next reset (None = keep none, e.g. while the page first loads)."""
        self.order_by = order_by
        self.materials: Dict[str, dict] = {}    # ad id → material + rank info
        self.full_pages: Dict[int, dict] = {}   # page number → pagination (limit>=20 calls only)
        self.preview_seen = False               # page 1 came back as the limit=3 preview
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
        path = urlparse(response.url).path
        if path not in (LIST_API_PATH, FILTERS_API_PATH) or response.status != 200:
            return
        try:
            payload = await response.json()
        except Exception:   # body gone after a navigation
            self.errors += 1
            return
        self.ingest(response.url, payload)

    # Split out so it can be tested without a browser.
    def ingest(self, url: str, payload: dict) -> None:
        if not isinstance(payload, dict) or payload.get("code") != 0:
            return
        data = payload.get("data") or {}
        path = urlparse(url).path
        if path == FILTERS_API_PATH:
            for item in data.get("industry") or []:
                if item.get("id") is not None:
                    self.industries[str(item["id"])] = {
                        "name": item.get("value") or "",
                        "parent_id": str(item["parent_id"]) if item.get("parent_id") else None,
                    }
            return

        query = {k: v[0] for k, v in parse_qs(urlparse(url).query).items()}
        if self.order_by is None or query.get("order_by") != self.order_by:
            return   # the initial load, or a response for an earlier sort order
        try:
            limit = int(query.get("limit", 0))
            page_no = int(query.get("page", 0))
        except ValueError:
            return
        if limit < 20:
            # The 3-ad preview: what a signed-out page gets after a sort pick.
            if page_no == 1 and not self.full_pages:
                self.preview_seen = True
            return
        pagination = data.get("pagination") or {}
        self.full_pages[page_no] = pagination
        for idx, material in enumerate(data.get("materials") or []):
            ad_id = str(material.get("id") or "")
            if not ad_id or ad_id in self.materials:
                continue
            self.materials[ad_id] = {
                **material,
                "_rank": (page_no - 1) * limit + idx + 1,
                "_api_page": page_no,
            }

    @property
    def has_more(self) -> bool:
        if not self.full_pages:
            return True
        return bool(self.full_pages[max(self.full_pages)].get("has_more"))

    @property
    def total_count(self) -> Optional[int]:
        if not self.full_pages:
            return None
        return self.full_pages[max(self.full_pages)].get("total_count")

    def industry_names(self, industry_id: Optional[str]) -> Tuple[str, str]:
        """→ (top-level name, sub-industry name). Unknown ids come back as ''."""
        info = self.industries.get(industry_id or "")
        if not info:
            return "", ""
        if info["parent_id"]:
            parent = self.industries.get(info["parent_id"], {})
            return parent.get("name", ""), info["name"]
        return info["name"], ""

    def resolve_industry(self, text: str) -> Optional[str]:
        """An industry id, or an English name from the site's filter list
        (case-insensitive, exact first, then 'contains')."""
        text = text.strip()
        if text.isdigit():
            return text
        low = text.lower()
        for iid, info in self.industries.items():
            if info["name"].lower() == low:
                return iid
        hits = [iid for iid, info in self.industries.items() if low in info["name"].lower()]
        return hits[0] if len(hits) == 1 else None


def _material_to_row(m: dict, capture: _TopAdsCapture, country: str, period: int,
                     order_by: str, industry_filter_id: Optional[str]) -> dict:
    video = m.get("video_info") or {}
    video_url, video_res = _best_video_url(video.get("video_url"))
    industry_id = _industry_id_from_key(m.get("industry_key"))
    industry_l1, industry_l2 = capture.industry_names(industry_id)
    filter_l1, filter_l2 = capture.industry_names(industry_filter_id)
    ctr = m.get("ctr")
    cost = m.get("cost")
    return {
        "country": country,
        "period_days": period,
        "order_by": order_by,
        "industry_filter_id": industry_filter_id or "",
        "industry_filter": filter_l2 or filter_l1 or industry_filter_id or "",
        "rank": m.get("_rank"),
        "ad_id": str(m.get("id") or ""),
        "ad_title": m.get("ad_title") or "",
        "brand_name": m.get("brand_name") or "",
        "industry_id": industry_id or "",
        "industry": industry_l1,
        "sub_industry": industry_l2,
        "objective": re.sub(r"^campaign_objective_", "", m.get("objective_key") or ""),
        "likes": m.get("like"),
        "ctr_top_pct": round(ctr * 100) if isinstance(ctr, (int, float)) else None,
        "budget_level": BUDGET_LEVELS.get(cost, "" if cost is None else str(cost)),
        "video_duration_s": video.get("duration"),
        "video_width": video.get("width"),
        "video_height": video.get("height"),
        "video_id": video.get("vid") or "",
        "cover_url": video.get("cover") or "",
        "video_url": video_url,
        "video_resolution": video_res,
        "collected_at": _collected_at(),
    }


# ════════════════════════════════════════════════════════════════
# Browser session
# ════════════════════════════════════════════════════════════════
async def _create_browser_context():
    from cloakbrowser import launch_persistent_context_async
    context = await launch_persistent_context_async(
        USER_DATA_DIR,
        headless=False,
        # Wide enough for the PC layout (narrower windows get the tablet layout).
        viewport={"width": 1366, "height": 900},
        locale="th-TH",
        timezone_id="Asia/Bangkok",
        humanize=True,
    )
    page = context.pages[0] if context.pages else await context.new_page()
    return context, page


async def create_tiktok_session():
    """Log in by hand and save the session. Run on first use or after it expires."""
    context, page = await _create_browser_context()
    target = list_page_url(DEFAULT_COUNTRY, DEFAULT_PERIOD)
    await page.goto(f"{LOGIN_URL}?{urlencode({'from': target})}",
                    wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
    print("\n" + "—" * 60)
    print("⚡ [SESSION] Browser is active.")
    print("—" * 60)
    print(" 1. Log in to TikTok for Business (QR code, phone or email).")
    print(" 2. After logging in, check that the Top Ads list shows more than 3 ads.")
    print(" 3. Close the browser window when finished.")
    print("—" * 60 + "\n")
    while len(context.pages) > 0:
        await asyncio.sleep(1)
    try:
        await context.close()
    except Exception:
        pass
    print(f"✅ [SUCCESS] Session saved to '{USER_DATA_DIR}'.")


async def _dismiss_popups(page) -> None:
    # The promo video dialog closes with Escape; it doesn't block scrolling anyway.
    try:
        await page.keyboard.press("Escape")
    except Exception:
        pass


async def _dump_debug(page, label: str) -> None:
    safe = re.sub(r"[^\w-]+", "_", label, flags=re.UNICODE)[:50]
    base = Path(LOG_DIR) / f"{datetime.now(TZ_THAI).strftime('%Y%m%d_%H%M%S')}_{safe}"
    try:
        base.with_suffix(".html").write_text(await page.content(), encoding="utf-8")
        await page.screenshot(path=str(base.with_suffix(".png")))
        print(f"   📸 Debug dump: {base}.{{html,png}}")
    except Exception as e:
        print(f"   ⚠️  Could not save debug dump: {e}")


async def _wait_for_first_page(capture: _TopAdsCapture, timeout_s: float = FIRST_PAGE_WAIT_S) -> str:
    """'full' (page=1&limit=20 arrived), 'preview' (limit=3 — signed out) or 'none'."""
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout_s
    while loop.time() < end:
        await capture.drain()
        if capture.full_pages:
            return "full"
        if capture.preview_seen:
            return "preview"
        await asyncio.sleep(1)
    return "none"


async def _pick_sort_option(page, label: str) -> None:
    """Click sort order `label` — a tab, or an option in the menu (real clicks)."""
    kind = await page.evaluate(_JS_MARK_SORT_TARGET,
                               {"label": label, "labels": list(SORT_OPTIONS.values())})
    if kind is None:
        raise _RunStopped("Could not find the sort control on the Top Ads page (layout changed?).")
    await _dismiss_popups(page)
    await page.click('[data-cc-sort="target"]', timeout=CLICK_TIMEOUT_MS)
    if kind == "direct":
        return
    await asyncio.sleep(random.uniform(0.6, 1.2))
    if not await page.evaluate(_JS_MARK_SORT_OPTION, label):
        raise _RunStopped(f"Sort option '{label}' not found in the menu.")
    await page.click('[data-cc-sort="option"]', timeout=CLICK_TIMEOUT_MS)


async def _apply_sort(page, capture: _TopAdsCapture, order_by: str) -> None:
    """Pick the sort order so the page re-fetches page 1 with limit=20. If it is
    already selected (or that can't be told), pick another one first —
    re-picking the current one fetches nothing."""
    label = SORT_OPTIONS[order_by]
    state = await page.evaluate(_JS_SORT_STATE, list(SORT_OPTIONS.values()))
    if not state or not state.get("layout"):
        raise _RunStopped("Could not find the sort control on the Top Ads page (layout changed?).")
    if state.get("current") in (label, None):
        detour = "impression" if order_by != "impression" else "for_you"
        capture.reset(order_by=None)
        await _pick_sort_option(page, SORT_OPTIONS[detour])
        await asyncio.sleep(random.uniform(2.0, 3.5))
    capture.reset(order_by=order_by)
    await _pick_sort_option(page, label)


async def _open_list(page, capture: _TopAdsCapture, url: str, order_by: str, ask_login) -> None:
    """Open a list page sorted by `order_by` and make sure the full list
    (limit=20) loaded. Signed out, ask the person to log in, then reload."""
    for attempt in range(MAX_LOGIN_PROMPTS + 1):
        capture.reset(order_by=None)
        await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
        await asyncio.sleep(random.uniform(3.0, 4.5))
        await _dismiss_popups(page)
        try:
            await _apply_sort(page, capture, order_by)
        except _RunStopped:
            await _dump_debug(page, "no_sort_control")
            raise
        except Exception as e:   # click timed out (something covering the sort control)
            await _dump_debug(page, "sort_click_failed")
            raise _RunStopped(f"Could not pick the sort order: {e}")
        state = await _wait_for_first_page(capture)
        if state == "full":
            return
        await _dump_debug(page, "signed_out" if state == "preview" else "no_list")
        if state != "preview":
            raise _RunStopped("The Top Ads list did not load after picking the sort order. "
                              "See the debug dump in LOG_DIR.")
        if attempt == MAX_LOGIN_PROMPTS:
            break
        print("\n⏸️  The page is signed out (only the 3-ad preview loads).")
        print("   Log in in the browser window (button top-right), then come back here.")
        await ask_login("   Press Enter once you're logged in… ")
    raise _RunStopped("Still signed out — run action 0 to log in, then try again.")


async def _scroll_to_end(page, capture: _TopAdsCapture, max_items: Optional[int]) -> None:
    stalled = 0
    for _ in range(MAX_SCROLL_ROUNDS):
        await capture.drain()
        if not capture.has_more:
            return
        if max_items and len(capture.materials) >= max_items:
            return
        before = len(capture.materials)
        # The next page loads when the "ScrollMoreLoading" sentinel under the
        # grid turns from not-fully-visible to fully visible (an
        # IntersectionObserver with threshold 1, which only fires while the
        # page is being drawn). So: scroll up and give the page a moment to
        # see the sentinel leave, then bring it to the MIDDLE of the window.
        # Aligning it to the bottom edge left it 0.2px off-screen on the wide
        # layout, so it never counted as visible and only 20 ads loaded.
        await page.evaluate("() => window.scrollBy(0, -800)")
        await asyncio.sleep(random.uniform(0.5, 0.9))
        await page.evaluate(_JS_SCROLL_TO_SENTINEL)
        await asyncio.sleep(random.uniform(SCROLL_DELAY_MIN, SCROLL_DELAY_MAX))
        await capture.drain()
        if len(capture.materials) > before:
            stalled = 0
        else:
            stalled += 1
            if stalled >= MAX_STALLED_SCROLLS:
                print(f"   ⚠️  Scrolling stopped loading new ads ({stalled} tries). "
                      "Keep the browser window open on screen (not minimised) while it runs.")
                return


# ════════════════════════════════════════════════════════════════
# Public entry-point
# ════════════════════════════════════════════════════════════════
async def run_top_ads(
    country: str = DEFAULT_COUNTRY,
    periods: Optional[List[int]] = None,
    industries: Optional[List[str]] = None,
    order_by: str = DEFAULT_ORDER,
    max_items_per_list: Optional[int] = None,
    ask=None,
) -> pd.DataFrame:
    """Collect Top Ads for each period (and each industry, if given).

    industries: industry ids (e.g. "14000000000") or English names from the
                site's filter ("Beauty & Personal Care", "Haircare"). Empty =
                all industries in one list.
    order_by:   a key of SORT_OPTIONS (for_you, impression, ctr, play_2s_rate,
                play_6s_rate, cvr, like) — sets the rank column.
    ask:        async callable(prompt) -> str for terminal input (defaults to input()).
    """
    country = (country or DEFAULT_COUNTRY).strip().upper()
    periods = periods or [DEFAULT_PERIOD]
    if order_by not in SORT_OPTIONS:
        raise ValueError(f"order_by must be one of {list(SORT_OPTIONS)}")
    if ask is None:
        async def ask(prompt: str) -> str:
            return await asyncio.to_thread(input, prompt)

    prefix = f"tiktok_topads_{country}_{'-'.join(str(p) for p in periods)}d_{order_by}"
    rows: List[dict] = []
    capture = _TopAdsCapture()
    context, page = await _create_browser_context()
    capture.attach(page)
    print("ℹ️  Keep the browser window on screen (not minimised): the list only loads more "
          "while the page is being drawn.")
    try:
        # First load (no industry) also brings the filter catalogue, which
        # turns industry names into ids.
        await _open_list(page, capture, list_page_url(country, periods[0]), order_by, ask)
        industry_ids: List[Optional[str]] = [None]
        if industries:
            industry_ids = []
            for text in industries:
                iid = capture.resolve_industry(text)
                if iid:
                    industry_ids.append(iid)
                else:
                    print(f"   ⚠️  Unknown or ambiguous industry '{text}' — skipped. "
                          "Use the English name from the site's filter or its id.")
            if not industry_ids:
                raise _RunStopped("No usable industry given.")

        combos = [(p, iid) for p in periods for iid in industry_ids]
        for n, (period, iid) in enumerate(combos, 1):
            l1, l2 = capture.industry_names(iid)
            label = (f"{country} · {period} days · {l2 or l1 or iid or 'all industries'}"
                     f" · sorted by {SORT_OPTIONS[order_by]}")
            print(f"\n📋 [{n}/{len(combos)}] {label}")
            if n > 1 or iid is not None:   # combo 1 without industry = the page already open
                await _open_list(page, capture, list_page_url(country, period, iid), order_by, ask)
            await _scroll_to_end(page, capture, max_items_per_list)
            await capture.drain()

            got = sorted(capture.materials.values(), key=lambda m: m["_rank"])
            if max_items_per_list:
                got = got[:max_items_per_list]
            rows.extend(_material_to_row(m, capture, country, period, order_by, iid) for m in got)
            total = capture.total_count
            note = "" if total is None or len(got) >= total or max_items_per_list else \
                f"  ⚠️ site says {total}"
            print(f"   ✅ {len(got)} ads (site total: {total}){note}")
            _save_checkpoint(rows, prefix)
            if n < len(combos):
                await asyncio.sleep(random.uniform(COMBO_PAUSE_MIN, COMBO_PAUSE_MAX))
    except _RunStopped as e:
        print(f"\n🛑 {e}")
    finally:
        try:
            await context.close()
        except Exception:
            pass
    return _save_final(rows, prefix)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="TikTok Creative Center Top Ads scraper")
    ap.add_argument("--login", action="store_true", help="open the browser to log in and save the session")
    ap.add_argument("--country", default=DEFAULT_COUNTRY)
    ap.add_argument("--periods", default=str(DEFAULT_PERIOD), help="comma separated: 7,30,180 — or all")
    ap.add_argument("--industries", default="", help='comma separated ids or names, e.g. "Beauty & Personal Care"')
    ap.add_argument("--order", default=DEFAULT_ORDER, choices=list(SORT_OPTIONS))
    ap.add_argument("--max", type=int, default=0, help="max ads per list (0 = all)")
    args = ap.parse_args()
    if args.login:
        asyncio.run(create_tiktok_session())
    else:
        asyncio.run(run_top_ads(
            country=args.country,
            periods=(list(PERIODS) if args.periods.strip().lower() == "all"
                     else [int(p) for p in args.periods.split(",") if p.strip()]),
            industries=[s.strip() for s in args.industries.split(",") if s.strip()],
            order_by=args.order,
            max_items_per_list=args.max or None,
        ))
