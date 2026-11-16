"""
cloak_taobao.py
────────────────────────────────────────────────────────────────
Taobao (s.taobao.com) search scraper — search-result data only.

Confirmed on the live site while logged in:
  • A search page holds 48 items. Each carries item_id, title, price,
    realSales ("1000+人付款", "1万+人付款" — rounded buckets), shop name,
    province/city, category path and an ad flag (isP4p). The page shows a
    machine-translated English UI, but this data stays in Chinese.
  • The data lives only as props on the page's React components, so items
    are read by walking the React tree (same technique as cloak_pinduoduo).
  • Changing ?page= in the URL is ignored (the site resets it to page 1);
    clicking the "下一页 / Next page" button works. The site offers 100 pages.
  • Searching without a login shows a slider CAPTCHA straight away.

Product details (run_product_detail) — written 2026-11-13, first live pilot the same day:
  • The first product page tried during the search work showed "访问异常"
    (abnormal access) after only three searches, so this step is built to
    open as few pages as possible, as slowly as possible:
      – only the best sellers: by default the items that together make up
        80% of the search file's sales (≈200 of 3,704 for "mistine")
      – 45–120 s between pages, a 5–10 min rest every 10 pages
      – at most DAILY_DETAIL_LIMIT pages per day across all runs (default 40),
        so ~200 items take about 5 days; it resumes from the checkpoint
  • Data is read passively, in this order: the detail JSON the page itself
    loads (mtop …pcdetail.data.get, captured from the response, never
    requested by this module), then a data object on window, then the
    "参数信息" table in the page. Field names in that JSON come from known
    layouts, not from a live page — the first SAVE_SAMPLE_DETAILS products
    save their raw JSON to LOG_DIR so the parser can be checked/fixed.
  • Taobao asks for a slider at most DETAIL_MAX_HUMAN_WAITS times per run,
    and two blank product pages in a row stop the run.
  • Seen live: from the 2nd product page on, the detail data carried
    feature.pcIdentityRisk / pcResistDetail = "true" and came WITHOUT the
    parameter list (title, price and SKUs were still there). Such a page is
    kept as page_status "degraded", tried again on a later run, and the run
    stops at once — more pages in a flagged session only add risk.

Not done on purpose:
  • No CAPTCHA is ever solved. A slider or verification page pauses the run
    until a person deals with it in the browser window (no time limit), and
    the run stops if that keeps recurring.
  • The product description (mostly images) and reviews are not collected.

Pacing is deliberately slow (10–20 s between pages, 1–3 min between
keywords) because the session belongs to a real account, possibly linked
to Alipay, and robots.txt disallows search URLs for generic user agents.
Use an account that isn't your main/Alipay one for product details.

Public entry-points:
  create_taobao_session()                login manually and save the session
  run_search_keyword(keywords)       →   pd.DataFrame  search rows incl. sales
  search_checkpoint_summary()        →   what the checkpoint holds (for resuming)
  select_best_sellers(df_search)     →   pd.DataFrame  the rows details are collected for
  run_product_detail(df_product=…)   →   pd.DataFrame  brand, net content, SKUs, attributes…
  checkpoint_summary("detail")       →   what the detail checkpoint holds
  detail_quota_left()                →   product pages still allowed today
"""

import asyncio
import json
import os
import shutil
import random
import re
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple
from urllib.parse import parse_qs, quote, urlparse

import pandas as pd
from cloakbrowser import launch_persistent_context_async

# ════════════════════════════════════════════════════════════════
# Directories & Config
# ════════════════════════════════════════════════════════════════

USER_DATA_DIR = "./data/taobao/my_session"
OUTPUT_DIR    = "./data/taobao/collected_data"
TEMP_DIR      = "./data/taobao/collected_data/temp"
LOG_DIR       = "./data/taobao/log"

SEARCH_URL = "https://s.taobao.com/search"
LOGIN_URL  = "https://login.taobao.com/"

DEFAULT_MAX_PAGES_PER_KEYWORD = 5      # 48 items per page
MAX_PAGES_HARD                = 100    # the site's own limit

PAGE_DELAY_MIN    = 10     # seconds before clicking to the next page (min/max)
PAGE_DELAY_MAX    = 20
KEYWORD_PAUSE_MIN = 60     # seconds between keywords (min/max)
KEYWORD_PAUSE_MAX = 180
PAGE_POLL_ROUNDS  = 10     # polls of 2s waiting for results to render

PAGE_TIMEOUT_MS         = 60000
HUMAN_WAIT_TIMEOUT      = None   # seconds to wait for a person at login/slider; None = no limit
HUMAN_WAIT_REMINDER     = 300    # while waiting, print a reminder every N seconds
HUMAN_WAIT_COOLDOWN     = 30     # seconds to pause after a person cleared it
MAX_HUMAN_WAITS_PER_RUN = 3      # stop instead of pushing on after repeated checks
MAX_EMPTY_KEYWORDS_IN_A_ROW = 2  # empty results twice running → probably blocked; stop

RESUME_FROM_BACKUP = False

SEARCH_DEDUPE_COLS = ["keyword_searched", "item_id"]

# ── Product details ───────────────────────────────────────────
ITEM_URL_TEMPLATE = "https://item.taobao.com/item.htm?id={item_id}"   # Tmall items redirect by themselves

DEFAULT_SALES_COVERAGE = 0.8   # details for the best sellers that make up this share of sales
DETAIL_DELAY_MIN   = 45        # seconds between product pages (min/max)
DETAIL_DELAY_MAX   = 120
DETAIL_REST_EVERY  = 10        # every N product pages, take a longer rest…
DETAIL_REST_MIN    = 300       # …of this many seconds (min/max)
DETAIL_REST_MAX    = 600
DAILY_DETAIL_LIMIT = 40        # product pages per day (Thai date) across all runs; None = no limit
DETAIL_POLL_ROUNDS = 10        # polls of 2s waiting for the product data to arrive
DETAIL_MAX_HUMAN_WAITS = 2     # sliders allowed per detail run before it stops
MAX_EMPTY_DETAILS_IN_A_ROW = 2
SAVE_SAMPLE_DETAILS = 3        # raw JSON of the first N products → LOG_DIR (to check the parser)
DETAIL_CHECKPOINT_EVERY = 5

DETAIL_DEDUPE_COLS = ["item_id"]
QUOTA_FILE = Path(LOG_DIR) / "detail_quota.json"

# The page's own detail request (h5api.m.taobao.com / h5api.m.tmall.com).
_DETAIL_API_HINTS = ("pcdetail.data.get", "mtop.taobao.detail.getdetail", "mtop.taobao.detail.data.get")
# mtop "ret" codes meaning the request was refused by anti-bot, not just a token refresh.
_BLOCK_RET_HINTS = ("FAIL_SYS_USER_VALIDATE", "RGV587_ERROR", "FAIL_SYS_ILLEGAL_ACCESS")
_UNAVAILABLE_PHRASES = ("宝贝不存在", "宝贝已下架", "商品已下架", "已下架", "商品已失效", "宝贝已失效")
# feature flags in the detail data when Taobao treats the session as a risk; the page
# then comes without its parameter list (seen live 2026-11-13).
_RISK_FLAGS = ("pcIdentityRisk", "pcResistDetail")
# The same state as seen on the page: a red "访问异常提示" box under the gallery saying the
# product detail page will recover by itself after a while (…商品详情页将在一段时间后自动恢复).
# It is NOT a verification — there is nothing for a person to clear.
_RESISTED_BOX = "访问异常提示"
_DEGRADED_STOP_MESSAGE = (
    "Taobao is serving product pages WITHOUT their details for this session (the red "
    "访问异常提示 box / pcIdentityRisk flag): 品牌/净含量/产地/备案号 are hidden. Taobao says detail "
    "pages recover by themselves after a while. Stopping — more pages would only add risk. "
    "Wait several hours (ideally until tomorrow), then run again; it continues from the checkpoint."
)

# Columns copied from a search dataframe onto detail rows.
_SEARCH_FIELDS_FOR_DETAIL = ["keyword_searched", "product_name", "price_cny", "sales_text",
                             "sold_count", "shop_name", "location"]

# Output column ← attribute names on the page (exact name first, then "name contains").
_ATTRIBUTE_COLUMNS: Dict[str, Tuple[str, ...]] = {
    "brand":           ("品牌",),
    "origin":          ("产地", "原产地", "国家/地区"),
    "net_content":     ("净含量", "容量"),
    "spf":             ("防晒指数", "SPF"),
    "efficacy":        ("功效",),
    "skin_type":       ("适合肤质",),
    "shelf_life":      ("保质期", "限期使用日期"),
    "registration_no": ("化妆品备案编号", "化妆品注册证号", "备案编号", "注册证号", "批准文号", "备案号"),
    "manufacturer":    ("生产企业名称", "生产企业", "生产商"),
}

_HUMAN_CHECK_PHRASES = ("drag the slider", "滑动验证", "请拖动", "访问异常", "安全验证")
_NEXT_PAGE_RE  = re.compile(r"下一页|Next page", re.I)
_SORT_SALES_RE = re.compile(r"^\s*(销量|Sales volume)\s*$", re.I)

for _folder in [OUTPUT_DIR, TEMP_DIR, LOG_DIR]:
    Path(_folder).mkdir(parents=True, exist_ok=True)

TZ_THAI = timezone(timedelta(hours=7))

_human_waits = 0
_human_wait_limit = MAX_HUMAN_WAITS_PER_RUN

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

_JS_PAGE_STATE = r"""() => {
  const text = document.body ? document.body.innerText.slice(0, 3000) : '';
  const slider = !!document.querySelector(
    '#nc_1_n1z, .nc_iconfont, #baxia-punish, iframe[src*=punish], .baxia-dialog');
  return { host: location.host, slider, text };
}"""

_JS_SEARCH_ITEMS = r"""() => {
  const rootFiber = () => {
    for (const el of document.querySelectorAll('div')) {
      const key = Object.keys(el).find(k => k.startsWith('__reactFiber'));
      if (key) { let f = el[key]; while (f.return) f = f.return; return f; }
    }
    return null;
  };
  const strip = (s) => String(s || '').replace(/<[^>]+>/g, '').trim();
  const seen = new Map();
  const root = rootFiber();
  const stack = root ? [root] : [];
  let guard = 0;
  while (stack.length && guard++ < 200000) {
    const node = stack.pop();
    const props = node.memoizedProps;
    if (props && typeof props === 'object') {
      for (const key of Object.keys(props)) {
        const v = props[key];
        if (!v || typeof v !== 'object' || Array.isArray(v) || !v.item_id || !('realSales' in v)) continue;
        const id = String(v.item_id);
        if (seen.has(id)) continue;
        seen.set(id, {
          item_id: id,
          product_name: strip(v.title),
          price: v.price,
          sales_text: v.realSales || '',
          shop_name: (v.shopInfo && v.shopInfo.title) || '',
          location: v.procity || '',
          category_path: v.categoryp || '',
          leaf_category_id: v.leafCategory || '',
          is_ad: String(v.isP4p) === 'true',
        });
      }
    }
    if (node.sibling) stack.push(node.sibling);
    if (node.child) stack.push(node.child);
  }
  return { items: [...seen.values()] };
}"""

# Fallback when the detail response wasn't captured: a data object the page keeps
# on window (e.g. a server-rendered app context) that has the same item/seller/
# skuBase shape. Read-only; returns a plain copy or null.
_JS_WINDOW_DETAIL = r"""() => {
  const looksLikeDetail = (o) => o && typeof o === 'object' && o.item && typeof o.item === 'object'
      && (o.skuBase || o.seller || o.componentsVO || o.skuCore);
  const seen = new Set();
  let found = null;
  const visit = (o, depth) => {
    if (found || !o || typeof o !== 'object' || depth > 8 || seen.has(o)) return;
    if (typeof Node !== 'undefined' && o instanceof Node) return;
    seen.add(o);
    if (looksLikeDetail(o)) { found = o; return; }
    for (const k of Object.keys(o).slice(0, 300)) {
      let v; try { v = o[k]; } catch (e) { continue; }
      if (v && typeof v === 'object') visit(v, depth + 1);
    }
  };
  for (const k of Object.keys(window)) {
    if (!/^__|INITIAL|DATA|STATE|CONTEXT/i.test(k)) continue;
    let v; try { v = window[k]; } catch (e) { continue; }
    if (v && typeof v === 'object' && v !== window) visit(v, 0);
    if (found) break;
  }
  if (!found) return null;
  try { return JSON.parse(JSON.stringify(found)); } catch (e) { return null; }
}"""

# Last fallback: the "参数信息" table as [name, value] pairs. Class names on the
# current layout are hashed, so only their readable stems are matched.
_JS_DOM_PARAMS = r"""() => {
  const text = (el) => (el ? el.innerText || el.textContent || '' : '').replace(/\s+/g, ' ').trim();
  const pairs = [];
  for (const item of document.querySelectorAll('[class*="ParamsInfoItem"], [class*="paramsInfoItem"]')) {
    const name = item.querySelector('[class*="Title"]:not([class*="SubTitle"])');
    const value = item.querySelector('[class*="SubTitle"]');
    if (name && value) pairs.push([text(name), text(value)]);
  }
  if (!pairs.length) {
    for (const li of document.querySelectorAll('.attributes-list li, #J_AttrUL li, .tm-attrs li')) {
      const m = text(li).match(/^([^:：]{1,20})[:：]\s*(.+)$/);
      if (m) pairs.push([m[1].trim(), m[2].trim()]);
    }
  }
  return pairs;
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


def _parse_sales(text: Optional[str]) -> Optional[int]:
    """'1000+人付款' → 1000, '1万+人付款' → 10000, '2.5万+' → 25000. Rounded buckets."""
    match = re.search(r"(\d+(?:\.\d+)?)\s*(万|w)?", str(text or "").replace(",", ""), re.I)
    if not match:
        return None
    return int(round(float(match.group(1)) * (10_000 if match.group(2) else 1)))


def _to_float(value) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _row(keyword: str, page_no: int, rank: int, item: dict) -> dict:
    return {
        "scraped_date": _scraped_date(),
        "keyword_searched": keyword,
        "page": page_no,
        "search_rank": rank,
        "item_id": item["item_id"],
        "product_name": item["product_name"],
        "price_cny": _to_float(item.get("price")),
        "sales_text": item.get("sales_text", ""),
        "sold_count": _parse_sales(item.get("sales_text")),
        "shop_name": item.get("shop_name", ""),
        "location": item.get("location", ""),
        "category_path": item.get("category_path", ""),
        "leaf_category_id": item.get("leaf_category_id", ""),
        "is_ad": bool(item.get("is_ad")),
        "product_url": f"https://item.taobao.com/item.htm?id={item['item_id']}",
        "collected_at": _collected_at(),
    }


def _sanitize_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    """Strip control characters openpyxl refuses to write into a cell."""
    from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].map(lambda v: ILLEGAL_CHARACTERS_RE.sub("", v) if isinstance(v, str) else v)
    return df


def _save_checkpoint(rows: list, data_prefix: str, reason: str = "checkpoint",
                     dedupe_cols: List[str] = SEARCH_DEDUPE_COLS, keep: str = "first"):
    if not rows:
        return
    df = pd.DataFrame(rows).drop_duplicates(subset=dedupe_cols, keep=keep)
    path = Path(TEMP_DIR) / f"{data_prefix}.csv"
    _write_atomic(path, lambda tmp: df.to_csv(tmp, index=False, encoding="utf-8-sig"))
    print(f"   💾 Checkpoint [{reason}]: {path}  ({len(df):,} rows)")


def _write_atomic(path: Path, write) -> None:
    """Write to a temp file next to `path`, then swap it in — a run stopped mid-write
    (Ctrl+C, closed window) leaves the old file intact instead of a broken one."""
    tmp = path.with_name(f"{path.stem}.partial{path.suffix}")
    write(tmp)
    os.replace(tmp, path)


def _save_final(rows: list, final_prefix: str,
                dedupe_cols: List[str] = SEARCH_DEDUPE_COLS, keep: str = "first") -> pd.DataFrame:
    if not rows:
        print(f"   ⚠️  No data to save: {final_prefix}")
        return pd.DataFrame()
    df = _sanitize_for_excel(pd.DataFrame(rows).drop_duplicates(subset=dedupe_cols, keep=keep))
    xlsx = Path(OUTPUT_DIR) / f"{final_prefix}_{_make_timestamp()}.xlsx"
    _write_atomic(xlsx, lambda tmp: df.to_excel(tmp, index=False))
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
        # IDs stay strings (12–13 digit item ids; leading zeros in registration numbers).
        rows = pd.read_csv(path, dtype={"item_id": str, "leaf_category_id": str, "shop_id": str,
                                        "category_id": str, "registration_no": str,
                                        "seller_type": str}).to_dict(orient="records")
        print(f"🔁 Backup loaded: {path}  ({len(rows):,} rows)")
        return rows
    except Exception as e:
        print(f"⚠️  Failed to load backup {path}: {e}")
        return []


def search_checkpoint_summary() -> Optional[dict]:
    """What the search checkpoint holds, so a caller can offer to continue from it."""
    path = Path(TEMP_DIR) / "temp_taobao_search.csv"
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, dtype=str, usecols=["keyword_searched", "collected_at"])
    except Exception:
        return None
    if df.empty:
        return None
    return {
        "rows": len(df),
        "keywords": list(dict.fromkeys(df["keyword_searched"].fillna(""))),
        "last_collected": df["collected_at"].max(),
    }


# ════════════════════════════════════════════════════════════════
# Browser session
# ════════════════════════════════════════════════════════════════
async def _create_browser_context():
    context = await launch_persistent_context_async(
        USER_DATA_DIR,
        headless=False,
        viewport={"width": 1366, "height": 900},
        locale="th-TH",
        timezone_id="Asia/Bangkok",
        humanize=True,
    )
    page = context.pages[0] if context.pages else await context.new_page()
    return context, page


async def create_taobao_session():
    """Log in by hand and save the session. Run on first use or after it expires."""
    context, page = await _create_browser_context()
    await page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
    print("\n" + "—" * 60)
    print("⚡ [SESSION] Browser is active.")
    print("—" * 60)
    print(" 1. Log in to Taobao manually (QR code with the Taobao app, or password).")
    print(" 2. Close the browser window when finished.")
    print("—" * 60 + "\n")
    while len(context.pages) > 0:
        await asyncio.sleep(1)
    try:
        await context.close()
    except Exception:
        pass
    print(f"✅ [SUCCESS] Session saved to '{USER_DATA_DIR}'.")


async def _evaluate(page, script: str):
    """page.evaluate that survives one in-page navigation during the call."""
    try:
        return await page.evaluate(script)
    except Exception as e:
        if "context was destroyed" not in str(e).lower():
            raise
        await asyncio.sleep(2)
        return await page.evaluate(script)


def _is_resisted(host: str, text: str) -> bool:
    """A product page showing the 访问异常提示 box (details hidden, nothing to clear)."""
    return _RESISTED_BOX in text and (host.startswith(("item.", "detail.")) or "商品详情页" in text)


async def _page_state(page) -> str:
    """'ok', 'login', 'human_check' or 'resisted' (product page shown without its details)."""
    state = await _evaluate(page, _JS_PAGE_STATE)
    if "login." in state["host"]:
        return "login"
    if not state["slider"] and _is_resisted(state["host"], state["text"]):
        return "resisted"
    if state["slider"] or any(p in state["text"] for p in _HUMAN_CHECK_PHRASES):
        return "human_check"
    return "ok"


_PAGE_USABLE = ("ok", "resisted")   # states where there is nothing for a person to do


async def _dump_debug(page, label: str) -> None:
    safe = re.sub(r"[^\w-]+", "_", label, flags=re.UNICODE)[:50]
    base = Path(LOG_DIR) / f"{datetime.now(TZ_THAI).strftime('%Y%m%d_%H%M%S')}_{safe}"
    try:
        base.with_suffix(".html").write_text(await page.content(), encoding="utf-8")
        await page.screenshot(path=str(base.with_suffix(".png")))
        print(f"   📸 Debug dump: {base}.{{html,png}}")
    except Exception as e:
        print(f"   ⚠️  Could not save debug dump: {e}")


async def _wait_for_human(page, state: str) -> None:
    """Pause until a person clears the login/slider page in the browser window.
    Nothing here attempts to solve or get around it."""
    global _human_waits
    _human_waits += 1
    if _human_waits > _human_wait_limit:
        raise _RunStopped(
            f"Taobao asked for login/verification more than {_human_wait_limit} times "
            "in this run. Stopping to protect the account — try again later."
        )

    await _dump_debug(page, state)
    what = "log in" if state == "login" else "complete the verification"
    how_long = (f"up to {HUMAN_WAIT_TIMEOUT / 60:.0f} minutes" if HUMAN_WAIT_TIMEOUT
                else "until you're done — it continues by itself (Ctrl+C to stop)")
    print(f"\n⏸️  Please {what} in the browser window. Waiting {how_long}…")

    loop = asyncio.get_running_loop()
    started = loop.time()
    next_reminder = started + HUMAN_WAIT_REMINDER
    while HUMAN_WAIT_TIMEOUT is None or loop.time() - started < HUMAN_WAIT_TIMEOUT:
        await asyncio.sleep(5)
        if await _page_state(page) in _PAGE_USABLE:
            print(f"▶️  Cleared. Resuming after {HUMAN_WAIT_COOLDOWN}s…")
            await asyncio.sleep(HUMAN_WAIT_COOLDOWN)
            return
        if loop.time() >= next_reminder:
            print(f"   ⏳ Still waiting for you to {what} ({(loop.time() - started) / 60:.0f} min so far)…")
            next_reminder += HUMAN_WAIT_REMINDER
    raise _RunStopped(f"Nobody cleared the {state} page in time.")


async def _goto_checked(page, url: str) -> None:
    await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
    await asyncio.sleep(random.uniform(3.0, 4.5))
    state = await _page_state(page)
    if state not in _PAGE_USABLE:
        await _wait_for_human(page, state)
        await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
        await asyncio.sleep(random.uniform(3.0, 4.5))


async def _read_items(page) -> dict:
    """Items currently rendered; pauses for a person first if a check is showing."""
    state = await _page_state(page)
    if state not in _PAGE_USABLE:
        await _wait_for_human(page, state)
    return await _evaluate(page, _JS_SEARCH_ITEMS)


async def _collect_page(page) -> dict:
    """Wait for the first page of results to render."""
    payload: dict = {"items": []}
    for _ in range(PAGE_POLL_ROUNDS):
        payload = await _read_items(page)
        if payload["items"]:
            break
        await asyncio.sleep(2)
    return payload


async def _wait_for_change(page, before: List[str]) -> dict:
    """After a click, wait until the rendered item list differs from `before`."""
    payload: dict = {"items": []}
    for _ in range(PAGE_POLL_ROUNDS):
        await asyncio.sleep(2)
        payload = await _read_items(page)
        if [i["item_id"] for i in payload["items"]] != before and payload["items"]:
            break
    return payload


async def _go_next_page(page, current: dict) -> Optional[dict]:
    """Click "下一页 / Next page". None when there is no further page."""
    button = page.locator("button", has_text=_NEXT_PAGE_RE).first
    if not await button.count() or await button.is_disabled():
        return None
    await button.scroll_into_view_if_needed()
    await button.click()
    return await _wait_for_change(page, [i["item_id"] for i in current["items"]])


async def _sort_by_sales(page, current: dict) -> dict:
    """Click the "销量 / Sales volume" sort tab; keeps the current order if it isn't there."""
    tab = page.get_by_text(_SORT_SALES_RE).first
    if not await tab.count():
        print("   ⚠️  Sales-volume sort not found — keeping the default order.")
        return current
    await tab.click()
    return await _wait_for_change(page, [i["item_id"] for i in current["items"]])


async def _pause_between_keywords(done: int, total: int) -> None:
    if done >= total:
        return
    pause = random.uniform(KEYWORD_PAUSE_MIN, KEYWORD_PAUSE_MAX)
    print(f"   ⏳ Waiting {pause:.0f}s before the next keyword…")
    await asyncio.sleep(pause)


# ════════════════════════════════════════════════════════════════
# Public API: Search
# ════════════════════════════════════════════════════════════════
async def run_search_keyword(
    keywords: List[str],
    max_pages_per_keyword: Optional[int] = DEFAULT_MAX_PAGES_PER_KEYWORD,
    sort_by_sales: bool = False,
    resume: Optional[bool] = None,
) -> pd.DataFrame:
    """
    Search each keyword and page through results by clicking "Next page".

    max_pages_per_keyword : 48 items per page; None = every page (site max 100)
    sort_by_sales         : click the sales-volume sort before collecting
    resume                : True = start from the checkpoint and skip keywords
                            already in it; None = RESUME_FROM_BACKUP

    Rows: item_id, product_name, price_cny, sales_text ("1000+人付款"),
    sold_count (its lower bound as a number), shop_name, location,
    category_path, leaf_category_id, is_ad, product_url, plus keyword/page/rank.
    Ads repeat across pages; each item is kept once per keyword.
    """
    global _human_waits, _human_wait_limit
    _human_waits = 0
    _human_wait_limit = MAX_HUMAN_WAITS_PER_RUN

    page_limit = min(max_pages_per_keyword or MAX_PAGES_HARD, MAX_PAGES_HARD)
    all_rows: List[dict] = _load_backup("temp_taobao_search", enabled=resume)
    done_keywords = {str(r.get("keyword_searched")) for r in all_rows}

    print(f"\n🔍 Taobao — Search  ({len(keywords)} keyword(s), up to {page_limit} page(s) each, "
          f"{'sorted by sales' if sort_by_sales else 'default order'})")

    try:
        context, page = await _create_browser_context()
        async with context:
            empty_in_a_row = 0
            for i, kw in enumerate(keywords, 1):
                if kw in done_keywords:
                    print(f"\n⏭️  [{i}/{len(keywords)}] '{kw}' — already collected, skip.")
                    continue

                url = f"{SEARCH_URL}?q={quote(kw)}&tab=all"
                print(f"\n🔑 [{i}/{len(keywords)}] '{kw}'  →  {url}")
                await _goto_checked(page, url)
                payload = await _collect_page(page)
                if payload["items"] and sort_by_sales:
                    payload = await _sort_by_sales(page, payload)

                if not payload["items"]:
                    empty_in_a_row += 1
                    print(f"   ⚠️  No products found for '{kw}'.")
                    await _dump_debug(page, f"empty_search_{kw}")
                    if empty_in_a_row >= MAX_EMPTY_KEYWORDS_IN_A_ROW:
                        raise _RunStopped(
                            f"{empty_in_a_row} keywords in a row came back empty — Taobao is "
                            "probably limiting requests. Wait a few hours, then run again and "
                            "continue from the checkpoint."
                        )
                    await _pause_between_keywords(i, len(keywords))
                    continue
                empty_in_a_row = 0

                seen: set = set()
                rows_kw: List[dict] = []
                page_no = 1
                while True:
                    new_items = [it for it in payload["items"] if it["item_id"] not in seen]
                    for item in new_items:
                        seen.add(item["item_id"])
                        rows_kw.append(_row(kw, page_no, len(rows_kw) + 1, item))
                    print(f"   📄 Page {page_no}: {len(new_items)} new item(s)  (total {len(rows_kw)})")

                    if page_no >= page_limit:
                        break
                    await _human_delay(PAGE_DELAY_MIN, PAGE_DELAY_MAX)
                    next_payload = await _go_next_page(page, payload)
                    if not next_payload or not any(it["item_id"] not in seen for it in next_payload["items"]):
                        print("   ⏹️  No further pages.")
                        break
                    payload = next_payload
                    page_no += 1

                all_rows.extend(rows_kw)
                done_keywords.add(kw)
                _save_checkpoint(all_rows, "temp_taobao_search", "auto")
                await _pause_between_keywords(i, len(keywords))
    except _RunStopped as e:
        print(f"\n🛑 Stopped: {e}")
    except Exception as e:
        print(f"\n🛑 Stopped by unexpected error — saving what was collected: {e}")

    _save_checkpoint(all_rows, "temp_taobao_search", "final")
    return _save_final(all_rows, "taobao_search_list")


# ════════════════════════════════════════════════════════════════
# Product detail: reading the page's own detail JSON
# ════════════════════════════════════════════════════════════════
_JSONP_RE = re.compile(r"^\s*[\w$.]+\s*\((.*)\)\s*;?\s*$", re.S)


def _item_id(value) -> Optional[str]:
    """Item id from a product URL (item.taobao.com / detail.tmall.com ?id=…) or a bare id."""
    text = str(value or "").strip()
    if re.fullmatch(r"\d{6,}", text):
        return text
    try:
        values = parse_qs(urlparse(text).query).get("id")
    except ValueError:
        return None
    return values[0] if values and values[0].isdigit() else None


def _loads_mtop(text: str) -> Optional[dict]:
    """An mtop response body — plain JSON or JSONP (mtopjsonp3({...}))."""
    text = (text or "").strip()
    match = _JSONP_RE.match(text)
    if match:
        text = match.group(1)
    try:
        data = json.loads(text)
    except ValueError:
        return None
    return data if isinstance(data, dict) else None


def _iter_nodes(obj, path: Tuple[str, ...] = (), depth: int = 0) -> Iterator[Tuple[Tuple[str, ...], object]]:
    """Every dict/list in a JSON tree with its key path. JSON held as a string
    (mtop 'valueType=string' payloads) is parsed and walked too."""
    if depth > 40:
        return
    if isinstance(obj, str):
        if len(obj) < 40 or obj[:1] not in "{[":
            return
        try:
            obj = json.loads(obj)
        except ValueError:
            return
    if isinstance(obj, dict):
        yield path, obj
        for key, value in obj.items():
            if isinstance(value, (dict, list, str)):
                yield from _iter_nodes(value, path + (str(key),), depth + 1)
    elif isinstance(obj, list):
        yield path, obj
        for index, value in enumerate(obj):
            if isinstance(value, (dict, list, str)):
                yield from _iter_nodes(value, path + (str(index),), depth + 1)


def _find_detail_root(payload) -> Optional[dict]:
    """The object holding item + seller/skuBase/componentsVO (the detail data)."""
    for _, node in _iter_nodes(payload):
        if (isinstance(node, dict) and isinstance(node.get("item"), dict)
                and any(k in node for k in ("skuBase", "seller", "componentsVO", "skuCore"))):
            return node
    return None


def _first_value(root, key: str):
    for _, node in _iter_nodes(root):
        if isinstance(node, dict) and node.get(key) not in (None, "", [], {}):
            return node[key]
    return None


def _as_text(value) -> str:
    if isinstance(value, (list, tuple)):
        return "、".join(t for t in (_as_text(v) for v in value) if t)
    if isinstance(value, dict):
        return _as_text(value.get("text") or value.get("value") or value.get("name") or "")
    text = re.sub(r"<[^>]+>", "", "" if value is None else str(value)).replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def _collect_attributes(root) -> Dict[str, str]:
    """The product's parameter list (品牌, 净含量, 产地 …) from the known layouts:
    {propertyName, valueName} entries (industryParamVO), a BASE_PROPS / 参数 group
    of {title, text}, or {name, value} entries somewhere under a 'param' key."""
    attrs: Dict[str, str] = {}

    def add(name, value):
        name, value = _as_text(name).rstrip(":："), _as_text(value)
        if name and value and name not in attrs:
            attrs[name] = value

    for path, node in _iter_nodes(root):
        if not isinstance(node, dict):
            continue
        if "propertyName" in node and ("valueName" in node or "value" in node):
            add(node["propertyName"], node.get("valueName", node.get("value")))
        items = node.get("items")
        if isinstance(items, list) and (str(node.get("type", "")).upper() == "BASE_PROPS"
                                        or "参数" in _as_text(node.get("title"))):
            for entry in items:
                if isinstance(entry, dict):
                    add(entry.get("title") or entry.get("name") or entry.get("propertyName"),
                        entry["text"] if "text" in entry else entry.get("value", entry.get("valueName")))
        if ("name" in node and "value" in node and not isinstance(node["value"], (dict, list))
                and any("param" in p.lower() for p in path)):
            add(node["name"], node["value"])
    return attrs


def _collect_skus(root) -> List[dict]:
    """[{sku_id, spec: '颜色分类: 01 | 规格: 40ml', price_text, promo_price_text, stock}]"""
    sku_base = _first_value(root, "skuBase")
    sku2info = _first_value(root, "sku2info")
    if not isinstance(sku_base, dict):
        return []
    sku2info = sku2info if isinstance(sku2info, dict) else {}
    names: Dict[str, str] = {}
    for prop in sku_base.get("props") or []:
        if not isinstance(prop, dict):
            continue
        for value in prop.get("values") or []:
            if isinstance(value, dict):
                names[f"{prop.get('pid')}:{value.get('vid')}"] = (
                    f"{_as_text(prop.get('name'))}: {_as_text(value.get('name'))}")

    def price_of(info: dict, key: str) -> str:
        price = info.get(key)
        return _as_text(price.get("priceText") if isinstance(price, dict) else price)

    skus = []
    for sku in sku_base.get("skus") or []:
        if not isinstance(sku, dict):
            continue
        sku_id = str(sku.get("skuId", ""))
        info = sku2info.get(sku_id) if isinstance(sku2info.get(sku_id), dict) else {}
        skus.append({
            "sku_id": sku_id,
            "spec": " | ".join(names.get(p, p) for p in str(sku.get("propPath", "")).split(";") if p),
            "price_text": price_of(info, "price"),
            "promo_price_text": price_of(info, "subPrice"),
            "stock": info.get("quantity"),
        })
    return skus


def _numbers(texts: List[str]) -> List[float]:
    return [float(n) for t in texts for n in re.findall(r"\d+(?:\.\d+)?", str(t).replace(",", ""))]


def _parse_detail(payload, item_id: Optional[str] = None) -> Optional[dict]:
    """Fields from a detail JSON. None when the payload has no detail object, or
    when it belongs to another item (a late response from a previous page)."""
    root = _find_detail_root(payload)
    if root is None:
        return None
    item = root.get("item") or {}
    page_item_id = _as_text(item.get("itemId") or item.get("id"))
    if item_id and page_item_id and page_item_id != str(item_id):
        return None
    seller = root.get("seller") if isinstance(root.get("seller"), dict) else {}
    attrs = _collect_attributes(root)
    skus = _collect_skus(root)

    # Price range from SKUs in stock — shops list placeholder SKUs (e.g. "0kg ⭐入会立减5元",
    # stock 0, ¥850–999) that would otherwise set the maximum. Stock unknown counts as in stock.
    def in_stock(sku: dict) -> bool:
        return str(sku.get("stock")).strip() not in ("0", "0.0")
    priced = [s for s in skus if s["price_text"]]
    price_texts = [s["price_text"] for s in (([s for s in priced if in_stock(s)]) or priced)]
    sku2info = _first_value(root, "sku2info")
    whole = sku2info.get("0") if isinstance(sku2info, dict) else None
    if not price_texts and isinstance(whole, dict) and isinstance(whole.get("price"), dict):
        price_texts = [_as_text(whole["price"].get("priceText"))]
    if not any(price_texts):
        price_texts = [_as_text(_first_value(root, "priceText"))]
    prices = _numbers([t for t in price_texts if t])
    promo = _numbers([s["promo_price_text"] for s in skus if s["promo_price_text"]])

    images = item.get("images") or []
    image = images[0] if images and isinstance(images[0], str) else ""
    if image.startswith("//"):
        image = "https:" + image

    return {
        "title": _as_text(item.get("title")),
        "shop_name_detail": _as_text(seller.get("shopName") or seller.get("sellerNick")),
        "shop_id": _as_text(seller.get("shopId")),
        "seller_type": _as_text(seller.get("sellerType")),   # 'B' = Tmall, 'C' = Taobao
        "ship_from": _as_text(_first_value(root, "deliveryFromAddr")),
        "sales_text_detail": _as_text(item.get("vagueSellCount") or _first_value(root, "vagueSellCount")
                                      or _first_value(root, "sellCount")),
        "price_min": min(prices) if prices else None,
        "price_max": max(prices) if prices else None,
        "promo_price_min": min(promo) if promo else None,
        "sku_count": len(skus),
        "skus_json": json.dumps(skus, ensure_ascii=False) if skus else "",
        "category_id": _as_text(item.get("categoryId") or item.get("cid") or item.get("rootCategoryId")),
        "image_url": image,
        "risk_flagged": _risk_flagged(root),
        "attributes": attrs,
    }


def _risk_flagged(root: dict) -> bool:
    feature = root.get("feature") if isinstance(root.get("feature"), dict) else {}
    return any(str(feature.get(flag, "")).lower() == "true" for flag in _RISK_FLAGS)


def _attribute_columns(attrs: Dict[str, str]) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Pull the common attributes into their own columns; the rest stay together."""
    rest = dict(attrs)
    columns: Dict[str, str] = {}
    for column, names in _ATTRIBUTE_COLUMNS.items():
        key = (next((n for n in names if n in rest), None)
               or next((k for k in rest for n in names if n in k), None))
        columns[column] = rest.pop(key) if key else ""
    return columns, rest


def _describe(attributes: Dict[str, str]) -> str:
    """'品牌: X | 净含量: 40ml | …' — the product's own parameter list in one cell."""
    return " | ".join(f"{key}: {value}" for key, value in attributes.items())


class _DetailCapture:
    """Keeps the detail JSON a product page loads by itself. Passive: it reads
    responses the page receives and never sends a request of its own."""

    def __init__(self):
        self.payload: Optional[dict] = None
        self.blocked_ret = ""
        self._tasks: set = set()

    def reset(self) -> None:
        self.payload, self.blocked_ret = None, ""

    def attach(self, page) -> None:
        self._handler = lambda response: self._spawn(self._read(response))
        page.on("response", self._handler)

    def _spawn(self, coroutine) -> None:
        task = asyncio.ensure_future(coroutine)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def drain(self) -> None:
        if self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)

    async def _read(self, response) -> None:
        if not any(hint in response.url for hint in _DETAIL_API_HINTS):
            return
        try:
            data = _loads_mtop(await response.text())
        except Exception:
            return
        if not data:
            return
        ret = " ".join(str(r) for r in data.get("ret") or [])
        if any(hint in ret for hint in _BLOCK_RET_HINTS):
            self.blocked_ret = ret
            return
        if ret and "SUCCESS" not in ret:
            return    # e.g. a token refresh — the page retries by itself
        if _find_detail_root(data) is not None:
            self.payload = data


# ════════════════════════════════════════════════════════════════
# Product detail: daily page budget & target selection
# ════════════════════════════════════════════════════════════════
def _today() -> str:
    return datetime.now(TZ_THAI).strftime("%Y-%m-%d")


def _read_quota() -> dict:
    try:
        return json.loads(QUOTA_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def detail_quota_left() -> Optional[int]:
    """Product pages still allowed today (Thai date); None = no daily limit."""
    if DAILY_DETAIL_LIMIT is None:
        return None
    return max(0, DAILY_DETAIL_LIMIT - int(_read_quota().get(_today(), 0)))


def _use_quota() -> None:
    today = _today()
    used = int(_read_quota().get(today, 0)) + 1
    QUOTA_FILE.write_text(json.dumps({today: used}), encoding="utf-8")


def _quota_used_up() -> bool:
    left = detail_quota_left()
    return left is not None and left <= 0


def select_best_sellers(df_search: pd.DataFrame,
                        sales_coverage: Optional[float] = DEFAULT_SALES_COVERAGE,
                        max_items: Optional[int] = None) -> pd.DataFrame:
    """
    One row per item, best sellers first, stopping once the chosen items make up
    `sales_coverage` of all sales in the file (0.8 = 80%). Items without a sales
    figure come last. sales_coverage None or ≥1 keeps every item.
    """
    if df_search is None or df_search.empty or "product_url" not in df_search.columns:
        return pd.DataFrame()
    df = df_search.copy()
    df["_item_id"] = df["product_url"].map(_item_id)
    df = df.dropna(subset=["_item_id"])
    df["_sold"] = pd.to_numeric(df["sold_count"], errors="coerce") if "sold_count" in df.columns else float("nan")
    df = df.sort_values("_sold", ascending=False, na_position="last").drop_duplicates("_item_id")
    sold = df["_sold"].fillna(0)
    if sales_coverage and sales_coverage < 1 and sold.sum() > 0:
        share = sold.cumsum() / sold.sum()
        df = df.head(int((share < sales_coverage).sum()) + 1)
    if max_items:
        df = df.head(max_items)
    return df.drop(columns=["_item_id", "_sold"]).reset_index(drop=True)


def _detail_targets(product_urls: Optional[List[str]], df_product: Optional[pd.DataFrame],
                    sales_coverage: Optional[float], max_items: Optional[int]) -> Dict[str, dict]:
    """{item_id: search row (or {})}. With a search dataframe only its best sellers are
    kept; pasted URLs that aren't in the dataframe at all are added after them."""
    targets: Dict[str, dict] = {}
    in_df: set = set()
    if df_product is not None and not df_product.empty and "product_url" in df_product.columns:
        in_df = {i for i in df_product["product_url"].map(_item_id) if i}
        for row in select_best_sellers(df_product, sales_coverage, max_items).to_dict(orient="records"):
            targets.setdefault(_item_id(row.get("product_url")), row)
    for url in product_urls or []:
        item_id = _item_id(url)
        if not item_id:
            print(f"   ⚠️  Skipping URL with no item id: {url}")
        elif item_id not in in_df:
            targets.setdefault(item_id, {})
    return targets


def checkpoint_summary(kind: str = "detail") -> Optional[dict]:
    """What a checkpoint holds ('listing' or 'detail'), so a caller can offer to continue."""
    if kind == "listing":
        return search_checkpoint_summary()
    path = Path(TEMP_DIR) / "temp_taobao_detail.csv"
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, dtype=str, usecols=["item_id", "collected_at"])
    except Exception:
        return None
    if df.empty:
        return None
    return {"rows": len(df), "products": df["item_id"].nunique(), "last_collected": df["collected_at"].max()}


# ════════════════════════════════════════════════════════════════
# Public API: Product Detail
# ════════════════════════════════════════════════════════════════
def _save_sample(item_id: str, payload) -> None:
    path = Path(LOG_DIR) / f"{datetime.now(TZ_THAI).strftime('%Y%m%d_%H%M%S')}_detail_sample_{item_id}.json"
    try:
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"      🧾 Raw detail data saved: {path}")
    except Exception as e:
        print(f"      ⚠️  Could not save sample: {e}")


async def _load_detail(page, capture: _DetailCapture, url: str, item_id: str):
    """Open one product page and read its data →
    (detail or None, source, raw payload, page shows the 访问异常提示 box)."""
    for attempt in (1, 2):
        capture.reset()
        await _goto_checked(page, url)
        for _ in range(DETAIL_POLL_ROUNDS):
            await capture.drain()
            if capture.payload is not None or capture.blocked_ret:
                break
            await asyncio.sleep(2)
        if capture.payload is None and capture.blocked_ret:
            state = await _page_state(page)
            if state in _PAGE_USABLE or attempt == 2:
                raise _RunStopped(
                    f"Taobao refused the product data ({capture.blocked_ret}). Stopping to protect "
                    "the account — wait a few hours, then run again; it continues from the checkpoint."
                )
            await _wait_for_human(page, state)
            continue
        break

    resisted = await _page_state(page) == "resisted"
    if capture.payload is not None:
        detail = _parse_detail(capture.payload, item_id)
        if detail:
            return detail, "mtop", capture.payload, resisted
    window_data = await _evaluate(page, _JS_WINDOW_DETAIL)
    detail = _parse_detail(window_data, item_id) if window_data else None
    return detail, ("page_data" if detail else ""), window_data, resisted


async def _dom_attributes(page) -> Dict[str, str]:
    """The "参数信息" table, after scrolling down to it (it renders lazily)."""
    try:
        await page.mouse.wheel(0, random.randint(1200, 2000))
        await asyncio.sleep(random.uniform(2.0, 3.5))
    except Exception:
        pass
    pairs = await _evaluate(page, _JS_DOM_PARAMS) or []
    attrs: Dict[str, str] = {}
    for name, value in pairs:
        name, value = _as_text(name).rstrip(":："), _as_text(value)
        if name and value:
            attrs.setdefault(name, value)
    return attrs


def _keep_old_checkpoint(data_prefix: str) -> None:
    """Starting over (resume off) would overwrite the checkpoint at the first save —
    copy it aside first so earlier days' rows are never lost."""
    path = Path(TEMP_DIR) / f"{data_prefix}.csv"
    if not path.exists():
        return
    backup = path.with_name(f"{data_prefix}_before_{datetime.now(TZ_THAI).strftime('%Y%m%d_%H%M%S')}.csv")
    shutil.copy2(path, backup)
    print(f"   🗂️  Starting over — the previous checkpoint is kept as {backup}")


def _row_complete(row: dict) -> bool:
    """A checkpoint row that needs no second visit: an 'ok' page that had its parameter
    list, or a removed item. 'degraded' rows (and older 'ok' rows saved without any
    parameters, from before degraded pages were recognised) are tried again."""
    status = str(row.get("page_status"))
    description = row.get("description")
    return status == "unavailable" or (
        status == "ok" and isinstance(description, str) and description.strip() != "")


def _put_row(rows: List[dict], row: dict) -> List[dict]:
    """Add a row, replacing an earlier one for the same item (e.g. a degraded retry)."""
    return [r for r in rows if str(r.get("item_id")) != row["item_id"]] + [row]


def _print_coverage(rows: List[dict]) -> None:
    status = pd.Series([str(r.get("page_status")) for r in rows]).value_counts().to_dict()
    if status:
        print(f"\n📊 Rows by page_status: {status}")
    ok = [r for r in rows if r.get("page_status") == "ok"]
    if not ok:
        return
    print(f"\n📈 Field coverage over {len(ok)} product page(s) read:")
    for col in ("title", "brand", "net_content", "origin", "spf", "registration_no",
                "ship_from", "price_min", "sku_count"):
        filled = sum(1 for r in ok if r.get(col) not in (None, "", 0) and not pd.isna(r.get(col)))
        print(f"   {col:<16} {filled / len(ok):>5.0%}")
    sources = pd.Series([r.get("data_source", "") for r in ok]).value_counts().to_dict()
    print(f"   data source      {sources}")


async def run_product_detail(
    product_urls: Optional[List[str]] = None,
    df_product: Optional[pd.DataFrame] = None,
    sales_coverage: Optional[float] = DEFAULT_SALES_COVERAGE,
    max_items: Optional[int] = None,
    resume: Optional[bool] = True,
) -> pd.DataFrame:
    """
    Open product pages one at a time and read brand, net content, origin, SPF,
    registration number, SKUs/prices, ship-from and the full parameter list.

    df_product     : a search dataframe/file (run_search_keyword). Only its best sellers
                     are opened — see select_best_sellers — and its price/sales columns
                     are copied across.
    product_urls   : item URLs or ids; with df_product, only ones not in it are added.
    sales_coverage : 0.8 = best sellers making up 80% of sales; None = every item.
    max_items      : hard cap on how many items are selected.
    resume         : True (default) = skip items already in the detail checkpoint —
                     a run normally spans several days because of DAILY_DETAIL_LIMIT.

    Rows: page_status 'ok', 'unavailable' (removed/sold-out page) or 'degraded'
    (Taobao flagged the session and hid the parameter list — price/SKUs only; the run
    stops there and a later run tries the item again). Blank pages are not recorded.
    """
    global _human_waits, _human_wait_limit
    _human_waits = 0
    _human_wait_limit = DETAIL_MAX_HUMAN_WAITS

    targets = _detail_targets(product_urls, df_product, sales_coverage, max_items)
    if not targets:
        print("❌ run_product_detail: no Taobao/Tmall item URLs or ids provided.")
        return pd.DataFrame()

    if not (RESUME_FROM_BACKUP if resume is None else resume):
        _keep_old_checkpoint("temp_taobao_detail")
    all_rows: List[dict] = _load_backup("temp_taobao_detail", enabled=resume)
    done = {str(r.get("item_id")) for r in all_rows if _row_complete(r)}
    todo = [(item_id, row) for item_id, row in targets.items() if item_id not in done]
    left = detail_quota_left()

    print(f"\n📝 Taobao — Product Detail")
    print(f"   Selected {len(targets):,} item(s)"
          + (f" (best sellers = {sales_coverage:.0%} of sales)" if df_product is not None and sales_coverage else "")
          + f", {len(targets) - len(todo):,} already in the checkpoint, {len(todo):,} to go.")
    if left is not None:
        print(f"   Today's page budget: {left} of {DAILY_DETAIL_LIMIT}"
              + (f" → about {-(-len(todo) // DAILY_DETAIL_LIMIT)} day(s) for the rest."
                 if todo and DAILY_DETAIL_LIMIT else "."))
    if not todo or _quota_used_up():
        if todo:
            print("   ⏸️  Today's limit is used up — run again tomorrow; it continues from the checkpoint.")
        return _save_final(all_rows, "taobao_detail", DETAIL_DEDUPE_COLS) if all_rows else pd.DataFrame()

    capture = _DetailCapture()
    interrupted: Optional[BaseException] = None
    samples_saved = 0
    shape_dumped = False
    empty_in_a_row = 0
    opened = 0

    try:
        context, page = await _create_browser_context()
        async with context:
            capture.attach(page)
            for i, (item_id, source) in enumerate(todo, 1):
                if _quota_used_up():
                    print(f"\n⏸️  Daily limit of {DAILY_DETAIL_LIMIT} product pages reached — "
                          "run again tomorrow; it continues from the checkpoint.")
                    break
                if opened and opened % DETAIL_REST_EVERY == 0:
                    rest = random.uniform(DETAIL_REST_MIN, DETAIL_REST_MAX)
                    print(f"   😴 Resting {rest / 60:.1f} min after {opened} pages…")
                    await asyncio.sleep(rest)

                url = ITEM_URL_TEMPLATE.format(item_id=item_id)
                print(f"   ⏳ {i}/{len(todo)} → {url}")
                _use_quota()
                opened += 1
                detail, data_source, raw, resisted = await _load_detail(page, capture, url, item_id)

                if raw is not None and samples_saved < SAVE_SAMPLE_DETAILS:
                    _save_sample(item_id, raw)
                    samples_saved += 1

                if resisted:
                    print("      ⚠️  The page shows Taobao's 访问异常提示 box — details are hidden.")
                    await _dump_debug(page, f"resisted_{item_id}")
                    if detail is None:
                        raise _RunStopped(_DEGRADED_STOP_MESSAGE)
                    detail["risk_flagged"] = True
                elif detail is None or not detail["attributes"]:
                    dom_attrs = await _dom_attributes(page)
                    if dom_attrs:
                        if detail is None:
                            detail, data_source = {"attributes": {}}, "dom"
                        detail["attributes"] = dom_attrs
                        data_source = data_source if data_source == "dom" else f"{data_source}+dom"

                product_url = (source.get("product_url") if isinstance(source.get("product_url"), str)
                               else f"https://item.taobao.com/item.htm?id={item_id}")
                carried = {col: source.get(col) for col in _SEARCH_FIELDS_FOR_DETAIL if col in source}

                if detail is None:
                    state = await _evaluate(page, _JS_PAGE_STATE)
                    if any(p in state["text"] for p in _UNAVAILABLE_PHRASES):
                        print("      ⚠️  Page says the item is gone (下架/不存在) — recorded without details.")
                        all_rows = _put_row(all_rows, {
                            "scraped_date": _scraped_date(), "item_id": item_id,
                            "page_status": "unavailable", "data_source": "",
                            "product_url": product_url, "final_url": page.url,
                            **carried, "collected_at": _collected_at(),
                        })
                        done.add(item_id)
                        empty_in_a_row = 0
                    else:
                        print("      ⚠️  No product data found on the page — skipped (tried again next run).")
                        await _dump_debug(page, f"empty_detail_{item_id}")
                        empty_in_a_row += 1
                        if empty_in_a_row >= MAX_EMPTY_DETAILS_IN_A_ROW:
                            raise _RunStopped(
                                f"{empty_in_a_row} product pages in a row came back without data — Taobao "
                                "may be limiting this account. Check the screenshot in the log folder and "
                                "wait a few hours before the next run."
                            )
                else:
                    empty_in_a_row = 0
                    attrs = detail.pop("attributes")
                    degraded = bool(detail.get("risk_flagged")) and not attrs
                    columns, rest = _attribute_columns(attrs)
                    row = {
                        "scraped_date": _scraped_date(),
                        "item_id": item_id,
                        "page_status": "degraded" if degraded else "ok",
                        "data_source": data_source,
                        "product_url": product_url,
                        "final_url": page.url,
                        "is_tmall": detail.get("seller_type") == "B" or ".tmall." in page.url,
                        **detail,
                        **columns,
                        "ship_from": detail.get("ship_from") or rest.pop("发货地", ""),
                        "other_attributes": json.dumps(rest, ensure_ascii=False) if rest else "",
                        "description": _describe(attrs),
                        **carried,
                        "collected_at": _collected_at(),
                    }
                    all_rows = _put_row(all_rows, row)
                    if degraded:
                        print("      ⚠️  Kept as 'degraded' (title/price/SKUs only) — tried again next run.")
                        raise _RunStopped(_DEGRADED_STOP_MESSAGE)
                    done.add(item_id)
                    got = [c for c in ("brand", "net_content", "price_min") if row.get(c) not in (None, "")]
                    print(f"      ✅ {data_source}: {len(attrs)} attribute(s), {row.get('sku_count', 0)} SKU(s)"
                          f"  [{', '.join(got) or 'no brand/net content/price'}]")
                    if not got and not shape_dumped:
                        await _dump_debug(page, f"detail_shape_{item_id}")
                        shape_dumped = True

                if opened % DETAIL_CHECKPOINT_EVERY == 0:
                    _save_checkpoint(all_rows, "temp_taobao_detail", "auto", DETAIL_DEDUPE_COLS, "last")
                if i < len(todo) and not _quota_used_up():
                    await _human_delay(DETAIL_DELAY_MIN, DETAIL_DELAY_MAX)
    except _RunStopped as e:
        print(f"\n🛑 Stopped: {e}")
    except (asyncio.CancelledError, KeyboardInterrupt) as e:
        # Ctrl+C: save first, then let the interrupt carry on.
        print("\n🛑 Interrupted — saving what was collected before exiting…")
        interrupted = e
    except Exception as e:
        print(f"\n🛑 Stopped by unexpected error — saving what was collected: {e}")

    remaining = len([1 for item_id in targets if item_id not in done])
    if remaining:
        print(f"\n📌 {remaining:,} selected item(s) still to go — run action 3 again "
              "(tomorrow if today's budget is used up).")
    _print_coverage(all_rows)
    _save_checkpoint(all_rows, "temp_taobao_detail", "final", DETAIL_DEDUPE_COLS, "last")
    df_out = _save_final(all_rows, "taobao_detail", DETAIL_DEDUPE_COLS, "last")
    if interrupted is not None:
        raise interrupted
    return df_out
