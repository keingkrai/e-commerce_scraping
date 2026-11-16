"""
cloak_pinduoduo.py
────────────────────────────────────────────────────────────────
Pinduoduo (mobile.pinduoduo.com) scraper — mirrors cloak_konvy.py structure.

Things specific to this site, confirmed on the live pages:
  • Every page redirects to login.html without a signed-in session, so a
    saved session is required (create_pinduoduo_session / action 0).
  • Search and review pages load more items as you scroll. Only the first
    page sits in window.rawData; later pages exist only as props on the
    page's React components, so items are read by walking the React tree.
  • Some product pages mask their price in the UI ("8.?", "view in the app"),
    so price always comes from the search listing. run_product_detail leaves
    price empty unless a search dataframe is passed in. Hidden price fields in
    the page's internal data are deliberately not read.
  • Listing price is before coupons; a listing can show a lower 券后
    (after-coupon) price next to it.
  • Category pages: the home page has ~23 top-level tabs (?lastTabItemID=16
    is 美妆), each with ~15 sub-categories that open as
    catgoods.html?opt_id=…. Sub-category listings share the search item shape.
  • Reviewer names are deliberately not collected.
  • A loaded shop page (mall_page.html) has NOT been seen yet, only its "shop
    not found" state, so the item reader accepts several field spellings. When
    a shop comes back empty or without price/sales, the field names on that
    page are saved to LOG_DIR as *_shop_shape_*.json.

Anti-bot handling: this module never solves a CAPTCHA. If a verification
page or the login page appears, the run pauses so a person can deal with it
in the open browser window (no time limit by default), and stops if it keeps
recurring. Volume is capped by default because the session belongs to a real
account (robots.txt disallows crawling for all generic user agents).

Public entry-points:
  create_pinduoduo_session()               log in manually and save the session
  run_search_keyword(keywords)         →   pd.DataFrame  listing rows (price, sales tip, …)
  run_category_products(categories)    →   pd.DataFrame  listing rows for whole categories
  run_shop_products(shops)             →   pd.DataFrame  listing rows per shop
  run_product_detail(product_urls)     →   pd.DataFrame  attribute rows (brand, origin, …)
  run_product_reviews(product_urls)    →   pd.DataFrame  review rows
"""

import asyncio
import json
import random
import re
import sys
from collections import Counter
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Awaitable, Callable, Dict, List, Optional, Tuple, Union
from urllib.parse import parse_qs, quote, urlparse

import pandas as pd
from cloakbrowser import launch_persistent_context_async

# ════════════════════════════════════════════════════════════════
# Directories & Config
# ════════════════════════════════════════════════════════════════

USER_DATA_DIR = "./data/pinduoduo/my_session"
OUTPUT_DIR    = "./data/pinduoduo/collected_data"
TEMP_DIR      = "./data/pinduoduo/collected_data/temp"
LOG_DIR       = "./data/pinduoduo/log"

BASE_URL = "https://mobile.pinduoduo.com"

# Slower than the other platforms on purpose: this runs on a real account.
REQUEST_DELAY_MIN = 3.0
REQUEST_DELAY_MAX = 7.0
# Raised from 2.5–5s after a run of ~32 quick scrolls got rate-limited.
SCROLL_DELAY_MIN  = 6.0
SCROLL_DELAY_MAX  = 12.0
CATEGORY_PAUSE_MIN = 60     # seconds between category pages (min/max)
CATEGORY_PAUSE_MAX = 180

DEFAULT_MAX_PRODUCTS_PER_KEYWORD = 200
DEFAULT_MAX_REVIEWS_PER_PRODUCT  = 200
DEFAULT_MAX_PRODUCTS_PER_CATEGORY = 200   # per sub-category
DEFAULT_MAX_PRODUCTS_PER_SHOP = 200

MAX_STALLED_SCROLLS = 3     # stop after this many scrolls with nothing new
MAX_SCROLL_ROUNDS   = 300    # hard ceiling, applies even with no item limit
SCROLL_REST_EVERY   = 10     # every N scrolls, take a longer rest…
SCROLL_REST_MIN     = 30     # …of this many seconds (min/max)
SCROLL_REST_MAX     = 60

# Seen in practice: after ~32 quick scrolls, the next category pages stayed on
# a loading spinner with no login/verification page — most likely the site
# rate-limiting. Stop instead of hitting more pages while that lasts.
MAX_EMPTY_CATEGORIES_IN_A_ROW = 2
MAX_EMPTY_DETAILS_IN_A_ROW = 3   # blank product pages before run_product_detail stops
DETAIL_POLL_ROUNDS  = 8      # polls of 2s waiting for the product page to hydrate

PAGE_TIMEOUT_MS         = 60000
HUMAN_WAIT_TIMEOUT      = None   # seconds to wait for a person at login/verification; None = no limit
HUMAN_WAIT_REMINDER     = 300    # while waiting, print a reminder every N seconds
HUMAN_WAIT_COOLDOWN     = 30     # seconds to pause after a person cleared it
MAX_HUMAN_WAITS_PER_RUN = 3      # stop instead of pushing on after repeated checks
LOGIN_BOUNCE_POLLS      = 5      # polls of 3s before treating a login page as real

CHECKPOINT_EVERY_PRODUCTS = 10

RESUME_FROM_BACKUP = False

SEARCH_DEDUPE_COLS = ["keyword_searched", "goods_id"]
CATEGORY_DEDUPE_COLS = ["category_opt_id", "goods_id"]
SHOP_DEDUPE_COLS = ["shop_key", "goods_id"]
DETAIL_DEDUPE_COLS = ["product_url"]
REVIEW_DEDUPE_COLS = ["review_id"]

# Columns copied from a search dataframe onto detail rows.
_SEARCH_FIELDS_FOR_DETAIL = ["price_cny", "sales_tip", "sold_count", "sold_scope", "image_url"]

# Not observed yet — no verification page appeared while this was written, so
# these are best guesses. An unmatched check shows up as an empty result, which
# dumps HTML + a screenshot to LOG_DIR for diagnosis.
_HUMAN_CHECK_PATH_HINTS = ("verification", "captcha")
_HUMAN_CHECK_PHRASES = (
    "安全验证", "滑动验证", "拖动滑块", "请完成验证", "点击完成验证",
    "访问频繁", "操作频繁", "请求频繁",
)
_HUMAN_CHECK_MAX_TEXT_LEN = 1500   # real pages can mention these words in reviews
# Pinduoduo's error page (手机网络繁忙，请稍后再试). There is nothing for a person to
# clear and more requests won't help, so a run stops as soon as it appears.
_BUSY_PHRASES = ("网络繁忙",)
# A product page for something that can't be bought (商品已售罄，推荐以下相似商品).
_SOLD_OUT_PHRASES = ("已售罄",)

for _folder in [OUTPUT_DIR, TEMP_DIR, LOG_DIR]:
    Path(_folder).mkdir(parents=True, exist_ok=True)

TZ_THAI = timezone(timedelta(hours=7))

_human_waits = 0

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

_JS_REACT_WALK = r"""
  const rootFiber = () => {
    for (const el of document.querySelectorAll('div')) {
      const key = Object.keys(el).find(k => k.startsWith('__reactFiber'));
      if (key) { let f = el[key]; while (f.return) f = f.return; return f; }
    }
    return null;
  };
  const walkProps = (visit) => {
    const root = rootFiber();
    if (!root) return;
    const stack = [root];
    let guard = 0;
    while (stack.length && guard++ < 200000) {
      const node = stack.pop();
      const props = node.memoizedProps;
      if (props && typeof props === 'object') visit(props);
      if (node.sibling) stack.push(node.sibling);
      if (node.child) stack.push(node.child);
    }
  };
"""

_JS_PAGE_STATE = r"""() => {
  const text = document.body ? document.body.innerText.replace(/​/g, '') : '';
  return { path: location.pathname.toLowerCase(), textLen: text.length, text: text.slice(0, 2000) };
}"""

# Search pages and category (catgoods) pages share this item shape.
_JS_LISTING_ITEMS = "() => {" + _JS_REACT_WALK + r"""
  const seen = new Map();
  const add = (v) => {
    if (!v || typeof v !== 'object' || !v.goodsID || !v.goodsName) return;
    const id = String(v.goodsID);
    if (seen.has(id)) return;
    seen.set(id, {
      goods_id: id,
      product_name: v.goodsName,
      price_cent: v.price,
      sales_tip: v.salesTip || '',
      mall_id: v.mallEntrance ? v.mallEntrance.mall_id : null,
      image_url: v.imgUrl || '',
      tags: (v.tagList || v.tag_list || []).map(t => t && (t.text || t.desc) || '').filter(Boolean).join(' | '),
    });
  };
  let ssr = null;
  try { ssr = window.rawData.stores.store.data.ssrListData; } catch (e) {}
  ((ssr && ssr.list) || []).forEach(add);
  walkProps((props) => {
    add(props);
    for (const key of Object.keys(props)) {
      const v = props[key];
      if (v && typeof v === 'object' && !Array.isArray(v)) add(v);
    }
  });
  return {
    items: [...seen.values()],
    total: ssr ? ssr.filterTotalNumStr : null,
    flagged: !!(ssr && (ssr.isRisk || ssr.isBlack)),
  };
}"""

# Home page (?lastTabItemID=N): top-level tabs and the current tab's sub-categories.
_JS_CATEGORY_TREE = "() => {" + _JS_REACT_WALK + r"""
  const arr = (x) => !x ? [] : Array.isArray(x) ? x : (typeof x.slice === 'function' ? x.slice() : []);
  let store = null;
  walkProps((props) => {
    const s = props.store;
    if (!store && s && s.tabDataMap && typeof s.tabDataMap.get === 'function') store = s;
  });
  if (!store) return null;
  const tab = store.tabDataMap.get(String(store.currentTabItemID));
  return {
    current: store.currentTabItemID,
    tabs: arr(store.operations).map(o => ({ opt_id: o.optID, name: o.tabName })),
    subs: tab ? arr(tab.categories).map(c => ({ opt_id: c.optID, name: c.optName })) : [],
  };
}"""

_JS_PRODUCT_DETAIL = "() => {" + _JS_REACT_WALK + r"""
  let init = null;
  walkProps((props) => {
    if (!init && props.store && props.store.initDataObj) init = props.store.initDataObj;
  });
  // The id alone is there before the page has loaded; a row with only an id is empty.
  if (!init || !init.goods || !init.goods.goodsID || !init.goods.goodsName) return null;
  const g = init.goods;
  const text = document.body.innerText.replace(/​/g, '');
  const from = text.indexOf('商品详情');
  const section = from >= 0 ? text.slice(from) : '';
  const attributes = {};
  for (const el of document.querySelectorAll('div')) {
    if (el.children.length !== 2 || el.textContent.length > 200) continue;
    const label = el.children[0].textContent.trim();
    const value = el.children[1].textContent.trim();
    if (!label || !value || label.length > 12 || label in attributes) continue;
    if (section.includes(label + value)) attributes[label] = value;
  }
  // 消费提醒 is a heading with its text on the next line, so it is not in the label+value table.
  let notice = '';
  const noticeAt = section.indexOf('消费提醒');
  if (noticeAt >= 0) {
    const lines = section.slice(noticeAt + 4).split('\n').map(s => s.trim()).filter(Boolean);
    const stop = lines.findIndex(l => /^(点击查看|顶部)/.test(l));
    notice = (stop >= 0 ? lines.slice(0, stop) : lines.slice(0, 2)).join(' ').slice(0, 300);
  }
  return {
    goods_id: String(g.goodsID),
    product_name: g.goodsName,
    cat_id_1: g.catID1,
    cat_id_2: g.catID2,
    cat_id_3: g.catID3,
    brand_id: g.brandId,
    mall_id: g.mallID,
    is_on_sale: g.isOnSale,
    sku_count: (g.skus || []).length,
    attributes,
    consumer_notice: notice,
  };
}"""

_JS_REVIEW_ITEMS = "() => {" + _JS_REACT_WALK + r"""
  const seen = new Map();
  const add = (v) => {
    if (!v || typeof v !== 'object' || !v.reviewId) return;
    const id = String(v.reviewId);
    if (seen.has(id)) return;
    seen.set(id, {
      review_id: id,
      comment: v.comment || '',
      review_date: v.time || '',
      rating: v.comprehensiveDsr,
      desc_score: v.descScore,
      logistics_score: v.logisticsScore,
      service_score: v.serviceScore,
      specs: v.specs || '',
      favor_count: v.favorCount,
      reply_count: v.replyCount,
      picture_count: (v.pictures || []).length,
    });
  };
  const store = window.rawData && window.rawData.store;
  ((store && store.commentsList) || []).forEach(add);
  walkProps((props) => {
    for (const key of Object.keys(props)) {
      const v = props[key];
      if (Array.isArray(v)) { if (v.length && v[0] && v[0].reviewId) v.forEach(add); }
      else if (v && typeof v === 'object') add(v);
    }
  });
  const all = store && (store.tags || []).find(t => String(t.id) === '0');
  return { items: [...seen.values()], total: all ? all.num : null };
}"""

# Shop pages were not seen live when this was written, so instead of reading one
# known path, every object in window.rawData and the React props is visited.
_JS_DEEP_SCAN = r"""
  const SKIP_KEYS = new Set(['children', 'ref', 'stateNode', 'return', 'child', 'sibling', 'alternate']);
  const visited = new WeakSet();
  const valuesOf = (o) => {
    if (Array.isArray(o)) return o;
    try {
      if (typeof o.slice === 'function' && typeof o.length === 'number') return o.slice();
      if (typeof o.get === 'function' && typeof o.values === 'function') return [...o.values()];
    } catch (e) {}
    return null;
  };
  const deepScan = (o, visit, depth) => {
    if (!o || typeof o !== 'object' || depth > 12 || visited.has(o) || o === window || o instanceof Node) return;
    visited.add(o);
    const values = valuesOf(o);
    if (values) { values.slice(0, 3000).forEach(v => deepScan(v, visit, depth + 1)); return; }
    visit(o);
    for (const key of Object.keys(o)) {
      if (SKIP_KEYS.has(key) || key[0] === '_' || key[0] === '$') continue;
      let v;
      try { v = o[key]; } catch (e) { continue; }
      deepScan(v, visit, depth + 1);
    }
  };
  const scanPage = (visit) => {
    deepScan(window.rawData, visit, 0);
    walkProps((props) => deepScan(props, visit, 0));
  };
"""

# Only listing-price fields are read, in line with the note on masked prices above.
_JS_SHOP_ITEMS = "() => {" + _JS_REACT_WALK + _JS_DEEP_SCAN + r"""
  const pick = (v, keys) => {
    for (const k of keys) { if (v[k] !== undefined && v[k] !== null && v[k] !== '') return v[k]; }
    return null;
  };
  const items = new Map();
  const malls = new Map();
  scanPage((v) => {
    const mallId = pick(v, ['mallID', 'mall_id', 'mallId']);
    const mallName = pick(v, ['mallName', 'mall_name']);
    if (mallId && mallName && !malls.has(String(mallId))) {
      malls.set(String(mallId), {
        id: String(mallId),
        name: mallName,
        goods_num: pick(v, ['goodsNum', 'goods_num']),
        sales: pick(v, ['salesNum', 'sales_num', 'mallSales', 'mall_sales', 'salesTip', 'sales_tip']),
      });
    }
    const id = pick(v, ['goodsID', 'goods_id', 'goodsId']);
    const name = pick(v, ['goodsName', 'goods_name', 'shortName', 'short_name']);
    if (!id || !name || items.has(String(id))) return;
    const group = v.group && typeof v.group === 'object' ? v.group : {};
    const price = pick(v, ['price', 'groupPrice', 'group_price', 'minGroupPrice', 'min_group_price']) ?? pick(group, ['price']);
    const ownerId = mallId || (v.mallEntrance ? v.mallEntrance.mall_id : null);
    const tags = v.tagList || v.tag_list;
    items.set(String(id), {
      goods_id: String(id),
      product_name: name,
      price_cent: typeof price === 'number' ? price : null,
      sales_tip: pick(v, ['salesTip', 'sales_tip']) || '',
      sold_cnt_raw: pick(v, ['cnt', 'soldQuantity', 'sold_quantity']),
      item_mall_id: ownerId ? String(ownerId) : null,
      image_url: pick(v, ['imgUrl', 'thumbUrl', 'thumb_url', 'hdThumbUrl', 'hd_thumb_url']) || '',
      tags: Array.isArray(tags) ? tags.map(t => t && (t.text || t.desc) || '').filter(Boolean).join(' | ') : '',
    });
  });
  const basic = window.rawData && window.rawData.store && window.rawData.store.mallBasicInfo;
  return {
    items: [...items.values()],
    malls: [...malls.values()],
    shop_missing: !!(basic && !Object.keys(basic).length && !items.size),
  };
}"""

# Field names and the type of price/sales fields, never their values: enough to
# adjust _JS_SHOP_ITEMS to the real page.
_JS_SHOP_SHAPE = "() => {" + _JS_REACT_WALK + _JS_DEEP_SCAN + r"""
  const shapes = new Map();
  const goodsTypes = [];
  const describe = (x) => x === null ? 'null' : Array.isArray(x) ? 'array(' + x.length + ')'
    : typeof x === 'object' ? 'object{' + Object.keys(x).slice(0, 8).join(',') + '}' : typeof x;
  scanPage((v) => {
    const keys = Object.keys(v);
    if (goodsTypes.length < 3 && (v.goodsId || v.goodsID || v.goods_id)) {
      const types = {};
      keys.filter(k => /price|sales|tip|cnt|quantity|sold/i.test(k)).forEach(k => {
        try { types[k] = describe(v[k]); } catch (e) { types[k] = 'unreadable'; }
      });
      goodsTypes.push(types);
    }
    if (shapes.size >= 40 || !keys.some(k => /goods|mall/i.test(k))) return;
    const shown = keys.slice(0, 60).sort();
    shapes.set(shown.join(','), shown);
  });
  return { path: location.pathname, shapes: [...shapes.values()], goods_field_types: goodsTypes };
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


async def _human_delay(min_d: float = REQUEST_DELAY_MIN, max_d: float = REQUEST_DELAY_MAX):
    await asyncio.sleep(random.uniform(min_d, max_d))


def _goods_id_from_url(url: str) -> Optional[str]:
    try:
        values = parse_qs(urlparse(str(url)).query).get("goods_id")
    except ValueError:
        return None
    return values[0] if values and values[0].isdigit() else None


def _product_url(goods_id: str) -> str:
    return f"{BASE_URL}/goods.html?goods_id={goods_id}"


def _parse_sales_tip(tip: Optional[str]) -> Tuple[Optional[int], Optional[str]]:
    """
    Turn a listing's sales text into (count, scope). The scope matters because
    these numbers measure different things and must not be compared directly:
      '本店已拼14.9万+件'    → (149000, 'shop')   whole-shop total, not this product
      '品牌热销12万+件'      → (120000, 'brand')  whole-brand total, not this product
      '24小时内500+人拼单'   → (500, 'last_24h')  people in the last 24 hours
      '热卖4911件'           → (4911, 'product')
      '49人想拼' / '5万人想买' → (49, 'interest')   people waiting on a pre-sale, not units sold
      '全店好评12万条'       → (120000, 'reviews') review count, not sales
    """
    text = str(tip or "").strip()
    if not text or text == "nan":
        return None, None
    scope = (
        "reviews" if "好评" in text else
        "interest" if "想拼" in text or "想买" in text else
        "shop" if "本店" in text else
        "brand" if "品牌" in text else
        "last_24h" if "24小时" in text else
        "product"
    )
    match = re.search(r"(\d+(?:\.\d+)?)\s*(万|亿)?", re.sub(r"24小时内?", "", text))
    if not match:
        return None, scope
    multiplier = {"万": 10_000, "亿": 100_000_000}.get(match.group(2), 1)
    return int(round(float(match.group(1)) * multiplier)), scope


def _parse_specs(specs: Optional[str]) -> str:
    """'[{"spec_key":"款式","spec_value":"A"}]' → '款式: A'"""
    if not specs:
        return ""
    try:
        parts = json.loads(specs)
        return " | ".join(f"{p.get('spec_key', '')}: {p.get('spec_value', '')}" for p in parts)
    except (TypeError, ValueError, AttributeError):
        return str(specs)


def _describe(attributes: dict, notice: str = "") -> str:
    """'品牌: X | 保质期: 1095天 | 消费提醒: …' — the product page's own text in one cell."""
    parts = [f"{key}: {value}" for key, value in attributes.items()]
    if notice:
        parts.append(f"消费提醒: {notice}")
    return " | ".join(parts)


def _listing_row(item: dict, lead_cols: dict) -> dict:
    """One output row from a search/category listing item; `lead_cols` go first."""
    item = dict(item)
    price_cent = item.pop("price_cent", None)
    sold_count, sold_scope = _parse_sales_tip(item.get("sales_tip"))
    return {
        "scraped_date": _scraped_date(),
        **lead_cols,
        **item,
        "price_cny": round(price_cent / 100, 2) if isinstance(price_cent, (int, float)) else None,
        "sold_count": sold_count,
        "sold_scope": sold_scope,
        "product_url": _product_url(item["goods_id"]),
        "collected_at": _collected_at(),
    }


def _save_checkpoint(rows: list, data_prefix: str, dedupe_cols: Optional[List[str]] = None,
                     reason: str = "checkpoint"):
    if not rows:
        return
    df = pd.DataFrame(rows)
    if dedupe_cols:
        valid = [c for c in dedupe_cols if c in df.columns]
        if valid:
            df = df.drop_duplicates(subset=valid)
    path = Path(TEMP_DIR) / f"{data_prefix}.csv"
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"   💾 Checkpoint [{reason}]: {path}  ({len(df):,} rows)")


def _sanitize_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    """Strip control characters openpyxl refuses to write into a cell."""
    from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
    str_cols = df.select_dtypes(include="object").columns
    for col in str_cols:
        df[col] = df[col].map(lambda v: ILLEGAL_CHARACTERS_RE.sub("", v) if isinstance(v, str) else v)
    return df


def _save_final(rows: list, final_prefix: str, dedupe_cols: Optional[List[str]] = None) -> pd.DataFrame:
    if not rows:
        print(f"   ⚠️  No data to save: {final_prefix}")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    if dedupe_cols:
        valid = [c for c in dedupe_cols if c in df.columns]
        if valid:
            df = df.drop_duplicates(subset=valid)

    xlsx = Path(OUTPUT_DIR) / f"{final_prefix}_{_make_timestamp()}.xlsx"
    df = _sanitize_for_excel(df)
    df.to_excel(xlsx, index=False)

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
        # IDs stay strings: review ids are 18 digits and lose precision as numbers.
        df = pd.read_csv(path, dtype={"goods_id": str, "review_id": str, "mall_id": str,
                                      "category_opt_id": str, "shop_key": str, "item_mall_id": str})
        rows = df.to_dict(orient="records")
        print(f"🔁 Backup loaded: {path}  ({len(rows):,} rows)")
        return rows
    except Exception as e:
        print(f"⚠️  Failed to load backup {path}: {e}")
        return []


def _targets_from_input(product_urls: Optional[List[str]],
                        df_product: Optional[pd.DataFrame]) -> Dict[str, dict]:
    """Merge URLs + a dataframe into {goods_id: row-or-empty}, in first-seen order."""
    targets: Dict[str, dict] = {}
    if df_product is not None and not df_product.empty and "product_url" in df_product.columns:
        for row in df_product.to_dict(orient="records"):
            goods_id = _goods_id_from_url(row.get("product_url"))
            if goods_id and goods_id not in targets:
                targets[goods_id] = row
    for url in product_urls or []:
        goods_id = _goods_id_from_url(url)
        if goods_id:
            targets.setdefault(goods_id, {})
        else:
            print(f"   ⚠️  Skipping URL with no goods_id: {url}")
    return targets


# ════════════════════════════════════════════════════════════════
# Browser session
# ════════════════════════════════════════════════════════════════
async def _create_browser_context():
    context = await launch_persistent_context_async(
        USER_DATA_DIR,
        headless=False,
        viewport={"width": 800, "height": 900},
        locale="th-TH",
        timezone_id="Asia/Bangkok",
        humanize=True,
    )
    page = context.pages[0] if context.pages else await context.new_page()
    return context, page


async def create_pinduoduo_session():
    """Log in by hand and save the session. Run on first use or after it expires."""
    context, page = await _create_browser_context()
    await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
    print("\n" + "—" * 60)
    print("⚡ [SESSION] Browser is active.")
    print("—" * 60)
    print(" 1. Log in to Pinduoduo manually (phone + SMS code, or QR).")
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


async def _page_state(page) -> str:
    """'ok', 'login', 'human_check' or 'busy'."""
    state = await _evaluate(page, _JS_PAGE_STATE)
    if "login" in state["path"]:
        return "login"
    if state["textLen"] < _HUMAN_CHECK_MAX_TEXT_LEN and any(p in state["text"] for p in _BUSY_PHRASES):
        return "busy"
    if any(hint in state["path"] for hint in _HUMAN_CHECK_PATH_HINTS):
        return "human_check"
    if state["textLen"] < _HUMAN_CHECK_MAX_TEXT_LEN and any(p in state["text"] for p in _HUMAN_CHECK_PHRASES):
        return "human_check"
    return "ok"


async def _page_says_sold_out(page) -> bool:
    state = await _evaluate(page, _JS_PAGE_STATE)
    return state["textLen"] < _HUMAN_CHECK_MAX_TEXT_LEN and any(p in state["text"] for p in _SOLD_OUT_PHRASES)


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
    """Pause until a person clears the login/verification page in the browser
    window. Nothing here attempts to solve or get around it."""
    global _human_waits

    if state == "busy":
        await _dump_debug(page, "busy")
        raise _RunStopped(
            "Pinduoduo answered 手机网络繁忙 (network busy, try again later). Stopping instead of "
            "opening more pages — wait 30–60 minutes before trying again."
        )

    # The login page has been seen clearing by itself within seconds on a
    # signed-in session (cause unknown), so don't ask for a login right away.
    if state == "login":
        for _ in range(LOGIN_BOUNCE_POLLS):
            await asyncio.sleep(3)
            if await _page_state(page) == "ok":
                print("   ↪️  Login page cleared by itself — continuing.")
                return

    _human_waits += 1
    if _human_waits > MAX_HUMAN_WAITS_PER_RUN:
        raise _RunStopped(
            f"Pinduoduo asked for login/verification more than {MAX_HUMAN_WAITS_PER_RUN} times "
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
        if await _page_state(page) == "ok":
            print(f"▶️  Cleared. Resuming after {HUMAN_WAIT_COOLDOWN}s…")
            await asyncio.sleep(HUMAN_WAIT_COOLDOWN)
            return
        if loop.time() >= next_reminder:
            print(f"   ⏳ Still waiting for you to {what} ({(loop.time() - started) / 60:.0f} min so far)…")
            next_reminder += HUMAN_WAIT_REMINDER
    raise _RunStopped(f"Nobody cleared the {state} page in time.")


async def _goto_checked(page, url: str) -> None:
    await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
    # The redirect to login.html happens a few seconds after load.
    await asyncio.sleep(random.uniform(3.0, 4.5))
    state = await _page_state(page)
    if state != "ok":
        await _wait_for_human(page, state)
        await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
        await asyncio.sleep(random.uniform(3.0, 4.5))


async def _scroll_and_collect(
    page, script: Union[str, Callable[[], Awaitable[dict]]], id_field: str, limit: Optional[int]
) -> Tuple[List[dict], dict]:
    """Scroll to load more until `limit`, or until scrolling stops yielding new
    items. Returns (items, last raw payload)."""
    collected: Dict[str, dict] = {}
    payload: dict = {}
    stalled = 0

    for round_no in range(1, MAX_SCROLL_ROUNDS + 1):
        payload = await script() if callable(script) else await _evaluate(page, script)
        before = len(collected)
        for item in payload["items"]:
            collected.setdefault(item[id_field], item)
        if limit and len(collected) >= limit:
            break

        stalled = 0 if len(collected) > before else stalled + 1
        if stalled >= MAX_STALLED_SCROLLS:
            break

        await _evaluate(page, "() => window.scrollTo(0, document.documentElement.scrollHeight)")
        if round_no % SCROLL_REST_EVERY == 0:
            rest = random.uniform(SCROLL_REST_MIN, SCROLL_REST_MAX)
            print(f"   💤 {len(collected):,} loaded so far — resting {rest:.0f}s to keep the pace down…")
            await asyncio.sleep(rest)
        else:
            await _human_delay(SCROLL_DELAY_MIN, SCROLL_DELAY_MAX)
        state = await _page_state(page)
        if state != "ok":
            await _wait_for_human(page, state)

    items = list(collected.values())
    return (items[:limit] if limit else items), payload


# ════════════════════════════════════════════════════════════════
# Public API: Search
# ════════════════════════════════════════════════════════════════
async def run_search_keyword(
    keywords: List[str],
    max_products_per_keyword: Optional[int] = DEFAULT_MAX_PRODUCTS_PER_KEYWORD,
) -> pd.DataFrame:
    """
    Search listings for each keyword. Listing rows have price and a sales tip
    but no brand/attributes — use run_product_detail for those.

    max_products_per_keyword : None = no limit (still bounded by MAX_SCROLL_ROUNDS)
    """
    global _human_waits
    _human_waits = 0

    all_rows: List[dict] = _load_backup("temp_pinduoduo_search")
    done = {(str(r.get("keyword_searched")), str(r.get("goods_id"))) for r in all_rows}

    print(f"\n🔍 Pinduoduo — Search  ({len(keywords)} keyword(s), "
          f"max {max_products_per_keyword or 'unlimited'} per keyword)")

    try:
        context, page = await _create_browser_context()
        async with context:
            for kw in keywords:
                url = f"{BASE_URL}/search_result.html?search_key={quote(kw)}"
                print(f"\n🔑 Keyword: '{kw}'  →  {url}")
                await _goto_checked(page, url)

                items, payload = await _scroll_and_collect(
                    page, _JS_LISTING_ITEMS, "goods_id", max_products_per_keyword
                )
                if payload.get("flagged"):
                    print("   ⚠️  Pinduoduo marked this search as risk-flagged — results may be limited.")
                if not items:
                    print(f"   ⚠️  No products found for '{kw}'.")
                    await _dump_debug(page, f"empty_search_{kw}")
                    await _human_delay()
                    continue

                print(f"   ✅ {len(items)} product(s) collected (site reports: {payload.get('total')})")
                for rank, item in enumerate(items, 1):
                    if (kw, item["goods_id"]) in done:
                        continue
                    done.add((kw, item["goods_id"]))
                    all_rows.append(_listing_row(item, {"keyword_searched": kw, "search_rank": rank}))

                _save_checkpoint(all_rows, "temp_pinduoduo_search", SEARCH_DEDUPE_COLS, "auto")
                await _human_delay()
    except _RunStopped as e:
        print(f"\n🛑 Stopped: {e}")
    except Exception as e:
        print(f"\n🛑 Stopped by unexpected error — saving what was collected: {e}")

    _save_checkpoint(all_rows, "temp_pinduoduo_search", SEARCH_DEDUPE_COLS, "final")
    return _save_final(all_rows, "pinduoduo_search_list", SEARCH_DEDUPE_COLS)


# ════════════════════════════════════════════════════════════════
# Public API: Category
# ════════════════════════════════════════════════════════════════
# Display-only Thai hints for the top-level list; names missing here print in Chinese only.
_TOP_CATEGORY_TH = {
    "美妆": "ความงาม", "百货": "ของใช้ทั่วไป", "母婴": "แม่และเด็ก", "电器": "เครื่องใช้ไฟฟ้า",
    "女装": "เสื้อผ้าผู้หญิง", "食品": "อาหาร", "鞋包": "รองเท้าและกระเป๋า", "内衣": "ชุดชั้นใน",
    "医药": "ยาและเวชภัณฑ์", "手机": "มือถือ", "运动": "กีฬา", "车品": "อุปกรณ์รถยนต์",
    "水果": "ผลไม้", "饰品": "เครื่องประดับ", "家纺": "เครื่องนอนและผ้า", "男装": "เสื้อผ้าผู้ชาย",
    "家装": "ตกแต่งบ้าน", "家具": "เฟอร์นิเจอร์", "电脑": "คอมพิวเตอร์", "海淘": "สินค้านำเข้า",
}


def _catgoods_url(opt_id: str, opt_name: str) -> str:
    return f"{BASE_URL}/catgoods.html?opt_id={opt_id}&opt_type=2&opt_name={quote(opt_name)}"


def _parse_picks(text: str, count: int, empty_means_all: bool = False) -> List[int]:
    """'1,3-5' → [0, 2, 3, 4] (0-based, in the order typed). 'all' picks everything."""
    text = (text or "").strip().lower()
    if not text:
        return list(range(count)) if empty_means_all else []
    if text in ("all", "a", "*"):
        return list(range(count))
    picks: List[int] = []
    for part in re.split(r"[,\s]+", text):
        if not part:
            continue
        match = re.fullmatch(r"(\d+)(?:-(\d+))?", part)
        if not match:
            print(f"   ⚠️  Ignoring '{part}' — not a number or range.")
            continue
        lo, hi = sorted((int(match.group(1)), int(match.group(2) or match.group(1))))
        for n in range(lo, hi + 1):
            if not 1 <= n <= count:
                print(f"   ⚠️  Ignoring {n} — choose 1 to {count}.")
            elif n - 1 not in picks:
                picks.append(n - 1)
    return picks


async def _choose_categories(page, ask: Callable[[str], Awaitable[str]]) -> List[dict]:
    """Show the site's current categories as numbered lists and let a person pick."""
    # 推荐 (opt_id 0) is the recommendation feed and has no sub-categories.
    tabs = [t for t in (await _load_category_tree(page)).get("tabs", []) if t["opt_id"]]
    if not tabs:
        print("   ⚠️  Could not read the category list from the site.")
        return []

    print("\n📂 Top-level categories:")
    for n, tab in enumerate(tabs, 1):
        hint = _TOP_CATEGORY_TH.get(tab["name"])
        print(f"   {n:>2}. {tab['name']}" + (f"  ({hint})" if hint else ""))
    picks = _parse_picks(await ask("\nChoose number(s), e.g. 1 or 1,4 or 2-5: "), len(tabs))
    if not picks:
        print("   ⚠️  No category chosen.")
        return []

    targets: List[dict] = []
    for top in (tabs[i] for i in picks):
        subs = (await _load_category_tree(page, top["opt_id"])).get("subs", [])
        if not subs:
            print(f"   ⚠️  No sub-categories found under {top['name']} — skipping.")
            continue
        print(f"\n📁 {top['name']} — sub-categories:")
        for n, sub in enumerate(subs, 1):
            print(f"   {n:>2}. {sub['name']}")
        chosen = _parse_picks(
            await ask(f"\nChoose number(s) in {top['name']} [Enter = all {len(subs)}]: "),
            len(subs), empty_means_all=True,
        )
        for sub in (subs[i] for i in chosen):
            opt_id = str(sub["opt_id"])
            targets.append({
                "category_l1": top["name"], "category_l2": sub["name"],
                "opt_id": opt_id, "url": _catgoods_url(opt_id, sub["name"]),
            })
    return targets


async def _load_category_tree(page, top_opt_id: Optional[int] = None) -> dict:
    """Top-level tabs, plus the sub-categories of `top_opt_id` when given.
    Sub-categories render a moment after the page loads, hence the polling."""
    url = BASE_URL if top_opt_id is None else f"{BASE_URL}/?lastTabItemID={top_opt_id}"
    await _goto_checked(page, url)
    tree: dict = {}
    for _ in range(DETAIL_POLL_ROUNDS):
        tree = await _evaluate(page, _JS_CATEGORY_TREE) or {}
        if tree.get("tabs") and (top_opt_id is None or (tree.get("current") == top_opt_id and tree.get("subs"))):
            break
        await asyncio.sleep(2)
    return tree


async def _resolve_categories(page, specs: List[str]) -> List[dict]:
    """Turn user input into category pages: [{category_l1, category_l2, opt_id, url}]."""
    targets: Dict[str, dict] = {}
    top_ids: Optional[Dict[str, int]] = None

    for spec in (str(s).strip() for s in specs):
        if not spec:
            continue
        query = parse_qs(urlparse(spec).query)

        if "catgoods" in spec and query.get("opt_id"):
            opt_id = query["opt_id"][0]
            targets.setdefault(opt_id, {
                "category_l1": "", "category_l2": query.get("opt_name", [""])[0],
                "opt_id": opt_id, "url": spec,
            })
            continue

        if query.get("lastTabItemID"):
            top_id, top_name, sub_name = int(query["lastTabItemID"][0]), "", ""
        else:
            top_name, _, sub_name = (part.strip() for part in spec.partition("/"))
            if top_ids is None:
                tree = await _load_category_tree(page)
                top_ids = {t["name"]: t["opt_id"] for t in tree.get("tabs", [])}
            if top_name not in top_ids:
                print(f"   ⚠️  Unknown category '{top_name}'. "
                      f"Top-level categories: {', '.join(top_ids) or '(none found)'}")
                continue
            top_id = top_ids[top_name]

        tree = await _load_category_tree(page, top_id)
        top_name = top_name or next(
            (t["name"] for t in tree.get("tabs", []) if t["opt_id"] == top_id), str(top_id)
        )
        subs = tree.get("subs", [])
        if sub_name:
            matched = [s for s in subs if s["name"] == sub_name]
            if not matched:
                print(f"   ⚠️  '{sub_name}' not found under '{top_name}'. "
                      f"Sub-categories: {', '.join(s['name'] for s in subs) or '(none found)'}")
                continue
            subs = matched
        if not subs:
            print(f"   ⚠️  No sub-categories found under '{top_name}'.")
            continue
        for s in subs:
            opt_id = str(s["opt_id"])
            targets.setdefault(opt_id, {
                "category_l1": top_name, "category_l2": s["name"],
                "opt_id": opt_id, "url": _catgoods_url(opt_id, s["name"]),
            })

    return list(targets.values())


async def _pause_between_categories(done: int, total: int, what: str = "category") -> None:
    """Long pause before the next category or shop page; none after the last one."""
    if done >= total:
        return
    pause = random.uniform(CATEGORY_PAUSE_MIN, CATEGORY_PAUSE_MAX)
    print(f"   ⏳ Waiting {pause:.0f}s before the next {what}…")
    await asyncio.sleep(pause)


def category_checkpoint_summary() -> Optional[dict]:
    """What the category checkpoint holds, so a caller can offer to continue from it."""
    path = Path(TEMP_DIR) / "temp_pinduoduo_category.csv"
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, dtype=str, usecols=["category_l1", "category_l2", "collected_at"])
    except Exception:
        return None
    if df.empty:
        return None
    labels = (df["category_l1"].fillna("") + "/" + df["category_l2"].fillna("")).str.strip("/")
    return {
        "rows": len(df),
        "categories": list(dict.fromkeys(labels)),
        "last_collected": df["collected_at"].max(),
    }


async def run_category_products(
    categories: Optional[List[str]] = None,
    max_products_per_category: Optional[int] = DEFAULT_MAX_PRODUCTS_PER_CATEGORY,
    ask: Optional[Callable[[str], Awaitable[str]]] = None,
    resume: Optional[bool] = None,
) -> pd.DataFrame:
    """
    Collect listings for whole categories by browsing the site's own category
    pages instead of searching.

    With no `categories` and an `ask` function (async, prompt → answer), the
    site's current categories are shown as numbered lists to pick from.
    Otherwise each entry of `categories` is one of:
      "美妆"         every sub-category of a top-level category (~15 pages)
      "美妆/面膜"    one sub-category
      https://mobile.pinduoduo.com/?lastTabItemID=16            same as "美妆"
      https://mobile.pinduoduo.com/catgoods.html?opt_id=314&…   that one page
    Names are the Chinese ones shown on the site. Rows have the search columns
    plus category_l1 / category_l2 / category_opt_id / category_rank.

    max_products_per_category : per sub-category; None = no limit
                                (still bounded by MAX_SCROLL_ROUNDS)
    resume : True = start from the checkpoint and skip categories already in it
             (see category_checkpoint_summary); None = RESUME_FROM_BACKUP

    Stops by itself after MAX_EMPTY_CATEGORIES_IN_A_ROW empty category pages,
    which has meant the site was rate-limiting. Rows so far are saved, and a
    resumed run picks up from the first empty category.
    """
    global _human_waits
    _human_waits = 0

    all_rows: List[dict] = _load_backup("temp_pinduoduo_category", enabled=resume)
    done_categories = {str(r.get("category_opt_id")) for r in all_rows}

    source = f"{len(categories)} input(s)" if categories else "pick from list"
    print(f"\n📂 Pinduoduo — Category  ({source}, "
          f"max {max_products_per_category or 'unlimited'} per sub-category)")

    try:
        context, page = await _create_browser_context()
        async with context:
            if categories:
                targets = await _resolve_categories(page, categories)
            elif ask:
                targets = await _choose_categories(page, ask)
            else:
                targets = []
            labels = [f"{t['category_l1']}/{t['category_l2']}".strip("/") for t in targets]
            print(f"   📋 {len(targets)} category page(s): {', '.join(labels)}")

            empty_in_a_row = 0
            for i, (target, label) in enumerate(zip(targets, labels), 1):
                if target["opt_id"] in done_categories:
                    print(f"\n⏭️  [{i}/{len(targets)}] {label} — already collected, skip.")
                    continue

                print(f"\n🗂️  [{i}/{len(targets)}] {label}  →  {target['url']}")
                await _goto_checked(page, target["url"])
                items, _ = await _scroll_and_collect(
                    page, _JS_LISTING_ITEMS, "goods_id", max_products_per_category
                )
                if not items:
                    empty_in_a_row += 1
                    print(f"   ⚠️  No products found in {label}.")
                    await _dump_debug(page, f"empty_category_{target['opt_id']}")
                    if empty_in_a_row >= MAX_EMPTY_CATEGORIES_IN_A_ROW:
                        raise _RunStopped(
                            f"{empty_in_a_row} categories in a row came back empty — Pinduoduo is "
                            "probably limiting requests. Wait 30–60 minutes, then run again and "
                            "continue from the checkpoint to skip categories already collected."
                        )
                    await _pause_between_categories(i, len(targets))
                    continue
                empty_in_a_row = 0

                print(f"   ✅ {len(items)} product(s) collected")
                lead_cols = {
                    "category_l1": target["category_l1"],
                    "category_l2": target["category_l2"],
                    "category_opt_id": target["opt_id"],
                }
                for rank, item in enumerate(items, 1):
                    all_rows.append(_listing_row(item, {**lead_cols, "category_rank": rank}))
                done_categories.add(target["opt_id"])

                _save_checkpoint(all_rows, "temp_pinduoduo_category", CATEGORY_DEDUPE_COLS, "auto")
                await _pause_between_categories(i, len(targets))
    except _RunStopped as e:
        print(f"\n🛑 Stopped: {e}")
    except Exception as e:
        print(f"\n🛑 Stopped by unexpected error — saving what was collected: {e}")

    _save_checkpoint(all_rows, "temp_pinduoduo_category", CATEGORY_DEDUPE_COLS, "final")
    return _save_final(all_rows, "pinduoduo_category_list", CATEGORY_DEDUPE_COLS)


# ════════════════════════════════════════════════════════════════
# Public API: Shop
# ════════════════════════════════════════════════════════════════
def _shop_target(raw: str) -> Optional[dict]:
    """A mall_id or a Pinduoduo shop link; None if neither. Links are opened exactly
    as given: msn without the signed mall_sn shows "No corresponding store found",
    and a ps= share link moved to the other host lands on the home page."""
    text = str(raw).strip()
    if text.isdigit():
        return {"key": text, "mall_id": text, "url": f"{BASE_URL}/mall_page.html?mall_id={text}"}
    try:
        parsed = urlparse(text if "://" in text else f"https://{text}")
    except ValueError:
        return None
    if not (parsed.hostname or "").endswith(("pinduoduo.com", "yangkeduo.com")):
        return None
    query = parse_qs(parsed.query)
    mall_id = (query.get("mall_id") or [""])[0]
    mall_id = mall_id if mall_id.isdigit() else None
    key = mall_id or (query.get("msn") or query.get("mall_sn") or query.get("ps") or [""])[0]
    if not key and parsed.path.endswith("mall_page.html"):
        key = parsed.query  # another way of naming the shop: the site can still resolve it
    if not key:
        return None
    return {"key": key, "mall_id": mall_id, "url": parsed.geturl()}


def _shop_info(malls: List[dict], mall_id: Optional[str]) -> dict:
    """The page's own shop among the shops it mentions."""
    for mall in malls:
        if mall["id"] == mall_id:
            return mall
    return malls[0] if malls and not mall_id else {}


async def _ensure_logged_in(page, targets: List[dict]) -> None:
    """Open the login page before any shop. A signed-out Playwright window was shown
    "store not found" on shop pages with no redirect to login, so the usual pause
    never happened. A signed-in session leaves the login page by itself."""
    for origin in dict.fromkeys(f"{urlparse(t['url']).scheme}://{urlparse(t['url']).netloc}" for t in targets):
        await _goto_checked(page, f"{origin}/login.html")


# A shop page loads its products 20 at a time from this endpoint while it is
# scrolled; later pages are not in the page's data, only in these responses.
_SHOP_LIST_PATH = "/query_cat_goods"


def _goods_from_list_response(payload: dict) -> List[dict]:
    """Listing rows from one product-list response. Prices are in fen (cent)."""
    rows = []
    for g in (payload.get("goods_list") or []) if isinstance(payload, dict) else []:
        if not isinstance(g, dict) or not g.get("goods_id"):
            continue
        name = g.get("goods_name") or g.get("short_name")
        if not name:
            continue
        price = g.get("price")
        special = g.get("special_text") if isinstance(g.get("special_text"), dict) else {}
        rows.append({
            "goods_id": str(g["goods_id"]),
            "product_name": name,
            "price_cent": price if isinstance(price, (int, float)) and not isinstance(price, bool) else None,
            "display_price": g.get("price_info"),
            "price_label": g.get("price_prefix_info"),
            "sales_tip": g.get("sales_tip") or "",
            "sold_cnt_raw": None,
            "item_mall_id": None,
            "image_url": g.get("hd_thumb_url") or g.get("thumb_url") or "",
            "delivery_note": special.get("img_text"),
            "tags": " | ".join(t["text"] for t in (g.get("tag_list") or []) if isinstance(t, dict) and t.get("text")),
        })
    return rows


class _ShopListCapture:
    """Collects the product pages the shop page itself loads while it is scrolled.
    Passive: it reads responses the browser already received and sends no requests."""

    def __init__(self):
        self.items: Dict[str, dict] = {}
        self.pages: List[object] = []
        self.errors = 0
        self._tasks: set = set()

    def attach(self, page) -> None:
        page.on("response", lambda response: self._spawn(self._read(response)))

    def reset(self) -> None:
        self.items.clear()
        self.pages.clear()

    def _spawn(self, coroutine) -> None:
        task = asyncio.ensure_future(coroutine)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def drain(self) -> None:
        if self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)

    async def _read(self, response) -> None:
        if _SHOP_LIST_PATH not in urlparse(response.url).path or response.status != 200:
            return
        try:
            payload = await response.json()
        except Exception:  # body gone after a navigation
            self.errors += 1
            return
        self.pages.append(payload.get("page_no") if isinstance(payload, dict) else None)
        for row in _goods_from_list_response(payload):
            self.items.setdefault(row["goods_id"], row)


async def _dump_shop_shape(page, shop_key: str) -> None:
    safe = re.sub(r"[^\w-]+", "_", shop_key, flags=re.UNICODE)[:40]
    path = Path(LOG_DIR) / f"{datetime.now(TZ_THAI).strftime('%Y%m%d_%H%M%S')}_shop_shape_{safe}.json"
    try:
        shape = await _evaluate(page, _JS_SHOP_SHAPE)
        path.write_text(json.dumps(shape, ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"   📐 Field names on this page saved: {path}")
    except Exception as e:
        print(f"   ⚠️  Could not save field names: {e}")


def shop_checkpoint_summary() -> Optional[dict]:
    """What the shop checkpoint holds, so a caller can offer to continue from it."""
    path = Path(TEMP_DIR) / "temp_pinduoduo_shop.csv"
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, dtype=str, usecols=["shop_key", "collected_at"])
    except Exception:
        return None
    if df.empty:
        return None
    return {
        "rows": len(df),
        "shops": list(dict.fromkeys(df["shop_key"].fillna(""))),
        "last_collected": df["collected_at"].max(),
    }


async def run_shop_products(
    shops: List[str],
    max_products_per_shop: Optional[int] = DEFAULT_MAX_PRODUCTS_PER_SHOP,
    resume: Optional[bool] = None,
) -> pd.DataFrame:
    """
    Collect the listing of each shop, from a mall_id or a shop link.

    Uses the same browser and saved login as the other actions (USER_DATA_DIR),
    so run action 0 first if the session has expired. It opens the login page
    first and pauses there until a person has logged in.
    Pace, caps and the pause at login/verification are the same as for categories.

    Products come from the product-list responses the shop page loads itself while
    it is scrolled (20 per page); the page's own data fills in what they lack. Nothing
    is requested on top of what the page does.

    Rows have the listing columns plus shop_key / mall_id / mall_name /
    shop_goods_num / shop_sales / shop_rank, display_price / price_label (the price
    the page shows, e.g. after coupon), delivery_note (e.g. 泰国不可配送 = not
    deliverable to Thailand). price_cny is the price before coupon.
    Check sold_scope before using sold_count: it may not be a per-product figure.

    max_products_per_shop : None = no limit (still bounded by MAX_SCROLL_ROUNDS)
    resume : True = start from the checkpoint and skip shops already in it
             (see shop_checkpoint_summary); None = RESUME_FROM_BACKUP
    """
    global _human_waits
    _human_waits = 0

    targets: Dict[str, dict] = {}
    for raw in shops:
        target = _shop_target(raw)
        if target:
            targets.setdefault(target["key"], target)
        else:
            print(f"   ⚠️  Skipping — not a Pinduoduo mall_id or shop link: {raw}")

    all_rows: List[dict] = _load_backup("temp_pinduoduo_shop", enabled=resume)
    done_shops = {str(r.get("shop_key")) for r in all_rows}

    print(f"\n🏬 Pinduoduo — Shop  ({len(targets)} shop(s), "
          f"max {max_products_per_shop or 'unlimited'} per shop)")

    capture = _ShopListCapture()

    async def read_shop_page() -> dict:
        """Product-list responses first (complete pages, in page order); the page's own
        data fills in anything they did not carry."""
        await capture.drain()
        scan = await _evaluate(page, _JS_SHOP_ITEMS)
        merged = dict(capture.items)
        for item in scan["items"]:
            merged.setdefault(item["goods_id"], item)
        return {**scan, "items": list(merged.values())}

    try:
        context, page = await _create_browser_context()
        capture.attach(page)
        async with context:
            await _ensure_logged_in(page, [t for t in targets.values() if t["key"] not in done_shops])
            empty_in_a_row = 0
            for i, target in enumerate(targets.values(), 1):
                key = target["key"]
                if key in done_shops:
                    print(f"\n⏭️  [{i}/{len(targets)}] {key} — already collected, skip.")
                    continue

                print(f"\n🏪 [{i}/{len(targets)}] {key}  →  {target['url']}")
                capture.reset()
                await _goto_checked(page, target["url"])
                items, payload = await _scroll_and_collect(
                    page, read_shop_page, "goods_id", max_products_per_shop
                )
                print(f"   📦 product-list pages seen: {len(capture.pages)} "
                      f"({len(capture.items)} items from them, {len(items)} in total)")

                malls = payload.get("malls") or []
                info = _shop_info(malls, target["mall_id"])
                mall_id = target["mall_id"] or info.get("id")
                own = [it for it in items if not mall_id or it["item_mall_id"] in (None, mall_id)]
                if len(own) < len(items):
                    print(f"   ℹ️  Left out {len(items) - len(own)} item(s) belonging to other shops.")

                if not own and payload.get("shop_missing"):
                    print("   ⚠️  Pinduoduo returned no data for this shop (not found, expired link, "
                          "or refused). Check the screenshot saved below.")
                    await _dump_debug(page, f"shop_not_found_{key}")
                    await _pause_between_categories(i, len(targets), "shop")
                    continue

                if not own or not any(it["price_cent"] for it in own) or not any(
                    it["sales_tip"] or it["sold_cnt_raw"] for it in own
                ):
                    await _dump_shop_shape(page, key)

                if not own:
                    empty_in_a_row += 1
                    print(f"   ⚠️  No products found in shop {key}.")
                    await _dump_debug(page, f"empty_shop_{key}")
                    if empty_in_a_row >= MAX_EMPTY_CATEGORIES_IN_A_ROW:
                        raise _RunStopped(
                            f"{empty_in_a_row} shops in a row came back empty — the page layout may "
                            "differ from what this reader expects, or Pinduoduo is limiting requests."
                        )
                    await _pause_between_categories(i, len(targets), "shop")
                    continue
                empty_in_a_row = 0

                print(f"   ✅ {len(own)} product(s) collected (site reports: {info.get('goods_num')})")
                lead_cols = {
                    "shop_key": key,
                    "mall_id": mall_id,
                    "mall_name": info.get("name"),
                    "shop_goods_num": info.get("goods_num"),
                    "shop_sales": info.get("sales"),
                }
                new_rows = [_listing_row(item, {**lead_cols, "shop_rank": rank}) for rank, item in enumerate(own, 1)]
                print(f"   📈 sold_scope: {dict(Counter(row['sold_scope'] for row in new_rows))}")
                all_rows.extend(new_rows)
                done_shops.add(key)

                _save_checkpoint(all_rows, "temp_pinduoduo_shop", SHOP_DEDUPE_COLS, "auto")
                await _pause_between_categories(i, len(targets), "shop")
    except _RunStopped as e:
        print(f"\n🛑 Stopped: {e}")
    except Exception as e:
        print(f"\n🛑 Stopped by unexpected error — saving what was collected: {e}")

    _save_checkpoint(all_rows, "temp_pinduoduo_shop", SHOP_DEDUPE_COLS, "final")
    return _save_final(all_rows, "pinduoduo_shop_list", SHOP_DEDUPE_COLS)


# ════════════════════════════════════════════════════════════════
# Public API: Product Detail
# ════════════════════════════════════════════════════════════════
async def run_product_detail(
    product_urls: Optional[List[str]] = None,
    df_product: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Scrape brand/attributes/category for each product. Pass the search dataframe
    as df_product to carry price and sales columns across — product pages don't
    always show a complete price. Brand and ship_from stay empty for products
    whose page doesn't list them.
    """
    global _human_waits
    _human_waits = 0

    targets = _targets_from_input(product_urls, df_product)
    if not targets:
        print("❌ run_product_detail: no valid Pinduoduo product URLs provided.")
        return pd.DataFrame()

    all_rows: List[dict] = _load_backup("temp_pinduoduo_detail")
    done = {str(r.get("goods_id")) for r in all_rows}
    debug_dumped = False
    empty_in_a_row = 0

    print(f"\n📝 Pinduoduo — Product Detail  ({len(targets)} product(s))")

    try:
        context, page = await _create_browser_context()
        async with context:
            for i, (goods_id, source) in enumerate(targets.items(), 1):
                if goods_id in done:
                    print(f"   ⏭️  [{i}/{len(targets)}] Already collected — skip.")
                    continue

                url = _product_url(goods_id)
                print(f"   ⏳ {i}/{len(targets)} → {url}")
                await _goto_checked(page, url)

                detail = None
                sold_out = False
                for _ in range(DETAIL_POLL_ROUNDS):
                    detail = await _evaluate(page, _JS_PRODUCT_DETAIL)
                    if detail:
                        break
                    if await _page_says_sold_out(page):
                        sold_out = True
                        break
                    await asyncio.sleep(2)

                if sold_out:
                    # A listing can still show a product whose own page says 商品已售罄; that page
                    # carries no data, but it is an answer, not a failure of the run.
                    print("      ⚠️  Page says the product is sold out — recorded without details.")
                    row = {
                        "scraped_date": _scraped_date(),
                        "goods_id": goods_id,
                        "is_on_sale": False,
                        "page_status": "sold_out",
                        "product_url": url,
                        **{col: source.get(col) for col in _SEARCH_FIELDS_FOR_DETAIL},
                        "collected_at": _collected_at(),
                    }
                else:
                    if not detail:
                        print("      ⚠️  Product data not found on page — skipping.")
                        if not debug_dumped:
                            await _dump_debug(page, f"empty_detail_{goods_id}")
                            debug_dumped = True
                        empty_in_a_row += 1
                        if empty_in_a_row >= MAX_EMPTY_DETAILS_IN_A_ROW:
                            raise _RunStopped(
                                f"{empty_in_a_row} product pages in a row came back blank (not even a sold-out "
                                "message) — Pinduoduo may be limiting requests. Look at the screenshot in the "
                                "log folder, wait 30–60 minutes, then try a smaller file."
                            )
                        await _human_delay()
                        continue

                    attributes = detail.pop("attributes", {})
                    description = _describe(attributes, detail.pop("consumer_notice", ""))
                    brand, origin, ship_from = (attributes.pop(k, "") for k in ("品牌", "产地", "发货地"))
                    row = {
                        "scraped_date": _scraped_date(),
                        **detail,
                        "page_status": "ok",
                        "product_url": url,
                        "brand": brand,
                        "origin": origin,
                        "ship_from": ship_from,
                        "other_attributes": json.dumps(attributes, ensure_ascii=False),
                        "description": description,
                        **{col: source.get(col) for col in _SEARCH_FIELDS_FOR_DETAIL},
                        "collected_at": _collected_at(),
                    }
                empty_in_a_row = 0
                all_rows.append(row)
                done.add(goods_id)

                if len(all_rows) % CHECKPOINT_EVERY_PRODUCTS == 0:
                    _save_checkpoint(all_rows, "temp_pinduoduo_detail", DETAIL_DEDUPE_COLS, "auto")
                await _human_delay()
    except _RunStopped as e:
        print(f"\n🛑 Stopped: {e}")
    except Exception as e:
        print(f"\n🛑 Stopped by unexpected error — saving what was collected: {e}")

    _save_checkpoint(all_rows, "temp_pinduoduo_detail", DETAIL_DEDUPE_COLS, "final")
    return _save_final(all_rows, "pinduoduo_detail", DETAIL_DEDUPE_COLS)


# ════════════════════════════════════════════════════════════════
# Public API: Product Reviews
# ════════════════════════════════════════════════════════════════
async def run_product_reviews(
    product_urls: Optional[List[str]] = None,
    df_product: Optional[pd.DataFrame] = None,
    max_reviews_per_product: Optional[int] = DEFAULT_MAX_REVIEWS_PER_PRODUCT,
) -> pd.DataFrame:
    """
    Scrape reviews for each product, in the order the page serves them.
    The `option` column is the variant the reviewer bought.

    The site's total can exceed what's collectable: it counts default positive
    reviews the page never lists ("该商品还有20条默认好评").

    max_reviews_per_product : None = no limit (still bounded by MAX_SCROLL_ROUNDS)
    """
    global _human_waits
    _human_waits = 0

    targets = _targets_from_input(product_urls, df_product)
    if not targets:
        print("❌ run_product_reviews: no valid Pinduoduo product URLs provided.")
        return pd.DataFrame()

    all_rows: List[dict] = _load_backup("temp_pinduoduo_reviews")
    done = {str(r.get("goods_id")) for r in all_rows}

    print(f"\n💬 Pinduoduo — Reviews  ({len(targets)} product(s), "
          f"max {max_reviews_per_product or 'unlimited'} per product)")

    try:
        context, page = await _create_browser_context()
        async with context:
            for i, (goods_id, source) in enumerate(targets.items(), 1):
                if goods_id in done:
                    print(f"   ⏭️  [{i}/{len(targets)}] Already collected — skip.")
                    continue

                url = f"{BASE_URL}/goods_comments.html?goods_id={goods_id}"
                print(f"   ⏳ {i}/{len(targets)} → {url}")
                await _goto_checked(page, url)

                items, payload = await _scroll_and_collect(
                    page, _JS_REVIEW_ITEMS, "review_id", max_reviews_per_product
                )
                print(f"      ✅ {len(items)} review(s) collected "
                      f"(site total: {payload.get('total')}, can include unlisted default reviews)")

                for item in items:
                    specs = item.pop("specs", "")
                    all_rows.append({
                        "scraped_date": _scraped_date(),
                        "goods_id": goods_id,
                        "product_url": _product_url(goods_id),
                        "product_name": str(source.get("product_name") or ""),
                        **item,
                        "option": _parse_specs(specs),
                        "collected_at": _collected_at(),
                    })
                done.add(goods_id)

                _save_checkpoint(all_rows, "temp_pinduoduo_reviews", REVIEW_DEDUPE_COLS, "auto")
                await _human_delay()
    except _RunStopped as e:
        print(f"\n🛑 Stopped: {e}")
    except Exception as e:
        print(f"\n🛑 Stopped by unexpected error — saving what was collected: {e}")

    _save_checkpoint(all_rows, "temp_pinduoduo_reviews", REVIEW_DEDUPE_COLS, "final")
    return _save_final(all_rows, "pinduoduo_reviews", REVIEW_DEDUPE_COLS)
