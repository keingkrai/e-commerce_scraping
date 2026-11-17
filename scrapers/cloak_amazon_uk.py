"""
cloak_amazon_uk.py
────────────────────────────────────────────────────────────────
Amazon UK (www.amazon.co.uk) scraper — search listings, product details and
the reviews shown on a product page.

⚠️  Read before running
  • Amazon's Conditions of Use do not allow automated data collection, and
    the site enforces it (CAPTCHAs, blocks). Run small, slow jobs. For
    regular large-scale data, Amazon's own APIs or a licensed data provider
    are the lower-risk route.
  • The page selectors below come from Amazon's known desktop layout and were
    NOT checked against the live site while this module was written (Claude
    may not fetch amazon.co.uk). The first runs save a sample search page and
    a sample product page to ./amazon_uk/log/ (SAVE_SAMPLE_PAGES) and print
    how many rows got each field, so selectors can be fixed from real pages.

How it works
  • Search : /s?k=<keyword>&page=N (or a pasted search URL with filters).
             Each result card is read from the DOM: ASIN, title, brand (when
             shown), price, list price, rating, rating count, "X bought in
             past month", Sponsored / Prime / badge.
  • Detail : /dp/<ASIN> — title, brand, price, RRP, availability, rating,
             rating histogram, Best Sellers Rank, "About this item" bullets,
             description, ingredients / important information, product
             details tables, images.
  • Category: run_category_crawl(start) follows a category's "Department"
             list down CATEGORY_MAX_DEPTH levels. For every sub-category it
             reads search page 1 (sorted by Best Sellers), more pages for the
             lowest ones, and the Best Sellers top 100 (/gp/bestsellers/…).
             This is the TOP of each sub-category — Amazon stops paging one
             search after ~7–20 pages, so "every product" is not reachable.
             Page loads per run are capped (CATEGORY_PAGES_PER_RUN); resume
             carries on. Output sheets: products / appearances / categories.
  • Reviews: only the reviews printed on the product page (usually ~8 from
             the UK plus some "from other countries"). The full review pages
             require signing in, and this module never signs in.
  • Delivery location: from outside the UK, Amazon shows prices and stock
             for exporting (often without UK VAT). create_amazon_session()
             sets a UK delivery postcode in the saved browser session once;
             every run checks it and warns if it is not set.
  • A CAPTCHA / "continue shopping" check pauses the run until a person
    deals with it in the browser window; nothing tries to solve it. Repeated
    checks or "Sorry, something went wrong" pages stop the run (rows so far
    are saved).

Public entry-points:
  create_amazon_session(postcode)      set a UK delivery postcode in the saved session
  run_search_keyword(keywords)      →  pd.DataFrame  listing rows
  run_category_crawl(start)         →  pd.DataFrame  one row per ASIN (best rank first)
  category_checkpoint_summary()     →  what the category checkpoint holds
  run_product_detail(product_urls)  →  pd.DataFrame  detail rows
  run_product_reviews(product_urls) →  pd.DataFrame  review rows (product-page reviews)
  checkpoint_summary(kind)          →  what a checkpoint holds ("listing" / "detail" / "review")
"""

import asyncio
import json
import random
import re
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, quote_plus, unquote, urlencode, urlparse, urlunparse

import pandas as pd
from cloakbrowser import launch_persistent_context_async

# ════════════════════════════════════════════════════════════════
# Directories & Config
# ════════════════════════════════════════════════════════════════

USER_DATA_DIR = "./data/amazon_uk/browser_session"
OUTPUT_DIR    = "./data/amazon_uk/collected_data"
TEMP_DIR      = "./data/amazon_uk/collected_data/temp"
LOG_DIR       = "./data/amazon_uk/log"

SITE        = "https://www.amazon.co.uk"
SEARCH_URL  = f"{SITE}/s"
PRODUCT_URL = f"{SITE}/dp/{{}}"

UK_POSTCODE = "EC1A 1BB"                 # any UK postcode works; only sets the delivery area

DEFAULT_MAX_PAGES_PER_KEYWORD = 3        # Amazon search pages hold ~16–60 results
MAX_PAGES_HARD = 20                      # Amazon rarely serves more than this per search

PAGE_DELAY_MIN,    PAGE_DELAY_MAX    = 8, 15     # seconds between search pages
KEYWORD_PAUSE_MIN, KEYWORD_PAUSE_MAX = 30, 60    # seconds between keywords
DETAIL_DELAY_MIN,  DETAIL_DELAY_MAX  = 8, 15     # seconds between product pages

# Category crawl (run_category_crawl / menu action 6). A whole category is far
# bigger than Amazon will page through (one search stops at ~7–20 pages), so
# this collects the TOP of every sub-category, not every product.
CATEGORY_MAX_DEPTH        = 3        # sub-category levels to follow below the start category
CATEGORY_MAX_NODES        = 400      # stop adding sub-categories after this many
CATEGORY_PAGES_PER_NODE   = 3        # search pages per lowest sub-category (~48 items a page)
CATEGORY_SORT             = "exact-aware-popularity-rank"   # the "Best Sellers" sort; "" = Featured
CATEGORY_BESTSELLER_PAGES = 2        # Best Sellers pages per category (50 each → top 100); 0 = skip
BESTSELLER_STORE          = "beauty" # /gp/bestsellers/<store>/<node> — the store the category is in
CATEGORY_PAGES_PER_RUN    = 300      # page loads per run; run again (resume) to continue
CATEGORY_EMPTY_IN_A_ROW   = 3        # categories in a row with no items and no sub-categories → stop

# "Every product" mode (run_category_crawl(exhaustive=True)): a lowest category
# with more results than Amazon will page through is split into price bands,
# and a band that is still too big is halved, until each band fits. Products
# without a price (unavailable) never match a price filter, so they are missed.
CATEGORY_SLICE_TARGET = 300          # results one search may have before it is split (~7 pages of 48)
PRICE_FILTER          = "p_36"       # Amazon's price refinement: rh=…,p_36:<min>-<max>, in pence
PRICE_BANDS_PENCE     = [0, 300, 500, 700, 1000, 1500, 2000, 3000, 5000, 10000]   # first split; last = £100+
MIN_SLICE_PENCE       = 50           # never split a band narrower than 50p — take what Amazon serves

PAGE_TIMEOUT_MS = 60000

HUMAN_WAIT_TIMEOUT      = 900   # seconds to wait for a person at a CAPTCHA; None = no limit
HUMAN_WAIT_REMINDER     = 120
HUMAN_WAIT_COOLDOWN     = 30
MAX_HUMAN_WAITS_PER_RUN = 3
BLOCK_BACKOFF_MIN, BLOCK_BACKOFF_MAX = 120, 240
MAX_BLOCKS_PER_RUN      = 2
MAX_EMPTY_KEYWORDS_IN_A_ROW = 2

SAVE_SAMPLE_PAGES = True   # save the first search page + first product page of each run to LOG_DIR
                           # (to check selectors) — turn off once the columns come out filled

DESCRIPTION_MAX_CHARS = 5000
CHECKPOINT_EVERY = 10
RESUME_FROM_BACKUP = False

LISTING_DEDUPE_COLS = ["keyword_searched", "asin"]
DETAIL_DEDUPE_COLS  = ["product_url"]
REVIEW_DEDUPE_COLS  = ["product_url", "review_id"]

_TEXT_COLS = {c: str for c in ("asin", "review_id")}

_ASIN_RE   = re.compile(r"/(?:dp|gp/product|gp/aw/d|product-reviews)/([A-Z0-9]{10})(?:[/?#]|$)")
_PRICE_RE  = re.compile(r"£\s?([\d,]+(?:\.\d{1,2})?)")
_RATING_RE = re.compile(r"([\d.,]+)\s*out of\s*5")
_COUNT_RE  = re.compile(r"([\d.,]+)\s*([KkMm])?")
_BOUGHT_RE = re.compile(r"([\d.,]+)\s*([KkMm])?\+?\s*bought in past month", re.I)
_BSR_RE    = re.compile(r"#?([\d,]+)\s+in\s+(.+?)(?=\s+#?[\d,]+\s+in\s+|$)")
_REVIEWED_RE = re.compile(r"Reviewed in (?:the )?(.+?) on (\d{1,2} \w+ \d{4})")

_CAPTCHA_PHRASES = ("type the characters you see", "enter the characters you see",
                    "click the button below to continue shopping")
_BLOCK_PHRASES = ("sorry! something went wrong", "sorry, something went wrong",
                  "to discuss automated access to amazon data")

for _folder in [OUTPUT_DIR, TEMP_DIR, LOG_DIR]:
    Path(_folder).mkdir(parents=True, exist_ok=True)

TZ_THAI = timezone(timedelta(hours=7))

_blocks = 0
_human_waits = 0
_samples_saved: set = set()
_location_checked = False
_dp_cache: Dict[str, dict] = {}   # asin → raw product-page data, so action 4 reuses action 3's page loads

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
  return {
    url: location.href,
    title: document.title || '',
    text,
    captchaForm: !!document.querySelector('form[action*="validateCaptcha"], input#captchacharacters'),
    signin: /\/ap\/signin/.test(location.pathname),
  };
}"""

_JS_LOCATION = r"""() => {
  const el = document.querySelector('#glow-ingress-line2') || document.querySelector('#nav-global-location-slot');
  return el ? el.textContent.replace(/[​-‏]/g, '').replace(/\s+/g, ' ').trim() : '';
}"""

_JS_SEARCH = r"""() => {
  const txt = (el) => el ? el.textContent.replace(/\s+/g, ' ').trim() : '';
  const items = [];
  for (const card of document.querySelectorAll('div[data-component-type="s-search-result"][data-asin]')) {
    const asin = card.getAttribute('data-asin');
    if (!asin) continue;
    const h2s = [...card.querySelectorAll('h2')];
    const titleH2 = h2s.find(h => h.closest('a') || h.querySelector('a')) || h2s[h2s.length - 1];
    const brandH2 = h2s.find(h => h !== titleH2 && !h.closest('a') && !h.querySelector('a'));
    let title = titleH2 ? (txt(titleH2) || titleH2.getAttribute('aria-label') || '') : '';
    title = title.replace(/^Sponsored Ad\s*[-–]\s*/i, '');
    const cardText = card.innerText || '';
    const ratingAlt = card.querySelector('i[class*="a-icon-star"] .a-icon-alt, span.a-icon-alt');
    const ratingAria = card.querySelector('[aria-label*="out of 5 stars"]');
    // "12,345 ratings" — never the "4.6 out of 5 stars, rating details" label next to it
    const countAria = [...card.querySelectorAll('[aria-label]')].map(e => (e.getAttribute('aria-label') || '').trim())
      .find(l => /^[\d.,]+\s*[KkMm]?\s+(global\s+)?ratings?$/.test(l)) || '';
    const countText = [...card.querySelectorAll('a[href*="customerReviews"] span, span.s-underline-text, '
      + '[data-cy="reviews-block"] a span')].map(txt).find(t => /^\(?[\d.,]+\s*[KkMm]?\)?$/.test(t)) || '';
    const bought = (cardText.match(/[\d.,]+[KkMm]?\+?\s+bought in past month/) || [''])[0];
    const sponsored = !!card.querySelector('.puis-sponsored-label-text, .s-sponsored-label-text, .puis-label-popover-default')
      || /^\s*Sponsored\b/m.test(cardText) || card.classList.contains('AdHolder');
    const img = card.querySelector('img.s-image');
    items.push({
      asin, title,
      brand: brandH2 ? txt(brandH2) : '',
      price: txt(card.querySelector('.a-price:not(.a-text-price) .a-offscreen')),
      list_price: txt(card.querySelector('.a-price.a-text-price .a-offscreen')),
      rating: txt(ratingAlt) || (ratingAria ? ratingAria.getAttribute('aria-label') : ''),
      rating_count: countAria || countText,
      bought,
      sponsored,
      prime: !!card.querySelector('i.a-icon-prime, [aria-label="Amazon Prime"]'),
      badge: txt(card.querySelector('.a-badge-text')),
      coupon: txt(card.querySelector('.s-coupon-unclipped, [data-component-type="s-coupon-component"]')),
      image: img ? (img.getAttribute('src') || '') : '',
    });
  }
  const next = document.querySelector('a.s-pagination-next');
  const disabled = !!document.querySelector('.s-pagination-next.s-pagination-disabled');
  return { items, hasNext: !!next && !disabled };
}"""

_JS_DETAIL = r"""() => {
  const txt = (el) => el ? el.textContent.replace(/[​-‏]/g, '').replace(/\s+/g, ' ').trim() : '';
  const q = (s) => document.querySelector(s);
  const first = (sels) => { for (const s of sels) { const t = txt(q(s)); if (t) return t; } return ''; };
  const clean = (k) => k.replace(/[:‎‏]/g, '').trim();
  const without = (el, sel) => { const c = el.cloneNode(true); c.querySelectorAll(sel).forEach(n => n.remove()); return txt(c); };

  const details = {};
  document.querySelectorAll('#productDetails_techSpec_section_1 tr, #productDetails_techSpec_section_2 tr, '
      + '#productDetails_detailBullets_sections1 tr, #prodDetails table tr').forEach(tr => {
    const k = txt(tr.querySelector('th')), v = txt(tr.querySelector('td'));
    if (k && v) details[clean(k)] = v;
  });
  document.querySelectorAll('#detailBullets_feature_div li span.a-list-item, '
      + '#detailBulletsWrapper_feature_div li span.a-list-item').forEach(li => {
    const b = li.querySelector('span.a-text-bold');
    if (!b) return;
    const k = clean(txt(b)), v = without(li, 'span.a-text-bold');
    if (k && v) details[k] = v;
  });

  const overview = {};
  document.querySelectorAll('#productOverview_feature_div tr, #poExpander tr').forEach(tr => {
    const td = tr.querySelectorAll('td');
    if (td.length >= 2 && txt(td[0])) overview[clean(txt(td[0]))] = txt(td[1]);
  });
  document.querySelectorAll('.product-facts-detail').forEach(row => {
    const l = row.querySelector('.a-col-left'), r = row.querySelector('.a-col-right');
    if (l && r && txt(l)) overview[clean(txt(l))] = txt(r);
  });

  let bsr = details['Best Sellers Rank'] || '';
  if (!bsr) {
    const el = [...document.querySelectorAll('#detailBulletsWrapper_feature_div li, #SalesRank, #prodDetails tr')]
      .find(e => /Best Sellers Rank/.test(e.textContent));
    if (el) bsr = txt(el).replace(/^.*?Best Sellers Rank:?/, '').trim();
  }

  const important = {};
  document.querySelectorAll('#important-information .content, #important-information .a-section.content').forEach(sec => {
    const h = txt(sec.querySelector('h4, h3, .a-text-bold'));
    const body = without(sec, 'h4, h3, .a-text-bold');
    if (h && body) important[clean(h)] = body;
  });

  const histogram = {};
  document.querySelectorAll('#histogramTable [aria-label], #cm_cr_dp_d_rating_histogram [aria-label]').forEach(el => {
    const m = (el.getAttribute('aria-label') || '').match(/(\d+)\s*percent of reviews have (\d) star/i);
    if (m) histogram[m[2]] = parseInt(m[1], 10);
  });
  if (!Object.keys(histogram).length) {
    document.querySelectorAll('#histogramTable li, #histogramTable tr, #histogramTable [aria-label]').forEach(row => {
      const s = txt(row) + ' ' + (row.getAttribute('aria-label') || '');
      const m = s.match(/(\d)\s*stars?\s*(?:represents?\s*)?(\d+)\s*%/i);
      if (m) histogram[m[1]] = parseInt(m[2], 10);
    });
  }

  const img = q('#landingImage, #imgBlkFront');
  let images = [];
  if (img) {
    if (img.getAttribute('data-old-hires')) images.push(img.getAttribute('data-old-hires'));
    try { images.push(...Object.keys(JSON.parse(img.getAttribute('data-a-dynamic-image') || '{}'))); } catch (e) {}
    if (!images.length && img.getAttribute('src')) images.push(img.getAttribute('src'));
  }

  const reviews = [...document.querySelectorAll('[data-hook="review"]')].map(r => {
    const titleEl = r.querySelector('[data-hook="review-title"]');
    const titleSpans = titleEl ? [...titleEl.querySelectorAll('span')].filter(s => !s.classList.contains('a-icon-alt')
      && !s.closest('i') && txt(s)) : [];
    return {
      id: r.id || '',
      name: txt(r.querySelector('.a-profile-name')),
      rating: txt(r.querySelector('[data-hook="review-star-rating"] .a-icon-alt, '
        + '[data-hook="cmps-review-star-rating"] .a-icon-alt, i[class*="a-icon-star"] .a-icon-alt')),
      title: titleSpans.length ? txt(titleSpans[titleSpans.length - 1]) : txt(titleEl),
      date: txt(r.querySelector('[data-hook="review-date"]')),
      body: txt(r.querySelector('[data-hook="review-body"]')),
      verified: !!r.querySelector('[data-hook="avp-badge"], [data-hook="avp-badge-linkless"]'),
      helpful: txt(r.querySelector('[data-hook="helpful-vote-statement"]')),
      variant: txt(r.querySelector('[data-hook="format-strip"], [data-hook="format-strip-linkless"]')),
    };
  });

  return {
    url: location.href,
    title: txt(q('#productTitle')),
    byline: txt(q('#bylineInfo')),
    price: first(['#corePrice_feature_div .a-price .a-offscreen',
                  '#corePriceDisplay_desktop_feature_div .priceToPay .a-offscreen',
                  '#corePriceDisplay_desktop_feature_div .a-price .a-offscreen',
                  '#apex_desktop .a-price .a-offscreen', '#price_inside_buybox',
                  '#priceblock_ourprice', '#priceblock_dealprice', '#sns-base-price']),
    list_price: first(['#corePriceDisplay_desktop_feature_div .basisPrice .a-offscreen',
                       '#corePriceDisplay_desktop_feature_div .a-text-price .a-offscreen',
                       '.basisPrice .a-offscreen']),
    savings: first(['#corePriceDisplay_desktop_feature_div .savingsPercentage', '.savingsPercentage']),
    unit_price: first(['#corePriceDisplay_desktop_feature_div .pricePerUnit', '.pricePerUnit']),
    availability: first(['#availability span', '#availability']),
    rating: q('#acrPopover') ? (q('#acrPopover').getAttribute('title') || txt(q('#acrPopover .a-icon-alt'))) : '',
    rating_count: txt(q('#acrCustomerReviewText')),
    bought: first(['#social-proofing-faceout-title-tk_bought', '#socialProofingAsinFaceout_feature_div']),
    bullets: [...document.querySelectorAll('#feature-bullets ul li span.a-list-item, '
      + '#featurebullets_feature_div li span.a-list-item')].map(txt).filter(Boolean),
    description: first(['#productDescription', '#aplus_feature_div']),
    details, overview, bsr, important, histogram,
    images: [...new Set(images)].slice(0, 15),
    reviews,
    location: txt(q('#glow-ingress-line2')),
  };
}"""

# The "Department" list in a category's search page (left column): ancestors
# ('‹ Any Department', '‹ Beauty'), the current category (bold, usually not a
# link), then its sub-categories (links, deeper s-navigation-indent-N).
_JS_CATEGORY = r"""() => {
  const txt = (el) => el ? el.textContent.replace(/[​-‏]/g, '').replace(/\s+/g, ' ').trim() : '';
  const nodeOf = (href) => {
    let h = href || '';
    try { h = decodeURIComponent(h); } catch (e) {}
    const ids = [...h.matchAll(/\bn:(\d+)/g)].map(m => m[1]);
    if (ids.length) return ids[ids.length - 1];          // rh=n:root,n:child → the deepest one
    const m = h.match(/[?&]node=(\d+)/);
    return m ? m[1] : '';
  };
  let box = document.querySelector('#departments');
  if (!box) {
    const head = [...document.querySelectorAll('#s-refinements span, #s-refinements h2')]
      .find(e => /^Department$/i.test(txt(e)));
    box = head ? (head.closest('#s-refinements > div, #s-refinements') || null) : null;
  }
  const items = [];
  if (box) {
    for (const li of box.querySelectorAll('li')) {
      const a = li.querySelector('a[href]');
      const raw = txt(a || li);
      const m = (li.className || '').match(/s-navigation-indent-(\d+)/);
      items.push({
        node: a ? nodeOf(a.getAttribute('href')) : '',
        name: raw.replace(/^[‹<]\s*/, ''),
        link: !!a,
        bold: !!li.querySelector('.a-text-bold'),
        indent: m ? parseInt(m[1], 10) : null,
        clear: !!(a && /s-navigation-clear-link/.test(a.className || '')) || /^[‹<]/.test(raw)
               || /^any department$/i.test(raw),
      });
    }
  }
  const info = txt(document.querySelector('[data-component-type="s-result-info-bar"] h1, '
    + '[data-component-type="s-result-info-bar"], .s-breadcrumb'));
  return { items, info, hasBox: !!box };
}"""

# A Best Sellers page: the grid's data-client-recs-list JSON lists all 50 ASINs
# with their rank even before lazy-loaded cards render; cards add title/price.
_JS_BESTSELLERS = r"""() => {
  const txt = (el) => el ? el.textContent.replace(/[​-‏]/g, '').replace(/\s+/g, ' ').trim() : '';
  const ranks = {};
  for (const g of document.querySelectorAll('[data-client-recs-list]')) {
    try {
      for (const r of JSON.parse(g.getAttribute('data-client-recs-list') || '[]')) {
        if (r && r.id) ranks[r.id] = parseInt(((r.metadataMap || {})['render.zg.rank']) || '0', 10) || null;
      }
    } catch (e) {}
  }
  let cards = [...document.querySelectorAll('[id="gridItemRoot"]')];
  if (!cards.length) cards = [...document.querySelectorAll('.zg-grid-general-faceout, li.zg-item-immersion')];
  const items = {};
  for (const c of cards) {
    let asin = '';
    const d = c.querySelector('[data-asin]');
    if (d) asin = d.getAttribute('data-asin') || '';
    const dp = [...c.querySelectorAll('a[href*="/dp/"]')];
    if (!asin && dp.length) { const m = (dp[0].getAttribute('href') || '').match(/\/dp\/([A-Z0-9]{10})/); if (m) asin = m[1]; }
    if (!asin || items[asin]) continue;
    const img = c.querySelector('img');
    const titles = dp.map(txt).filter(t => t && !/^£/.test(t)).sort((x, y) => y.length - x.length);
    const reviewA = c.querySelector('a[href*="product-reviews"]');
    items[asin] = {
      asin,
      rank: parseInt(txt(c.querySelector('.zg-bdg-text')).replace(/\D/g, ''), 10) || null,
      title: titles[0] || (img ? (img.getAttribute('alt') || '') : ''),
      price: ((c.innerText || '').match(/£\s?[\d,]+(?:\.\d{1,2})?/) || [''])[0],
      rating: txt(c.querySelector('.a-icon-alt')) || (reviewA ? (reviewA.getAttribute('title') || '') : ''),
      rating_count: reviewA ? txt(reviewA.querySelector('span.a-size-small')) : '',
      image: img ? (img.getAttribute('src') || '') : '',
    };
  }
  const next = document.querySelector('ul.a-pagination li.a-last:not(.a-disabled) a');
  return { ranks: Object.entries(ranks), items: Object.values(items), heading: txt(document.querySelector('h1')),
           hasNext: !!next };
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


def _where(e: Exception) -> str:
    """'ValueError: … (cloak_amazon_uk.py:123 in _listing_row)' for unexpected-error messages."""
    import traceback
    frames = traceback.extract_tb(e.__traceback__)
    last = frames[-1] if frames else None
    at = f" ({Path(last.filename).name}:{last.lineno} in {last.name})" if last else ""
    return f"{type(e).__name__}: {e}{at}"


def _asin(url: str) -> Optional[str]:
    match = _ASIN_RE.search(str(url or ""))
    return match.group(1) if match else None


def _price(text: str) -> Optional[float]:
    """'£12.50' → 12.5 (the first £ amount in the text)."""
    match = _PRICE_RE.search(str(text or ""))
    return float(match.group(1).replace(",", "")) if match else None


def _rating(text: str) -> Optional[float]:
    """'4.6 out of 5 stars' → 4.6"""
    match = _RATING_RE.search(str(text or ""))
    return float(match.group(1).replace(",", ".")) if match else None


def _count(text: str) -> Optional[int]:
    """'12,345 ratings' → 12345, '(12.3K)' → 12300, '1M' → 1000000."""
    match = _COUNT_RE.search(str(text or "").replace("\xa0", " "))
    if not match:
        return None
    number, unit = match.group(1), (match.group(2) or "").upper()
    if unit:
        return int(round(float(number.replace(",", "")) * (1_000 if unit == "K" else 1_000_000)))
    return int(number.replace(",", "").replace(".", ""))


def _bought(text: str) -> Optional[int]:
    """'1K+ bought in past month' → 1000 (the lower bound Amazon shows)."""
    match = _BOUGHT_RE.search(str(text or ""))
    if not match:
        return None
    number = float(match.group(1).replace(",", ""))
    unit = (match.group(2) or "").upper()
    return int(round(number * {"K": 1_000, "M": 1_000_000}.get(unit, 1)))


def _brand_from_byline(byline: str) -> str:
    """'Visit the CeraVe Store' / 'Brand: CeraVe' → 'CeraVe'."""
    text = str(byline or "").strip()
    for pattern in (r"^Visit the (.+?) Store$", r"^Brand:\s*(.+)$"):
        match = re.match(pattern, text, re.I)
        if match:
            return match.group(1).strip()
    return text


def _bsr(text: str) -> List[Tuple[int, str]]:
    """'126 in Beauty (See Top 100 in Beauty) 3 in Facial Cleansers' → [(126, 'Beauty'), (3, 'Facial Cleansers')]."""
    text = re.sub(r"\(\s*See Top[^)]*\)", " ", str(text or ""))
    return [(int(n.replace(",", "")), cat.strip()) for n, cat in _BSR_RE.findall(text.strip())]


def _review_meta(date_text: str) -> Tuple[str, str]:
    """'Reviewed in the United Kingdom on 3 March 2026' → ('United Kingdom', '2026-03-03')."""
    match = _REVIEWED_RE.search(str(date_text or ""))
    if not match:
        return "", ""
    try:
        return match.group(1), datetime.strptime(match.group(2), "%d %B %Y").strftime("%Y-%m-%d")
    except ValueError:
        return match.group(1), match.group(2)


def _helpful(text: str) -> int:
    """'12 people found this helpful' → 12, 'One person found this helpful' → 1."""
    text = str(text or "")
    if re.match(r"^\s*one person", text, re.I):
        return 1
    match = re.search(r"([\d,]+)", text)
    return int(match.group(1).replace(",", "")) if match else 0


def _unit_price(text: str) -> str:
    """'(£52.97£52.97 / litre)' → '£52.97 / litre'."""
    text = re.sub(r"(£[\d.,]+)\1", r"\1", str(text or "")).strip(" ()")
    return re.sub(r"\s+", " ", text)


def _sanitize_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    """Strip control characters openpyxl refuses to write into a cell."""
    from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].map(lambda v: ILLEGAL_CHARACTERS_RE.sub("", v) if isinstance(v, str) else v)
    return df


def _save_checkpoint(rows: list, data_prefix: str, dedupe_cols: List[str], reason: str = "checkpoint"):
    if not rows:
        return
    df = pd.DataFrame(rows).drop_duplicates(subset=dedupe_cols)
    path = Path(TEMP_DIR) / f"{data_prefix}.csv"
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"   💾 Checkpoint [{reason}]: {path}  ({len(df):,} rows)")


def _save_final(rows: list, final_prefix: str, dedupe_cols: List[str]) -> pd.DataFrame:
    if not rows:
        print(f"   ⚠️  No data to save: {final_prefix}")
        return pd.DataFrame()
    df = _sanitize_for_excel(pd.DataFrame(rows).drop_duplicates(subset=dedupe_cols))
    xlsx = Path(OUTPUT_DIR) / f"{final_prefix}_{_make_timestamp()}.xlsx"
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
        df = pd.read_csv(path, dtype=_TEXT_COLS)
        rows = df.astype(object).where(df.notna(), None).to_dict(orient="records")
        print(f"🔁 Backup loaded: {path}  ({len(rows):,} rows)")
        return rows
    except Exception as e:
        print(f"⚠️  Failed to load backup {path}: {e}")
        return []


_CHECKPOINT_FILES = {
    "listing": "temp_amazon_uk_listing",
    "detail": "temp_amazon_uk_detail",
    "review": "temp_amazon_uk_review",
}


def checkpoint_summary(kind: str) -> Optional[dict]:
    """What the 'listing' / 'detail' / 'review' checkpoint holds, so a caller can offer to resume."""
    path = Path(TEMP_DIR) / f"{_CHECKPOINT_FILES[kind]}.csv"
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, dtype=str)
    except Exception:
        return None
    if df.empty:
        return None
    return {
        "rows": len(df),
        "products": df["product_url"].nunique() if "product_url" in df.columns else len(df),
        "keywords": list(dict.fromkeys(df["keyword_searched"].fillna(""))) if "keyword_searched" in df.columns else [],
        "last_collected": df["collected_at"].max() if "collected_at" in df.columns else "",
    }


def _field_coverage(rows: List[dict], fields: List[str]) -> str:
    """'price 45/48 · rating 44/48' — a quick sign of selectors drifting."""
    total = len(rows)
    parts = []
    for f in fields:
        filled = sum(1 for r in rows if r.get(f) not in (None, "", [], {}))
        parts.append(f"{f} {filled}/{total}")
    return " · ".join(parts)


# ════════════════════════════════════════════════════════════════
# Rows
# ════════════════════════════════════════════════════════════════
def _listing_row(label: str, page_no: int, rank: int, it: dict) -> dict:
    return {
        "scraped_date": _scraped_date(),
        "keyword_searched": label,
        "page": page_no,
        "search_rank": rank,
        "asin": it["asin"],
        "product_name": it.get("title", ""),
        "brand": it.get("brand", ""),
        "price_gbp": _price(it.get("price")),
        "list_price_gbp": _price(it.get("list_price")),
        "rating": _rating(it.get("rating")),
        "rating_count": _count(it.get("rating_count")),
        "bought_past_month": _bought(it.get("bought")),
        "bought_past_month_text": it.get("bought", ""),
        "is_sponsored": bool(it.get("sponsored")),
        "is_prime": bool(it.get("prime")),
        "badge": it.get("badge", ""),
        "coupon": it.get("coupon", ""),
        "image_url": it.get("image", ""),
        "product_url": PRODUCT_URL.format(it["asin"]),
        "source": "Amazon UK",
        "collected_at": _collected_at(),
    }


def _detail_row(d: dict, asin: str) -> dict:
    details, overview, important = d.get("details") or {}, d.get("overview") or {}, d.get("important") or {}
    ranks = _bsr(d.get("bsr"))
    brand = _brand_from_byline(d.get("byline")) or overview.get("Brand") or details.get("Brand") \
        or details.get("Manufacturer", "")
    return {
        "scraped_date": _scraped_date(),
        "asin": asin,
        "product_name": d.get("title", ""),
        "brand": brand,
        "price_gbp": _price(d.get("price")),
        "list_price_gbp": _price(d.get("list_price")),
        "savings": d.get("savings", ""),
        "unit_price": _unit_price(d.get("unit_price")),
        "availability": d.get("availability", ""),
        "rating": _rating(d.get("rating")),
        "rating_count": _count(d.get("rating_count")),
        "rating_distribution_pct": json.dumps(d.get("histogram") or {}, ensure_ascii=False) if d.get("histogram") else "",
        "bought_past_month": _bought(d.get("bought")),
        "best_sellers_rank": d.get("bsr", ""),
        "bsr_rank": ranks[0][0] if ranks else None,
        "bsr_category": ranks[0][1] if ranks else "",
        "bsr_sub_rank": ranks[1][0] if len(ranks) > 1 else None,
        "bsr_sub_category": ranks[1][1] if len(ranks) > 1 else "",
        "about_this_item": "\n".join(d.get("bullets") or []),
        "description": (d.get("description") or "")[:DESCRIPTION_MAX_CHARS],
        "ingredients": important.get("Ingredients", ""),
        "directions": important.get("Directions", ""),
        "safety_information": important.get("Safety Information", "") or important.get("Safety Warning", ""),
        "date_first_available": details.get("Date First Available", ""),
        "manufacturer": details.get("Manufacturer", ""),
        "product_overview": json.dumps(overview, ensure_ascii=False) if overview else "",
        "product_details": json.dumps({k: v for k, v in details.items() if k != "Best Sellers Rank"},
                                      ensure_ascii=False) if details else "",
        "main_image": (d.get("images") or [""])[0],
        "detail_images": json.dumps(d.get("images") or [], ensure_ascii=False),
        "delivery_location": d.get("location", ""),
        "product_url": PRODUCT_URL.format(asin),
        "source": "Amazon UK",
        "collected_at": _collected_at(),
    }


def _review_id(r: dict, asin: str) -> str:
    """Amazon's own id ('R1ABC…', sometimes 'customer_review-R1ABC…'), else a stable hash."""
    rid = re.sub(r"^customer_review(?:_foreign)?-", "", str(r.get("id") or ""))
    if rid:
        return rid
    import hashlib
    key = f"{asin}|{r.get('name')}|{r.get('date')}|{r.get('body')}".encode("utf-8")
    return f"{asin}_{hashlib.md5(key).hexdigest()[:12]}"


def _review_row(r: dict, asin: str, product_name: str) -> dict:
    country, date = _review_meta(r.get("date"))
    return {
        "product_url": PRODUCT_URL.format(asin),
        "asin": asin,
        "product_name": product_name,
        "review_id": _review_id(r, asin),
        "rating_star": _rating(r.get("rating")),
        "headline": r.get("title", ""),
        "comment": r.get("body", ""),
        "user_name": r.get("name", ""),
        "review_country": country,
        "comment_date": date,
        "verified_purchase": bool(r.get("verified")),
        "helpful_votes": _helpful(r.get("helpful")),
        "variant": r.get("variant", ""),
        "source": "Amazon UK",
        "collected_at": _collected_at(),
    }


def _targets_from_input(product_urls: Optional[List[str]],
                        df_product: Optional[pd.DataFrame]) -> Dict[str, dict]:
    """Merge URLs + a dataframe into {canonical product_url: row-or-empty}, in first-seen order."""
    targets: Dict[str, dict] = {}
    if df_product is not None and not df_product.empty and "product_url" in df_product.columns:
        for row in df_product.to_dict(orient="records"):
            asin = _asin(row.get("product_url")) or (row.get("asin") if isinstance(row.get("asin"), str) else None)
            if asin:
                targets.setdefault(PRODUCT_URL.format(asin), row)
    for url in product_urls or []:
        asin = _asin(url) or (url.strip() if re.fullmatch(r"[A-Z0-9]{10}", str(url).strip()) else None)
        if asin:
            targets.setdefault(PRODUCT_URL.format(asin), {})
        else:
            print(f"   ⚠️  Skipping URL with no /dp/<ASIN>: {url}")
    return targets


def _search_url(entry: str, page_no: int) -> Tuple[str, str]:
    """(label, url) for a keyword or a pasted amazon.co.uk search URL at page N."""
    entry = entry.strip()
    if entry.startswith("http"):
        parts = urlparse(entry)
        query = {k: v[0] for k, v in parse_qs(parts.query).items() if k not in ("page", "ref", "qid")}
        query["page"] = str(page_no)
        return query.get("k", "search"), urlunparse(parts._replace(query=urlencode(query)))
    return entry, f"{SEARCH_URL}?k={quote_plus(entry)}&page={page_no}"


# ════════════════════════════════════════════════════════════════
# Browser session & page loading
# ════════════════════════════════════════════════════════════════
async def _create_browser_context():
    context = await launch_persistent_context_async(
        USER_DATA_DIR,
        headless=False,
        viewport={"width": 1366, "height": 900},
        locale="en-GB",
        timezone_id="Asia/Bangkok",   # matches the IP the requests come from
        humanize=True,
    )
    page = context.pages[0] if context.pages else await context.new_page()
    return context, page


async def _evaluate(page, script: str):
    """page.evaluate that survives one in-page navigation during the call."""
    try:
        return await page.evaluate(script)
    except Exception as e:
        if "context was destroyed" not in str(e).lower():
            raise
        await asyncio.sleep(2)
        return await page.evaluate(script)


async def _dump_debug(page, label: str) -> None:
    safe = re.sub(r"[^\w-]+", "_", label, flags=re.UNICODE)[:50]
    base = Path(LOG_DIR) / f"{datetime.now(TZ_THAI).strftime('%Y%m%d_%H%M%S')}_{safe}"
    try:
        base.with_suffix(".html").write_text(await page.content(), encoding="utf-8")
        await page.screenshot(path=str(base.with_suffix(".png")))
        print(f"   📸 Saved page: {base}.{{html,png}}")
    except Exception as e:
        print(f"   ⚠️  Could not save page: {e}")


async def _save_sample(page, kind: str) -> None:
    if SAVE_SAMPLE_PAGES and kind not in _samples_saved:
        _samples_saved.add(kind)
        await _dump_debug(page, f"sample_{kind}")


async def _dismiss_cookies(page) -> None:
    """Decline non-essential cookies on the consent banner if it is showing."""
    for selector in ("#sp-cc-rejectall-link", "button:has-text('Decline')", "input[name='reject']"):
        try:
            button = page.locator(selector).first
            if await button.count() and await button.is_visible(timeout=2000):
                await button.click()
                await asyncio.sleep(1)
                return
        except Exception:
            continue


async def _page_state(page) -> str:
    """'ok', 'captcha', 'signin' or 'blocked'."""
    state = await _evaluate(page, _JS_PAGE_STATE)
    text = f"{state['title']} {state['text']}".lower()
    if state["captchaForm"] or "validatecaptcha" in state["url"].lower() \
            or any(p in text for p in _CAPTCHA_PHRASES):
        return "captcha"
    if state["signin"]:
        return "signin"
    if any(p in text for p in _BLOCK_PHRASES):
        return "blocked"
    return "ok"


async def _wait_for_human(page, state: str) -> None:
    """Pause until a person clears the CAPTCHA / check in the browser window.
    Nothing here attempts to solve or get around it."""
    global _human_waits
    _human_waits += 1
    if _human_waits > MAX_HUMAN_WAITS_PER_RUN:
        raise _RunStopped(f"Amazon showed a check more than {MAX_HUMAN_WAITS_PER_RUN} times in this run. "
                          "Stopping — try again later, with fewer pages.")
    await _dump_debug(page, state)
    how_long = f"up to {HUMAN_WAIT_TIMEOUT / 60:.0f} minutes" if HUMAN_WAIT_TIMEOUT else "until you're done"
    print(f"\n⏸️  Amazon is showing a check ({state}) — please deal with it in the browser window. "
          f"Waiting {how_long} (Ctrl+C to stop)…")
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
            print(f"   ⏳ Still waiting ({(loop.time() - started) / 60:.0f} min so far)…")
            next_reminder += HUMAN_WAIT_REMINDER
    raise _RunStopped("Nobody cleared the check in time.")


async def _back_off(page, state: str) -> None:
    global _blocks
    _blocks += 1
    await _dump_debug(page, state)
    if _blocks > MAX_BLOCKS_PER_RUN:
        raise _RunStopped(f"Amazon refused pages {_blocks} times in this run. Stopping — wait a few hours, "
                          "then run again with fewer pages and continue from the checkpoint.")
    pause = random.uniform(BLOCK_BACKOFF_MIN, BLOCK_BACKOFF_MAX)
    print(f"   🚧 Page refused ({state}) — {_blocks}/{MAX_BLOCKS_PER_RUN}. Backing off {pause:.0f}s…")
    await asyncio.sleep(pause)


async def _check_location(page) -> None:
    """Warn once per run if the delivery area is not in the UK (prices / stock would be for export)."""
    global _location_checked
    if _location_checked:
        return
    _location_checked = True
    where = await _evaluate(page, _JS_LOCATION)
    uk = bool(re.search(r"\b[A-Z]{1,2}\d[A-Z\d]?\s*\d?[A-Z]{0,2}\b", where)) or "united kingdom" in where.lower()
    if where and not uk:
        print(f"\n⚠️  Delivery location is '{where}', not a UK postcode — prices and stock may be the export "
              "ones (often without UK VAT). Run action 0 (create_amazon_session) once to set a UK postcode.\n")
    elif where:
        print(f"   📍 Delivery location: {where}")


async def _goto_checked(page, url: str) -> None:
    """Open a page; pause for a person at a check, back off at an error page."""
    for _ in range(MAX_BLOCKS_PER_RUN + MAX_HUMAN_WAITS_PER_RUN + 1):
        await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
        await asyncio.sleep(random.uniform(2.5, 4.5))
        state = await _page_state(page)
        if state == "ok":
            await _dismiss_cookies(page)
            await _check_location(page)
            return
        if state == "signin":
            raise _RunStopped("Amazon redirected to sign-in. This module never signs in — stopping.")
        if state == "captcha":
            await _wait_for_human(page, state)
            if await _page_state(page) == "ok":
                return
            continue
        await _back_off(page, state)
    raise _RunStopped(f"Could not load {url}")


async def create_amazon_session(postcode: str = UK_POSTCODE) -> None:
    """Set a UK delivery postcode in the saved browser session (run once, or when
    the warning says it is not set). Tries to fill it in; you can also do it by hand."""
    context, page = await _create_browser_context()
    await page.goto(SITE + "/", wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
    await asyncio.sleep(3)
    await _dismiss_cookies(page)
    try:
        await page.locator("#nav-global-location-popover-link").first.click(timeout=8000)
        field = page.locator("#GLUXZipUpdateInput").first
        await field.wait_for(state="visible", timeout=10000)
        await field.fill(postcode)
        await page.locator("#GLUXZipUpdate input, #GLUXZipUpdate").first.click(timeout=8000)
        await asyncio.sleep(3)
        for selector in ("#GLUXConfirmClose", "button[name='glowDoneButton']", ".a-popover-footer .a-button-input"):
            button = page.locator(selector).first
            if await button.count() and await button.is_visible():
                await button.click()
                break
        await asyncio.sleep(2)
        await page.reload(wait_until="domcontentloaded")
        await asyncio.sleep(3)
    except Exception as e:
        print(f"⚠️  Could not set the postcode automatically ({str(e).splitlines()[0][:120]}).")
    where = await _evaluate(page, _JS_LOCATION)
    print("\n" + "—" * 60)
    print(f"📍 'Deliver to' now shows: {where or '(not found)'}")
    print(f" 1. If it is not a UK postcode, click 'Deliver to' (top left) and enter one, e.g. {postcode}.")
    print(" 2. Do NOT sign in — it is not needed.")
    print(" 3. Close the browser window when done; the session is saved.")
    print("—" * 60 + "\n")
    while len(context.pages) > 0:
        await asyncio.sleep(1)
    try:
        await context.close()
    except Exception:
        pass
    print(f"✅ Session saved to '{USER_DATA_DIR}'.")


def _reset_run() -> None:
    global _blocks, _human_waits, _location_checked
    _blocks = _human_waits = 0
    _location_checked = False


# ════════════════════════════════════════════════════════════════
# Public API: search
# ════════════════════════════════════════════════════════════════
async def run_search_keyword(
    keywords: List[str],
    max_pages_per_keyword: Optional[int] = DEFAULT_MAX_PAGES_PER_KEYWORD,
    resume: Optional[bool] = None,
) -> pd.DataFrame:
    """
    Search amazon.co.uk for each keyword (or open each pasted search URL) and
    page through the results.

    max_pages_per_keyword : None = as many as Amazon serves (capped at MAX_PAGES_HARD)
    resume                : True = start from the checkpoint and skip keywords already in it

    Rows: asin, product_name, brand (when the card shows it), price_gbp,
    list_price_gbp, rating, rating_count, bought_past_month (+ the text),
    is_sponsored, is_prime, badge, coupon, image_url, product_url,
    plus keyword / page / rank. An ASIN is kept once per keyword (its first
    appearance — sponsored slots often repeat organic results).
    """
    _reset_run()
    page_limit = min(max_pages_per_keyword or MAX_PAGES_HARD, MAX_PAGES_HARD)
    prefix = _CHECKPOINT_FILES["listing"]
    all_rows: List[dict] = _load_backup(prefix, enabled=resume)
    done = {str(r.get("keyword_searched")) for r in all_rows}

    print(f"\n🔍 Amazon UK — Search  ({len(keywords)} keyword(s), up to {page_limit} page(s) each)")
    try:
        context, page = await _create_browser_context()
        async with context:
            empty_in_a_row = 0
            for i, entry in enumerate(keywords, 1):
                label, url = _search_url(entry, 1)
                if label in done:
                    print(f"\n⏭️  [{i}/{len(keywords)}] '{label}' — already collected, skip.")
                    continue
                print(f"\n🔑 [{i}/{len(keywords)}] '{label}'  →  {url}")

                seen: set = set()
                rows_kw: List[dict] = []
                for page_no in range(1, page_limit + 1):
                    _, url = _search_url(entry, page_no)
                    await _goto_checked(page, url)
                    if page_no == 1:
                        await _save_sample(page, "search")
                    payload = await _evaluate(page, _JS_SEARCH)
                    new = [it for it in payload["items"] if it["asin"] not in seen]
                    page_rows = []
                    for it in new:
                        seen.add(it["asin"])
                        page_rows.append(_listing_row(label, page_no, len(rows_kw) + len(page_rows) + 1, it))
                    rows_kw.extend(page_rows)
                    print(f"   📄 Page {page_no}: {len(new)} new item(s)  (total {len(rows_kw)})")
                    if page_rows:
                        print("      fields: " + _field_coverage(
                            page_rows, ["product_name", "price_gbp", "rating", "rating_count", "bought_past_month"]))
                    if not new:
                        if page_no == 1:
                            await _dump_debug(page, f"empty_search_{label}")
                        break
                    if not payload["hasNext"] or page_no >= page_limit:
                        break
                    await _human_delay(PAGE_DELAY_MIN, PAGE_DELAY_MAX)

                if not rows_kw:
                    empty_in_a_row += 1
                    if empty_in_a_row >= MAX_EMPTY_KEYWORDS_IN_A_ROW:
                        raise _RunStopped(f"{empty_in_a_row} keywords in a row came back empty — the page "
                                          "layout may have changed, or Amazon is limiting requests. "
                                          f"Check the saved pages in {LOG_DIR}.")
                else:
                    empty_in_a_row = 0
                all_rows.extend(rows_kw)
                done.add(label)
                _save_checkpoint(all_rows, prefix, LISTING_DEDUPE_COLS, "auto")
                if i < len(keywords):
                    pause = random.uniform(KEYWORD_PAUSE_MIN, KEYWORD_PAUSE_MAX)
                    print(f"   ⏳ Waiting {pause:.0f}s before the next keyword…")
                    await asyncio.sleep(pause)
    except _RunStopped as e:
        print(f"\n🛑 Stopped: {e}")
    except Exception as e:
        print(f"\n🛑 Stopped by unexpected error — saving what was collected: {_where(e)}")

    _save_checkpoint(all_rows, prefix, LISTING_DEDUPE_COLS, "final")
    return _save_final(all_rows, "amazon_uk_search_list", LISTING_DEDUPE_COLS)


# ════════════════════════════════════════════════════════════════
# Public API: category crawl
# ════════════════════════════════════════════════════════════════
_CATEGORY_FILES = {
    "tree": "temp_amazon_uk_category_tree",
    "listing": "temp_amazon_uk_category_listing",
    "tasks": "temp_amazon_uk_category_tasks",
}


class _BudgetSpent(Exception):
    """This run's page budget is used up — the next (resumed) run carries on."""


def _node_from_url(text: str) -> Optional[str]:
    """Browse-node id from a category / search / Best Sellers URL, or a bare id.
    'https://www.amazon.co.uk/s?rh=n%3A117332031%2Cn%3A118464031' → '118464031' (the deepest)."""
    s = unquote(str(text or "")).strip()
    if re.fullmatch(r"\d{4,}", s):
        return s
    ids = re.findall(r"\bn:(\d+)", s)
    if ids:
        return ids[-1]
    for pattern in (r"[?&]node=(\d+)", r"/(?:gp/bestsellers|zgbs)/[^/?#]+/(\d+)"):
        match = re.search(pattern, s)
        if match:
            return match.group(1)
    return None


def _category_url(node: str, page_no: int = 1, sort: str = CATEGORY_SORT,
                  price: Optional[Tuple[int, Optional[int]]] = None) -> str:
    rh = f"n:{node}"
    if price is not None:
        lo, hi = price
        rh += f",{PRICE_FILTER}:{lo}-{'' if hi is None else hi}"
    query = {"rh": rh, "fs": "true"}
    if sort:
        query["s"] = sort
    if page_no > 1:
        query["page"] = str(page_no)
    return f"{SEARCH_URL}?{urlencode(query)}"


def _bestsellers_url(node: str, pg: int = 1, store: str = BESTSELLER_STORE) -> str:
    return f"{SITE}/gp/bestsellers/{store}/{node}" + (f"?pg={pg}" if pg > 1 else "")


def _result_count_info(text: str) -> Tuple[Optional[int], bool]:
    """(count, exact): '1-48 of over 30,000 results' → (30000, False); '1-16 of 213 results' → (213, True)."""
    text = str(text or "")
    match = re.search(r"of\s+(over\s+)?([\d,]+)\s+results", text, re.I)
    if match:
        return int(match.group(2).replace(",", "")), not match.group(1)
    match = re.search(r"([\d,]+)\s+results", text, re.I)
    return (int(match.group(1).replace(",", "")), True) if match else (None, False)


def _result_count(text: str) -> Optional[int]:
    """'1-48 of over 30,000 results for' → 30000."""
    return _result_count_info(text)[0]


def _split_price(lo: Optional[int], hi: Optional[int]) -> List[Tuple[int, Optional[int]]]:
    """Sub-bands of a price band in pence (hi None = no upper end); [] = too narrow to split.
    The whole category (lo None) first splits into PRICE_BANDS_PENCE."""
    if lo is None:
        b = PRICE_BANDS_PENCE
        return [(b[i], b[i + 1]) for i in range(len(b) - 1)] + [(b[-1], None)]
    if hi is None:
        return [(lo, lo * 2), (lo * 2, None)] if lo < 10_000_000 else []
    if hi - lo < 2 * MIN_SLICE_PENCE:
        return []
    mid = lo + (hi - lo) // 2
    mid -= mid % 10                      # whole 10p steps
    if mid <= lo:
        mid = lo + (hi - lo) // 2
    return [(lo, mid), (mid, hi)]


def _slice_label(lo: Optional[int], hi: Optional[int]) -> str:
    if lo is None:
        return ""
    return f"£{lo / 100:.2f}+" if hi is None else f"£{lo / 100:.2f}–£{hi / 100:.2f}"


def _parse_departments(data: dict, node: str) -> Tuple[str, List[List[str]]]:
    """(name of this category, [[child_node, child_name], …]) from _JS_CATEGORY's items.
    Children are the links listed after the current category and indented deeper —
    so ancestors above it and siblings at the same level are never followed."""
    items = data.get("items") or []
    idx = next((i for i, it in enumerate(items) if it.get("node") == node), None)
    if idx is None:
        idx = next((i for i, it in enumerate(items) if it.get("bold") and not it.get("link")), None)
    if idx is None:
        idx = next((i for i, it in enumerate(items) if it.get("bold")), None)
    if idx is None:   # current category not shown: everything that isn't a '‹ back' link
        name, after, cur_indent = "", [it for it in items if not it.get("clear")], None
    else:
        name, after, cur_indent = items[idx].get("name", ""), items[idx + 1:], items[idx].get("indent")
    children, seen = [], {node}
    for it in after:
        child = it.get("node")
        if not child or child in seen or it.get("clear"):
            continue
        if cur_indent is not None and it.get("indent") is not None and it["indent"] <= cur_indent:
            continue
        seen.add(child)
        children.append([child, it.get("name", "")])
    return name, children


def _bestseller_items(payload: dict) -> List[dict]:
    """All ranked ASINs of a Best Sellers page (from the grid JSON), with card data where rendered."""
    ranks = {asin: rank for asin, rank in (payload.get("ranks") or [])}
    cards = {it["asin"]: it for it in (payload.get("items") or []) if it.get("asin")}
    out = []
    for asin in list(ranks) + [a for a in cards if a not in ranks]:
        it = dict(cards.get(asin) or {"asin": asin})
        it["rank"] = it.get("rank") or ranks.get(asin)
        out.append(it)
    return sorted(out, key=lambda it: it.get("rank") or 10 ** 6)


def _category_row(node: dict, list_type: str, page_no: int, rank: int, it: dict, price_slice: str = "") -> dict:
    row = _listing_row(node["path"], page_no, rank, it)
    row.update({
        "node_id": node["node_id"],
        "node_name": node["name"],
        "node_path": node["path"],
        "node_depth": int(node["depth"]),
        "list_type": list_type,                       # 'best_sellers' or 'search'
        "sort": "" if list_type == "best_sellers" else (CATEGORY_SORT or "featured"),
        "price_slice": price_slice,                   # '£5.00–£7.00' in "every product" mode
    })
    return row


def _truthy(value) -> bool:
    return value is True or str(value).strip().lower() in ("true", "1", "yes")


def _save_category_state(tree: Dict[str, dict], rows: List[dict], done: set) -> None:
    """Checkpoint the tree, the rows and the finished page tasks (quietly — it runs after every page)."""
    for key, data, cols in (("tree", list(tree.values()), ["node_id"]),
                            ("listing", rows, ["node_id", "list_type", "asin"]),
                            ("tasks", [{"task": t} for t in sorted(done)], ["task"])):
        if data:
            pd.DataFrame(data).drop_duplicates(subset=cols).to_csv(
                Path(TEMP_DIR) / f"{_CATEGORY_FILES[key]}.csv", index=False, encoding="utf-8-sig")


def _load_category_state(root: str):
    """(tree, rows, done_tasks, other_root) from the category checkpoint. other_root is
    set (and the rest empty) when the checkpoint belongs to a different start category.
    Read as text first: a NaN parent_id would otherwise turn every node id into a float."""
    folder = Path(TEMP_DIR)
    path = folder / f"{_CATEGORY_FILES['tree']}.csv"
    if not path.exists():
        return {}, [], set(), None
    try:
        tdf = pd.read_csv(path, dtype=str).fillna("")
    except Exception as e:
        print(f"⚠️  Couldn't read {path}: {e} — starting fresh.")
        return {}, [], set(), None
    if tdf.empty:
        return {}, [], set(), None
    if tdf["node_id"].iloc[0] != root:
        return {}, [], set(), tdf["node_id"].iloc[0]
    tree = {}
    for n in tdf.to_dict("records"):
        n["depth"] = int(float(n["depth"] or 0))
        n["visited"] = _truthy(n["visited"])
        n["result_count"] = int(float(n["result_count"])) if n["result_count"] else None
        n["children"] = n["children"] or "[]"
        tree[n["node_id"]] = n
    rows: List[dict] = []
    lpath = folder / f"{_CATEGORY_FILES['listing']}.csv"
    if lpath.exists():
        ldf = pd.read_csv(lpath, dtype={"asin": str, "node_id": str})
        rows = ldf.astype(object).where(ldf.notna(), None).to_dict(orient="records")
    done: set = set()
    kpath = folder / f"{_CATEGORY_FILES['tasks']}.csv"
    if kpath.exists():
        done = set(pd.read_csv(kpath, dtype=str)["task"].dropna())
    print(f"🔁 Checkpoint loaded: {len(tree)} categor(ies), {len(rows):,} row(s), {len(done):,} page(s) done")
    return tree, rows, done, None


def category_checkpoint_summary() -> Optional[dict]:
    """What the category checkpoint holds, so a caller can offer to resume."""
    path = Path(TEMP_DIR) / f"{_CATEGORY_FILES['tree']}.csv"
    if not path.exists():
        return None
    try:
        tree = pd.read_csv(path, dtype=str)
    except Exception:
        return None
    if tree.empty:
        return None
    listing = Path(TEMP_DIR) / f"{_CATEGORY_FILES['listing']}.csv"
    rows = pd.read_csv(listing, dtype=str) if listing.exists() else pd.DataFrame()
    return {
        "root_node": tree["node_id"].iloc[0],
        "root_name": tree["name"].iloc[0],
        "categories": len(tree),
        "visited": int(tree["visited"].map(_truthy).sum()),
        "rows": len(rows),
        "products": rows["asin"].nunique() if "asin" in rows.columns else 0,
        "last_collected": rows["collected_at"].max() if "collected_at" in rows.columns else "",
    }


async def _scroll_page(page, rounds: int = 8) -> None:
    """Best Sellers cards render as you scroll."""
    for _ in range(rounds):
        await page.mouse.wheel(0, 1600)
        await asyncio.sleep(random.uniform(0.8, 1.5))


def _save_category_final(rows: List[dict], tree: Dict[str, dict]) -> pd.DataFrame:
    """One workbook: products (one row per ASIN, best rank first), appearances
    (every category / list an ASIN showed up in), categories (the tree)."""
    if not rows:
        print("   ⚠️  No data to save: amazon_uk_category")
        return pd.DataFrame()
    df = _sanitize_for_excel(pd.DataFrame(rows).drop_duplicates(subset=["node_id", "list_type", "asin"]))
    df["_order"] = (df["list_type"] != "best_sellers").astype(int)
    ordered = df.sort_values(["_order", "search_rank"])
    first = ordered.drop_duplicates("asin")
    # A Best Sellers item known only from the grid JSON has no title/price — take
    # each field from the ASIN's best-ranked appearance that has it.
    fill_cols = [c for c in ("product_name", "brand", "price_gbp", "list_price_gbp", "rating", "rating_count",
                             "bought_past_month", "bought_past_month_text", "image_url") if c in df.columns]
    filled = ordered.replace("", pd.NA).groupby("asin")[fill_cols].first()
    first = first.drop(columns=fill_cols).merge(filled, left_on="asin", right_index=True, how="left")
    per_asin = df.groupby("asin").agg(
        n_categories=("node_id", "nunique"),
        categories=("node_path", lambda s: " | ".join(list(dict.fromkeys(s))[:5])),
    )
    products = first.drop(columns=["_order", "keyword_searched", "page"]).rename(columns={
        "search_rank": "best_rank", "list_type": "best_rank_list", "node_path": "best_rank_category",
    }).merge(per_asin, on="asin", how="left")
    lead = ["asin", "product_name", "brand", "best_rank", "best_rank_list", "best_rank_category",
            "n_categories", "categories"]
    products = products[lead + [c for c in products.columns if c not in lead]].reset_index(drop=True)

    parents = {str(n.get("parent_id")) for n in tree.values()}
    cats = pd.DataFrame([{
        "node_id": n["node_id"], "name": n["name"], "path": n["path"], "depth": n["depth"],
        "parent_id": n["parent_id"], "result_count": n.get("result_count"),
        "result_count_is_exact": _truthy(n.get("result_exact")),
        "sub_categories_listed": len(json.loads(n.get("children") or "[]")),
        "crawled_as_lowest": str(n["node_id"]) not in parents, "visited": _truthy(n.get("visited")),
        "products_collected": len({r["asin"] for r in rows if str(r.get("node_id")) == str(n["node_id"])}),
        "url": _category_url(n["node_id"], 1, ""),
    } for n in tree.values()])

    root = next(iter(tree.values()))
    slug = re.sub(r"[^\w]+", "_", str(root.get("name") or root["node_id"]))[:40].strip("_")
    xlsx = Path(OUTPUT_DIR) / f"amazon_uk_category_{slug}_{_make_timestamp()}.xlsx"
    with pd.ExcelWriter(xlsx, engine="openpyxl") as xw:
        products.to_excel(xw, sheet_name="products", index=False)
        df.drop(columns=["_order"]).to_excel(xw, sheet_name="appearances", index=False)
        cats.to_excel(xw, sheet_name="categories", index=False)
    print("\n" + "=" * 60)
    print("✅ FINAL FILE SAVED : amazon_uk_category")
    print(f"📊 Products         : {len(products):,} unique ASIN(s) from {cats['visited'].sum()} categor(ies)")
    print(f"💾 XLSX             : {xlsx}")
    print("=" * 60)
    return products


async def run_category_crawl(
    start: str,
    max_depth: int = CATEGORY_MAX_DEPTH,
    pages_per_node: int = CATEGORY_PAGES_PER_NODE,
    bestseller_pages: int = CATEGORY_BESTSELLER_PAGES,
    page_budget: Optional[int] = CATEGORY_PAGES_PER_RUN,
    resume: Optional[bool] = None,
    exhaustive: bool = False,
) -> pd.DataFrame:
    """
    Collect the top products of a category and all its sub-categories — or,
    with exhaustive=True, every product Amazon will list in the lowest ones.

    start            : a category URL (/b?node=…, a search URL with rh=n:…, a Best
                       Sellers URL) or the browse-node number
    max_depth        : sub-category levels to follow (0 = only the start category)
    pages_per_node   : search pages (sorted by CATEGORY_SORT, ~48 items each) for
                       every lowest sub-category; page 1 is read for every category
    bestseller_pages : Best Sellers pages (50 each) for every category; 0 = skip
    page_budget      : page loads this run (None = no limit); resume continues
    resume           : True = continue from the checkpoint of the same start category
    exhaustive       : page through every lowest category to the end instead of
                       pages_per_node; one with more than CATEGORY_SLICE_TARGET
                       results is split into price bands (halved until each fits).
                       Many more page loads — plan on several runs (resume).

    Steps: (1) open each category's search page — its "Department" list gives
    the sub-categories, its results give page 1; (2) more search pages for the
    lowest sub-categories; (3) Best Sellers pages. Returns one row per ASIN
    (best rank first), the same as the 'products' sheet of the saved workbook.
    """
    _reset_run()
    root = _node_from_url(start)
    if not root:
        print(f"❌ No category (browse node) found in: {start}")
        return pd.DataFrame()

    tree: Dict[str, dict] = {}
    rows: List[dict] = []
    done: set = set()
    if (RESUME_FROM_BACKUP if resume is None else resume):
        tree, rows, done, other_root = _load_category_state(root)
        if other_root:
            print(f"ℹ️  The checkpoint is for category {other_root}, not {root} — starting fresh.")
    debug_left = {"n": 3}   # empty-page HTML dumps per run

    async def dump_once(label: str):
        if debug_left["n"] > 0:
            debug_left["n"] -= 1
            await _dump_debug(page, label)

    if root not in tree:
        tree[root] = {"node_id": root, "name": "", "parent_id": "", "depth": 0, "path": "",
                      "result_count": None, "children": "[]", "visited": False}

    left = {"pages": page_budget}

    def spend():
        if left["pages"] is not None:
            if left["pages"] <= 0:
                raise _BudgetSpent()
            left["pages"] -= 1

    print(f"\n📂 Amazon UK — Category crawl from node {root}  (depth {max_depth}, {pages_per_node} search "
          f"page(s) per lowest category, {bestseller_pages} Best Sellers page(s), "
          f"budget {page_budget or 'no limit'} pages)")
    try:
        context, page = await _create_browser_context()
        async with context:
            # (1) the tree, breadth first; each visit also yields search page 1
            empty_in_a_row = 0
            while True:
                todo = [n for n in tree.values() if not _truthy(n.get("visited"))]
                if not todo:
                    break
                n = min(todo, key=lambda x: int(x["depth"]))
                node, depth = n["node_id"], int(n["depth"])
                spend()
                await _goto_checked(page, _category_url(node, 1))
                await _save_sample(page, "category")
                data = await _evaluate(page, _JS_CATEGORY)
                name, children = _parse_departments(data, node)
                parent = tree.get(str(n.get("parent_id") or ""))
                n["name"] = name or n.get("name") or node
                n["path"] = f"{parent['path']} > {n['name']}" if parent and parent.get("path") else n["name"]
                n["result_count"], n["result_exact"] = _result_count_info(data.get("info"))
                n["children"] = json.dumps(children, ensure_ascii=False)
                n["visited"] = True

                payload = await _evaluate(page, _JS_SEARCH)
                seen, page_rows = set(), []
                for it in payload["items"]:
                    if it["asin"] not in seen:
                        seen.add(it["asin"])
                        page_rows.append(_category_row(n, "search", 1, len(page_rows) + 1, it))
                rows.extend(page_rows)
                done.add(f"search:{node}:1")
                if not payload["hasNext"]:
                    done.update(f"search:{node}:{p}" for p in range(2, MAX_PAGES_HARD + 1))

                added = 0
                if depth < max_depth:
                    for child, child_name in children:
                        if child not in tree and len(tree) < CATEGORY_MAX_NODES:
                            tree[child] = {"node_id": child, "name": child_name, "parent_id": node,
                                           "depth": depth + 1, "path": "", "result_count": None,
                                           "children": "[]", "visited": False}
                            added += 1
                count = f"{n['result_count']:,}" if n["result_count"] else "?"
                print(f"   📂 [{depth}] {n['path']} — {len(children)} sub-categor(ies) listed, {added} queued · "
                      f"{len(page_rows)} item(s) on page 1 · {count} results"
                      + ("" if data.get("hasBox") else "  ⚠️ no Department list found"))
                if page_rows:
                    print("      fields: " + _field_coverage(page_rows, ["product_name", "price_gbp", "rating",
                                                                         "rating_count", "bought_past_month"]))
                if not page_rows and not children:
                    empty_in_a_row += 1
                    await dump_once(f"empty_category_{node}")
                    if empty_in_a_row >= CATEGORY_EMPTY_IN_A_ROW:
                        raise _RunStopped(f"{empty_in_a_row} categories in a row had no items and no sub-categories "
                                          f"— the layout may have changed. Check the saved pages in {LOG_DIR}.")
                else:
                    empty_in_a_row = 0
                _save_category_state(tree, rows, done)
                await _human_delay(PAGE_DELAY_MIN, PAGE_DELAY_MAX)

            # (2) the lowest categories: more search pages — or, exhaustive, every
            #     product Amazon will list, splitting big ones into price bands
            parents = {str(x.get("parent_id")) for x in tree.values()}
            lowest = [x for x in tree.values() if x["node_id"] not in parents]
            seen_by_node: Dict[str, set] = {}
            for r in rows:
                if r.get("list_type") == "search":
                    seen_by_node.setdefault(str(r["node_id"]), set()).add(r["asin"])

            def add_items(n: dict, items: List[dict], page_no: int, price_slice: str = "") -> int:
                have = seen_by_node.setdefault(n["node_id"], set())
                new = 0
                for it in items:
                    if it["asin"] not in have:
                        have.add(it["asin"])
                        new += 1
                        rows.append(_category_row(n, "search", page_no, len(have), it, price_slice))
                return new

            async def every_product(n: dict) -> None:
                """Page through one lowest category to the end, split by price where it is too big.
                Progress lives in `done` (split:/sp:/slice: keys), so a resumed run picks up mid-split."""
                node = n["node_id"]
                small = _truthy(n.get("result_exact")) and int(n.get("result_count") or 0) <= CATEGORY_SLICE_TARGET
                queue: List[Tuple[Optional[int], Optional[int]]] = [(None, None)] if small else _split_price(None, None)
                while queue:
                    lo, hi = queue.pop(0)
                    key = f"{node}:{lo}-{hi}"
                    if f"split:{key}" in done:
                        queue[0:0] = _split_price(lo, hi)
                        continue
                    if f"slice:{key}" in done:
                        continue
                    label = _slice_label(lo, hi)
                    if lo is None:   # unsplit: page 1 was read in step 1
                        done.add(f"sp:{key}:1")
                        if f"search:{node}:2" in done:   # …and it had no next page
                            done.add(f"slice:{key}")
                            continue
                    page_no = 1
                    while f"sp:{key}:{page_no}" in done:
                        page_no += 1
                    while page_no <= MAX_PAGES_HARD:
                        spend()
                        await _goto_checked(page, _category_url(node, page_no, price=None if lo is None else (lo, hi)))
                        payload = await _evaluate(page, _JS_SEARCH)
                        if page_no == 1:
                            count, exact = _result_count_info((await _evaluate(page, _JS_CATEGORY)).get("info"))
                            children = _split_price(lo, hi)
                            if children and (not exact or (count or 0) > CATEGORY_SLICE_TARGET):
                                add_items(n, payload["items"], 1, label)
                                done.add(f"split:{key}")
                                queue[0:0] = children
                                print(f"   ✂️  {n['path']} {label}: {count if count is not None else '?'}"
                                      f"{'' if exact else '+'} results — split into {len(children)} price bands")
                                _save_category_state(tree, rows, done)
                                await _human_delay(PAGE_DELAY_MIN, PAGE_DELAY_MAX)
                                break
                        new = add_items(n, payload["items"], page_no, label)
                        done.add(f"sp:{key}:{page_no}")
                        print(f"   📄 {n['path']} {label or '(all prices)'} page {page_no}: {new} new "
                              f"(category total {len(seen_by_node.get(node, ())):,})")
                        _save_category_state(tree, rows, done)
                        await _human_delay(PAGE_DELAY_MIN, PAGE_DELAY_MAX)
                        if not payload["items"] or not payload["hasNext"]:
                            done.add(f"slice:{key}")
                            break
                        page_no += 1
                    else:
                        done.add(f"slice:{key}")

            if exhaustive:
                total = sum(int(x.get("result_count") or 0) for x in lowest)
                pages_needed = total // 45 + len(lowest)
                runs = f"~{-(-pages_needed // page_budget)} run(s) of {page_budget} pages" if page_budget else "one long run"
                print(f"\n🧮 Every product of {len(lowest)} lowest categor(ies): Amazon reports about {total:,} "
                      f"results → at least {pages_needed:,} search pages (more for price splits) — {runs}.")
                for n in lowest:
                    await every_product(n)
            else:
                for n in lowest:
                    node = n["node_id"]
                    for page_no in range(2, min(pages_per_node, MAX_PAGES_HARD) + 1):
                        task = f"search:{node}:{page_no}"
                        if task in done:
                            continue
                        spend()
                        await _goto_checked(page, _category_url(node, page_no))
                        payload = await _evaluate(page, _JS_SEARCH)
                        new = add_items(n, payload["items"], page_no)
                        done.add(task)
                        print(f"   📄 {n['path']} — page {page_no}: {new} new item(s)")
                        if not new or not payload["hasNext"]:
                            done.update(f"search:{node}:{p}" for p in range(page_no + 1, MAX_PAGES_HARD + 1))
                        _save_category_state(tree, rows, done)
                        await _human_delay(PAGE_DELAY_MIN, PAGE_DELAY_MAX)
                        if f"search:{node}:{page_no + 1}" in done:
                            break

            # (3) Best Sellers for every category
            for n in tree.values():
                node = n["node_id"]
                for pg in range(1, bestseller_pages + 1):
                    task = f"bs:{node}:{pg}"
                    if task in done:
                        continue
                    spend()
                    await _goto_checked(page, _bestsellers_url(node, pg))
                    await _scroll_page(page)
                    await _save_sample(page, "bestsellers")
                    payload = await _evaluate(page, _JS_BESTSELLERS)
                    items = _bestseller_items(payload)
                    for i, it in enumerate(items, start=1):
                        rows.append(_category_row(n, "best_sellers", pg, it.get("rank") or 50 * (pg - 1) + i, it))
                    done.add(task)
                    print(f"   🏆 {n['path']} — Best Sellers page {pg}: {len(items)} item(s)"
                          + (f"  ({payload.get('heading')})" if pg == 1 else ""))
                    if items:
                        print("      fields: " + _field_coverage(
                            [_listing_row("", 0, 0, it) for it in items], ["product_name", "price_gbp", "rating"]))
                    else:
                        await dump_once(f"empty_bestsellers_{node}")
                    if not items or not payload["hasNext"]:
                        done.update(f"bs:{node}:{p}" for p in range(pg + 1, bestseller_pages + 1))
                    _save_category_state(tree, rows, done)
                    await _human_delay(PAGE_DELAY_MIN, PAGE_DELAY_MAX)
                    if f"bs:{node}:{pg + 1}" in done:
                        break
            print("\n✅ Category crawl complete.")
    except _BudgetSpent:
        unopened = sum(1 for n in tree.values() if not _truthy(n.get("visited")))
        where = (f"{unopened} categor(ies) not opened yet" if unopened
                 else "all categories opened; still paging through them" + (" / Best Sellers" if bestseller_pages else ""))
        print(f"\n⏸️  Page budget ({page_budget}) used up — {where}. "
              "Run action 6 again with the same category and settings and answer 'y' to continue.")
    except _RunStopped as e:
        print(f"\n🛑 Stopped: {e}")
    except Exception as e:
        print(f"\n🛑 Stopped by unexpected error — saving what was collected: {_where(e)}")

    _save_category_state(tree, rows, done)
    return _save_category_final(rows, tree)


# ════════════════════════════════════════════════════════════════
# Public API: product pages (details + the reviews printed on them)
# ════════════════════════════════════════════════════════════════
async def _product_page(page, url: str) -> Optional[dict]:
    """Raw data of one product page; reuses an earlier load in this process."""
    asin = _asin(url)
    if asin in _dp_cache:
        return _dp_cache[asin]
    await _goto_checked(page, url)
    await _save_sample(page, "product")
    data = await _evaluate(page, _JS_DETAIL)
    if not data.get("title"):
        await _dump_debug(page, f"no_title_{asin}")
        return None
    _dp_cache[asin] = data
    return data


async def _run_product_pages(targets: Dict[str, dict], prefix: str, dedupe_cols: List[str],
                             resume: Optional[bool], make_rows, label: str) -> List[dict]:
    _reset_run()
    all_rows: List[dict] = _load_backup(prefix, enabled=resume)
    done_urls = {r.get("product_url") for r in all_rows}
    todo = [u for u in targets if u not in done_urls]
    print(f"\n{label}  ({len(todo)} to fetch, {len(targets) - len(todo)} already done)")
    if not todo:
        return all_rows
    context, page = None, None
    try:
        for i, url in enumerate(todo, 1):
            asin = _asin(url)
            if asin not in _dp_cache and page is None:
                context, page = await _create_browser_context()
            loaded_now = asin not in _dp_cache
            data = await _product_page(page, url)
            if not data:
                print(f"   ⚠️  [{i}/{len(todo)}] No product data for {asin} (removed, or the layout changed).")
            else:
                rows = make_rows(data, asin)
                all_rows.extend(rows)
                print(f"   ✅ [{i}/{len(todo)}] {data['title'][:65]}  → {len(rows)} row(s)"
                      + ("" if loaded_now else "  (page from earlier step)"))
            if i % CHECKPOINT_EVERY == 0:
                _save_checkpoint(all_rows, prefix, dedupe_cols, "auto")
            if loaded_now and i < len(todo):
                await _human_delay(DETAIL_DELAY_MIN, DETAIL_DELAY_MAX)
    except _RunStopped as e:
        print(f"\n🛑 Stopped: {e}")
    except Exception as e:
        print(f"\n🛑 Stopped by unexpected error — saving what was collected: {_where(e)}")
    finally:
        if context:
            try:
                await context.close()
            except Exception:
                pass
    _save_checkpoint(all_rows, prefix, dedupe_cols, "final")
    return all_rows


async def run_product_detail(
    product_urls: Optional[List[str]] = None,
    df_product: Optional[pd.DataFrame] = None,
    resume: Optional[bool] = None,
) -> pd.DataFrame:
    """
    Product-page detail for each ASIN: price, RRP, availability, rating +
    histogram, Best Sellers Rank, bullets, description, ingredients,
    product details / overview tables, images.

    product_urls / df_product : either or both (URLs, bare ASINs, or a df with 'product_url')
    resume                    : True = start from the checkpoint and skip products in it
    """
    targets = _targets_from_input(product_urls, df_product)
    if not targets:
        print("❌ run_product_detail: no Amazon product URLs / ASINs provided.")
        return pd.DataFrame()
    rows = await _run_product_pages(
        targets, _CHECKPOINT_FILES["detail"], DETAIL_DEDUPE_COLS, resume,
        lambda data, asin: [_detail_row(data, asin)], "📝 Amazon UK — Product Detail",
    )
    if rows:
        print("   fields: " + _field_coverage(rows, ["price_gbp", "rating", "rating_count", "brand",
                                                    "best_sellers_rank", "about_this_item", "ingredients"]))
    return _save_final(rows, "amazon_uk_detail", DETAIL_DEDUPE_COLS)


async def run_product_reviews(
    product_urls: Optional[List[str]] = None,
    df_product: Optional[pd.DataFrame] = None,
    resume: Optional[bool] = None,
) -> pd.DataFrame:
    """
    The reviews printed on each product page (no sign-in, so usually ~8 UK
    reviews plus some from other countries — see review_country). If
    run_product_detail already opened the page in this run, it is not loaded again.
    """
    targets = _targets_from_input(product_urls, df_product)
    if not targets:
        print("❌ run_product_reviews: no Amazon product URLs / ASINs provided.")
        return pd.DataFrame()
    rows = await _run_product_pages(
        targets, _CHECKPOINT_FILES["review"], REVIEW_DEDUPE_COLS, resume,
        lambda data, asin: [_review_row(r, asin, data.get("title", "")) for r in data.get("reviews") or []],
        "💬 Amazon UK — Reviews on product pages",
    )
    if rows:
        print("   fields: " + _field_coverage(rows, ["rating_star", "headline", "comment", "comment_date"]))
    return _save_final(rows, "amazon_uk_reviews", REVIEW_DEDUPE_COLS)
