"""
find_brand_links.py — Phase 2: look up the links brand_master.xlsx is missing.

For every brand whose link on a platform is missing (or is only a "company
shop" candidate — skip those with --missing-only), look the brand up and
write candidates WITH EVIDENCE to brand_links_found.xlsx for review:

  watsons  one page: the brand list at /th/b/brandlist (robots.txt allows
           it; /search is disallowed, so no searching) -> name match
  shopee   page 1 of search (60 items) per brand, via cloak_shopee_AI_bot.
           Items are grouped by shop: how many are this brand (Shopee's
           brand field, else the title), sold count, Shopee Mall flag.
           Link = https://shopee.co.th/shop/<shopid>
  lazada   page 1 of search sorted by sold (40 items), via cloak_lazada —
           same grouping (LazMall flag); then one product of each top shop
           is opened to read its /shop/<name> link
  konvy    page 1 of the title search, via cloak_konvy: counts products
           whose URL starts with the brand's slug (/<brand-slug>/<item>-<id>.html).
           Konvy is scraped by keyword, so the link kept is that search URL

FastMoss is not looked up (no scraper; paid service).

Review brand_links_found.xlsx: `accept` is pre-filled "y" only for strong
matches. Set y / n yourself, then rebuild the master:
    python scripts/build_brand_master.py --found brand_links_found.xlsx

Each platform's results are saved as it goes; a (brand, platform) already in
brand_links_found.xlsx is skipped next run — delete its rows to look again.

USAGE
python scripts/find_brand_links.py --platforms watsons
python scripts/find_brand_links.py --platforms shopee --limit 5          # pilot
python scripts/find_brand_links.py --platforms shopee,lazada,konvy
python scripts/find_brand_links.py --platforms lazada --brands "PALMOLIVE,PROTEX"
"""

import argparse
import asyncio
import csv
import os
import random
import re
import sys
import tempfile
from collections import Counter
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from pathlib import Path
from urllib.parse import quote, urlsplit

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
from build_brand_master import key, name_score  # noqa: E402

MASTER_DEFAULT = "brand_master.xlsx"
FOUND_DEFAULT = "brand_links_found.xlsx"
RAW_DIR = Path("./data/brand_links")
TZ_THAI = timezone(timedelta(hours=7))
LOOKUP_PLATFORMS = ["watsons", "shopee", "lazada", "konvy"]
CHUNK = 3  # brands per scraper call; results are saved after each chunk (a stop loses at most this many)

FOUND_COLS = [
    "brand", "platform", "reason", "rank", "confidence", "accept",
    "candidate_url", "name_on_site", "official", "brand_items",
    "items_from_this_shop", "brand_items_all_shops", "sold", "th_hint",
    "sample_titles", "keyword", "note", "checked_at",
]
THAI = re.compile(r"[฀-๿]")


def now() -> str:
    return datetime.now(TZ_THAI).strftime("%Y-%m-%d %H:%M")


def num(v) -> float:
    try:
        f = float(v)
        return 0.0 if f != f else f  # NaN -> 0
    except (TypeError, ValueError):
        return 0.0


def chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


# ── which brands to look up ──────────────────────────────────────────────────

def brands_to_look_up(master: pd.DataFrame, platform: str, missing_only: bool, only: set):
    out = []
    for r in master.to_dict("records"):
        if only and r["brand"] not in only:
            continue
        hows = [h for h in str(r.get(f"{platform}_how", "")).split("\n") if h.strip()]
        if not hows:
            reason = "missing"
        elif all(h == "company shop" for h in hows):
            reason = "company shop only"
        else:
            continue
        if missing_only and reason != "missing":
            continue
        aliases = [a.strip() for a in str(r.get("aliases", "")).split(",") if a.strip()]
        out.append({
            "brand": r["brand"],
            "keyword": (r.get("keyword_en") or r["brand"]).strip(),
            "aliases": aliases,
            "keys": {key(r["brand"])} | {key(a) for a in aliases},
            "short": len(key(r["brand"])) <= 3,
            "reason": reason,
        })
    return out


# ── results file ─────────────────────────────────────────────────────────────

class FoundFile:
    """brand_links_found.xlsx — appended to and saved after every chunk, so
    an interrupted run keeps what it found. The user's accept / note edits
    on existing rows are kept because existing rows are never rewritten."""

    def __init__(self, path: Path):
        self.path = path
        if path.exists():
            self.df = pd.read_excel(path, sheet_name="found", dtype=str).fillna("")
        else:
            self.df = pd.DataFrame(columns=FOUND_COLS)
        self.done = set(zip(self.df["brand"], self.df["platform"]))

    def add(self, rows):
        if not rows:
            return
        new = pd.DataFrame(rows, columns=FOUND_COLS).astype(str)
        self.df = pd.concat([self.df, new], ignore_index=True)
        self.done |= set(zip(new["brand"], new["platform"]))
        self.save()

    def save(self):
        fd, tmp = tempfile.mkstemp(suffix=".xlsx", dir=self.path.parent)
        os.close(fd)
        df = self.df.copy()
        for c in ("rank", "brand_items", "items_from_this_shop", "brand_items_all_shops", "sold"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        with pd.ExcelWriter(tmp, engine="openpyxl") as xw:
            df.to_excel(xw, sheet_name="found", index=False)
            ws = xw.sheets["found"]
            ws.freeze_panes = "C2"
            ws.auto_filter.ref = ws.dimensions
            widths = {"brand": 22, "candidate_url": 48, "name_on_site": 28, "sample_titles": 60, "note": 50}
            for c in ws[1]:
                ws.column_dimensions[c.column_letter].width = widths.get(str(c.value), 13)
        try:
            os.replace(tmp, self.path)
        except PermissionError:
            os.remove(tmp)
            alt = self.path.with_name(f"{self.path.stem}_{datetime.now():%H%M%S}.xlsx")
            self.df.to_excel(alt, sheet_name="found", index=False)
            print(f"⚠️  {self.path} is open in Excel — saved to {alt} instead. Close it and merge by hand.")


def row(b, platform, **kw):
    r = {c: "" for c in FOUND_COLS}
    r.update(brand=b["brand"], platform=platform, reason=b["reason"], keyword=b["keyword"], checked_at=now())
    r.update({k: ("" if v is None else v) for k, v in kw.items()})
    return r


# ── shared: rank shops from search items ─────────────────────────────────────

def item_hit(b, brand_field: str, title: str) -> str:
    if key(brand_field) in b["keys"]:
        return "brand field"
    if not b["short"]:
        tk = key(title)
        if any(len(k) >= 4 and k in tk for k in b["keys"]):
            return "title"
    return ""


def thai_hint(titles) -> str:
    """Most common Thai word across this brand's item titles — a hint for
    keyword_th, taken from the site's own text (check it before using)."""
    cnt = Counter()
    for t in titles:
        words = {w for w in re.split(r"[\s\[\]()【】/|,+\-–:!*]+", str(t)) if THAI.search(w) and 2 <= len(w) <= 20}
        cnt.update(words)
    if not cnt:
        return ""
    word, n = cnt.most_common(1)[0]
    return word if n >= 3 and n / max(len(titles), 1) >= 0.4 else ""


# Shopee's current search response has no Mall flag (is_official_shop is
# False even for "Colgate Official Shop", checked 2026-11-16), so a shop whose
# own name says it is official counts too — shown as "name says official".
OFFICIAL_WORDS = re.compile(r"official|flagship|\bmall\b|ออฟฟิเชียล|ทางการ", re.I)


def rank_shops(b, items, top=3):
    """items: dicts with shop_id, shop_name, official, brand_field, title, sold, url."""
    shops, brand_titles = {}, []
    for it in items:
        s = shops.setdefault(it["shop_id"], {
            "shop_id": it["shop_id"], "shop_name": it["shop_name"], "official": False, "official_flag": False,
            "shop_items": 0, "brand_items": 0, "sold": 0.0, "titles": [], "product_url": "", "by_field": 0,
        })
        s["shop_items"] += 1
        s["official_flag"] = s["official_flag"] or bool(it["official"])
        s["official"] = s["official_flag"] or bool(OFFICIAL_WORDS.search(s["shop_name"]))
        hit = item_hit(b, it["brand_field"], it["title"])
        if hit:
            s["brand_items"] += 1
            s["by_field"] += hit == "brand field"
            s["sold"] += num(it["sold"])
            brand_titles.append(it["title"])
            if len(s["titles"]) < 3:
                s["titles"].append(str(it["title"])[:80])
            if not s["product_url"]:
                s["product_url"] = it["url"]
    total = sum(s["brand_items"] for s in shops.values())
    hits = [s for s in shops.values() if s["brand_items"]]
    hits.sort(key=lambda s: (name_score(b["brand"], s["shop_name"]) > 0, s["official"], s["brand_items"], s["sold"]),
              reverse=True)
    return hits[:top], total, thai_hint(brand_titles)


def shop_confidence(b, s) -> str:
    named = any(name_score(n, s["shop_name"]) > 0 for n in [b["brand"], *b["aliases"]])
    share = s["brand_items"] / max(s["shop_items"], 1)
    if s["official"] and (named or (s["brand_items"] >= 3 and share >= 0.6)):
        return "high"
    if s["official"] or (named and s["brand_items"] >= 2):
        return "medium"
    return "low"


def shop_rows(b, platform, items, url_of, n_items):
    if not items:
        return [row(b, platform, rank=0, confidence="not found", note="search returned nothing")]
    ranked, total, th = rank_shops(b, items)
    if not ranked:
        return [row(b, platform, rank=0, confidence="not found", th_hint=th,
                    note=f"no item of this brand among the first {n_items} results")]
    out = []
    for i, s in enumerate(ranked, start=1):
        conf = shop_confidence(b, s)
        out.append(row(
            b, platform, rank=i, confidence=conf, accept="y" if conf == "high" else "",
            candidate_url=url_of(s), name_on_site=s["shop_name"],
            official="yes" if s["official_flag"] else ("name says official" if s["official"] else "no"),
            brand_items=s["brand_items"],
            items_from_this_shop=s["shop_items"], brand_items_all_shops=total, sold=int(s["sold"]),
            th_hint=th if i == 1 else "", sample_titles=" | ".join(s["titles"]),
            note="" if s["by_field"] else "matched on title only (site's brand field differs)",
            _product_url=s["product_url"], _shop_id=s["shop_id"],
        ))
    return out


# ── Watsons ──────────────────────────────────────────────────────────────────

async def watsons_brandlist(list_file):
    if list_file:
        with open(list_file, encoding="utf-8-sig") as fh:  # written with a BOM for Excel
            return [(r["name"], r["url"]) for r in csv.DictReader(fh)]
    from scrapers import cloak_watsons as watsons
    context, page = await watsons._create_browser_context(block_images=True)
    async with context:
        await page.goto("https://www.watsons.co.th/th/b/brandlist", wait_until="domcontentloaded", timeout=60_000)
        await watsons._accept_cookie(page)
        await asyncio.sleep(4)
        pairs = await page.eval_on_selector_all(
            'a[href*="/all-brands/"]', "els => els.map(e => [e.textContent.trim(), e.getAttribute('href')])")
    seen, entries = set(), []
    for name, href in pairs:
        url = href if href.startswith("http") else "https://www.watsons.co.th" + href
        if name and url not in seen:
            seen.add(url)
            entries.append((name, url))
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    raw = RAW_DIR / f"watsons_brandlist_{datetime.now(TZ_THAI):%Y%m%d}.csv"
    with open(raw, "w", encoding="utf-8-sig", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["name", "url"])
        w.writerows(entries)
    print(f"   💾 {len(entries)} Watsons brands -> {raw}")
    return entries


async def lookup_watsons(todo, found, args):
    entries = await watsons_brandlist(args.watsons_list)
    if len(entries) < 100:
        print(f"⚠️  Only {len(entries)} brands read from the Watsons brand list — page changed? Not saving.")
        return
    rows = []
    for b in todo:
        ours = [k for k in b["keys"] if k]
        first = key(b["brand"].split()[0])
        scored = []
        for name, url in entries:
            nk = key(name)
            if not nk:
                continue
            if nk in b["keys"]:
                s, conf = 3.0, "high"
            elif (len(first) >= 4 and nk == first) or any(
                    len(o) >= 5 and len(nk) >= 5 and (nk.startswith(o) or o.startswith(nk)) for o in ours):
                s, conf = 2.0, "medium"  # e.g. 'Faris' for FARIS BY NARIS
            else:
                s = max(SequenceMatcher(None, o, nk).ratio() for o in ours)
                if s < 0.75:
                    continue  # substring hits like KSA -> 'Phruksa spa' are noise
                conf = "low"
            scored.append((s, conf, name, url))
        scored.sort(reverse=True)
        if not scored:
            rows.append(row(b, "watsons", rank=0, confidence="not found",
                            note=f"not in the Watsons brand list ({len(entries)} brands)"))
            continue
        for i, (s, conf, name, url) in enumerate(scored[:3], start=1):
            rows.append(row(b, "watsons", rank=i, confidence=conf, accept="y" if conf == "high" else "",
                            candidate_url=url, name_on_site=name))
    found.add(rows)
    print(f"   ✅ Watsons: {len(todo)} brands checked")


# ── Shopee ───────────────────────────────────────────────────────────────────

async def lookup_shopee(todo, found, args):
    from scrapers import cloak_shopee_AI_bot as shopee
    for part in chunks(todo, CHUNK):
        kws = list(dict.fromkeys(b["keyword"] for b in part))
        df = await shopee.run_search_keyword(kws, max_pages=1)
        df = df if isinstance(df, pd.DataFrame) else pd.DataFrame()
        rows = []
        for b in part:
            sub = df[df["search_keyword"] == b["keyword"]] if "search_keyword" in df.columns else df.iloc[0:0]
            items = [{
                "shop_id": str(r.get("shopid", "")), "shop_name": str(r.get("shop_name", "") or ""),
                "official": bool(r.get("is_official_shop")), "brand_field": str(r.get("brand", "") or ""),
                "title": str(r.get("name") or r.get("product_name") or ""),
                "sold": r.get("sold_total") or r.get("historical_sold"),
                "url": str(r.get("product_url", "") or ""),
            } for r in sub.to_dict("records")]
            rows += shop_rows(b, "shopee", items, lambda s: f"https://shopee.co.th/shop/{s['shop_id']}", 60)
        found.add([{k: v for k, v in r.items() if not k.startswith("_")} for r in rows])
        print(f"   ✅ Shopee: saved {len(part)} brands")


# ── Lazada ───────────────────────────────────────────────────────────────────

LAZADA_SHOP_RE = re.compile(r"lazada\.co\.th(?:\\?/)+shop(?:\\?/)+([A-Za-z0-9_.\-]+)")


async def lazada_shop_slug(page, product_url: str) -> str:
    url = product_url if product_url.startswith("http") else "https:" + product_url
    await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
    await asyncio.sleep(random.uniform(3, 5))
    for _ in range(3):
        m = LAZADA_SHOP_RE.search(await page.content())
        if m:
            return m.group(1).rstrip(".")
        await page.mouse.wheel(0, 1500)
        await asyncio.sleep(2)
    return ""


async def lookup_lazada(todo, found, args):
    from scrapers import cloak_lazada as lazada
    for part in chunks(todo, CHUNK):
        kws = list(dict.fromkeys(b["keyword"] for b in part))
        df = await lazada.run_search_keyword(kws, end_page_by_keyword={k: 1 for k in kws})
        df = df if isinstance(df, pd.DataFrame) else pd.DataFrame()
        rows = []
        for b in part:
            sub = df[df["search_keyword"] == b["keyword"]] if "search_keyword" in df.columns else df.iloc[0:0]
            items = [{
                "shop_id": str(r.get("shop_id", "")), "shop_name": str(r.get("shop_name", "") or ""),
                "official": bool(r.get("is_mall")), "brand_field": str(r.get("brand_name", "") or ""),
                "title": str(r.get("product_name", "") or ""), "sold": r.get("sold_count"),
                "url": str(r.get("product_url", "") or ""),
            } for r in sub.to_dict("records")]
            rows += shop_rows(b, "lazada", items, lambda s: "", 40)

        # The search API has no shop link: open one product per shop worth
        # keeping (rank 1, or LazMall) and read /shop/<name> from the page.
        need = [r for r in rows if r.get("_product_url") and (r["rank"] == 1 or r["official"] != "no")]
        slugs = {}
        if need:
            context, page = await lazada.create_browser_context(block_images=True)
            async with context:
                for r in need:
                    sid = r["_shop_id"]
                    if sid not in slugs:
                        try:
                            slugs[sid] = await lazada_shop_slug(page, r["_product_url"])
                        except Exception as e:
                            print(f"   ⚠️  {r['brand']}: couldn't open product for shop link ({e})")
                            slugs[sid] = ""
                        await lazada.human_delay()
        for r in rows:
            slug = slugs.get(r.get("_shop_id", ""), "")
            if slug:
                r["candidate_url"] = f"https://www.lazada.co.th/shop/{slug}"
            elif r.get("_product_url"):
                r["accept"] = ""
                r["note"] = (r["note"] + "; " if r["note"] else "") + f"shop link not read — open {r['_product_url']}"
        found.add([{k: v for k, v in r.items() if not k.startswith("_")} for r in rows])
        print(f"   ✅ Lazada: saved {len(part)} brands")


# ── Konvy ────────────────────────────────────────────────────────────────────

async def lookup_konvy(todo, found, args):
    from scrapers import cloak_konvy as konvy
    context, page = await konvy._create_browser_context(block_images=True)
    cookie_done = False
    async with context:
        for part in chunks(todo, CHUNK):
            rows = []
            for b in part:
                kw = b["keyword"].replace("'", "").replace("’", "")  # Konvy search breaks on apostrophes
                url = f"https://www.konvy.com/list/?title={quote(kw)}"
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
                    if not cookie_done:
                        await konvy._accept_cookie(page)
                        cookie_done = True
                    await asyncio.sleep(2)
                    await konvy._scroll_until_stable(page)
                    hrefs = await page.eval_on_selector_all("a[href]", "els => els.map(e => e.getAttribute('href'))")
                except Exception as e:
                    rows.append(row(b, "konvy", rank=0, confidence="error", note=str(e)[:200]))
                    continue
                urls = set()
                for h in hrefs:
                    if h and re.search(r"-\d+\.html", h):
                        urls.add(h if h.startswith("http") else ("https:" + h if h.startswith("//") else "https://www.konvy.com" + h))
                slugs = Counter(urlsplit(u).path.strip("/").split("/")[0] for u in urls)
                scored = sorted(((max(name_score(n, s) for n in [b["brand"], *b["aliases"]]), c, s)
                                 for s, c in slugs.items()), reverse=True)
                scored = [x for x in scored if x[0] > 0]
                if not scored:
                    rows.append(row(b, "konvy", rank=0, confidence="not found", candidate_url=url,
                                    items_from_this_shop=len(urls),
                                    note=f"{len(urls)} products on page 1, none with this brand's slug"))
                else:
                    for i, (s, c, slug) in enumerate(scored[:3], start=1):
                        conf = "high" if s >= 100 and c >= 2 else ("medium" if s >= 100 else "low")
                        rows.append(row(b, "konvy", rank=i, confidence=conf, accept="y" if conf == "high" else "",
                                        candidate_url=url, name_on_site=slug, brand_items=c,
                                        items_from_this_shop=len(urls),
                                        note="Konvy is scraped by keyword: this search URL + filter on the slug"))
                await konvy._human_delay()
            found.add(rows)
            print(f"   ✅ Konvy: saved {len(part)} brands")


LOOKUPS = {"watsons": lookup_watsons, "shopee": lookup_shopee, "lazada": lookup_lazada, "konvy": lookup_konvy}


async def main(args):
    master_path, found_path = Path(args.master), Path(args.out)
    if not master_path.exists():
        sys.exit(f"❌ {master_path} not found — run scripts/build_brand_master.py first")
    master = pd.read_excel(master_path, sheet_name="brands", dtype=str).fillna("")
    found = FoundFile(found_path)
    only = {b.strip().upper() for b in args.brands.split(",") if b.strip()} if args.brands else set()
    platforms = [p.strip() for p in args.platforms.split(",") if p.strip()]
    bad = [p for p in platforms if p not in LOOKUPS]
    if bad:
        sys.exit(f"❌ Unknown platform(s): {bad} — choose from {list(LOOKUPS)}")

    for p in platforms:
        todo = [b for b in brands_to_look_up(master, p, args.missing_only, only) if (b["brand"], p) not in found.done]
        if args.limit:
            todo = todo[:args.limit]
        print(f"\n{'=' * 60}\n🔎 {p}: {len(todo)} brand(s) to look up\n{'=' * 60}")
        if todo:
            await LOOKUPS[p](todo, found, args)

    df = found.df
    if not df.empty:
        print(f"\n✅ {found_path}: {len(df)} rows")
        print(df.groupby(["platform", "confidence"]).size().unstack(fill_value=0).to_string())


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--platforms", default="watsons,shopee,lazada,konvy",
                    help="comma-separated: " + ",".join(LOOKUP_PLATFORMS))
    ap.add_argument("--brands", default="", help="only these brands (as in brand_master), comma-separated")
    ap.add_argument("--limit", type=int, default=0, help="at most N brands per platform (pilot)")
    ap.add_argument("--missing-only", action="store_true",
                    help="skip brands that already have a company-shop candidate")
    ap.add_argument("--master", default=MASTER_DEFAULT)
    ap.add_argument("--out", default=FOUND_DEFAULT)
    ap.add_argument("--watsons-list", default="", help="CSV (name,url) of the Watsons brand list instead of fetching it")
    a = ap.parse_args()
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main(a))
