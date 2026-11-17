"""
build_brand_master.py — Phase 1: one row per brand from the company sheet.

Reads "BRAND NAME (update 03-2026)" and the five link columns of
company_url_filled_v6.xlsx (one row per customer, several brands and shop
links per cell) and writes brand_master.xlsx:

  brands  — one row per unique brand: customers, search keywords (EN/TH),
            links per platform, how each link was tied to the brand, flags
  links   — one row per (brand, platform, url) with the match method and
            the source rows it came from
  issues  — problems found in the source sheet, by Excel row, to fix there

How a link is tied to a brand (column *_how / links.match):
  name in URL        the shop/brand slug contains the brand name
  only brand in row  the customer row lists just this brand
  same position      FastMoss has no name in its URL; matched to the brand
                     of the Shopee link in the same position (a guess)
  company shop       the row lists several brands and the URL names none of
                     them (or names a sibling brand) — a candidate only:
                     check the shop actually sells this brand
  found (phase 2)    looked up by scripts/find_brand_links.py and accepted
                     (accept = y) in brand_links_found.xlsx

Re-running keeps what you typed in brand_master.xlsx (keyword_en,
keyword_th, keyword_th_source, note) for brands still in the source.

USAGE
python scripts/build_brand_master.py
python scripts/build_brand_master.py --src company_url_filled_v6.xlsx --out brand_master.xlsx
"""

import argparse
import os
import re
import sys
import tempfile
from collections import OrderedDict, defaultdict
from pathlib import Path
from urllib.parse import urlsplit

import openpyxl
import pandas as pd
from openpyxl.styles import Alignment, Font, PatternFill

SRC_DEFAULT = "company_url_filled_v6.xlsx"
OUT_DEFAULT = "brand_master.xlsx"

COL_ORG = "Sales Organization"
COL_CODE = "CUSTOMER ALLO."
COL_CUSTOMER = "CUSTOMER NAME"
COL_BRANDS = "BRAND NAME (update 03-2026)"
LINK_COLS = OrderedDict([
    ("shopee", "SHOPEE LINK"),
    ("lazada", "LAZADA LINK"),
    ("watsons", "WATSONS LINK"),
    ("konvy", "KONVY LINK"),
    ("fastmoss", "FASTMOSS LINK"),
])
PLATFORMS = list(LINK_COLS)
HOSTS = {
    "shopee.co.th": "shopee",
    "lazada.co.th": "lazada",
    "watsons.co.th": "watsons",
    "konvy.com": "konvy",
    "fastmoss.com": "fastmoss",
}

# Typos in the source cell -> corrected name (each one is listed in `issues`).
NAME_FIXES = {"PRIMROS E": "PRIMROSE"}
# Other spellings of the same brand -> the name used in brand_master.
ALIASES = {"MONDAMIN": "MONDAHMIN"}

# Thai search keywords. Only names already used for searching or written in
# the source file count as checked; "claude_draft" ones are transliterations
# to verify on the brand's own shop page before relying on them. Brands not
# listed here get an empty keyword_th (flagged) — fill them from the shop.
TH_KEYWORDS = {
    # already searched with scripts/scrape_brand_search.py
    "WATSONS": ("วัตสัน", "scrape_brand_search.py"),
    "BOOTS": ("บู๊ทส์", "scrape_brand_search.py"),
    "JABS": ("แจ๊บส์", "scrape_brand_search.py"),
    "SRICHAND": ("ศรีจันทร์", "scrape_brand_search.py"),
    "SASI": ("ศศิ", "scrape_brand_search.py"),
    "CHARMISS": ("ชาร์มิส", "scrape_brand_search.py"),
    "4U2": ("โฟร์ยูทู", "scrape_brand_search.py"),
    "LOVE POTION": ("เลิฟโพชั่น", "scrape_brand_search.py"),
    # written in the source file (customer name)
    "RANGMAI PHUTAWAN": ("รังไหมภูตะวัน", "source_customer_name"),
    # drafts — verify
    "MISTINE": ("มิสทิน", "claude_draft"),
    "FARIS BY NARIS": ("ฟาริส บาย นาริส", "claude_draft"),
    "MIZUMI": ("มิซึมิ", "claude_draft"),
    "CLEAR NOSE": ("เคลียร์โนส", "claude_draft"),
    "MONDAHMIN": ("มอนดามิน", "claude_draft"),
    "PALMOLIVE": ("ปาล์มโอลีฟ", "claude_draft"),
    "PROTEX": ("โพรเทคส์", "claude_draft"),
    "ROJUKISS": ("โรจูคิส", "claude_draft"),
    "BIG C": ("บิ๊กซี", "claude_draft"),
    "MUJI": ("มูจิ", "claude_draft"),
    "ENFANT": ("อองฟองต์", "claude_draft"),
    "KODOMO": ("โคโดโม", "claude_draft"),
    "TAOYEABLOK": ("เต่าเหยียบโลก", "claude_draft"),
    "BIONIC": ("ไบโอนิค", "claude_draft"),
    "YOKO": ("โยโกะ", "claude_draft"),
    "DONKI": ("ดองกิ", "claude_draft"),
    "LOLANE": ("โลแลน", "claude_draft"),
    "BEAUTY BUFFET": ("บิวตี้บุฟเฟ่ต์", "claude_draft"),
    "SCENTIO": ("เซนทิโอ", "claude_draft"),
    "DAISO": ("ไดโซะ", "claude_draft"),
    "ISME": ("อิสมี", "claude_draft"),
    "LESASHA": ("เลอซาช่า", "claude_draft"),
    "MOSHI MOSHI": ("โมชิ โมชิ", "claude_draft"),
    "SMOOTO": ("สมูทโตะ", "claude_draft"),
    "MINISO": ("มินิโซ", "claude_draft"),
    "SMOOTH E": ("สมูทอี", "claude_draft"),
}

# Names that are ordinary words / common names: a keyword search on them
# pulls in other brands' products, so prefer the shop link and filter.
COMMON_WORD_NAMES = {
    "HATCH", "MEDINA", "NOW HOW", "LAURENT", "BOND", "KINDNESS", "ENFANT",
    "WELCOME WINTER", "PROTEA", "SCRUPLES", "CAMELLA", "THE BAKERY",
    "THE INGREDIENTS", "SUNNY DAY", "ROYAL BEAUTY", "HYGIENIST", "FRESHLIGHT",
    "BIONIC", "PRIMROSE", "FACE SO", "SUN BEARS", "TOP COUNTRY", "BERNIE",
    "ADALINE", "MADELYN", "YOKO", "AURA RICH", "MOSHI MOSHI", "HAIRSHINE",
    "GOODAGE", "SOSU", "ATIA", "SASI",
}
# Retailers whose official shop also sells other brands.
RETAILERS = {"WATSONS", "BOOTS", "BIG C", "DONKI"}

HOW_RANK = {"name in URL": 0, "found (phase 2)": 1, "only brand in row": 2, "same position": 3, "company shop": 4}
FOUND_DEFAULT = "brand_links_found.xlsx"
THAI = re.compile(r"[฀-๿]")


def key(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(s).lower())


def clean(v) -> str:
    return "" if v is None else str(v).strip()


# ── brand cell ────────────────────────────────────────────────────────────────

def parse_brand_cell(cell: str):
    """'MIZUMI, GENTLE COLORS' -> [(raw, NAME, thai)], with a Thai name in
    trailing parentheses split off: 'ON ANONG (อร อนงค์)' -> ('ON ANONG', 'อร อนงค์')."""
    out = []
    for part in str(cell).split(","):
        raw = part.strip()
        if not raw:
            continue
        thai = ""
        m = re.match(r"^(.*?)\s*\(([^)]*)\)\s*$", raw)
        if m and THAI.search(m.group(2)):
            raw_name, thai = m.group(1).strip(), m.group(2).strip()
        else:
            raw_name = raw
        name = re.sub(r"\s+", " ", raw_name.replace("’", "'")).upper()
        out.append((raw, name, thai))
    return out


# ── links ─────────────────────────────────────────────────────────────────────

def split_links(cell):
    return [t for t in re.split(r"[,\s]+", clean(cell)) if t]


def parse_link(token: str):
    """-> (platform, clean_url, slug, problem). slug is the shop / brand name
    part of the URL (None for FastMoss, which only has numeric ids)."""
    url = token if re.match(r"https?://", token, re.I) else "https://" + token
    parts = urlsplit(url)
    netloc = parts.netloc.lower()
    platform = HOSTS.get(netloc.removeprefix("www."))
    path = parts.path
    clean_url = f"https://{netloc}{path}".rstrip("/")  # '/shop/lion' == '/shop/lion/'
    slug, problem = None, ""
    segs = [s for s in path.split("/") if s]
    if platform == "shopee":
        slug = segs[0] if segs else ""
    elif platform == "lazada":
        # cloak_lazada.extract_shop_key_from_url takes /shop/<name> and bare /<name>
        m = re.search(r"/shop/([^/]+)", path)
        slug = m.group(1) if m else (segs[0] if segs else "")
    elif platform == "watsons":
        m = re.search(r"/all-brands/(?:b|list)/\d+/([^/]+)", path)
        if m:
            slug = m.group(1)
        else:
            problem = "not a Watsons brand page (expected /all-brands/b/<id>/<name>)"
    elif platform == "konvy":
        m = re.search(r"/brand/([^/]+)", path)
        if m:
            slug = m.group(1)
        else:
            problem = "not a Konvy brand page (expected /brand/<name>/)"
    elif platform == "fastmoss":
        if "/influencer/" in path:
            problem = "FastMoss influencer page, not a shop page"
        elif "/shop-marketing/detail/" not in path:
            problem = "not a FastMoss shop page"
    return platform, clean_url, slug, problem


def name_score(brand: str, slug: str) -> int:
    """How strongly a URL slug names a brand (0 = not at all). Longer
    matches win, so 'arome-by-watsons' goes to AROME BY WATSONS, not WATSONS."""
    sk, bk = key(slug or ""), key(brand)
    if not sk or not bk:
        return 0
    if (len(bk) >= 3 and bk in sk) or bk == sk:
        return 100 + len(bk)
    first = key(brand.split()[0])
    if len(first) >= 4 and first in sk:
        return len(first)
    pre = os.path.commonprefix([bk, sk])
    return len(pre) if len(pre) >= 5 else 0


def best_brand(brands, slug):
    scored = sorted(((name_score(b, slug), b) for b in brands), reverse=True)
    if not scored or scored[0][0] == 0:
        return None
    if len(scored) > 1 and scored[1][0] == scored[0][0]:
        return None  # tie -> can't tell which brand
    return scored[0][1]


# ── main build ────────────────────────────────────────────────────────────────

def read_rows(src: Path):
    wb = openpyxl.load_workbook(src, read_only=True, data_only=True)
    ws = wb.active
    it = ws.iter_rows(values_only=True)
    header = [clean(h) for h in next(it)]
    idx = {h: i for i, h in enumerate(header) if h}
    missing = [c for c in [COL_ORG, COL_CODE, COL_CUSTOMER, COL_BRANDS, *LINK_COLS.values()] if c not in idx]
    if missing:
        sys.exit(f"❌ Columns not found in {src}: {missing}")
    rows = []
    for excel_row, r in enumerate(it, start=2):
        get = lambda c: r[idx[c]] if idx[c] < len(r) else None
        if not clean(get(COL_BRANDS)):
            continue
        rows.append({
            "row": excel_row,
            "org": clean(get(COL_ORG)),
            "code": clean(get(COL_CODE)),
            "customer": clean(get(COL_CUSTOMER)),
            "brands_cell": clean(get(COL_BRANDS)),
            "links": {p: split_links(get(c)) for p, c in LINK_COLS.items()},
        })
    wb.close()
    return rows


def read_found(path: Path):
    """Accepted rows of brand_links_found.xlsx (scripts/find_brand_links.py)."""
    if not path or not path.exists():
        return []
    df = pd.read_excel(path, sheet_name="found", dtype=str).fillna("")
    ok = df[df["accept"].str.strip().str.lower().isin(["y", "yes"]) & (df["candidate_url"].str.strip() != "")]
    return ok[["brand", "platform", "candidate_url", "th_hint"]].to_dict("records")


def build(rows, found_rows=()):
    brands = OrderedDict()   # NAME -> info
    links = OrderedDict()    # (brand, platform, url) -> info
    issues = []

    def issue(row, column, value, problem, fix=""):
        issues.append({"source_row": row["row"] if row else "", "customer_code": row["code"] if row else "",
                       "customer": row["customer"] if row else "", "column": column, "value": value,
                       "problem": problem, "suggested_fix": fix})

    def add_link(brand, platform, url, how, row, slug, problem):
        k = (brand, platform, url)
        cur = links.get(k)
        if cur is None:
            links[k] = {"brand": brand, "platform": platform, "url": url, "match": how,
                        "url_names_brand": "" if slug is None else ("yes" if name_score(brand, slug) else "no"),
                        "source_rows": [row["row"]], "problem": problem}
        else:
            if HOW_RANK[how] < HOW_RANK[cur["match"]]:
                cur["match"] = how
            if row["row"] not in cur["source_rows"]:
                cur["source_rows"].append(row["row"])

    for row in rows:
        # 1) brands in this row
        row_brands = []
        for raw, name, thai in parse_brand_cell(row["brands_cell"]):
            fixed = NAME_FIXES.get(name, name)
            if fixed != name:
                issue(row, COL_BRANDS, raw, f"typo: '{raw}'", f"rename to '{fixed}'")
            canon = ALIASES.get(fixed, fixed)
            b = brands.setdefault(canon, {
                "aliases": [], "codes": [], "customers": [], "orgs": [], "rows": [], "thai_from_file": "",
            })
            if canon != fixed and fixed not in b["aliases"]:
                b["aliases"].append(fixed)
                issue(row, COL_BRANDS, raw, f"'{fixed}' and '{canon}' are the same brand spelled two ways",
                      f"keep one spelling ('{canon}' used here)")
            for lst, val in ((b["codes"], row["code"]), (b["customers"], row["customer"]),
                             (b["orgs"], row["org"]), (b["rows"], row["row"])):
                if val and val not in lst:
                    lst.append(val)
            if thai and not b["thai_from_file"]:
                b["thai_from_file"] = thai
            if canon not in row_brands:
                row_brands.append(canon)

        # 2) links in this row, each put under the platform its host says
        by_platform = defaultdict(list)  # platform -> [(url, slug, problem)]
        for col_platform, tokens in row["links"].items():
            for tok in tokens:
                platform, url, slug, problem = parse_link(tok)
                if platform is None:
                    issue(row, LINK_COLS[col_platform], tok, "unknown site in a link column")
                    continue
                if platform != col_platform:
                    issue(row, LINK_COLS[col_platform], tok,
                          f"{platform} link in the {col_platform} column", f"move it to {LINK_COLS[platform]}")
                if problem:
                    issue(row, LINK_COLS[platform], tok, problem)
                if url not in [u for u, _, _ in by_platform[platform]]:
                    by_platform[platform].append((url, slug, problem))

        # 3) tie each link to a brand
        single = len(row_brands) == 1
        shopee_owner = []  # brand matched by name for each Shopee link, in order
        for url, slug, _ in by_platform.get("shopee", []):
            shopee_owner.append(best_brand(row_brands, slug) if not single else row_brands[0])

        for platform in PLATFORMS:
            items = by_platform.get(platform, [])
            if not items:
                continue
            if single:
                for url, slug, problem in items:
                    add_link(row_brands[0], platform, url, "only brand in row", row, slug, problem)
                continue
            owned = set()
            leftovers = []
            for pos, (url, slug, problem) in enumerate(items):
                owner = None
                how = ""
                if platform == "fastmoss":
                    if (len(items) == len(shopee_owner) and all(shopee_owner)
                            and len(set(shopee_owner)) == len(shopee_owner)):
                        owner, how = shopee_owner[pos], "same position"
                else:
                    owner = best_brand(row_brands, slug)
                    how = "name in URL"
                if owner:
                    add_link(owner, platform, url, how, row, slug, problem)
                    owned.add(owner)
                else:
                    leftovers.append((url, slug, problem))
            # brands of this row with no link of their own on this platform get
            # the row's shops as company-shop candidates. A sibling's own shop
            # may carry this brand too; a sibling's brand page (Watsons /b/,
            # Konvy /brand/) never does, so only unnamed ones are passed on.
            shared = leftovers if platform in ("watsons", "konvy") else items
            for b in row_brands:
                if b in owned:
                    continue
                for url, slug, problem in shared:
                    add_link(b, platform, url, "company shop", row, slug, problem)

    # 3b) links accepted in Phase 2 (brand_links_found.xlsx, accept = y)
    for f in found_rows:
        b, p, u = f["brand"], f["platform"], f["candidate_url"].rstrip("/")
        if b not in brands or p not in PLATFORMS or not u:
            continue
        cur = links.get((b, p, u))
        if cur is None:
            links[(b, p, u)] = {"brand": b, "platform": p, "url": u, "match": "found (phase 2)",
                                "url_names_brand": "", "source_rows": [], "problem": ""}
        elif HOW_RANK["found (phase 2)"] < HOW_RANK[cur["match"]]:
            cur["match"] = "found (phase 2)"

    # 4) the same URL given for different brands in different rows
    by_url = defaultdict(list)
    for (b, p, u), info in links.items():
        if info["match"] not in ("company shop", "found (phase 2)"):
            by_url[(p, u)].append(info)
    for (p, u), infos in by_url.items():
        names = sorted({i["brand"] for i in infos})
        rows_ = sorted({r for i in infos for r in i["source_rows"]})
        if len(names) < 2 or len({tuple(i["source_rows"]) for i in infos}) < 2:
            continue
        named = sorted({i["brand"] for i in infos if i["url_names_brand"] == "yes"})
        if named:
            others = [n for n in names if n not in named]
            fix = f"URL names {', '.join(named)} — the link given for {', '.join(others)} is probably a copy-paste slip"
        else:
            fix = "one shop for several brands? check it sells all of them"
        issues.append({"source_row": ", ".join(map(str, rows_)), "customer_code": "", "customer": "",
                       "column": LINK_COLS[p], "value": u,
                       "problem": f"same URL given for different brands: {', '.join(names)}",
                       "suggested_fix": fix})

    # 5) brand names that may be one brand (longer name contains the shorter)
    names = list(brands)
    for a in names:
        for b in names:
            ka, kb = key(a), key(b)
            if a != b and len(ka) >= 6 and ka in kb and " BY " not in b:
                issues.append({"source_row": ", ".join(map(str, brands[a]["rows"] + brands[b]["rows"])),
                               "customer_code": "", "customer": "", "column": COL_BRANDS,
                               "value": f"{a} / {b}", "problem": "possibly the same brand",
                               "suggested_fix": "confirm; merge if same"})
                brands[a].setdefault("maybe_same", []).append(b)
                brands[b].setdefault("maybe_same", []).append(a)
    return brands, links, issues


def to_frames(brands, links, issues, previous, found_rows=()):
    th_hints = {}
    for f in found_rows:  # Thai word most used in the brand's titles on that site
        if f.get("th_hint") and f["brand"] not in th_hints:
            th_hints[f["brand"]] = (f["th_hint"], f"{f['platform']} titles (phase 2)")
    by_brand = defaultdict(lambda: defaultdict(list))
    for (b, p, u), info in links.items():
        by_brand[b][p].append(info)

    brand_rows = []
    for name, b in brands.items():
        th, th_src = "", ""
        if b["thai_from_file"]:
            th, th_src = b["thai_from_file"], "source_brand_cell"
        elif name in TH_KEYWORDS:
            th, th_src = TH_KEYWORDS[name]
        elif name in th_hints:
            th, th_src = th_hints[name]
        kw_en = name
        flags = []
        rec = OrderedDict([
            ("brand", name),
            ("aliases", ", ".join(b["aliases"])),
            ("customer_codes", ", ".join(b["codes"])),
            ("customers", " | ".join(b["customers"])),
            ("sales_orgs", ", ".join(b["orgs"])),
            ("source_rows", ", ".join(map(str, b["rows"]))),
            ("keyword_en", kw_en),
            ("keyword_th", th),
            ("keyword_th_source", th_src),
        ])
        own_platforms, shared_only, unnamed = [], [], []
        for p in PLATFORMS:
            infos = sorted(by_brand[name][p], key=lambda i: HOW_RANK[i["match"]])
            rec[p] = "\n".join(i["url"] for i in infos)
            rec[f"{p}_how"] = "\n".join(i["match"] for i in infos)
            if any(i["match"] != "company shop" for i in infos):
                own_platforms.append(p)
            elif infos:
                shared_only.append(p)
            if any(i["url_names_brand"] == "no" and i["match"] != "company shop" for i in infos):
                unnamed.append(p)
        rec["platforms_own_link"] = len(own_platforms)
        rec["platforms_missing"] = ", ".join(p for p in PLATFORMS if p not in own_platforms and p not in shared_only)

        if not own_platforms and not shared_only:
            flags.append("no link on any platform")
        if shared_only:
            flags.append("only a company-shop link on: " + ", ".join(shared_only))
        if unnamed:
            flags.append("shop URL doesn't name the brand: " + ", ".join(unnamed))
        if name in RETAILERS:
            flags.append("retailer — its shop sells other brands too; filter by brand")
        if "'" in name:
            flags.append("apostrophe — Konvy search breaks on it; search without it")
        if b.get("maybe_same"):
            flags.append("possibly same brand as " + ", ".join(b["maybe_same"]))
        if not th:
            flags.append("Thai keyword missing")
        k = key(name)
        if len(k) <= 3:
            rec["search_risk"] = "high — short name"
        elif name in COMMON_WORD_NAMES:
            rec["search_risk"] = "high — common word / name"
        else:
            rec["search_risk"] = ""
        rec["flags"] = "; ".join(flags)
        rec["note"] = ""

        # keep what the user typed in an earlier brand_master.xlsx
        old = previous.get(name)
        if old:
            for col in ("keyword_en", "keyword_th", "keyword_th_source", "note"):
                v = old.get(col)
                if v is not None and str(v).strip() and str(v).lower() != "nan":
                    rec[col] = str(v).strip()
            if rec["keyword_th"] and "Thai keyword missing" in rec["flags"]:
                rec["flags"] = "; ".join(f for f in flags if f != "Thai keyword missing")
        brand_rows.append(rec)

    link_rows = [{
        "brand": i["brand"], "platform": i["platform"], "url": i["url"], "match": i["match"],
        "url_names_brand": i["url_names_brand"], "source_rows": ", ".join(map(str, i["source_rows"])),
        "problem": i["problem"],
    } for i in links.values()]

    return pd.DataFrame(brand_rows), pd.DataFrame(link_rows), pd.DataFrame(issues)


def read_previous(out: Path) -> dict:
    if not out.exists():
        return {}
    try:
        df = pd.read_excel(out, sheet_name="brands", dtype=str)
    except Exception as e:
        print(f"⚠️  Couldn't read earlier {out} ({e}) — starting fresh.")
        return {}
    return {r["brand"]: r for r in df.to_dict("records") if r.get("brand")}


def write(out: Path, frames):
    fd, tmp = tempfile.mkstemp(suffix=".xlsx", dir=out.parent)
    os.close(fd)
    with pd.ExcelWriter(tmp, engine="openpyxl") as xw:
        for sheet, df in frames.items():
            df.to_excel(xw, sheet_name=sheet, index=False)
    wb = openpyxl.load_workbook(tmp)
    head_fill = PatternFill("solid", fgColor="DDEBF7")
    widths = {"brand": 24, "aliases": 14, "customer_codes": 16, "customers": 28, "sales_orgs": 14,
              "source_rows": 12, "keyword_en": 22, "keyword_th": 18, "keyword_th_source": 20,
              "platforms_own_link": 10, "platforms_missing": 22, "search_risk": 22, "flags": 60,
              "note": 30, "url": 60, "problem": 50, "suggested_fix": 50, "value": 50, "column": 26,
              "customer": 26, "match": 18}
    for ws in wb.worksheets:
        ws.freeze_panes = "B2"
        ws.auto_filter.ref = ws.dimensions
        for c in ws[1]:
            c.font = Font(bold=True)
            c.fill = head_fill
            name = str(c.value)
            w = widths.get(name, 45 if name in PLATFORMS else 16)
            ws.column_dimensions[c.column_letter].width = w
        for row in ws.iter_rows(min_row=2):
            for c in row:
                c.alignment = Alignment(wrap_text=True, vertical="top")
    wb.save(tmp)
    try:
        os.replace(tmp, out)
    except PermissionError:
        os.remove(tmp)
        sys.exit(f"❌ Can't write {out} — is it open in Excel? Close it and run again.")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--src", default=SRC_DEFAULT)
    ap.add_argument("--out", default=OUT_DEFAULT)
    ap.add_argument("--found", default=FOUND_DEFAULT,
                    help="Phase 2 results; rows with accept = y are added (skipped if the file doesn't exist)")
    args = ap.parse_args()
    src, out = Path(args.src), Path(args.out)
    if not src.exists():
        sys.exit(f"❌ Source not found: {src}")

    rows = read_rows(src)
    found_rows = read_found(Path(args.found))
    if found_rows:
        print(f"➕ {len(found_rows)} accepted link(s) from {args.found}")
    brands, links, issues = build(rows, found_rows)
    df_b, df_l, df_i = to_frames(brands, links, issues, read_previous(out), found_rows)
    write(out, {"brands": df_b, "links": df_l, "issues": df_i})

    n_none = (df_b["flags"].str.contains("no link on any platform")).sum()
    print(f"✅ {out}")
    print(f"   {len(rows)} customer rows -> {len(df_b)} brands, {len(df_l)} brand-links, {len(df_i)} issues")
    print(f"   brands with no link at all: {n_none}")
    for p in PLATFORMS:
        own = (df_b[f"{p}_how"].fillna("").str.contains("name in URL|found|only brand in row|same position")).sum()
        print(f"   {p:<9} own link: {own:>3}   company-shop only: "
              f"{(df_b['flags'].str.contains(f'only a company-shop link on:.*{p}')).sum():>3}")


if __name__ == "__main__":
    main()
