# Web Scraping — E-Commerce Scrapers

ชุด scraper สำหรับเก็บข้อมูลสินค้า / รายละเอียด / รีวิว จากแพลตฟอร์ม e-commerce
ไทย จีน และ UK โดยใช้ [CloakBrowser](https://pypi.org/project/cloakbrowser/)
มี `scrap_agent.py` เป็นตัวควบคุมหลัก (เมนูแบบ interactive)

| ภูมิภาค | แพลตฟอร์ม | โมดูล |
|---|---|---|
| ไทย | Shopee | `scrapers/cloak_shopee_AI_bot.py` |
| ไทย | Lazada | `scrapers/cloak_lazada.py` |
| ไทย | Watsons | `scrapers/cloak_watsons.py` |
| ไทย | Konvy | `scrapers/cloak_konvy.py` |
| จีน | Pinduoduo | `scrapers/cloak_pinduoduo.py` |
| จีน | Taobao | `scrapers/cloak_taobao.py` |
| UK | Superdrug | `scrapers/cloak_superdrug.py` |
| UK | Tesco | `scrapers/cloak_tesco.py` |
| UK | Amazon UK | `scrapers/cloak_amazon_uk.py` |
| ทุกประเทศ (ค่าเริ่มต้น TH) | TikTok Top Ads (Creative Center) | `scrapers/cloak_tiktok_topads.py` |
| ทุกประเทศ (ค่าเริ่มต้น TH) | TikTok Hashtag ยอดนิยม (Trends) | `scrapers/cloak_tiktok_hashtags.py` |

## โครงสร้างโปรเจกต์

```
Web_Scraping/
├── scrap_agent.py          # ตัวควบคุมหลัก — เลือกแพลตฟอร์ม + action จากเมนู
├── scrapers/               # scraper แยกตามแพลตฟอร์ม (cloak_<site>.py)
├── scripts/                # สคริปต์ batch สำหรับงานเฉพาะ
│   ├── scrape_brand_search.py     # ค้นหาตามรายชื่อแบรนด์ (Shopee/Lazada/Konvy)
│   ├── scrape_category_search.py  # ค้นหาตามหมวดสินค้า (Shopee/Lazada/Konvy)
│   ├── scrape_target_skus.py      # ดึง detail + รีวิวจากไฟล์ target SKUs
│   ├── scrape_lemon8_search.py    # ดึงโพสต์ Lemon8 + คอมเมนต์ตาม keyword
│   └── lemon8_scraper.py          # parser โพสต์ Lemon8 (ใช้เดี่ยว ๆ ได้)
├── notebooks/              # Jupyter notebook (Konvy)
├── data/                   # ข้อมูลตอนรัน — ไม่ขึ้น git (ดู .gitignore)
│   ├── <site>/browser_session | my_session   # โปรไฟล์เบราว์เซอร์ (cookies/login)
│   ├── <site>/collected_data/                # ผลลัพธ์ .xlsx (+ temp/ checkpoint)
│   ├── <site>/log/                           # log, HTML/screenshot ตอนเจอ captcha
│   └── combined_output/                      # ไฟล์รวมจาก scrap_agent.py
├── requirements.txt
└── .env.example
```

โฟลเดอร์ใน `data/` สร้างให้อัตโนมัติตอนรันครั้งแรก

## ติดตั้ง

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env      # แล้วใส่ OPEN_ROUTER_API
```

## วิธีใช้

**ต้องรันจากโฟลเดอร์ root ของโปรเจกต์เสมอ** (path ทั้งหมดอ้างอิงจาก
current directory เช่น `./data/shopee/...`)

```bash
python scrap_agent.py
```

เลือก Auto (A) เพื่ออ่าน URL จากไฟล์ memory JSON หรือ Manual (M) เพื่อพิมพ์ URL
เอง — รายละเอียด action ของแต่ละแพลตฟอร์มอยู่ใน docstring ด้านบนของ
`scrap_agent.py`

สคริปต์ batch:

```bash
python scripts/scrape_brand_search.py --platforms shopee,lazada
python scripts/scrape_category_search.py --categories shampoo,conditioner
python scripts/scrape_target_skus.py "path\to\dd_target_skus_REVISED.xlsx"
python scripts/scrape_lemon8_search.py --keyword "..." --urls-file urls.txt
```

### หมายเหตุ

- Pinduoduo / Taobao ต้อง login ก่อน (action 0) — session จะเก็บไว้ใน
  `data/<site>/my_session`
- Amazon UK ต้องรัน action 0 หนึ่งครั้งเพื่อตั้ง postcode UK
- TikTok Top Ads: Manual → `10` → action 0 (login บัญชี TikTok ครั้งแรก — ถ้าไม่ login
  เห็นแค่ 3 โฆษณา) แล้ว action 7 เลือกประเทศ / ช่วงเวลา (7, 30, 180 วัน) / การเรียงลำดับ /
  อุตสาหกรรม (เช่น `Beauty & Personal Care`) หรือรันตรง ๆ:
  `python -m scrapers.cloak_tiktok_topads --periods 30,180 --industries "Beauty & Personal Care"`
  — ลิงก์วิดีโอ / ปกในไฟล์ผลลัพธ์หมดอายุภายในไม่กี่ชั่วโมง
- TikTok Hashtag ยอดนิยม: Manual → `10` → action 8 (ใช้ login เดียวกับ Top Ads) — top 100
  hashtag ต่อประเทศ / ช่วงเวลา (7, 30, 90 วัน) / อุตสาหกรรม พร้อมจำนวนโพสต์ ยอดวิว
  กราฟความนิยมรายวัน และครีเอเตอร์ 3 อันดับแรก หรือรันตรง ๆ:
  `python -m scrapers.cloak_tiktok_hashtags --periods 30 --industries "Beauty & Personal Care"`
- TikTok ช่วงเวลา: พิมพ์ `ALL` ในเมนู (หรือ `--periods all`) = ทุกช่วงที่เว็บมี
  (Top Ads: 7, 30, 180 วัน · Hashtag: 7, 30, 90 วัน) รวมในไฟล์เดียว แยกด้วยคอลัมน์ `period_days`
- TikTok ทั้งสองแบบ: อย่าย่อหน้าต่างเบราว์เซอร์ระหว่างรัน (หน้าเว็บโหลดเพิ่มเฉพาะตอนที่แสดงบนจอ)
- Watsons โหมด **ทุกแบรนด์**: Manual → Watsons → action 2 → เลือก `2` (หรือใส่ `"ALL"`
  ใน `WATSONS_LINK` ของไฟล์ memory) — วนทุกแบรนด์ใน `/th/b/brandlist` (~755 แบรนด์)
  เก็บความคืบหน้าทุกแบรนด์ หยุดแล้วรันต่อได้ (เลือก resume) ตั้งจำนวนแบรนด์ต่อรอบได้
  และเลือกเก็บเฉพาะรายชื่อสินค้า+URL (เร็ว, ไว้ทำ action 3 ทีหลัง) หรือ detail เต็ม
  หน่วงระหว่างแบรนด์ 10–15 วินาทีตาม robots.txt (Crawl-delay 10; ขอให้เข้าช่วง 11:00–15:45 น.)
  ผลอยู่ที่ `data/watsons/collected_data/watsons_all_brands*_<เวลา>.xlsx` + ไฟล์สรุปรายแบรนด์
- `scrap_agent.py` ใช้ไฟล์ memory JSON ที่ `MEMORY_FILE` (แก้ path ได้ในไฟล์
  หรือพิมพ์ path ใหม่ตอนรัน)
