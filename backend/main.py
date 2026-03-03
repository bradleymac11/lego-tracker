"""
LEGO Price Tracker — Australia
10 retailers, 100% free — no paid proxy or scraping service required.

Big W, Target, Kmart, Catch are all Shopify stores.
JB Hi-Fi, EB Games, MyHobbies, Toys R Us are Shopify stores.
Myer uses their internal REST API.
Amazon is best-effort direct HTML.

Environment variables (Render dashboard → Environment):
  ALERT_EMAIL_FROM   Gmail address for sending alerts (optional)
  ALERT_EMAIL_PASS   Gmail App Password (optional)
  ALERT_EMAIL_TO     Recipient email (optional)
  ALERT_MIN_DISCOUNT Minimum % discount to alert on (default: 50)
  WATCHED_SETS       Comma-separated set numbers e.g. 75375,42151
  NTFY_TOPIC         ntfy.sh topic for push notifications (optional)
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import httpx
from bs4 import BeautifulSoup
import json
import re
import logging
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone, date, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="LEGO AU Price Tracker")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ─── Config ───────────────────────────────────────────────────────────────────
EMAIL_FROM         = os.getenv("ALERT_EMAIL_FROM", "")
EMAIL_PASS         = os.getenv("ALERT_EMAIL_PASS", "")
EMAIL_TO           = os.getenv("ALERT_EMAIL_TO", "")
ALERT_MIN_DISCOUNT = float(os.getenv("ALERT_MIN_DISCOUNT", "50"))
WATCHED_SETS       = {s.strip() for s in os.getenv("WATCHED_SETS", "").split(",") if s.strip()}
NTFY_TOPIC         = os.getenv("NTFY_TOPIC", "")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
}

# ─── In-memory state ──────────────────────────────────────────────────────────
CACHE: dict = {"products": [], "last_updated": None, "scrape_status": {}}
PRICE_HISTORY: dict[str, list[dict]] = {}
DAILY_DROPS: list[dict] = []
ALERTED: set[str] = set()


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def parse_price(text) -> float | None:
    if not text:
        return None
    text = str(text).replace(",", "").replace("$", "").strip()
    m = re.search(r"\d+\.?\d*", text)
    if not m:
        return None
    try:
        v = float(m.group())
        return v if 0.5 < v < 100000 else None
    except ValueError:
        return None

def pct_off(rrp, price) -> float:
    try:
        if rrp and price and float(rrp) > float(price) > 0:
            return round((float(rrp) - float(price)) / float(rrp) * 100, 1)
    except Exception:
        pass
    return 0.0

def lego_img(set_number: str) -> str:
    if set_number:
        clean = re.sub(r"[^0-9]", "", str(set_number))
        if len(clean) >= 4:
            return f"https://cdn.rebrickable.com/media/sets/{clean}-1.jpg"
    return ""

def make_product(name, price, rrp, retailer, url,
                 set_number="", image_url="", in_stock=True) -> dict:
    return {
        "name":         name.strip(),
        "set_number":   str(set_number).strip(),
        "price":        round(float(price), 2),
        "rrp":          round(float(rrp), 2) if rrp else None,
        "discount_pct": pct_off(rrp, price),
        "retailer":     retailer,
        "url":          url,
        "in_stock":     bool(in_stock),
        "image_url":    image_url or lego_img(str(set_number)),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  PRICE HISTORY & DAILY DROPS
# ══════════════════════════════════════════════════════════════════════════════

def product_key(p: dict) -> str:
    return f"{p['retailer']}::{p['name'][:60]}"

def record_prices(products: list[dict]):
    today = date.today().isoformat()
    for p in products:
        key = product_key(p)
        if key not in PRICE_HISTORY:
            PRICE_HISTORY[key] = []
        existing = next((e for e in PRICE_HISTORY[key] if e["date"] == today), None)
        if existing:
            existing["price"] = p["price"]
        else:
            PRICE_HISTORY[key].append({"date": today, "price": p["price"]})
        PRICE_HISTORY[key] = sorted(PRICE_HISTORY[key], key=lambda x: x["date"])[-30:]

def compute_daily_drops(products: list[dict]) -> list[dict]:
    today     = date.today().isoformat()
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    drops = []
    for p in products:
        history = PRICE_HISTORY.get(product_key(p), [])
        t = next((e for e in history if e["date"] == today),     None)
        y = next((e for e in history if e["date"] == yesterday), None)
        if t and y and t["price"] < y["price"]:
            drop_amt = round(y["price"] - t["price"], 2)
            drop_pct = round((y["price"] - t["price"]) / y["price"] * 100, 1)
            drops.append({**p, "prev_price": y["price"],
                          "drop_amount": drop_amt, "drop_pct": drop_pct})
    return sorted(drops, key=lambda x: -x["drop_pct"])


# ══════════════════════════════════════════════════════════════════════════════
#  ALERTS
# ══════════════════════════════════════════════════════════════════════════════

async def send_ntfy(title: str, body: str, url: str = ""):
    if not NTFY_TOPIC: return
    try:
        h = {"Title": title, "Priority": "high", "Tags": "lego,money,fire"}
        if url: h["Click"] = url
        async with httpx.AsyncClient() as c:
            await c.post(f"https://ntfy.sh/{NTFY_TOPIC}", content=body, headers=h, timeout=10)
    except Exception as e:
        logger.warning(f"ntfy: {e}")

def send_email(subject: str, body_html: str):
    if not (EMAIL_FROM and EMAIL_PASS and EMAIL_TO): return
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = EMAIL_FROM
        msg["To"]      = EMAIL_TO
        msg.attach(MIMEText(body_html, "html"))
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
            s.login(EMAIL_FROM, EMAIL_PASS)
            s.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())
    except Exception as e:
        logger.warning(f"email: {e}")

async def check_and_send_alerts(products: list[dict]):
    big_deals, watched_deals = [], []
    for p in products:
        ak = f"{p['retailer']}::{p['name'][:60]}::{p['price']}"
        if p["discount_pct"] >= ALERT_MIN_DISCOUNT and ak not in ALERTED:
            big_deals.append(p); ALERTED.add(ak)
        if (p.get("set_number") and p["set_number"] in WATCHED_SETS
                and p["discount_pct"] > 0 and ak not in ALERTED):
            watched_deals.append(p); ALERTED.add(ak)
    if big_deals:
        html = "<h2>🔥 BrickHunt AU — Big Deals!</h2><ul>" + "".join(
            f"<li><b>{p['name']}</b> @ {p['retailer']} — <b>${p['price']:.2f}</b> "
            f"({p['discount_pct']}% off)<br><a href='{p['url']}'>View →</a></li>"
            for p in big_deals[:10]) + "</ul>"
        send_email(f"🔥 {len(big_deals)} LEGO deal(s) at {ALERT_MIN_DISCOUNT:.0f}%+ off!", html)
        for p in big_deals[:3]:
            await send_ntfy(
                f"🔥 {p['discount_pct']}% OFF: {p['name'][:45]}",
                f"{p['retailer']} — ${p['price']:.2f}" + (f" (was ${p['rrp']:.2f})" if p["rrp"] else ""),
                p["url"])
    if watched_deals:
        html = "<h2>🎯 Watched Set on Sale!</h2><ul>" + "".join(
            f"<li><b>#{p['set_number']}</b> {p['name']} @ {p['retailer']} — "
            f"${p['price']:.2f} ({p['discount_pct']}% off)<br><a href='{p['url']}'>View →</a></li>"
            for p in watched_deals) + "</ul>"
        send_email("🎯 Watched LEGO set on sale!", html)
        for p in watched_deals:
            await send_ntfy(
                f"🎯 #{p['set_number']} on sale!",
                f"{p['name'][:45]} — ${p['price']:.2f} ({p['discount_pct']}% off)",
                p["url"])


# ══════════════════════════════════════════════════════════════════════════════
#  SHOPIFY SCRAPERS
#  All Shopify /products.json endpoints are public — no auth, no proxy needed.
# ══════════════════════════════════════════════════════════════════════════════

def _parse_shopify_item(item: dict, retailer: str, base_url: str) -> dict | None:
    """Parse a single Shopify product item into our standard format."""
    name = item.get("title", "")
    if not name or "lego" not in name.lower():
        return None
    variants = item.get("variants", [{}])
    variant  = variants[0] if variants else {}
    price    = parse_price(str(variant.get("price", "")))
    rrp      = parse_price(str(variant.get("compare_at_price") or ""))
    if not price:
        return None
    images = item.get("images", [])
    img    = images[0].get("src", "") if images else ""
    handle = item.get("handle", "")
    return make_product(
        name, price, rrp, retailer,
        f"{base_url}/products/{handle}",
        str(variant.get("sku", item.get("id", ""))),
        img,
        variant.get("available", True)
    )


async def scrape_shopify_collection(client: httpx.AsyncClient,
                                     base_url: str,
                                     collection: str,
                                     retailer: str) -> list[dict]:
    """
    Fetch /collections/{collection}/products.json — paginated.
    Works for top-level collection slugs like 'lego'.
    """
    products = []
    page = 1
    while True:
        url = f"{base_url}/collections/{collection}/products.json?limit=250&page={page}"
        try:
            r = await client.get(url, headers=HEADERS, timeout=30)
            if r.status_code != 200:
                logger.warning(f"{retailer} collection page {page}: HTTP {r.status_code}")
                break
            items = r.json().get("products", [])
            if not items:
                break
            for item in items:
                p = _parse_shopify_item(item, retailer, base_url)
                if p:
                    products.append(p)
            if len(items) < 250:
                break
            page += 1
        except Exception as e:
            logger.warning(f"{retailer} collection page {page} error: {e}")
            break
    logger.info(f"{retailer}: {len(products)} products")
    return products


async def scrape_shopify_all(client: httpx.AsyncClient,
                              base_url: str,
                              retailer: str) -> list[dict]:
    """
    Fetch /products.json (all products) and filter for 'lego' in title.
    Used when the store doesn't have a top-level /collections/lego path.
    """
    products = []
    page = 1
    while page <= 20:  # safety cap
        url = f"{base_url}/products.json?limit=250&page={page}"
        try:
            r = await client.get(url, headers=HEADERS, timeout=30)
            if r.status_code != 200:
                logger.warning(f"{retailer} all-products page {page}: HTTP {r.status_code}")
                break
            items = r.json().get("products", [])
            if not items:
                break
            for item in items:
                p = _parse_shopify_item(item, retailer, base_url)
                if p:
                    products.append(p)
            if len(items) < 250:
                break
            page += 1
        except Exception as e:
            logger.warning(f"{retailer} all-products page {page} error: {e}")
            break
    logger.info(f"{retailer}: {len(products)} products")
    return products


# ══════════════════════════════════════════════════════════════════════════════
#  INDIVIDUAL SCRAPERS
# ══════════════════════════════════════════════════════════════════════════════

async def scrape_bigw(client: httpx.AsyncClient) -> list[dict]:
    return await scrape_shopify_collection(client, "https://www.bigw.com.au", "lego", "Big W")

async def scrape_target(client: httpx.AsyncClient) -> list[dict]:
    return await scrape_shopify_collection(client, "https://www.target.com.au", "lego", "Target")

async def scrape_kmart(client: httpx.AsyncClient) -> list[dict]:
    return await scrape_shopify_collection(client, "https://www.kmart.com.au", "lego", "Kmart")

async def scrape_catch(client: httpx.AsyncClient) -> list[dict]:
    return await scrape_shopify_collection(client, "https://www.catch.com.au", "lego", "Catch")

async def scrape_myhobbies(client: httpx.AsyncClient) -> list[dict]:
    return await scrape_shopify_collection(client, "https://www.myhobbies.com.au", "lego", "MyHobbies")

async def scrape_toysrus(client: httpx.AsyncClient) -> list[dict]:
    return await scrape_shopify_collection(client, "https://www.toysrus.com.au", "lego", "Toys R Us")

async def scrape_jbhifi(client: httpx.AsyncClient) -> list[dict]:
    # JB Hi-Fi nested collection path doesn't work with products.json
    # so we scan all products instead
    return await scrape_shopify_all(client, "https://www.jbhifi.com.au", "JB Hi-Fi")

async def scrape_eb_games(client: httpx.AsyncClient) -> list[dict]:
    # Try collection path first, fall back to all-products scan
    products = await scrape_shopify_collection(client, "https://www.ebgames.com.au", "lego", "EB Games")
    if not products:
        products = await scrape_shopify_all(client, "https://www.ebgames.com.au", "EB Games")
    return products

async def scrape_myer(client: httpx.AsyncClient) -> list[dict]:
    """Myer internal REST API — returns JSON directly, no Cloudflare protection."""
    products = []
    try:
        url = "https://www.myer.com.au/api/2.0/page/search?query=lego&pageSize=96&page=1"
        r = await client.get(url, headers={**HEADERS, "Accept": "application/json",
                                           "Referer": "https://www.myer.com.au/"}, timeout=30)
        logger.info(f"Myer API: {r.status_code}")
        if r.status_code == 200:
            data  = r.json()
            items = data.get("products", data.get("results", []))
            logger.info(f"Myer raw items: {len(items)}")
            for item in items:
                name = item.get("name", item.get("displayName", item.get("title", "")))
                if not name or "lego" not in name.lower(): continue
                price = parse_price(item.get("price", item.get("sellingPrice", item.get("salePrice", ""))))
                rrp   = parse_price(item.get("wasPrice", item.get("rrp", item.get("regularPrice", ""))))
                if not price: continue
                set_num = str(item.get("sku", item.get("productId", item.get("id", ""))))
                img  = item.get("imageUrl", item.get("primaryImage", item.get("image", "")))
                slug = item.get("url", item.get("pdpUrl", ""))
                products.append(make_product(
                    name, price, rrp, "Myer",
                    f"https://www.myer.com.au{slug}" if slug.startswith("/") else "https://www.myer.com.au/search?query=lego",
                    set_num, img
                ))
    except Exception as e:
        logger.warning(f"Myer error: {e}")
    logger.info(f"Myer: {len(products)} products")
    return products

async def scrape_amazon_au(client: httpx.AsyncClient) -> list[dict]:
    """Amazon direct HTML — best effort, no proxy. May be blocked by Amazon."""
    products = []
    try:
        all_cards = []
        for pg in range(1, 4):
            url = f"https://www.amazon.com.au/s?k=lego&i=toys&s=review-rank&page={pg}"
            r   = await client.get(url, headers={**HEADERS, "Accept": "text/html"}, timeout=30)
            if r.status_code != 200:
                logger.info(f"Amazon AU page {pg}: HTTP {r.status_code}")
                break
            soup  = BeautifulSoup(r.text, "html.parser")
            cards = soup.select("[data-component-type='s-search-result']")
            all_cards.extend(cards)
            if len(cards) < 10:
                break
        logger.info(f"Amazon AU: {len(all_cards)} cards found")
        for card in all_cards:
            title_el = (
                card.select_one("h2 a span.a-text-normal")
                or card.select_one("[data-cy='title-recipe'] span")
                or card.select_one("h2 span")
            )
            if not title_el: continue
            name = title_el.get_text(strip=True)
            if len(name) < 6 or "lego" not in name.lower(): continue
            offscreen = card.select_one(".a-price .a-offscreen")
            pw = card.select_one(".a-price-whole")
            pf = card.select_one(".a-price-fraction")
            if offscreen:
                price = parse_price(offscreen.get_text())
            elif pw:
                ps = pw.get_text(strip=True).replace(",", "").rstrip(".")
                if pf: ps += f".{pf.get_text(strip=True)}"
                price = parse_price(ps)
            else:
                price = None
            if not price: continue
            was_el = card.select_one(".a-text-price .a-offscreen")
            rrp    = parse_price(was_el.get_text()) if was_el else None
            asin   = card.get("data-asin", "")
            link   = card.select_one("h2 a[href]")
            img_el = card.select_one("img.s-image")
            href   = link["href"] if link else f"/dp/{asin}"
            products.append(make_product(
                name, price, rrp, "Amazon AU",
                f"https://www.amazon.com.au{href}" if href.startswith("/") else href,
                asin, img_el.get("src", "") if img_el else ""
            ))
    except Exception as e:
        logger.warning(f"Amazon AU error: {e}")
    logger.info(f"Amazon AU: {len(products)} products")
    return products


# ══════════════════════════════════════════════════════════════════════════════
#  MASTER ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════════

async def run_all_scrapers():
    logger.info("=== Starting scrape run ===")
    async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client:
        results = await asyncio.gather(
            scrape_bigw(client),
            scrape_target(client),
            scrape_kmart(client),
            scrape_catch(client),
            scrape_myer(client),
            scrape_amazon_au(client),
            scrape_jbhifi(client),
            scrape_eb_games(client),
            scrape_myhobbies(client),
            scrape_toysrus(client),
            return_exceptions=True,
        )

    retailer_names = [
        "Big W", "Target", "Kmart", "Catch", "Myer",
        "Amazon AU", "JB Hi-Fi", "EB Games", "MyHobbies", "Toys R Us",
    ]

    all_products = []
    for name, result in zip(retailer_names, results):
        if isinstance(result, Exception):
            logger.error(f"{name} exception: {result}")
            CACHE["scrape_status"][name] = f"error: {str(result)[:80]}"
        else:
            all_products.extend(result)
            CACHE["scrape_status"][name] = f"{len(result)} items"
            logger.info(f"  {name}: {len(result)} items")

    # Deduplicate within each retailer
    seen, deduped = set(), []
    for p in sorted(all_products, key=lambda x: -x["discount_pct"]):
        key = (p["retailer"], p["name"].lower()[:50])
        if key not in seen:
            seen.add(key)
            deduped.append(p)

    CACHE["products"]     = deduped
    CACHE["last_updated"] = datetime.now(timezone.utc).isoformat()
    record_prices(deduped)

    global DAILY_DROPS
    DAILY_DROPS = compute_daily_drops(deduped)
    logger.info(f"=== Done: {len(deduped)} products, {len(DAILY_DROPS)} daily drops ===")
    await check_and_send_alerts(deduped)


# ─── Scheduler ────────────────────────────────────────────────────────────────

scheduler = AsyncIOScheduler()

@app.on_event("startup")
async def startup():
    asyncio.create_task(run_all_scrapers())
    scheduler.add_job(run_all_scrapers, "interval", hours=1)
    scheduler.start()

@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown()


# ══════════════════════════════════════════════════════════════════════════════
#  API ROUTES
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/")
def root():
    return {"status": "ok", "message": "LEGO AU Price Tracker", "retailers": 10}

@app.get("/api/prices")
def get_prices(on_sale_only: bool = False, min_discount: float = 0, retailer: str = ""):
    products = CACHE["products"]
    if on_sale_only: products = [p for p in products if p["discount_pct"] > 0]
    if min_discount:  products = [p for p in products if p["discount_pct"] >= min_discount]
    if retailer:      products = [p for p in products if p["retailer"].lower() == retailer.lower()]
    return {
        "products":      products,
        "total":         len(products),
        "last_updated":  CACHE["last_updated"],
        "scrape_status": CACHE["scrape_status"],
    }

@app.get("/api/drops")
def get_daily_drops(retailer: str = ""):
    drops = DAILY_DROPS
    if retailer: drops = [d for d in drops if d["retailer"].lower() == retailer.lower()]
    return {"drops": drops, "total": len(drops),
            "last_updated": CACHE["last_updated"], "date": date.today().isoformat()}

@app.get("/api/compare/{set_number}")
def compare_prices(set_number: str):
    clean = re.sub(r"[^0-9a-zA-Z]", "", set_number).lower()
    matches = [p for p in CACHE["products"]
               if re.sub(r"[^0-9a-zA-Z]", "", str(p.get("set_number", ""))).lower() == clean]
    if not matches:
        tokens = [t for t in set_number.lower().split() if len(t) > 3]
        if tokens:
            matches = [p for p in CACHE["products"]
                       if sum(1 for t in tokens if t in p["name"].lower()) >= min(2, len(tokens))]
    result = sorted([dict(m) for m in matches], key=lambda x: x["price"])
    for i, m in enumerate(result):
        m["price_rank"]    = i + 1
        m["is_cheapest"]   = (i == 0)
        m["price_history"] = PRICE_HISTORY.get(product_key(m), [])
    img = next((m.get("image_url") or lego_img(m.get("set_number", ""))
                for m in result if m.get("image_url") or m.get("set_number")), "")
    return {
        "set_number":    set_number,
        "retailers":     result,
        "lowest_price":  result[0]["price"] if result else None,
        "highest_price": result[-1]["price"] if result else None,
        "price_spread":  round(result[-1]["price"] - result[0]["price"], 2) if len(result) > 1 else 0,
        "image_url":     img,
    }

@app.post("/api/refresh")
async def trigger_refresh():
    asyncio.create_task(run_all_scrapers())
    return {"message": "Scrape triggered — check /api/debug in ~60 seconds"}

@app.get("/api/retailers")
def get_retailers():
    return {"retailers": sorted({p["retailer"] for p in CACHE["products"]})}

@app.get("/api/alerts/config")
def alert_config():
    return {
        "email_configured":   bool(EMAIL_FROM and EMAIL_PASS and EMAIL_TO),
        "push_configured":    bool(NTFY_TOPIC),
        "ntfy_topic":         NTFY_TOPIC,
        "min_discount_alert": ALERT_MIN_DISCOUNT,
        "watched_sets":       list(WATCHED_SETS),
    }

@app.post("/api/alerts/test")
async def test_alerts():
    await send_ntfy("🧱 BrickHunt Test", "Alerts are working!")
    send_email("🧱 BrickHunt Test", "<h2>Email alerts working!</h2>")
    return {"message": "Test notifications sent"}

@app.get("/api/debug")
def debug():
    by_retailer = {}
    for p in CACHE["products"]:
        if p["retailer"] not in by_retailer:
            by_retailer[p["retailer"]] = {"name": p["name"], "price": p["price"]}
    return {
        "scrape_status":       CACHE["scrape_status"],
        "total_products":      len(CACHE["products"]),
        "sample_per_retailer": by_retailer,
        "last_updated":        CACHE["last_updated"],
    }

@app.get("/api/search")
def search_products(q: str, limit: int = 50):
    q_lower = q.lower()
    results = [p for p in CACHE["products"] if q_lower in p["name"].lower()]
    return {"results": results[:limit], "total": len(results)}
