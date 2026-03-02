"""
LEGO Price Tracker - Australia v2
FastAPI backend with price history, daily drop tracking, push/email alerts, hourly scraping.
Deploy on Render.com (free tier).
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

app = FastAPI(title="LEGO AU Price Tracker v2")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ─── Config via Render Environment Variables ───────────────────────────────────
# Set these in Render dashboard → your service → Environment:
#
#   ALERT_EMAIL_FROM   your Gmail (e.g. you@gmail.com)
#   ALERT_EMAIL_PASS   Gmail App Password (google: "Gmail App Password" for instructions)
#   ALERT_EMAIL_TO     recipient email
#   ALERT_MIN_DISCOUNT minimum % to trigger alert (default 50)
#   WATCHED_SETS       comma-separated set numbers: 75375,42151,10307
#   NTFY_TOPIC         your secret ntfy.sh topic name (e.g. lego-deals-abc123)
#                      Subscribe at https://ntfy.sh/YOUR_TOPIC or via the free ntfy app

EMAIL_FROM = os.getenv("ALERT_EMAIL_FROM", "")
EMAIL_PASS = os.getenv("ALERT_EMAIL_PASS", "")
EMAIL_TO = os.getenv("ALERT_EMAIL_TO", "")
ALERT_MIN_DISCOUNT = float(os.getenv("ALERT_MIN_DISCOUNT", "50"))
WATCHED_SETS = {s.strip() for s in os.getenv("WATCHED_SETS", "").split(",") if s.strip()}
NTFY_TOPIC = os.getenv("NTFY_TOPIC", "")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-AU,en;q=0.9",
}

# ─── In-memory stores ─────────────────────────────────────────────────────────
CACHE: dict = {"products": [], "last_updated": None, "scrape_status": {}}
PRICE_HISTORY: dict[str, list[dict]] = {}  # key -> [{date, price}, ...]
DAILY_DROPS: list[dict] = []
ALERTED: set[str] = set()


# ─── Price history ─────────────────────────────────────────────────────────────

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
    today = date.today().isoformat()
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    drops = []
    for p in products:
        history = PRICE_HISTORY.get(product_key(p), [])
        t = next((e for e in history if e["date"] == today), None)
        y = next((e for e in history if e["date"] == yesterday), None)
        if t and y and t["price"] < y["price"]:
            drop_amt = round(y["price"] - t["price"], 2)
            drop_pct = round((y["price"] - t["price"]) / y["price"] * 100, 1)
            drops.append({**p, "prev_price": y["price"], "drop_amount": drop_amt, "drop_pct": drop_pct})
    return sorted(drops, key=lambda x: -x["drop_pct"])


# ─── Alerts ────────────────────────────────────────────────────────────────────

async def send_ntfy(title: str, body: str, url: str = ""):
    if not NTFY_TOPIC:
        return
    try:
        headers = {"Title": title, "Priority": "high", "Tags": "lego,money,fire"}
        if url:
            headers["Click"] = url
        async with httpx.AsyncClient() as c:
            await c.post(f"https://ntfy.sh/{NTFY_TOPIC}", content=body, headers=headers, timeout=10)
    except Exception as e:
        logger.warning(f"ntfy failed: {e}")

def send_email(subject: str, body_html: str):
    if not (EMAIL_FROM and EMAIL_PASS and EMAIL_TO):
        return
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = EMAIL_FROM
        msg["To"] = EMAIL_TO
        msg.attach(MIMEText(body_html, "html"))
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
            s.login(EMAIL_FROM, EMAIL_PASS)
            s.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())
    except Exception as e:
        logger.warning(f"Email failed: {e}")

async def check_and_send_alerts(products: list[dict]):
    big_deals, watched_deals = [], []
    for p in products:
        ak = f"{p['retailer']}::{p['name'][:60]}::{p['price']}"
        if p["discount_pct"] >= ALERT_MIN_DISCOUNT and ak not in ALERTED:
            big_deals.append(p)
            ALERTED.add(ak)
        if p.get("set_number") and p["set_number"] in WATCHED_SETS and p["discount_pct"] > 0 and ak not in ALERTED:
            watched_deals.append(p)
            ALERTED.add(ak)

    if big_deals:
        html = "<h2>🔥 BrickHunt AU — Big Deals!</h2><ul>" + "".join(
            f"<li><b>{p['name']}</b> @ {p['retailer']} — <b>${p['price']:.2f}</b> "
            f"({p['discount_pct']}% off RRP ${p['rrp']:.2f if p['rrp'] else 0:.2f})<br>"
            f"<a href='{p['url']}'>View →</a></li>"
            for p in big_deals[:10]
        ) + "</ul>"
        send_email(f"🔥 {len(big_deals)} LEGO set(s) at {ALERT_MIN_DISCOUNT:.0f}%+ off!", html)
        for p in big_deals[:3]:
            await send_ntfy(
                f"🔥 {p['discount_pct']}% OFF: {p['name'][:45]}",
                f"{p['retailer']} — ${p['price']:.2f}" + (f" (was ${p['rrp']:.2f})" if p["rrp"] else ""),
                p["url"],
            )

    if watched_deals:
        html = "<h2>🎯 Watched Set on Sale!</h2><ul>" + "".join(
            f"<li><b>#{p['set_number']}</b> {p['name']} @ {p['retailer']} — ${p['price']:.2f} ({p['discount_pct']}% off)<br>"
            f"<a href='{p['url']}'>View →</a></li>"
            for p in watched_deals
        ) + "</ul>"
        send_email("🎯 Watched LEGO set on sale!", html)
        for p in watched_deals:
            await send_ntfy(
                f"🎯 #{p['set_number']} on sale!",
                f"{p['name'][:45]} @ {p['retailer']} — ${p['price']:.2f} ({p['discount_pct']}% off)",
                p["url"],
            )


# ─── Scrapers ──────────────────────────────────────────────────────────────────

def parse_price(text: str) -> float | None:
    if not text:
        return None
    m = re.search(r"[\d,]+\.?\d*", text.replace(",", ""))
    return float(m.group()) if m else None

def pct_off(original: float, sale: float) -> float:
    if not original or original <= 0:
        return 0.0
    return round((original - sale) / original * 100, 1)

async def scrape_bigw(client):
    products = []
    try:
        r = await client.get("https://api.bigw.com.au/api/2.0/page/category/toys-games/lego?page=1&pageSize=48&sortby=TOP_SELLERS", headers=HEADERS, timeout=15)
        for item in r.json().get("products", r.json().get("results", [])):
            name = item.get("name", "")
            if "lego" not in name.lower(): continue
            rrp = parse_price(str(item.get("wasPrice", "") or item.get("rrp", "")))
            price = parse_price(str(item.get("price", "") or item.get("nowPrice", "")))
            if not price: continue
            products.append({"name": name, "set_number": item.get("productCode", item.get("sku", "")), "price": price, "rrp": rrp, "discount_pct": pct_off(rrp, price) if rrp and rrp > price else 0.0, "retailer": "Big W", "url": f"https://www.bigw.com.au{item.get('url', '')}", "in_stock": item.get("inStock", True)})
    except Exception as e:
        logger.warning(f"Big W: {e}")
    return products

async def scrape_target(client):
    products = []
    try:
        r = await client.get("https://www.target.com.au/api/2.0/page/search?q=lego&pageSize=48&page=1", headers=HEADERS, timeout=15)
        for item in (r.json().get("products") or r.json().get("results") or []):
            name = item.get("title", item.get("name", ""))
            if "lego" not in name.lower(): continue
            price = parse_price(str(item.get("price", "") or item.get("salePrice", "")))
            rrp = parse_price(str(item.get("wasPrice", "") or item.get("regularPrice", "")))
            if not price: continue
            products.append({"name": name, "set_number": item.get("productId", ""), "price": price, "rrp": rrp, "discount_pct": pct_off(rrp, price) if rrp and rrp > price else 0.0, "retailer": "Target", "url": f"https://www.target.com.au{item.get('url', '')}", "in_stock": item.get("availability", "IN_STOCK") != "OUT_OF_STOCK"})
    except Exception as e:
        logger.warning(f"Target: {e}")
    return products

async def scrape_kmart(client):
    products = []
    try:
        r = await client.get("https://www.kmart.com.au/api/search?q=lego&pageSize=48", headers=HEADERS, timeout=15)
        for item in (r.json().get("results") or r.json().get("products") or []):
            name = item.get("name", "")
            if "lego" not in name.lower(): continue
            price = parse_price(str(item.get("price", "")))
            rrp = parse_price(str(item.get("wasPrice", "") or item.get("compareAtPrice", "")))
            if not price: continue
            products.append({"name": name, "set_number": item.get("articleId", ""), "price": price, "rrp": rrp, "discount_pct": pct_off(rrp, price) if rrp and rrp > price else 0.0, "retailer": "Kmart", "url": f"https://www.kmart.com.au{item.get('url', '')}", "in_stock": True})
    except Exception as e:
        logger.warning(f"Kmart: {e}")
    return products

async def scrape_amazon_au(client):
    products = []
    try:
        r = await client.get("https://www.amazon.com.au/s?k=lego&i=toys", headers={**HEADERS, "Accept": "text/html"}, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        for card in soup.select("[data-component-type='s-search-result']")[:20]:
            title_el = card.select_one("h2 span")
            if not title_el or "lego" not in title_el.text.lower(): continue
            pw = card.select_one(".a-price-whole")
            pf = card.select_one(".a-price-fraction")
            was_el = card.select_one(".a-text-price span")
            if not pw: continue
            ps = pw.text.replace(",", "").strip(".")
            if pf: ps += f".{pf.text}"
            price = float(ps) if ps else None
            if not price: continue
            rrp = parse_price(was_el.text) if was_el else None
            link_el = card.select_one("a.a-link-normal")
            products.append({"name": title_el.text.strip(), "set_number": card.get("data-asin", ""), "price": price, "rrp": rrp, "discount_pct": pct_off(rrp, price) if rrp and rrp > price else 0.0, "retailer": "Amazon AU", "url": f"https://www.amazon.com.au{link_el['href']}" if link_el else "", "in_stock": True})
    except Exception as e:
        logger.warning(f"Amazon AU: {e}")
    return products

async def scrape_catch(client):
    products = []
    try:
        r = await client.get("https://catch.com.au/search/?q=lego", headers=HEADERS, timeout=15)
        script = BeautifulSoup(r.text, "html.parser").find("script", {"id": "__NEXT_DATA__"})
        if script:
            items = json.loads(script.string).get("props", {}).get("pageProps", {}).get("initialData", {}).get("results", [])
            for item in items[:30]:
                name = item.get("name", "")
                if "lego" not in name.lower(): continue
                pd = item.get("price", {})
                current = parse_price(str(pd.get("current", "")))
                was = parse_price(str(pd.get("was", "")))
                if not current: continue
                products.append({"name": name, "set_number": str(item.get("id", "")), "price": current, "rrp": was, "discount_pct": pct_off(was, current) if was and was > current else 0.0, "retailer": "Catch", "url": f"https://www.catch.com.au{item.get('url', '')}", "in_stock": item.get("inStock", True)})
    except Exception as e:
        logger.warning(f"Catch: {e}")
    return products

async def scrape_jbhifi(client):
    products = []
    try:
        r = await client.get("https://www.jbhifi.com.au/pages/search-results-page?q=lego", headers=HEADERS, timeout=15)
        script = BeautifulSoup(r.text, "html.parser").find("script", {"id": "__NEXT_DATA__"})
        if script:
            items = json.loads(script.string).get("props", {}).get("pageProps", {}).get("searchResults", {}).get("products", [])
            for item in items[:30]:
                name = item.get("title", "")
                if "lego" not in name.lower(): continue
                price = parse_price(str(item.get("price", "")))
                rrp = parse_price(str(item.get("compareAtPrice", "")))
                if not price: continue
                products.append({"name": name, "set_number": item.get("sku", ""), "price": price, "rrp": rrp, "discount_pct": pct_off(rrp, price) if rrp and rrp > price else 0.0, "retailer": "JB Hi-Fi", "url": f"https://www.jbhifi.com.au{item.get('url', '')}", "in_stock": item.get("available", True)})
    except Exception as e:
        logger.warning(f"JB Hi-Fi: {e}")
    return products

async def scrape_myer(client):
    products = []
    try:
        r = await client.get("https://www.myer.com.au/search?query=lego", headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        for card in soup.select(".ProductTile")[:20]:
            name_el = card.select_one(".ProductTile-title")
            if not name_el or "lego" not in name_el.text.lower(): continue
            price_el = card.select_one(".Price-value")
            was_el = card.select_one(".Price-was")
            price = parse_price(price_el.text) if price_el else None
            rrp = parse_price(was_el.text) if was_el else None
            if not price: continue
            link_el = card.select_one("a")
            products.append({"name": name_el.text.strip(), "set_number": "", "price": price, "rrp": rrp, "discount_pct": pct_off(rrp, price) if rrp and rrp > price else 0.0, "retailer": "Myer", "url": f"https://www.myer.com.au{link_el['href']}" if link_el else "", "in_stock": True})
    except Exception as e:
        logger.warning(f"Myer: {e}")
    return products

async def scrape_eb_games(client):
    products = []
    try:
        r = await client.get("https://www.ebgames.com.au/search?q=lego", headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        for card in soup.select(".product-item")[:20]:
            name_el = card.select_one(".product-item__title")
            if not name_el or "lego" not in name_el.text.lower(): continue
            price = parse_price(card.select_one(".price__sale .price-item--sale").text if card.select_one(".price__sale .price-item--sale") else "")
            rrp = parse_price(card.select_one(".price__compare .price-item--regular").text if card.select_one(".price__compare .price-item--regular") else "")
            if not price: continue
            link_el = card.select_one("a.product-item__link")
            products.append({"name": name_el.text.strip(), "set_number": "", "price": price, "rrp": rrp, "discount_pct": pct_off(rrp, price) if rrp and rrp > price else 0.0, "retailer": "EB Games", "url": f"https://www.ebgames.com.au{link_el['href']}" if link_el else "", "in_stock": "sold-out" not in card.get("class", [])})
    except Exception as e:
        logger.warning(f"EB Games: {e}")
    return products


# ─── Master orchestrator ───────────────────────────────────────────────────────

async def run_all_scrapers():
    logger.info("Starting scrape run...")
    async with httpx.AsyncClient(follow_redirects=True, timeout=20) as client:
        results = await asyncio.gather(
            scrape_bigw(client), scrape_target(client), scrape_kmart(client),
            scrape_amazon_au(client), scrape_catch(client), scrape_jbhifi(client),
            scrape_myer(client), scrape_eb_games(client), return_exceptions=True,
        )
    names = ["Big W", "Target", "Kmart", "Amazon AU", "Catch", "JB Hi-Fi", "Myer", "EB Games"]
    all_products = []
    for name, result in zip(names, results):
        if isinstance(result, Exception):
            CACHE["scrape_status"][name] = "error"
        else:
            all_products.extend(result)
            CACHE["scrape_status"][name] = f"{len(result)} items"

    seen, deduped = set(), []
    for p in sorted(all_products, key=lambda x: -x["discount_pct"]):
        key = (p["retailer"], p["name"].lower()[:40])
        if key not in seen:
            seen.add(key)
            deduped.append(p)

    CACHE["products"] = deduped
    CACHE["last_updated"] = datetime.now(timezone.utc).isoformat()

    record_prices(deduped)
    global DAILY_DROPS
    DAILY_DROPS = compute_daily_drops(deduped)
    logger.info(f"Done. {len(deduped)} products, {len(DAILY_DROPS)} daily drops.")
    await check_and_send_alerts(deduped)


# ─── Scheduler ─────────────────────────────────────────────────────────────────

scheduler = AsyncIOScheduler()

@app.on_event("startup")
async def startup():
    asyncio.create_task(run_all_scrapers())
    scheduler.add_job(run_all_scrapers, "interval", hours=1)  # ← HOURLY
    scheduler.start()

@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown()


# ─── Routes ────────────────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {"message": "LEGO AU Price Tracker v2"}

@app.get("/api/prices")
def get_prices(on_sale_only: bool = False, min_discount: float = 0, retailer: str = ""):
    products = CACHE["products"]
    if on_sale_only: products = [p for p in products if p["discount_pct"] > 0]
    if min_discount: products = [p for p in products if p["discount_pct"] >= min_discount]
    if retailer: products = [p for p in products if p["retailer"].lower() == retailer.lower()]
    return {"products": products, "total": len(products), "last_updated": CACHE["last_updated"], "scrape_status": CACHE["scrape_status"]}

@app.get("/api/drops")
def get_daily_drops(retailer: str = ""):
    drops = DAILY_DROPS
    if retailer: drops = [d for d in drops if d["retailer"].lower() == retailer.lower()]
    return {"drops": drops, "total": len(drops), "last_updated": CACHE["last_updated"], "date": date.today().isoformat()}

@app.post("/api/refresh")
async def trigger_refresh():
    asyncio.create_task(run_all_scrapers())
    return {"message": "Scrape triggered"}

@app.get("/api/retailers")
def get_retailers():
    return {"retailers": sorted({p["retailer"] for p in CACHE["products"]})}

@app.get("/api/alerts/config")
def alert_config():
    return {"email_configured": bool(EMAIL_FROM and EMAIL_PASS and EMAIL_TO), "push_configured": bool(NTFY_TOPIC), "ntfy_topic": NTFY_TOPIC, "min_discount_alert": ALERT_MIN_DISCOUNT, "watched_sets": list(WATCHED_SETS)}

@app.post("/api/alerts/test")
async def test_alerts():
    await send_ntfy("🧱 BrickHunt Test", "Alerts are working!")
    send_email("🧱 BrickHunt Test", "<h2>Email alerts working!</h2>")
    return {"message": "Test notifications sent"}


# ─── Image URL helper ─────────────────────────────────────────────────────────

def get_set_image_url(set_number: str) -> str:
    if not set_number:
        return ""
    clean = set_number.strip().rstrip("-1")
    return f"https://cdn.rebrickable.com/media/sets/{clean}-1.jpg"


# ─── Compare endpoint ─────────────────────────────────────────────────────────

@app.get("/api/compare/{set_number}")
def compare_prices(set_number: str):
    matches = [p for p in CACHE["products"] if p.get("set_number", "").strip() == set_number.strip()]
    if len(matches) <= 1 and matches:
        base_name = matches[0]["name"].lower()[:35]
        name_matches = [p for p in CACHE["products"] if base_name in p["name"].lower() and p not in matches]
        matches = matches + name_matches
    result = []
    for m in matches:
        entry = dict(m)
        entry["price_history"] = PRICE_HISTORY.get(product_key(m), [])
        result.append(entry)
    return {
        "set_number": set_number,
        "retailers": sorted(result, key=lambda x: x["price"]),
        "lowest_price": min((m["price"] for m in result), default=None),
        "image_url": get_set_image_url(set_number),
    }


@app.get("/api/search")
def search_products(q: str, limit: int = 30):
    q_lower = q.lower()
    results = [p for p in CACHE["products"] if q_lower in p["name"].lower()]
    return {"results": results[:limit], "total": len(results)}
