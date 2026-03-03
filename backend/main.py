"""
LEGO Price Tracker — Australia
10 retailers: Big W, Target, Kmart, Amazon AU, Catch, JB Hi-Fi, Myer, EB Games,
              MyHobbies, Toys R Us AU

Cloudflare-protected sites (Big W, Target, Kmart, Amazon, Catch, Myer) are
fetched via ScraperAPI which rotates residential IPs and handles bot detection.

Shopify stores (JB Hi-Fi, EB Games, MyHobbies, Toys R Us) use /products.json
which is always public and never blocked — no proxy needed.

Environment variables (set in Render dashboard → Environment):
  SCRAPER_API_KEY    Required for big retailers. Get free key at scraperapi.com
  ALERT_EMAIL_FROM   Gmail address for email alerts (optional)
  ALERT_EMAIL_PASS   Gmail App Password (optional)
  ALERT_EMAIL_TO     Recipient email (optional)
  ALERT_MIN_DISCOUNT Minimum % discount to alert on (default: 50)
  WATCHED_SETS       Comma-separated set numbers to watch e.g. 75375,42151
  NTFY_TOPIC         ntfy.sh topic name for push notifications (optional)
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
from urllib.parse import quote_plus

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="LEGO AU Price Tracker")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ─── Config ───────────────────────────────────────────────────────────────────
SCRAPER_API_KEY    = os.getenv("SCRAPER_API_KEY", "")
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

def scraper_api_url(target_url: str, render_js: bool = False) -> str:
    """Route a URL through ScraperAPI to bypass Cloudflare. Falls back to direct if no key."""
    if not SCRAPER_API_KEY:
        return target_url
    params = f"api_key={SCRAPER_API_KEY}&url={quote_plus(target_url)}&country_code=au"
    if render_js:
        params += "&render=true"  # JS rendering costs 5 credits vs 1 for plain HTML
    return f"https://api.scraperapi.com?{params}"

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

def extract_next_data(html: str) -> dict:
    """Extract JSON from Next.js __NEXT_DATA__ script tag."""
    try:
        soup   = BeautifulSoup(html, "html.parser")
        script = soup.find("script", {"id": "__NEXT_DATA__"})
        if script:
            return json.loads(script.string)
    except Exception:
        pass
    return {}


# ══════════════════════════════════════════════════════════════════════════════
#  PRICE HISTORY
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
#  SHOPIFY HELPER
#  JB Hi-Fi, EB Games, MyHobbies, Toys R Us all run Shopify.
#  Their /collections/lego/products.json endpoint is always public — no proxy.
# ══════════════════════════════════════════════════════════════════════════════


async def scrape_shopify_proxied(client: httpx.AsyncClient,
                                  base_url: str,
                                  collection: str,
                                  retailer_name: str) -> list[dict]:
    """
    Shopify scraper for stores that block direct access.
    Routes through ScraperAPI using plain HTTP mode (no JS render needed for /products.json).
    """
    products = []
    page = 1
    while True:
        url = f"{base_url}/collections/{collection}/products.json?limit=250&page={page}"
        try:
            proxied = scraper_api_url(url)  # plain HTTP, no JS render needed
            r = await client.get(proxied, headers=HEADERS, timeout=60)
            if r.status_code != 200:
                logger.warning(f"{retailer_name} proxied page {page}: HTTP {r.status_code}")
                break
            try:
                data = r.json()
            except Exception:
                # ScraperAPI might return HTML if something went wrong
                logger.warning(f"{retailer_name} proxied page {page}: non-JSON response")
                break
            items = data.get("products", [])
            if not items:
                break
            for item in items:
                name = item.get("title", "")
                if not name or "lego" not in name.lower():
                    continue
                variants = item.get("variants", [{}])
                variant  = variants[0] if variants else {}
                price    = parse_price(str(variant.get("price", "")))
                rrp      = parse_price(str(variant.get("compare_at_price") or ""))
                if not price:
                    continue
                images = item.get("images", [])
                img    = images[0].get("src", "") if images else ""
                handle = item.get("handle", "")
                products.append(make_product(
                    name, price, rrp, retailer_name,
                    f"{base_url}/products/{handle}",
                    str(variant.get("sku", item.get("id", ""))),
                    img,
                    variant.get("available", True)
                ))
            if len(items) < 250:
                break
            page += 1
        except Exception as e:
            logger.warning(f"{retailer_name} proxied page {page} error: {e}")
            break
    logger.info(f"{retailer_name}: {len(products)} products")
    return products

async def scrape_shopify(client: httpx.AsyncClient,
                         base_url: str,
                         collection: str,
                         retailer_name: str) -> list[dict]:
    """
    Generic Shopify scraper. Paginates through all pages until no more products.
    base_url  e.g. https://www.jbhifi.com.au
    collection e.g. lego  (used in /collections/{collection}/products.json)
    """
    products = []
    page = 1
    while True:
        url = f"{base_url}/collections/{collection}/products.json?limit=250&page={page}"
        try:
            r = await client.get(url, headers=HEADERS, timeout=30)
            if r.status_code != 200:
                logger.warning(f"{retailer_name} Shopify page {page}: HTTP {r.status_code}")
                break
            items = r.json().get("products", [])
            if not items:
                break
            for item in items:
                name = item.get("title", "")
                if not name or "lego" not in name.lower():
                    continue
                variants = item.get("variants", [{}])
                variant  = variants[0] if variants else {}
                price    = parse_price(str(variant.get("price", "")))
                rrp      = parse_price(str(variant.get("compare_at_price") or ""))
                if not price:
                    continue
                images = item.get("images", [])
                img    = images[0].get("src", "") if images else ""
                handle = item.get("handle", "")
                products.append(make_product(
                    name, price, rrp, retailer_name,
                    f"{base_url}/products/{handle}",
                    str(variant.get("sku", item.get("id", ""))),
                    img,
                    variant.get("available", True)
                ))
            if len(items) < 250:
                break   # last page
            page += 1
        except Exception as e:
            logger.warning(f"{retailer_name} Shopify page {page} error: {e}")
            break

    logger.info(f"{retailer_name}: {len(products)} products")
    return products


# ══════════════════════════════════════════════════════════════════════════════
#  SCRAPERAPI SCRAPERS  (Big W, Target, Kmart, Amazon, Catch, Myer)
#  All use scraper_api_url() to route through residential IPs.
#  If SCRAPER_API_KEY is not set these will likely get blocked — set it!
# ══════════════════════════════════════════════════════════════════════════════

async def scrape_bigw(client: httpx.AsyncClient) -> list[dict]:
    products = []
    try:
        # Big W has a direct search API — returns JSON cleanly without JS rendering
        api_url = "https://api.bigw.com.au/api/2.0/page/search?q=lego&pageSize=96&page=0&sortby=TOP_SELLERS"
        r = await client.get(api_url, headers={**HEADERS, "Accept": "application/json"}, timeout=30)
        if r.status_code == 200:
            try:
                data = r.json()
                items = data.get("products", data.get("results", data.get("data", {}).get("products", [])))
                logger.info(f"Big W API items: {len(items)}")
                for item in items:
                    name = item.get("name", item.get("title", ""))
                    if not name or "lego" not in name.lower(): continue
                    price = parse_price(item.get("nowPrice") or item.get("price") or item.get("salePrice"))
                    rrp   = parse_price(item.get("wasPrice") or item.get("rrp") or item.get("regularPrice"))
                    if not price: continue
                    set_num = str(item.get("productId", item.get("sku", item.get("articleId", ""))))
                    img = item.get("imageUrl", item.get("image", item.get("thumbnail", "")))
                    slug = item.get("url", item.get("pdpUrl", ""))
                    products.append(make_product(
                        name, price, rrp, "Big W",
                        f"https://www.bigw.com.au{slug}" if slug.startswith("/") else "https://www.bigw.com.au/search?q=lego",
                        set_num, img, item.get("inStock", True)
                    ))
                logger.info(f"Big W API: {len(products)} products")
                if products:
                    return products
            except Exception as e:
                logger.warning(f"Big W API parse error: {e}")

        # Fallback: ScraperAPI plain HTML + __NEXT_DATA__
        target = "https://www.bigw.com.au/search?q=lego&inStoreOnly=false&sz=96"
        r = await client.get(scraper_api_url(target), headers=HEADERS, timeout=60)
        data  = extract_next_data(r.text)
        pprops = data.get("props", {}).get("pageProps", {})

        items = (
            pprops.get("searchResult", {}).get("products", [])
            or pprops.get("initialState", {}).get("search", {}).get("searchResult", {}).get("products", [])
            or pprops.get("products", [])
            or []
        )
        logger.info(f"Big W fallback items found: {len(items)}")

        for item in items:
            name = item.get("name", item.get("title", ""))
            if not name or "lego" not in name.lower(): continue

            # Price can be nested or flat
            p_obj = item.get("price", {})
            price = parse_price(
                p_obj.get("current") if isinstance(p_obj, dict)
                else item.get("priceValue") or p_obj
            )
            rrp = parse_price(
                (p_obj.get("was") if isinstance(p_obj, dict) else None)
                or item.get("wasPrice") or item.get("rrp")
            )
            if not price: continue

            set_num = str(item.get("sku", item.get("id", item.get("articleId", ""))))
            img     = item.get("imageUrl", item.get("image", ""))
            slug    = item.get("url", item.get("urlPath", ""))
            products.append(make_product(
                name, price, rrp, "Big W",
                f"https://www.bigw.com.au{slug}" if slug.startswith("/") else "https://www.bigw.com.au/search?q=lego",
                set_num, img,
                item.get("inStock", item.get("availability", True))
            ))

        logger.info(f"Big W: {len(products)} products")
    except Exception as e:
        logger.warning(f"Big W error: {e}")
    return products


async def scrape_target(client: httpx.AsyncClient) -> list[dict]:
    products = []
    try:
        target = "https://www.target.com.au/search?SearchTerm=lego&sz=96"
        r = await client.get(scraper_api_url(target), headers=HEADERS, timeout=60)
        data   = extract_next_data(r.text)
        pprops = data.get("props", {}).get("pageProps", {})

        items = (
            pprops.get("searchResults", {}).get("products", [])
            or pprops.get("searchResult", {}).get("products", [])
            or pprops.get("products", [])
            or []
        )
        logger.info(f"Target items found: {len(items)}")

        for item in items:
            name = item.get("name", item.get("displayName", item.get("title", "")))
            if not name or "lego" not in name.lower(): continue
            pricing = item.get("pricing", item.get("price", {}))
            if isinstance(pricing, dict):
                price = parse_price(pricing.get("now", pricing.get("current", pricing.get("selling", ""))))
                rrp   = parse_price(pricing.get("was", pricing.get("rrp", pricing.get("original", ""))))
            else:
                price = parse_price(pricing)
                rrp   = None
            if not price: continue
            set_num = str(item.get("productId", item.get("id", item.get("sku", ""))))
            imgs    = item.get("images", [{}])
            img     = (imgs[0].get("url", "") if isinstance(imgs[0], dict) else "") if imgs else item.get("imageUrl", "")
            slug    = item.get("url", item.get("pdpUrl", ""))
            stock   = item.get("availability", item.get("inStock", "IN_STOCK"))
            products.append(make_product(
                name, price, rrp, "Target",
                f"https://www.target.com.au{slug}" if slug.startswith("/") else "https://www.target.com.au/search?SearchTerm=lego",
                set_num, img,
                stock not in (False, "OUT_OF_STOCK", "UNAVAILABLE")
            ))

        logger.info(f"Target: {len(products)} products")
    except Exception as e:
        logger.warning(f"Target error: {e}")
    return products


async def scrape_kmart(client: httpx.AsyncClient) -> list[dict]:
    products = []
    try:
        target = "https://www.kmart.com.au/search/?q=lego&sz=96"
        r = await client.get(scraper_api_url(target), headers=HEADERS, timeout=60)
        data   = extract_next_data(r.text)
        pprops = data.get("props", {}).get("pageProps", {})

        items = (
            pprops.get("searchResult", {}).get("products", [])
            or pprops.get("searchResults", {}).get("products", [])
            or pprops.get("products", [])
            or []
        )
        logger.info(f"Kmart items found: {len(items)}")

        for item in items:
            name = item.get("name", item.get("title", ""))
            if not name or "lego" not in name.lower(): continue
            pricing = item.get("pricing", item.get("price", {}))
            if isinstance(pricing, dict):
                price = parse_price(pricing.get("current", pricing.get("now", pricing.get("selling", ""))))
                rrp   = parse_price(pricing.get("was", pricing.get("rrp", "")))
            else:
                price = parse_price(pricing)
                rrp   = None
            if not price: continue
            set_num = str(item.get("id", item.get("sku", item.get("articleId", ""))))
            img     = item.get("imageUrl", item.get("image", ""))
            slug    = item.get("url", item.get("urlPath", ""))
            products.append(make_product(
                name, price, rrp, "Kmart",
                f"https://www.kmart.com.au{slug}" if slug.startswith("/") else "https://www.kmart.com.au/search/?q=lego",
                set_num, img
            ))

        logger.info(f"Kmart: {len(products)} products")
    except Exception as e:
        logger.warning(f"Kmart error: {e}")
    return products


async def scrape_amazon_au(client: httpx.AsyncClient) -> list[dict]:
    products = []
    try:
        target = "https://www.amazon.com.au/s?k=lego&i=toys&s=review-rank"
        all_cards = []
        for pg in range(1, 4):  # scrape pages 1-3 for broader coverage
            page_url = target + f"&page={pg}"
            r = await client.get(scraper_api_url(page_url), headers=HEADERS, timeout=60)
            soup = BeautifulSoup(r.text, "html.parser")
            cards = soup.select("[data-component-type='s-search-result']")
            all_cards.extend(cards)
            if len(cards) < 10: break
        logger.info(f"Amazon AU total cards: {len(all_cards)}")
        # Debug: log first card's HTML snippet to see what selectors are available
        if all_cards:
            snippet = str(all_cards[0])[:500]
            logger.info(f"Amazon first card snippet: {snippet}")

        for card in all_cards:
            # Try multiple selectors — Amazon changes their HTML structure frequently
            title_el = (
                card.select_one("h2 a span.a-text-normal")
                or card.select_one("[data-cy='title-recipe'] span")
                or card.select_one("h2 span")
                or card.select_one(".a-size-base-plus.a-color-base.a-text-normal")
                or card.select_one(".a-size-medium.a-color-base.a-text-normal")
            )
            if not title_el: continue
            name = title_el.get_text(strip=True)
            # Skip very short names or non-LEGO results
            if len(name) < 6 or "lego" not in name.lower(): continue

            # Price: try structured price block first, then fallback
            pw = card.select_one(".a-price-whole")
            pf = card.select_one(".a-price-fraction")
            offscreen = card.select_one(".a-price .a-offscreen")
            if offscreen:
                price = parse_price(offscreen.get_text())
            elif pw:
                price_str = pw.get_text(strip=True).replace(",", "").rstrip(".")
                if pf: price_str += f".{pf.get_text(strip=True)}"
                price = parse_price(price_str)
            else:
                price = None
            if not price: continue

            was_el = card.select_one(".a-text-price .a-offscreen, .a-text-price span.a-offscreen")
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

        logger.info(f"Amazon AU: {len(products)} products")
    except Exception as e:
        logger.warning(f"Amazon AU error: {e}")
    return products


async def scrape_catch(client: httpx.AsyncClient) -> list[dict]:
    products = []
    try:
        target = "https://www.catch.com.au/search/?q=lego&sort=popularity"
        r = await client.get(scraper_api_url(target), headers=HEADERS, timeout=60)
        data   = extract_next_data(r.text)
        pprops = data.get("props", {}).get("pageProps", {})

        items = (
            pprops.get("searchResults", {}).get("results", [])
            or pprops.get("results", [])
            or pprops.get("initialData", {}).get("results", [])
            or []
        )
        logger.info(f"Catch items: {len(items)}")

        for item in items[:60]:
            name = item.get("name", item.get("title", ""))
            if not name or "lego" not in name.lower(): continue
            pricing = item.get("price", item.get("pricing", {}))
            if isinstance(pricing, dict):
                price = parse_price(pricing.get("current", pricing.get("selling", pricing.get("now", ""))))
                rrp   = parse_price(pricing.get("was", pricing.get("rrp", pricing.get("original", ""))))
            else:
                price = parse_price(pricing)
                rrp   = None
            if not price: continue
            imgs = item.get("images", item.get("media", []))
            if imgs and isinstance(imgs[0], dict):
                img = imgs[0].get("url", imgs[0].get("src", ""))
            elif imgs:
                img = str(imgs[0])
            else:
                img = item.get("imageUrl", "")
            slug = item.get("url", item.get("urlSlug", ""))
            products.append(make_product(
                name, price, rrp, "Catch",
                f"https://www.catch.com.au{slug}" if slug.startswith("/") else slug or "https://www.catch.com.au/search/?q=lego",
                str(item.get("id", "")), img,
                item.get("inStock", item.get("stock", 1)) not in (False, 0, "OUT_OF_STOCK")
            ))

        logger.info(f"Catch: {len(products)} products")
    except Exception as e:
        logger.warning(f"Catch error: {e}")
    return products


async def scrape_myer(client: httpx.AsyncClient) -> list[dict]:
    products = []
    try:
        target = "https://www.myer.com.au/search?query=lego&pageSize=48"
        r = await client.get(scraper_api_url(target), headers=HEADERS, timeout=60)
        data   = extract_next_data(r.text)
        pprops = data.get("props", {}).get("pageProps", {})

        items = (
            pprops.get("searchResults", {}).get("products", [])
            or pprops.get("products", [])
            or []
        )
        logger.info(f"Myer items: {len(items)}")

        # Also try raw HTML tile selectors as fallback
        if not items:
            soup = BeautifulSoup(r.text, "html.parser")
            for tile in soup.select("[class*='ProductTile'], [class*='product-tile']")[:48]:
                name_el  = tile.select_one("[class*='title'], [class*='name'], h2, h3")
                if not name_el or "lego" not in name_el.get_text().lower(): continue
                price_el = tile.select_one("[class*='price'], [class*='Price']")
                price    = parse_price(price_el.get_text()) if price_el else None
                if not price: continue
                link_el  = tile.select_one("a[href]")
                img_el   = tile.select_one("img")
                href     = link_el["href"] if link_el else ""
                products.append(make_product(
                    name_el.get_text().strip(), price, None, "Myer",
                    f"https://www.myer.com.au{href}" if href.startswith("/") else "https://www.myer.com.au/search?query=lego",
                    "", img_el.get("src", "") if img_el else ""
                ))
        else:
            for item in items:
                name  = item.get("name", item.get("displayName", item.get("title", "")))
                if not name or "lego" not in name.lower(): continue
                price = parse_price(str(item.get("price", item.get("sellingPrice", item.get("salePrice", "")))))
                rrp   = parse_price(str(item.get("wasPrice", item.get("rrp", ""))))
                if not price: continue
                set_num = str(item.get("sku", item.get("productId", item.get("id", ""))))
                img     = item.get("imageUrl", item.get("primaryImage", item.get("image", "")))
                slug    = item.get("url", item.get("pdpUrl", ""))
                products.append(make_product(
                    name, price, rrp, "Myer",
                    f"https://www.myer.com.au{slug}" if slug.startswith("/") else "https://www.myer.com.au/search?query=lego",
                    set_num, img
                ))

        logger.info(f"Myer: {len(products)} products")
    except Exception as e:
        logger.warning(f"Myer error: {e}")
    return products



async def scrape_shopify_search(client: httpx.AsyncClient,
                                 base_url: str,
                                 keyword: str,
                                 retailer_name: str) -> list[dict]:
    """
    Shopify scraper that fetches /products.json with no collection filter.
    Works for stores where the collection path doesn't support products.json.
    Filters results by keyword in title.
    """
    products = []
    page = 1
    while page <= 10:  # safety limit
        url = f"{base_url}/products.json?limit=250&page={page}"
        try:
            r = await client.get(url, headers=HEADERS, timeout=30)
            if r.status_code != 200:
                logger.warning(f"{retailer_name} search page {page}: HTTP {r.status_code}")
                break
            try:
                data = r.json()
            except Exception:
                logger.warning(f"{retailer_name} search page {page}: non-JSON response")
                break
            items = data.get("products", [])
            if not items:
                break
            for item in items:
                name = item.get("title", "")
                if not name or keyword.lower() not in name.lower():
                    continue
                variants = item.get("variants", [{}])
                variant  = variants[0] if variants else {}
                price    = parse_price(str(variant.get("price", "")))
                rrp      = parse_price(str(variant.get("compare_at_price") or ""))
                if not price:
                    continue
                images = item.get("images", [])
                img    = images[0].get("src", "") if images else ""
                handle = item.get("handle", "")
                products.append(make_product(
                    name, price, rrp, retailer_name,
                    f"{base_url}/products/{handle}",
                    str(variant.get("sku", item.get("id", ""))),
                    img,
                    variant.get("available", True)
                ))
            if len(items) < 250:
                break
            page += 1
        except Exception as e:
            logger.warning(f"{retailer_name} search page {page} error: {e}")
            break
    logger.info(f"{retailer_name}: {len(products)} products")
    return products

# ══════════════════════════════════════════════════════════════════════════════
#  MASTER ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════════

async def run_all_scrapers():
    logger.info("=== Starting scrape run ===")

    if not SCRAPER_API_KEY:
        logger.warning("⚠️  SCRAPER_API_KEY not set — Big W, Target, Kmart, Amazon, Catch, Myer will likely be blocked")

    async with httpx.AsyncClient(follow_redirects=True, timeout=60) as client:
        results = await asyncio.gather(
            # ScraperAPI-proxied (Cloudflare-protected)
            scrape_bigw(client),
            scrape_target(client),
            scrape_kmart(client),
            scrape_amazon_au(client),
            scrape_catch(client),
            scrape_myer(client),
            # Shopify (native — no proxy needed)
            scrape_shopify_search(client, "https://www.jbhifi.com.au", "lego", "JB Hi-Fi"),
            scrape_shopify_search(client, "https://www.ebgames.com.au", "lego", "EB Games"),
            scrape_shopify(client, "https://www.myhobbies.com.au",   "lego",       "MyHobbies"),
            scrape_shopify(client, "https://www.toysrus.com.au",     "lego",       "Toys R Us"),
            return_exceptions=True,
        )

    retailer_names = [
        "Big W", "Target", "Kmart", "Amazon AU", "Catch", "Myer",
        "JB Hi-Fi", "EB Games", "MyHobbies", "Toys R Us",
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

    # Deduplicate within each retailer (same product listed twice)
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
    logger.info(f"=== Done: {len(deduped)} products across {len(retailer_names)} retailers, {len(DAILY_DROPS)} daily drops ===")
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
#  ROUTES
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
        "products":     products,
        "total":        len(products),
        "last_updated": CACHE["last_updated"],
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
        m["price_rank"]   = i + 1
        m["is_cheapest"]  = (i == 0)
        m["price_history"] = PRICE_HISTORY.get(product_key(m), [])
    img = next((m.get("image_url") or lego_img(m.get("set_number", ""))
                for m in result if m.get("image_url") or m.get("set_number")), "")
    return {
        "set_number":   set_number,
        "retailers":    result,
        "lowest_price":  result[0]["price"] if result else None,
        "highest_price": result[-1]["price"] if result else None,
        "price_spread":  round(result[-1]["price"] - result[0]["price"], 2) if len(result) > 1 else 0,
        "image_url":    img,
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
        "scraper_api_active": bool(SCRAPER_API_KEY),
    }

@app.post("/api/alerts/test")
async def test_alerts():
    await send_ntfy("🧱 BrickHunt Test", "Alerts are working!")
    send_email("🧱 BrickHunt Test", "<h2>Email alerts working!</h2>")
    return {"message": "Test notifications sent"}

@app.get("/api/debug")
def debug():
    """Quick health check — shows scrape status and one sample product per retailer."""
    by_retailer = {}
    for p in CACHE["products"]:
        if p["retailer"] not in by_retailer:
            by_retailer[p["retailer"]] = {"name": p["name"], "price": p["price"], "discount_pct": p["discount_pct"]}
    return {
        "scraper_api_configured": bool(SCRAPER_API_KEY),
        "scrape_status":          CACHE["scrape_status"],
        "total_products":         len(CACHE["products"]),
        "sample_per_retailer":    by_retailer,
        "last_updated":           CACHE["last_updated"],
    }
