"""
LEGO Price Tracker - Australia
FastAPI backend with working scrapers for all 8 AU retailers.
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

EMAIL_FROM         = os.getenv("ALERT_EMAIL_FROM", "")
EMAIL_PASS         = os.getenv("ALERT_EMAIL_PASS", "")
EMAIL_TO           = os.getenv("ALERT_EMAIL_TO", "")
ALERT_MIN_DISCOUNT = float(os.getenv("ALERT_MIN_DISCOUNT", "50"))
WATCHED_SETS       = {s.strip() for s in os.getenv("WATCHED_SETS", "").split(",") if s.strip()}
NTFY_TOPIC         = os.getenv("NTFY_TOPIC", "")

# Use a realistic browser user-agent and AU headers
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

CACHE: dict = {"products": [], "last_updated": None, "scrape_status": {}}
PRICE_HISTORY: dict[str, list[dict]] = {}
DAILY_DROPS: list[dict] = []
ALERTED: set[str] = set()


# ─── Helpers ──────────────────────────────────────────────────────────────────

def parse_price(text) -> float | None:
    if not text:
        return None
    text = str(text).replace(",", "").replace("$", "").strip()
    m = re.search(r"\d+\.?\d*", text)
    if m:
        try:
            v = float(m.group())
            return v if v > 0 else None
        except ValueError:
            return None
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

def make_product(name, price, rrp, retailer, url, set_number="", image_url="", in_stock=True) -> dict:
    return {
        "name": name.strip(),
        "set_number": str(set_number).strip(),
        "price": price,
        "rrp": rrp,
        "discount_pct": pct_off(rrp, price),
        "retailer": retailer,
        "url": url,
        "in_stock": in_stock,
        "image_url": image_url or lego_img(str(set_number)),
    }


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
            drops.append({**p, "prev_price": y["price"], "drop_amount": drop_amt, "drop_pct": drop_pct})
    return sorted(drops, key=lambda x: -x["drop_pct"])


# ─── Alerts ────────────────────────────────────────────────────────────────────

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
        msg["Subject"] = subject; msg["From"] = EMAIL_FROM; msg["To"] = EMAIL_TO
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
        if p.get("set_number") and p["set_number"] in WATCHED_SETS and p["discount_pct"] > 0 and ak not in ALERTED:
            watched_deals.append(p); ALERTED.add(ak)
    if big_deals:
        html = "<h2>🔥 BrickHunt AU</h2><ul>" + "".join(
            f"<li><b>{p['name']}</b> @ {p['retailer']} — ${p['price']:.2f} ({p['discount_pct']}% off)<br><a href='{p['url']}'>View →</a></li>"
            for p in big_deals[:10]) + "</ul>"
        send_email(f"🔥 {len(big_deals)} LEGO deal(s) at {ALERT_MIN_DISCOUNT:.0f}%+ off!", html)
        for p in big_deals[:3]:
            await send_ntfy(f"🔥 {p['discount_pct']}% OFF: {p['name'][:45]}", f"{p['retailer']} — ${p['price']:.2f}", p["url"])
    if watched_deals:
        html = "<h2>🎯 Watched Set on Sale!</h2><ul>" + "".join(
            f"<li><b>#{p['set_number']}</b> {p['name']} @ {p['retailer']} — ${p['price']:.2f}<br><a href='{p['url']}'>View →</a></li>"
            for p in watched_deals) + "</ul>"
        send_email("🎯 Watched LEGO set on sale!", html)
        for p in watched_deals:
            await send_ntfy(f"🎯 #{p['set_number']} on sale!", f"{p['name'][:45]} — ${p['price']:.2f}", p["url"])


# ══════════════════════════════════════════════════════════════════════════════
#  SCRAPERS
#  Each scraper uses the actual live URL for that retailer and handles
#  multiple fallback selectors so if the site updates its structure we
#  still have a chance of getting data.
# ══════════════════════════════════════════════════════════════════════════════

async def scrape_bigw(client: httpx.AsyncClient) -> list[dict]:
    """
    Big W uses a Next.js frontend. Product data is in __NEXT_DATA__ JSON
    embedded in the search results page HTML.
    """
    products = []
    try:
        url = "https://www.bigw.com.au/search?q=lego"
        r = await client.get(url, headers=HEADERS, timeout=25)
        soup = BeautifulSoup(r.text, "html.parser")

        # Try __NEXT_DATA__ first — most reliable
        script = soup.find("script", {"id": "__NEXT_DATA__"})
        if script:
            data = json.loads(script.string)
            # Walk common paths Big W uses
            page_props = data.get("props", {}).get("pageProps", {})
            items = (
                page_props.get("searchResult", {}).get("products", [])
                or page_props.get("initialState", {}).get("search", {}).get("searchResult", {}).get("products", [])
                or page_props.get("products", [])
            )
            logger.info(f"Big W __NEXT_DATA__ items: {len(items)}")
            for item in items:
                name = item.get("name", "")
                if not name or "lego" not in name.lower(): continue
                # Price structure varies — try several field names
                price = parse_price(
                    item.get("priceValue")
                    or item.get("price", {}).get("current") if isinstance(item.get("price"), dict) else item.get("price")
                )
                rrp = parse_price(
                    item.get("wasPrice")
                    or item.get("rrp")
                    or (item.get("price", {}).get("was") if isinstance(item.get("price"), dict) else None)
                )
                if not price: continue
                set_num = str(item.get("sku", item.get("id", item.get("articleId", ""))))
                img = item.get("imageUrl", item.get("image", ""))
                slug = item.get("url", item.get("urlPath", ""))
                products.append(make_product(
                    name, price, rrp, "Big W",
                    f"https://www.bigw.com.au{slug}" if slug.startswith("/") else f"https://www.bigw.com.au/search?q=lego",
                    set_num, img,
                    item.get("inStock", item.get("availability", True))
                ))

        # HTML fallback — scan product tiles
        if not products:
            logger.info("Big W: falling back to HTML tile scrape")
            tile_selectors = [
                "article[data-testid]",
                "[class*='ProductTile']",
                "[class*='product-tile']",
                "[class*='product-card']",
            ]
            for sel in tile_selectors:
                tiles = soup.select(sel)
                if not tiles: continue
                for tile in tiles[:48]:
                    name_el = tile.select_one("h2, h3, [class*='title'], [class*='name']")
                    if not name_el or "lego" not in name_el.get_text().lower(): continue
                    price_el = tile.select_one("[class*='price'], [data-testid*='price']")
                    price = parse_price(price_el.get_text()) if price_el else None
                    if not price: continue
                    link_el = tile.select_one("a[href]")
                    img_el  = tile.select_one("img")
                    href = link_el["href"] if link_el else ""
                    products.append(make_product(
                        name_el.get_text().strip(), price, None, "Big W",
                        f"https://www.bigw.com.au{href}" if href.startswith("/") else href or "https://www.bigw.com.au/search?q=lego",
                        "", img_el.get("src", "") if img_el else ""
                    ))
                if products: break

        logger.info(f"Big W: {len(products)} products")
    except Exception as e:
        logger.warning(f"Big W error: {e}")
    return products


async def scrape_target(client: httpx.AsyncClient) -> list[dict]:
    """
    Target AU — Next.js site, data in __NEXT_DATA__.
    """
    products = []
    try:
        url = "https://www.target.com.au/search?SearchTerm=lego&sz=48"
        r = await client.get(url, headers=HEADERS, timeout=25)
        soup = BeautifulSoup(r.text, "html.parser")

        script = soup.find("script", {"id": "__NEXT_DATA__"})
        if script:
            data  = json.loads(script.string)
            pprops = data.get("props", {}).get("pageProps", {})
            items = (
                pprops.get("searchResults", {}).get("products", [])
                or pprops.get("products", [])
                or pprops.get("searchResult", {}).get("products", [])
                or []
            )
            logger.info(f"Target __NEXT_DATA__ items: {len(items)}")
            for item in items:
                name = item.get("name", item.get("displayName", item.get("title", "")))
                if not name or "lego" not in name.lower(): continue
                pricing = item.get("pricing", item.get("price", {}))
                if isinstance(pricing, dict):
                    price = parse_price(pricing.get("now", pricing.get("current", pricing.get("selling", ""))))
                    rrp   = parse_price(pricing.get("was", pricing.get("rrp",     pricing.get("original", ""))))
                else:
                    price = parse_price(pricing)
                    rrp   = None
                if not price: continue
                set_num = str(item.get("productId", item.get("id", item.get("sku", ""))))
                img     = item.get("imageUrl", item.get("images", [{}])[0].get("url", "") if item.get("images") else "")
                slug    = item.get("url", item.get("pdpUrl", f"/p/{set_num}"))
                stock   = item.get("availability", item.get("inStock", "IN_STOCK"))
                products.append(make_product(
                    name, price, rrp, "Target",
                    f"https://www.target.com.au{slug}" if slug.startswith("/") else slug or "https://www.target.com.au/search?SearchTerm=lego",
                    set_num, img,
                    stock not in (False, "OUT_OF_STOCK", "UNAVAILABLE")
                ))

        if not products:
            logger.info("Target: falling back to HTML tile scrape")
            for tile in soup.select("[class*='ProductCard'], [class*='product-card'], article")[:48]:
                name_el  = tile.select_one("h2, h3, [class*='name'], [class*='title']")
                if not name_el or "lego" not in name_el.get_text().lower(): continue
                price_el = tile.select_one("[class*='price'], [class*='Price']")
                price    = parse_price(price_el.get_text()) if price_el else None
                if not price: continue
                link_el  = tile.select_one("a[href]")
                img_el   = tile.select_one("img")
                href     = link_el["href"] if link_el else ""
                products.append(make_product(
                    name_el.get_text().strip(), price, None, "Target",
                    f"https://www.target.com.au{href}" if href.startswith("/") else href or "https://www.target.com.au/search?SearchTerm=lego",
                    "", img_el.get("src","") if img_el else ""
                ))

        logger.info(f"Target: {len(products)} products")
    except Exception as e:
        logger.warning(f"Target error: {e}")
    return products


async def scrape_kmart(client: httpx.AsyncClient) -> list[dict]:
    """
    Kmart AU — Next.js. Data in __NEXT_DATA__.
    """
    products = []
    try:
        url = "https://www.kmart.com.au/search/?q=lego&sz=48"
        r = await client.get(url, headers=HEADERS, timeout=25)
        soup = BeautifulSoup(r.text, "html.parser")

        script = soup.find("script", {"id": "__NEXT_DATA__"})
        if script:
            data   = json.loads(script.string)
            pprops = data.get("props", {}).get("pageProps", {})
            items  = (
                pprops.get("searchResult", {}).get("products", [])
                or pprops.get("products", [])
                or pprops.get("searchResults", {}).get("products", [])
                or []
            )
            logger.info(f"Kmart __NEXT_DATA__ items: {len(items)}")
            for item in items:
                name = item.get("name", item.get("title", ""))
                if not name or "lego" not in name.lower(): continue
                pricing = item.get("pricing", item.get("price", {}))
                if isinstance(pricing, dict):
                    price = parse_price(pricing.get("current", pricing.get("now", pricing.get("selling", ""))))
                    rrp   = parse_price(pricing.get("was",     pricing.get("rrp",     "")))
                else:
                    price = parse_price(pricing)
                    rrp   = None
                if not price: continue
                set_num = str(item.get("id", item.get("sku", item.get("articleId", ""))))
                img     = item.get("imageUrl", item.get("image", ""))
                slug    = item.get("url", item.get("urlPath", f"/product/{set_num}"))
                products.append(make_product(
                    name, price, rrp, "Kmart",
                    f"https://www.kmart.com.au{slug}" if slug.startswith("/") else slug or "https://www.kmart.com.au/search/?q=lego",
                    set_num, img
                ))

        if not products:
            logger.info("Kmart: falling back to HTML tile scrape")
            for tile in soup.select("[class*='product'], article")[:48]:
                name_el  = tile.select_one("h2, h3, [class*='name'], [class*='title']")
                if not name_el or "lego" not in name_el.get_text().lower(): continue
                price_el = tile.select_one("[class*='price']")
                price    = parse_price(price_el.get_text()) if price_el else None
                if not price: continue
                link_el  = tile.select_one("a[href]")
                img_el   = tile.select_one("img")
                href     = link_el["href"] if link_el else ""
                products.append(make_product(
                    name_el.get_text().strip(), price, None, "Kmart",
                    f"https://www.kmart.com.au{href}" if href.startswith("/") else href or "https://www.kmart.com.au/search/?q=lego",
                    "", img_el.get("src","") if img_el else ""
                ))

        logger.info(f"Kmart: {len(products)} products")
    except Exception as e:
        logger.warning(f"Kmart error: {e}")
    return products


async def scrape_amazon_au(client: httpx.AsyncClient) -> list[dict]:
    """
    Amazon AU — HTML scrape of the search results page.
    Fixed: use h2 a span for title (not just h2 span which can grab short labels).
    """
    products = []
    try:
        url = "https://www.amazon.com.au/s?k=lego+set&i=toys&rh=n%3A4975211011"
        r = await client.get(url, headers={**HEADERS, "Accept": "text/html"}, timeout=25)
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select("[data-component-type='s-search-result']")
        logger.info(f"Amazon AU cards found: {len(cards)}")

        for card in cards[:40]:
            # Get full title from the link element — more reliable than span alone
            title_el = card.select_one("h2 a span.a-text-normal, h2 a span, h2 span.a-text-normal")
            if not title_el: continue
            name = title_el.get_text(strip=True)
            # Must be a real LEGO product name (not just "LEGO")
            if len(name) < 8 or "lego" not in name.lower(): continue

            price_whole = card.select_one(".a-price-whole")
            price_frac  = card.select_one(".a-price-fraction")
            if not price_whole: continue
            price_str = price_whole.get_text(strip=True).replace(",", "").rstrip(".")
            if price_frac:
                price_str += f".{price_frac.get_text(strip=True)}"
            price = parse_price(price_str)
            if not price: continue

            # RRP / was price
            was_el = card.select_one(".a-text-price .a-offscreen, .a-text-price span.a-offscreen")
            rrp = parse_price(was_el.get_text()) if was_el else None

            asin    = card.get("data-asin", "")
            link_el = card.select_one("h2 a[href]")
            img_el  = card.select_one("img.s-image")
            img     = img_el.get("src", "") if img_el else ""
            href    = link_el["href"] if link_el else f"/dp/{asin}"

            products.append(make_product(
                name, price, rrp, "Amazon AU",
                f"https://www.amazon.com.au{href}" if href.startswith("/") else href,
                asin, img
            ))

        logger.info(f"Amazon AU: {len(products)} products")
    except Exception as e:
        logger.warning(f"Amazon AU error: {e}")
    return products


async def scrape_catch(client: httpx.AsyncClient) -> list[dict]:
    """
    Catch.com.au — Next.js, data in __NEXT_DATA__.
    """
    products = []
    try:
        url = "https://www.catch.com.au/search/?q=lego&sort=popularity"
        r = await client.get(url, headers=HEADERS, timeout=25)
        soup = BeautifulSoup(r.text, "html.parser")
        script = soup.find("script", {"id": "__NEXT_DATA__"})
        if not script:
            logger.warning("Catch: no __NEXT_DATA__ found")
            return products
        data   = json.loads(script.string)
        pprops = data.get("props", {}).get("pageProps", {})
        items  = (
            pprops.get("searchResults", {}).get("results", [])
            or pprops.get("results", [])
            or pprops.get("initialData", {}).get("results", [])
            or []
        )
        logger.info(f"Catch items: {len(items)}")
        for item in items[:48]:
            name = item.get("name", item.get("title", ""))
            if not name or "lego" not in name.lower(): continue
            # Price may be nested under price.current or flat
            pricing = item.get("price", item.get("pricing", {}))
            if isinstance(pricing, dict):
                price = parse_price(pricing.get("current", pricing.get("selling", pricing.get("now", ""))))
                rrp   = parse_price(pricing.get("was",     pricing.get("rrp",     pricing.get("original", ""))))
            else:
                price = parse_price(pricing)
                rrp   = None
            if not price: continue
            # Image
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


async def scrape_jbhifi(client: httpx.AsyncClient) -> list[dict]:
    """
    JB Hi-Fi — Shopify-based. Try their collection JSON endpoint first,
    fall back to __NEXT_DATA__ HTML scrape.
    """
    products = []
    try:
        # JB Hi-Fi Shopify collection JSON — much more reliable than HTML
        url = "https://www.jbhifi.com.au/collections/lego/products.json?limit=100"
        r = await client.get(url, headers=HEADERS, timeout=25)
        try:
            data  = r.json()
            items = data.get("products", [])
            logger.info(f"JB Hi-Fi Shopify JSON items: {len(items)}")
            for item in items:
                name = item.get("title", "")
                if not name or "lego" not in name.lower(): continue
                variants = item.get("variants", [{}])
                variant  = variants[0] if variants else {}
                price    = parse_price(str(variant.get("price", "")))
                rrp      = parse_price(str(variant.get("compare_at_price", "")))
                if not price: continue
                images = item.get("images", [])
                img    = images[0].get("src", "") if images else ""
                handle = item.get("handle", "")
                products.append(make_product(
                    name, price, rrp, "JB Hi-Fi",
                    f"https://www.jbhifi.com.au/products/{handle}",
                    str(variant.get("sku", item.get("id", ""))), img,
                    variant.get("available", True)
                ))
        except Exception:
            # Fallback to __NEXT_DATA__ search page
            logger.info("JB Hi-Fi: Shopify JSON failed, trying HTML search page")
            url2 = "https://www.jbhifi.com.au/pages/search-results-page?q=lego"
            r2   = await client.get(url2, headers=HEADERS, timeout=25)
            soup = BeautifulSoup(r2.text, "html.parser")
            script = soup.find("script", {"id": "__NEXT_DATA__"})
            if script:
                d2    = json.loads(script.string)
                items = (
                    d2.get("props", {}).get("pageProps", {}).get("searchResults", {}).get("hits", [])
                    or d2.get("props", {}).get("pageProps", {}).get("searchResults", {}).get("products", [])
                    or []
                )
                for item in items:
                    name = item.get("title", item.get("name", ""))
                    if not name or "lego" not in name.lower(): continue
                    # JB prices sometimes in cents
                    raw_price = item.get("priceInCents", item.get("price", ""))
                    price = raw_price / 100 if isinstance(raw_price, int) and raw_price > 1000 else parse_price(str(raw_price))
                    raw_rrp = item.get("comparePriceInCents", item.get("compareAtPrice", ""))
                    rrp   = raw_rrp / 100 if isinstance(raw_rrp, int) and raw_rrp > 1000 else parse_price(str(raw_rrp))
                    if not price: continue
                    handle = item.get("handle", item.get("url", ""))
                    img    = item.get("image", item.get("featuredImage", ""))
                    products.append(make_product(
                        name, price, rrp, "JB Hi-Fi",
                        f"https://www.jbhifi.com.au/products/{handle}" if handle else "https://www.jbhifi.com.au",
                        str(item.get("sku", "")), img, item.get("available", True)
                    ))

        logger.info(f"JB Hi-Fi: {len(products)} products")
    except Exception as e:
        logger.warning(f"JB Hi-Fi error: {e}")
    return products


async def scrape_myer(client: httpx.AsyncClient) -> list[dict]:
    """
    Myer — try their search API first, then HTML fallback.
    """
    products = []
    try:
        # Myer uses Bloomreach search — try their API endpoint
        url = "https://www.myer.com.au/api/2.0/page/search?query=lego&pageSize=48&page=1"
        r = await client.get(url, headers=HEADERS, timeout=25)
        try:
            data  = r.json()
            items = data.get("products", data.get("results", data.get("data", {}).get("products", [])))
            logger.info(f"Myer API items: {len(items)}")
            for item in items:
                name = item.get("name", item.get("displayName", item.get("title", "")))
                if not name or "lego" not in name.lower(): continue
                price = parse_price(str(item.get("price", item.get("sellingPrice", item.get("salePrice", "")))))
                rrp   = parse_price(str(item.get("wasPrice", item.get("rrp", item.get("originalPrice", "")))))
                if not price: continue
                set_num = str(item.get("sku", item.get("productId", item.get("id", ""))))
                img     = item.get("imageUrl", item.get("primaryImage", item.get("image", "")))
                slug    = item.get("url", item.get("pdpUrl", ""))
                products.append(make_product(
                    name, price, rrp, "Myer",
                    f"https://www.myer.com.au{slug}" if slug.startswith("/") else slug or "https://www.myer.com.au/search?query=lego",
                    set_num, img
                ))
        except Exception:
            pass

        # HTML fallback
        if not products:
            logger.info("Myer: trying HTML search page")
            url2 = "https://www.myer.com.au/search?query=lego"
            r2   = await client.get(url2, headers=HEADERS, timeout=25)
            soup = BeautifulSoup(r2.text, "html.parser")
            # Try __NEXT_DATA__
            script = soup.find("script", {"id": "__NEXT_DATA__"})
            if script:
                d2 = json.loads(script.string)
                items = (
                    d2.get("props", {}).get("pageProps", {}).get("searchResults", {}).get("products", [])
                    or d2.get("props", {}).get("pageProps", {}).get("products", [])
                    or []
                )
                for item in items:
                    name  = item.get("name", item.get("title", ""))
                    if not name or "lego" not in name.lower(): continue
                    price = parse_price(str(item.get("price", item.get("sellingPrice", ""))))
                    rrp   = parse_price(str(item.get("wasPrice", item.get("rrp", ""))))
                    if not price: continue
                    set_num = str(item.get("sku", item.get("id", "")))
                    img     = item.get("imageUrl", item.get("image", ""))
                    slug    = item.get("url", "")
                    products.append(make_product(
                        name, price, rrp, "Myer",
                        f"https://www.myer.com.au{slug}" if slug.startswith("/") else "https://www.myer.com.au/search?query=lego",
                        set_num, img
                    ))
            # Raw HTML tiles
            if not products:
                for tile in soup.select("[class*='ProductTile'], [class*='product-tile'], [class*='product-card']")[:48]:
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
                        f"https://www.myer.com.au{href}" if href.startswith("/") else href or "https://www.myer.com.au/search?query=lego",
                        "", img_el.get("src","") if img_el else ""
                    ))

        logger.info(f"Myer: {len(products)} products")
    except Exception as e:
        logger.warning(f"Myer error: {e}")
    return products


async def scrape_eb_games(client: httpx.AsyncClient) -> list[dict]:
    """
    EB Games — Shopify store. Use /collections/lego/products.json — very reliable.
    """
    products = []
    try:
        url = "https://www.ebgames.com.au/collections/lego/products.json?limit=100"
        r   = await client.get(url, headers=HEADERS, timeout=25)
        data  = r.json()
        items = data.get("products", [])
        logger.info(f"EB Games Shopify JSON items: {len(items)}")
        for item in items:
            name = item.get("title", "")
            if not name or "lego" not in name.lower(): continue
            variants = item.get("variants", [{}])
            variant  = variants[0] if variants else {}
            price    = parse_price(str(variant.get("price", "")))
            rrp      = parse_price(str(variant.get("compare_at_price", "")))
            if not price: continue
            images = item.get("images", [])
            img    = images[0].get("src", "") if images else ""
            handle = item.get("handle", "")
            products.append(make_product(
                name, price, rrp, "EB Games",
                f"https://www.ebgames.com.au/products/{handle}",
                str(variant.get("sku", item.get("id", ""))), img,
                variant.get("available", True)
            ))
        logger.info(f"EB Games: {len(products)} products")
    except Exception as e:
        logger.warning(f"EB Games error: {e}")
    return products


# ─── Master orchestrator ───────────────────────────────────────────────────────

async def run_all_scrapers():
    logger.info("=== Starting scrape run ===")
    async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client:
        results = await asyncio.gather(
            scrape_bigw(client),
            scrape_target(client),
            scrape_kmart(client),
            scrape_amazon_au(client),
            scrape_catch(client),
            scrape_jbhifi(client),
            scrape_myer(client),
            scrape_eb_games(client),
            return_exceptions=True,
        )

    retailer_names = ["Big W", "Target", "Kmart", "Amazon AU", "Catch", "JB Hi-Fi", "Myer", "EB Games"]
    all_products = []
    for name, result in zip(retailer_names, results):
        if isinstance(result, Exception):
            logger.error(f"{name} raised exception: {result}")
            CACHE["scrape_status"][name] = f"error: {str(result)[:80]}"
        else:
            all_products.extend(result)
            CACHE["scrape_status"][name] = f"{len(result)} items"
            logger.info(f"  {name}: {len(result)} items")

    # Deduplicate per retailer (keep best-discounted version of same product per store)
    # Do NOT deduplicate across retailers — each retailer entry is shown separately
    seen, deduped = set(), []
    for p in sorted(all_products, key=lambda x: -x["discount_pct"]):
        key = (p["retailer"], p["name"].lower()[:50])
        if key not in seen:
            seen.add(key)
            deduped.append(p)

    CACHE["products"] = deduped
    CACHE["last_updated"] = datetime.now(timezone.utc).isoformat()
    record_prices(deduped)
    global DAILY_DROPS
    DAILY_DROPS = compute_daily_drops(deduped)
    logger.info(f"=== Scrape done: {len(deduped)} unique products, {len(DAILY_DROPS)} daily drops ===")
    await check_and_send_alerts(deduped)


# ─── Scheduler ─────────────────────────────────────────────────────────────────

scheduler = AsyncIOScheduler()

@app.on_event("startup")
async def startup():
    asyncio.create_task(run_all_scrapers())
    scheduler.add_job(run_all_scrapers, "interval", hours=1)
    scheduler.start()

@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown()


# ─── Routes ────────────────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {"status": "ok", "message": "LEGO AU Price Tracker"}

@app.get("/api/prices")
def get_prices(on_sale_only: bool = False, min_discount: float = 0, retailer: str = ""):
    products = CACHE["products"]
    if on_sale_only: products = [p for p in products if p["discount_pct"] > 0]
    if min_discount:  products = [p for p in products if p["discount_pct"] >= min_discount]
    if retailer:      products = [p for p in products if p["retailer"].lower() == retailer.lower()]
    return {
        "products": products,
        "total": len(products),
        "last_updated": CACHE["last_updated"],
        "scrape_status": CACHE["scrape_status"],
    }

@app.get("/api/drops")
def get_daily_drops(retailer: str = ""):
    drops = DAILY_DROPS
    if retailer: drops = [d for d in drops if d["retailer"].lower() == retailer.lower()]
    return {"drops": drops, "total": len(drops), "last_updated": CACHE["last_updated"], "date": date.today().isoformat()}

@app.get("/api/compare/{set_number}")
def compare_prices(set_number: str):
    # Match by set number, or by name similarity if set number not found
    clean_sn = re.sub(r"[^0-9a-zA-Z]", "", set_number).lower()
    matches  = [p for p in CACHE["products"] if re.sub(r"[^0-9a-zA-Z]", "", str(p.get("set_number", ""))).lower() == clean_sn]
    if not matches:
        # Try name-based fuzzy match
        tokens = [t for t in set_number.lower().split() if len(t) > 3]
        if tokens:
            for p in CACHE["products"]:
                score = sum(1 for t in tokens if t in p["name"].lower())
                if score >= min(2, len(tokens)):
                    matches.append(p)
    result = sorted([dict(m) for m in matches], key=lambda x: x["price"])
    for i, m in enumerate(result):
        m["price_rank"] = i + 1
        m["is_cheapest"] = (i == 0)
        m["price_history"] = PRICE_HISTORY.get(product_key(m), [])
    img = next((m.get("image_url") or lego_img(m.get("set_number","")) for m in result if m.get("image_url") or m.get("set_number")), "")
    return {
        "set_number": set_number,
        "retailers": result,
        "lowest_price":  result[0]["price"] if result else None,
        "highest_price": result[-1]["price"] if result else None,
        "price_spread":  round(result[-1]["price"] - result[0]["price"], 2) if len(result) > 1 else 0,
        "image_url": img,
    }

@app.post("/api/refresh")
async def trigger_refresh():
    asyncio.create_task(run_all_scrapers())
    return {"message": "Scrape triggered — check /api/prices in ~60 seconds"}

@app.get("/api/retailers")
def get_retailers():
    return {"retailers": sorted({p["retailer"] for p in CACHE["products"]})}

@app.get("/api/alerts/config")
def alert_config():
    return {
        "email_configured": bool(EMAIL_FROM and EMAIL_PASS and EMAIL_TO),
        "push_configured":  bool(NTFY_TOPIC),
        "ntfy_topic":       NTFY_TOPIC,
        "min_discount_alert": ALERT_MIN_DISCOUNT,
        "watched_sets": list(WATCHED_SETS),
    }

@app.post("/api/alerts/test")
async def test_alerts():
    await send_ntfy("🧱 BrickHunt Test", "Alerts are working!")
    send_email("🧱 BrickHunt Test", "<h2>Email alerts working!</h2>")
    return {"message": "Test notifications sent"}

@app.get("/api/debug")
def debug():
    """Shows raw scrape status and first product from each retailer for debugging."""
    by_retailer = {}
    for p in CACHE["products"]:
        if p["retailer"] not in by_retailer:
            by_retailer[p["retailer"]] = p
    return {
        "scrape_status": CACHE["scrape_status"],
        "total_products": len(CACHE["products"]),
        "sample_per_retailer": by_retailer,
        "last_updated": CACHE["last_updated"],
    }
