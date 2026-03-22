"""
Bot runner – Railway/cloud versie.

Grote verschillen t.o.v. VPS-versie:
- Geen SIGHUP: poll Supabase elke 30s op config-versie
- Geen SQLite: alles via db.py → Supabase
- Start/stop via db.get_bot_command() in plaats van subprocess
- log_fn callback zodat API de logs kan streamen via SSE
"""

import time
import random
import logging
import requests
from bs4 import BeautifulSoup
import db

log = logging.getLogger(__name__)

UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0",
]

CONFIG_POLL_INTERVAL = 30   # seconden tussen config-checks
IDLE_POLL_INTERVAL   = 10   # seconden polling als bot gestopt is


# ── HTTP ──────────────────────────────────────────────────────────────────────

def fetch(url, params=None):
    time.sleep(random.uniform(2, 5))
    try:
        r = requests.get(url, params=params, timeout=15, headers={
            "User-Agent": random.choice(UA_LIST),
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
            "Accept-Language": "nl-NL,nl;q=0.9",
        })
        r.raise_for_status()
        return r
    except Exception as e:
        log.warning(f"Fetch fout {url}: {e}")
        return None


# ── Scrapers ──────────────────────────────────────────────────────────────────

def scrape_marktplaats(z: dict) -> list:
    query = f"{z.get('merk','')} {z.get('model','')}".strip()
    params = {"query": query, "categoryId": "91", "limit": 30,
              "sortBy": "SORT_INDEX", "sortOrder": "DECREASING"}
    if z.get("prijs_min"): params["priceFrom"] = z["prijs_min"]
    if z.get("prijs_max"): params["priceTo"]   = z["prijs_max"]

    resp = fetch("https://www.marktplaats.nl/lrp/api/search", params)
    if not resp: return []

    ads = []
    for item in resp.json().get("listings", []):
        try:
            iid = str(item.get("itemId", ""))
            if not iid: continue
            p = item.get("priceInfo", {}).get("priceCents")
            attrs = {a["key"]: a.get("value", "") for a in item.get("attributes", [])}
            pics = item.get("pictures", [])
            ads.append({
                "id":          f"mp_{iid}",
                "website":     "marktplaats",
                "search_naam": z["naam"],
                "titel":       item.get("title", "?"),
                "prijs":       f"€ {int(p)//100:,}".replace(",",".") if p else "Op aanvraag",
                "url":         f"https://www.marktplaats.nl{item.get('vipUrl','')}",
                "afbeelding":  pics[0].get("mediumUrl") if pics else None,
                "bouwjaar":    str(attrs.get("constructionYear", "?")),
                "km":          str(attrs.get("mileage", "?")),
                "brandstof":   attrs.get("fuel", ""),
                "transmissie": attrs.get("transmission", ""),
            })
        except Exception as e:
            log.debug(f"MP parse: {e}")
    return ads


def scrape_autoscout(z: dict, site: str) -> list:
    domain = "nl" if site == "autoscout_nl" else "de"
    merk   = z.get("merk", "").lower().replace(" ", "-")
    model  = z.get("model", "").lower().replace(" ", "-")
    url    = f"https://www.autoscout24.{domain}/lst/{merk}/{model}" if merk else f"https://www.autoscout24.{domain}/lst"
    params = {"sort": "age", "desc": "0", "size": 20, "ustate": "N,U"}
    if z.get("prijs_min"): params["pricefrom"] = z["prijs_min"]
    if z.get("prijs_max"): params["priceto"]   = z["prijs_max"]
    if z.get("bouwjaar_min"): params["yearfrom"] = z["bouwjaar_min"]
    if z.get("km_max"):    params["kmto"]      = z["km_max"]

    resp = fetch(url, params)
    if not resp: return []

    ads = []
    try:
        soup = BeautifulSoup(resp.text, "html.parser")
        for article in soup.select("article[data-guid]"):
            try:
                ad_id    = article.get("data-guid", "")
                titel_el = article.select_one("h2")
                prijs_el = article.select_one("[class*='price']")
                link_el  = article.select_one("a[href]")
                img_el   = article.select_one("img[src]")
                href = link_el["href"] if link_el else ""
                if href and not href.startswith("http"):
                    href = f"https://www.autoscout24.{domain}{href}"
                ads.append({
                    "id":          f"{site}_{ad_id}",
                    "website":     site,
                    "search_naam": z["naam"],
                    "titel":       titel_el.get_text(strip=True) if titel_el else "?",
                    "prijs":       prijs_el.get_text(strip=True) if prijs_el else "?",
                    "url":         href,
                    "afbeelding":  img_el["src"] if img_el else None,
                    "bouwjaar":    "?", "km": "?", "brandstof": "", "transmissie": "",
                })
            except Exception as e:
                log.debug(f"AS24 parse: {e}")
    except Exception as e:
        log.warning(f"AS24 HTML: {e}")
    return ads


# ── Telegram ──────────────────────────────────────────────────────────────────

def send_telegram(token: str, chat_id: str, ad: dict):
    emoji = {"marktplaats":"🟡","autoscout_nl":"🔵","autoscout_de":"🇩🇪"}.get(ad["website"],"🚗")
    naam  = {"marktplaats":"Marktplaats.nl","autoscout_nl":"AutoScout24.nl","autoscout_de":"AutoScout24.de"}.get(ad["website"],ad["website"])
    tekst = (
        f"{emoji} *Nieuwe advertentie – {naam}*\n"
        f"🔍 _{ad['search_naam']}_\n\n"
        f"🚘 *{ad['titel']}*\n"
        f"💶 {ad['prijs']}\n"
        f"📅 {ad['bouwjaar']}  📏 {ad['km']} km\n\n"
        f"🔗 [Bekijk advertentie]({ad['url']})"
    )
    try:
        if ad.get("afbeelding"):
            r = requests.post(f"https://api.telegram.org/bot{token}/sendPhoto",
                data={"chat_id": chat_id, "photo": ad["afbeelding"],
                      "caption": tekst, "parse_mode": "Markdown"}, timeout=10)
            if r.ok: return
        requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
            data={"chat_id": chat_id, "text": tekst, "parse_mode": "Markdown"}, timeout=10)
    except Exception as e:
        log.warning(f"Telegram fout: {e}")


# ── Hoofdloop ─────────────────────────────────────────────────────────────────

def run(log_fn=None):
    """
    Hoofdloop — wordt als thread gestart door api.py.
    log_fn: callback om logregels naar SSE te sturen.
    """

    def L(msg):
        log.info(msg)
        if log_fn: log_fn(f"[BOT] {msg}")

    L("=== Bot runner gestart (Railway) ===")

    searches      = []
    config_versie = 0
    bot_versie    = 0
    interval      = 300
    ronde         = 0
    actief        = False
    last_config_check = 0

    while True:
        now = time.time()

        # ── Config-poll (elke 30s) ────────────────────────────────────────
        if now - last_config_check > CONFIG_POLL_INTERVAL:
            last_config_check = now
            try:
                nieuwe_versie = db.get_searches_versie()
                if nieuwe_versie != config_versie:
                    searches, config_versie = db.get_searches()
                    searches = [s for s in searches if s.get("actief", True)]
                    cfg = db.get_config()
                    interval = int(cfg.get("interval", 300))
                    L(f"Config herladen: {len(searches)} zoekopdrachten, interval={interval}s (versie {config_versie})")

                # Start/stop commando checken
                command, nieuwe_bot_versie = db.get_bot_command()
                if nieuwe_bot_versie != bot_versie:
                    bot_versie = nieuwe_bot_versie
                    actief = command == "start"
                    L(f"Bot {'gestart' if actief else 'gestopt'} via dashboard")
            except Exception as e:
                L(f"Config-poll fout: {e}")

        # ── Idle als gestopt ──────────────────────────────────────────────
        if not actief:
            time.sleep(IDLE_POLL_INTERVAL)
            continue

        # ── Zoekronde ─────────────────────────────────────────────────────
        ronde += 1
        L(f"── Ronde {ronde} ──")

        try:
            cfg     = db.get_config()
            token   = cfg.get("telegram_token", "")
            chat_id = cfg.get("telegram_chat_id", "")
            nieuw   = 0

            for z in searches:
                for site in z.get("websites", []):
                    if site == "marktplaats":
                        ads = scrape_marktplaats(z)
                    elif site in ("autoscout_nl", "autoscout_de"):
                        ads = scrape_autoscout(z, site)
                    else:
                        continue

                    for ad in ads:
                        if db.is_new(ad["id"]):
                            L(f"NIEUW: {ad['titel']} – {ad['prijs']} [{site}]")
                            if token and chat_id:
                                send_telegram(token, chat_id, ad)
                            db.mark_seen(ad)
                            nieuw += 1

            if nieuw == 0:
                L("Geen nieuwe advertenties")
            else:
                L(f"{nieuw} nieuwe advertentie(s) verstuurd")

        except Exception as e:
            L(f"Fout in zoekronde: {e}")

        L(f"Volgende ronde over {interval}s")
        time.sleep(interval)
