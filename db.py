"""
Supabase database client.
Vervangt de lokale SQLite uit de VPS-versie.
Alle queries via de Supabase REST API (postgrest).
"""

import os
import logging
import requests
from datetime import datetime, timezone

log = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]          # https://xxxx.supabase.co
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]  # service_role key (niet anon!)

def _headers():
    return {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
    }

def _url(table: str) -> str:
    return f"{SUPABASE_URL}/rest/v1/{table}"


# ── Advertenties ──────────────────────────────────────────────────────────────

def is_new(ad_id: str) -> bool:
    """True als het ID nog niet in Supabase staat."""
    r = requests.get(
        _url("seen_ads"),
        headers={**_headers(), "Prefer": ""},
        params={"id": f"eq.{ad_id}", "select": "id"},
        timeout=8,
    )
    return len(r.json()) == 0


def mark_seen(ad: dict):
    """Sla advertentie op in Supabase."""
    requests.post(
        _url("seen_ads"),
        headers={**_headers(), "Prefer": "resolution=ignore-duplicates"},
        json={
            "id":          ad["id"],
            "website":     ad["website"],
            "search_naam": ad["search_naam"],
            "titel":       ad["titel"],
            "prijs":       ad["prijs"],
            "url":         ad["url"],
            "afbeelding":  ad.get("afbeelding"),
            "bouwjaar":    ad.get("bouwjaar", "?"),
            "km":          ad.get("km", "?"),
            "brandstof":   ad.get("brandstof", ""),
            "transmissie": ad.get("transmissie", ""),
        },
        timeout=8,
    )


def get_ads(limit=50, offset=0, website=None, search_naam=None):
    params = {
        "select": "*",
        "order":  "gevonden_op.desc",
        "limit":  limit,
        "offset": offset,
    }
    if website:     params["website"]     = f"eq.{website}"
    if search_naam: params["search_naam"] = f"eq.{search_naam}"

    headers = {**_headers(), "Prefer": "count=exact"}
    r = requests.get(_url("seen_ads"), headers=headers, params=params, timeout=8)
    total = int(r.headers.get("content-range", "0/0").split("/")[-1] or 0)
    return r.json(), total


def get_stats():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    r_total = requests.get(_url("seen_ads"),
        headers={**_headers(), "Prefer": "count=exact"},
        params={"select": "id", "limit": 0}, timeout=8)
    r_today = requests.get(_url("seen_ads"),
        headers={**_headers(), "Prefer": "count=exact"},
        params={"select": "id", "limit": 0,
                "gevonden_op": f"gte.{today}T00:00:00Z"}, timeout=8)
    r_last = requests.get(_url("seen_ads"),
        headers={**_headers(), "Prefer": ""},
        params={"select": "gevonden_op", "order": "gevonden_op.desc", "limit": 1}, timeout=8)

    total = int(r_total.headers.get("content-range","0/0").split("/")[-1] or 0)
    today_count = int(r_today.headers.get("content-range","0/0").split("/")[-1] or 0)
    last_data = r_last.json()
    last_found = last_data[0]["gevonden_op"] if last_data else None
    return total, today_count, last_found


# ── Zoekopdrachten ────────────────────────────────────────────────────────────

def get_searches() -> list:
    r = requests.get(_url("searches"),
        headers={**_headers(), "Prefer": ""},
        params={"select": "config,versie", "order": "id.asc", "limit": 1},
        timeout=8)
    data = r.json()
    if data:
        return data[0]["config"], data[0]["versie"]
    return [], 0


def save_searches(searches: list) -> int:
    versie = int(datetime.now(timezone.utc).timestamp())
    payload = {"config": searches, "versie": versie,
               "bijgewerkt": datetime.now(timezone.utc).isoformat()}

    # Upsert: altijd één rij (id=1)
    r = requests.get(_url("searches"),
        headers={**_headers(), "Prefer": ""},
        params={"select": "id", "limit": 1}, timeout=8)
    rows = r.json()

    if rows:
        requests.patch(_url("searches"),
            headers=_headers(),
            params={"id": f"eq.{rows[0]['id']}"},
            json=payload, timeout=8)
    else:
        requests.post(_url("searches"),
            headers=_headers(), json=payload, timeout=8)
    return versie


def get_searches_versie() -> int:
    r = requests.get(_url("searches"),
        headers={**_headers(), "Prefer": ""},
        params={"select": "versie", "limit": 1}, timeout=8)
    data = r.json()
    return data[0]["versie"] if data else 0


# ── App config ────────────────────────────────────────────────────────────────

def get_config() -> dict:
    r = requests.get(_url("app_config"),
        headers={**_headers(), "Prefer": ""},
        params={"select": "key,value"}, timeout=8)
    return {row["key"]: row["value"] for row in r.json()}


def save_config(data: dict):
    for key, value in data.items():
        requests.post(_url("app_config"),
            headers={**_headers(), "Prefer": "resolution=merge-duplicates"},
            json={"key": key, "value": str(value),
                  "bijgewerkt": datetime.now(timezone.utc).isoformat()},
            timeout=8)


# ── Bot control (start/stop via DB) ──────────────────────────────────────────

def set_bot_command(command: str):
    """Sla 'start' of 'stop' op in de DB zodat de bot het kan lezen."""
    versie = int(datetime.now(timezone.utc).timestamp())
    requests.patch(_url("bot_control"),
        headers=_headers(),
        params={"id": "eq.1"},
        json={"command": command, "versie": versie,
              "bijgewerkt": datetime.now(timezone.utc).isoformat()},
        timeout=8)
    return versie


def get_bot_command() -> tuple[str, int]:
    r = requests.get(_url("bot_control"),
        headers={**_headers(), "Prefer": ""},
        params={"select": "command,versie", "id": "eq.1"}, timeout=8)
    data = r.json()
    if data:
        return data[0]["command"], data[0]["versie"]
    return "stop", 0
