#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CHP Traffic -> Telegram notifier (v3)
- Выбирает Communications Center (ASP.NET postback)
- Присылает только Collision
- Добавляет ссылку на Google Maps
  - сначала пытается достать Lat/Lon из "Details"
  - если не получилось, делает ссылку по тексту локации
"""
import os
import re
import time
import json
import urllib.parse
import datetime as dt
from typing import List, Dict, Optional, Tuple

import requests
from bs4 import BeautifulSoup

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

BASE_URL = os.getenv("CHP_URL", "https://cad.chp.ca.gov/Traffic.aspx")
COMM_CENTER = os.getenv("COMM_CENTER", "Inland")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

TYPE_REGEX = os.getenv("TYPE_REGEX", r"Collision")
AREA_REGEX = os.getenv("AREA_REGEX", r"")
LOCATION_REGEX = os.getenv("LOCATION_REGEX", r"")

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))
SEEN_FILE = os.getenv("SEEN_FILE", "seen.json")

# управление получением координат
FETCH_COORDS = os.getenv("FETCH_COORDS", "1") not in ("0", "false", "False", "no", "No")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36"
}

def load_seen() -> Dict[str, str]:
    try:
        with open(SEEN_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_seen(seen: Dict[str, str]) -> None:
    if len(seen) > 5000:
        keys = list(seen.keys())[-2000:]
        seen = {k: seen[k] for k in keys}
    with open(SEEN_FILE, "w", encoding="utf-8") as f:
        json.dump(seen, f, ensure_ascii=False, indent=2)

def send_telegram(text: str) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("[WARN] TELEGRAM_TOKEN или TELEGRAM_CHAT_ID не заданы. Сообщение не отправлено:\n", text)
        return
    api = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "disable_web_page_preview": True}
    try:
        r = requests.post(api, data=payload, timeout=20)
        if r.status_code != 200:
            print("[ERR] Telegram API status:", r.status_code, r.text[:200])
    except Exception as e:
        print("[ERR] Telegram send error:", e)

def extract_form(ctx: BeautifulSoup) -> Tuple[str, Dict[str,str]]:
    form = ctx.find("form")
    if not form:
        raise RuntimeError("Не найден тег <form>")
    action = form.get("action") or BASE_URL
    payload = {}
    for inp in form.find_all("input"):
        name = inp.get("name")
        if not name:
            continue
        t = inp.get("type", "").lower()
        if t in ("checkbox", "radio"):
            if inp.has_attr("checked"):
                payload[name] = inp.get("value", "on")
        elif t in ("submit", "button", "image"):
            continue
        else:
            payload[name] = inp.get("value", "")
    for sel in form.find_all("select"):
        name = sel.get("name")
        if not name:
            continue
        opt = sel.find("option", selected=True) or sel.find("option")
        if opt:
            payload[name] = opt.get("value", opt.get_text(strip=True))
    for ta in form.find_all("textarea"):
        name = ta.get("name")
        if not name:
            continue
        payload[name] = ta.get_text()
    return action, payload

def choose_communications_center(session: requests.Session) -> str:
    r = session.get(BASE_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    action, payload = extract_form(soup)

    selects = soup.find_all("select")
    if not selects:
        raise RuntimeError("Не найдено ни одного <select> на странице")

    def looks_like_comm_select(sel) -> bool:
        text = (sel.find_previous(string=True) or "") + " " + (sel.find_next(string=True) or "")
        return "communications" in str(text).lower() and "center" in str(text).lower()

    comm_select = None
    for sel in selects:
        if looks_like_comm_select(sel):
            comm_select = sel
            break
    if comm_select is None:
        comm_select = selects[0]

    target = COMM_CENTER.strip().lower()
    option_value = None
    for opt in comm_select.find_all("option"):
        label = opt.get_text(strip=True).lower()
        if target in label:
            option_value = opt.get("value") or opt.get_text(strip=True)
            break
    if not option_value:
        raise RuntimeError(f"Не нашёл Communications Center '{COMM_CENTER}'")

    name = comm_select.get("name")
    payload[name] = option_value

    form = soup.find("form")
    submit_name = None
    submit_value = None
    for btn in form.find_all("input", {"type": "submit"}):
        val = (btn.get("value") or "").strip().lower()
        if val in ("ok", "submit", "go"):
            submit_name = btn.get("name")
            submit_value = btn.get("value")
            break
    if not submit_name:
        btn = form.find("input", {"type": "submit"})
        if btn:
            submit_name = btn.get("name")
            submit_value = btn.get("value", "OK")
    if submit_name:
        payload[submit_name] = submit_value

    post_url = requests.compat.urljoin(BASE_URL, action)
    r2 = session.post(post_url, data=payload, headers=HEADERS, timeout=30)
    r2.raise_for_status()
    return r2.text

def find_incidents_table(soup: BeautifulSoup) -> Optional[BeautifulSoup]:
    for table in soup.find_all("table"):
        header = table.find("tr")
        if not header:
            continue
        headers = [h.get_text(strip=True).lower() for h in header.find_all(["th", "td"])]
        if not headers:
            continue
        if all(x in headers for x in ["time", "type", "location"]):
            return table
    return None

def parse_incidents_with_links(html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    table = find_incidents_table(soup)
    if not table:
        return []
    rows = table.find_all("tr")[1:]
    incidents = []
    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 7:
            continue

        details_href = None
        a = cols[0].find("a")
        if a and a.get("href"):
            details_href = requests.compat.urljoin(BASE_URL, a.get("href"))

        no = cols[1].get_text(strip=True)
        tm = cols[2].get_text(strip=True)
        itype = cols[3].get_text(strip=True)
        loc = cols[4].get_text(strip=True)
        locdesc = cols[5].get_text(strip=True)
        area = cols[6].get_text(strip=True)
        incidents.append({
            "no": no, "time": tm, "type": itype,
            "location": loc, "locdesc": locdesc, "area": area,
            "details_href": details_href
        })
    return incidents

def try_fetch_latlon_from_details(session: requests.Session, href: str) -> Optional[Tuple[float, float]]:
    try:
        r = session.get(href, headers=HEADERS, timeout=30)
        if r.status_code != 200:
            return None
        text = r.text
        m = re.search(r"Lat/Lon:\s*([+-]?\d+\.\d+)\s*[, ]\s*([+-]?\d+\.\d+)", text, re.IGNORECASE)
        if not m:
            m = re.search(r"([+-]?\d+\.\d+)\s*[, ]\s*([+-]?\d+\.\d+)", text)
        if m:
            lat = float(m.group(1)); lon = float(m.group(2))
            if -90 <= lat <= 90 and -180 <= lon <= 180:
                return (lat, lon)
    except Exception:
        return None
    return None

def filter_collisions(incidents: List[Dict[str, str]]) -> List[Dict[str, str]]:
    type_re = re.compile(TYPE_REGEX, re.IGNORECASE) if TYPE_REGEX else None
    area_re = re.compile(AREA_REGEX, re.IGNORECASE) if AREA_REGEX else None
    loc_re = re.compile(LOCATION_REGEX, re.IGNORECASE) if LOCATION_REGEX else None
    result = []
    for x in incidents:
        ok = True
        if type_re and not type_re.search(x["type"]):
            ok = False
        if ok and area_re and not area_re.search(x["area"]):
            ok = False
        if ok and loc_re and not (loc_re.search(x["location"]) or loc_re.search(x["locdesc"])):
            ok = False
        if ok:
            result.append(x)
    return result

def make_key(inc: Dict[str, str]) -> str:
    today = dt.date.today().isoformat()
    return f"{today}:{inc['no']}:{inc['time']}:{inc['type']}"

def google_maps_link(lat: Optional[float], lon: Optional[float], q: str) -> str:
    if lat is not None and lon is not None:
        return f"https://maps.google.com/?q={lat},{lon}"
    q_enc = urllib.parse.quote(q)
    return f"https://maps.google.com/?q={q_enc}"

def format_message(inc: Dict[str, str], lat: Optional[float], lon: Optional[float]) -> str:
    if lat is not None and lon is not None:
        # строим маршрут по координатам
        link = f"https://www.google.com/maps/dir/?api=1&destination={lat},{lon}&travelmode=driving"
    else:
        # запасной вариант, если координат нет
        link = f"https://www.google.com/maps/search/?api=1&query={inc['location']} {inc['locdesc']} {inc['area']}"
    return (
        f"🚨 ДТП {inc['time']}\n"
        f"{inc['type']}\n"
        f"📍 {inc['location']} — {inc['locdesc']}\n"
        f"🏷️ {inc['area']}\n"
        f"🗺️ {link}"
    )

def main() -> None:
    print("[INFO] CHP notifier v3 started. Center:", COMM_CENTER, "| Interval:", POLL_INTERVAL, "sec")
    seen = load_seen()
    last_day = dt.date.today()
    session = requests.Session()

    while True:
        try:
            if dt.date.today() != last_day:
                seen = {}
                last_day = dt.date.today()
                save_seen(seen)

            html = choose_communications_center(session)
            incidents = parse_incidents_with_links(html)
            collisions = filter_collisions(incidents)

            new_count = 0
            for inc in collisions:
                key = make_key(inc)
                if key in seen:
                    continue

                lat = lon = None
                if FETCH_COORDS and inc.get("details_href"):
                    coords = try_fetch_latlon_from_details(session, inc["details_href"])
                    if coords:
                        lat, lon = coords

                message = format_message(inc, lat, lon)
                send_telegram(message)
                seen[key] = dt.datetime.utcnow().isoformat()
                new_count += 1
            if new_count:
                save_seen(seen)

            print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {COMM_CENTER}: total collisions {len(collisions)}, new {new_count}")
        except KeyboardInterrupt:
            print("\n[INFO] Stopped by user.")
            break
        except Exception as e:
            print("[ERR] loop error:", e)
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
