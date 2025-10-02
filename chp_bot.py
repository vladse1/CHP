#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CHP Traffic -> Telegram notifier (v5)
- Выбирает Communications Center (ASP.NET postback)
- Фильтрует только Collision
- Для каждой строки жмёт её "Details" через __doPostBack и вытаскивает координаты из ссылки рядом с "Lat/Lon:"
- Отправляет маршрут Google Maps по координатам (без текстового поиска)

.env:
  TELEGRAM_TOKEN=...
  TELEGRAM_CHAT_ID=...
  COMM_CENTER=Inland
  POLL_INTERVAL=30
  TYPE_REGEX=Collision
"""

import os
import re
import time
import json
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

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36"
}

# ---------- утилиты хранения ----------
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
        print("[WARN] TELEGRAM_TOKEN/CHAT_ID не заданы. Сообщение не отправлено:\n", text)
        return
    api = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "disable_web_page_preview": True}
    try:
        r = requests.post(api, data=payload, timeout=20)
        if r.status_code != 200:
            print("[ERR] Telegram API status:", r.status_code, r.text[:200])
    except Exception as e:
        print("[ERR] Telegram send error:", e)

# ---------- работа с формой ASP.NET ----------
def extract_form_state(soup: BeautifulSoup) -> Tuple[str, Dict[str, str]]:
    """Возвращает (action_url, payload) со всеми скрытыми полями формы (__VIEWSTATE, и т.д.)"""
    form = soup.find("form")
    if not form:
        raise RuntimeError("Не найден <form> на странице")
    action = form.get("action") or BASE_URL
    payload: Dict[str, str] = {}
    for inp in form.find_all("input"):
        name = inp.get("name")
        if not name:
            continue
        t = (inp.get("type") or "").lower()
        if t in ("submit", "button", "image"):
            continue
        if t in ("checkbox", "radio"):
            if inp.has_attr("checked"):
                payload[name] = inp.get("value", "on")
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
    """Открывает главную, выбирает COMM_CENTER, жмёт OK, возвращает HTML со списком инцидентов."""
    r = session.get(BASE_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    action, payload = extract_form_state(soup)

    # находим селект для "Communications Centers"
    def looks_like_comm_select(sel) -> bool:
        text = (sel.find_previous(string=True) or "") + " " + (sel.find_next(string=True) or "")
        return "communications" in str(text).lower() and "center" in str(text).lower()

    selects = soup.find_all("select")
    if not selects:
        raise RuntimeError("Не найдено ни одного <select> на странице")

    comm_select = None
    for sel in selects:
        if looks_like_comm_select(sel):
            comm_select = sel
            break
    if comm_select is None:
        comm_select = selects[0]

    # выбираем значение центра по имени
    option_value = None
    target = COMM_CENTER.strip().lower()
    for opt in comm_select.find_all("option"):
        label = opt.get_text(strip=True).lower()
        if target in label:
            option_value = opt.get("value") or opt.get_text(strip=True)
            break
    if not option_value:
        raise RuntimeError(f"Не нашёл Communications Center '{COMM_CENTER}'")

    payload[comm_select.get("name")] = option_value

    # жмём OK (submit)
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

# ---------- парсинг таблицы и постбэки Details ----------
def find_incidents_table(soup: BeautifulSoup) -> Optional[BeautifulSoup]:
    for table in soup.find_all("table"):
        header = table.find("tr")
        if not header:
            continue
        headers = [h.get_text(strip=True).lower() for h in header.find_all(["th", "td"])]
        if headers and all(x in headers for x in ["time", "type", "location"]):
            return table
    return None

def parse_incidents_with_postbacks(html: str) -> Tuple[BeautifulSoup, List[Dict[str, str]]]:
    """Возвращает soup и список инцидентов, где для каждой строки извлекает параметры __doPostBack"""
    soup = BeautifulSoup(html, "html.parser")
    table = find_incidents_table(soup)
    if not table:
        return soup, []
    rows = table.find_all("tr")[1:]
    incidents: List[Dict[str, str]] = []
    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 7:
            continue
        # первая колонка: "Details"
        a = cols[0].find("a")
        postback = None
        if a and a.get("href", "").startswith("javascript:__doPostBack"):
            m = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", a["href"])
            if m:
                postback = {"target": m.group(1), "argument": m.group(2)}

        incidents.append({
            "no": cols[1].get_text(strip=True),
            "time": cols[2].get_text(strip=True),
            "type": cols[3].get_text(strip=True),
            "location": cols[4].get_text(strip=True),
            "locdesc": cols[5].get_text(strip=True),
            "area": cols[6].get_text(strip=True),
            "postback": postback
        })
    return soup, incidents

def extract_coords_from_details_html(html: str) -> Optional[Tuple[float, float]]:
    """Из блока Details достаём координаты из ссылки рядом с 'Lat/Lon:'"""
    soup = BeautifulSoup(html, "html.parser")
    label = soup.find(string=re.compile(r"Lat\s*/?\s*Lon", re.IGNORECASE))
    a = None
    if label:
        parent = getattr(label, "parent", None)
        if parent:
            a = parent.find("a", href=True) or parent.find_next("a", href=True)
    if not a:
        # запасной поиск любой ссылки с вида '34.123 -117.456'
        a = soup.find("a", href=True, string=re.compile(r"[-+]?\d+(?:\.\d+)?\s+[-+]?\d+(?:\.\d+)?"))
    if not a:
        return None

    nums = re.findall(r"[-+]?\d+(?:\.\d+)?", a.get_text(strip=True))
    if len(nums) >= 2:
        lat, lon = float(nums[0]), float(nums[1])
        if -90 <= lat <= 90 and -180 <= lon <= 180:
            return (lat, lon)
    return None

def fetch_coords_by_postback(session: requests.Session, action_url: str, base_payload: Dict[str, str],
                             target: str, argument: str) -> Optional[Tuple[float, float]]:
    """Имитация клика по 'Details' через __EVENTTARGET/__EVENTARGUMENT"""
    payload = base_payload.copy()
    payload["__EVENTTARGET"] = target
    payload["__EVENTARGUMENT"] = argument
    post_url = requests.compat.urljoin(BASE_URL, action_url)
    r = session.post(post_url, data=payload, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return extract_coords_from_details_html(r.text)

# ---------- фильтры/формат ----------
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
        if ok and loc_re and not (loc_re.search(x["location"]) or loc_re.search(x["locdesc"])):  # noqa
            ok = False
        if ok:
            result.append(x)
    return result

def make_key(inc: Dict[str, str]) -> str:
    today = dt.date.today().isoformat()
    return f"{today}:{inc['no']}:{inc['time']}:{inc['type']}"

def format_message(inc: Dict[str, str], lat: Optional[float], lon: Optional[float]) -> str:
    parts = [
        f"🚨 ДТП {inc['time']}",
        f"{inc['type']}",
        f"📍 {inc['location']} — {inc['locdesc']}",
        f"🏷️ {inc['area']}",
    ]
    if lat is not None and lon is not None:
        link = f"https://www.google.com/maps/dir/?api=1&destination={lat:.6f},{lon:.6f}&travelmode=driving"
        parts.append(f"🗺️ {link}")
    else:
        parts.append("🗺️ Координаты недоступны")
    return "\n".join(parts)

# ---------- главный цикл ----------
def main() -> None:
    print(f"[INFO] CHP notifier v5 started. Center: {COMM_CENTER} | Interval: {POLL_INTERVAL}s")
    seen = load_seen()
    last_day = dt.date.today()
    session = requests.Session()

    while True:
        try:
            if dt.date.today() != last_day:
                seen = {}
                last_day = dt.date.today()
                save_seen(seen)

            # 1) центр и таблица
            html = choose_communications_center(session)

            # 2) парсим строки + берём состояние формы
            soup, incidents = parse_incidents_with_postbacks(html)
            action_url, base_payload = extract_form_state(soup)

            # 3) фильтруем только Collision
            collisions = filter_collisions(incidents)

            new_count = 0
            for inc in collisions:
                key = make_key(inc)
                if key in seen:
                    continue

                lat = lon = None
                if inc.get("postback"):
                    coords = fetch_coords_by_postback(
                        session,
                        action_url,
                        base_payload,
                        inc["postback"]["target"],
                        inc["postback"]["argument"],
                    )
                    if coords:
                        lat, lon = coords

                # Отправляем (если координаты не нашлись, линк не будет добавлен)
                send_telegram(format_message(inc, lat, lon))
                seen[key] = dt.datetime.utcnow().isoformat()
                new_count += 1

            if new_count:
                save_seen(seen)

            print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {COMM_CENTER}: rows={len(incidents)}, collisions={len(collisions)}, new={new_count}")
        except KeyboardInterrupt:
            print("\n[INFO] Stopped by user.")
            break
        except Exception as e:
            print("[ERR] loop error:", e)
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
