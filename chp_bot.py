#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CHP Traffic -> Telegram notifier (v5.2)
- Выбирает Communications Center (ASP.NET postback)
- Фильтрует по TYPE_REGEX (по умолчанию Collision; можно задать Hit&Run через .env)
- Для каждой строки делает postback по "Details"
- Из HTML Details:
  * достаёт координаты из ссылки рядом с "Lat/Lon:"
  * забирает ВЕСЬ блок "Detail Information" (до "Unit Information"/"Close")
- Отправляет: маршрут Google Maps по координатам + блок "Detail Information" как цитату

ENV:
  TELEGRAM_TOKEN=...
  TELEGRAM_CHAT_ID=...
  COMM_CENTER=Inland
  POLL_INTERVAL=30
  TYPE_REGEX=(Collision|Hit\s*(?:&|and)\s*Run)   # пример
  AREA_REGEX=...
  LOCATION_REGEX=...
  SEEN_FILE=seen.json
  MAX_DETAIL_CHARS=2500   # чтобы не упереться в лимит 4096 у Telegram
"""

import os
import re
import time
import json
import html
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
MAX_DETAIL_CHARS = int(os.getenv("MAX_DETAIL_CHARS", "2500"))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36"
}

# -------- Telegram ----------
def send_telegram(text: str, button_url: Optional[str] = None) -> None:
    """Отправка с parse_mode=HTML. Можно прикрепить кнопку."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("[WARN] TELEGRAM_TOKEN/CHAT_ID не заданы. Не отправляю:\n", text)
        return
    api = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
        "parse_mode": "HTML",
    }
    if button_url:
        kb = {"inline_keyboard": [[{"text": "🗺 Открыть маршрут", "url": button_url}]]}
        payload["reply_markup"] = json.dumps(kb := kb)  # noqa
    try:
        r = requests.post(api, data=payload, timeout=20)
        if r.status_code != 200:
            print("[ERR] Telegram API:", r.status_code, r.text[:200])
    except Exception as e:
        print("[ERR] Telegram send error:", e)

# -------- Seen store ----------
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

# -------- ASP.NET form helpers ----------
def extract_form_state(soup: BeautifulSoup):
    form = soup.find("form")
    if not form:
        raise RuntimeError("Не найден <form>")
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
    r = session.get(BASE_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    action, payload = extract_form_state(soup)

    def looks_like_comm_select(sel) -> bool:
        txt = (sel.find_previous(string=True) or "") + " " + (sel.find_next(string=True) or "")
        return "communications" in str(txt).lower() and "center" in str(txt).lower()

    selects = soup.find_all("select")
    if not selects:
        raise RuntimeError("Нет <select> на странице")

    comm_select = next((s for s in selects if looks_like_comm_select(s)), selects[0])

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

    form = soup.find("form")
    submit_name = submit_value = None
    for btn in form.find_all("input", {"type": "submit"}):
        val = (btn.get("value") or "").strip().lower()
        if val in ("ok", "submit", "go"):
            submit_name = btn.get("name"); submit_value = btn.get("value"); break
    if not submit_name:
        btn = form.find("input", {"type": "submit"})
        if btn:
            submit_name = btn.get("name"); submit_value = btn.get("value", "OK")
    if submit_name:
        payload[submit_name] = submit_value

    post_url = requests.compat.urljoin(BASE_URL, action)
    r2 = session.post(post_url, data=payload, headers=HEADERS, timeout=30)
    r2.raise_for_status()
    return r2.text

# -------- table parsing & postback ----------
def find_incidents_table(soup: BeautifulSoup):
    for table in soup.find_all("table"):
        header = table.find("tr")
        if not header: continue
        headers = [h.get_text(strip=True).lower() for h in header.find_all(["th", "td"])]
        if headers and all(x in headers for x in ["time", "type", "location"]):
            return table
    return None

def parse_incidents_with_postbacks(html: str):
    soup = BeautifulSoup(html, "html.parser")
    table = find_incidents_table(soup)
    if not table: return soup, []
    rows = table.find_all("tr")[1:]
    incidents = []
    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 7: continue
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

# -------- Details parsing ----------
def extract_coords_from_details_html(soup: BeautifulSoup) -> Optional[Tuple[float, float]]:
    label = soup.find(string=re.compile(r"Lat\s*/?\s*Lon", re.IGNORECASE))
    a = None
    if label:
        parent = getattr(label, "parent", None)
        if parent:
            a = parent.find("a", href=True) or parent.find_next("a", href=True)
    if not a:
        a = soup.find("a", href=True, string=re.compile(r"[-+]?\d+(?:\.\d+)?\s+[-+]?\d+(?:\.\d+)?"))
    if not a: return None
    nums = re.findall(r"[-+]?\d+(?:\.\d+)?", a.get_text(strip=True))
    if len(nums) >= 2:
        lat, lon = float(nums[0]), float(nums[1])
        if -90 <= lat <= 90 and -180 <= lon <= 180:
            return (lat, lon)
    return None

def extract_detail_information_block(soup: BeautifulSoup) -> Optional[str]:
    """
    Возвращает ПОЛНЫЙ блок 'Detail Information' в чистом виде (строки, как на сайте).
    Форматирует как HTML <blockquote> с <br> между строками.
    """
    flat = soup.get_text("\n", strip=True)
    # Начало блока
    m_start = re.search(r"(?im)^Detail Information$", flat)
    if not m_start:
        return None
    start = m_start.end()

    # Конец блока — перед "Unit Information" или "Close"
    m_end = re.search(r"(?im)^(Unit Information|Close)$", flat[start:])
    end = start + (m_end.start() if m_end else len(flat) - start)

    block = flat[start:end]
    # нормализуем: убираем пустые и повторяющиеся пробелы внутри строк
    lines = []
    for raw in block.splitlines():
        s = " ".join(raw.split()).strip()
        if s:
            lines.append(s)

    if not lines:
        return None

    # Собираем HTML blockquote; режем по лимиту, чтобы не превысить 4096 символов
    acc = ""
    for ln in lines:
        candidate = acc + ("" if not acc else "<br>") + html.escape(ln)
        if len(candidate) > MAX_DETAIL_CHARS:
            acc += "<br>… (truncated)"
            break
        acc = candidate
    return f"<blockquote>{acc}</blockquote>"

def fetch_details_by_postback(session: requests.Session, action_url: str, base_payload: Dict[str, str],
                              target: str, argument: str) -> Tuple[Optional[Tuple[float, float]], Optional[str]]:
    payload = base_payload.copy()
    payload["__EVENTTARGET"] = target
    payload["__EVENTARGUMENT"] = argument
    post_url = requests.compat.urljoin(BASE_URL, action_url)
    r = session.post(post_url, data=payload, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    coords = extract_coords_from_details_html(soup)
    details_html = extract_detail_information_block(soup)
    return coords, details_html

# -------- filters/format ----------
def filter_collisions(incidents: List[Dict[str, str]]) -> List[Dict[str, str]]:
    type_re = re.compile(TYPE_REGEX, re.IGNORECASE) if TYPE_REGEX else None
    area_re = re.compile(AREA_REGEX, re.IGNORECASE) if AREA_REGEX else None
    loc_re = re.compile(LOCATION_REGEX, re.IGNORECASE) if LOCATION_REGEX else None
    result = []
    for x in incidents:
        ok = True
        if type_re and not type_re.search(x["type"]): ok = False
        if ok and area_re and not area_re.search(x["area"]): ok = False
        if ok and loc_re and not (loc_re.search(x["location"]) or loc_re.search(x["locdesc"])): ok = False
        if ok: result.append(x)
    return result

def make_key(inc: Dict[str, str]) -> str:
    today = dt.date.today().isoformat()
    return f"{today}:{inc['no']}:{inc['time']}:{inc['type']}"

def format_message(inc: Dict[str, str], latlon: Optional[Tuple[float, float]], details_block: Optional[str]) -> Tuple[str, Optional[str]]:
    title = (
        f"🚨 ДТП {html.escape(inc['time'])}\n"
        f"{html.escape(inc['type'])}\n"
        f"📍 {html.escape(inc['location'])} — {html.escape(inc['locdesc'])}\n"
        f"🏷️ {html.escape(inc['area'])}"
    )
    route_url = None
    if latlon:
        lat, lon = latlon
        route_url = f"https://www.google.com/maps/dir/?api=1&destination={lat:.6f},{lon:.6f}&travelmode=driving"
        title += f"\n🗺️ <a href=\"{route_url}\">Открыть маршрут</a>"
    else:
        title += "\n🗺️ Координаты недоступны"

    if details_block:
        title += f"\n📝 <b>Detail Information</b>:\n{details_block}"
    return title, route_url

# -------- main loop ----------
def main() -> None:
    print(f"[INFO] CHP notifier v5.2 started. Center: {COMM_CENTER} | Interval: {POLL_INTERVAL}s")
    seen = load_seen()
    last_day = dt.date.today()
    session = requests.Session()

    while True:
        try:
            if dt.date.today() != last_day:
                seen = {}; last_day = dt.date.today(); save_seen(seen)

            html = choose_communications_center(session)
            soup, incidents = parse_incidents_with_postbacks(html)
            action_url, base_payload = extract_form_state(soup)

            collisions = filter_collisions(incidents)

            new_count = 0
            for inc in collisions:
                key = make_key(inc)
                if key in seen:
                    continue

                latlon = None
                details_block = None
                if inc.get("postback"):
                    latlon, details_block = fetch_details_by_postback(
                        session, action_url, base_payload,
                        inc["postback"]["target"], inc["postback"]["argument"]
                    )

                text, url = format_message(inc, latlon, details_block)
                send_telegram(text)  # если хочешь кнопку: send_telegram(text, button_url=url)
                seen[key] = dt.datetime.utcnow().isoformat()
                new_count += 1

            if new_count:
                save_seen(seen)

            print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {COMM_CENTER}: rows={len(incidents)}, collisions={len(collisions)}, new={new_count}")
        except KeyboardInterrupt:
            print("\n[INFO] Stopped by user."); break
        except Exception as e:
            print("[ERR] loop error:", e)
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()

