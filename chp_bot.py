#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CHP Traffic -> Telegram notifier (v5.6, location+count only)
- Выбирает Communications Center и парсит таблицу
- По каждой строке делает postback "Details", достаёт координаты и ПОЛНЫЙ блок Detail Information
- Сообщение содержит:
  Шапка → (опционально) Расположение/Машины → Маршрут (URL) → Detail Information (blockquote)
  * Расположение: только "правая обочина" или "CD" или "съезд"
  * Машины: число (по X VEH / SOLO / VS)
  * Если ни расположения, ни числа — секция не выводится вообще
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

# ---------- конфиг из .env ----------
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

BASE_URL = os.getenv("CHP_URL", "https://cad.chp.ca.gov/Traffic.aspx")
COMM_CENTER = os.getenv("COMM_CENTER", "Inland")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# добавь Hit & Run так: TYPE_REGEX=(Collision|Hit\s*(?:&|and)\s*Run)
TYPE_REGEX = os.getenv("TYPE_REGEX", r"Collision")
AREA_REGEX = os.getenv("AREA_REGEX", r"")
LOCATION_REGEX = os.getenv("LOCATION_REGEX", r"")

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))
SEEN_FILE = os.getenv("SEEN_FILE", "seen.json")
MAX_DETAIL_CHARS = int(os.getenv("MAX_DETAIL_CHARS", "2500"))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36"
}

# ---------- Telegram ----------
def send_telegram(text: str) -> None:
    """Отправка сообщения с parse_mode=HTML (без инлайн-кнопок)."""
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
    r = requests.post(api, data=payload, timeout=20)
    if r.status_code != 200:
        print("[ERR] Telegram API:", r.status_code, r.text[:400])

# ---------- хранение seen ----------
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

# ---------- ASP.NET helpers ----------
def extract_form_state(soup: BeautifulSoup) -> Tuple[str, Dict[str, str]]:
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
    r = session.get(BASE_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    action, payload = extract_form_state(soup)

    def looks_like_comm_select(sel) -> bool:
        text = (sel.find_previous(string=True) or "") + " " + (sel.find_next(string=True) or "")
        return "communications" in str(text).lower() and "center" in str(text).lower()

    selects = soup.find_all("select")
    if not selects:
        raise RuntimeError("Не найдено ни одного <select> на странице")
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

    # submit
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

# ---------- таблица и postback ----------
def find_incidents_table(soup: BeautifulSoup):
    for table in soup.find_all("table"):
        header = table.find("tr")
        if not header:
            continue
        headers = [h.get_text(strip=True).lower() for h in header.find_all(["th", "td"])]
        if headers and all(x in headers for x in ["time", "type", "location"]):
            return table
    return None

def parse_incidents_with_postbacks(html: str) -> Tuple[BeautifulSoup, List[Dict[str, str]]]:
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

# ---------- Details: координаты + Detail Information ----------
def extract_coords_from_details_html(soup: BeautifulSoup) -> Optional[Tuple[float, float]]:
    label = soup.find(string=re.compile(r"Lat\s*/?\s*Lon", re.IGNORECASE))
    a = None
    if label:
        parent = getattr(label, "parent", None)
        if parent:
            a = parent.find("a", href=True) or parent.find_next("a", href=True)
    if not a:
        a = soup.find("a", href=True, string=re.compile(r"[-+]?\d+(?:\.\d+)?\s+[-+]?\d+(?:\.\d+)?"))
    if not a:
        return None
    nums = re.findall(r"[-+]?\d+(?:\.\d+)?", a.get_text(strip=True))
    if len(nums) >= 2:
        lat, lon = float(nums[0]), float(nums[1])
        if -90 <= lat <= 90 and -180 <= lon <= 180:
            return (lat, lon)
    return None

def extract_detail_lines(soup: BeautifulSoup) -> Optional[List[str]]:
    """Сырые строки блока 'Detail Information'."""
    flat = soup.get_text("\n", strip=True)

    m_start = re.search(r"(?im)^Detail Information$", flat)
    if not m_start:
        return None
    start = m_start.end()

    m_end = re.search(r"(?im)^(Unit Information|Close)$", flat[start:])
    end = start + (m_end.start() if m_end else len(flat) - start)

    block = flat[start:end]
    lines = []
    for raw in block.splitlines():
        s = " ".join(raw.split()).strip()
        if s:
            lines.append(s)
    return lines or None

def extract_detail_information_block_from_lines(lines: List[str]) -> str:
    """
    Делаем аккуратные строки "8:29 AM: текст" и собираем цитату (<blockquote> с \n).
    Без <br>, чтобы Telegram не ругался.
    """
    clean = condense_detail_lines(lines)
    if not clean:
        return "<blockquote>No details</blockquote>"

    acc = ""
    for ln in clean:
        piece = html.escape(ln)
        candidate = acc + ("" if not acc else "\n") + piece
        if len(candidate) > MAX_DETAIL_CHARS:
            acc += ("\n" if acc else "") + "… (truncated)"
            break
        acc = candidate

    return f"<blockquote>{acc}</blockquote>"

def fetch_details_by_postback(session: requests.Session, action_url: str, base_payload: Dict[str, str],
                              target: str, argument: str) -> Tuple[Optional[Tuple[float, float]], Optional[str], Optional[List[str]]]:
    """Жмём 'Details' и возвращаем (coords, details_block_html, detail_lines_raw)."""
    payload = base_payload.copy()
    payload["__EVENTTARGET"] = target
    payload["__EVENTARGUMENT"] = argument
    post_url = requests.compat.urljoin(BASE_URL, action_url)
    r = session.post(post_url, data=payload, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    coords = extract_coords_from_details_html(soup)
    lines = extract_detail_lines(soup)
    details_block_html = extract_detail_information_block_from_lines(lines) if lines else None
    return coords, details_block_html, lines

# ---------- ВЫЧИСЛЕНИЕ: расположение + число машин ----------
BARRIER_WORDS = {"BARRIER", "GUARDRAIL", "FENCE", "DEBRIS", "ANIMAL", "DEER", "TREE", "POLE", "SIGN"}

def parse_location_and_count(detail_lines: Optional[List[str]]) -> Tuple[Optional[str], Optional[int]]:
    """
    Возвращает (location_label, veh_count)
      location_label ∈ {"правая обочина", "CD", "съезд"} или None
      veh_count ∈ {1,2,...} или None
    """
    if not detail_lines:
        return None, None

    text_up = " ".join(detail_lines).upper()

    # --- РАСПОЛОЖЕНИЕ ---
    # правая обочина
    loc = None
    if re.search(r"\bRS\b|\bRIGHT SHOULDER\b", text_up):
        loc = "правая обочина"
    # CD (center divider)
    if re.search(r"\bCD\b|\bCENTER DIVIDER\b", text_up):
        loc = "CD"
    # съезд (on/off ramp, exit)
    if re.search(r"\bON[- ]?RAMP\b|\bOFF[- ]?RAMP\b|\bEXIT\b", text_up):
        loc = "съезд"

    # --- КОЛ-ВО МАШИН ---
    # X VEH / SOLO VEH
    nums = [int(n) for n in re.findall(r"\b(\d{1,2})\s*VEH\b", text_up)]
    veh_count = max(nums) if nums else None
    if veh_count is None and "SOLO VEH" in text_up:
        veh_count = 1

    # "XXX VS YYY" — считаем как 2 ТС, если обе стороны не из BARRIER_WORDS
    if veh_count is None and re.search(r"\bVS\b", text_up):
        # возьмём пару первых слов по шаблону "... VS ..."
        m = re.search(r"\b([A-Z0-9/&\- ]{2,30}?)\s+VS\s+([A-Z0-9/&\- ]{2,30}?)\b", text_up)
        if m:
            left = m.group(1).strip().split()[0]
            right = m.group(2).strip().split()[0]
            if left not in BARRIER_WORDS and right not in BARRIER_WORDS:
                veh_count = 2

    return loc, veh_count

# --- Сгущение Detail Information до "HH:MM AM/PM: текст" ---
TIME_RE = re.compile(r'^\d{1,2}:\d{2}\s*(?:AM|PM)$', re.IGNORECASE)
FOOTER_PATTERNS = [
    r'^Click on Details for additional information\.', r'^Your screen will refresh in \d+ seconds\.$',
    r'^Contact Us$', r'^CHP Home Page$', r'^CHP Mobile Traffic$', r'^\|$'
]
FOOTER_RE = re.compile("|".join(FOOTER_PATTERNS), re.IGNORECASE)

def condense_detail_lines(lines: list[str]) -> list[str]:
    """
    Из последовательностей вида:
        8:29 AM
        2
        [4] 2ND VEH BLK SD BLKG #3
    делаем строки:
        8:29 AM: 2ND VEH BLK SD BLKG #3
    Футер/мусор отбрасываем.
    """
    out = []
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        # пропускаем футер/разделители
        if not line or FOOTER_RE.search(line):
            i += 1
            continue

        # если это строка времени — собираем запись
        if TIME_RE.match(line):
            t = line
            j = i + 1

            # пропустить возможный порядковый номер ("1", "2", ...)
            if j < len(lines) and re.match(r'^\d+$', lines[j].strip()):
                j += 1

            # взять первую содержательную строку после времени
            desc = None
            if j < len(lines):
                cand = lines[j].strip()
                # удалить префикс вида "[4] "
                cand = re.sub(r'^\[\d+\]\s*', '', cand)
                # игнорировать футер и пустое
                if cand and not FOOTER_RE.search(cand):
                    desc = cand

            if desc:
                out.append(f"{t}: {desc}")
                i = j + 1
                continue
            else:
                # нет текста — просто пропускаем время
                i += 1
                continue

        # если это не время — пропустить
        i += 1

    return out

# ---------- фильтры/формат ----------
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

def format_message(inc: Dict[str, str],
                   latlon: Optional[Tuple[float, float]],
                   details_block: Optional[str],
                   loc_label: Optional[str],
                   veh_count: Optional[int]) -> str:
    # Шапка
    title = (
        f"🚨 ДТП {html.escape(inc['time'])}\n"
        f"{html.escape(inc['type'])}\n"
        f"📍 {html.escape(inc['location'])} — {html.escape(inc['locdesc'])}\n"
        f"🏷️ {html.escape(inc['area'])}"
    )

    # Раздел «Расположение/Машины» — только если есть что сказать
    lines = []
    if loc_label:
        lines.append(loc_label)
    if veh_count is not None:
        lines.append(f"{veh_count} ТС")
    if lines:
        title += "\n\n<b>📌 Расположение / Машины:</b>\n" + ", ".join(lines)

    # Маршрут
    if latlon:
        lat, lon = latlon
        url = f"https://www.google.com/maps/dir/?api=1&destination={lat:.6f},{lon:.6f}&travelmode=driving"
        title += f"\n\n<b>🗺️ Маршрут:</b>\n{url}"
    else:
        title += "\n\n<b>🗺️ Маршрут:</b>\nКоординаты недоступны"

    # Полный Detail Information
    if details_block:
        title += f"\n\n<b>📝 Detail Information:</b>\n{details_block}"

    return title

# ---------- главный цикл ----------
def main() -> None:
    print(f"[INFO] CHP notifier v5.6 started. Center: {COMM_CENTER} | Interval: {POLL_INTERVAL}s")
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

            filtered = filter_collisions(incidents)

            new_count = 0
            for inc in filtered:
                key = make_key(inc)
                if key in seen:
                    continue

                latlon = None
                details_block = None
                detail_lines = None
                if inc.get("postback"):
                    latlon, details_block, detail_lines = fetch_details_by_postback(
                        session, action_url, base_payload,
                        inc["postback"]["target"], inc["postback"]["argument"]
                    )

                loc_label, veh_count = parse_location_and_count(detail_lines)
                text = format_message(inc, latlon, details_block, loc_label, veh_count)
                send_telegram(text)
                seen[key] = dt.datetime.utcnow().isoformat()
                new_count += 1

            if new_count:
                save_seen(seen)

            print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {COMM_CENTER}: rows={len(incidents)}, matched={len(filtered)}, new={new_count}")
        except KeyboardInterrupt:
            print("\n[INFO] Stopped by user."); break
        except Exception as e:
            print("[ERR] loop error:", e)
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
