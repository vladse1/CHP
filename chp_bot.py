#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CHP Telegram Bot (geo-filter + dedupe + smart summary)
Версия v9-geo-merge-30m

Что делает бот:
- заходит на https://cad.chp.ca.gov/Traffic.aspx
- выбирает Communications Center (например Inland)
- парсит таблицу инцидентов
- для каждого инцидента запрашивает Details через __doPostBack
- достаёт координаты, детали, ключевые факты
- шлёт сообщение в Telegram ОДИН РАЗ на инцидент
- потом редактирует то же сообщение, если инцидент обновляется
- помечает как закрытый, когда пропал из фида

НОВОЕ:
1. Геозона:
   мы отсекаем аварии, которые не попадают в прямоугольник:
   LON_MIN = -117.793774
   LAT_MIN = 33.842413
   LON_MAX = -117.079663
   LAT_MAX = 34.164597

   Если координат нет -> мы тоже пропускаем.
   Это значит:
   - не создаём Telegram-пост,
   - не сохраняем этот инцидент в state,
   - не считаем его для закрытий и т.д.
   Т.е. бот вообще делает вид, что этого инцидента нет.

2. Merge по координатам:
   Если появляется новый инцидент (другой номер CHP, другая area, иногда FSP),
   но координаты очень близко (< ~100 м) к уже существующему активному инциденту,
   и последний апдейт того активного инцидента был не позже чем 30 минут назад,
   то:
     - НЕ создаём новое сообщение в Telegram,
     - а считаем это тем же кейсом,
     - редактируем СТАРОЕ сообщение,
     - и делаем алиас: новый ключ -> тот же message_id.
   То есть оператор не получает два почти одинаковых алерта подряд про одно и то же место.

   Это борется с ситуацией Riverside vs Riverside FSP.

3. MISSES_TO_CLOSE = 4 (было 2):
   Прежде чем объявить "Инцидент закрыт CHP", нужно, чтобы инцидент отсутствовал
   в списке 4 цикла подряд. Это уменьшает ложные "закрыто" -> "ой снова вернулся".

4. SOLO предупреждение:
   Если в деталях встречается SOLO / SOLO VEH / SOLO VEH TC / SOLO VEH INTO CD и т.п.,
   в начале сообщения добавляется жирная строка:
   <b>❗ Соло ДТП, не ехать</b>

5. "Автоматическое уведомление":
   Если в деталях видно, что это просто iPhone auto-crash alert / no response from caller:
     - "NO RESP FRM CALLER"
     - "IPHONE WATCH TC NOTIFICATION"
     - "IPHONE TC NOTIFICATION"
     - "CAN HEAR TRAFFIC IN BACKGROUND"
   то в начало сообщения добавляется:
   <b>📱 Автоматическое уведомление — ждём обновления информации</b>

   Если присутствуют и SOLO, и автоуведомление, показываем обе строки, SOLO первой.

6. Чистый блок "📌 Расположение / Машины":
   - сначала локация (обочина / съезд / полоса / HOV),
   - потом типы ТС (фура, мото, пикап, грузовик),
   - потом службы (эвакуатор, CHP, медики),
   - без повторов, без лишнего мусора, без слов "блокировка" и без кодов.

7. Сигнатура инцидента теперь включает дату дня (UTC date), чтобы один и тот же номер
   (например "0300") завтра не считался тем же инцидентом.

8. Очистка старых записей state:
   - если инциденту больше 24 часов (по last_seen или first_seen),
     мы его выбрасываем из памяти при следующем сохранении.
   Это не даст "вечным" id мешать, и не будет залипаний.

ENV (.env) переменные смотри после кода.
"""

import os
import re
import time
import json
import html
import math
import random
import logging
import hashlib
import datetime as dt
from typing import List, Dict, Optional, Tuple

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------
# ENV / CONFIG
# ---------------------------------------------------------------------

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

BASE_URL = os.getenv("CHP_URL", "https://cad.chp.ca.gov/Traffic.aspx")
COMM_CENTER = os.getenv("COMM_CENTER", "Inland")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# фильтрация по типу (Collision, Hit&Run)
TYPE_REGEX = os.getenv("TYPE_REGEX", r"(Collision|Hit\s*(?:&|and)\s*Run)")
# опциональные доп. фильтры по area/location (обычно пусто)
AREA_REGEX = os.getenv("AREA_REGEX", r"")
LOCATION_REGEX = os.getenv("LOCATION_REGEX", r"")

# интервал опроса в секундах
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))

# сколько циклов должен пропасть инцидент, чтобы мы объявили "закрыт"
MISSES_TO_CLOSE = int(os.getenv("MISSES_TO_CLOSE", "4"))

# максимум символов детального блока ДО динамического обрезания
MAX_DETAIL_CHARS_BASE = int(os.getenv("MAX_DETAIL_CHARS", "2500"))

# файл состояния (seen.json)
SEEN_FILE = os.getenv("SEEN_FILE", "seen.json")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Telegram лимит
TG_HARD_LIMIT = 4096

# --- Геозона (жёстко по ТЗ) ---
# прямоугольник: только в нём отправляем инциденты.
# Если координат нет — мы игнорируем инцидент.
GEO_ENABLED = True
LAT_MIN = 33.842413
LAT_MAX = 34.164597
LON_MIN = -117.793774
LON_MAX = -117.079663
DROP_IF_NO_COORDS = True  # если нет координат, не шлём вообще

# merge по координатам: окно по времени и расстояние
MERGE_TIME_WINDOW_MIN = 30          # минут
MERGE_RADIUS_METERS = 100.0         # метров

# ---------------------------------------------------------------------
# logging
# ---------------------------------------------------------------------

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s: %(message)s"
)
log = logging.getLogger("chp_bot")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36"
}

# ---------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------

def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def utc_iso() -> str:
    return now_utc().isoformat()

def minutes_between(t1_iso: str, t2_iso: str) -> float:
    try:
        a = dt.datetime.fromisoformat(t1_iso)
        b = dt.datetime.fromisoformat(t2_iso)
        if a.tzinfo is None:
            a = a.replace(tzinfo=dt.timezone.utc)
        if b.tzinfo is None:
            b = b.replace(tzinfo=dt.timezone.utc)
        return abs((b - a).total_seconds() / 60.0)
    except Exception:
        return 999999.0

def older_than_hours(ts_iso: str, hours: float) -> bool:
    try:
        t = dt.datetime.fromisoformat(ts_iso)
        if t.tzinfo is None:
            t = t.replace(tzinfo=dt.timezone.utc)
        return (now_utc() - t).total_seconds() > hours * 3600.0
    except Exception:
        return False

# ---------------------------------------------------------------------
# Geo helpers
# ---------------------------------------------------------------------

def in_geofence(latlon: Optional[Tuple[float,float]]) -> bool:
    """
    Возвращает True только если инцидент внутри геобокса.
    Если координат нет и DROP_IF_NO_COORDS=True -> False.
    """
    if not GEO_ENABLED:
        return True
    if not latlon:
        return (not DROP_IF_NO_COORDS)
    lat, lon = latlon
    if lat is None or lon is None:
        return (not DROP_IF_NO_COORDS)
    inside = (LAT_MIN <= lat <= LAT_MAX) and (LON_MIN <= lon <= LON_MAX)
    return inside

def haversine_m(lat1, lon1, lat2, lon2) -> float:
    """
    Расстояние между двумя точками (lat/lon в градусах) в метрах.
    """
    R = 6371000.0
    from math import radians, sin, cos, sqrt, asin
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1))*cos(radians(lat2))*sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    return R * c

def is_close_coords(latlon_a: Tuple[float,float],
                    latlon_b: Tuple[float,float],
                    radius_m: float) -> bool:
    la, loa = latlon_a
    lb, lob = latlon_b
    dist = haversine_m(la, loa, lb, lob)
    return dist <= radius_m

# ---------------------------------------------------------------------
# Requests with retry/backoff
# ---------------------------------------------------------------------

RETRY_MAX_ATTEMPTS = 5
RETRY_BASE_DELAY = 0.5  # sec
RETRY_MAX_DELAY = 10.0  # sec

def should_retry(resp: Optional[requests.Response], err: Optional[Exception]) -> bool:
    if err is not None:
        # network/timeout
        return True
    if resp is None:
        return True
    if resp.status_code >= 500:
        return True
    if resp.status_code in (403, 429):
        return True
    return False

def request_with_retry(method: str, url: str, session: requests.Session, **kwargs) -> requests.Response:
    attempt = 0
    while True:
        attempt += 1
        err = None
        resp = None
        try:
            resp = session.request(method, url, headers=HEADERS, timeout=30, **kwargs)
            if not should_retry(resp, None):
                return resp
            log.debug(f"HTTP {resp.status_code} -> retryable for {url}")
        except requests.RequestException as e:
            err = e
            log.debug(f"Request error (attempt {attempt}) {e}")
        if attempt >= RETRY_MAX_ATTEMPTS:
            if err:
                raise err
            else:
                resp.raise_for_status()
        back = min(
            RETRY_MAX_DELAY,
            RETRY_BASE_DELAY * (2 ** (attempt - 1))
        )
        jitter = random.uniform(0, 0.5 * back)
        sleep_for = back + jitter
        log.debug(f"Backoff {sleep_for:.2f}s before retry #{attempt+1} {url}")
        time.sleep(sleep_for)

# ---------------------------------------------------------------------
# Telegram helpers
# ---------------------------------------------------------------------

def safe_len_for_telegram(text: str) -> int:
    return len(text)

def tg_send(text: str, chat_id: Optional[str] = None) -> Optional[int]:
    chat_id = (chat_id or TELEGRAM_CHAT_ID).strip()
    if not TELEGRAM_TOKEN or not chat_id:
        log.warning("TELEGRAM_TOKEN/CHAT_ID не заданы. Сообщение не отправлено.")
        return None
    api = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
        "parse_mode": "HTML"
    }
    r = requests.post(api, data=payload, timeout=20)
    if r.status_code != 200:
        log.error("Telegram send %s %s", r.status_code, r.text[:400])
        return None
    try:
        return int(r.json()["result"]["message_id"])
    except Exception:
        return None

def tg_edit(message_id: int, text: str, chat_id: Optional[str] = None) -> bool:
    chat_id = (chat_id or TELEGRAM_CHAT_ID).strip()
    if not TELEGRAM_TOKEN or not chat_id or not message_id:
        return False
    api = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/editMessageText"
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "disable_web_page_preview": True,
        "parse_mode": "HTML"
    }
    r = requests.post(api, data=payload, timeout=20)
    if r.status_code != 200:
        log.error("Telegram edit %s %s", r.status_code, r.text[:400])
        return False
    return True

# ---------------------------------------------------------------------
# State load/save with cleanup
# ---------------------------------------------------------------------

def load_state() -> Dict[str, dict]:
    try:
        with open(SEEN_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return {}

def save_state(state: Dict[str, dict]) -> None:
    # чистка инцидентов старше 24ч
    to_del = []
    for k, st in state.items():
        if not isinstance(st, dict):
            continue
        ts = st.get("last_seen") or st.get("first_seen")
        if ts and older_than_hours(ts, 24.0):
            to_del.append(k)
    for k in to_del:
        del state[k]

    with open(SEEN_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

# ---------------------------------------------------------------------
# ASP.NET form helpers
# ---------------------------------------------------------------------

def extract_form_state(soup: BeautifulSoup):
    form = soup.find("form")
    if not form:
        raise RuntimeError("Не найден <form> на странице")
    action = form.get("action") or BASE_URL
    payload = {}
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
        n = sel.get("name")
        if not n:
            continue
        opt = sel.find("option", selected=True) or sel.find("option")
        if opt:
            payload[n] = opt.get("value", opt.get_text(strip=True))
    for ta in form.find_all("textarea"):
        n = ta.get("name")
        if not n:
            continue
        payload[n] = ta.get_text()
    return action, payload

def choose_communications_center(session: requests.Session, center_name: str) -> str:
    r = request_with_retry("GET", BASE_URL, session)
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
    target = center_name.strip().lower()
    for opt in comm_select.find_all("option"):
        label = opt.get_text(strip=True).lower()
        if target in label:
            option_value = opt.get("value") or opt.get_text(strip=True)
            break
    if not option_value:
        raise RuntimeError(f"Не нашёл Communications Center '{center_name}'")
    payload[comm_select.get("name")] = option_value

    # submit
    form = soup.find("form")
    submit_name = submit_value = None
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
    r2 = request_with_retry("POST", post_url, session, data=payload)
    return r2.text

# ---------------------------------------------------------------------
# Incidents table parsing
# ---------------------------------------------------------------------

def find_incidents_table(soup: BeautifulSoup):
    for table in soup.find_all("table"):
        header = table.find("tr")
        if not header:
            continue
        headers = [h.get_text(strip=True).lower() for h in header.find_all(["th", "td"])]
        if headers and all(x in headers for x in ["time", "type", "location"]):
            return table
    return None

def parse_incidents_with_postbacks(html_text: str):
    soup = BeautifulSoup(html_text, "html.parser")
    table = find_incidents_table(soup)
    if not table:
        return soup, []
    rows = table.find_all("tr")[1:]
    incs = []
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
        incs.append({
            "no": cols[1].get_text(strip=True),
            "time": cols[2].get_text(strip=True),
            "type": cols[3].get_text(strip=True),
            "location": cols[4].get_text(strip=True),
            "locdesc": cols[5].get_text(strip=True),
            "area": cols[6].get_text(strip=True),
            "postback": postback
        })
    return soup, incs

# ---------------------------------------------------------------------
# Details parsing
# ---------------------------------------------------------------------

TIME_RE = re.compile(r'^\d{1,2}:\d{2}\s*(?:AM|PM)$', re.I)
FOOTER_PATTERNS = [
    r'^Click on Details for additional information\.',
    r'^Your screen will refresh in \d+ seconds\.$',
    r'^Contact Us$', r'^CHP Home Page$', r'^CHP Mobile Traffic$', r'^\|$'
]
FOOTER_RE = re.compile("|".join(FOOTER_PATTERNS), re.I)

def extract_coords_from_details_html(soup: BeautifulSoup) -> Optional[Tuple[float, float]]:
    label = soup.find(string=re.compile(r"Lat\s*/?\s*Lon", re.IGNORECASE))
    a = None
    if label:
        par = getattr(label, "parent", None)
        if par:
            a = par.find("a", href=True) or par.find_next("a", href=True)
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
    flat = soup.get_text("\n", strip=True)
    m_start = re.search(r"(?im)^Detail Information$", flat)
    if not m_start:
        return None
    start = m_start.end()
    m_end = re.search(r"(?im)^(Unit Information|Close)$", flat[start:])
    end = start + (m_end.start() if m_end else len(flat)-start)
    block = flat[start:end]
    lines = []
    for raw in block.splitlines():
        s = " ".join(raw.split()).strip()
        if s:
            lines.append(s)
    return lines or None

def condense_detail_lines(lines: List[str]) -> List[str]:
    out = []
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if not line or FOOTER_RE.search(line):
            i += 1
            continue
        if TIME_RE.match(line):
            t = line
            j = i + 1
            if j < len(lines) and re.match(r'^\d+$', lines[j].strip()):
                j += 1
            desc = None
            if j < len(lines):
                cand = lines[j].strip()
                cand = re.sub(r'^\[\d+\]\s*', '', cand)
                if cand and not FOOTER_RE.search(cand):
                    desc = cand
            if desc:
                out.append(f"{t}: {desc}")
                i = j + 1
                continue
            i += 1
            continue
        i += 1
    return out

def blockquote_from_lines(clean_lines: List[str], cap_chars: int) -> str:
    if not clean_lines:
        return "<blockquote>No details</blockquote>"
    acc = ""
    for ln in clean_lines:
        piece = html.escape(ln)
        cand = acc + ("" if not acc else "\n") + piece
        if len(cand) > cap_chars:
            acc += ("\n" if acc else "") + "… (truncated)"
            break
        acc = cand
    return f"<blockquote>{acc}</blockquote>"

def fetch_details_by_postback(session: requests.Session,
                              action_url: str,
                              base_payload: Dict[str, str],
                              target: str,
                              argument: str):
    payload = base_payload.copy()
    payload["__EVENTTARGET"] = target
    payload["__EVENTARGUMENT"] = argument

    # анти-бан джиттер между постбэками
    time.sleep(random.uniform(0.5, 1.5))

    post_url = requests.compat.urljoin(BASE_URL, action_url)
    r = request_with_retry("POST", post_url, session, data=payload)
    soup = BeautifulSoup(r.text, "html.parser")
    coords = extract_coords_from_details_html(soup)
    lines = extract_detail_lines(soup)
    clean = condense_detail_lines(lines) if lines else None
    return coords, (clean or [])

# ---------------------------------------------------------------------
# Rich facts extraction
# ---------------------------------------------------------------------

AUTO_NOTIFY_RE = re.compile(
    r"(NO RESP FRM CALLER|IPHONE (WATCH )?TC NOTIFICATION|CAN HEAR TRAFFIC IN BACKGROUND)",
    re.I
)
SOLO_RE = re.compile(
    r"\bSOLO\b|\bSOLO\s+VEH\b|\bSOLO\s+VEH\s+TC\b|\bSOLO\s+VEH\s+INTO\b",
    re.I
)

def parse_rich_facts(detail_lines: Optional[List[str]]) -> dict:
    """
    Вытаскиваем структурированные факты из Detail Information.
    'blocked' мы храним, но не выводим пользователю.
    """
    facts = {
        "vehicles": None,
        "vehicle_tags": set(),  # {'мотоцикл','фура','пикап','грузовик'}
        "loc_label": None,      # 'правая обочина','левая обочина','CD'
        "lane_nums": set(),
        "hov": False,
        "blocked": False,       # не показываем, но учитываем в логике приоритета
        "ramp": None,           # 'on-ramp'|'off-ramp'|'exit'
        "driveable": None,      # True/False/None
        "chp_on": False,
        "chp_enrt": False,
        "fire_on": False,
        "tow": None,            # 'requested'|'enroute'|'on_scene'
        "last_time_hint": None,
        "solo": False,
        "auto_notify": False,
    }

    if not detail_lines:
        return facts

    full_text = " ".join(detail_lines)
    up = full_text.upper()

    # SOLO?
    if SOLO_RE.search(full_text):
        facts["solo"] = True

    # Автоуведомление?
    if AUTO_NOTIFY_RE.search(full_text):
        facts["auto_notify"] = True

    # Локация
    if re.search(r"\bRS\b|\bRIGHT SHOULDER\b", up):
        facts["loc_label"] = "правая обочина"
    if re.search(r"\bLS\b|\bLEFT SHOULDER\b", up):
        facts["loc_label"] = "левая обочина"
    if re.search(r"\bCD\b|\bCENTER DIVIDER\b", up):
        facts["loc_label"] = "CD"

    if re.search(r"\bON[- ]?RAMP\b", up):
        facts["ramp"] = "on-ramp"
    if re.search(r"\bOFF[- ]?RAMP\b", up):
        facts["ramp"] = "off-ramp"
    if re.search(r"\bEXIT\b", up):
        facts["ramp"] = "exit"
    if re.search(r"\bHOV\b", up):
        facts["hov"] = True

    # какие полосы
    for m in re.finditer(r"#\s*(\d+)", up):
        facts["lane_nums"].add(m.group(1))

    # блокировки
    if re.search(r"\bBLKG?\b|\bBLOCK(ED|ING)\b|\bALL LNS STOPPED\b", up):
        facts["blocked"] = True
    if re.search(r"\b1125\b\s+(IN|#)", up):
        facts["blocked"] = True

    # какие ТС
    if re.search(r"\bMC\b|\bMOTORCYCLE\b", up):
        facts["vehicle_tags"].add("мотоцикл")
    if re.search(r"\bSEMI\b|\bBIG\s*RIG\b|\bTRACTOR TRAILER\b", up):
        facts["vehicle_tags"].add("фура")
    if re.search(r"\bTRK\b|\bTRUCK\b", up):
        facts["vehicle_tags"].add("грузовик")
    if re.search(r"\bPK\b|\bPICK ?UP\b", up):
        facts["vehicle_tags"].add("пикап")

    # сколько ТС
    nums = [int(n) for n in re.findall(r"\b(\d{1,2})\s*VEHS?\b", up)]
    if nums:
        facts["vehicles"] = max(nums)
    elif "SOLO VEH" in up or "SOLO VEHICLE" in up or "SOLO TC" in up:
        facts["vehicles"] = 1
    else:
        vs_line = next((ln for ln in detail_lines if re.search(r"\bVS\b", ln.upper())), None)
        if vs_line:
            parts = [p for p in re.split(r"\bVS\b", vs_line.upper()) if p.strip()]
            if len(parts) >= 2:
                facts["vehicles"] = max(facts["vehicles"] or 0, len(parts))

    # Driveable
    if re.search(r"\bNOT\s*DRIV(?:E|)ABLE\b|\bUNABLE TO MOVE VEH", up):
        facts["driveable"] = False
    elif re.search(r"\bVEH\s+IS\s+DRIVABLE\b|\bDRIVABLE\b", up):
        facts["driveable"] = True

    # Службы / эвакуатор
    # временные метки для "в XX:XX вызвали эвакуатор"
    time_marks = re.findall(r'\b\d{1,2}:\d{2}\s*(?:AM|PM)\b', full_text, flags=re.I)
    last_tmark = time_marks[-1] if time_marks else None
    facts["last_time_hint"] = last_tmark

    # CHP:
    # 97 = на месте, ENRT = в пути
    if re.search(r"\b97\b", up):
        facts["chp_on"] = True
    if re.search(r"\bENRT\b", up):
        facts["chp_enrt"] = True
    # FIRE/1141 -> пожарные/медики
    if re.search(r"\bFIRE\b|\b1141\b", up):
        facts["fire_on"] = True

    # Tow 1185:
    if re.search(r"\bREQ\s+1185\b|\bSTART\s+1185\b", up):
        facts["tow"] = "requested"
    if re.search(r"\b1185\b.*\bENRT\b", up):
        facts["tow"] = "enroute"
    if re.search(r"\b1185\s+97\b|\bTOW\b.*\b97\b", up):
        facts["tow"] = "on_scene"

    return facts

# ---------------------------------------------------------------------
# Human summary + formatting helpers
# ---------------------------------------------------------------------

def _compact_lanes(lanes: set) -> str:
    """
    {1,2,3} -> '#1–#3'; {1,3,5} -> '#1,#3,#5'
    """
    if not lanes:
        return ""
    nums = sorted(int(x) for x in lanes if x.isdigit())
    if not nums:
        return ""
    spans = []
    start = prev = nums[0]
    for n in nums[1:]:
        if n == prev + 1:
            prev = n
            continue
        spans.append((start, prev))
        start = prev = n
    spans.append((start, prev))
    parts = []
    for a, b in spans:
        parts.append(f"#{a}" if a == b else f"#{a}–#{b}")
    return ", ".join(parts)

def _unique_join(parts: list, sep: str = ", ") -> str:
    seen, out = set(), []
    for p in parts:
        p = p.strip()
        if not p or p in seen:
            continue
        seen.add(p)
        out.append(p)
    return sep.join(out)

def human_summary_from_facts(facts: dict) -> tuple[str, set]:
    """
    summary_line (короткая фраза)
    consumed_keys (что мы уже сказали, чтобы не дублировать)
    """
    consumed = set()
    bits = []

    # сколько и какие ТС
    v = facts.get("vehicles")
    tags = list(sorted(facts.get("vehicle_tags") or []))  # "мотоцикл", "фура", ...
    veh_phrase = None
    if v is not None and v > 0:
        if tags:
            veh_phrase = f"{v} маш. ({', '.join(tags)})"
            consumed.update({"vehicles", "vehicle_tags"})
        else:
            veh_phrase = f"{v} маш."
            consumed.add("vehicles")
    elif tags:
        veh_phrase = _unique_join(tags, " / ")
        consumed.add("vehicle_tags")
    if veh_phrase:
        bits.append(veh_phrase)

    # где стоят
    loc = facts.get("loc_label")   # правая обочина / левая / CD
    ramp = facts.get("ramp")       # on-ramp/off-ramp/exit
    lane = _compact_lanes(facts.get("lane_nums") or set())
    where_parts = []
    if ramp:
        where_parts.append("съезд")
    if loc:
        where_parts.append(loc)
    if lane:
        where_parts.append(f"полоса {lane}")
    if facts.get("hov"):
        where_parts.append("HOV")
    if where_parts:
        bits.append(_unique_join(where_parts, ", "))
        consumed.update({"loc_label", "ramp", "lane_nums"})

    # ходовые или нет
    if facts.get("driveable") is True:
        bits.append("на ходу")
        consumed.add("driveable")
    elif facts.get("driveable") is False:
        bits.append("не на ходу")
        consumed.add("driveable")

    # службы (приоритет эвакуатор -> CHP -> медики)
    tmark = facts.get("last_time_hint") or ""
    tow = facts.get("tow")
    if tow == "requested":
        bits.append("эвакуатор вызван" + (f" ({tmark})" if tmark else ""))
        consumed.add("tow")
    elif tow == "enroute":
        bits.append("эвакуатор в пути" + (f" ({tmark})" if tmark else ""))
        consumed.add("tow")
    elif tow == "on_scene":
        bits.append("эвакуатор на месте" + (f" ({tmark})" if tmark else ""))
        consumed.add("tow")

    if facts.get("chp_on"):
        bits.append("офицеры CHP на месте")
        consumed.update({"chp_on", "chp_enrt"})
    elif facts.get("chp_enrt"):
        bits.append("офицеры CHP в пути")
        consumed.add("chp_enrt")

    if facts.get("fire_on"):
        bits.append("медики/пожарные на месте")
        consumed.add("fire_on")

    summary = _unique_join(bits, ", ")
    return (summary, consumed)

# ---------------------------------------------------------------------
# Text builder
# ---------------------------------------------------------------------

def build_warning_prefix(facts: dict) -> str:
    """
    Первая(ые) специальные строки перед всем текстом:
    - SOLO ДТП
    - Автоматическое уведомление
    SOLO идёт первой, затем автоуведомление.
    """
    lines = []
    if facts.get("solo"):
        lines.append("<b>❗ Соло ДТП, не ехать</b>")
    if facts.get("auto_notify"):
        lines.append("<b>📱 Автоматическое уведомление — ждём обновления информации</b>")
    if not lines:
        return ""
    return "\n".join(lines) + "\n"

def make_text(inc: Dict[str, str],
              latlon: Optional[Tuple[float, float]],
              details_lines_clean: List[str],
              facts: dict,
              closed: bool = False) -> str:
    # иконка по типу
    icon = ""
    if "Collision" in inc['type']:
        icon = "🚨"
    elif "Hit" in inc['type'] and "Run" in inc['type']:
        icon = "🚗"

    # заголовок (время | area, потом тип, потом адрес)
    head_core = (
        f"⏳ {html.escape(inc['time'])} | 🏙 {html.escape(inc['area'])}\n"
        f"{icon} {html.escape(inc['type'])}\n\n"
        f"📍 {html.escape(inc['location'])} — {html.escape(inc['locdesc'])}"
    )

    # спец предупреждения (solo / auto-notify)
    warning_prefix = build_warning_prefix(facts)
    head = warning_prefix + head_core

    # резюме фактов
    summary_line, consumed = human_summary_from_facts(facts)

    # Доп. маркеры по группам (без повторов):
    markers = []

    # 1. Локация
    loc_bits = []
    if "loc_label" not in consumed and facts.get("loc_label"):
        loc_bits.append(facts["loc_label"])
    if "ramp" not in consumed and facts.get("ramp"):
        loc_bits.append("съезд")
    if "lane_nums" not in consumed and facts.get("lane_nums"):
        lane = _compact_lanes(facts["lane_nums"])
        if lane:
            loc_bits.append(f"полоса {lane}")
    if facts.get("hov"):
        loc_bits.append("HOV")
    loc_bits = [_ for _ in loc_bits if _]
    if loc_bits:
        markers.append(_unique_join(loc_bits, " · "))

    # 2. Типы машин / количество
    veh_bits = []
    if "vehicles" not in consumed and facts.get("vehicles") is not None:
        veh_bits.append(f"{facts['vehicles']} ТС")
    if "vehicle_tags" not in consumed and facts.get("vehicle_tags"):
        veh_bits.append(", ".join(sorted(facts["vehicle_tags"])))
    veh_bits = [_ for _ in veh_bits if _]
    if veh_bits:
        markers.append(" / ".join(veh_bits))

    # 3. Службы/статусы
    st_bits = []
    if "tow" not in consumed and facts.get("tow"):
        if facts["tow"] == "requested":
            st_bits.append("эвакуатор вызван")
        elif facts["tow"] == "enroute":
            st_bits.append("эвакуатор в пути")
        elif facts["tow"] == "on_scene":
            st_bits.append("эвакуатор на месте")
    if "chp_on" not in consumed and facts.get("chp_on"):
        st_bits.append("офицеры CHP на месте")
    elif "chp_enrt" not in consumed and facts.get("chp_enrt"):
        st_bits.append("офицеры CHP в пути")
    if "fire_on" not in consumed and facts.get("fire_on"):
        st_bits.append("медики/пожарные")
    if "driveable" not in consumed:
        if facts.get("driveable") is True:
            st_bits.append("на ходу")
        elif facts.get("driveable") is False:
            st_bits.append("не на ходу")
    st_bits = [_ for _ in st_bits if _]
    if st_bits:
        markers.append(_unique_join(st_bits, ", "))

    facts_block_lines = []
    if summary_line:
        facts_block_lines.append(summary_line)
    if markers:
        facts_block_lines.append(" | ".join(markers))

    facts_block = ""
    if facts_block_lines:
        facts_block = "\n\n<b>📌 Расположение / Машины:</b>\n" + "\n".join(facts_block_lines)

    # карта (метка, не маршрут)
    if latlon:
        lat, lon = latlon
        map_url = f"https://www.google.com/maps/search/?api=1&query={lat:.6f},{lon:.6f}"
        route_block = f"\n\n<b>🗺️ Карта:</b>\n{map_url}"
    else:
        route_block = "\n\n<b>🗺️ Карта:</b>\nКоординаты недоступны"

    # динамически режем детали, чтоб не превысить 4096
    skeleton = head + facts_block + route_block
    leftover = TG_HARD_LIMIT \
        - len(skeleton) \
        - len("\n\n<b>📝 Detail Information:</b>\n") \
        - (len("\n\n<b>❗️ Инцидент закрыт CHP</b>") if closed else 0)

    cap = max(0, min(MAX_DETAIL_CHARS_BASE, leftover))
    details_block = blockquote_from_lines(details_lines_clean, cap) if cap > 0 else ""
    det_block = f"\n\n<b>📝 Detail Information:</b>\n{details_block}" if details_block else ""

    text = skeleton + det_block
    if closed:
        text += "\n\n<b>❗️ Инцидент закрыт CHP</b>"

    # страховка — если вдруг всё равно >4096
    if len(text) > TG_HARD_LIMIT and det_block:
        shrink = int(cap * 0.8)
        details_block = blockquote_from_lines(details_lines_clean, max(0, shrink))
        det_block = f"\n\n<b>📝 Detail Information:</b>\n{details_block}" if details_block else ""
        text = skeleton + det_block
        if closed:
            text += "\n\n<b>❗️ Инцидент закрыт CHP</b>"

    return text

def signature_for_update(day_key: str,
                         inc: Dict[str, str],
                         details_lines_clean: List[str],
                         facts: dict) -> str:
    """
    Сигнатура определяет "сильно ли поменялось".
    Включаем day_key (YYYY-MM-DD), чтобы не было коллизий между днями.
    """
    norm_details = "\n".join(details_lines_clean or []).strip()
    fact_key = "|".join([
        str(facts.get("vehicles")),
        ",".join(sorted((facts.get("vehicle_tags") or set()))),
        facts.get("loc_label") or "",
        ",".join(sorted((facts.get("lane_nums") or set()))),
        "HOV" if facts.get("hov") else "",
        "BLK" if facts.get("blocked") else "",
        facts.get("ramp") or "",
        "DRV1" if facts.get("driveable") is True else ("DRV0" if facts.get("driveable") is False else ""),
        "C97" if facts.get("chp_on") else ("CENRT" if facts.get("chp_enrt") else ""),
        "FIRE" if facts.get("fire_on") else "",
        {"requested":"TREQ","enroute":"TENRT","on_scene":"T97"}.get(facts.get("tow") or "", ""),
        "SOLO" if facts.get("solo") else "",
        "AUTO" if facts.get("auto_notify") else "",
    ])
    base = (
        day_key + "||" +
        inc.get("type","").strip() + "||" +
        norm_details + "||" +
        fact_key
    ).encode("utf-8", "ignore")
    return hashlib.sha1(base).hexdigest()

# ---------------------------------------------------------------------
# Helper: merge / alias logic
# ---------------------------------------------------------------------

def find_nearby_active_incident(state: Dict[str, dict],
                                latlon: Tuple[float,float],
                                now_iso: str) -> Optional[str]:
    """
    Ищем в state активный инцидент, который:
      - имеет latlon близко по координатам (< MERGE_RADIUS_METERS)
      - был обновлён < MERGE_TIME_WINDOW_MIN минут назад
      - НЕ закрыт
    Возвращаем его ключ, если нашли.
    """
    for k, st in state.items():
        if not isinstance(st, dict):
            continue
        if st.get("closed"):
            continue
        st_latlon = st.get("latlon")
        if not st_latlon or len(st_latlon) != 2:
            continue
        # время свежести
        last_seen = st.get("last_seen") or st.get("first_seen")
        if not last_seen:
            continue
        age_min = minutes_between(last_seen, now_iso)
        if age_min > MERGE_TIME_WINDOW_MIN:
            continue
        # дистанция
        if is_close_coords(latlon, tuple(st_latlon), MERGE_RADIUS_METERS):
            return k
    return None

def attach_alias(state: Dict[str, dict],
                 alias_key: str,
                 master_key: str) -> None:
    """
    alias_key -> указывает на тот же Telegram message_id и т.д. что master_key.
    По сути мы копируем ссылку на то же сообщение.
    """
    master = state.get(master_key)
    if not master:
        return
    state[alias_key] = {
        "message_id": master.get("message_id"),
        "chat_id": master.get("chat_id"),
        "last_sig": master.get("last_sig"),
        "last_text": master.get("last_text"),
        "closed": master.get("closed", False),
        "misses": 0,
        "first_seen": master.get("first_seen"),
        "last_seen": utc_iso(),
        "latlon": master.get("latlon"),
        "master_of": master_key  # чтобы понимать чей он алиас
    }

def update_master_from_alias_merge(master_rec: dict,
                                   new_text: str,
                                   new_sig: str,
                                   latlon: Optional[Tuple[float,float]]):
    """
    Обновляем мастер после merge (чтобы last_text / last_sig были новые).
    """
    master_rec["last_text"] = new_text
    master_rec["last_sig"] = new_sig
    master_rec["closed"] = False
    master_rec["misses"] = 0
    master_rec["last_seen"] = utc_iso()
    if latlon:
        master_rec["latlon"] = list(latlon)

# ---------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------

def main():
    log.info(f"CHP notifier v9 | Center={COMM_CENTER} | Interval={POLL_INTERVAL}s | GeoFilter=ON | Merge=30min")

    state = load_state()
    session = requests.Session()

   import certifi
session.verify = certifi.where()  # используем актуальный корневой пакет

if CHP_INSECURE_SSL:
    session.verify = False
    try:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    except Exception:
        pass

    while True:
        cycle_seen_ids = set()
        day_key = dt.datetime.utcnow().strftime("%Y-%m-%d")
        now_iso_str = utc_iso()

        try:
            html_text = choose_communications_center(session, COMM_CENTER)
            soup, incidents = parse_incidents_with_postbacks(html_text)
            action_url, base_payload = extract_form_state(soup)

            # применяем фильтры по типу/локации/ареа заранее
            type_re = re.compile(TYPE_REGEX, re.I) if TYPE_REGEX else None
            area_re = re.compile(AREA_REGEX, re.I) if AREA_REGEX else None
            loc_re  = re.compile(LOCATION_REGEX, re.I) if LOCATION_REGEX else None

            for inc in incidents:
                # фильтр по типу/ареа/локации
                if type_re and not type_re.search(inc["type"]):
                    continue
                if area_re and not area_re.search(inc["area"]):
                    continue
                if loc_re and not (loc_re.search(inc["location"]) or loc_re.search(inc["locdesc"])):
                    continue

                # формируем дневной уникальный ключ (номер CHP + дата + центр)
                # чтобы 0300 сегодня != 0300 завтра
                inc_key = f"{COMM_CENTER}:{day_key}:{inc['no']}"
                cycle_seen_ids.add(inc_key)

                # тянем details
                latlon = None
                details_lines_clean: List[str] = []
                if inc.get("postback"):
                    latlon, details_lines_clean = fetch_details_by_postback(
                        session, action_url, base_payload,
                        inc["postback"]["target"], inc["postback"]["argument"]
                    )
                else:
                    details_lines_clean = []

                # геофильтр: если нет координат или точка вне зоны => пропускаем
                if not in_geofence(latlon):
                    log.debug("skip: out of geofence %s", inc_key)
                    continue

                # факты + текст
                facts = parse_rich_facts(details_lines_clean)
                text = make_text(inc, latlon, details_lines_clean, facts, closed=False)
                sig = signature_for_update(day_key, inc, details_lines_clean, facts)

                # MERGE: если у нас НЕТ этой записи, попробуем найти рядом активный
                st_existing = state.get(inc_key)
                if not st_existing:
                    if latlon:
                        master_key = find_nearby_active_incident(
                            state, latlon, now_iso_str
                        )
                    else:
                        master_key = None
                else:
                    master_key = None  # уже есть, не надо мерджить

                # если нашли похожий активный рядом по координатам
                if (not st_existing) and master_key:
                    master_rec = state.get(master_key)
                    if not master_rec:
                        # теоретически не должен быть None, но на всякий
                        master_key = None

                if (not st_existing) and master_key:
                    # Мы не создаём новое сообщение.
                    # Вместо этого редактируем мастер.
                    mid = master_rec.get("message_id")
                    if mid:
                        ok = tg_edit(mid, text, chat_id=master_rec.get("chat_id") or TELEGRAM_CHAT_ID)
                        if ok:
                            update_master_from_alias_merge(master_rec, text, sig, latlon)
                            attach_alias(state, inc_key, master_key)
                            log.info("merged %s -> %s (%s)", inc_key, master_key, inc.get("type"))
                        else:
                            # fallback: если вдруг не получилось отредачить,
                            # отправим отдельно, как новый
                            new_mid = tg_send(text, TELEGRAM_CHAT_ID)
                            state[inc_key] = {
                                "message_id": new_mid,
                                "chat_id": TELEGRAM_CHAT_ID,
                                "last_sig": sig,
                                "last_text": text,
                                "closed": False,
                                "misses": 0,
                                "first_seen": utc_iso(),
                                "last_seen": utc_iso(),
                                "latlon": list(latlon) if latlon else None
                            }
                            log.info("new(fallback) %s (%s)", inc_key, inc.get("type"))
                    else:
                        # мастер без message_id?? fallback аналогично
                        new_mid = tg_send(text, TELEGRAM_CHAT_ID)
                        state[inc_key] = {
                            "message_id": new_mid,
                            "chat_id": TELEGRAM_CHAT_ID,
                            "last_sig": sig,
                            "last_text": text,
                            "closed": False,
                            "misses": 0,
                            "first_seen": utc_iso(),
                            "last_seen": utc_iso(),
                            "latlon": list(latlon) if latlon else None
                        }
                        log.info("new(fallback2) %s (%s)", inc_key, inc.get("type"))

                else:
                    # обычная логика new/edit
                    st = state.get(inc_key)
                    if st and st.get("message_id"):
                        # Уже знаем про него -> возможно редактируем
                        if st.get("last_sig") != sig or st.get("closed", False):
                            ok = tg_edit(st["message_id"], text,
                                         chat_id=st.get("chat_id") or TELEGRAM_CHAT_ID)
                            if ok:
                                st["last_sig"] = sig
                                st["last_text"] = text
                                st["closed"] = False
                                log.info("edited %s (%s)", inc_key, inc.get("type"))
                        st["misses"] = 0
                        st["last_seen"] = utc_iso()
                        if latlon:
                            st["latlon"] = list(latlon)
                    else:
                        # Новый инцидент -> отправляем
                        mid = tg_send(text, TELEGRAM_CHAT_ID)
                        state[inc_key] = {
                            "message_id": mid,
                            "chat_id": TELEGRAM_CHAT_ID,
                            "last_sig": sig,
                            "last_text": text,
                            "closed": False,
                            "misses": 0,
                            "first_seen": utc_iso(),
                            "last_seen": utc_iso(),
                            "latlon": list(latlon) if latlon else None
                        }
                        log.info("new %s (%s)", inc_key, inc.get("type"))

            # закрытия
            for key, st in list(state.items()):
                # не трогаем вообще те ключи, которые сегодня не появились
                if key not in cycle_seen_ids and isinstance(st, dict):
                    st["misses"] = st.get("misses", 0) + 1
                    if st.get("closed"):
                        continue
                    if st["misses"] >= MISSES_TO_CLOSE and st.get("message_id"):
                        # помечаем текст как закрытый
                        new_text = (st.get("last_text") or "") + "\n\n<b>❗️ Инцидент закрыт CHP</b>"
                        ok = tg_edit(st["message_id"], new_text,
                                     chat_id=st.get("chat_id") or TELEGRAM_CHAT_ID)
                        if ok:
                            st["last_text"] = new_text
                            st["closed"] = True
                            log.info("closed %s", key)

            save_state(state)
            log.debug("%s: rows=%d, tracked=%d", COMM_CENTER, len(incidents), len(state))

        except KeyboardInterrupt:
            log.info("Stopped by user.")
            break
        except Exception as e:
            log.error("loop error: %s", e)

        # главный цикл джиттер
        jitter = random.uniform(2.0, 5.0)
        time.sleep(POLL_INTERVAL + jitter)

if __name__ == "__main__":
    main()
