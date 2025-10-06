#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

4) Запусти бота — при старте увидишь "DB connected" и авто-создание таблицы "incidents".
5) Проверка: посмотри логи о INSERT/UPDATE или выполни SELECT в консоли БД.

SQL для аналитики:
-- Пиковые часы × дни недели
SELECT EXTRACT(DOW FROM opened_at_local) AS dow,
       EXTRACT(HOUR FROM opened_at_local) AS hour,
       COUNT(*) AS n
FROM incidents
WHERE opened_at_local IS NOT NULL
GROUP BY 1,2
ORDER BY 1,2;

-- Средняя длительность ≥ 90 мин по типам
SELECT type, COUNT(*) AS n, ROUND(AVG(duration_min),1) AS avg_min
FROM incidents
WHERE duration_min IS NOT NULL AND duration_min >= 90
GROUP BY type
ORDER BY avg_min DESC;

-- Топ-локации
SELECT location, COUNT(*) AS n
FROM incidents
GROUP BY location
ORDER BY n DESC
LIMIT 30;

-- Гео-выгрузка (для тепловой карты)
SELECT lat, lon, opened_at_local
FROM incidents
WHERE lat IS NOT NULL AND lon IS NOT NULL;

Зависимости (requirements.txt):
  python-dotenv
  requests
  beautifulsoup4
  psycopg[binary]    # для PostgreSQL; можно убрать, если ANALYTICS_ENABLED=false
  folium             # только для heatmap.py (необязательно для бота)

=======================================================================
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

# ---------- ENV / CONFIG ----------
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

BASE_URL = os.getenv("CHP_URL", "https://cad.chp.ca.gov/Traffic.aspx")
COMM_CENTER = os.getenv("COMM_CENTER", "Inland")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

TYPE_REGEX = os.getenv("TYPE_REGEX", r"(Collision|Hit\s*(?:&|and)\s*Run)")
AREA_REGEX = os.getenv("AREA_REGEX", r"")
LOCATION_REGEX = os.getenv("LOCATION_REGEX", r"")

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))
SEEN_FILE = os.getenv("SEEN_FILE", "seen.json")
MAX_DETAIL_CHARS_BASE = int(os.getenv("MAX_DETAIL_CHARS", "2500"))
MISSES_TO_CLOSE = int(os.getenv("MISSES_TO_CLOSE", "2"))

ANALYTICS_ENABLED = os.getenv("ANALYTICS_ENABLED", "false").lower() == "true"
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
TZ_NAME = os.getenv("TZ", "America/Los_Angeles")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Telegram hard limit
TG_HARD_LIMIT = 4096

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s: %(message)s"
)
log = logging.getLogger("chp_bot")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36"
}

# ---------- Timezone utilities ----------
try:
    from zoneinfo import ZoneInfo  # py3.9+
    TZ = ZoneInfo(TZ_NAME)
except Exception:
    TZ = None

def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def to_local_from_hhmm_ampm(hhmm_ampm: str) -> Optional[dt.datetime]:
    try:
        t = dt.datetime.strptime(hhmm_ampm.strip().upper(), "%I:%M %p").time()
        today = dt.date.today()
        if TZ:
            return dt.datetime.combine(today, t).replace(tzinfo=TZ)
        return dt.datetime.combine(today, t)
    except Exception:
        return None

# ---------- Requests with retry/backoff ----------
RETRY_MAX_ATTEMPTS = 5
RETRY_BASE_DELAY = 0.5  # seconds
RETRY_MAX_DELAY = 10.0  # seconds

def should_retry(resp: Optional[requests.Response], err: Optional[Exception]) -> bool:
    if err is not None:
        # network/timeout -> retry
        return True
    if resp is None:
        return True
    if resp.status_code >= 500:
        return True
    if resp.status_code in (403, 429):
        return True
    # other 4xx: do not retry
    return False

def request_with_retry(method: str, url: str, session: requests.Session, **kwargs) -> requests.Response:
    attempt = 0
    delay = RETRY_BASE_DELAY
    while True:
        attempt += 1
        err = None
        resp = None
        try:
            resp = session.request(method, url, headers=HEADERS, timeout=30, **kwargs)
            if not should_retry(resp, None):
                return resp
            # retryable statuses: raise for flow to retry
            log.debug(f"HTTP {resp.status_code} -> retryable for {url}")
        except requests.RequestException as e:
            err = e
            log.debug(f"Request error (attempt {attempt}) {e}")

        if attempt >= RETRY_MAX_ATTEMPTS:
            if err:
                raise err
            else:
                resp.raise_for_status()

        time_to_sleep = min(RETRY_MAX_DELAY, delay * (2 ** (attempt - 1)))
        jitter = random.uniform(0, 0.5 * time_to_sleep)
        sleep_for = time_to_sleep + jitter
        log.debug(f"Backoff sleeping {sleep_for:.2f}s before retry (attempt {attempt+1}) for {url}")
        time.sleep(sleep_for)

# ---------- Telegram ----------
def safe_len_for_telegram(text: str) -> int:
    return len(text)

def tg_send(text: str, chat_id: Optional[str] = None) -> Optional[int]:
    chat_id = (chat_id or TELEGRAM_CHAT_ID).strip()
    if not TELEGRAM_TOKEN or not chat_id:
        log.warning("TELEGRAM_TOKEN/CHAT_ID не заданы. Сообщение не отправлено.")
        return None
    api = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True, "parse_mode": "HTML"}
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
    payload = {"chat_id": chat_id, "message_id": message_id, "text": text, "disable_web_page_preview": True, "parse_mode": "HTML"}
    r = requests.post(api, data=payload, timeout=20)
    if r.status_code != 200:
        log.error("Telegram edit %s %s", r.status_code, r.text[:400])
        return False
    return True

# ---------- state (for editing) ----------
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
    with open(SEEN_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

# ---------- ASP.NET form helpers ----------
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
            submit_name = btn.get("name"); submit_value = btn.get("value"); break
    if not submit_name:
        btn = form.find("input", {"type": "submit"})
        if btn:
            submit_name = btn.get("name"); submit_value = btn.get("value", "OK")
    if submit_name:
        payload[submit_name] = submit_value

    post_url = requests.compat.urljoin(BASE_URL, action)
    r2 = request_with_retry("POST", post_url, session, data=payload)
    return r2.text

# ---------- incidents table parsing ----------
def find_incidents_table(soup: BeautifulSoup):
    for table in soup.find_all("table"):
        header = table.find("tr")
        if not header:
            continue
        headers = [h.get_text(strip=True).lower() for h in header.find_all(["th", "td"])]
        if headers and all(x in headers for x in ["time", "type", "location"]):
            return table
    return None

def parse_incidents_with_postbacks(html_text: str) -> Tuple[BeautifulSoup, List[Dict[str, str]]]:
    soup = BeautifulSoup(html_text, "html.parser")
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

# ---------- Details parsing ----------
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

TIME_RE = re.compile(r'^\d{1,2}:\d{2}\s*(?:AM|PM)$', re.IGNORECASE)
FOOTER_PATTERNS = [
    r'^Click on Details for additional information\.',
    r'^Your screen will refresh in \d+ seconds\.$',
    r'^Contact Us$', r'^CHP Home Page$', r'^CHP Mobile Traffic$', r'^\|$'
]
FOOTER_RE = re.compile("|".join(FOOTER_PATTERNS), re.IGNORECASE)

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
    """
    Сформировать <blockquote> с ограничением по символам.
    """
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

def fetch_details_by_postback(session: requests.Session, action_url: str, base_payload: Dict[str, str],
                              target: str, argument: str) -> Tuple[Optional[Tuple[float, float]], Optional[str], Optional[List[str]]]:
    payload = base_payload.copy()
    payload["__EVENTTARGET"] = target
    payload["__EVENTARGUMENT"] = argument
    post_url = requests.compat.urljoin(BASE_URL, action_url)
    # маленький джиттер перед постбэком
    time.sleep(random.uniform(0.5, 1.5))
    r = request_with_retry("POST", post_url, session, data=payload)
    soup = BeautifulSoup(r.text, "html.parser")
    coords = extract_coords_from_details_html(soup)
    lines = extract_detail_lines(soup)
    clean = condense_detail_lines(lines) if lines else None
    details_block_html = blockquote_from_lines(clean or [], MAX_DETAIL_CHARS_BASE)
    return coords, details_block_html, (clean or [])

# ---------- Rich facts & human summary ----------
BARRIER_WORDS = {"BARRIER", "GUARDRAIL", "FENCE", "DEBRIS", "ANIMAL", "DEER", "TREE", "POLE", "SIGN"}

def parse_rich_facts(detail_lines: Optional[List[str]]) -> dict:
    """
    Извлечь факты из Detail Information.
    !!! ВНИМАНИЕ: 'blocked' сохраняем в БД, но НЕ показываем в кратком описании.
    """
    facts = {
        "vehicles": None,
        "vehicle_tags": set(),
        "loc_label": None,   # 'правая обочина' / 'левая обочина' / 'CD'
        "lane_nums": set(),
        "hov": False,
        "blocked": False,    # хранить, но не показывать в кратком описании
        "ramp": None,        # 'on-ramp'|'off-ramp'|'exit'
        "driveable": None,   # True/False/None
        "chp_on": False,     # офицеры на месте
        "chp_enrt": False,   # офицеры в пути
        "fire_on": False,    # пожарные/медики
        "tow": None,         # 'requested'|'enroute'|'on_scene'
        "last_time_hint": None,  # последнее время из детайлов, если пригодится для фразы
    }
    if not detail_lines:
        return facts

    text_up = " ".join(detail_lines).upper()

    # Место
    if re.search(r"\bRS\b|\bRIGHT SHOULDER\b", text_up): facts["loc_label"] = "правая обочина"
    if re.search(r"\bLS\b|\bLEFT SHOULDER\b", text_up):  facts["loc_label"] = "левая обочина"
    if re.search(r"\bCD\b|\bCENTER DIVIDER\b", text_up): facts["loc_label"] = "CD"

    if re.search(r"\bON[- ]?RAMP\b", text_up):   facts["ramp"] = "on-ramp"
    if re.search(r"\bOFF[- ]?RAMP\b", text_up):  facts["ramp"] = "off-ramp"
    if re.search(r"\bEXIT\b", text_up):          facts["ramp"] = "exit"
    if re.search(r"\bHOV\b", text_up):           facts["hov"] = True

    for m in re.finditer(r"#\s*(\d+)", text_up):
        facts["lane_nums"].add(m.group(1))

    if re.search(r"\bBLKG?\b|\bBLOCK(ED|ING)\b|\bALL LNS STOPPED\b", text_up):
        facts["blocked"] = True
    if re.search(r"\b1125\b\s+(IN|#)", text_up):
        facts["blocked"] = True

    # Типы ТС
    if re.search(r"\bMC\b|\bMOTORCYCLE\b", text_up):                       facts["vehicle_tags"].add("мотоцикл")
    if re.search(r"\bSEMI\b|\bBIG\s*RIG\b|\bTRACTOR TRAILER\b", text_up):  facts["vehicle_tags"].add("фура")
    if re.search(r"\bTRK\b|\bTRUCK\b", text_up):                           facts["vehicle_tags"].add("грузовик")
    if re.search(r"\bPK\b|\bPICK ?UP\b", text_up):                         facts["vehicle_tags"].add("пикап")

    # Число ТС
    nums = [int(n) for n in re.findall(r"\b(\d{1,2})\s*VEHS?\b", text_up)]
    if nums:
        facts["vehicles"] = max(nums)
    elif "SOLO VEH" in text_up:
        facts["vehicles"] = 1
    else:
        vs_line = next((ln for ln in detail_lines if re.search(r"\bVS\b", ln.upper())), None)
        if vs_line:
            parts = [p for p in re.split(r"\bVS\b", vs_line.upper()) if p.strip()]
            if len(parts) >= 2:
                facts["vehicles"] = max(facts["vehicles"] or 0, len(parts))

    # Driveable
    if re.search(r"\bNOT\s*DRIV(?:E|)ABLE\b|\bUNABLE TO MOVE VEH", text_up):
        facts["driveable"] = False
    elif re.search(r"\bVEH\s+IS\s+DRIVABLE\b|\bDRIVABLE\b", text_up):
        facts["driveable"] = True

    # Службы
    # 97 -> на месте, ENRT -> в пути. 1141/Fire — медики/пожарные
    # Мы не показываем коды в тексте, только смысл.
    # Определим последнюю метку времени, чтобы сформулировать фразу типа "в 9:30 вызвали эвакуатор".
    time_marks = [m.group(0) for m in re.finditer(r'\b\d{1,2}:\d{2}\s*(?:AM|PM)\b', " ".join(detail_lines))]
    facts["last_time_hint"] = time_marks[-1] if time_marks else None

    if re.search(r"\b97\b", text_up): facts["chp_on"] = True
    if re.search(r"\bENRT\b", text_up): facts["chp_enrt"] = True
    if re.search(r"\bFIRE\b|\b1141\b", text_up): facts["fire_on"] = True

    # Эвакуатор 1185
    if re.search(r"\bREQ\s+1185\b|\bSTART\s+1185\b", text_up):
        facts["tow"] = "requested"
    if re.search(r"\b1185\b.*\bENRT\b", text_up):
        facts["tow"] = "enroute"
    if re.search(r"\b1185\s+97\b|\bTOW\b.*\b97\b", text_up):
        facts["tow"] = "on_scene"

    return facts

# ---------- improved human summary (no codes, no “blocked”) ----------
def human_summary_from_facts(facts: dict) -> tuple[str, set]:
    """
    Возвращает (summary_text, consumed_keys)
    consumed_keys — какие элементы уже «съедены» резюме (чтобы не повторять в маркерах).
    """
    consumed = set()
    bits = []

    # 1) Сколько и какие ТС
    v = facts.get("vehicles")
    tags = list(sorted(facts.get("vehicle_tags") or []))  # русские: "мотоцикл","фура","грузовик","пикап"
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

    # 2) Где именно
    loc = facts.get("loc_label")  # правая/левая обочина / CD
    ramp = facts.get("ramp")      # on/off/exit -> "съезд"
    lane = _compact_lanes(facts.get("lane_nums") or set())
    where_parts = []
    if ramp: where_parts.append("съезд")
    if loc:  where_parts.append(loc)
    if lane: where_parts.append(f"полоса {lane}")
    if where_parts:
        bits.append(_unique_join(where_parts, ", "))
        consumed.update({"loc_label", "ramp", "lane_nums"})

    # 3) Ходовость
    if facts.get("driveable") is True:
        bits.append("на ходу")
        consumed.add("driveable")
    elif facts.get("driveable") is False:
        bits.append("не на ходу")
        consumed.add("driveable")

    # 4) Самое важное из служб (кратко, без кодов)
    # — эвакуатор приоритетнее, затем CHP, затем медики/пожарные
    tmark = (facts.get("last_time_hint") or "").lower()
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
        consumed.update({"chp_enrt"})

    if facts.get("fire_on"):
        bits.append("медики/пожарные на месте")
        consumed.add("fire_on")

    # итого
    summary = _unique_join(bits, ", ")
    return (summary, consumed)

# ---------- filters ----------
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

# --- helpers for clean phrasing & no-dup ---

def _compact_lanes(lanes: set) -> str:
    """#1,#2,#3 -> '#1–#3'; иначе просто '#1,#4'."""
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
    # форматируем
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
        seen.add(p); out.append(p)
    return sep.join(out)

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

    # шапка
    head = (
        f"⏳ {html.escape(inc['time'])} | 🏷️ {html.escape(inc['area'])}\n"
        f"{icon} {html.escape(inc['type'])}\n\n"
        f"📍 {html.escape(inc['location'])} — {html.escape(inc['locdesc'])}"
    )

    # 1) человеческое резюме + какие поля уже употребили
    summary_line, consumed = human_summary_from_facts(facts)

    # 2) компактные маркеры (без повторов и БЕЗ «блокировок»/кодов)
    markers = []

    # — Место
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
    if loc_bits:
        markers.append(_unique_join(loc_bits, " · "))

    # — Машины
    veh_bits = []
    if "vehicles" not in consumed and facts.get("vehicles") is not None:
        veh_bits.append(f"{facts['vehicles']} ТС")
    if "vehicle_tags" not in consumed and facts.get("vehicle_tags"):
        veh_bits.append(", ".join(sorted(facts["vehicle_tags"])))
    if veh_bits:
        markers.append(" / ".join(veh_bits))

    # — Службы (без кодов; не повторять то, что уже в резюме)
    st_bits = []
    if "chp_on" not in consumed and facts.get("chp_on"):
        st_bits.append("офицеры CHP на месте")
    elif "chp_enrt" not in consumed and facts.get("chp_enrt"):
        st_bits.append("офицеры CHP в пути")
    if "fire_on" not in consumed and facts.get("fire_on"):
        st_bits.append("медики/пожарные")
    if "tow" not in consumed and facts.get("tow"):
        if facts["tow"] == "requested":
            st_bits.append("эвакуатор вызван")
        elif facts["tow"] == "enroute":
            st_bits.append("эвакуатор в пути")
        elif facts["tow"] == "on_scene":
            st_bits.append("эвакуатор на месте")
    if "driveable" not in consumed:
        if facts.get("driveable") is True:
            st_bits.append("на ходу")
        elif facts.get("driveable") is False:
            st_bits.append("не на ходу")
    if st_bits:
        markers.append(_unique_join(st_bits, ", "))

    # Фактовый блок
    facts_block_lines = []
    if summary_line:
        facts_block_lines.append(summary_line)
    if markers:
        facts_block_lines.append(" | ".join(markers))
    facts_block = "\n\n<b>📌 Расположение / Машины:</b>\n" + "\n".join(facts_block_lines) if facts_block_lines else ""

    # КАРТА: всегда метка по координатам (а не маршрут)
    if latlon:
        lat, lon = latlon
        map_url = f"https://www.google.com/maps/search/?api=1&query={lat:.6f},{lon:.6f}"
        route_block = f"\n\n<b>🗺️ Карта:</b>\n{map_url}"
    else:
        route_block = "\n\n<b>🗺️ Карта:</b>\nКоординаты недоступны"

    # детали (с динамическим ограничением до 4096)
    skeleton = head + facts_block + route_block
    leftover = TG_HARD_LIMIT - len(skeleton) - len("\n\n<b>📝 Detail Information:</b>\n") - (len("\n\n<b>❗️ Инцидент закрыт CHP</b>") if closed else 0)
    cap = max(0, min(MAX_DETAIL_CHARS_BASE, leftover))
    details_block = blockquote_from_lines(details_lines_clean, cap) if cap > 0 else ""
    det_block = f"\n\n<b>📝 Detail Information:</b>\n{details_block}" if details_block else ""

    text = skeleton + det_block
    if closed:
        text += "\n\n<b>❗️ Инцидент закрыт CHP</b>"

    # страховка — если вдруг перелезли лимит
    if len(text) > TG_HARD_LIMIT and det_block:
        shrink = int(cap * 0.8)
        details_block = blockquote_from_lines(details_lines_clean, max(0, shrink))
        det_block = f"\n\n<b>📝 Detail Information:</b>\n{details_block}" if details_block else ""
        text = skeleton + det_block
        if closed:
            text += "\n\n<b>❗️ Инцидент закрыт CHP</b>"

    return text

def signature_for_update(inc: Dict[str, str], details_lines_clean: List[str], facts: dict) -> str:
    # нормализуем детали и факты — сигнатура меняется при реально новых фактах/строках
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
        {"requested":"TREQ","enroute":"TENRT","on_scene":"T97"}.get(facts.get("tow") or "", "")
    ])
    base = (inc.get("type","").strip() + "||" + norm_details + "||" + fact_key).encode("utf-8", "ignore")
    return hashlib.sha1(base).hexdigest()

# ---------- Optional PostgreSQL analytics ----------
PG = None
def db_connect_if_enabled():
    global PG
    if not ANALYTICS_ENABLED or not DATABASE_URL:
        log.warning("Analytics disabled or DATABASE_URL empty — working without DB.")
        return
    try:
        import psycopg
        PG = psycopg.connect(DATABASE_URL, autocommit=True)
        with PG.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS incidents (
              incident_id       text PRIMARY KEY,
              center            text,
              type              text,
              area              text,
              location          text,
              locdesc           text,
              lat               double precision,
              lon               double precision,
              vehicles          integer,
              vehicle_tags      text,
              location_label    text,
              lane_nums         text,
              hov               boolean,
              blocked           boolean,
              ramp              text,
              driveable         boolean,
              chp_on            boolean,
              chp_enrt          boolean,
              fire_on           boolean,
              tow               text,
              opened_at_local   timestamptz,
              opened_at_utc     timestamptz,
              closed_at_local   timestamptz,
              closed_at_utc     timestamptz,
              duration_min      double precision,
              last_details      text,
              last_updated_utc  timestamptz
            );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_incidents_opened ON incidents(opened_at_utc);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_incidents_area   ON incidents(area);")
        log.info("DB connected and ensured schema.")
    except Exception as e:
        PG = None
        log.warning("DB not available (%s). Continuing without analytics.", e)

def db_insert_open(inc_id: str, center: str, inc: dict,
                   latlon: Optional[Tuple[float, float]],
                   facts: dict,
                   details_lines_clean: List[str]) -> None:
    if PG is None:
        return
    lat, lon = (latlon if latlon else (None, None))
    opened_local = to_local_from_hhmm_ampm(inc.get("time",""))
    opened_local_iso = opened_local.isoformat() if isinstance(opened_local, dt.datetime) else None
    opened_utc_iso = now_utc().isoformat()
    last_details = "\n".join(details_lines_clean) if details_lines_clean else None
    vehicle_tags_csv = ",".join(sorted(facts.get("vehicle_tags") or []))
    lane_csv = ",".join(sorted(facts.get("lane_nums") or []))
    try:
        with PG.cursor() as cur:
            cur.execute("""
                INSERT INTO incidents
                (incident_id, center, type, area, location, locdesc, lat, lon,
                 vehicles, vehicle_tags, location_label, lane_nums, hov, blocked, ramp, driveable,
                 chp_on, chp_enrt, fire_on, tow,
                 opened_at_local, opened_at_utc, last_details, last_updated_utc)
                VALUES
                (%s,%s,%s,%s,%s,%s,%s,%s,
                 %s,%s,%s,%s,%s,%s,%s,%s,
                 %s,%s,%s,%s,
                 %s,%s,%s,%s)
                ON CONFLICT (incident_id) DO NOTHING
            """, (
                inc_id, center, inc.get("type"), inc.get("area"), inc.get("location"), inc.get("locdesc"),
                lat, lon,
                facts.get("vehicles"), vehicle_tags_csv, facts.get("loc_label"), lane_csv, facts.get("hov"), facts.get("blocked"), facts.get("ramp"), facts.get("driveable"),
                facts.get("chp_on"), facts.get("chp_enrt"), facts.get("fire_on"), facts.get("tow"),
                opened_local_iso, opened_utc_iso, last_details, opened_utc_iso
            ))
    except Exception as e:
        log.debug("db_insert_open error: %s", e)

def db_update_on_change(inc_id: str, inc: dict,
                        latlon: Optional[Tuple[float, float]],
                        facts: dict,
                        details_lines_clean: List[str]) -> None:
    if PG is None:
        return
    lat, lon = (latlon if latlon else (None, None))
    last_details = "\n".join(details_lines_clean) if details_lines_clean else None
    vehicle_tags_csv = ",".join(sorted(facts.get("vehicle_tags") or []))
    lane_csv = ",".join(sorted(facts.get("lane_nums") or []))
    try:
        with PG.cursor() as cur:
            cur.execute("""
                UPDATE incidents
                   SET type=%s, area=%s, location=%s, locdesc=%s,
                       lat=COALESCE(%s, lat), lon=COALESCE(%s, lon),
                       vehicles=%s,
                       vehicle_tags=%s,
                       location_label=%s,
                       lane_nums=%s,
                       hov=%s,
                       blocked=%s,
                       ramp=%s,
                       driveable=%s,
                       chp_on=%s,
                       chp_enrt=%s,
                       fire_on=%s,
                       tow=%s,
                       last_details=%s,
                       last_updated_utc=%s
                 WHERE incident_id=%s
            """, (
                inc.get("type"), inc.get("area"), inc.get("location"), inc.get("locdesc"),
                lat, lon,
                facts.get("vehicles"),
                vehicle_tags_csv,
                facts.get("loc_label"),
                lane_csv,
                facts.get("hov"),
                facts.get("blocked"),
                facts.get("ramp"),
                facts.get("driveable"),
                facts.get("chp_on"),
                facts.get("chp_enrt"),
                facts.get("fire_on"),
                facts.get("tow"),
                last_details,
                now_utc().isoformat(),
                inc_id
            ))
    except Exception as e:
        log.debug("db_update_on_change error: %s", e)

def db_mark_closed(inc_id: str) -> None:
    if PG is None:
        return
    try:
        with PG.cursor() as cur:
            cur.execute("SELECT opened_at_utc FROM incidents WHERE incident_id=%s", (inc_id,))
            row = cur.fetchone()
            opened_utc = None
            if row and row[0]:
                opened_utc = row[0]
            closed_utc = now_utc()
            closed_local = closed_utc.astimezone(TZ) if TZ else None
            duration_min = None
            if opened_utc:
                # opened_utc может быть aware (psycopg возвращает aware)
                if opened_utc.tzinfo is None:
                    opened_utc = opened_utc.replace(tzinfo=dt.timezone.utc)
                diff = closed_utc - opened_utc
                duration_min = round(diff.total_seconds() / 60.0, 1)
            cur.execute("""
                UPDATE incidents
                   SET closed_at_utc=%s,
                       closed_at_local=%s,
                       duration_min=%s
                 WHERE incident_id=%s
            """, (
                closed_utc.isoformat(),
                closed_local.isoformat() if closed_local else None,
                duration_min,
                inc_id
            ))
    except Exception as e:
        log.debug("db_mark_closed error: %s", e)

# ---------- main loop ----------
def main() -> None:
    log.info(f"CHP notifier v8.0 | Center={COMM_CENTER} | Interval={POLL_INTERVAL}s | Analytics={'ON' if ANALYTICS_ENABLED else 'OFF'}")
    db_connect_if_enabled()
    state = load_state()
    session = requests.Session()

    while True:
        cycle_seen_ids = set()
        try:
            html_text = choose_communications_center(session, COMM_CENTER)
            soup, incidents = parse_incidents_with_postbacks(html_text)
            action_url, base_payload = extract_form_state(soup)
            filtered = filter_collisions(incidents)

            for inc in filtered:
                inc_id = inc["no"]
                cycle_seen_ids.add(inc_id)

                # детали
                latlon = None
                details_lines_clean: List[str] = []
                if inc.get("postback"):
                    latlon, details_block_initial, details_lines_clean = fetch_details_by_postback(
                        session, action_url, base_payload,
                        inc["postback"]["target"], inc["postback"]["argument"]
                    )
                else:
                    details_block_initial = "<blockquote>No details</blockquote>"

                # факты + текст
                facts = parse_rich_facts(details_lines_clean)
                text = make_text(inc, latlon, details_lines_clean, facts, closed=False)
                sig = signature_for_update(inc, details_lines_clean, facts)

                st = state.get(inc_id)
                if st and st.get("message_id"):
                    if st.get("last_sig") != sig or st.get("closed", False):
                        ok = tg_edit(st["message_id"], text, chat_id=st.get("chat_id") or TELEGRAM_CHAT_ID)
                        if ok:
                            st["last_sig"] = sig
                            st["last_text"] = text
                            st["closed"] = False
                            db_update_on_change(inc_id, inc, latlon, facts, details_lines_clean)
                            log.info("edited %s (%s)", inc_id, inc.get("type"))
                    st["misses"] = 0
                    st["last_seen"] = dt.datetime.utcnow().isoformat()
                else:
                    mid = tg_send(text, chat_id=TELEGRAM_CHAT_ID)
                    state[inc_id] = {
                        "message_id": mid,
                        "chat_id": TELEGRAM_CHAT_ID,
                        "last_sig": sig,
                        "last_text": text,
                        "closed": False,
                        "misses": 0,
                        "first_seen": dt.datetime.utcnow().isoformat(),
                        "last_seen": dt.datetime.utcnow().isoformat(),
                    }
                    db_insert_open(inc_id, COMM_CENTER, inc, latlon, facts, details_lines_clean)
                    log.info("new %s (%s)", inc_id, inc.get("type"))

            # закрытия
            for inc_id, st in list(state.items()):
                if inc_id not in cycle_seen_ids and isinstance(st, dict):
                    st["misses"] = st.get("misses", 0) + 1
                    if st.get("closed"):
                        continue
                    if st["misses"] >= MISSES_TO_CLOSE and st.get("message_id"):
                        new_text = (st.get("last_text") or "") + "\n\n<b>❗️ Инцидент закрыт CHP</b>"
                        ok = tg_edit(st["message_id"], new_text, chat_id=st.get("chat_id") or TELEGRAM_CHAT_ID)
                        if ok:
                            st["last_text"] = new_text
                            st["closed"] = True
                            db_mark_closed(inc_id)
                            log.info("closed %s", inc_id)

            save_state(state)
            log.debug("%s: rows=%d, matched=%d, tracked=%d", COMM_CENTER, len(incidents), len(filtered), len(state))

        except KeyboardInterrupt:
            log.info("Stopped by user.")
            break
        except Exception as e:
            log.error("loop error: %s", e)

        # главный джиттер цикла
        jitter = random.uniform(2.0, 5.0)
        time.sleep(POLL_INTERVAL + jitter)

if __name__ == "__main__":
    main()
