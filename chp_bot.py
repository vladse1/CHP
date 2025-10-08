#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CHP CAD → Telegram бот (инциденты ДТП)

Зависимости (requirements.txt):
  requests
  beautifulsoup4
  python-dotenv

.env (пример):
  TELEGRAM_TOKEN=123456:ABC...your-bot-token
  TELEGRAM_CHAT_ID=-1001234567890
  COMM_CENTER=Inland
  TYPE_REGEX=(Collision|Hit\s*(?:&|and)\s*Run)
  POLL_INTERVAL=30
  MISSES_TO_CLOSE=2
  MAX_DETAIL_CHARS=2500
  TZ=America/Los_Angeles
  LOG_LEVEL=INFO

Особенности:
- Устойчивый выбор Communications Center: кнопка OK → __EVENTTARGET → AJAX (ScriptManager + __ASYNCPOST)
- Ключ инцидента: CENTER:YYYYMMDD:NNNN (нет конфликтов номеров между днями/центрами)
- Очистка «вчерашних» записей из seen.json
- Парсинг таблицы по заголовкам (не привязан к фиксированным индексам)
- Постбэк Details → извлекаем координаты и строки "Detail Information"
- Нормализация details в факты (обочины/полосы/HOV/съезды, driveable, CHP/FIRE, 1185, типы ТС)
- Человеческое резюме без повторов/кодов/«блокировок»
- Карта: всегда ссылка-метка (search?query=lat,lon) — универсально
- Телеграм: send/edit + фоллбэк (если редактировать нельзя → отправить новое)
- Retry с экспоненциальным backoff, джиттер в запросах и основном цикле
- Динамическая подрезка деталей до 4096 символов
"""

import os
import re
import json
import time
import html
import random
import logging as log
import datetime as dt
import zoneinfo
from typing import Dict, Optional, Tuple, List, Set

import requests
from bs4 import BeautifulSoup as BS
from dotenv import load_dotenv

# ===== ENV & constants =====

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
COMM_CENTER = os.getenv("COMM_CENTER", "Inland").strip()
TYPE_RE = re.compile(os.getenv("TYPE_REGEX", r"(Collision|Hit\s*(?:&|and)\s*Run)"), re.I)

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))
MISSES_TO_CLOSE = int(os.getenv("MISSES_TO_CLOSE", "2"))
MAX_DETAIL_CHARS_BASE = int(os.getenv("MAX_DETAIL_CHARS", "2500"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOCAL_TZ = zoneinfo.ZoneInfo(os.getenv("TZ", "America/Los_Angeles"))

TG_HARD_LIMIT = 4096
STATE_FILE = "seen.json"

log.basicConfig(
    level=getattr(log, LOG_LEVEL, log.INFO),
    format="%(asctime)s %(levelname)s: %(message)s",
)

BASE_URL = "https://cad.chp.ca.gov/Traffic.aspx"
UA_POOL = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
]


# ===== utilities: state =====

def today_str() -> str:
    return dt.datetime.now(LOCAL_TZ).strftime("%Y%m%d")

def compose_incident_key(center: str, number: str | int) -> str:
    n = str(number).strip()
    if n.isdigit():
        n = n.zfill(4)
    return f"{center}:{today_str()}:{n}"

def load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_state(state: dict) -> None:
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_FILE)

def purge_old_state(state: dict) -> None:
    """Удаляем записи не за сегодня и мусор без message_id."""
    t = today_str()
    for k, v in list(state.items()):
        parts = str(k).split(":")
        if len(parts) >= 3 and parts[1] != t:
            state.pop(k, None)
            continue
        if not isinstance(v, dict) or not v.get("message_id"):
            state.pop(k, None)


# ===== HTTP with backoff =====

def backoff_sleep(attempt: int, base: float = 0.5, cap: float = 10.0):
    delay = min(cap, base * (2 ** (attempt - 1)))
    delay += random.uniform(0.0, 0.5)  # лёгкий джиттер
    time.sleep(delay)

def http_get(session: requests.Session, url: str, **kw) -> requests.Response:
    kw.setdefault("timeout", 20)
    for i in range(1, 6):
        try:
            r = session.get(url, **kw)
            if r.status_code >= 500 or r.status_code in (429, 403):
                log.debug("GET %s -> %s, retry #%d", url, r.status_code, i)
                backoff_sleep(i)
                continue
            return r
        except requests.RequestException as e:
            log.debug("GET error %s (try %d)", e, i)
            backoff_sleep(i)
    raise RuntimeError(f"GET failed after retries: {url}")

def http_post(session: requests.Session, url: str, data: dict, **kw) -> requests.Response:
    kw.setdefault("timeout", 20)
    for i in range(1, 6):
        try:
            r = session.post(url, data=data, **kw)
            if r.status_code >= 500 or r.status_code in (429, 403):
                log.debug("POST %s -> %s, retry #%d", url, r.status_code, i)
                backoff_sleep(i)
                continue
            return r
        except requests.RequestException as e:
            log.debug("POST error %s (try %d)", e, i)
            backoff_sleep(i)
    raise RuntimeError(f"POST failed after retries: {url}")


# ===== Telegram =====

def tg_send(text: str, chat_id: Optional[str] = None) -> Optional[int]:
    chat_id = (chat_id or TELEGRAM_CHAT_ID).strip()
    if not TELEGRAM_TOKEN or not chat_id:
        return None
    api = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
        "parse_mode": "HTML",
    }
    try:
        r = requests.post(api, data=payload, timeout=20)
        if r.status_code != 200:
            log.error("Telegram send %s %s", r.status_code, r.text[:400])
            return None
        j = r.json()
        return j.get("result", {}).get("message_id")
    except Exception as e:
        log.error("Telegram send err %s", e)
        return None

def tg_edit(message_id: int, text: str, chat_id: Optional[str] = None) -> tuple[bool, str]:
    chat_id = (chat_id or TELEGRAM_CHAT_ID).strip()
    if not TELEGRAM_TOKEN or not chat_id or not message_id:
        return (False, "bad-params")
    api = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/editMessageText"
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "disable_web_page_preview": True,
        "parse_mode": "HTML",
    }
    try:
        r = requests.post(api, data=payload, timeout=20)
        if r.status_code != 200:
            try:
                desc = r.json().get("description", "")
            except Exception:
                desc = r.text[:200]
            log.error("Telegram edit %s %s", r.status_code, r.text[:400])
            return (False, f"{r.status_code}:{desc}")
        return (True, "")
    except Exception as e:
        return (False, str(e))


# ===== debug dump (опционально) =====

def _dump(name: str, html_text: str):
    try:
        with open(name, "w", encoding="utf-8") as f:
            f.write(html_text)
        log.debug("dumped %s (%d bytes)", name, len(html_text))
    except Exception:
        pass


# ===== CHP page scraping =====

def get_soup(session: requests.Session) -> BS:
    r = http_get(session, BASE_URL, headers={"User-Agent": random.choice(UA_POOL)})
    _dump("page_before.html", r.text)
    return BS(r.text, "html.parser")

def _collect_form_fields(soup: BS) -> dict:
    data = {}
    for name in ["__EVENTTARGET", "__EVENTARGUMENT", "__LASTFOCUS",
                 "__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"]:
        tag = soup.find("input", {"name": name})
        if tag and tag.has_attr("value"):
            data[name] = tag["value"]
        else:
            data.setdefault(name, "")
    return data

def _find_control_names(soup: BS) -> dict:
    """
    Находим имена ключевых контролов:
    - dropdown центра (ddlCommCenters)
    - ScriptManager (для AJAX)
    - UpdatePanel (контейнер с сеткой)
    - submit-кнопки с надписью OK/Apply/Go/View/Load
    """
    names = {}

    sel = soup.find("select", {"name": re.compile(r"ddlCommCenters", re.I)})
    if sel:
        names["ddl_name"] = sel.get("name")
        names["ddl_id"] = sel.get("id")
        names["ddl_elem"] = sel

    sm = soup.find("input", {"name": re.compile(r"ScriptManager", re.I)})
    if sm:
        names["sm_name"] = sm.get("name")
        names["sm_id"] = sm.get("id")

    up = soup.find(attrs={"id": re.compile(r"UpdatePanel", re.I)}) or soup.find("div", {"id": re.compile(r"MainContent", re.I)})
    if up:
        names["up_id"] = up.get("id")

    btns = []
    for inp in soup.find_all(["input", "button"]):
        if inp.name == "input" and inp.get("type", "").lower() not in ("submit", "button"):
            continue
        label = (inp.get("value") or inp.get_text() or "").strip()
        if re.search(r"\b(OK|Apply|Go|View|Load)\b", label, re.I):
            nm = inp.get("name")
            if nm:
                btns.append(nm)
    names["ok_buttons"] = btns

    return names

def post_select_center(session: requests.Session, soup: BS, center: str) -> BS:
    """
    Пытаемся выбрать Communications Center:
      План A: обычный submit с dropdown + любой 'OK/Apply/Go/View/Load' кнопкой.
      План B: обычный postback с __EVENTTARGET = ddlCommCenters (non-AJAX).
      План C: AJAX postback (ScriptManager + __ASYNCPOST=true) + добивающий GET.
    """
    names = _find_control_names(soup)
    ddl = names.get("ddl_elem")
    ddl_name = names.get("ddl_name")
    selected_val = None

    # найдём value нужного option
    if ddl:
        for opt in ddl.find_all("option"):
            if opt.text.strip().lower() == center.strip().lower():
                selected_val = opt.get("value", opt.text.strip())
                break
    if not selected_val:
        selected_val = center

    # План A: submit с кнопкой
    if names.get("ok_buttons") and ddl_name:
        for btn_name in names["ok_buttons"]:
            data = _collect_form_fields(soup)
            data.update({
                ddl_name: selected_val,
                btn_name: "OK",
            })
            r = http_post(session, BASE_URL, data=data, headers={"User-Agent": random.choice(UA_POOL)})
            _dump("page_after_center_btn.html", r.text)
            s2 = BS(r.text, "html.parser")
            if s2.find("table", {"id": re.compile(r"gvIncidents", re.I)}):
                return s2

    # План B: __EVENTTARGET на dropdown
    if ddl_name:
        data = _collect_form_fields(soup)
        data.update({
            ddl_name: selected_val,
            "__EVENTTARGET": ddl_name,
            "__EVENTARGUMENT": "",
        })
        r = http_post(session, BASE_URL, data=data, headers={"User-Agent": random.choice(UA_POOL)})
        _dump("page_after_center_evt.html", r.text)
        s2 = BS(r.text, "html.parser")
        if s2.find("table", {"id": re.compile(r"gvIncidents", re.I)}):
            return s2

    # План C: AJAX (ScriptManager + __ASYNCPOST)
    sm_name = names.get("sm_name")
    up_id = names.get("up_id")
    ddl_id = names.get("ddl_id")
    if sm_name and ddl_name and (up_id or ddl_id):
        data = _collect_form_fields(soup)
        data.update({
            ddl_name: selected_val,
            "__EVENTTARGET": ddl_name,
            "__EVENTARGUMENT": "",
            "__ASYNCPOST": "true",
            sm_name: f"{up_id or ''}|{ddl_id or ddl_name}",
        })
        headers = {
            "User-Agent": random.choice(UA_POOL),
            "X-MicrosoftAjax": "Delta=true"
        }
        r = http_post(session, BASE_URL, data=data, headers=headers)
        _dump("page_after_center_ajax.html", r.text)
        # добивающий GET — сервер уже применил выбранный центр
        r2 = http_get(session, BASE_URL, headers={"User-Agent": random.choice(UA_POOL)})
        _dump("page_after_center_get.html", r2.text)
        s3 = BS(r2.text, "html.parser")
        if s3.find("table", {"id": re.compile(r"gvIncidents", re.I)}):
            return s3

    # если ничего не сработало — вернём исходный soup (парсер увидит warning)
    return soup

def do_postback(session: requests.Session, soup: BS, target: str) -> BS:
    data = _collect_form_fields(soup)
    data.update({
        "__EVENTTARGET": target,
        "__EVENTARGUMENT": "",
    })
    time.sleep(random.uniform(0.5, 1.5))  # маленький джиттер
    r = http_post(session, BASE_URL, data=data, headers={"User-Agent": random.choice(UA_POOL)})
    _dump("page_details.html", r.text)
    return BS(r.text, "html.parser")

def parse_table_rows(soup: BS) -> List[dict]:
    out = []
    grid = soup.find("table", {"id": re.compile(r"gvIncidents", re.I)})
    if not grid:
        log.warning("incidents grid not found after center select")
        return out

    header = grid.find("tr")
    ths = [th.get_text(" ", strip=True) for th in (header.find_all("th") if header else [])]
    cols = {name.lower(): idx for idx, name in enumerate(ths)}

    def idx(key: str, default: int) -> int:
        for k in cols:
            if key in k:
                return cols[k]
        return default

    i_details = 0
    i_no      = idx("no", 1)
    i_time    = idx("time", 2)
    i_type    = idx("type", 3)
    i_loc     = idx("location", 4)
    i_locd    = idx("loc", 5)   # Location Desc.
    i_area    = idx("area", 6)

    body = grid.find("tbody") or grid
    for tr in body.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 6:
            continue
        a = tds[i_details].find("a")
        if not a:
            continue
        href = a.get("href", "")
        m = re.search(r"__doPostBack\('([^']+)'", href)
        target = m.group(1) if m else None

        def get(i):
            return tds[i].get_text(" ", strip=True) if i < len(tds) else ""

        out.append({
            "no": get(i_no),
            "time": get(i_time),
            "type": get(i_type),
            "location": get(i_loc),
            "locdesc": get(i_locd),
            "area": get(i_area),
            "target": target
        })
    return out

def parse_details_panel(soup: BS) -> tuple[Optional[Tuple[float,float]], List[str]]:
    """Возвращает (latlon, detail_lines). detail_lines — уже очищенные строки Detail Information."""
    # координаты из ссылки
    latlon = None
    coord_link = None
    for a in soup.find_all("a"):
        href = a.get("href", "")
        if "google.com/maps/place" in href or "google.com/maps/search" in href:
            coord_link = href
            break
    if coord_link:
        mm = re.search(r"([-+]?\d+\.\d+)[ ,]+([-+]?\d+\.\d+)", coord_link)
        if mm:
            try:
                latlon = (float(mm.group(1)), float(mm.group(2)))
            except Exception:
                latlon = None

    # строки Detail Information
    details_lines = []
    marker = None
    for b in soup.find_all(text=re.compile(r"Detail Information", re.I)):
        marker = b.parent
        break

    if marker:
        cur = marker.parent
        text_region = []
        for el in cur.next_siblings:
            if getattr(el, "get_text", None):
                tx = el.get_text("\n", strip=True)
                if re.search(r"Unit Information", tx, re.I):
                    break
                if tx:
                    text_region.append(tx)
        joined = "\n".join(text_region)
        for ln in joined.splitlines():
            s = ln.strip()
            if not s:
                continue
            if s.startswith("Click on Details"):
                break
            if s.startswith("| Contact Us") or s.endswith("CHP Mobile Traffic") or s.endswith("CHP Home Page"):
                continue
            details_lines.append(s)

    return latlon, details_lines


# ===== Details → facts =====

VEHICLE_TAG_MAP = [
    (re.compile(r'\bMC\b|MOTORCYCLE', re.I), "мотоцикл"),
    (re.compile(r'\bSEMI\b|BIG\s*RIG|TRACTOR\s*TRAILER', re.I), "фура"),
    (re.compile(r'\bTRK\b|TRUCK\b', re.I), "грузовик"),
    (re.compile(r'\bPK\b|PICK[\s\-]?UP', re.I), "пикап"),
    (re.compile(r'\bSUV\b', re.I), "SUV"),
    (re.compile(r'\bVAN\b|MINI\s*VAN|MINIVAN', re.I), "минивэн"),
    (re.compile(r'\bSEDAN\b', re.I), "седан"),
]

LANE_RE = re.compile(r'#\s*(\d)', re.I)
HOV_RE  = re.compile(r'\bHOV\b', re.I)

RS_RE   = re.compile(r'\bRHS?\b|\bR\.?S\b|RIGHT\s+SHOULDER', re.I)
LS_RE   = re.compile(r'\bLHS?\b|\bL\.?S\b|LEFT\s+SHOULDER', re.I)
CD_RE   = re.compile(r'\bCD\b|CENTER\s+DIVIDER|CENTER\s+DIV', re.I)
DIRT_RE = re.compile(r'DIRT\s+AREA', re.I)

ONR_RE  = re.compile(r'\bON[-\s]?RAMP\b|\bONR\b', re.I)
OFFR_RE = re.compile(r'\bOFF[-\s]?RAMP\b|\bOFFR\b', re.I)
EXIT_RE = re.compile(r'\bEXIT\b', re.I)

DRIVEABLE_Y = re.compile(r'\bDRIV?E?ABLE\b|\bDRIVABLE\b|\bABLE\s+TO\s+DRIVE', re.I)
DRIVEABLE_N = re.compile(r'\bNOT\s+DRIVABLE\b|\bUNDRIVABLE\b', re.I)

CHP_ON_RE   = re.compile(r'\bCHP\b.*\b97\b|\b97\b.*\bCHP\b', re.I)
CHP_ENRT_RE = re.compile(r'\bCHP\b.*ENR[T]?\b|\bENR[T]?\b.*\bCHP\b', re.I)
FIRE_RE     = re.compile(r'\b1141\b|\bFIRE\b|\bMEDIC(S)?\b|AMBU?LANCE', re.I)

TOW_REQ_RE  = re.compile(r'\b1185\b.*\b(REQ|REQUEST|RQST)\b|\bTOW\b.*(REQ|REQUEST|RQST)', re.I)
TOW_ENR_RE  = re.compile(r'\b1185\b.*\bENR[T]?\b', re.I)
TOW_ON_RE   = re.compile(r'\b1185\b.*\b97\b', re.I)

BLOCK_RE    = re.compile(r'\bBLOCK(ING|ED)?\b|LANE(S)?\s+BLOCK(ED|ING)', re.I)
AT_LEAST_TWO = [
    re.compile(r'\bVS\b', re.I),
    re.compile(r'\bX\b', re.I),
    re.compile(r'\b2(ND)?\s+VEH\b', re.I),
    re.compile(r'\bBOTH\s+VEH', re.I),
]

def parse_details_to_facts(detail_lines: List[str]) -> dict:
    facts = {
        "vehicles": None,
        "vehicle_tags": set(),  # type: Set[str]
        "loc_label": None,
        "lane_nums": set(),
        "hov": False,
        "ramp": None,
        "driveable": None,
        "chp_on": False,
        "chp_enrt": False,
        "fire_on": False,
        "tow": None,
        "blocked": False,          # в БД можно хранить, в тексте не показываем
        "last_time_hint": None,
    }

    veh_count = 0
    at_least_two_flag = False

    for raw in detail_lines:
        line = raw.strip()
        if not line:
            continue

        m = re.match(r'^\s*([0-9]{1,2}:[0-9]{2}\s*(?:AM|PM))\b', line, re.I)
        if m:
            facts["last_time_hint"] = m.group(1)

        for rx, tag in VEHICLE_TAG_MAP:
            if rx.search(line):
                facts["vehicle_tags"].add(tag)

        m = re.search(r'\b(\d+)\s*(?:VEH|VEHS|VEHICLES|CARS|TCs?)\b', line, re.I)
        if m:
            veh_count = max(veh_count, int(m.group(1)))

        if any(rx.search(line) for rx in AT_LEAST_TWO):
            at_least_two_flag = True

        for mm in LANE_RE.findall(line):
            facts["lane_nums"].add(str(mm))
        if HOV_RE.search(line):
            facts["hov"] = True

        if RS_RE.search(line):
            facts["loc_label"] = "правая обочина"
        elif LS_RE.search(line):
            facts["loc_label"] = "левая обочина"
        elif CD_RE.search(line):
            facts["loc_label"] = "центральная разделительная"
        elif DIRT_RE.search(line):
            facts["loc_label"] = "обочина (грязь)"

        if ONR_RE.search(line):
            facts["ramp"] = "on-ramp"
        elif OFFR_RE.search(line):
            facts["ramp"] = "off-ramp"
        elif EXIT_RE.search(line):
            facts["ramp"] = "exit"

        if DRIVEABLE_Y.search(line):
            facts["driveable"] = True
        elif DRIVEABLE_N.search(line):
            facts["driveable"] = False

        if CHP_ON_RE.search(line):
            facts["chp_on"] = True
        elif CHP_ENRT_RE.search(line):
            facts["chp_enrt"] = True
        if FIRE_RE.search(line):
            facts["fire_on"] = True

        if TOW_ON_RE.search(line):
            facts["tow"] = "on_scene"
        elif TOW_ENR_RE.search(line):
            if facts["tow"] != "on_scene":
                facts["tow"] = "enroute"
        elif TOW_REQ_RE.search(line):
            if facts["tow"] not in ("on_scene", "enroute"):
                facts["tow"] = "requested"

        if BLOCK_RE.search(line):
            facts["blocked"] = True

    if veh_count == 0 and at_least_two_flag:
        veh_count = 2
    facts["vehicles"] = veh_count or None

    return facts


# ===== Formatting (summary + message) =====

def _compact_lanes(lanes: set) -> str:
    if not lanes:
        return ""
    nums = sorted(int(x) for x in lanes if str(x).isdigit())
    if not nums:
        return ""
    spans = []
    a = b = nums[0]
    for n in nums[1:]:
        if n == b + 1:
            b = n
            continue
        spans.append((a, b))
        a = b = n
    spans.append((a, b))
    parts = [f"#{x}" if x == y else f"#{x}–#{y}" for x, y in spans]
    return ", ".join(parts)

def _unique_join(parts: list, sep: str = ", ") -> str:
    seen, out = set(), []
    for p in parts:
        p = (p or "").strip()
        if not p or p in seen:
            continue
        seen.add(p); out.append(p)
        # dedup
    return sep.join(out)

def human_summary_from_facts(facts: dict) -> tuple[str, set]:
    consumed = set()
    bits = []

    v = facts.get("vehicles")
    tags = list(sorted(facts.get("vehicle_tags") or []))
    if v and v > 0:
        if tags:
            bits.append(f"{v} маш. ({', '.join(tags)})")
            consumed.update({"vehicles", "vehicle_tags"})
        else:
            bits.append(f"{v} маш.")
            consumed.add("vehicles")
    elif tags:
        bits.append(_unique_join(tags, " / "))
        consumed.add("vehicle_tags")

    where = []
    if facts.get("ramp"): where.append("съезд")
    if facts.get("loc_label"): where.append(facts["loc_label"])
    lanes = _compact_lanes(facts.get("lane_nums") or set())
    if lanes: where.append(f"полоса {lanes}")
    if where:
        bits.append(_unique_join(where, ", "))
        consumed.update({"ramp", "loc_label", "lane_nums"})

    if facts.get("driveable") is True:
        bits.append("на ходу"); consumed.add("driveable")
    elif facts.get("driveable") is False:
        bits.append("не на ходу"); consumed.add("driveable")

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
        consumed.add("chp_enrt")

    if facts.get("fire_on"):
        bits.append("медики/пожарные на месте")
        consumed.add("fire_on")

    summary = _unique_join(bits, ", ")
    return summary, consumed

def blockquote_from_lines(lines: List[str], limit_chars: int) -> str:
    if limit_chars <= 0 or not lines:
        return ""
    out, total = [], 0
    for ln in lines:
        s = ln.strip()
        if not s:
            continue
        # "8:29 AM 2 [..]" -> "8:29 AM: .."
        s = re.sub(r'^\s*([0-9]{1,2}:[0-9]{2}\s*(?:AM|PM))\s+\d+\s+', r'\1: ', s, flags=re.I)
        s = html.escape(s)
        chunk = f"› {s}\n"
        if total + len(chunk) > limit_chars:
            out.append("… (truncated)")
            break
        out.append(chunk); total += len(chunk)
    return "".join(out)

def make_text(inc: Dict[str, str],
              latlon: Optional[Tuple[float, float]],
              details_lines_clean: List[str],
              facts: dict,
              closed: bool = False) -> str:

    icon = ""
    t = inc.get('type', '')
    if "Collision" in t:
        icon = "🚨"
    elif "Hit" in t and "Run" in t:
        icon = "🚗"

    head = (
        f"⏳ {html.escape(inc['time'])} | 🏙 {html.escape(inc['area'])}\n"
        f"{icon} {html.escape(t)}\n\n"
        f"📍 {html.escape(inc['location'])} — {html.escape(inc['locdesc'])}"
    )

    summary_line, consumed = human_summary_from_facts(facts)

    markers = []
    loc_bits = []
    if "loc_label" not in consumed and facts.get("loc_label"):
        loc_bits.append(facts["loc_label"])
    if "ramp" not in consumed and facts.get("ramp"):
        loc_bits.append("съезд")
    if "lane_nums" not in consumed and facts.get("lane_nums"):
        lane = _compact_lanes(facts["lane_nums"])
        if lane: loc_bits.append(f"полоса {lane}")
    if facts.get("hov"): loc_bits.append("HOV")
    if loc_bits: markers.append(_unique_join(loc_bits, " · "))

    veh_bits = []
    if "vehicles" not in consumed and facts.get("vehicles") is not None:
        veh_bits.append(f"{facts['vehicles']} ТС")
    if "vehicle_tags" not in consumed and facts.get("vehicle_tags"):
        veh_bits.append(", ".join(sorted(facts["vehicle_tags"])))
    if veh_bits: markers.append(" / ".join(veh_bits))

    st_bits = []
    if "chp_on" not in consumed and facts.get("chp_on"):
        st_bits.append("офицеры CHP на месте")
    elif "chp_enrt" not in consumed and facts.get("chp_enrt"):
        st_bits.append("офицеры CHP в пути")
    if "fire_on" not in consumed and facts.get("fire_on"):
        st_bits.append("медики/пожарные")
    if "tow" not in consumed and facts.get("tow"):
        if facts["tow"] == "requested": st_bits.append("эвакуатор вызван")
        elif facts["tow"] == "enroute": st_bits.append("эвакуатор в пути")
        elif facts["tow"] == "on_scene": st_bits.append("эвакуатор на месте")
    if "driveable" not in consumed:
        if facts.get("driveable") is True:  st_bits.append("на ходу")
        elif facts.get("driveable") is False: st_bits.append("не на ходу")
    if st_bits: markers.append(_unique_join(st_bits, ", "))

    facts_block_lines = []
    if summary_line: facts_block_lines.append(summary_line)
    if markers:      facts_block_lines.append(" | ".join(markers))
    facts_block = "\n\n<b>📌 Расположение / Машины:</b>\n" + "\n".join(facts_block_lines) if facts_block_lines else ""

    # карта: всегда ссылка-метка
    if latlon:
        lat, lon = latlon
        map_url = f"https://www.google.com/maps/search/?api=1&query={lat:.6f},{lon:.6f}"
        map_block = f"\n\n<b>🗺️ Карта:</b>\n{map_url}"
    else:
        map_block = "\n\n<b>🗺️ Карта:</b>\nКоординаты недоступны"

    skeleton = head + facts_block + map_block

    footer_len = len("\n\n<b>📝 Detail Information:</b>\n") + (len("\n\n<b>❗️ Инцидент закрыт CHP</b>") if closed else 0)
    leftover = TG_HARD_LIMIT - len(skeleton) - footer_len
    cap = max(0, min(MAX_DETAIL_CHARS_BASE, leftover))

    details_block = blockquote_from_lines(details_lines_clean, cap) if cap > 0 else ""
    det_block = f"\n\n<b>📝 Detail Information:</b>\n{details_block}" if details_block else ""

    text = skeleton + det_block
    if closed:
        text += "\n\n<b>❗️ Инцидент закрыт CHP</b>"

    # страховка: если внезапно перебрали лимит
    if len(text) > TG_HARD_LIMIT and det_block:
        shrink = int(cap * 0.8)
        details_block = blockquote_from_lines(details_lines_clean, max(0, shrink))
        det_block = f"\n\n<b>📝 Detail Information:</b>\n{details_block}" if details_block else ""
        text = skeleton + det_block
        if closed:
            text += "\n\n<b>❗️ Инцидент закрыт CHP</b>"

    return text


# ===== Main loop =====

def main():
    state = load_state()
    purge_old_state(state)

    with requests.Session() as s:
        s.headers.update({"User-Agent": random.choice(UA_POOL)})

        while True:
            try:
                soup0 = get_soup(s)
                soup = post_select_center(s, soup0, COMM_CENTER)
                rows = parse_table_rows(soup)

                seen_ids = set()
                for row in rows:
                    if not TYPE_RE.search(row["type"]):
                        log.debug("skip by type: %s", row["type"])
                        continue

                    inc_id = compose_incident_key(COMM_CENTER, row["no"])
                    seen_ids.add(inc_id)

                    if not row.get("target"):
                        continue
                    soup_det = do_postback(s, soup, row["target"])
                    latlon, detail_lines = parse_details_panel(soup_det)
                    detail_lines_clean = detail_lines[:]

                    facts = parse_details_to_facts(detail_lines_clean)
                    text = make_text(row, latlon, detail_lines_clean, facts, closed=False)
                    sig = hash((text,))

                    st = state.get(inc_id)

                    # если чат сменился — форсим новое сообщение
                    if st and st.get("chat_id") and st["chat_id"] != TELEGRAM_CHAT_ID:
                        log.warning("Chat changed for %s: %s -> %s; resending",
                                    inc_id, st["chat_id"], TELEGRAM_CHAT_ID)
                        st = None

                    if not st:
                        mid = tg_send(text, chat_id=TELEGRAM_CHAT_ID)
                        if mid:
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
                            log.info("new %s (%s)", row["no"], row["type"])
                        else:
                            log.error("send failed for %s", inc_id)
                        continue

                    # обновление
                    st["misses"] = 0
                    st["last_seen"] = dt.datetime.utcnow().isoformat()
                    if st.get("last_sig") != sig or st.get("closed"):
                        ok, reason = tg_edit(st["message_id"], text, chat_id=st.get("chat_id") or TELEGRAM_CHAT_ID)
                        if not ok:
                            if "message to edit not found" in reason.lower():
                                log.warning("Edit failed (not found). Resending new message for %s", inc_id)
                                mid = tg_send(text, chat_id=TELEGRAM_CHAT_ID)
                                if mid:
                                    st["message_id"] = mid
                                    st["chat_id"] = TELEGRAM_CHAT_ID
                                    st["last_sig"] = sig
                                    st["last_text"] = text
                                    st["closed"] = False
                                    st["misses"] = 0
                                    st["last_seen"] = dt.datetime.utcnow().isoformat()
                                    log.info("reposted %s (%s)", row["no"], row["type"])
                                else:
                                    log.error("Resend also failed for %s", inc_id)
                            else:
                                log.error("Edit failed for %s: %s", inc_id, reason)
                        else:
                            st["last_sig"] = sig
                            st["last_text"] = text
                            st["closed"] = False
                            log.info("edited %s (%s)", row["no"], row["type"])

                # закрытие пропавших
                for inc_id, st in list(state.items()):
                    parts = inc_id.split(":")
                    if len(parts) < 3 or parts[1] != today_str():
                        continue
                    if inc_id not in seen_ids and not st.get("closed"):
                        st["misses"] = st.get("misses", 0) + 1
                        if st["misses"] >= MISSES_TO_CLOSE:
                            try:
                                final = st.get("last_text", "")
                                if final:
                                    if len(final) + len("\n\n<b>❗️ Инцидент закрыт CHP</b>") <= TG_HARD_LIMIT:
                                        final += "\n\n<b>❗️ Инцидент закрыт CHP</b>"
                                    ok, _ = tg_edit(st["message_id"], final, chat_id=st.get("chat_id") or TELEGRAM_CHAT_ID)
                                    if ok:
                                        log.info("closed %s", parts[-1])
                            except Exception:
                                pass
                            st["closed"] = True

                save_state(state)

                # джиттер в главном цикле
                time.sleep(POLL_INTERVAL + random.uniform(2.0, 5.0))

            except Exception as e:
                log.exception("loop error: %s", e)
                time.sleep(5)


if __name__ == "__main__":
    main()
