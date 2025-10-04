#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CHP Traffic -> Telegram notifier (v6.2 rich-facts)
- Один инцидент = одно сообщение; при изменениях редактируем (type, details, извлечённые факты).
- По исчезновению несколько циклов подряд помечаем "❗️ Инцидент закрыт CHP".
- Detail Information сжимаем до "HH:MM AM/PM: текст".
- Карта — прямая ссылка (URL).
- Блок "📌 Расположение / Машины" теперь строится из расширенных фактов:
  * Сколько ТС (по X VEH/VEHS, SOLO VEH, VS), типы ТС (MC/SEMI/TRK/PK)
  * Место: правая/левая обочина, CD, съезд (on/off/exit), HOV, полосы #1/#2..., блокировки
  * Службы: CHP (97/enrt), FIRE/1141, эвакуатор 1185 (req/enrt/97)
  * Driveable / NOT driveable
"""

import os
import re
import time
import json
import html
import hashlib
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

# фильтр типов (коллизии + хит-энд-ран по умолчанию)
TYPE_REGEX = os.getenv("TYPE_REGEX", r"(Collision|Hit\s*(?:&|and)\s*Run)")
AREA_REGEX = os.getenv("AREA_REGEX", r"")
LOCATION_REGEX = os.getenv("LOCATION_REGEX", r"")

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))
SEEN_FILE = os.getenv("SEEN_FILE", "seen.json")
MAX_DETAIL_CHARS = int(os.getenv("MAX_DETAIL_CHARS", "2500"))
MISSES_TO_CLOSE = int(os.getenv("MISSES_TO_CLOSE", "2"))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36"
}

# ---------- Telegram ----------
def tg_send(text: str, chat_id: Optional[str] = None) -> Optional[int]:
    chat_id = (chat_id or TELEGRAM_CHAT_ID).strip()
    if not TELEGRAM_TOKEN or not chat_id:
        print("[WARN] TELEGRAM_TOKEN/CHAT_ID не заданы. Не отправляю:\n", text)
        return None
    api = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
        "parse_mode": "HTML",
    }
    r = requests.post(api, data=payload, timeout=20)
    if r.status_code != 200:
        print("[ERR] Telegram send:", r.status_code, r.text[:400])
        return None
    data = r.json()
    try:
        return int(data["result"]["message_id"])
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
        "parse_mode": "HTML",
    }
    r = requests.post(api, data=payload, timeout=20)
    if r.status_code != 200:
        print("[ERR] Telegram edit:", r.status_code, r.text[:400])
        return False
    return True

# ---------- state (для редактирования) ----------
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

def choose_communications_center(session: requests.Session, center_name: str) -> str:
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
            "no": cols[1].get_text(strip=True),         # уникальный ID строки на сутки
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
    """Сырые строки блока 'Detail Information' (между заголовком и 'Unit Information'/'Close')."""
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

# --- Сгущение Detail Information до "HH:MM AM/PM: текст" ---
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

def details_block_from_lines(lines: List[str]) -> str:
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
    payload = base_payload.copy()
    payload["__EVENTTARGET"] = target
    payload["__EVENTARGUMENT"] = argument
    post_url = requests.compat.urljoin(BASE_URL, action_url)
    r = session.post(post_url, data=payload, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    coords = extract_coords_from_details_html(soup)
    lines = extract_detail_lines(soup)
    details_block_html = details_block_from_lines(lines) if lines else None
    return coords, details_block_html, lines

# ---------- Расширенный парсер фактов ----------
BARRIER_WORDS = {"BARRIER", "GUARDRAIL", "FENCE", "DEBRIS", "ANIMAL", "DEER", "TREE", "POLE", "SIGN"}

def parse_rich_facts(detail_lines: Optional[List[str]]) -> dict:
    """
    Возвращает словарь фактов, вычитанных из Detail Information.
    Ключи:
      vehicles (int|None)         — оценка числа ТС
      vehicle_tags (set[str])     — {'MC','SEMI','TRK','PK'}
      loc_label (str|None)        — 'правая обочина' | 'левая обочина' | 'CD'
      lane_nums (set[str])        — {'1','2','3'} если упоминаются #1/#2/#3
      hov (bool)                  — HOV упомянут
      blocked (bool)              — BLKG/BLOCKING/ALL LNS STOPPED/1125 IN #
      ramp (str|None)             — 'on-ramp' | 'off-ramp' | 'exit'
      driveable (True|False|None) — машины на ходу?
      chp_on (bool)               — CHP на месте (97)
      chp_enrt (bool)             — CHP в пути (ENRT)
      fire_on (bool)              — FIRE/1141
      tow (str|None)              — 'requested' | 'enroute' | 'on_scene'
    """
    facts = {
        "vehicles": None,
        "vehicle_tags": set(),
        "loc_label": None,
        "lane_nums": set(),
        "hov": False,
        "blocked": False,
        "ramp": None,
        "driveable": None,
        "chp_on": False,
        "chp_enrt": False,
        "fire_on": False,
        "tow": None,
    }
    if not detail_lines:
        return facts

    text = " ".join(detail_lines).upper()

    # --- место / полосы / hov / блокировки
    if re.search(r"\bRS\b|\bRIGHT SHOULDER\b", text):
        facts["loc_label"] = "правая обочина"
    if re.search(r"\bLS\b|\bLEFT SHOULDER\b", text):
        facts["loc_label"] = "левая обочина"
    if re.search(r"\bCD\b|\bCENTER DIVIDER\b", text):
        facts["loc_label"] = "CD"
    if re.search(r"\bON[- ]?RAMP\b", text):
        facts["ramp"] = "on-ramp"
    if re.search(r"\bOFF[- ]?RAMP\b", text):
        facts["ramp"] = "off-ramp"
    if re.search(r"\bEXIT\b", text):
        facts["ramp"] = "exit"
    if re.search(r"\bHOV\b", text):
        facts["hov"] = True

    for m in re.finditer(r"#\s*(\d+)", text):
        facts["lane_nums"].add(m.group(1))
    if re.search(r"\bBLKG?\b|\bBLOCK(ED|ING)\b|\bALL LNS STOPPED\b", text):
        facts["blocked"] = True
    if re.search(r"\b1125\b\s+(IN|#)", text):
        facts["blocked"] = True

    # --- виды ТС
    if re.search(r"\bMC\b|\bMOTORCYCLE\b", text): facts["vehicle_tags"].add("MC")
    if re.search(r"\bSEMI\b|\bBIG\s*RIG\b|\bTRACTOR TRAILER\b", text): facts["vehicle_tags"].add("SEMI")
    if re.search(r"\bTRK\b|\bTRUCK\b", text): facts["vehicle_tags"].add("TRK")
    if re.search(r"\bPK\b|\bPICK ?UP\b", text): facts["vehicle_tags"].add("PK")

    # --- число ТС
    nums = [int(n) for n in re.findall(r"\b(\d{1,2})\s*VEHS?\b", text)]
    if nums:
        facts["vehicles"] = max(nums)
    elif "SOLO VEH" in text:
        facts["vehicles"] = 1
    else:
        vs_line = None
        for ln in detail_lines:
            if re.search(r"\bVS\b", ln.upper()):
                vs_line = ln.upper(); break
        if vs_line:
            parts = [p for p in re.split(r"\bVS\b", vs_line) if p.strip()]
            if len(parts) >= 2:
                facts["vehicles"] = max(facts["vehicles"] or 0, len(parts))

    # --- driveable
    if re.search(r"\bNOT\s*DRIV(?:E|)ABLE\b|\bUNABLE TO MOVE VEH", text):
        facts["driveable"] = False
    elif re.search(r"\bVEH\s+IS\s+DRIVABLE\b|\bDRIVABLE\b", text):
        facts["driveable"] = True

    # --- CHP/FIRE/ENRT
    if re.search(r"\b97\b", text):
        facts["chp_on"] = True
    if re.search(r"\bENRT\b", text):
        facts["chp_enrt"] = True
    if re.search(r"\bFIRE\b|\b1141\b", text):
        # если 97 где-то в строках FIRE, считаем на месте
        facts["fire_on"] = True if re.search(r"\bFIRE.*97\b|\b1141.*97\b", text) else facts["fire_on"]

    # --- 1185 tow
    if re.search(r"\bREQ\s+1185\b|\bSTART\s+1185\b", text):
        facts["tow"] = "requested"
    if re.search(r"\b1185\b.*\bENRT\b", text):
        facts["tow"] = "enroute"
    if re.search(r"\b1185\s+97\b|\bTOW\b.*\b97\b", text):
        facts["tow"] = "on_scene"

    return facts

# ---------- формат/фильтры ----------
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

def make_text(inc: Dict[str, str],
              latlon: Optional[Tuple[float, float]],
              details_block: Optional[str],
              facts: dict,
              closed: bool = False) -> str:
    # эмодзи по типу – во второй строке
    icon = ""
    if "Collision" in inc['type']:
        icon = "🚨"
    elif "Hit" in inc['type'] and "Run" in inc['type']:
        icon = "🚗"

    # первая строка — время и area (другие эмодзи для времени/ареа)
    title = (
        f"⏱ {html.escape(inc['time'])} | 🏙 {html.escape(inc['area'])}\n"
        f"{icon} {html.escape(inc['type'])}\n\n"
        f"📍 {html.escape(inc['location'])} — {html.escape(inc['locdesc'])}"
    )

    # --- компактный блок фактов ---
    lines = []

    # 1) Расположение/полосы/блокировки
    loc_bits = []
    if facts.get("loc_label"): loc_bits.append(facts["loc_label"])
    if facts.get("ramp"): loc_bits.append(facts["ramp"])
    if facts.get("lane_nums"):
        loc_bits.append("#" + ",".join(sorted(facts["lane_nums"])))
    if facts.get("hov"): loc_bits.append("HOV")
    if facts.get("blocked"): loc_bits.append("блокировки")
    if loc_bits:
        lines.append(" · ".join(loc_bits))

    # 2) Машины: число + типы
    veh_bits = []
    v = facts.get("vehicles")
    if v is not None: veh_bits.append(f"{v} ТС")
    tags = facts.get("vehicle_tags") or set()
    if tags:
        veh_bits.append(", ".join(sorted(tags)))
    if veh_bits:
        lines.append(" / ".join(veh_bits))

    # 3) Службы и driveable
    st_bits = []
    if facts.get("chp_on"): st_bits.append("CHP 97")
    elif facts.get("chp_enrt"): st_bits.append("CHP enrt")
    if facts.get("fire_on"): st_bits.append("FIRE/1141")
    tow = facts.get("tow")
    if tow == "requested": st_bits.append("tow req")
    elif tow == "enroute": st_bits.append("tow enrt")
    elif tow == "on_scene": st_bits.append("tow 97")
    if facts.get("driveable") is True: st_bits.append("driveable")
    elif facts.get("driveable") is False: st_bits.append("NOT driveable")
    if st_bits:
        lines.append(", ".join(st_bits))

    if lines:
        title += "\n\n<b>📌 Расположение / Машины:</b>\n" + " | ".join(lines)

    # маршрут
    if latlon:
        lat, lon = latlon
        url = f"https://www.google.com/maps/dir/?api=1&destination={lat:.6f},{lon:.6f}&travelmode=driving"
        title += f"\n\n<b>🗺️ Маршрут:</b>\n{url}"
    else:
        title += "\n\n<b>🗺️ Маршрут:</b>\nКоординаты недоступны"

    # детали — сжатый blockquote
    if details_block:
        title += f"\n\n<b>📝 Detail Information:</b>\n{details_block}"

    if closed:
        title += "\n\n<b>❗️ Инцидент закрыт CHP</b>"

    return title

def signature_for_update(inc: Dict[str, str], details_block: Optional[str], facts: dict) -> str:
    # нормализуем детали и факты, чтобы редактировать только при реальных изменениях
    norm_details = (details_block or "").replace("\u200b", "").strip()
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
    base = (inc.get("type","").strip() + "||" + norm_details + "||" + fact_key).encode("utf-8","ignore")
    return hashlib.sha1(base).hexdigest()

# ---------- главный цикл ----------
def main() -> None:
    print(f"[INFO] CHP notifier v6.2 started. Center: {COMM_CENTER} | Interval: {POLL_INTERVAL}s")
    state = load_state()
    session = requests.Session()

    while True:
        cycle_seen_ids = set()
        try:
            html = choose_communications_center(session, COMM_CENTER)
            soup, incidents = parse_incidents_with_postbacks(html)
            action_url, base_payload = extract_form_state(soup)
            filtered = filter_collisions(incidents)

            for inc in filtered:
                inc_id = inc["no"]  # уникальный ID на текущие сутки
                cycle_seen_ids.add(inc_id)

                # тянем детали
                latlon = None
                details_block = None
                detail_lines = None
                if inc.get("postback"):
                    latlon, details_block, detail_lines = fetch_details_by_postback(
                        session, action_url, base_payload,
                        inc["postback"]["target"], inc["postback"]["argument"]
                    )

                # извлекаем расширенные факты
                facts = parse_rich_facts(detail_lines)

                # формируем текст и подпись
                text = make_text(inc, latlon, details_block, facts, closed=False)
                sig = signature_for_update(inc, details_block, facts)

                st = state.get(inc_id)
                if st and st.get("message_id"):
                    # было ли изменение?
                    if st.get("last_sig") != sig or st.get("closed", False):
                        ok = tg_edit(st["message_id"], text, chat_id=st.get("chat_id") or TELEGRAM_CHAT_ID)
                        if ok:
                            st["last_sig"] = sig
                            st["last_text"] = text
                            st["closed"] = False
                    st["misses"] = 0
                    st["last_seen"] = dt.datetime.utcnow().isoformat()
                else:
                    # новое событие
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

            # обработка потенциально закрытых
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

            save_state(state)
            print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {COMM_CENTER}: rows={len(incidents)}, matched={len(filtered)}, tracked={len(state)}")

        except KeyboardInterrupt:
            print("\n[INFO] Stopped by user.")
            break
        except Exception as e:
            print("[ERR] loop error:", e)

        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
