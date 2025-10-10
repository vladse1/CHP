#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, time, json, html, random, hashlib, datetime as dt
from typing import List, Dict, Optional, Tuple, Set

import requests
from bs4 import BeautifulSoup

# ===================== ENV =====================
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

BASE_URL        = os.getenv("CHP_URL", "https://cad.chp.ca.gov/Traffic.aspx")
COMM_CENTER     = os.getenv("COMM_CENTER", "Inland").strip()
TELEGRAM_TOKEN  = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID= os.getenv("TELEGRAM_CHAT_ID", "").strip()

TYPE_REGEX      = os.getenv("TYPE_REGEX", r"(Collision|Hit\s*(?:&|and)\s*Run)")
AREA_REGEX      = os.getenv("AREA_REGEX", r"")
LOCATION_REGEX  = os.getenv("LOCATION_REGEX", r"")

POLL_INTERVAL   = int(os.getenv("POLL_INTERVAL", "30"))
MISSES_TO_CLOSE = int(os.getenv("MISSES_TO_CLOSE", "2"))
MAX_DETAIL_CHARS_BASE = int(os.getenv("MAX_DETAIL_CHARS", "2500"))

SEEN_FILE       = os.getenv("SEEN_FILE", "seen.json")
TZ_NAME         = os.getenv("TZ", "America/Los_Angeles")
LOG_LEVEL       = os.getenv("LOG_LEVEL", "INFO").upper()

TG_HARD_LIMIT   = 4096

import logging
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger("chp-bot")

try:
    from zoneinfo import ZoneInfo
    TZ = ZoneInfo(TZ_NAME)
except Exception:
    TZ = None

UA_POOL = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17 Safari/605.1.15",
]

# ===================== helpers =====================
def today_str() -> str:
    now = dt.datetime.now(TZ) if TZ else dt.datetime.now()
    return now.strftime("%Y%m%d")

def compose_incident_key(center: str, number: str|int) -> str:
    n = str(number).strip()
    if n.isdigit():
        n = n.zfill(4)
    return f"{center}:{today_str()}:{n}"

def load_state() -> Dict[str, dict]:
    try:
        with open(SEEN_FILE, "r", encoding="utf-8") as f:
            d = json.load(f)
            if isinstance(d, dict):
                return d
    except Exception:
        pass
    return {}

def save_state(st: Dict[str, dict]) -> None:
    with open(SEEN_FILE, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=2)

# ----- retry/backoff -----
RETRY_MAX_ATTEMPTS = 5
RETRY_BASE_DELAY   = 0.5
RETRY_MAX_DELAY    = 10.0

def should_retry(resp: Optional[requests.Response], err: Optional[Exception]) -> bool:
    if err: return True
    if resp is None: return True
    if resp.status_code >= 500: return True
    if resp.status_code in (403, 429): return True
    return False

def request_with_retry(session: requests.Session, method: str, url: str, **kw) -> requests.Response:
    attempts = 0
    while True:
        attempts += 1
        err = None; resp = None
        try:
            kw.setdefault("timeout", 30)
            resp = session.request(method, url, **kw)
            if not should_retry(resp, None):
                return resp
            log.debug("HTTP %s -> retryable (%s)", resp.status_code, url)
        except requests.RequestException as e:
            err = e
            log.debug("HTTP error %s (attempt %d): %s", url, attempts, e)

        if attempts >= RETRY_MAX_ATTEMPTS:
            if err: raise err
            resp.raise_for_status()

        back = min(RETRY_MAX_DELAY, RETRY_BASE_DELAY * (2 ** (attempts-1)))
        jitter = random.uniform(0.0, 0.5*back)
        time.sleep(back + jitter)

# ===================== Telegram =====================
def tg_send(text: str, chat_id: Optional[str]=None) -> Optional[int]:
    chat_id = (chat_id or TELEGRAM_CHAT_ID).strip()
    if not TELEGRAM_TOKEN or not chat_id:
        log.warning("No TELEGRAM_TOKEN/CHAT_ID — skip send")
        return None
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    r = requests.post(url, data={
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }, timeout=20)
    if r.status_code != 200:
        log.error("Telegram send %s %s", r.status_code, r.text[:300])
        return None
    try:
        return int(r.json()["result"]["message_id"])
    except Exception:
        return None

def tg_edit(mid: int, text: str, chat_id: Optional[str]=None) -> tuple[bool,str]:
    chat_id = (chat_id or TELEGRAM_CHAT_ID).strip()
    if not TELEGRAM_TOKEN or not chat_id or not mid:
        return (False, "bad-params")
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/editMessageText"
    r = requests.post(url, data={
        "chat_id": chat_id,
        "message_id": mid,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }, timeout=20)
    if r.status_code != 200:
        try: desc = r.json().get("description","")
        except Exception: desc = r.text[:200]
        return (False, desc)
    return (True, "")

# ===================== ASP.NET form helpers =====================
HEADERS = {"User-Agent": random.choice(UA_POOL)}

def get_initial(session: requests.Session) -> BeautifulSoup:
    r = request_with_retry(session, "GET", BASE_URL, headers=HEADERS)
    return BeautifulSoup(r.text, "html.parser")

def extract_form_state(soup: BeautifulSoup) -> Tuple[str, Dict[str,str]]:
    form = soup.find("form")
    if not form: raise RuntimeError("form not found")
    action = form.get("action") or BASE_URL
    data = {}
    for inp in form.find_all("input"):
        name = inp.get("name")
        if not name: continue
        t = (inp.get("type") or "").lower()
        if t in ("submit","button","image"): continue
        if t in ("checkbox","radio"):
            if inp.has_attr("checked"):
                data[name] = inp.get("value","on")
        else:
            data[name] = inp.get("value","")
    for sel in form.find_all("select"):
        name = sel.get("name")
        if not name: continue
        opt = sel.find("option", selected=True) or sel.find("option")
        if opt:
            data[name] = opt.get("value", opt.get_text(strip=True))
    return action, data

def choose_center(session: requests.Session, first_soup: BeautifulSoup, center: str) -> BeautifulSoup:
    action, payload = extract_form_state(first_soup)
    # найдём селект, где есть нужная опция
    comm_select = None; option_value=None
    for sel in first_soup.find_all("select"):
        for opt in sel.find_all("option"):
            if opt.get_text(strip=True).lower() == center.lower():
                comm_select = sel
                option_value = opt.get("value") or opt.get_text(strip=True)
                break
        if comm_select: break
    if not comm_select:
        raise RuntimeError(f"center '{center}' not found")
    payload[comm_select.get("name")] = option_value

    # кнопка OK если есть
    form = first_soup.find("form")
    submit_name = submit_val = None
    for btn in form.find_all("input", {"type":"submit"}):
        val = (btn.get("value") or "").strip().lower()
        if val in ("ok","submit","go"):
            submit_name = btn.get("name"); submit_val = btn.get("value"); break
    if submit_name:
        payload[submit_name] = submit_val
    post_url = requests.compat.urljoin(BASE_URL, action)
    r = request_with_retry(session, "POST", post_url, headers=HEADERS, data=payload)
    return BeautifulSoup(r.text, "html.parser")

# ===================== grid parsing =====================
def find_grid(soup: BeautifulSoup):
    # по id
    tbl = soup.find("table", {"id": re.compile(r"gvIncidents", re.I)})
    if tbl: return tbl
    # по заголовкам
    for t in soup.find_all("table"):
        tr = t.find("tr")
        if not tr: continue
        headers = [h.get_text(strip=True).lower() for h in tr.find_all(["th","td"])]
        if headers and ("time" in " ".join(headers)) and ("type" in " ".join(headers)) and ("location" in " ".join(headers)):
            return t
    return None

def parse_rows(soup: BeautifulSoup) -> List[Dict[str,str]]:
    grid = find_grid(soup)
    if not grid: return []
    trs = grid.find_all("tr")[1:]
    out=[]
    for tr in trs:
        tds = tr.find_all("td")
        if len(tds) < 7: continue
        a = tds[0].find("a")
        postback=None
        if a and a.get("href","").startswith("javascript:__doPostBack"):
            m = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", a["href"])
            if m: postback={"target": m.group(1), "argument": m.group(2)}
        out.append({
            "no": tds[1].get_text(strip=True),
            "time": tds[2].get_text(strip=True),
            "type": tds[3].get_text(strip=True),
            "location": tds[4].get_text(strip=True),
            "locdesc": tds[5].get_text(strip=True),
            "area": tds[6].get_text(strip=True),
            "postback": postback
        })
    return out

# ===================== Details =====================
TIME_RE = re.compile(r'^\d{1,2}:\d{2}\s*(?:AM|PM)$', re.I)
FOOTER_RE = re.compile(r"(Click on Details|Your screen will refresh|Contact Us|CHP Home Page|CHP Mobile Traffic|\|)$", re.I)

def do_postback(session: requests.Session, soup: BeautifulSoup, target: str, argument: str) -> BeautifulSoup:
    action, payload = extract_form_state(soup)
    payload["__EVENTTARGET"]   = target
    payload["__EVENTARGUMENT"] = argument
    post_url = requests.compat.urljoin(BASE_URL, action)
    # небольшой джиттер, чтобы не долбить сервер
    time.sleep(random.uniform(0.5, 1.5))
    r = request_with_retry(session, "POST", post_url, headers=HEADERS, data=payload)
    return BeautifulSoup(r.text, "html.parser")

def extract_coords(soup: BeautifulSoup) -> Optional[Tuple[float,float]]:
    a = None
    for link in soup.find_all("a", href=True):
        if "google.com/maps" in link.get("href","") and re.search(r"[-+]?\d+\.\d+.*[-+]?\d+\.\d+", link.get_text(" ", strip=True)):
            a = link; break
    if not a: return None
    nums = re.findall(r"[-+]?\d+\.\d+", a.get_text(" ", strip=True))
    if len(nums) >= 2:
        lat, lon = float(nums[0]), float(nums[1])
        if -90 <= lat <= 90 and -180 <= lon <= 180:
            return (lat, lon)
    return None

def extract_detail_lines(soup: BeautifulSoup) -> List[str]:
    flat = soup.get_text("\n", strip=True)
    m = re.search(r"(?im)^Detail Information$", flat)
    if not m: return []
    start = m.end()
    m2 = re.search(r"(?im)^(Unit Information|Close)$", flat[start:])
    end = start + (m2.start() if m2 else len(flat)-start)
    block = flat[start:end]
    lines=[]
    for raw in block.splitlines():
        s = " ".join(raw.split()).strip()
        if not s or FOOTER_RE.search(s): continue
        lines.append(s)
    return lines

def condense_detail_lines(lines: List[str]) -> List[str]:
    out=[]; i=0
    while i < len(lines):
        ln = lines[i].strip()
        if TIME_RE.match(ln):
            t = ln
            j = i+1
            if j < len(lines) and re.match(r'^\d+$', lines[j]): j += 1
            desc = None
            if j < len(lines):
                cand = re.sub(r'^\[\d+\]\s*','', lines[j]).strip()
                if cand and not FOOTER_RE.search(cand): desc = cand
            if desc:
                out.append(f"{t}: {desc}")
                i = j+1; continue
        i += 1
    return out

def blockquote_from_lines(clean_lines: List[str], cap: int) -> str:
    if not clean_lines: return "<blockquote>No details</blockquote>"
    acc=""; total=0
    for ln in clean_lines:
        piece = html.escape(ln)
        add = ("" if not acc else "\n") + piece
        if total + len(add) > cap:
            acc += ("\n" if acc else "") + "… (truncated)"
            break
        acc += add; total += len(add)
    return f"<blockquote>{acc}</blockquote>"

# ===================== Facts & Summary =====================
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
CD_RE   = re.compile(r'\bCD\b|CENTER\s+DIVIDER', re.I)
ONR_RE  = re.compile(r'\bON[-\s]?RAMP\b|\bONR\b', re.I)
OFFR_RE = re.compile(r'\bOFF[-\s]?RAMP\b|\bOFFR\b', re.I)
EXIT_RE = re.compile(r'\bEXIT\b', re.I)
DRIVEABLE_Y = re.compile(r'\bDRIVABLE\b|\bABLE\s+TO\s+DRIVE', re.I)
DRIVEABLE_N = re.compile(r'\bNOT\s+DRIVABLE\b|\bUNDRIVABLE\b', re.I)
CHP_ON_RE   = re.compile(r'\bCHP\b.*\b97\b|\b97\b.*\bCHP\b', re.I)
CHP_ENRT_RE = re.compile(r'\bCHP\b.*ENR[T]?\b|\bENR[T]?\b.*\bCHP\b', re.I)
FIRE_RE     = re.compile(r'\b1141\b|\bFIRE\b|\bMEDIC(S)?\b|AMBU?LANCE', re.I)
TOW_REQ_RE  = re.compile(r'\b1185\b.*\b(REQ|REQUEST|RQST)\b|\bTOW\b.*(REQ|REQUEST|RQST)', re.I)
TOW_ENR_RE  = re.compile(r'\b1185\b.*\bENR[T]?\b', re.I)
TOW_ON_RE   = re.compile(r'\b1185\b.*\b97\b', re.I)
BLOCK_RE    = re.compile(r'\bBLOCK(ED|ING)\b|ALL LNS STOPPED', re.I)

AT_LEAST_TWO = [re.compile(r'\bVS\b', re.I), re.compile(r'\bX\b', re.I),
                re.compile(r'\b2(ND)?\s+VEH\b', re.I), re.compile(r'\bBOTH\s+VEH', re.I)]

def parse_facts(detail_lines: List[str]) -> dict:
    facts = {"vehicles": None, "vehicle_tags": set(), "loc_label": None, "lane_nums": set(),
             "hov": False, "ramp": None, "driveable": None, "chp_on": False, "chp_enrt": False,
             "fire_on": False, "tow": None, "blocked": False, "last_time_hint": None}
    veh_count = 0; two_flag=False
    for raw in detail_lines:
        line = raw.strip(); up=line.upper()
        m = re.match(r'^\s*([0-9]{1,2}:[0-9]{2}\s*(?:AM|PM))\b', line, re.I)
        if m: facts["last_time_hint"]=m.group(1)
        for rx, tag in VEHICLE_TAG_MAP:
            if rx.search(up): facts["vehicle_tags"].add(tag)
        m = re.search(r'\b(\d+)\s*(?:VEH|VEHS|VEHICLES|CARS|TCs?)\b', up)
        if m: veh_count=max(veh_count, int(m.group(1)))
        if any(rx.search(up) for rx in AT_LEAST_TWO): two_flag=True
        for mm in LANE_RE.findall(up): facts["lane_nums"].add(str(mm))
        if HOV_RE.search(up): facts["hov"]=True
        if RS_RE.search(up): facts["loc_label"]="правая обочина"
        elif LS_RE.search(up): facts["loc_label"]="левая обочина"
        elif CD_RE.search(up): facts["loc_label"]="центральная разделительная"
        if ONR_RE.search(up): facts["ramp"]="on-ramp"
        elif OFFR_RE.search(up): facts["ramp"]="off-ramp"
        elif EXIT_RE.search(up): facts["ramp"]="exit"
        if DRIVEABLE_Y.search(up): facts["driveable"]=True
        elif DRIVEABLE_N.search(up): facts["driveable"]=False
        if CHP_ON_RE.search(up): facts["chp_on"]=True
        elif CHP_ENRT_RE.search(up): facts["chp_enrt"]=True
        if FIRE_RE.search(up): facts["fire_on"]=True
        if TOW_ON_RE.search(up): facts["tow"]="on_scene"
        elif TOW_ENR_RE.search(up) and facts["tow"]!="on_scene": facts["tow"]="enroute"
        elif TOW_REQ_RE.search(up) and facts["tow"] not in ("on_scene","enroute"): facts["tow"]="requested"
        if BLOCK_RE.search(up): facts["blocked"]=True
    if veh_count==0 and two_flag: veh_count=2
    facts["vehicles"]=veh_count or None
    return facts

def _compact_lanes(lanes: Set[str]) -> str:
    if not lanes: return ""
    nums = sorted(int(x) for x in lanes if str(x).isdigit())
    if not nums: return ""
    spans=[]; a=b=nums[0]
    for n in nums[1:]:
        if n==b+1: b=n; continue
        spans.append((a,b)); a=b=n
    spans.append((a,b))
    return ", ".join(f"#{x}" if x==y else f"#{x}–#{y}" for x,y in spans)

def _unique_join(parts: list, sep: str=", ") -> str:
    seen=set(); out=[]
    for p in parts:
        p=(p or "").strip()
        if not p or p in seen: continue
        seen.add(p); out.append(p)
    return sep.join(out)

def human_summary_from_facts(f: dict) -> tuple[str,set]:
    consumed=set(); bits=[]
    v=f.get("vehicles"); tags=sorted(f.get("vehicle_tags") or [])
    if v and v>0:
        if tags: bits.append(f"{v} маш. ({', '.join(tags)})"); consumed.update({"vehicles","vehicle_tags"})
        else: bits.append(f"{v} маш."); consumed.add("vehicles")
    elif tags:
        bits.append(_unique_join(tags," / ")); consumed.add("vehicle_tags")
    where=[]
    if f.get("ramp"): where.append("съезд")
    if f.get("loc_label"): where.append(f["loc_label"])
    lane=_compact_lanes(f.get("lane_nums") or set())
    if lane: where.append(f"полоса {lane}")
    if where: bits.append(_unique_join(where,", ")); consumed.update({"ramp","loc_label","lane_nums"})
    if f.get("driveable") is True: bits.append("на ходу"); consumed.add("driveable")
    elif f.get("driveable") is False: bits.append("не на ходу"); consumed.add("driveable")
    tmark=(f.get("last_time_hint") or "").lower()
    tow=f.get("tow")
    if tow=="requested": bits.append("эвакуатор вызван"+(f" ({tmark})" if tmark else "")); consumed.add("tow")
    elif tow=="enroute": bits.append("эвакуатор в пути"+(f" ({tmark})" if tmark else "")); consumed.add("tow")
    elif tow=="on_scene": bits.append("эвакуатор на месте"+(f" ({tmark})" if tmark else "")); consumed.add("tow")
    if f.get("chp_on"): bits.append("офицеры CHP на месте"); consumed.update({"chp_on","chp_enrt"})
    elif f.get("chp_enrt"): bits.append("офицеры CHP в пути"); consumed.add("chp_enrt")
    if f.get("fire_on"): bits.append("медики/пожарные на месте"); consumed.add("fire_on")
    return _unique_join(bits,", "), consumed

def make_text(inc: Dict[str,str], latlon: Optional[Tuple[float,float]],
              details_lines_clean: List[str], facts: dict, closed: bool=False) -> str:
    icon = "🚨" if "Collision" in inc["type"] else ("🚗" if ("Hit" in inc["type"] and "Run" in inc["type"]) else "")
    head = f"⏳ {html.escape(inc['time'])} | 🏙 {html.escape(inc['area'])}\n" \
           f"{icon} {html.escape(inc['type'])}\n\n" \
           f"📍 {html.escape(inc['location'])} — {html.escape(inc['locdesc'])}"

    summary_line, consumed = human_summary_from_facts(facts)
    markers=[]; loc_bits=[]
    if "loc_label" not in consumed and facts.get("loc_label"): loc_bits.append(facts["loc_label"])
    if "ramp" not in consumed and facts.get("ramp"): loc_bits.append("съезд")
    if "lane_nums" not in consumed and facts.get("lane_nums"):
        lane=_compact_lanes(facts["lane_nums"])
        if lane: loc_bits.append(f"полоса {lane}")
    if facts.get("hov"): loc_bits.append("HOV")
    if loc_bits: markers.append(_unique_join(loc_bits," · "))

    veh_bits=[]
    if "vehicles" not in consumed and facts.get("vehicles") is not None: veh_bits.append(f"{facts['vehicles']} ТС")
    if "vehicle_tags" not in consumed and facts.get("vehicle_tags"): veh_bits.append(", ".join(sorted(facts["vehicle_tags"])))
    if veh_bits: markers.append(" / ".join(veh_bits))

    st_bits=[]
    if "chp_on" not in consumed and facts.get("chp_on"): st_bits.append("офицеры CHP на месте")
    elif "chp_enrt" not in consumed and facts.get("chp_enrt"): st_bits.append("офицеры CHP в пути")
    if "fire_on" not in consumed and facts.get("fire_on"): st_bits.append("медики/пожарные")
    if "tow" not in consumed and facts.get("tow"):
        st_bits.append({"requested":"эвакуатор вызван","enroute":"эвакуатор в пути","on_scene":"эвакуатор на месте"}.get(facts["tow"],""))
    if "driveable" not in consumed:
        if facts.get("driveable") is True: st_bits.append("на ходу")
        elif facts.get("driveable") is False: st_bits.append("не на ходу")
    st_bits=[b for b in st_bits if b]
    if st_bits: markers.append(_unique_join(st_bits,", "))

    facts_block_lines=[]
    if summary_line: facts_block_lines.append(summary_line)
    if markers: facts_block_lines.append(" | ".join(markers))
    facts_block = "\n\n<b>📌 Расположение / Машины:</b>\n" + "\n".join(facts_block_lines) if facts_block_lines else ""

    # карта = ПИН по координатам
    if latlon:
        lat,lon = latlon
        map_url = f"https://www.google.com/maps/search/?api=1&query={lat:.6f},{lon:.6f}"
        map_block = f"\n\n<b>🗺️ Карта:</b>\n{map_url}"
    else:
        map_block = "\n\n<b>🗺️ Карта:</b>\nКоординаты недоступны"

    skeleton = head + facts_block + map_block
    footer_base = len("\n\n<b>📝 Detail Information:</b>\n")
    footer_close = len("\n\n<b>❗️ Инцидент закрыт CHP</b>") if closed else 0
    leftover = TG_HARD_LIMIT - len(skeleton) - footer_base - footer_close
    cap = max(0, min(MAX_DETAIL_CHARS_BASE, leftover))
    details_block = blockquote_from_lines(details_lines_clean, cap) if cap>0 else ""
    det = f"\n\n<b>📝 Detail Information:</b>\n{details_block}" if details_block else ""
    text = skeleton + det
    if closed: text += "\n\n<b>❗️ Инцидент закрыт CHP</b>"
    if len(text) > TG_HARD_LIMIT and det:
        shrink = int(cap*0.8)
        details_block = blockquote_from_lines(details_lines_clean, max(0,shrink))
        det = f"\n\n<b>📝 Detail Information:</b>\n{details_block}" if details_block else ""
        text = skeleton + det
        if closed: text += "\n\n<b>❗️ Инцидент закрыт CHP</b>"
    return text

def signature_for_update(inc: Dict[str,str], details_lines_clean: List[str], facts: dict) -> str:
    norm = "\n".join(details_lines_clean or [])
    fkey = "|".join([
        str(facts.get("vehicles")),
        ",".join(sorted(facts.get("vehicle_tags") or [])),
        facts.get("loc_label") or "",
        ",".join(sorted(facts.get("lane_nums") or [])),
        "HOV" if facts.get("hov") else "",
        facts.get("ramp") or "",
        "DRV1" if facts.get("driveable") is True else ("DRV0" if facts.get("driveable") is False else ""),
        "C97" if facts.get("chp_on") else ("CENRT" if facts.get("chp_enrt") else ""),
        "FIRE" if facts.get("fire_on") else "",
        {"requested":"TREQ","enroute":"TENRT","on_scene":"T97"}.get(facts.get("tow") or "","")
    ])
    base = (inc.get("type","") + "||" + norm + "||" + fkey).encode("utf-8","ignore")
    return hashlib.sha1(base).hexdigest()

# ===================== main loop =====================
def main():
    log.info("CHP bot starting | Center=%s", COMM_CENTER)
    state = load_state()
    session = requests.Session()
    session.headers.update(HEADERS)

    while True:
        seen_ids=set()
        try:
            soup0 = get_initial(session)
            soup  = choose_center(session, soup0, COMM_CENTER)
            rows  = parse_rows(soup) or parse_rows(soup0)  # fallback если центр уже выбран

            # фильтры по типу/ареа/локации
            t_re = re.compile(TYPE_REGEX, re.I) if TYPE_REGEX else None
            a_re = re.compile(AREA_REGEX, re.I) if AREA_REGEX else None
            l_re = re.compile(LOCATION_REGEX, re.I) if LOCATION_REGEX else None

            for inc in rows:
                if t_re and not t_re.search(inc["type"]): continue
                if a_re and not a_re.search(inc["area"]): continue
                if l_re and not (l_re.search(inc["location"]) or l_re.search(inc["locdesc"])): continue

                inc_key = compose_incident_key(COMM_CENTER, inc["no"])
                seen_ids.add(inc_key)

                # детали
                details_lines=[]; latlon=None
                if inc.get("postback"):
                    soup_det = do_postback(session, soup, inc["postback"]["target"], inc["postback"]["argument"])
                    latlon   = extract_coords(soup_det)
                    lines    = extract_detail_lines(soup_det)
                    details_lines = condense_detail_lines(lines) if lines else []
                # факты, текст, сигнатура
                facts = parse_facts(details_lines)
                text  = make_text(inc, latlon, details_lines, facts, closed=False)
                sig   = signature_for_update(inc, details_lines, facts)

                st = state.get(inc_key)
                if not st or not st.get("message_id"):
                    mid = tg_send(text, chat_id=TELEGRAM_CHAT_ID)
                    state[inc_key] = {
                        "message_id": mid,
                        "chat_id": TELEGRAM_CHAT_ID,
                        "last_sig": sig,
                        "last_text": text,
                        "closed": False,
                        "misses": 0,
                        "first_seen": dt.datetime.utcnow().isoformat(),
                        "last_seen": dt.datetime.utcnow().isoformat()
                    }
                    log.info("new %s (%s)", inc["no"], inc["type"])
                else:
                    st["last_seen"] = dt.datetime.utcnow().isoformat()
                    st["misses"] = 0
                    if st.get("last_sig") != sig or st.get("closed"):
                        ok, why = tg_edit(st["message_id"], text, chat_id=st.get("chat_id") or TELEGRAM_CHAT_ID)
                        if not ok and "message to edit not found" in why.lower():
                            # репостим как новое сообщение
                            mid = tg_send(text, chat_id=TELEGRAM_CHAT_ID)
                            if mid: st["message_id"]=mid; ok=True
                        if ok:
                            st["last_sig"] = sig
                            st["last_text"] = text
                            st["closed"] = False
                            log.info("edited %s (%s)", inc["no"], inc["type"])

            # закрытия (инцидент исчез N циклов)
            for inc_id, st in list(state.items()):
                parts = inc_id.split(":")
                if len(parts) < 3 or parts[1] != today_str():  # чистим старые дни
                    continue
                if inc_id not in seen_ids and not st.get("closed"):
                    st["misses"]=st.get("misses",0)+1
                    if st["misses"] >= MISSES_TO_CLOSE and st.get("message_id"):
                        new_text = (st.get("last_text") or "") + "\n\n<b>❗️ Инцидент закрыт CHP</b>"
                        ok,_ = tg_edit(st["message_id"], new_text, chat_id=st.get("chat_id") or TELEGRAM_CHAT_ID)
                        if ok:
                            st["last_text"] = new_text
                            st["closed"] = True
                            log.info("closed %s", inc_id)

            save_state(state)

        except Exception as e:
            log.error("loop error: %s", e)

        # главный цикл с джиттером
        time.sleep(POLL_INTERVAL + random.uniform(2.0, 5.0))

if __name__ == "__main__":
    main()
