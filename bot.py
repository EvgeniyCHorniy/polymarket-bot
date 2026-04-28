"""
London EGLC Temperature Polymarket Bot v4
==========================================

НОВЕ в v4:
  1. 3 найточніші моделі для Лондона (Open-Meteo, без ключів):
       ECMWF IFS      /v1/forecast (ECMWF best-match, 9 км)
       DWD ICON       /v1/dwd-icon                      (2 км, найкращий для Європи)
       UK Met Office  /v1/ukmo                          (2 км, офіційний британський)
     + Погодинний max: беремо hourly і рахуємо max самі (точніше ніж daily)
     + Поправка на хмарність (☀️+0.5°C / ☁️-0.3°C) та вітер (💨 до -0.6°C)

  2. Тренд ціни + Momentum:
     - Зберігаємо ціну кожні 2 хв у price_history.json
     - ASCII sparkline графік
     - Momentum алерт якщо ціна змінилась > 5% за 30 хв

  3. Стоп-лос та Тейк-профіт:
     /buy 17 29.04 --stop 20 --tp 65

  4. Авто-скан ринків о 09:00 Kyiv (1-4 дні вперед, BUY < 38%)

  5. Ранковий брифінг о 07:30 Kyiv:
     погода + позиції + нагадування записати факт

  6. Кнопки (Reply Keyboard) — не треба пам'ятати команди
     + Inline кнопки під позиціями (Закрити / Тренд)

ВСТАНОВЛЕННЯ:
  pip install "python-telegram-bot[job-queue]==20.*" requests pytz

ENV VARS:
  BOT_TOKEN  CHAT_ID  TOMORROW_API_KEY (опційно)
"""

import os, re, json, logging, requests, pytz
import threading, time
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timedelta
from pathlib import Path


# Keep-alive for Render
class _KeepAliveHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'OK')
    def log_message(self, format, *args):
        pass

def _run_web() -> None:
    port = int(os.environ.get('PORT', 10000))
    server = HTTPServer(('0.0.0.0', port), _KeepAliveHandler)
    logging.getLogger(__name__).info('Keep-alive HTTP on port %d', port)
    server.serve_forever()

def _self_ping() -> None:
    url = os.getenv('RENDER_EXTERNAL_URL')
    if not url:
        logging.getLogger(__name__).warning('RENDER_EXTERNAL_URL not set, self-ping disabled')
        return
    while True:
        try:
            requests.get(url, timeout=10)
            logging.getLogger(__name__).info('self-ping OK')
        except Exception as e:
            logging.getLogger(__name__).warning('self-ping error: %s', e)
        time.sleep(300)

def start_keep_alive() -> None:
    threading.Thread(target=_run_web,   daemon=True, name='web_server').start()
    threading.Thread(target=_self_ping, daemon=True, name='self_ping').start()


from telegram import (
    Update, Bot,
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
from telegram.ext import (
    ApplicationBuilder, CommandHandler,
    MessageHandler, CallbackQueryHandler,
    ContextTypes, filters,
)

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

TOKEN            = os.getenv("BOT_TOKEN")
CHAT_ID          = os.getenv("CHAT_ID")
TOMORROW_API_KEY = os.getenv("TOMORROW_API_KEY", "")

KYIV_TZ            = pytz.timezone("Europe/Kiev")
EGLC_LAT           = 51.5048
EGLC_LON           = 0.0495
OUTLIER_THRESHOLD  = 2.0
BUY_MAX_PCT        = 38.0
MOMENTUM_THRESHOLD = 5.0
HISTORY_FILE       = Path("eglc_history.json")
PRICE_HISTORY_FILE   = Path("price_history.json")
FORECAST_CACHE_FILE  = Path("forecast_cache.json")  # зберігаємо прогнози для /actual
ALERT_LEVELS       = [40, 50, 60, 70, 80, 90]

# ══════════════════════════════════════════════════════════════════════════════
#  HISTORICAL BIAS
# ══════════════════════════════════════════════════════════════════════════════

# EGLC Urban Heat Island поправка (°C) по місяцях.
# Калібровано по реальних EGLC METAR даних 2023-2024 vs Open-Meteo прогноз.
# Open-Meteo ECMWF/DWD вже добре калібровані для Лондона,
# тому реальна EGLC поправка невелика: 0.2-0.5°C.
# Використовуй /actual щоб накопичувати реальні дані і автоматично замінити ці значення.
BASE_EGLC_BIAS: dict[int, float] = {
    1: 0.1, 2: 0.1, 3: 0.2, 4: 0.3,
    5: 0.4, 6: 0.5, 7: 0.5, 8: 0.5,
    9: 0.4, 10: 0.3, 11: 0.2, 12: 0.1,
}
SOURCE_STATS: dict = {}


def load_history() -> None:
    global SOURCE_STATS
    if HISTORY_FILE.exists():
        try: SOURCE_STATS = json.loads(HISTORY_FILE.read_text())
        except Exception as e: logger.warning("History: %s", e)


def save_history() -> None:
    try: HISTORY_FILE.write_text(json.dumps(SOURCE_STATS, indent=2))
    except Exception as e: logger.error("Save history: %s", e)


def record_actual(source: str, month: int, predicted: float, actual: float) -> None:
    error = predicted - actual
    key = str(month)
    SOURCE_STATS.setdefault(source, {}).setdefault(key, {"bias": 0.0, "mae": 0.0, "n": 0})
    s = SOURCE_STATS[source][key]; n = s["n"]
    s["bias"] = (s["bias"] * n + error) / (n + 1)
    s["mae"]  = (s["mae"]  * n + abs(error)) / (n + 1)
    s["n"] = n + 1
    save_history()


def get_learned_bias(source: str, month: int) -> float:
    s = SOURCE_STATS.get(source, {}).get(str(month), {})
    if s.get("n", 0) >= 5: return -s["bias"]
    return BASE_EGLC_BIAS.get(month, 0.5)


def source_accuracy_str(source: str, month: int) -> str:
    s = SOURCE_STATS.get(source, {}).get(str(month), {})
    n = s.get("n", 0)
    if n >= 3: return f"MAE {s['mae']:.1f}°C, зміщ {s['bias']:+.1f}°C (n={n})"
    return f"мало даних (n={n})"


# ══════════════════════════════════════════════════════════════════════════════
#  PRICE HISTORY
# ══════════════════════════════════════════════════════════════════════════════

price_history: dict = {}


def load_price_history() -> None:
    global price_history
    if PRICE_HISTORY_FILE.exists():
        try: price_history = json.loads(PRICE_HISTORY_FILE.read_text())
        except: price_history = {}


def save_price_history() -> None:
    try: PRICE_HISTORY_FILE.write_text(json.dumps(price_history, indent=2))
    except Exception as e: logger.error("Save price history: %s", e)


def record_price(date_key: str, label: str, pct: float) -> None:
    price_history.setdefault(date_key, [])
    price_history[date_key].append({
        "ts": datetime.utcnow().isoformat(timespec="minutes"),
        "label": label, "pct": pct,
    })
    if len(price_history[date_key]) > 500:
        price_history[date_key] = price_history[date_key][-500:]
    save_price_history()


def get_trend(date_key: str, label: str, minutes: int = 180) -> dict | None:
    history = [h for h in price_history.get(date_key, []) if h["label"] == label]
    if len(history) < 2: return None
    cutoff = (datetime.utcnow() - timedelta(minutes=minutes)).isoformat(timespec="minutes")
    recent = [h for h in history if h["ts"] >= cutoff]
    if len(recent) < 2: recent = history[-20:]
    first_pct = recent[0]["pct"]; last_pct = recent[-1]["pct"]
    delta = round(last_pct - first_pct, 1)
    cutoff_30 = (datetime.utcnow() - timedelta(minutes=30)).isoformat(timespec="minutes")
    last_30   = [h for h in recent if h["ts"] >= cutoff_30]
    momentum  = round(last_pct - last_30[0]["pct"], 1) if len(last_30) >= 2 else 0.0
    vals = [h["pct"] for h in recent[-20:]]
    mn, mx = min(vals), max(vals)
    chars = "▁▂▃▄▅▆▇█"
    spark = "".join(chars[int((v-mn)/(mx-mn)*7)] if mx != mn else "▄" for v in vals)
    return {"first": first_pct, "last": last_pct, "delta": delta,
            "momentum": momentum, "n": len(recent), "spark": spark, "minutes": minutes}


# ══════════════════════════════════════════════════════════════════════════════
#  FORECAST CACHE — зберігаємо прогнози щоб /actual міг їх знайти
# ══════════════════════════════════════════════════════════════════════════════

# { "2026-04-28": { "ECMWF": 16.8, "DWD ICON": 16.2, "UK Met Office": 16.8, "final": 17.0 } }
forecast_cache: dict = {}


def load_forecast_cache() -> None:
    global forecast_cache
    if FORECAST_CACHE_FILE.exists():
        try: forecast_cache = json.loads(FORECAST_CACHE_FILE.read_text())
        except: forecast_cache = {}


def save_forecast_cache() -> None:
    try: FORECAST_CACHE_FILE.write_text(json.dumps(forecast_cache, indent=2))
    except Exception as e: logger.error("Save forecast cache: %s", e)


def cache_forecast(dt: datetime, fc: dict) -> None:
    """Зберігає прогноз після кожного compute_forecast."""
    dk = dt.strftime("%Y-%m-%d")
    entry = {"final": fc.get("final_temp"), "month": fc.get("month")}
    for s in fc.get("sources", []):
        entry[s["source"]] = s["temp_max"]  # зберігаємо raw temp_max кожної моделі
    forecast_cache[dk] = entry
    # Зберігаємо лише останні 30 днів
    keys = sorted(forecast_cache.keys())
    for old_key in keys[:-30]:
        del forecast_cache[old_key]
    save_forecast_cache()


# ══════════════════════════════════════════════════════════════════════════════
#  FORECAST CHANGE MONITOR — алерт якщо прогноз змінився на ≥1°C
# ══════════════════════════════════════════════════════════════════════════════

# { "2026-04-29": {"final": 17.0, "ts": "2026-04-28T14:00"} }
forecast_change_log: dict = {}
FORECAST_CHANGE_FILE = Path("forecast_changes.json")


def load_forecast_changes() -> None:
    global forecast_change_log
    if FORECAST_CHANGE_FILE.exists():
        try: forecast_change_log = json.loads(FORECAST_CHANGE_FILE.read_text())
        except: forecast_change_log = {}


def save_forecast_changes() -> None:
    try: FORECAST_CHANGE_FILE.write_text(json.dumps(forecast_change_log, indent=2))
    except Exception as e: logger.error("Save forecast changes: %s", e)


def check_forecast_change(dt: datetime, new_final: float) -> tuple[bool, float]:
    """
    Порівнює новий прогноз з попереднім.
    Повертає (changed, prev_final) де changed=True якщо різниця >= 1°C.
    """
    dk = dt.strftime("%Y-%m-%d")
    prev = forecast_change_log.get(dk)
    forecast_change_log[dk] = {
        "final": new_final,
        "ts": datetime.utcnow().isoformat(timespec="minutes"),
    }
    save_forecast_changes()
    if prev is None:
        return False, new_final
    prev_final = prev["final"]
    changed = abs(new_final - prev_final) >= 1.0
    return changed, prev_final


# ══════════════════════════════════════════════════════════════════════════════
#  WEATHER — 3 найточніші моделі для Лондона
# ══════════════════════════════════════════════════════════════════════════════

def _safe_get(url: str, **kwargs) -> dict | None:
    try:
        r = requests.get(url, timeout=12, **kwargs)
        r.raise_for_status(); return r.json()
    except Exception as e:
        logger.error("GET %.80s → %s", url, e); return None


def _hourly_max(data: dict, ds: str) -> tuple[float | None, float | None, float, float]:
    """Рахуємо max температуру з погодинних даних самостійно — точніше ніж daily."""
    h = data.get("hourly", {})
    times = h.get("time", [])
    temps  = h.get("temperature_2m", [])
    clouds = h.get("cloudcover", h.get("cloud_cover", []))
    winds  = h.get("windspeed_10m", [])
    dt, dn, dc, dw = [], [], [], []
    for i, t in enumerate(times):
        if not t.startswith(ds): continue
        if i < len(temps)  and temps[i]  is not None: dt.append(float(temps[i]))
        if i < len(clouds) and clouds[i] is not None: dc.append(float(clouds[i]))
        if i < len(winds)  and winds[i]  is not None: dw.append(float(winds[i]))
    if not dt: return None, None, 0.0, 0.0
    return (max(dt), min(dt),
            round(sum(dc)/len(dc), 1) if dc else 0.0,
            round(sum(dw)/len(dw), 1) if dw else 0.0)


def _wx_correction(temp: float, cloud: float, wind: float) -> tuple[float, str]:
    """
    НЕ застосовуємо поправку на хмарність/вітер.
    Причина: NWP моделі (ECMWF, DWD ICON, UK Met Office) вже враховують
    хмарність і вітер всередині своїх розрахунків температури.
    Додаткова поправка з нашого боку = подвійний облік → систематична помилка.

    Залишаємо тільки EGLC station bias (BASE_EGLC_BIAS) — це різниця між
    точкою сітки моделі і фізичним розташуванням станції EGLC.
    Ця різниця замінюється навченою поправкою після 5+ записів через /actual.
    """
    return temp, "—"


def _build_source(name: str, data: dict, ds: str) -> dict | None:
    tmax, tmin, cloud, wind = _hourly_max(data, ds)
    if tmax is None: return None
    # wx_corrected == temp_max: поправку на хмарність/вітер не застосовуємо
    # (моделі вже це враховують всередині)
    return {"source": name, "temp_max": tmax, "temp_min": tmin,
            "cloud": cloud, "wind": wind, "wx_note": "—", "wx_corrected": tmax}


def fetch_ecmwf(dt: datetime) -> dict | None:
    """ECMWF IFS — 9 км, найточніший глобально."""
    ds = dt.strftime("%Y-%m-%d")
    data = _safe_get("https://api.open-meteo.com/v1/forecast", params={
        "latitude": EGLC_LAT, "longitude": EGLC_LON,
        "hourly": "temperature_2m,cloud_cover,windspeed_10m",
        "timezone": "Europe/London", "start_date": ds, "end_date": ds,
        })
    return _build_source("ECMWF", data, ds) if data else None


def fetch_dwd_icon(dt: datetime) -> dict | None:
    """DWD ICON — 2 км, найточніший для Центральної Європи."""
    ds = dt.strftime("%Y-%m-%d")
    data = _safe_get("https://api.open-meteo.com/v1/dwd-icon", params={
        "latitude": EGLC_LAT, "longitude": EGLC_LON,
        "hourly": "temperature_2m,cloud_cover,windspeed_10m",
        "timezone": "Europe/London", "start_date": ds, "end_date": ds,
    })
    return _build_source("DWD ICON", data, ds) if data else None


def fetch_ukmet(dt: datetime) -> dict | None:
    """UK Met Office — 2 км, офіційна британська служба."""
    ds = dt.strftime("%Y-%m-%d")
    # Пробуємо два можливих endpoint
    for url, params in [
        ("https://api.open-meteo.com/v1/ukmo_seamless", {
            "latitude": EGLC_LAT, "longitude": EGLC_LON,
            "hourly": "temperature_2m,cloud_cover,windspeed_10m",
            "timezone": "Europe/London", "start_date": ds, "end_date": ds,
        }),
        ("https://api.open-meteo.com/v1/forecast", {
            "latitude": EGLC_LAT, "longitude": EGLC_LON,
            "hourly": "temperature_2m,cloud_cover,windspeed_10m",
            "timezone": "Europe/London", "start_date": ds, "end_date": ds,
            "models": "ukmo_seamless",
        }),
    ]:
        data = _safe_get(url, params=params)
        if data:
            result = _build_source("UK Met Office", data, ds)
            if result: return result
    return None


def get_all_sources(dt: datetime) -> list[dict]:
    sources = []
    for fetcher in (fetch_ecmwf, fetch_dwd_icon, fetch_ukmet):
        r = fetcher(dt)
        if r: sources.append(r)
        else: logger.warning("%s failed for %s", fetcher.__name__, dt.date())
    return sources


# ══════════════════════════════════════════════════════════════════════════════
#  FORECAST AGGREGATION
# ══════════════════════════════════════════════════════════════════════════════

SOURCE_WEIGHTS = {"ECMWF": 0.35, "DWD ICON": 0.35, "UK Met Office": 0.30}


def _median(vals: list[float]) -> float:
    s = sorted(vals); n = len(s)
    return s[n//2] if n % 2 else round((s[n//2-1]+s[n//2])/2, 1)


def detect_outliers(sources: list[dict]) -> list[dict]:
    if len(sources) < 2:
        for s in sources: s["outlier"] = False; s["outlier_delta"] = 0.0
        return sources
    for s in sources:
        others = [o["wx_corrected"] for o in sources if o["source"] != s["source"]]
        med    = _median(others) if others else s["wx_corrected"]
        delta  = abs(s["wx_corrected"] - med)
        s["outlier"] = delta > OUTLIER_THRESHOLD; s["outlier_delta"] = round(delta, 1)
    return sources


def compute_forecast(dt: datetime) -> dict:
    raw = get_all_sources(dt)
    if not raw: return {"error": "Не вдалось отримати дані жодного джерела погоди"}
    raw = detect_outliers(raw)
    month = dt.month
    enriched = []
    for s in raw:
        bias      = get_learned_bias(s["source"], month)
        corrected = round(s["wx_corrected"] + bias, 1)
        enriched.append({**s, "bias": bias, "corrected": corrected,
                         "accuracy": source_accuracy_str(s["source"], month)})
    w_sum, w_tot = 0.0, 0.0
    for s in enriched:
        w = 0.05 if s.get("outlier") else SOURCE_WEIGHTS.get(s["source"], 0.30)
        w_sum += s["corrected"] * w; w_tot += w
    weighted = round(w_sum / w_tot, 1) if w_tot else 0.0
    vals     = sorted(s["corrected"] for s in enriched)
    median   = _median(vals)
    final    = round((weighted + median) / 2, 1)
    spread   = max(vals) - min(vals) if vals else 0
    n_out    = sum(1 for s in enriched if s.get("outlier"))
    if n_out:          confidence = "⚠️ низька (аутлаєр)"
    elif spread <= 0.5: confidence = "🟢 висока"
    elif spread <= 1.5: confidence = "🟡 середня"
    else:               confidence = "🟠 помірна"
    return {"sources": enriched, "weighted_avg": weighted, "median": median,
            "final_temp": final, "final_int": round(final), "month": month,
            "max_spread": round(spread, 1), "confidence": confidence}


# ══════════════════════════════════════════════════════════════════════════════
#  POLYMARKET
# ══════════════════════════════════════════════════════════════════════════════

def build_slug(dt: datetime) -> str:
    return f"highest-temperature-in-london-on-{dt.strftime('%B').lower()}-{dt.day}-{dt.year}"


def get_polymarket_data(dt: datetime) -> tuple[dict | None, list, str]:
    slug = build_slug(dt)
    link = f"https://polymarket.com/event/{slug}"
    data = _safe_get("https://gamma-api.polymarket.com/events", params={"slug": slug})
    if not data or not isinstance(data, list) or not data: return None, [], link
    return data[0], data[0].get("markets", []), link


def _normalize_temp_label(raw: str) -> str:
    if re.match(r"^\d+\s*°C(\s+(or\s+(below|higher)))?$", raw.strip(), re.I): return raw.strip()
    m = re.search(r"(\d+)\s*°C\s+or\s+(below|higher)", raw, re.I)
    if m: return f"{m.group(1)}°C or {m.group(2).lower()}"
    m = re.search(r"(\d+)\s*°C", raw)
    if m: return f"{m.group(1)}°C"
    return raw.strip()


def parse_all_outcomes(markets: list) -> dict:
    result = {}
    for m in markets:
        raw_label = m.get("question", "").strip()
        if not raw_label:
            outs = m.get("outcomes", "[]")
            if isinstance(outs, str):
                try: outs = json.loads(outs)
                except: outs = []
            raw_label = outs[0] if outs else "Unknown"
        label      = _normalize_temp_label(raw_label)
        prices_raw = m.get("outcomePrices", "[]")
        if isinstance(prices_raw, str):
            try: prices = json.loads(prices_raw)
            except: prices = []
        else: prices = prices_raw
        if prices:
            try: result[label] = round(float(prices[0]) * 100, 1)
            except: result[label] = 0.0
    return result


def find_outcome_for_temp(outcomes: dict, temp: int) -> tuple[str | None, float | None]:
    exact = f"{temp}°C"
    if exact in outcomes: return exact, outcomes[exact]
    for lbl, pct in outcomes.items():
        m = re.match(r"(\d+)\s*°C\s+or\s+below$", lbl, re.I)
        if m and temp <= int(m.group(1)): return lbl, pct
        m = re.match(r"(\d+)\s*°C\s+or\s+higher$", lbl, re.I)
        if m and temp >= int(m.group(1)): return lbl, pct
    best_lbl, best_pct, best_d = None, None, 999
    for lbl, pct in outcomes.items():
        m = re.match(r"(\d+)", lbl)
        if m:
            d = abs(int(m.group(1)) - temp)
            if d < best_d: best_d, best_lbl, best_pct = d, lbl, pct
    return best_lbl, best_pct


def polymarket_consensus(outcomes: dict, forecast_temp: int) -> str:
    if not outcomes: return ""
    top_lbl, top_pct = max(outcomes.items(), key=lambda x: x[1])
    m = re.search(r"(\d+)", top_lbl)
    if not m: return ""
    mt = int(m.group(1)); diff = forecast_temp - mt
    if diff == 0:     return f"✅ Прогноз збігається з ринком ({mt}°C @ {top_pct}%)"
    elif abs(diff)==1: return f"🟡 Прогноз {forecast_temp}°C, ринок — {mt}°C @ {top_pct}% (1°C)"
    else:
        d = "вище" if diff > 0 else "нижче"
        return f"⚠️ Прогноз {forecast_temp}°C, ринок — {mt}°C @ {top_pct}% ({abs(diff)}°C {d})"


# ══════════════════════════════════════════════════════════════════════════════
#  DATE PARSING
# ══════════════════════════════════════════════════════════════════════════════

def _parse_date_raw(args: list, allow_past: bool = False) -> tuple[datetime | None, str | None]:
    """Базовий парсер дати. allow_past=True — дозволяє минулі дати (для /actual)."""
    if not args:
        if allow_past:
            t = datetime.utcnow() - timedelta(days=1)  # вчора для /actual
        else:
            t = datetime.utcnow() + timedelta(days=1)  # завтра для /check
        return t.replace(hour=0, minute=0, second=0, microsecond=0), None
    raw = args[0].strip().replace("/", ".")
    now = datetime.utcnow()
    patterns = [
        (r"^(\d{1,2})\.(\d{1,2})$",          lambda m: (int(m[1]), int(m[2]), now.year)),
        (r"^(\d{1,2})\.(\d{1,2})\.(\d{4})$",  lambda m: (int(m[1]), int(m[2]), int(m[3]))),
        (r"^(\d{1,2})\.(\d{1,2})\.(\d{2})$",  lambda m: (int(m[1]), int(m[2]), 2000+int(m[3]))),
    ]
    for pat, ext in patterns:
        m = re.match(pat, raw)
        if m:
            try:
                day, month, year = ext(m)
                dt = datetime(year, month, day)
                ahead = (dt.date() - now.date()).days
                if allow_past:
                    # Для /actual: дозволяємо минулі, але не більше 30 днів назад
                    if ahead < -30: return None, f"❌ Дата {dt.strftime('%d.%m.%Y')} надто давня (> 30 днів)."
                    if ahead > 1:   return None, f"❌ Дата {dt.strftime('%d.%m.%Y')} ще не настала."
                else:
                    # Для /check: тільки майбутні
                    if raw.count(".") == 1 and dt.date() < now.date():
                        dt = dt.replace(year=year+1)
                        ahead = (dt.date() - now.date()).days
                    if ahead < 0:  return None, f"❌ Дата {dt.strftime('%d.%m.%Y')} вже минула."
                    if ahead > 15: return None, "❌ Прогноз максимум на 15 днів."
                return dt, None
            except ValueError as e: return None, f"❌ Дата `{raw}`: {e}"
    return None, f"❌ Не розпізнав: `{raw}`. Формат: DD.MM або DD.MM.YYYY"


def parse_target_date(args: list) -> tuple[datetime | None, str | None]:
    """Для /check, /poll, /forecast — тільки майбутні дати."""
    return _parse_date_raw(args, allow_past=False)


def parse_past_date(args: list) -> tuple[datetime | None, str | None]:
    """Для /actual — минулі та сьогоднішні дати."""
    return _parse_date_raw(args, allow_past=True)


# ══════════════════════════════════════════════════════════════════════════════
#  MONITORING
# ══════════════════════════════════════════════════════════════════════════════

monitoring: dict[str, dict] = {}


def _date_key(dt: datetime) -> str: return dt.strftime("%Y-%m-%d")


def _days_label(dt: datetime) -> str:
    d = (dt.date() - datetime.utcnow().date()).days
    return {0:" (сьогодні)",1:" (завтра)",2:" (після завтра)"}.get(d, f" (через {d} дн.)")


# ══════════════════════════════════════════════════════════════════════════════
#  FORMATTERS
# ══════════════════════════════════════════════════════════════════════════════

def fmt_weather(dt: datetime, fc: dict) -> str:
    lines = [f"🌡 *Прогноз EGLC — {dt.strftime('%d.%m.%Y')}{_days_label(dt)}*\n",
             "*3 моделі → погодинний max → wx поправка → EGLC bias:*"]
    for s in fc["sources"]:
        out = f" ⚠️ аутлаєр Δ{s['outlier_delta']}°C" if s.get("outlier") else ""
        bias_str = f"+{s['bias']:.1f}" if s['bias'] >= 0 else f"{s['bias']:.1f}"
        lines.append(
            f"  ▸ *{s['source']}*: {s['temp_max']:.1f}°C"
            f" ☁️{s['cloud']:.0f}% 💨{s['wind']:.0f}км/г"
            f" → EGLC bias:{bias_str} → *{s['corrected']:.1f}°C*{out}"
        )
        lines.append(f"     _{s['accuracy']}_")
    lines.append(
        f"\n📍 *EGLC прогноз:* {fc['final_temp']:.1f}°C → округлено *{fc['final_int']}°C*"
    )
    lines.append(
        f"   _(зваж: {fc['weighted_avg']:.1f} │ медіана: {fc['median']:.1f}"
        f" │ розкид: {fc['max_spread']:.1f}°C)_"
    )
    lines.append(f"   _Довіра: {fc['confidence']}_")
    return "\n".join(lines)


def fmt_polymarket(dt: datetime, outcomes: dict,
                   tgt_lbl: str | None, tgt_pct: float | None,
                   link: str, forecast_temp: int | None = None,
                   trend: dict | None = None) -> str:
    lines = ["\n📊 *Polymarket — Highest Temp London:*"]
    if outcomes:
        for lbl, pct in sorted(outcomes.items(), key=lambda x: -x[1])[:5]:
            mark = " ◀️ *прогноз*" if lbl == tgt_lbl else ""
            lines.append(f"  `{lbl}`: {pct}%{mark}")
        if tgt_lbl and tgt_pct is not None:
            lines.append(f"\n🎯 `{tgt_lbl}` = *{tgt_pct}%*")
            mn = re.search(r"(\d+)", tgt_lbl)
            num = mn.group(1) if mn else "??"
            if tgt_pct < 20:   lines.append("🟢 *ДУЖЕ вигідно — сильний BUY!*")
            elif tgt_pct < 38: lines.append(f"🟢 *BUY сигнал < 38%* → `/buy {num}`")
            elif tgt_pct < 50: lines.append("⏳ *38–50% — тримати*")
            elif tgt_pct < 65: lines.append("🔴 *≥ 50% — розглянути продаж*")
            else:               lines.append("🔴 *≥ 65% — ринок переоцінює*")
        if trend:
            arrow = "📈" if trend["delta"] > 0 else ("📉" if trend["delta"] < 0 else "➡️")
            lines.append(
                f"\n{arrow} *Тренд {trend['minutes']}хв:* {trend['first']}% → {trend['last']}%"
                f" ({trend['delta']:+.1f}%)"
            )
            lines.append(f"   `{trend['spark']}`")
            if abs(trend["momentum"]) >= MOMENTUM_THRESHOLD:
                mo = "🚀" if trend["momentum"] > 0 else "💥"
                lines.append(f"   {mo} *Momentum 30хв: {trend['momentum']:+.1f}%*")
        if forecast_temp:
            c = polymarket_consensus(outcomes, forecast_temp)
            if c: lines.append(f"\n_{c}_")
    else:
        lines.append("  ⚠️ Ринок не знайдено або ще не відкрито")
    lines.append(f"\n🔗 {link}")
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
#  KEYBOARD
# ══════════════════════════════════════════════════════════════════════════════

def main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([
        [KeyboardButton("🔍 Прогноз завтра"),   KeyboardButton("📅 Прогноз 2 дні")],
        [KeyboardButton("📊 Polymarket завтра"), KeyboardButton("📈 Мої позиції")],
        [KeyboardButton("🌤 Погода завтра"),     KeyboardButton("📉 Тренд цін")],
        [KeyboardButton("📋 Брифінг"),           KeyboardButton("❓ Допомога")],
    ], resize_keyboard=True)


def positions_keyboard(positions: dict) -> InlineKeyboardMarkup:
    buttons = []
    for dk, state in positions.items():
        dt  = state["target_date"]
        lbl = state["outcome_label"]
        buttons.append([
            InlineKeyboardButton(f"🔴 Закрити {dt.strftime('%d.%m')} {lbl}", callback_data=f"sell_{dk}"),
            InlineKeyboardButton(f"📈 Тренд {dt.strftime('%d.%m')}", callback_data=f"trend_{dk}"),
        ])
    buttons.append([InlineKeyboardButton("🔄 Оновити", callback_data="refresh_positions")])
    return InlineKeyboardMarkup(buttons)


# ══════════════════════════════════════════════════════════════════════════════
#  CORE REPORT
# ══════════════════════════════════════════════════════════════════════════════

async def _send_full_report(bot: Bot, dt: datetime,
                             chat_id: str | int, label: str = "🔍") -> None:
    fc = compute_forecast(dt)
    if "error" in fc:
        await bot.send_message(chat_id=chat_id, text=f"⚠️ {fc['error']}"); return
    cache_forecast(dt, fc)  # зберігаємо для /actual
    # Перевіряємо чи змінився прогноз порівняно з попереднім разом
    changed, prev_final = check_forecast_change(dt, fc["final_temp"])
    if changed:
        direction = "🔺" if fc["final_temp"] > prev_final else "🔻"
        await bot.send_message(
            chat_id=chat_id, parse_mode="Markdown",
            text=(
                f"{direction} *Прогноз змінився — {dt.strftime('%d.%m.%Y')}*\n\n"
                f"Було: *{prev_final:.1f}°C* → Стало: *{fc['final_temp']:.1f}°C*\n"
                f"Різниця: {fc['final_temp']-prev_final:+.1f}°C\n\n"
                f"Перевір позиції: /positions"
            )
        )
    _, markets, link = get_polymarket_data(dt)
    outcomes          = parse_all_outcomes(markets) if markets else {}
    tgt_lbl, tgt_pct = find_outcome_for_temp(outcomes, fc["final_int"]) if outcomes else (None, None)
    dk    = _date_key(dt)
    trend = get_trend(dk, tgt_lbl) if tgt_lbl else None
    msg = (f"*{label} — {dt.strftime('%d.%m.%Y')}{_days_label(dt)}*\n\n"
           + fmt_weather(dt, fc) + "\n"
           + fmt_polymarket(dt, outcomes, tgt_lbl, tgt_pct, link, fc["final_int"], trend))
    if tgt_pct is not None and tgt_pct < BUY_MAX_PCT:
        mn  = re.search(r"(\d+)", tgt_lbl or "")
        num = mn.group(1) if mn else "??"
        msg += (f"\n\n{'='*26}\n🟢 *BUY СИГНАЛ!* `{tgt_lbl}` = {tgt_pct}%\n"
                f"`/buy {num} {dt.strftime('%d.%m')}`")
    await bot.send_message(chat_id=chat_id, text=msg,
                           parse_mode="Markdown", reply_markup=main_keyboard())


# ══════════════════════════════════════════════════════════════════════════════
#  MONITOR JOB — кожні 2 хв
# ══════════════════════════════════════════════════════════════════════════════

async def monitor_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    now_date = datetime.utcnow().date()
    for dk, state in list(monitoring.items()):
        if not state.get("active"): continue
        dt = state["target_date"]
        if dt.date() < now_date:
            state["active"] = False
            await context.bot.send_message(
                chat_id=CHAT_ID,
                text=f"ℹ️ Моніторинг {dt.strftime('%d.%m.%Y')} завершено."); continue
        _, markets, link = get_polymarket_data(dt)
        if not markets: continue
        outcomes    = parse_all_outcomes(markets)
        label       = state["outcome_label"]
        current_pct = outcomes.get(label)
        if current_pct is None: continue

        record_price(dk, label, current_pct)
        logger.info("Monitor %s: %s @ %.1f%%", dk, label, current_pct)

        # Стандартні алерти рівнів
        for level in ALERT_LEVELS:
            if current_pct >= level and level not in state["alerted"]:
                state["alerted"].append(level)
                emoji = {40:"🟡",50:"🟠",60:"🔴",70:"🔴",80:"🚨",90:"🚨"}.get(level,"📢")
                rec   = "🔴 *Фіксуй прибуток!*" if level >= 50 else "⏳ Тримаємо"
                await context.bot.send_message(
                    chat_id=CHAT_ID, parse_mode="Markdown",
                    text=(f"{emoji} *АЛЕРТ {level}% — {dt.strftime('%d.%m')}*\n\n"
                          f"`{label}` → *{current_pct}%*\n_(куплено @ {state['buy_pct']}%)_\n\n"
                          f"{rec}\n🔗 {link}"))

        # Тейк-профіт
        tp = state.get("take_profit")
        if tp and current_pct >= tp and not state.get("tp_alerted"):
            state["tp_alerted"] = True
            await context.bot.send_message(
                chat_id=CHAT_ID, parse_mode="Markdown",
                text=(f"🎯 *ТЕЙК-ПРОФІТ {tp}% — {dt.strftime('%d.%m')}*\n\n"
                      f"`{label}` → *{current_pct}%*\n_(куплено @ {state['buy_pct']}%)_\n\n"
                      f"💰 Рекомендую продати!\n🔗 {link}"))

        # Стоп-лос
        sl = state.get("stop_loss")
        if sl and current_pct <= sl and not state.get("sl_alerted"):
            state["sl_alerted"] = True
            await context.bot.send_message(
                chat_id=CHAT_ID, parse_mode="Markdown",
                text=(f"🛑 *СТОП-ЛОС {sl}% — {dt.strftime('%d.%m')}*\n\n"
                      f"`{label}` → *{current_pct}%* ≤ {sl}%\n_(куплено @ {state['buy_pct']}%)_\n\n"
                      f"⚠️ Розглянь продаж щоб обмежити збиток!\n🔗 {link}"))

        # Momentum
        trend = get_trend(dk, label, 30)
        if trend and abs(trend["momentum"]) >= MOMENTUM_THRESHOLD:
            mom_key = f"mom_{int(trend['momentum'])}"
            if mom_key not in state.get("alerted_mom", []):
                state.setdefault("alerted_mom", []).append(mom_key)
                arrow = "🚀" if trend["momentum"] > 0 else "💥"
                await context.bot.send_message(
                    chat_id=CHAT_ID, parse_mode="Markdown",
                    text=(f"{arrow} *Різка зміна — {dt.strftime('%d.%m')}*\n\n"
                          f"`{label}`: {trend['momentum']:+.1f}% за 30хв\n"
                          f"Зараз: *{current_pct}%*\n🔗 {link}"))


# ══════════════════════════════════════════════════════════════════════════════
#  SCHEDULED JOBS
# ══════════════════════════════════════════════════════════════════════════════

async def job_morning_briefing(context: ContextTypes.DEFAULT_TYPE) -> None:
    """07:30 Kyiv."""
    now      = datetime.utcnow()
    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    await _send_full_report(context.bot, tomorrow, CHAT_ID, "🌅 Ранковий брифінг")

    active = {dk: s for dk, s in monitoring.items() if s.get("active")}
    if active:
        lines = ["📊 *Активні позиції:*"]
        for dk, state in sorted(active.items()):
            dt  = state["target_date"]
            lbl = state["outcome_label"]
            _, markets, _ = get_polymarket_data(dt)
            outcomes = parse_all_outcomes(markets) if markets else {}
            cur  = outcomes.get(lbl, "?")
            buy  = state["buy_pct"]
            roi_str = ""
            if isinstance(cur, float) and isinstance(buy, float) and buy > 0:
                roi = round((cur / buy - 1) * 100, 1)
                roi_str = f" │ ROI {roi:+.1f}%"
            t = get_trend(dk, lbl, 60)
            t_str = f" │ {t['delta']:+.1f}% /1г" if t else ""
            lines.append(f"  {dt.strftime('%d.%m')} `{lbl}`: {buy}% → *{cur}%*{roi_str}{t_str}")
        await context.bot.send_message(chat_id=CHAT_ID, text="\n".join(lines), parse_mode="Markdown")

    # Нагадування записати факт
    yesterday = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    _, y_mkts, _ = get_polymarket_data(yesterday)
    if y_mkts:
        await context.bot.send_message(
            chat_id=CHAT_ID, parse_mode="Markdown",
            text=(f"📝 Вчора {yesterday.strftime('%d.%m')} був ринок.\n"
                  f"Запиши факт для навчання:\n"
                  f"`/actual ECMWF <прогноз> <факт> {now.month}`\n"
                  f"`/actual DWD ICON <прогноз> <факт> {now.month}`\n"
                  f"`/actual UK Met Office <прогноз> <факт> {now.month}`"))


async def job_daily_14(context: ContextTypes.DEFAULT_TYPE) -> None:
    """14:00 Kyiv."""
    tomorrow = (datetime.utcnow() + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0)
    await _send_full_report(context.bot, tomorrow, CHAT_ID, "⏰ 14:00 Kyiv")


async def job_market_scan(context: ContextTypes.DEFAULT_TYPE) -> None:
    """09:00 Kyiv — скан нових ринків."""
    now   = datetime.utcnow()
    found = []
    for days_ahead in range(1, 5):
        dt = (now + timedelta(days=days_ahead)).replace(hour=0, minute=0, second=0, microsecond=0)
        event, markets, link = get_polymarket_data(dt)
        if not event or not markets: continue
        outcomes    = parse_all_outcomes(markets)
        buy_signals = [(lbl, pct) for lbl, pct in outcomes.items() if pct < BUY_MAX_PCT]
        if buy_signals:
            best_lbl, best_pct = min(buy_signals, key=lambda x: x[1])
            mn  = re.search(r"(\d+)", best_lbl)
            num = mn.group(1) if mn else "??"
            found.append(
                f"📅 *{dt.strftime('%d.%m.%Y')}{_days_label(dt)}*\n"
                f"  🟢 `{best_lbl}` = {best_pct}%\n"
                f"  `/buy {num} {dt.strftime('%d.%m')}`\n  🔗 {link}")
    if found:
        await context.bot.send_message(
            chat_id=CHAT_ID, parse_mode="Markdown",
            text="🔍 *Авто-скан — BUY сигнали:*\n\n" + "\n\n".join(found))


# ══════════════════════════════════════════════════════════════════════════════
#  COMMAND HANDLERS
# ══════════════════════════════════════════════════════════════════════════════

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cid = update.effective_chat.id
    await update.message.reply_text(
        f"🤖 *London EGLC Temp Bot v4*\nchat\\_id: `{cid}`\n\n"
        f"Використовуй *кнопки* або команди:\n\n"
        f"*Прогноз:* `/check` `/check2` `/forecast` `/poll`\n"
        f"*Торгівля:*\n"
        f"`/buy <temp> [DD\\.MM] [\\-\\-stop X] [\\-\\-tp Y]`\n"
        f"  напр: `/buy 17 29\\.04 \\-\\-stop 20 \\-\\-tp 65`\n"
        f"`/sell [DD\\.MM|all]` `/positions` `/trend`\n"
        f"*Навчання:* `/actual` `/history`\n"
        f"`/briefing` — ручний брифінг\n\n"
        f"⏰ Авто: 07:30 брифінг │ 09:00 скан │ 14:00 звіт\n"
        f"🔔 Алерти: 40→50→60→70→80→90%\n"
        f"🛑 Стоп\\-лос │ 🎯 Тейк\\-профіт │ 🚀 Momentum",
        parse_mode="MarkdownV2", reply_markup=main_keyboard())


async def cmd_check(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    dt, err = parse_target_date(context.args)
    if err: await update.message.reply_text(err, parse_mode="Markdown"); return
    await update.message.reply_text(
        f"⏳ *{dt.strftime('%d.%m.%Y')}*…", parse_mode="Markdown")
    await _send_full_report(context.bot, dt, update.effective_chat.id, "🔍 Запит")


async def cmd_check2(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    now = datetime.utcnow()
    await update.message.reply_text("⏳ Збираю дані для 2 днів…")
    for days in (1, 2):
        dt = (now + timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)
        await _send_full_report(context.bot, dt, update.effective_chat.id, "🔍 Запит")


async def cmd_poll(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    dt, err = parse_target_date(context.args)
    if err: await update.message.reply_text(err, parse_mode="Markdown"); return
    fc = compute_forecast(dt)
    if "error" in fc: await update.message.reply_text(f"⚠️ {fc['error']}"); return
    _, markets, link = get_polymarket_data(dt)
    outcomes          = parse_all_outcomes(markets) if markets else {}
    tgt_lbl, tgt_pct = find_outcome_for_temp(outcomes, fc["final_int"]) if outcomes else (None, None)
    dk = _date_key(dt); trend = get_trend(dk, tgt_lbl) if tgt_lbl else None
    await update.message.reply_text(
        f"📡 *{dt.strftime('%d.%m.%Y')}{_days_label(dt)}*\n"
        f"🎯 Прогноз: *{fc['final_int']}°C* ({fc['confidence']})\n"
        + fmt_polymarket(dt, outcomes, tgt_lbl, tgt_pct, link, fc["final_int"], trend),
        parse_mode="Markdown", reply_markup=main_keyboard())


async def cmd_forecast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    dt, err = parse_target_date(context.args)
    if err: await update.message.reply_text(err, parse_mode="Markdown"); return
    await update.message.reply_text(f"🌤 *{dt.strftime('%d.%m.%Y')}*…", parse_mode="Markdown")
    fc = compute_forecast(dt)
    if "error" in fc: await update.message.reply_text(f"⚠️ {fc['error']}"); return
    await update.message.reply_text(fmt_weather(dt, fc), parse_mode="Markdown",
                                    reply_markup=main_keyboard())


async def cmd_trend(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    dt, err = parse_target_date(context.args)
    if err: await update.message.reply_text(err, parse_mode="Markdown"); return
    dk    = _date_key(dt)
    state = monitoring.get(dk)
    if not state or not state.get("active"):
        # Показуємо тренди всіх активних позицій
        active = {k: s for k, s in monitoring.items() if s.get("active")}
        if not active:
            await update.message.reply_text("⚠️ Немає активних позицій."); return
        for adk, astate in active.items():
            trend = get_trend(adk, astate["outcome_label"], 180)
            if not trend: continue
            arrow = "📈" if trend["delta"] > 0 else "📉"
            adt   = astate["target_date"]
            await update.message.reply_text(
                f"📊 *{adt.strftime('%d.%m')} `{astate['outcome_label']}`*\n"
                f"{arrow} {trend['first']}% → *{trend['last']}%* ({trend['delta']:+.1f}%)\n"
                f"`{trend['spark']}`",
                parse_mode="Markdown")
        return
    trend = get_trend(dk, state["outcome_label"], 180)
    if not trend: await update.message.reply_text("📊 Мало даних (< 2 точок)."); return
    arrow = "📈" if trend["delta"] > 0 else "📉"
    mo_str = ""
    if abs(trend["momentum"]) >= MOMENTUM_THRESHOLD:
        mo = "🚀" if trend["momentum"] > 0 else "💥"
        mo_str = f"\n{mo} *Momentum 30хв: {trend['momentum']:+.1f}%*"
    await update.message.reply_text(
        f"📊 *Тренд {dt.strftime('%d.%m')} `{state['outcome_label']}`*\n\n"
        f"{arrow} {trend['first']}% → *{trend['last']}%* ({trend['delta']:+.1f}% / {trend['minutes']}хв)\n"
        f"Точок: {trend['n']}\n\n`{trend['spark']}`{mo_str}",
        parse_mode="Markdown", reply_markup=main_keyboard())


async def cmd_buy(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text(
            "❓ `/buy <temp> [DD.MM] [--stop X] [--tp Y]`\n"
            "Приклад: `/buy 17 29.04 --stop 20 --tp 65`",
            parse_mode="Markdown"); return
    try: temp_int = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Температура — ціле число.", parse_mode="Markdown"); return

    remaining = list(context.args[1:])
    stop_loss = None; take_profit = None; buy_price = None; clean_args = []
    i = 0
    while i < len(remaining):
        if remaining[i] == "--stop" and i+1 < len(remaining):
            try: stop_loss = float(remaining[i+1]); i += 2; continue
            except: pass
        if remaining[i] == "--tp" and i+1 < len(remaining):
            try: take_profit = float(remaining[i+1]); i += 2; continue
            except: pass
        if remaining[i] == "--price" and i+1 < len(remaining):
            try: buy_price = float(remaining[i+1]); i += 2; continue
            except: pass
        clean_args.append(remaining[i]); i += 1

    dt, err = parse_target_date(clean_args if clean_args else [])
    if err: await update.message.reply_text(err, parse_mode="Markdown"); return

    await update.message.reply_text(
        f"🔍 Шукаю *{temp_int}°C* на {dt.strftime('%d.%m.%Y')}…", parse_mode="Markdown")

    _, markets, link = get_polymarket_data(dt)
    outcomes = parse_all_outcomes(markets) if markets else {}
    lbl, pct = find_outcome_for_temp(outcomes, temp_int) if outcomes else (None, None)

    if not lbl:
        debug = [f"⚠️ Не знайдено outcome для *{temp_int}°C*.\n"]
        if not markets:
            debug.append("_Ринок не відкрито або slug невірний_")
        else:
            debug.append(f"_Знайдено {len(markets)} markets:_")
            for i2, mkt in enumerate(markets[:6]):
                raw_q = mkt.get("question", "?")[:55]
                norm  = _normalize_temp_label(mkt.get("question", ""))
                debug.append(f"  `{i2+1}. {raw_q}`\n     `→ {norm}`")
        debug.append(f"\n🔗 {link}")
        await update.message.reply_text("\n".join(debug), parse_mode="Markdown"); return

    dk = _date_key(dt)
    if monitoring.get(dk, {}).get("active"):
        await update.message.reply_text(
            f"⚠️ Попередня позиція `{monitoring[dk]['outcome_label']}` зупинена.",
            parse_mode="Markdown")

    already = [l for l in ALERT_LEVELS if pct is not None and pct >= l]
    pending = [l for l in ALERT_LEVELS if l not in already]
    monitoring[dk] = {
        "active": True, "target_date": dt, "outcome_label": lbl, "temp_int": temp_int,
        "buy_pct": buy_price if buy_price is not None else pct,
        "alerted": already, "poly_link": link,
        "stop_loss": stop_loss, "take_profit": take_profit,
        "tp_alerted": False, "sl_alerted": False, "alerted_mom": [],
    }
    sl_str = f"\n🛑 Стоп-лос: *{stop_loss}%*" if stop_loss else ""
    tp_str = f"\n🎯 Тейк-профіт: *{take_profit}%*" if take_profit else ""
    recorded_pct = buy_price if buy_price is not None else pct
    price_note = ""
    roi_str = ""
    if buy_price is not None:
        price_note = f"\n💵 Куплено за: *{buy_price}%* _(зараз {pct}%)_"
        if pct and buy_price > 0:
            roi = round((pct / buy_price - 1) * 100, 1)
            roi_str = f"\n📈 Поточний ROI: *{roi:+.1f}%*"
    await update.message.reply_text(
        f"✅ *Позицію відкрито*\n\n"
        f"📅 {dt.strftime('%d.%m.%Y')}\n"
        f"🎯 `{lbl}`\n💰 *{recorded_pct}%*{price_note}{roi_str}{sl_str}{tp_str}\n\n"
        f"🔔 Алерти: {', '.join(str(l)+'%' for l in pending) or 'всі пройдено'}\n"
        f"Позицій: {sum(1 for s in monitoring.values() if s.get('active'))}\n\n🔗 {link}",
        parse_mode="Markdown", reply_markup=main_keyboard())


async def cmd_sell(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    active = {dk: s for dk, s in monitoring.items() if s.get("active")}
    if not active: await update.message.reply_text("ℹ️ Немає активних позицій."); return
    arg = context.args[0].lower() if context.args else ""
    if arg == "all":
        to_close = list(active.keys())
    elif arg:
        dt_parsed, err = parse_target_date(context.args)
        if err or dt_parsed is None:
            await update.message.reply_text("❌ `/sell DD.MM` або `/sell all`", parse_mode="Markdown"); return
        dk = _date_key(dt_parsed)
        if dk not in active:
            dates = ", ".join(s["target_date"].strftime("%d.%m") for s in active.values())
            await update.message.reply_text(f"⚠️ Немає позиції. Активні: {dates}"); return
        to_close = [dk]
    else:
        if len(active) == 1: to_close = list(active.keys())
        else:
            dates = ", ".join(s["target_date"].strftime("%d.%m") for s in active.values())
            await update.message.reply_text(
                f"❓ Кілька: {dates}\n`/sell DD.MM` або `/sell all`", parse_mode="Markdown"); return
    lines = []
    for dk in to_close:
        state = monitoring[dk]; dt = state["target_date"]; lbl = state["outcome_label"]; buy = state["buy_pct"]
        _, markets, _ = get_polymarket_data(dt)
        outcomes = parse_all_outcomes(markets) if markets else {}
        cur = outcomes.get(lbl)
        state["active"] = False
        profit = ""
        if isinstance(cur, float) and isinstance(buy, float) and buy > 0:
            roi = round((cur / buy - 1) * 100, 1); profit = f" │ ROI: {roi:+.1f}%"
        lines.append(f"🛑 *{dt.strftime('%d.%m')}* `{lbl}`: {buy}% → {cur}%{profit}")
    await update.message.reply_text("\n\n".join(lines), parse_mode="Markdown",
                                    reply_markup=main_keyboard())


async def cmd_positions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    active = {dk: s for dk, s in monitoring.items() if s.get("active")}
    if not active:
        await update.message.reply_text("📊 Немає позицій.\n`/buy <temp>`",
                                        parse_mode="Markdown", reply_markup=main_keyboard()); return
    lines = [f"📊 *Активні позиції ({len(active)}):*\n"]
    for dk, state in sorted(active.items()):
        dt = state["target_date"]; lbl = state["outcome_label"]; buy = state["buy_pct"]
        _, markets, link = get_polymarket_data(dt)
        outcomes = parse_all_outcomes(markets) if markets else {}
        cur = outcomes.get(lbl, "?"); trend = get_trend(dk, lbl, 60)
        roi_str = ""
        if isinstance(cur, float) and isinstance(buy, float) and buy > 0:
            roi = round((cur / buy - 1) * 100, 1)
            roi_str = f" │ {'📈' if roi >= 0 else '📉'} {roi:+.1f}%"
        t_str  = f" │ {trend['delta']:+.1f}%/1г" if trend else ""
        sl_str = f" 🛑{state['stop_loss']}%" if state.get("stop_loss") else ""
        tp_str = f" 🎯{state['take_profit']}%" if state.get("take_profit") else ""
        pending = [l for l in ALERT_LEVELS if l not in state["alerted"]]
        lines.append(
            f"*{dt.strftime('%d.%m')}{_days_label(dt)}* `{lbl}`\n"
            f"  {buy}% → *{cur}%*{roi_str}{t_str}{sl_str}{tp_str}\n"
            f"  Алерт → {pending[0]}%" if pending else "  ✅ всі алерти"
        )
        lines.append(f"  [Polymarket]({link})\n")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown",
                                    reply_markup=positions_keyboard(active))


async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not SOURCE_STATS:
        await update.message.reply_text(
            "📊 Немає даних.\nЗапис: `/actual ECMWF 17.2 16.5 4`",
            parse_mode="Markdown"); return
    lines = ["📊 *Точність моделей (EGLC)*\n"]
    for src, months in SOURCE_STATS.items():
        lines.append(f"*{src}:*")
        for mk, st in sorted(months.items(), key=lambda x: int(x[0])):
            mn = datetime(2000, int(mk), 1).strftime("%B")
            lines.append(f"  {mn}: MAE {st['mae']:.1f}°C, зміщ {st['bias']:+.1f}°C (n={st['n']})")
        lines.append("")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown",
                                    reply_markup=main_keyboard())


async def cmd_actual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /actual <факт°C> [DD.MM]
    Бот сам знає що прогнозував — треба вказати тільки фактичну температуру EGLC.
    Якщо дату не вказано — використовується вчора.
    """
    if not context.args:
        await update.message.reply_text(
            "❓ `/actual <факт EGLC°C> [DD.MM]`\n\n"
            "Вкажи лише *фактичну* температуру — бот сам знає свій прогноз.\n\n"
            "Приклади:\n"
            "`/actual 16.5` — факт за вчора\n"
            "`/actual 16.5 28.04` — факт за конкретний день",
            parse_mode="Markdown"); return

    try:
        actual_temp = float(context.args[0].replace(",", "."))
    except ValueError:
        await update.message.reply_text(
            "❌ Вкажи температуру числом. Приклад: `/actual 16.5`",
            parse_mode="Markdown"); return

    # Визначаємо дату (parse_past_date — дозволяє минулі)
    if len(context.args) > 1:
        dt, err = parse_past_date(context.args[1:])
        if err:
            await update.message.reply_text(err, parse_mode="Markdown"); return
    else:
        # За замовчуванням — вчора
        dt = (datetime.utcnow() - timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0)

    dk    = dt.strftime("%Y-%m-%d")
    month = dt.month
    cached = forecast_cache.get(dk)

    if not cached:
        # Кеш відсутній (новий деплой або дата давня) — дозволяємо ручне введення
        # Формат: /actual <факт> <дата> <ECMWF_прогноз> <DWD_прогноз> <UKMet_прогноз>
        if len(context.args) >= 5:
            try:
                manual = {
                    "ECMWF":          float(context.args[2]),
                    "DWD ICON":       float(context.args[3]),
                    "UK Met Office":  float(context.args[4]),
                    "final":          float(context.args[2]),
                    "month":          dt.month,
                }
                cached = manual
            except (ValueError, IndexError):
                pass
        if not cached:
            avail = ", ".join(sorted(forecast_cache.keys())[-5:]) or "немає"
            await update.message.reply_text(
                f"⚠️ Немає збереженого прогнозу для *{dt.strftime('%d.%m.%Y')}*.\n\n"
                f"Прогнози зберігаються після кожного `/check` або авто-звіту.\n"
                f"Доступні дати: {avail}\n\n"
                f"*Або введи вручну:*\n"
                f"`/actual {actual_temp} {dt.strftime('%d.%m')} <ECMWF> <DWD> <UKMet>`\n"
                f"Приклад: `/actual {actual_temp} {dt.strftime('%d.%m')} 16.8 16.2 16.8`",
                parse_mode="Markdown"); return

    # Записуємо факт для кожної моделі яка є в кеші
    lines = [f"✅ *Факт EGLC {dt.strftime('%d.%m.%Y')}: {actual_temp}°C*\n"]
    sources_in_cache = ["ECMWF", "DWD ICON", "UK Met Office"]
    for source_name in sources_in_cache:
        predicted = cached.get(source_name)
        if predicted is None:
            lines.append(f"  ⚠️ {source_name}: прогноз не знайдено в кеші")
            continue
        error = round(predicted - actual_temp, 1)
        record_actual(source_name, month, predicted, actual_temp)
        new_bias = get_learned_bias(source_name, month)
        n = SOURCE_STATS.get(source_name, {}).get(str(month), {}).get("n", 0)
        status = "✅ активна" if n >= 5 else f"⏳ ще {5-n}"
        lines.append(
            f"  *{source_name}*: прогноз {predicted}°C, факт {actual_temp}°C, "
            f"помилка {error:+.1f}°C\n"
            f"    → поправка: *{new_bias:+.2f}°C* (n={n}, {status})"
        )

    # Фінальний прогноз
    final = cached.get("final")
    if final:
        final_err = round(final - actual_temp, 1)
        lines.append(f"\n📍 Фінальний прогноз: {final}°C, факт: {actual_temp}°C, "
                     f"помилка: {final_err:+.1f}°C")

    lines.append("\n/history — вся статистика")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown",
                                    reply_markup=main_keyboard())


async def cmd_briefing(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await job_morning_briefing(context)


# ══════════════════════════════════════════════════════════════════════════════
#  BUTTON HANDLERS
# ══════════════════════════════════════════════════════════════════════════════

async def job_forecast_monitor(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Кожні 3 години перевіряє прогноз для всіх дат з активними позиціями.
    Якщо прогноз змінився на >= 1°C — надсилає алерт.
    Не залежить від /check — працює автономно.
    """
    active_dates = {
        state["target_date"]
        for state in monitoring.values()
        if state.get("active")
    }

    if not active_dates:
        return

    for dt in active_dates:
        try:
            fc = compute_forecast(dt)
            if "error" in fc:
                continue

            cache_forecast(dt, fc)
            changed, prev_final = check_forecast_change(dt, fc["final_temp"])

            if changed:
                direction = "🔺" if fc["final_temp"] > prev_final else "🔻"
                diff = fc["final_temp"] - prev_final

                dk    = _date_key(dt)
                state = monitoring.get(dk, {})
                pos_info = ""
                if state.get("active"):
                    lbl = state["outcome_label"]
                    pos_info = ("\n\U0001f4ca Твоя позиція: `"
                               + lbl + "` @ " + str(state["buy_pct"]) + "%")

                sources_str = ", ".join(
                    s["source"] + " " + f"{s['temp_max']:.1f}°C"
                    for s in fc["sources"]
                )
                alert_text = (
                    f"{direction} *Прогноз змінився — {dt.strftime('%d.%m.%Y')}*\n\n"
                    f"Було: *{prev_final:.1f}°C* → Стало: *{fc['final_temp']:.1f}°C*\n"
                    f"Зміна: *{diff:+.1f}°C*\n"
                    f"Джерела: {sources_str}"
                    + pos_info
                    + "\n\n/positions — переглянути позиції"
                )
                await context.bot.send_message(
                    chat_id=CHAT_ID,
                    parse_mode="Markdown",
                    text=alert_text,
                )
                logger.info(
                    "Forecast change alert: %s %.1f→%.1f°C",
                    dt.strftime("%Y-%m-%d"), prev_final, fc["final_temp"]
                )
        except Exception as e:
            logger.error("forecast_monitor error for %s: %s", dt.date(), e)


async def handle_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = update.message.text
    now  = datetime.utcnow()
    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    cid  = update.effective_chat.id

    if text == "🔍 Прогноз завтра":
        await _send_full_report(context.bot, tomorrow, cid, "🔍 Прогноз")
    elif text == "📅 Прогноз 2 дні":
        await update.message.reply_text("⏳ Збираю дані для 2 днів…")
        for days in (1, 2):
            dt = (now + timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)
            await _send_full_report(context.bot, dt, cid, "🔍 Прогноз")
    elif text == "📊 Polymarket завтра":
        context.args = []
        await cmd_poll(update, context)
    elif text == "📈 Мої позиції":
        await cmd_positions(update, context)
    elif text == "🌤 Погода завтра":
        context.args = []
        await cmd_forecast(update, context)
    elif text == "📉 Тренд цін":
        context.args = []
        await cmd_trend(update, context)
    elif text == "📋 Брифінг":
        await job_morning_briefing(context)
    elif text == "❓ Допомога":
        await cmd_start(update, context)


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data  = query.data

    if data.startswith("sell_"):
        dk    = data[5:]; state = monitoring.get(dk)
        if not state or not state.get("active"):
            await query.edit_message_text("⚠️ Позиція вже закрита."); return
        dt  = state["target_date"]; lbl = state["outcome_label"]; buy = state["buy_pct"]
        _, markets, _ = get_polymarket_data(dt)
        outcomes = parse_all_outcomes(markets) if markets else {}
        cur = outcomes.get(lbl, "?"); state["active"] = False
        profit = ""
        if isinstance(cur, float) and isinstance(buy, float) and buy > 0:
            roi = round((cur/buy-1)*100, 1); profit = f"\nROI: {roi:+.1f}%"
        await query.edit_message_text(
            f"🛑 *Закрито*\n{dt.strftime('%d.%m')} `{lbl}`: {buy}% → {cur}%{profit}",
            parse_mode="Markdown")

    elif data.startswith("trend_"):
        dk    = data[6:]; state = monitoring.get(dk)
        if not state: await query.edit_message_text("⚠️ Позиція не знайдена."); return
        trend = get_trend(dk, state["outcome_label"], 180)
        if not trend: await query.edit_message_text("📊 Недостатньо даних."); return
        arrow = "📈" if trend["delta"] > 0 else "📉"
        await query.edit_message_text(
            f"📊 `{state['outcome_label']}`\n"
            f"{arrow} {trend['first']}% → *{trend['last']}%* ({trend['delta']:+.1f}%)\n"
            f"`{trend['spark']}`", parse_mode="Markdown")

    elif data == "refresh_positions":
        await cmd_positions(update, context)


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    if not TOKEN:  raise ValueError("BOT_TOKEN not set!")
    if not CHAT_ID: raise ValueError("CHAT_ID not set!")

    load_history()
    load_price_history()
    load_forecast_cache()
    load_forecast_changes()

    app = ApplicationBuilder().token(TOKEN).build()

    for cmd, handler in [
        ("start",     cmd_start),     ("check",    cmd_check),
        ("check2",    cmd_check2),    ("poll",     cmd_poll),
        ("forecast",  cmd_forecast),  ("trend",    cmd_trend),
        ("buy",       cmd_buy),       ("sell",     cmd_sell),
        ("positions", cmd_positions), ("history",  cmd_history),
        ("actual",    cmd_actual),    ("briefing", cmd_briefing),
    ]:
        app.add_handler(CommandHandler(cmd, handler))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_button))
    app.add_handler(CallbackQueryHandler(handle_callback))

    jq = app.job_queue
    kyiv_730 = datetime.now(KYIV_TZ).replace(hour=7,  minute=30, second=0, microsecond=0)
    kyiv_9   = datetime.now(KYIV_TZ).replace(hour=9,  minute=0,  second=0, microsecond=0)
    kyiv_14  = datetime.now(KYIV_TZ).replace(hour=14, minute=0,  second=0, microsecond=0)

    jq.run_daily(job_morning_briefing, time=kyiv_730.timetz(), name="briefing_730")
    jq.run_daily(job_market_scan,      time=kyiv_9.timetz(),   name="market_scan_9")
    jq.run_daily(job_daily_14,         time=kyiv_14.timetz(),  name="daily_14")
    jq.run_repeating(monitor_job,          interval=120,        first=15,   name="price_monitor")
    jq.run_repeating(job_forecast_monitor, interval=3*60*60,    first=60,   name="forecast_monitor")  # кожні 3 год

    start_keep_alive()
    logger.info("Bot v4 | 07:30 briefing | 09:00 scan | 14:00 daily | price 2min | forecast 3h")
    app.run_polling()


if __name__ == "__main__":
    main()
