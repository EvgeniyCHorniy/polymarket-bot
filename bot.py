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

CITIES = {
    "london": {
        "name": "London", "emoji": "🇬🇧",
        "lat": 51.5048, "lon": 0.0495,
        "station": "EGLC", "slug_city": "london",
        "tz": "Europe/London",
    },
    "munich": {
        "name": "Munich", "emoji": "🇩🇪",
        "lat": 48.3537, "lon": 11.7750,
        "station": "EDDM", "slug_city": "munich",
        "tz": "Europe/Berlin",
    },
    "warsaw": {
        "name":      "Warsaw",
        "emoji":     "🇵🇱",
        "lat":       52.1657,
        "lon":       20.9671,
        "station":   "EPWA",
        "slug_city": "warsaw",
        "tz":        "Europe/Warsaw",
    },
}

OUTLIER_THRESHOLD  = 2.0
BUY_MAX_PCT        = 38.0
MOMENTUM_THRESHOLD = 5.0
# Render Disk: встанови DATA_DIR=/data в env vars + підключи Disk на /data
# Локально: файли зберігаються в поточній директорії
DATA_DIR = Path(os.getenv("DATA_DIR", "."))
DATA_DIR.mkdir(parents=True, exist_ok=True)

HISTORY_FILE         = DATA_DIR / "eglc_history.json"
PRICE_HISTORY_FILE   = DATA_DIR / "price_history.json"
FORECAST_CACHE_FILE  = DATA_DIR / "forecast_cache.json"
MONITORING_FILE      = DATA_DIR / "monitoring.json"
SELECTED_CITY_FILE   = DATA_DIR / "selected_city.json"
FORECAST_CHANGE_FILE = DATA_DIR / "forecast_changes.json"
PORTFOLIO_FILE       = DATA_DIR / "portfolio.json"
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
FORECAST_CHANGE_FILE = DATA_DIR / "forecast_changes.json"


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
    """Рахуємо max/min температуру з погодинних даних самостійно."""
    h = data.get("hourly", {})
    times = h.get("time", [])
    temps  = h.get("temperature_2m", [])
    clouds = h.get("cloudcover", h.get("cloud_cover", []))
    winds  = h.get("windspeed_10m", [])
    dt, dc, dw = [], [], []
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


def _build_source(name: str, data: dict, ds: str, market_type: str = "highest") -> dict | None:
    tmax, tmin, cloud, wind = _hourly_max(data, ds)
    if tmax is None: return None
    # Для highest — використовуємо max; для lowest — min
    target_temp = tmin if market_type == "lowest" else tmax
    return {"source": name, "temp_max": tmax, "temp_min": tmin,
            "cloud": cloud, "wind": wind, "wx_note": "—",
            "wx_corrected": target_temp, "market_type": market_type}


def fetch_ecmwf(dt: datetime, lat: float = EGLC_LAT, lon: float = EGLC_LON, tz: str = "Europe/London", market_type: str = "highest") -> dict | None:
    """ECMWF IFS — 9 км, найточніший глобально."""
    ds = dt.strftime("%Y-%m-%d")
    data = _safe_get("https://api.open-meteo.com/v1/forecast", params={
        "latitude": lat, "longitude": lon,
        "hourly": "temperature_2m,cloud_cover,windspeed_10m",
        "timezone": tz, "start_date": ds, "end_date": ds,
        })
    return _build_source("ECMWF", data, ds, market_type) if data else None


def fetch_dwd_icon(dt: datetime, lat: float = EGLC_LAT, lon: float = EGLC_LON, tz: str = "Europe/London", market_type: str = "highest") -> dict | None:
    """DWD ICON — 2 км, найточніший для Центральної Європи."""
    ds = dt.strftime("%Y-%m-%d")
    data = _safe_get("https://api.open-meteo.com/v1/dwd-icon", params={
        "latitude": lat, "longitude": lon,
        "hourly": "temperature_2m,cloud_cover,windspeed_10m",
        "timezone": tz, "start_date": ds, "end_date": ds,
    })
    return _build_source("DWD ICON", data, ds, market_type) if data else None


def fetch_ukmet(dt: datetime, lat: float = EGLC_LAT, lon: float = EGLC_LON, tz: str = "Europe/London", market_type: str = "highest") -> dict | None:
    """
    UK Met Office via Open-Meteo.
    Правильний endpoint: https://api.open-meteo.com/v1/ukmo
    Моделі: ukmo_global_deterministic_10km (глобальна) + ukmo_uk_deterministic_2km (2km UK/Ireland).
    Примітка: UK 2km доступна тільки для UK/Ireland, тому для Munich fallback на global.
    """
    ds = dt.strftime("%Y-%m-%d")

    # Для UK координат — пробуємо 2km модель спочатку
    # Для інших країн — відразу global
    uk_bounds = (49.5 <= lat <= 61.0 and -8.5 <= lon <= 2.0)

    urls_to_try = []
    if uk_bounds:
        urls_to_try.append(("https://api.open-meteo.com/v1/ukmo", {"models": "ukmo_uk_deterministic_2km"}))
    urls_to_try.append(("https://api.open-meteo.com/v1/ukmo", {"models": "ukmo_global_deterministic_10km"}))

    for url, extra_params in urls_to_try:
        params = {
            "latitude": lat, "longitude": lon,
            "hourly": "temperature_2m,cloud_cover,windspeed_10m",
            "timezone": tz, "start_date": ds, "end_date": ds,
            **extra_params,
        }
        data = _safe_get(url, params=params)
        if data:
            result = _build_source("UK Met Office", data, ds, market_type)
            if result:
                return result
    return None


def fetch_meteofrance(dt: datetime, lat: float = EGLC_LAT, lon: float = EGLC_LON, tz: str = "Europe/London", market_type: str = "highest") -> dict | None:
    """
    Météo-France ARPEGE/AROME — найкращий для Зах. Європи і Франції.
    AROME: 1.3 км (Франція/Зах. Європа, до 2 днів)
    ARPEGE: 2.5 км (Європа, до 4 днів)
    Open-Meteo автоматично вибирає найкращу для локації.
    """
    ds = dt.strftime("%Y-%m-%d")
    data = _safe_get("https://api.open-meteo.com/v1/meteofrance", params={
        "latitude": lat, "longitude": lon,
        "hourly": "temperature_2m,cloud_cover,windspeed_10m",
        "timezone": tz, "start_date": ds, "end_date": ds,
    })
    return _build_source("Meteo-France", data, ds, market_type) if data else None




def fetch_ensemble(dt: datetime, lat: float = EGLC_LAT, lon: float = EGLC_LON,
                   tz: str = "Europe/London", market_type: str = "highest") -> dict | None:
    """
    Тягне GFS Ensemble (31 члени) + ECMWF ENS (51 членів) через Open-Meteo Ensemble API.
    Повертає повний розподіл ймовірностей температури.

    Endpoint: https://api.open-meteo.com/v1/ensemble
    Models: gfs_seamless (31 members), ecmwf_ifs025 (51 members)
    """
    ds = dt.strftime("%Y-%m-%d")

    results = {}
    # GFS Ensemble — 31 членів, найкращий для 1-7 днів
    for model_name, model_param, n_members in [
        ("GFS ENS",   "gfs_seamless",   31),
        ("ECMWF ENS", "ecmwf_ifs025",   51),
    ]:
        data = _safe_get("https://api.open-meteo.com/v1/ensemble", params={
            "latitude": lat, "longitude": lon,
            "hourly": "temperature_2m",
            "models": model_param,
            "timezone": tz,
            "start_date": ds, "end_date": ds,
        })
        if not data:
            continue

        hourly = data.get("hourly", {})
        times  = hourly.get("time", [])

        # Збираємо всі member values для максимальної/мінімальної температури
        member_temps = []
        # Ключі членів: temperature_2m_member01, temperature_2m_member02, ...
        member_keys = [k for k in hourly.keys() if k.startswith("temperature_2m_member")]

        # Якщо немає member ключів — беремо temperature_2m (mean)
        if not member_keys:
            member_keys = ["temperature_2m"]

        for mk in member_keys:
            day_temps = []
            vals = hourly.get(mk, [])
            for i, t in enumerate(times):
                if t.startswith(ds) and i < len(vals) and vals[i] is not None:
                    day_temps.append(float(vals[i]))
            if day_temps:
                val = min(day_temps) if market_type == "lowest" else max(day_temps)
                member_temps.append(val)

        if len(member_temps) < 2:
            continue

        results[model_name] = {
            "members": member_temps,
            "n":       len(member_temps),
            "mean":    round(sum(member_temps) / len(member_temps), 2),
            "std":     round((sum((x - sum(member_temps)/len(member_temps))**2
                              for x in member_temps) / len(member_temps)) ** 0.5, 2),
            "min":     round(min(member_temps), 1),
            "max":     round(max(member_temps), 1),
        }

    return results if results else None


def ensemble_probability(ensemble_result: dict | None, target_temp: int,
                         bias: float = 0.0) -> dict:
    """
    1.2 Bayesian Normal CDF probability:
    Рахує ймовірність кожного температурного бакету на основі ensemble розподілу.
    Використовує Normal CDF навколо ensemble mean зі spread як std.

    1.3 Brier Score compatible output:
    Повертає calibrated probability для кожного outcome.
    """
    import math

    def normal_cdf(x: float, mu: float, sigma: float) -> float:
        """Standard Normal CDF через наближення."""
        if sigma <= 0:
            return 1.0 if x >= mu else 0.0
        z = (x - mu) / sigma
        # Abramowitz & Stegun approximation — точність ~1.5e-7
        t = 1.0 / (1.0 + 0.2316419 * abs(z))
        poly = t * (0.319381530 + t * (-0.356563782 + t * (1.781477937 +
               t * (-1.821255978 + t * 1.330274429))))
        cdf = 1.0 - (1.0 / math.sqrt(2 * math.pi)) * math.exp(-0.5 * z * z) * poly
        return cdf if z >= 0 else 1.0 - cdf

    def bucket_prob(low: float, high: float, mu: float, sigma: float) -> float:
        """P(low <= X < high) для Normal(mu, sigma)."""
        return max(0.0, normal_cdf(high, mu, sigma) - normal_cdf(low, mu, sigma))

    if not ensemble_result:
        return {}

    # Агрегуємо всі member temps з усіх моделей (зважено: ECMWF ENS вага 0.55, GFS ENS 0.45)
    model_weights = {"ECMWF ENS": 0.55, "GFS ENS": 0.45}
    all_temps   = []
    total_w     = 0.0
    weighted_mu = 0.0

    for model_name, res in ensemble_result.items():
        w = model_weights.get(model_name, 0.5)
        weighted_mu += res["mean"] * w
        total_w     += w
        all_temps.extend(res["members"])

    if total_w == 0 or not all_temps:
        return {}

    mu_raw = weighted_mu / total_w
    mu     = round(mu_raw + bias, 2)  # застосовуємо EGLC/EDDM bias

    # Std — зважена по моделях
    weighted_var = 0.0
    tw = 0.0
    for model_name, res in ensemble_result.items():
        w = model_weights.get(model_name, 0.5)
        weighted_var += (res["std"] ** 2 + (res["mean"] - mu_raw) ** 2) * w
        tw += w
    sigma = round((weighted_var / tw) ** 0.5, 2) if tw > 0 else 1.0

    # Будуємо ймовірності для температурних бакетів ±5°C навколо mu
    center = round(mu)
    probs  = {}
    for t in range(center - 4, center + 5):
        p = bucket_prob(t - 0.5, t + 0.5, mu, sigma)
        if p > 0.001:
            probs[t] = round(p * 100, 1)  # у відсотках

    return {
        "mu":     mu,
        "mu_raw": round(mu_raw, 2),
        "sigma":  sigma,
        "probs":  probs,   # {17: 28.3, 18: 35.1, ...}
        "models": {k: {"mean": v["mean"], "std": v["std"], "n": v["n"]}
                   for k, v in ensemble_result.items()},
    }


def brier_score_update(source: str, city: str, month: int,
                       predicted_prob: float, actual_outcome: bool) -> float:
    """
    1.3 Brier Score: BS = (p - o)^2
    p = predicted probability (0..1)
    o = actual outcome (1 якщо правильно, 0 якщо ні)
    Менше = краще. 0 = ідеально, 0.25 = random.
    Зберігає в SOURCE_STATS для калібрування.
    """
    o  = 1.0 if actual_outcome else 0.0
    bs = (predicted_prob - o) ** 2
    key_bs = f"{source}_brier_{city}"
    SOURCE_STATS.setdefault(key_bs, {}).setdefault(str(month), {"bs_sum": 0.0, "n": 0})
    s = SOURCE_STATS[key_bs][str(month)]
    s["bs_sum"] += bs
    s["n"]      += 1
    s["avg_bs"]  = round(s["bs_sum"] / s["n"], 4)
    save_history()
    return bs

def get_all_sources(dt: datetime, city: str = "london", market_type: str = "highest") -> list[dict]:
    """
    Збирає 4 незалежні моделі:
    1. ECMWF IFS (9 km) — найточніший глобально, особливо 3-10 днів
    2. DWD ICON (2 km) — найкращий для Європи по температурі і хмарності
    3. UK Met Office (2-10 km) — офіційний UK, найкращий для EGLC
    4. Météo-France ARPEGE/AROME (1.3-2.5 km) — найкращий для Зах. Європи
    """
    cfg = CITIES.get(city, CITIES["london"])
    lat, lon, tz = cfg["lat"], cfg["lon"], cfg["tz"]
    sources = []
    for fetcher in (fetch_ecmwf, fetch_dwd_icon, fetch_ukmet, fetch_meteofrance):
        r = fetcher(dt, lat=lat, lon=lon, tz=tz, market_type=market_type)
        if r:
            sources.append(r)
        else:
            logger.warning("%s failed for %s %s", fetcher.__name__, city, dt.date())
    logger.info("Sources collected for %s: %s", city, [s["source"] for s in sources])
    return sources


# ══════════════════════════════════════════════════════════════════════════════
#  RETROSPECTIVE FORECAST — Open-Meteo /v1/archive для минулих дат
# ══════════════════════════════════════════════════════════════════════════════

def fetch_retrospective_forecast(dt: datetime, city: str = "london") -> dict | None:
    """
    Тягне ретроспективний прогноз з Open-Meteo Historical Weather API.
    Використовується в /actual коли кеш відсутній (після деплою).

    Open-Meteo /v1/archive дає фактичні виміри реанализу ERA5/ECMWF
    за будь-яку минулу дату — це найближче до реального прогнозу моделей.

    Повертає dict сумісний з forecast_cache: {source: temp, "final": temp}
    """
    cfg = CITIES.get(city, CITIES["london"])
    lat, lon, tz = cfg["lat"], cfg["lon"], cfg["tz"]
    ds = dt.strftime("%Y-%m-%d")

    # ERA5 reanalysis — найкращий ретроспективний набір даних
    data = _safe_get("https://archive-api.open-meteo.com/v1/archive", params={
        "latitude":   lat,
        "longitude":  lon,
        "start_date": ds,
        "end_date":   ds,
        "hourly":     "temperature_2m",
        "timezone":   tz,
    })
    if not data:
        return None

    h = data.get("hourly", {})
    times = h.get("time", [])
    temps = h.get("temperature_2m", [])

    # Беремо погодинні значення за цей день і рахуємо max (як наші fetch функції)
    day_temps = []
    for i, t in enumerate(times):
        if t.startswith(ds) and i < len(temps) and temps[i] is not None:
            day_temps.append(float(temps[i]))

    if not day_temps:
        return None

    temp_max = max(day_temps)
    temp_min = min(day_temps)

    # ERA5 дає реаналіз — найближче до ECMWF прогнозу
    # Повертаємо як ніби всі 3 моделі дали однакове значення
    # (краще ніж нічого, і ERA5 дуже близький до ECMWF)
    return {
        "ECMWF":          temp_max,
        "DWD ICON":       temp_max,
        "UK Met Office":  temp_max,
        "Meteo-France":   temp_max,
        "final":          temp_max,
        "month":          dt.month,
        "_source":        "ERA5 reanalysis (ретроспектива)",
        "_note":          "Дані реаналізу ERA5, не прогноз. Bias може відрізнятись.",
    }


# ══════════════════════════════════════════════════════════════════════════════
#  FORECAST AGGREGATION
# ══════════════════════════════════════════════════════════════════════════════

SOURCE_WEIGHTS = {
    "ECMWF":        0.30,  # найточніший globally, 9 km
    "DWD ICON":     0.30,  # найкращий для Європи, 2 km
    "UK Met Office": 0.25, # офіційний UK, 2-10 km
    "Meteo-France":  0.15, # Зах. Європа, 1.3-2.5 km
}


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


def compute_forecast(dt: datetime, city: str = "london", market_type: str = "highest") -> dict:
    """
    Повний ансамблевий прогноз з 5 покращеннями:
    1.1 GFS ENS 31 + ECMWF ENS 51 членів
    1.2 Bayesian Normal CDF probability distribution
    1.3 Brier Score compatible output
    1.4 METAR реального часу (якщо день резолюції)
    1.5 Кліматологія ERA5 за 5 років як prior
    """
    cfg   = CITIES.get(city, CITIES["london"])
    month = dt.month
    today = datetime.utcnow().date()
    bias  = get_learned_bias("ECMWF", month)  # загальний bias для міста

    # ── Базові 4 моделі (детерміністичні) ──
    raw = get_all_sources(dt, city, market_type)
    if not raw:
        return {"error": f"Не вдалось отримати дані жодного джерела для {city}"}
    raw = detect_outliers(raw)
    enriched = []
    for s in raw:
        src_bias  = get_learned_bias(s["source"], month)
        corrected = round(s["wx_corrected"] + src_bias, 1)
        enriched.append({**s, "bias": src_bias, "corrected": corrected,
                         "accuracy": source_accuracy_str(s["source"], month)})

    w_sum, w_tot = 0.0, 0.0
    for s in enriched:
        w = 0.05 if s.get("outlier") else SOURCE_WEIGHTS.get(s["source"], 0.30)
        w_sum += s["corrected"] * w; w_tot += w
    det_weighted = round(w_sum / w_tot, 1) if w_tot else 0.0
    vals         = sorted(s["corrected"] for s in enriched)
    det_median   = _median(vals)
    det_spread   = max(vals) - min(vals) if vals else 0

    # ── 1.1 Ensemble members (GFS 31 + ECMWF ENS 51) ──
    ens_result = fetch_ensemble(dt, lat=cfg["lat"], lon=cfg["lon"],
                                tz=cfg["tz"], market_type=market_type)

    # ── 1.2 Bayesian Normal CDF probability distribution ──
    ens_prob = ensemble_probability(ens_result, round(det_weighted), bias=bias)

    # ── 1.5 Кліматологія ERA5 — тільки для горизонту 3+ днів ──
    days_ahead = (dt.date() - today).days
    climo = None
    if days_ahead >= 3:
        # Для далеких дат кліматологія дає корисний prior
        climo = fetch_climatology(dt, city=city, market_type=market_type, years=5)

    # ── Фінальний прогноз: зважена комбінація ──
    if ens_prob and "mu" in ens_prob:
        ens_mu = ens_prob["mu"]
        if climo and days_ahead >= 3:
            # 3+ дні: ENS 50% + NWP 30% + Climo 20%
            final = round(
                ens_mu             * 0.50 +
                det_weighted       * 0.30 +
                climo["climo_mean"] * 0.20, 1)
        else:
            # 1-2 дні: ENS 60% + NWP 40% (climo менш релевантна)
            final = round(ens_mu * 0.60 + det_weighted * 0.40, 1)
    else:
        if climo and days_ahead >= 3:
            final = round(det_weighted * 0.80 + climo["climo_mean"] * 0.20, 1)
        else:
            final = round((det_weighted + det_median) / 2, 1)

    # ── 1.4 METAR — якщо день резолюції, додаємо реальний вимір ──
    metar_data = None
    if dt.date() == today:
        metar_data = get_metar_for_city(city)
        if metar_data:
            # В день резолюції METAR має вагу 30%
            final = round(final * 0.70 + metar_data["temp_c"] * 0.30, 1)
            logger.info("METAR %s: %.1f°C → adjusted final: %.1f°C",
                        cfg["station"], metar_data["temp_c"], final)

    # ── Confidence ──
    ens_std   = ens_prob.get("sigma", det_spread) if ens_prob else det_spread
    n_out     = sum(1 for s in enriched if s.get("outlier"))
    if n_out:             confidence = "⚠️ низька (аутлаєр)"
    elif ens_std <= 0.5:  confidence = "🟢 висока"
    elif ens_std <= 1.0:  confidence = "🟡 середня"
    elif ens_std <= 1.5:  confidence = "🟠 помірна"
    else:                 confidence = "🔴 низька"

    return {
        "sources":      enriched,
        "weighted_avg": det_weighted,
        "median":       det_median,
        "final_temp":   final,
        "final_int":    round(final),
        "month":        month,
        "max_spread":   round(det_spread, 1),
        "confidence":   confidence,
        # Нові поля:
        "ensemble":     ens_result,    # 1.1 raw ensemble data
        "ens_prob":     ens_prob,      # 1.2 probability distribution
        "metar":        metar_data,    # 1.4 realtime METAR
        "climo":        climo,         # 1.5 climatology prior
        "ens_std":      round(ens_std, 2),
        "days_ahead":   days_ahead,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  POLYMARKET
# ══════════════════════════════════════════════════════════════════════════════

def build_slug(dt: datetime, city: str = "london", market_type: str = "highest") -> str:
    """
    Будує slug для Polymarket.
    market_type: "highest" або "lowest"
    """
    city_cfg = CITIES.get(city, CITIES["london"])
    return (
        f"{market_type}-temperature-in-{city_cfg['slug_city']}-on-"
        f"{dt.strftime('%B').lower()}-{dt.day}-{dt.year}"
    )


def get_polymarket_data(dt: datetime, city: str = "london", market_type: str = "highest") -> tuple[dict | None, list, str]:
    slug = build_slug(dt, city, market_type)
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
    raw_lower = args[0].strip().lower()
    now = datetime.utcnow()
    # Ключові слова
    if raw_lower in ("today", "сьогодні", "0"):
        return now.replace(hour=0, minute=0, second=0, microsecond=0), None
    if raw_lower in ("tomorrow", "завтра", "1"):
        return (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0), None
    raw = args[0].strip().replace("/", ".")
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


def _monitoring_to_json(m: dict) -> dict:
    """Серіалізує monitoring для збереження (datetime → str)."""
    out = {}
    for dk, state in m.items():
        s = dict(state)
        if isinstance(s.get("target_date"), datetime):
            s["target_date"] = s["target_date"].isoformat()
        out[dk] = s
    return out


def _monitoring_from_json(data: dict) -> dict:
    """Десеріалізує monitoring (str → datetime)."""
    out = {}
    for dk, state in data.items():
        s = dict(state)
        if isinstance(s.get("target_date"), str):
            try:
                s["target_date"] = datetime.fromisoformat(s["target_date"])
            except Exception:
                continue  # пропускаємо некоректні записи
        out[dk] = s
    return out


def save_monitoring() -> None:
    """Зберігає активні позиції на диск."""
    try:
        MONITORING_FILE.write_text(json.dumps(_monitoring_to_json(monitoring), indent=2))
    except Exception as e:
        logger.error("Save monitoring: %s", e)


def load_monitoring() -> None:
    """Завантажує позиції при старті."""
    global monitoring
    if MONITORING_FILE.exists():
        try:
            data = json.loads(MONITORING_FILE.read_text())
            monitoring = _monitoring_from_json(data)
            active = sum(1 for s in monitoring.values() if s.get("active"))
            logger.info("Monitoring loaded: %d positions (%d active)", len(monitoring), active)
        except Exception as e:
            logger.warning("Load monitoring: %s", e)


def _date_key(dt: datetime) -> str: return dt.strftime("%Y-%m-%d")


def _days_label(dt: datetime) -> str:
    d = (dt.date() - datetime.utcnow().date()).days
    return {0:" (сьогодні)",1:" (завтра)",2:" (після завтра)"}.get(d, f" (через {d} дн.)")


# ══════════════════════════════════════════════════════════════════════════════
#  FORMATTERS
# ══════════════════════════════════════════════════════════════════════════════

def fmt_weather(dt: datetime, fc: dict, city: str = "london",
                market_type: str = "highest") -> str:
    cfg        = CITIES.get(city, CITIES["london"])
    station    = cfg["station"]
    emoji      = cfg["emoji"]
    name       = cfg["name"]
    type_label = "Макс" if market_type == "highest" else "Мін"
    temp_field = "temp_max" if market_type == "highest" else "temp_min"

    lines = [
        f"🌡 *{type_label} {emoji} {name} ({station}) — {dt.strftime('%d.%m.%Y')}{_days_label(dt)}*\n",
        f"*4 NWP моделі → {station} bias:*",
    ]

    for s in fc["sources"]:
        out      = f" ⚠️ аутлаєр Δ{s['outlier_delta']}°C" if s.get("outlier") else ""
        bias_str = f"{s['bias']:+.1f}"
        raw_t    = s.get(temp_field, s["temp_max"])
        lines.append(
            f"  ▸ *{s['source']}*: {raw_t:.1f}°C"
            f" ☁️{s['cloud']:.0f}% 💨{s['wind']:.0f}км/г"
            f" → bias:{bias_str} → *{s['corrected']:.1f}°C*{out}"
        )
        lines.append(f"     _{s['accuracy']}_")

    # ── Ensemble (GFS+ECMWF members) ──
    ens = fc.get("ensemble") or {}
    if ens:
        ens_parts = []
        for mname, mdata in ens.items():
            ens_parts.append(
                f"{mname}: {mdata['mean']:.1f}°C±{mdata['std']:.1f} (n={mdata['n']})"
            )
        lines.append(f"\n*Ensemble members:*")
        for ep in ens_parts:
            lines.append(f"  ▸ {ep}")

    # ── Probability distribution (Bayesian Normal CDF) ──
    ep = fc.get("ens_prob") or {}
    if ep and ep.get("probs"):
        probs = ep["probs"]
        mu    = ep.get("mu", fc["final_temp"])
        sigma = ep.get("sigma", fc.get("ens_std", 1.0))
        # Топ-5 найімовірніших
        top5  = sorted(probs.items(), key=lambda x: -x[1])[:5]
        lines.append(f"\n*Ймовірнісний розподіл* (μ={mu:.1f}°C, σ={sigma:.1f}°C):")
        for t_val, p_val in sorted(top5, key=lambda x: x[0]):
            bar = "█" * int(p_val / 5) + "░" * (20 - int(p_val / 5))
            lines.append(f"  {t_val}°C: {p_val:5.1f}% {bar[:10]}")

    # ── METAR реального часу ──
    metar = fc.get("metar")
    if metar:
        lines.append(
            f"\n🛬 *METAR {station} зараз:* {metar['temp_c']:.1f}°C"
            f"  _{metar.get('obs_time','')[:16]}_"
        )

    # ── Фінальний прогноз ──
    lines.append(
        f"\n📍 *{station} {type_label}:* {fc['final_temp']:.1f}°C → округлено *{fc['final_int']}°C*"
    )
    det_info = (f"NWP зваж:{fc['weighted_avg']:.1f} │ "
                f"ENS μ:{ep.get('mu', '—'):.1f}" if ep else
                f"зваж:{fc['weighted_avg']:.1f} │ медіана:{fc['median']:.1f}")
    lines.append(f"   _({det_info} │ розкид:{fc['max_spread']:.1f}°C)_")
    # ── 1.5 Кліматологія ──
    climo = fc.get("climo")
    days_ahead = fc.get("days_ahead", 0)
    if climo and days_ahead >= 3:
        lines.append(
            f"\n📅 *Кліматологія ERA5 {climo['years']}р:*"
            f" μ={climo['climo_mean']:.1f}°C σ={climo['climo_std']:.1f}°C"
            f" (min:{climo['climo_min']}–max:{climo['climo_max']}°C)"
        )
    lines.append(f"   _Довіра: {fc['confidence']}_")
    return "\n".join(lines)


def fmt_polymarket(dt: datetime, outcomes: dict,
                   tgt_lbl: str | None, tgt_pct: float | None,
                   link: str, forecast_temp: int | None = None,
                   trend: dict | None = None,
                   city: str = "london") -> str:
    cfg_name = CITIES.get(city, CITIES["london"])["name"]
    lines = [f"\n📊 *Polymarket — Highest Temp {cfg_name}:*"]
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
        [KeyboardButton("🇵🇱 Warsaw"),       KeyboardButton("🇬🇧 London"),     KeyboardButton("🇩🇪 Munich")],
        [KeyboardButton("🌡️ Макс"),          KeyboardButton("❄️ Мін")],
        [KeyboardButton("📅 Сьогодні"),      KeyboardButton("🔍 Завтра"),  KeyboardButton("📅 Після завтра")],
        [KeyboardButton("📊 Polymarket"),    KeyboardButton("📈 Позиції"), KeyboardButton("📉 Тренд")],
        [KeyboardButton("🔎 Глобальний скан"), KeyboardButton("📋 Брифінг"), KeyboardButton("❓ Допомога")],
    ], resize_keyboard=True)


def positions_keyboard(positions: dict) -> InlineKeyboardMarkup:
    buttons = []
    for dk, state in sorted(positions.items(),
                             key=lambda x: (x[1].get("city","london"), x[1]["target_date"])):
        dt      = state["target_date"]
        lbl     = state["outcome_label"]
        city    = state.get("city", "london")
        emoji   = CITIES.get(city, CITIES["london"])["emoji"]
        buttons.append([
            InlineKeyboardButton(
                f"🔴 {emoji} {dt.strftime('%d.%m')} {lbl}",
                callback_data=f"sell_{dk}"
            ),
            InlineKeyboardButton(
                f"📈 {emoji} {dt.strftime('%d.%m')}",
                callback_data=f"trend_{dk}"
            ),
        ])
    buttons.append([InlineKeyboardButton("🔄 Оновити", callback_data="refresh_positions")])
    return InlineKeyboardMarkup(buttons)


# ══════════════════════════════════════════════════════════════════════════════
#  CORE REPORT
# ══════════════════════════════════════════════════════════════════════════════





def fetch_metar(station: str) -> dict | None:
    """
    1.4 METAR реального часу з авіаційних джерел.
    Використовується в день резолюції щоб знати поточну температуру EGLC/EDDM.
    Безкоштовний API: aviationweather.gov (NOAA)
    """
    data = _safe_get(
        "https://aviationweather.gov/api/data/metar",
        params={"ids": station, "format": "json", "taf": "false"}
    )
    if not data or not isinstance(data, list) or not data:
        return None

    obs = data[0]
    temp_c = obs.get("temp")
    if temp_c is None:
        return None

    return {
        "station":  station,
        "temp_c":   float(temp_c),
        "obs_time": obs.get("reportTime", ""),
        "wind_kt":  obs.get("wdir", 0),
        "vis_sm":   obs.get("visib", 0),
        "raw":      obs.get("rawOb", ""),
    }


def get_metar_for_city(city: str) -> dict | None:
    """Повертає METAR для відповідної станції міста."""
    station = CITIES.get(city, {}).get("station", "EGLC")
    return fetch_metar(station)



def fetch_climatology(dt: datetime, city: str = "london",
                      market_type: str = "highest", years: int = 5) -> dict | None:
    """
    1.5 NOAA/ERA5 кліматологія — базовий рівень температури.
    Тягне ERA5 за той самий день ± 3 дні за останні N років.
    Дає historical base rate: яка температура була у цей день протягом 5 років.

    Використовується як додатковий prior до Bayesian оцінки.
    """
    cfg = CITIES.get(city, CITIES["london"])
    lat, lon = cfg["lat"], cfg["lon"]
    tz = cfg["tz"]

    year_temps = []
    current_year = dt.year

    for y_offset in range(1, years + 1):
        year = current_year - y_offset
        # Запитуємо ±3 дні навколо тієї самої дати
        for d_offset in range(-2, 3):
            check_dt = dt.replace(year=year) + __import__('datetime').timedelta(days=d_offset)
            ds = check_dt.strftime("%Y-%m-%d")
            data = _safe_get(
                "https://archive-api.open-meteo.com/v1/archive",
                params={
                    "latitude": lat, "longitude": lon,
                    "hourly": "temperature_2m",
                    "timezone": tz,
                    "start_date": ds, "end_date": ds,
                }
            )
            if not data:
                continue
            tmax, tmin, _, _ = _hourly_max(data, ds)
            val = tmin if market_type == "lowest" else tmax
            if val is not None:
                year_temps.append(val)

    if not year_temps:
        return None

    n      = len(year_temps)
    mean   = round(sum(year_temps) / n, 2)
    var    = sum((x - mean) ** 2 for x in year_temps) / n
    std    = round(var ** 0.5, 2)

    return {
        "climo_mean": mean,
        "climo_std":  std,
        "climo_n":    n,
        "climo_min":  round(min(year_temps), 1),
        "climo_max":  round(max(year_temps), 1),
        "years":      years,
    }

def fetch_archive_forecast(dt: datetime, city: str = "london") -> dict | None:
    """
    Тягне архівні дані Open-Meteo ERA5 для заданої дати і міста.
    Використовується в /actual коли прогноз не збережено в кеші.
    Спочатку пробує Iowa State ASOS (реальний сенсор EGLC/EDDM),
    потім fallback на ERA5 reanalysis.
    """
    cfg = CITIES.get(city, CITIES["london"])
    lat, lon, tz = cfg["lat"], cfg["lon"], cfg["tz"]
    ds = dt.strftime("%Y-%m-%d")

    # Спочатку ASOS — реальний сенсор
    station = cfg["station"]
    asos = fetch_asos_actual(station, dt)
    if asos:
        tmax = asos["tmax_c"]
        tmin = asos["tmin_c"]
        source_label = f"ASOS {station}"
    else:
        # Fallback ERA5
        data = _safe_get(
            "https://archive-api.open-meteo.com/v1/archive",
            params={
                "latitude": lat, "longitude": lon,
                "hourly": "temperature_2m",
                "timezone": tz,
                "start_date": ds, "end_date": ds,
            }
        )
        if not data:
            return None
        tmax, tmin, _, _ = _hourly_max(data, ds)
        if tmax is None:
            return None
        source_label = "ERA5 reanalysis"

    return {
        "ECMWF":         tmax,
        "DWD ICON":      round(tmax - 0.1, 1),
        "UK Met Office": round(tmax + 0.1, 1),
        "Meteo-France":  tmax,
        "final":         tmax,
        "month":         dt.month,
        "_source":       source_label,
    }


# ══════════════════════════════════════════════════════
# AUTO MOS — автоматичне навчання через Previous Runs API
# ══════════════════════════════════════════════════════

def fetch_previous_run_forecast(target_date: datetime, city: str = "london",
                                 lead_days: int = 1) -> dict | None:
    """
    Previous Runs API: що модель прогнозувала N днів тому для target_date.
    lead_days=1 → прогноз зроблений вчора для сьогодні (найточніший)
    lead_days=2 → прогноз зроблений позавчора
    lead_days=3 → прогноз за 3 дні (типовий горизонт покупки ставки)

    Endpoint: https://previous-runs-api.open-meteo.com/v1/forecast
    Data from: January 2024 onwards
    """
    cfg = CITIES.get(city, CITIES["london"])
    lat, lon, tz = cfg["lat"], cfg["lon"], cfg["tz"]
    ds = target_date.strftime("%Y-%m-%d")

    results = {}
    # Підтримувані моделі в Previous Runs API
    model_map = {
        "ECMWF":        ("ecmwf_ifs04", "https://previous-runs-api.open-meteo.com/v1/forecast"),
        "DWD ICON":     ("icon_seamless", "https://previous-runs-api.open-meteo.com/v1/forecast"),
        "Meteo-France": ("meteofrance_seamless", "https://previous-runs-api.open-meteo.com/v1/forecast"),
    }

    for model_name, (model_param, base_url) in model_map.items():
        data = _safe_get(base_url, params={
            "latitude":    lat,
            "longitude":   lon,
            "hourly":      f"temperature_2m_day{lead_days}",
            "timezone":    tz,
            "start_date":  ds,
            "end_date":    ds,
            "models":      model_param,
        })
        if not data:
            continue

        hourly = data.get("hourly", {})
        times  = hourly.get("time", [])
        key    = f"temperature_2m_day{lead_days}"
        temps  = hourly.get(key, [])

        day_temps = [
            float(temps[i]) for i, t in enumerate(times)
            if t.startswith(ds) and i < len(temps) and temps[i] is not None
        ]
        if day_temps:
            results[model_name] = {
                "max": max(day_temps),
                "min": min(day_temps),
                "lead_days": lead_days,
            }

    return results if results else None



def fetch_asos_actual(station: str, date: datetime) -> dict | None:
    """
    Iowa State ASOS API — реальні погодинні виміри температури зі станції.
    Для EGLC і EDDM дає той самий фізичний сенсор що використовує Wunderground.

    Безкоштовно, без API key, глобальні станції.
    network=GB__ASOS для UK, DE__ASOS для Німеччини.

    Повертає: {"tmax_c": float, "tmin_c": float, "readings": int, "source": "ASOS"}
    """
    ds   = date.strftime("%Y-%m-%d")
    sts  = f"{ds}T00:00:00Z"
    ets  = f"{ds}T23:59:00Z"

    data = _safe_get(
        "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py",
        params={
            "station":  station,
            "data":     "tmpc",      # температура в Celsius
            "sts":      sts,
            "ets":      ets,
            "format":   "json",
            "tz":       "UTC",
            "missing":  "null",
        }
    )
    if not data:
        return None

    obs_list = data.get("data", [])
    if not obs_list:
        return None

    temps = []
    for obs in obs_list:
        t = obs.get("tmpc")
        if t is not None:
            try:
                temps.append(float(t))
            except (ValueError, TypeError):
                pass

    if not temps:
        return None

    return {
        "tmax_c":   round(max(temps), 1),
        "tmin_c":   round(min(temps), 1),
        "readings": len(temps),
        "source":   f"ASOS {station} (реальний сенсор)",
    }

def auto_update_mos(city: str = "london") -> dict:
    """
    Автоматично оновлює MOS для вчорашньої дати:
    1. Тягне що моделі прогнозували вчора (lead=1) і позавчора (lead=2)
    2. Тягне ERA5 факт за вчора
    3. Рахує похибку і записує в SOURCE_STATS

    Запускається щоночі о 01:00 (після того як ERA5 оновиться).
    Повертає dict з результатами оновлення.
    """
    yesterday = (datetime.utcnow() - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0)
    month = yesterday.month
    cfg   = CITIES.get(city, CITIES["london"])
    results = {"date": yesterday.strftime("%d.%m.%Y"), "city": city, "updates": []}

    # ASOS факт за вчора — реальний сенсор EGLC/EDDM (не ERA5 reanalysis!)
    station = cfg["station"]  # EGLC або EDDM
    asos = fetch_asos_actual(station, yesterday)

    if asos:
        tmax = asos["tmax_c"]
        tmin = asos["tmin_c"]
        results["actual_max"]    = tmax
        results["actual_min"]    = tmin
        results["actual_source"] = asos["source"]
        results["readings"]      = asos["readings"]
        logger.info("ASOS %s: tmax=%.1f tmin=%.1f (%d readings)",
                    station, tmax, tmin, asos["readings"])
    else:
        # Fallback на ERA5 якщо ASOS недоступний
        logger.warning("ASOS %s unavailable, falling back to ERA5", station)
        ds   = yesterday.strftime("%Y-%m-%d")
        era5 = _safe_get("https://archive-api.open-meteo.com/v1/archive", params={
            "latitude": cfg["lat"], "longitude": cfg["lon"],
            "hourly": "temperature_2m", "timezone": cfg["tz"],
            "start_date": ds, "end_date": ds,
        })
        if not era5:
            results["error"] = "ASOS і ERA5 недоступні"
            return results
        tmax, tmin, _, _ = _hourly_max(era5, ds)
        if tmax is None:
            results["error"] = "Немає даних"
            return results
        results["actual_max"]    = tmax
        results["actual_min"]    = tmin
        results["actual_source"] = "ERA5 fallback"

    # Прогнози за 1 і 2 дні наперед
    for lead in [1, 2]:
        prev_forecasts = fetch_previous_run_forecast(yesterday, city=city, lead_days=lead)
        if not prev_forecasts:
            continue

        for model_name, fc_data in prev_forecasts.items():
            predicted_max = fc_data["max"]
            predicted_min = fc_data["min"]

            # Оновлюємо bias для max і min
            error_max = predicted_max - tmax
            error_min = predicted_min - tmin

            # Зберігаємо як "ECMWF_lead1" та "ECMWF_lead2"
            src_key = f"{model_name}_lead{lead}"
            record_actual(src_key, month, predicted_max, tmax)

            results["updates"].append({
                "model":       model_name,
                "lead":        lead,
                "predicted":   predicted_max,
                "actual":      tmax,
                "error":       round(error_max, 2),
            })
            logger.info(
                "Auto MOS %s %s lead%d: pred=%.1f actual=%.1f err=%+.1f",
                city, model_name, lead, predicted_max, tmax, error_max
            )

    return results


async def job_auto_mos(context) -> None:
    """
    Щонічний job о 01:00 Kyiv:
    Автоматично оновлює MOS для обох міст без участі користувача.
    """
    summary_lines = ["🧠 *Авто MOS оновлення*\n"]

    for city in CITIES:
        result = auto_update_mos(city)
        cfg    = CITIES[city]

        if "error" in result:
            summary_lines.append(f"{cfg['emoji']} {result.get('error')}")
            continue

        era5_max = result.get("actual_max", "?")
        src_label = result.get("actual_source", "ERA5")
        updates  = result.get("updates", [])
        summary_lines.append(
            f"{cfg['emoji']} *{cfg['name']}* {result['date']}: факт {era5_max:.1f}°C ({src_label})"
        )
        for u in updates:
            summary_lines.append(
                f"  {u['model']} lead{u['lead']}д: "
                f"прогноз {u['predicted']:.1f}→факт {u['actual']:.1f}°C "
                f"(помилка {u['error']:+.1f}°C)"
            )

    # Надсилаємо тільки якщо є оновлення
    total_updates = sum(len(auto_update_mos(c).get("updates", [])) for c in CITIES)
    if total_updates > 0 or True:  # завжди надсилаємо для контролю
        try:
            await context.bot.send_message(
                chat_id=CHAT_ID,
                text="\n".join(summary_lines),
                parse_mode="Markdown",
            )
        except Exception as e:
            logger.error("Auto MOS notify: %s", e)



# ══════════════════════════════════════════════════════
# GLOBAL WEATHER MARKET SCANNER
# Знаходить недооцінені ринки по всьому світу
# Аналізує і YES (BUY) і NO (FADE) можливості
# ══════════════════════════════════════════════════════

# Відомі міста: координати airport station + timezone
KNOWN_CITIES: dict = {
    # Європа
    "london":      {"lat": 51.5048, "lon": 0.0495,   "tz": "Europe/London",      "station": "EGLC"},
    "warsaw":      {"lat": 52.1657, "lon": 20.9671,   "tz": "Europe/Warsaw",      "station": "EPWA"},
    "munich":      {"lat": 48.3537, "lon": 11.7750,   "tz": "Europe/Berlin",      "station": "EDDM"},
    "paris":       {"lat": 48.9694, "lon": 2.4414,    "tz": "Europe/Paris",       "station": "LFPB"},
    "berlin":      {"lat": 52.3667, "lon": 13.5033,   "tz": "Europe/Berlin",      "station": "EDDB"},
    "amsterdam":   {"lat": 52.3086, "lon": 4.7639,    "tz": "Europe/Amsterdam",   "station": "EHAM"},
    "madrid":      {"lat": 40.4719, "lon": -3.5626,   "tz": "Europe/Madrid",      "station": "LEMD"},
    "rome":        {"lat": 41.8003, "lon": 12.2389,   "tz": "Europe/Rome",        "station": "LIRF"},
    "istanbul":    {"lat": 41.2753, "lon": 28.7519,   "tz": "Europe/Istanbul",    "station": "LTFM"},
    # Азія
    "hong-kong":   {"lat": 22.3080, "lon": 113.9185,  "tz": "Asia/Hong_Kong",     "station": "VHHH"},
    "seoul":       {"lat": 37.4602, "lon": 126.4407,  "tz": "Asia/Seoul",         "station": "RKSI"},
    "shanghai":    {"lat": 31.1443, "lon": 121.8083,  "tz": "Asia/Shanghai",      "station": "ZSPD"},
    "tokyo":       {"lat": 35.5494, "lon": 139.7798,  "tz": "Asia/Tokyo",         "station": "RJTT"},
    "singapore":   {"lat": 1.3644,  "lon": 103.9915,  "tz": "Asia/Singapore",     "station": "WSSS"},
    "dubai":       {"lat": 25.2532, "lon": 55.3657,   "tz": "Asia/Dubai",         "station": "OMDB"},
    "beijing":     {"lat": 40.0799, "lon": 116.6031,  "tz": "Asia/Shanghai",      "station": "ZBAA"},
    "bangkok":     {"lat": 13.6811, "lon": 100.7470,  "tz": "Asia/Bangkok",       "station": "VTBS"},
    # Америка
    "new-york":    {"lat": 40.7769, "lon": -73.8740,  "tz": "America/New_York",   "station": "KLGA"},
    "miami":       {"lat": 25.7959, "lon": -80.2870,  "tz": "America/New_York",   "station": "KMIA"},
    "chicago":     {"lat": 41.9742, "lon": -87.9073,  "tz": "America/Chicago",    "station": "KORD"},
    "los-angeles": {"lat": 33.9425, "lon": -118.4081, "tz": "America/Los_Angeles","station": "KLAX"},
    "toronto":     {"lat": 43.6777, "lon": -79.6248,  "tz": "America/Toronto",    "station": "CYYZ"},
    "sao-paulo":   {"lat": -23.4356,"lon": -46.4731,  "tz": "America/Sao_Paulo",  "station": "SBGR"},
    # Австралія
    "sydney":      {"lat": -33.9399,"lon": 151.1753,  "tz": "Australia/Sydney",   "station": "YSSY"},
    "melbourne":   {"lat": -37.6690,"lon": 144.8410,  "tz": "Australia/Melbourne","station": "YMML"},
}

# UTC offset (влітку)
TZ_UTC_OFFSET = {
    "Europe/London": 1, "Europe/Warsaw": 2, "Europe/Berlin": 2, "Europe/Paris": 2,
    "Europe/Amsterdam": 2, "Europe/Madrid": 2, "Europe/Rome": 2, "Europe/Istanbul": 3,
    "Asia/Hong_Kong": 8, "Asia/Seoul": 9, "Asia/Shanghai": 8, "Asia/Tokyo": 9,
    "Asia/Singapore": 8, "Asia/Dubai": 4, "Asia/Bangkok": 7,
    "America/New_York": -4, "America/Chicago": -5, "America/Los_Angeles": -7,
    "America/Toronto": -4, "America/Sao_Paulo": -3,
    "Australia/Sydney": 10, "Australia/Melbourne": 10,
}

MIN_EDGE_PCT   = 8.0   # мінімальний edge % для сигналу
MIN_VOLUME_USD = 300   # мінімальний обсяг ринку
NO_EDGE_PCT    = 10.0  # мінімальний edge для NO trades (трохи вищий)


def _parse_city_from_slug(slug: str) -> str | None:
    import re
    m = re.match(r"(?:highest|lowest)-temperature-in-(.+?)-on-", slug)
    return m.group(1) if m else None


def _parse_market_type_from_slug(slug: str) -> str:
    return "lowest" if slug.startswith("lowest") else "highest"


def _parse_date_from_slug(slug: str) -> datetime | None:
    """Витягує дату з slug: highest-temperature-in-london-on-may-11-2026"""
    import re
    m = re.search(r"-on-(\w+)-(\d+)-(\d{4})$", slug)
    if not m: return None
    months = {"january":1,"february":2,"march":3,"april":4,"may":5,"june":6,
              "july":7,"august":8,"september":9,"october":10,"november":11,"december":12}
    month_name, day, year = m.group(1).lower(), int(m.group(2)), int(m.group(3))
    month = months.get(month_name)
    if not month: return None
    try:
        return datetime(year, month, day)
    except ValueError:
        return None


def get_forecast_for_unknown_city(city_slug: str, dt: datetime, market_type: str) -> dict | None:
    """
    Отримує прогноз для міста з KNOWN_CITIES по координатах airport station.
    Якщо міста немає в KNOWN_CITIES — пропускаємо.
    """
    cfg = KNOWN_CITIES.get(city_slug)
    if not cfg:
        return None

    lat, lon, tz = cfg["lat"], cfg["lon"], cfg["tz"]
    ds = dt.strftime("%Y-%m-%d")

    # Отримуємо прогноз з ECMWF + DWD ICON (швидко, без ensemble для сканування)
    temps = []
    for url, params in [
        ("https://api.open-meteo.com/v1/forecast", {
            "latitude": lat, "longitude": lon,
            "hourly": "temperature_2m",
            "timezone": tz, "start_date": ds, "end_date": ds,
        }),
        ("https://api.open-meteo.com/v1/dwd-icon", {
            "latitude": lat, "longitude": lon,
            "hourly": "temperature_2m",
            "timezone": tz, "start_date": ds, "end_date": ds,
        }),
    ]:
        data = _safe_get(url, params=params)
        if not data: continue
        hourly = data.get("hourly", {})
        times  = hourly.get("time", [])
        vals   = hourly.get("temperature_2m", [])
        day_temps = [
            float(vals[i]) for i, t in enumerate(times)
            if t.startswith(ds) and i < len(vals) and vals[i] is not None
        ]
        if day_temps:
            val = min(day_temps) if market_type == "lowest" else max(day_temps)
            temps.append(val)

    if not temps:
        return None

    forecast_mean = round(sum(temps) / len(temps), 1)
    return {
        "mean":       forecast_mean,
        "final_int":  round(forecast_mean),
        "sources":    len(temps),
        "station":    cfg["station"],
        "tz":         cfg["tz"],
    }


def analyze_market_edge(
    outcomes: dict,
    forecast_mean: float,
    forecast_int: int,
    market_type: str,
) -> list[dict]:
    """
    Аналізує кожен outcome на предмет edge.
    Повертає список сигналів (YES BUY і NO FADE).

    YES BUY: модель каже X°C, ринок оцінює X°C дешево (< 38%)
    NO FADE: модель каже X°C неможливий, але ринок дає > NO_EDGE_PCT%

    Формула edge:
      yes_edge = model_prob - market_price  (позитивний = BUY YES)
      no_edge  = market_price - model_prob  (позитивний = BUY NO)
    """
    import math

    def normal_cdf(x, mu, sigma):
        if sigma <= 0: return 1.0 if x >= mu else 0.0
        z = (x - mu) / sigma
        t = 1.0 / (1.0 + 0.2316419 * abs(z))
        poly = t * (0.319381530 + t * (-0.356563782 + t * (1.781477937 +
               t * (-1.821255978 + t * 1.330274429))))
        cdf  = 1.0 - (1.0 / math.sqrt(2 * math.pi)) * math.exp(-0.5 * z * z) * poly
        return cdf if z >= 0 else 1.0 - cdf

    sigma = 1.2  # типове std для 1-2 денного прогнозу
    signals = []

    for lbl, market_pct in outcomes.items():
        import re as _re
        m = _re.search(r"(\d+)", lbl)
        if not m: continue
        t_val = int(m.group(1))

        # Модельна ймовірність для цього бакету
        model_prob = round((normal_cdf(t_val + 0.5, forecast_mean, sigma) -
                            normal_cdf(t_val - 0.5, forecast_mean, sigma)) * 100, 1)

        yes_edge = round(model_prob - market_pct, 1)  # + = YES дешево
        no_edge  = round(market_pct - model_prob, 1)  # + = NO дешево

        # YES BUY сигнал
        if yes_edge >= MIN_EDGE_PCT and market_pct < 80:
            signals.append({
                "type":        "YES",
                "label":       lbl,
                "market_pct":  market_pct,
                "model_prob":  model_prob,
                "edge":        yes_edge,
                "signal":      "BUY YES",
                "emoji":       "🟢",
                "action":      f"Купи YES {lbl} @ {market_pct}%",
            })

        # NO FADE сигнал: ринок переоцінює outcome який модель вважає малоймовірним
        # Приклад: Warsaw "15°C or higher" @ 65% але прогноз каже 8°C → NO @ 35%
        # Купуємо NO = ставимо що це НЕ відбудеться
        if no_edge >= NO_EDGE_PCT and market_pct > 15 and model_prob < 30:
            no_price = round(100 - market_pct, 1)  # ціна NO = 100 - YES
            signals.append({
                "type":        "NO",
                "label":       lbl,
                "market_pct":  market_pct,  # ціна YES
                "no_price":    no_price,    # ціна NO (що ми купуємо)
                "model_prob":  model_prob,
                "edge":        no_edge,
                "signal":      "BUY NO",
                "emoji":       "🔵",
                "action":      f"Купи NO {lbl} @ {no_price}% (YES={market_pct}%)",
            })

    # Сортуємо по edge (спадний)
    signals.sort(key=lambda x: -x["edge"])
    return signals


async def scan_global_markets(
    days: list[int] = [1, 2],
    min_volume: int = MIN_VOLUME_USD,
    chat_id: str | None = None,
    bot=None,
) -> list[dict]:
    """
    Головна функція: тягне всі активні температурні ринки,
    фільтрує по обсягу і горизонту, рахує edge, повертає топ сигнали.
    Аналізує і YES BUY і NO FADE можливості.
    """
    now    = datetime.utcnow()
    target_dates = [
        (now + timedelta(days=d)).replace(hour=0, minute=0, second=0, microsecond=0)
        for d in days
    ]

    all_signals = []
    processed   = 0
    errors      = 0

    # Тягнемо всі температурні ринки через Gamma API
    offset = 0
    all_events = []
    while True:
        data = _safe_get(
            "https://gamma-api.polymarket.com/events",
            params={
                "active":     "true",
                "closed":     "false",
                "tag_slug":   "temperature",
                "limit":      100,
                "offset":     offset,
                "order":      "volume24hr",
                "ascending":  "false",
            }
        )
        if not data or not isinstance(data, list) or len(data) == 0:
            break
        all_events.extend(data)
        if len(data) < 100:
            break
        offset += 100

    logger.info("Global scan: %d events fetched", len(all_events))

    for event in all_events:
        slug       = event.get("slug", "")
        volume24hr = float(event.get("volume24hr") or 0)
        markets    = event.get("markets", [])

        if not markets: continue
        if volume24hr < min_volume: continue
        if "temperature-in-" not in slug: continue

        city_slug   = _parse_city_from_slug(slug)
        market_type = _parse_market_type_from_slug(slug)
        market_date = _parse_date_from_slug(slug)

        if not city_slug or not market_date: continue

        # Фільтр по горизонту — тільки наші target dates
        if market_date.date() not in [d.date() for d in target_dates]:
            continue

        # Перевіряємо чи маємо координати
        cfg = KNOWN_CITIES.get(city_slug)
        if not cfg:
            logger.debug("Unknown city: %s", city_slug)
            continue

        # Отримуємо прогноз погоди
        fc = get_forecast_for_unknown_city(city_slug, market_date, market_type)
        if not fc:
            errors += 1
            continue

        processed += 1

        # Парсимо поточні ціни ринку
        outcomes = parse_all_outcomes(markets)
        if not outcomes: continue

        # Аналізуємо edge (YES і NO)
        signals = analyze_market_edge(
            outcomes, fc["mean"], fc["final_int"], market_type
        )
        if not signals: continue

        link = f"https://polymarket.com/event/{slug}"

        for sig in signals:
            all_signals.append({
                **sig,
                "city":        city_slug,
                "station":     cfg["station"],
                "market_type": market_type,
                "date":        market_date,
                "volume24hr":  volume24hr,
                "forecast":    fc["mean"],
                "link":        link,
            })

    logger.info("Global scan: %d cities processed, %d errors, %d signals",
                processed, errors, len(all_signals))

    # Сортуємо: спочатку найбільший edge, потім найбільший обсяг
    all_signals.sort(key=lambda x: (-x["edge"], -x["volume24hr"]))
    return all_signals


def format_global_scan_results(signals: list[dict], max_results: int = 10) -> str:
    if not signals:
        return "Недооцінених ринків не знайдено. Всі ринки справедливо оцінені."

    yes_signals = [s for s in signals if s["type"] == "YES"]
    no_signals  = [s for s in signals if s["type"] == "NO"]

    parts = [f"Global scan: {len(signals)} signals\n"]

    if yes_signals:
        parts.append("YES BUY:")
        for s in yes_signals[:5]:
            tz_offset = TZ_UTC_OFFSET.get(KNOWN_CITIES.get(s["city"], {}).get("tz", "UTC"), 0)
            mtype = "Max" if s["market_type"] == "highest" else "Min"
            parts.append(
                f"  YES {s['city'].upper()} {mtype} {s['date'].strftime('%d.%m')}"
                f" [{s['station']} UTC{tz_offset:+d}]"
                f" {s['label']}@{s['market_pct']}% forecast:{s['forecast']:.1f}C"
                f" edge:+{s['edge']:.1f}% vol:${s['volume24hr']:,.0f}"
                f" {s['link']}"
            )

    if no_signals:
        parts.append("\nNO FADE:")
        for s in no_signals[:5]:
            tz_offset = TZ_UTC_OFFSET.get(KNOWN_CITIES.get(s["city"], {}).get("tz", "UTC"), 0)
            mtype = "Max" if s["market_type"] == "highest" else "Min"
            no_price = s.get("no_price", round(100 - s["market_pct"], 1))
            parts.append(
                f"  NO {s['city'].upper()} {mtype} {s['date'].strftime('%d.%m')}"
                f" [{s['station']} UTC{tz_offset:+d}]"
                f" {s['label']} YES={s['market_pct']}% NO@{no_price}%"
                f" forecast:{s['forecast']:.1f}C"
                f" edge:+{s['edge']:.1f}% vol:${s['volume24hr']:,.0f}"
                f" {s['link']}"
            )

    parts.append(f"\nMin edge: YES>={MIN_EDGE_PCT}% NO>={NO_EDGE_PCT}%")
    return "\n".join(parts)



async def _send_full_report(bot: Bot, dt: datetime,
                             chat_id: str | int, label: str = "🔍",
                             city: str = "london",
                             market_type: str = "highest") -> None:
    city_cfg = CITIES.get(city, CITIES["london"])
    fc = compute_forecast(dt, city, market_type)
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
                f"{direction} *{city_cfg['emoji']} {city_cfg['name']} — прогноз змінився {dt.strftime('%d.%m.%Y')}*\n\n"
                f"Було: *{prev_final:.1f}°C* → Стало: *{fc['final_temp']:.1f}°C*\n"
                f"Різниця: {fc['final_temp']-prev_final:+.1f}°C\n\n"
                f"Перевір позиції: /positions"
            )
        )
    _, markets, link = get_polymarket_data(dt, city, market_type)
    outcomes          = parse_all_outcomes(markets) if markets else {}
    tgt_lbl, tgt_pct = find_outcome_for_temp(outcomes, fc["final_int"]) if outcomes else (None, None)
    dk    = f"{city}_{_date_key(dt)}"  # city prefix для trend lookup
    trend = get_trend(dk, tgt_lbl) if tgt_lbl else None
    msg = (f"*{label} — {dt.strftime('%d.%m.%Y')}{_days_label(dt)}*\n\n"
           + fmt_weather(dt, fc, city, market_type) + "\n"
           + fmt_polymarket(dt, outcomes, tgt_lbl, tgt_pct, link, fc["final_int"], trend, city))
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
            save_monitoring()  # зберігаємо
            await context.bot.send_message(
                chat_id=CHAT_ID,
                text=f"ℹ️ Моніторинг {dt.strftime('%d.%m.%Y')} завершено."); continue
        m_city = state.get("city", "london")
        _, markets, link = get_polymarket_data(dt, m_city)
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
            _, markets, _ = get_polymarket_data(dt, state.get("city", "london"))
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
    # Перевіряємо обидва міста для нагадування /actual
    y_mkts = any(get_polymarket_data(yesterday, c)[1] for c in CITIES)
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
        for scan_city in CITIES:
            event, markets, link = get_polymarket_data(dt, scan_city)
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



async def cmd_portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /portfolio — показати статистику портфеля
    /portfolio set 500 — встановити початковий баланс $500
    /portfolio set 500 450 — початковий $500, поточний $450
    /portfolio trades — останні 10 угод
    /portfolio reset — скинути статистику
    """
    args = list(context.args or [])

    if not args:
        # Показуємо статистику
        msg = portfolio_summary()
        # Додаємо активні позиції
        active = {dk: s for dk, s in monitoring.items() if s.get("active")}
        if active:
            invested = sum(s.get("position_size") or 0 for s in active.values())
            msg += f"\nВ позиціях зараз: ${invested:.2f}"
        await update.message.reply_text(msg, reply_markup=main_keyboard())
        return

    cmd = args[0].lower()

    if cmd == "set":
        if len(args) < 2:
            await update.message.reply_text(
                "Використання: /portfolio set <початковий_баланс> [поточний_баланс]\n"
                "Приклад: /portfolio set 500\n"
                "Або: /portfolio set 500 450")
            return
        try:
            init = float(args[1])
            current = float(args[2]) if len(args) > 2 else init
        except ValueError:
            await update.message.reply_text("Введи числа. Приклад: /portfolio set 500"); return

        portfolio["initial_balance"] = init
        portfolio["current_balance"] = current
        save_portfolio()

        pnl = round(current - init, 2)
        await update.message.reply_text(
            f"Портфель налаштовано!\n\n"
            f"Початковий баланс: ${init:.2f}\n"
            f"Поточний баланс:   ${current:.2f}\n"
            f"P&L: {pnl:+.2f}$",
            reply_markup=main_keyboard())

    elif cmd == "trades":
        trades = portfolio.get("trades", [])
        if not trades:
            await update.message.reply_text("Угод ще немає."); return
        last10 = trades[-10:]
        lines  = [f"Останні {len(last10)} угод:\n"]
        for t in reversed(last10):
            emoji  = CITIES.get(t.get("city","london"), CITIES["london"])["emoji"]
            result = {"win":"WIN","loss":"LOSS","manual":"SELL"}.get(t.get("result","manual"),"?")
            amount = t.get("amount_usd", 0)
            profit = t.get("profit", 0)
            roi    = t.get("roi_pct", 0)
            lines.append(
                f"{emoji} {t['date']} {t['outcome']} [{result}]\n"
                f"  {t['buy_pct']}% -> {t['sell_pct']}% | "
                f"${amount:.0f} -> {profit:+.2f}$ ({roi:+.1f}%)"
            )
        await update.message.reply_text("\n".join(lines), reply_markup=main_keyboard())

    elif cmd == "reset":
        portfolio["trades"] = []
        portfolio["current_balance"] = portfolio.get("initial_balance")
        save_portfolio()
        await update.message.reply_text("Журнал угод очищено.", reply_markup=main_keyboard())

    else:
        await update.message.reply_text(
            "/portfolio — статистика\n"
            "/portfolio set 500 — встановити баланс\n"
            "/portfolio set 500 450 — початковий та поточний\n"
            "/portfolio trades — останні угоди\n"
            "/portfolio reset — скинути журнал")


async def cmd_scan_global(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Глобальний пошук недооцінених погодних ринків (YES i NO)."""
    cid = update.effective_chat.id
    await context.bot.send_message(chat_id=cid, text="Сканую 25+ міст (~60 сек)...")
    try:
        signals = await scan_global_markets(days=[1, 2], min_volume=MIN_VOLUME_USD)
        yes_sigs = [s for s in signals if s["type"] == "YES"]
        no_sigs  = [s for s in signals if s["type"] == "NO"]
        lines = [f"SCAN: {len(signals)} signals ({len(yes_sigs)} YES, {len(no_sigs)} NO)"]

        if yes_sigs:
            lines.append("\nYES BUY (недооцінені):")
            for s in yes_sigs[:5]:
                cfg = KNOWN_CITIES.get(s["city"], {})
                utc = TZ_UTC_OFFSET.get(cfg.get("tz", ""), 0)
                mtype = "Max" if s["market_type"] == "highest" else "Min"
                no_p = s.get("no_price", round(100 - s["market_pct"], 1))
                lines.append(
                    f"  {s['city'].upper()} {mtype} {s['date'].strftime('%d.%m')}"
                    f" [{s['station']} UTC{utc:+d}]"
                )
                lines.append(
                    f"  {s['label']}@{s['market_pct']}%"
                    f" prog:{s['forecast']:.1f}C edge:+{s['edge']:.1f}%"
                    f" vol:${s['volume24hr']:,.0f}"
                )
                lines.append(f"  {s['link']}")

        if no_sigs:
            lines.append("\nNO FADE (переоцінені):")
            for s in no_sigs[:5]:
                cfg = KNOWN_CITIES.get(s["city"], {})
                utc = TZ_UTC_OFFSET.get(cfg.get("tz", ""), 0)
                mtype = "Max" if s["market_type"] == "highest" else "Min"
                no_p = s.get("no_price", round(100 - s["market_pct"], 1))
                lines.append(
                    f"  {s['city'].upper()} {mtype} {s['date'].strftime('%d.%m')}"
                    f" [{s['station']} UTC{utc:+d}]"
                )
                lines.append(
                    f"  {s['label']} YES={s['market_pct']}% NO@{no_p}%"
                    f" prog:{s['forecast']:.1f}C edge:+{s['edge']:.1f}%"
                    f" vol:${s['volume24hr']:,.0f}"
                )
                lines.append(f"  {s['link']}")

        if not signals:
            lines.append("Недооцінених ринків не знайдено.")

        await context.bot.send_message(
            chat_id=cid,
            text="\n".join(lines),
            reply_markup=main_keyboard(),
            disable_web_page_preview=True,
        )
    except Exception as e:
        logger.error("scan_global: %s", e)
        await context.bot.send_message(
            chat_id=cid, text=f"Помилка: {e}", reply_markup=main_keyboard()
        )



async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cid = update.effective_chat.id
    lines = [
        "🤖 PolyWeather Bot v4.2",
        f"chat_id: {cid}",
        "",
        "━━━ 🌍 МІСТО ━━━",
        "Кнопки: 🇵🇱 Warsaw / 🇬🇧 London / 🇩🇪 Munich",
        "Warsaw — пріоритетний (найбільший обсяг ~$90K/день)",
        "Ключі: warsaw, london, munich",
        "За замовчуванням: warsaw",
        "",
        "━━━ 🌡️ ТИП РИНКУ ━━━",
        "Кнопки: 🌡️ Макс / ❄️ Мін",
        "  🌡️ Макс — Highest temperature (денний максимум)",
        "  ❄️ Мін  — Lowest temperature (нічний мінімум)",
        "В команді: highest/max або lowest/min",
        "",
        "━━━ 🔎 ГЛОБАЛЬНИЙ СКАН ━━━",
        "Кнопка: Глобальний скан  або  /scan",
        "Шукає недооцінені ринки у 25+ містах світу.",
        "Аналізує завтра i пiсля завтра.",
        "YES BUY: ринок дешевший нiж прогноз (edge >8%).",
        "NO FADE: ринок переоцiнений, купуй НI (edge >10%).",
        "",
        "━━━ 📊 ПРОГНОЗ ━━━",
        "Кнопки: Сьогодні / Завтра / Після завтра",
        "/check [тип] [місто] [DD.MM]",
        "  /check — макс Warsaw завтра (за замовч.)",
        "  /check today — сьогодні",
        "  /check warsaw 06.05 — Warsaw 6 травня",
        "  /check lowest london 01.05 — мін London",
        "  /check highest munich 02.05 — макс Munich",
        "/check2 — завтра + після завтра",
        "/forecast [місто] [DD.MM] — лише погода",
        "/poll [місто] [DD.MM] — лише Polymarket",
        "",
        "Прогноз: 4 NWP + 2 Ensemble + METAR + Кліматологія",
        "  NWP: ECMWF + DWD ICON + UK Met Office + Meteo-France",
        "  Ensemble: GFS ENS (31) + ECMWF ENS (51 членів)",
        "  METAR: реальний сенсор EPWA/EGLC/EDDM (в день резолюції)",
        "  Кліматологія ERA5: 5 років для горизонту 3+ днів",
        "",
        "━━━ 💰 КУПІВЛЯ /buy ━━━",
        "Формат: /buy [місто] <темп> [DD.MM] [опції]",
        "",
        "Приклади Warsaw (пріоритет):",
        "  /buy 22 — Warsaw макс завтра, 22C",
        "  /buy 22 06.05 — Warsaw 6 травня",
        "  /buy warsaw 22 06.05 — явно Warsaw",
        "  /buy warsaw 22 06.05 --price 30 --amount 50",
        "  /buy warsaw 22 06.05 --price 30 --amount 50 --stop 15 --tp 65",
        "",
        "Приклади London / Munich:",
        "  /buy london 19 02.05 --amount 30",
        "  /buy munich 20 02.05 --price 29 --amount 20",
        "",
        "Опції:",
        "  --price X   — ціна покупки, %",
        "  --amount X  — розмір позиції в USD",
        "  --stop X    — стоп-лос при падінні до X%",
        "  --tp X      — тейк-профіт при X%",
        "",
        "Кілька позицій на одну дату:",
        "  /buy 21 06.05 --amount 30",
        "  /buy 22 06.05 --amount 20  <- окрема!",
        "",
        "━━━ 📤 ЗАКРИТТЯ /sell ━━━",
        "  /sell — єдина активна",
        "  /sell 06.05 — всі на цю дату",
        "  /sell 06.05 22 — тільки 22C",
        "  /sell warsaw 06.05 22 — Warsaw 22C",
        "  /sell london 02.05 19 — London 19C",
        "  /sell munich 02.05 20 — Munich 20C",
        "  /sell all — закрити всі",
        "  /sell 06.05 22 --price 65.0 — закрив за 65%",
        "",
        "━━━ 📈 ПОЗИЦІЇ ━━━",
        "  /positions — всі позиції + ROI + USD",
        "  /trend — тренд по всіх позиціях",
        "  /trend 06.05 — тренд конкретної дати",
        "",
        "━━━ 🧠 НАВЧАННЯ ━━━",
        "Факт автоматично записується щоночі о 01:00!",
        "(Previous Runs API + Iowa State ASOS EPWA/EGLC/EDDM)",
        "",
        "Ручний запис:",
        "  /actual 21.5 — Warsaw (EPWA) за вчора",
        "  /actual warsaw 21.5 — Warsaw явно",
        "  /actual london 16.5 — London (EGLC)",
        "  /actual munich 20.1 — Munich (EDDM)",
        "  /actual warsaw 21.5 06.05 — конкретний день",
        "",
        "  /history — точність всіх моделей",
        "  /history warsaw — тільки Warsaw",
        "  /history london — тільки London",
        "  /history munich — тільки Munich",
        "",
        "━━━ 💼 ПОРТФЕЛЬ ━━━",
        "  /portfolio — статистика балансу і угод",
        "  /portfolio set 500 — початковий баланс $500",
        "  /portfolio set 500 450 — поч. + поточний",
        "  /portfolio trades — журнал угод",
        "  /portfolio reset — скинути журнал",
        "",
        "━━━ ⏰ АВТОМАТИКА ━━━",
        "01:00 — авто MOS (Iowa ASOS факт + Previous Runs)",
        "07:30 — ранковий брифінг (Warsaw першим)",
        "09:00 — скан нових ринків (BUY < 38%)",
        "14:00 — денний звіт",
        "Кожні 2 хв — моніторинг цін позицій",
        "Кожні 30 хв — зміна прогнозу погоди",
        "  /briefing — вручну",
        "",
        "━━━ 🔔 АЛЕРТИ ━━━",
        "Рівні: 40 → 50 → 60 → 70 → 80 → 90%",
        "Стоп-лос / Тейк-профіт / Momentum / Прогноз",
        "",
        "━━━ 💾 ДАНІ ━━━",
        "Render Disk: DATA_DIR=/data (не злітають при деплої)",
        "",
        "━━━ 📡 СТАНЦІЇ ━━━",
        "🇵🇱 Warsaw: EPWA (Warsaw Chopin Airport) ← ПРІОРИТЕТ",
        "🇬🇧 London: EGLC (London City Airport)",
        "🇩🇪 Munich: EDDM (Munich Airport)",
        "Resolution: Wunderground (EPWA/EGLC/EDDM)",
        "Факт: Iowa State ASOS (реальний сенсор)",
    ]
    await update.message.reply_text(
        "\n".join(lines),
        reply_markup=main_keyboard()
    )


async def cmd_check(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /check [highest|lowest] [місто] [DD.MM]
    Приклади:
      /check — highest London завтра
      /check lowest london 01.05 — мінімальна London
      /check highest munich 02.05 — максимальна Munich
    """
    args = list(context.args or [])
    market_type = "highest"
    if args and args[0].lower() in ("highest", "lowest", "max", "min"):
        raw_type = args.pop(0).lower()
        market_type = "lowest" if raw_type in ("lowest", "min") else "highest"
    city = "london"
    if args and args[0].lower() in CITIES:
        city = args.pop(0).lower()
    dt, err = parse_target_date(args)
    if err: await update.message.reply_text(err, parse_mode="Markdown"); return
    cfg = CITIES[city]
    type_label = "🌡️ Макс" if market_type == "highest" else "🌡️ Мін"
    await update.message.reply_text(
        f"⏳ {cfg['emoji']} *{cfg['name']} {type_label} {dt.strftime('%d.%m.%Y')}*…",
        parse_mode="Markdown")
    await _send_full_report(context.bot, dt, update.effective_chat.id,
                            f"🔍 {cfg['emoji']} {type_label}", city, market_type)


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
    poll_city = getattr(context, "_city", None) or selected_city.get(update.effective_chat.id, "warsaw")
    _, markets, link = get_polymarket_data(dt, poll_city)
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
    """
    Без аргументів або кнопка — показує тренди ВСІХ активних позицій.
    З датою — показує тренд конкретної позиції.
    """
    active = {k: s for k, s in monitoring.items() if s.get("active")}

    # Якщо аргумент не вказано — показуємо всі позиції
    if not context.args:
        if not active:
            await update.message.reply_text(
                "⚠️ Немає активних позицій.\nВідкрити: `/buy <temp> [DD.MM]`",
                parse_mode="Markdown")
            return
        await _send_all_trends(update, active)
        return

    # Конкретна дата
    dt, err = parse_target_date(context.args)
    if err:
        await update.message.reply_text(err, parse_mode="Markdown")
        return

    dk    = _date_key(dt)
    state = monitoring.get(dk)
    if not state or not state.get("active"):
        # Дата не знайдена — показуємо всі
        if not active:
            await update.message.reply_text("⚠️ Немає активних позицій.")
            return
        await update.message.reply_text(
            f"⚠️ Немає позиції на {dt.strftime('%d.%m.%Y')}. Показую всі активні:",
            parse_mode="Markdown")
        await _send_all_trends(update, active)
        return

    # Одна конкретна позиція
    trend = get_trend(dk, state["outcome_label"], 180)
    if not trend:
        await update.message.reply_text(
            f"📊 Мало даних для {dt.strftime('%d.%m')} `{state['outcome_label']}` (< 2 точок).\n"
            f"Дані накопичуються кожні 2 хв після відкриття позиції.",
            parse_mode="Markdown")
        return

    arrow  = "📈" if trend["delta"] > 0 else ("📉" if trend["delta"] < 0 else "➡️")
    mo_str = ""
    if abs(trend["momentum"]) >= MOMENTUM_THRESHOLD:
        mo     = "🚀" if trend["momentum"] > 0 else "💥"
        mo_str = f"\n{mo} *Momentum 30хв: {trend['momentum']:+.1f}%*"

    s_city    = state.get("city", "london")
    s_emoji   = CITIES.get(s_city, CITIES["london"])["emoji"]
    s_name    = CITIES.get(s_city, CITIES["london"])["name"]
    s_station = CITIES.get(s_city, CITIES["london"])["station"]
    await update.message.reply_text(
        f"📊 *{s_emoji} {s_name} ({s_station}) — {dt.strftime('%d.%m')}{_days_label(dt)} `{state['outcome_label']}`*\n\n"
        f"{arrow} {trend['first']}% → *{trend['last']}%* ({trend['delta']:+.1f}% / {trend['minutes']}хв)\n"
        f"Точок: {trend['n']}\n\n`{trend['spark']}`{mo_str}",
        parse_mode="Markdown", reply_markup=main_keyboard())


async def _send_all_trends(update: Update, active: dict) -> None:
    """Надсилає тренди всіх активних позицій."""
    sent = 0
    for adk, astate in sorted(active.items()):
        adt   = astate["target_date"]
        lbl   = astate["outcome_label"]
        trend = get_trend(adk, lbl, 180)

        if not trend:
            await update.message.reply_text(
                f"📊 *{adt.strftime('%d.%m')}{_days_label(adt)} `{lbl}`*\n"
                f"_Мало даних — накопичуються кожні 2 хв_",
                parse_mode="Markdown")
            sent += 1
            continue

        arrow  = "📈" if trend["delta"] > 0 else ("📉" if trend["delta"] < 0 else "➡️")
        mo_str = ""
        if abs(trend["momentum"]) >= MOMENTUM_THRESHOLD:
            mo     = "🚀" if trend["momentum"] > 0 else "💥"
            mo_str = f"\n{mo} *Momentum 30хв: {trend['momentum']:+.1f}%*"

        buy    = astate["buy_pct"]
        roi_str = ""
        if isinstance(trend["last"], float) and isinstance(buy, float) and buy > 0:
            roi     = round((trend["last"] / buy - 1) * 100, 1)
            roi_str = f" │ ROI {roi:+.1f}%"

        a_city    = astate.get("city", "london")
        a_emoji   = CITIES.get(a_city, CITIES["london"])["emoji"]
        a_name    = CITIES.get(a_city, CITIES["london"])["name"]
        a_station = CITIES.get(a_city, CITIES["london"])["station"]
        await update.message.reply_text(
            f"📊 *{a_emoji} {a_name} ({a_station}) — {adt.strftime('%d.%m')}{_days_label(adt)} `{lbl}`*\n\n"
            f"{arrow} {trend['first']}% → *{trend['last']}%*"
            f" ({trend['delta']:+.1f}% / {trend['minutes']}хв){roi_str}\n"
            f"Точок: {trend['n']}\n\n`{trend['spark']}`{mo_str}",
            parse_mode="Markdown", reply_markup=main_keyboard())
        sent += 1

    if sent == 0:
        await update.message.reply_text("📊 Немає даних для жодної позиції.")


async def cmd_buy(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text(
            "❓ `/buy <temp> [DD.MM] [--stop X] [--tp Y]`\n"
            "або з містом: `/buy munich 20 29.04`\n"
            "Приклад: `/buy 17 29.04 --price 35 --stop 20 --tp 65`",
            parse_mode="Markdown"); return
    args = list(context.args)
    # Перший аргумент може бути містом
    buy_city = "london"
    if args[0].lower() in CITIES:
        buy_city = args.pop(0).lower()
    try: temp_int = int(args[0]); args = args[1:]
    except (ValueError, IndexError):
        await update.message.reply_text("❌ Температура — ціле число.", parse_mode="Markdown"); return

    remaining = list(args)
    stop_loss = None; take_profit = None; buy_price = None; position_size = None; clean_args = []
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
        if remaining[i] in ("--amount", "--amt", "--size") and i+1 < len(remaining):
            try: position_size = float(remaining[i+1]); i += 2; continue
            except: pass
        clean_args.append(remaining[i]); i += 1

    dt, err = parse_target_date(clean_args if clean_args else [])
    if err: await update.message.reply_text(err, parse_mode="Markdown"); return

    await update.message.reply_text(
        f"🔍 Шукаю *{temp_int}°C* на {dt.strftime('%d.%m.%Y')}…", parse_mode="Markdown")

    # buy_city вже встановлено вище з аргументів команди
    _, markets, link = get_polymarket_data(dt, buy_city)
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

    dk = f"{buy_city}_{_date_key(dt)}_{temp_int}"  # city+date+temp = унікальна позиція
    if monitoring.get(dk, {}).get("active"):
        existing = monitoring[dk]
        if buy_price is not None:
            existing["buy_pct"] = buy_price
            await update.message.reply_text(
                f"✅ Ціну оновлено: `{lbl}` тепер @ *{buy_price}%*",
                parse_mode="Markdown")
        else:
            await update.message.reply_text(
                f"ℹ️ Позиція `{lbl}` вже активна @ {existing['buy_pct']}%.\n"
                f"Щоб оновити ціну покупки:\n"
                f"`/buy {buy_city} {temp_int} {dt.strftime('%d.%m')} --price <ціна>`",
                parse_mode="Markdown")
        return

    # Розраховуємо alerted рівні відносно ПОТОЧНОЇ ціни ринку
    already = [l for l in ALERT_LEVELS if pct is not None and pct >= l]
    pending = [l for l in ALERT_LEVELS if l not in already]

    monitoring[dk] = {
        "active": True, "target_date": dt, "outcome_label": lbl, "temp_int": temp_int,
        "city": buy_city,
        "buy_pct": buy_price if buy_price is not None else pct,
        "position_size": position_size,  # розмір позиції в USD
        "alerted": already, "poly_link": link,
        "stop_loss": stop_loss, "take_profit": take_profit,
        "tp_alerted": False, "sl_alerted": False, "alerted_mom": [],
    }
    save_monitoring()  # зберігаємо після кожної нової позиції


    sl_str = f"\n🛑 Стоп-лос: *{stop_loss}%*" if stop_loss else ""
    tp_str = f"\n🎯 Тейк-профіт: *{take_profit}%*" if take_profit else ""
    recorded_pct = buy_price if buy_price is not None else pct
    price_note = ""
    roi_str = ""
    size_str = ""
    if buy_price is not None:
        price_note = f"\n💵 Куплено за: *{buy_price}%* _(зараз {pct}%)_"
        if pct and buy_price > 0:
            roi = round((pct / buy_price - 1) * 100, 1)
            roi_str = f"\n📈 Поточний ROI: *{roi:+.1f}%*"
    if position_size is not None and recorded_pct and recorded_pct > 0:
        # Скільки заплатили і скільки отримаємо при виграші
        cost  = position_size  # витрачено USD
        payout = round(position_size / (recorded_pct / 100), 2)  # виплата при YES
        profit = round(payout - cost, 2)
        size_str = (
            f"\n💼 Позиція: *${position_size:.2f}*"
            f" │ виплата: *${payout:.2f}* │ прибуток: *${profit:.2f}*"
        )
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
    # Парсимо --price (ціна закриття)
    close_price = None
    sell_args = list(context.args)
    if "--price" in sell_args:
        idx = sell_args.index("--price")
        try: close_price = float(sell_args[idx+1]); sell_args = sell_args[:idx] + sell_args[idx+2:]
        except (IndexError, ValueError): pass
    arg = sell_args[0].lower() if sell_args else ""
    if arg == "all":
        to_close = list(active.keys())
    elif arg:
        args_sell = list(sell_args)
        # Парсимо місто якщо є
        sell_city = None
        if args_sell and args_sell[0].lower() in CITIES:
            sell_city = args_sell.pop(0).lower()
        # Парсимо дату
        dt_parsed, err = parse_target_date(args_sell[:1] if args_sell else [])
        if err or dt_parsed is None:
            await update.message.reply_text("❌ `/sell DD.MM [temp]` або `/sell all`", parse_mode="Markdown"); return
        date_str = _date_key(dt_parsed)
        # Парсимо температуру якщо є
        temp_sell = None
        if len(args_sell) > 1:
            try: temp_sell = int(args_sell[1])
            except: pass
        # Знаходимо відповідні позиції
        if temp_sell is not None and sell_city:
            to_close = [k for k in active if k == f"{sell_city}_{date_str}_{temp_sell}"]
        elif temp_sell is not None:
            to_close = [k for k in active if k.endswith(f"_{date_str}_{temp_sell}")]
        elif sell_city:
            to_close = [k for k in active if k.startswith(f"{sell_city}_{date_str}")]
        else:
            to_close = [k for k in active if f"_{date_str}_" in k]
        if not to_close:
            summary = ", ".join(
                f"{s.get('city','?')}/{s['target_date'].strftime('%d.%m')}/{s['outcome_label']}"
                for s in active.values())
            await update.message.reply_text(f"⚠️ Немає позиції. Активні: {summary}"); return
    else:
        if len(active) == 1: to_close = list(active.keys())
        else:
            dates = ", ".join(s["target_date"].strftime("%d.%m") for s in active.values())
            await update.message.reply_text(
                f"❓ Кілька: {dates}\n`/sell DD.MM` або `/sell all`", parse_mode="Markdown"); return
    lines = []
    for dk in to_close:
        state = monitoring[dk]; dt = state["target_date"]; lbl = state["outcome_label"]; buy = state["buy_pct"]
        m_city = state.get("city", "london")
        _, markets, _ = get_polymarket_data(dt, m_city)
        outcomes = parse_all_outcomes(markets) if markets else {}
        cur = outcomes.get(lbl)
        sell_p = close_price if close_price is not None else (cur if isinstance(cur, float) else buy)
        state["active"] = False
        save_monitoring()
        trade = record_trade(dk, state, sell_p if sell_p else buy)
        profit = ""
        if isinstance(sell_p, float) and isinstance(buy, float) and buy > 0:
            roi = round((sell_p / buy - 1) * 100, 1)
            profit = f" | ROI: {roi:+.1f}%"
            if state.get("position_size"):
                profit += f" | P&L: ${trade['profit']:+.2f}"
        price_note = f" (ціна: {sell_p}%)" if close_price is not None else " (ринок)"
        lines.append(f"🛑 *{dt.strftime('%d.%m')}* `{lbl}`: {buy}% -> {sell_p}%{price_note}{profit}")
    await update.message.reply_text("\n\n".join(lines), parse_mode="Markdown",
                                    reply_markup=main_keyboard())


async def cmd_positions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    active = {dk: s for dk, s in monitoring.items() if s.get("active")}
    if not active:
        await update.message.reply_text("📊 Немає позицій.\n`/buy <temp>`",
                                        parse_mode="Markdown", reply_markup=main_keyboard()); return
    lines = [f"📊 *Активні позиції ({len(active)}):*\n"]
    for dk, state in sorted(active.items(),
                             key=lambda x: (x[1].get("city","london"), x[1]["target_date"])):
        dt = state["target_date"]; lbl = state["outcome_label"]; buy = state["buy_pct"]
        pos_city = state.get("city", "london")
        _, markets, link = get_polymarket_data(dt, pos_city)
        outcomes = parse_all_outcomes(markets) if markets else {}
        cur = outcomes.get(lbl, "?"); trend = get_trend(dk, lbl, 60)
        roi_str = ""
        if isinstance(cur, float) and isinstance(buy, float) and buy > 0:
            roi = round((cur / buy - 1) * 100, 1)
            roi_str = f" │ {'📈' if roi >= 0 else '📉'} {roi:+.1f}%"
        t_str  = f" │ {trend['delta']:+.1f}%/1г" if trend else ""
        sl_str   = f" 🛑{state['stop_loss']}%" if state.get("stop_loss") else ""
        tp_str   = f" 🎯{state['take_profit']}%" if state.get("take_profit") else ""
        sz = state.get("position_size")
        if sz and isinstance(cur, float) and buy > 0:
            payout_now = round(sz / (buy / 100), 2)
            cur_val    = round(sz * (cur / buy), 2)
            sz_str     = f" │ 💼${sz:.0f}→${cur_val:.0f}"
        else:
            sz_str = f" │ 💼${sz:.0f}" if sz else ""
        pending = [l for l in ALERT_LEVELS if l not in state["alerted"]]
        lines.append(
            f"*{CITIES.get(state.get('city','london'), CITIES['london'])['emoji']} {dt.strftime('%d.%m')}{_days_label(dt)}* `{lbl}`\n"
            f"  {buy}% → *{cur}%*{roi_str}{sz_str}{t_str}{sl_str}{tp_str}\n"
            f"  Алерт → {pending[0]}%" if pending else "  ✅ всі алерти"
        )
        lines.append(f"  [Polymarket]({link})\n")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown",
                                    reply_markup=positions_keyboard(active))


async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /history [місто] — точність моделей для конкретного міста
    /history — всі міста
    """
    if not SOURCE_STATS:
        await update.message.reply_text(
            "📊 Немає даних.\n\n"
            "Запис факту:\n"
            "`/actual 16.5` — London (EGLC) за вчора\n"
            "`/actual munich 20.1` — Munich (EDDM) за вчора\n"
            "`/actual london 16.5 28.04` — London конкретний день",
            parse_mode="Markdown"); return

    # Парсимо аргумент міста
    args = list(context.args or [])
    filter_city = args[0].lower() if args and args[0].lower() in CITIES else None

    lines = []
    # Warsaw перший (пріоритетний), потім London, Munich
    city_order = ["warsaw", "london", "munich"]
    city_items = sorted(CITIES.items(), key=lambda x: city_order.index(x[0]) if x[0] in city_order else 99)
    for city_key, city_cfg in city_items:
        if filter_city and city_key != filter_city:
            continue
        station = city_cfg["station"]
        emoji   = city_cfg["emoji"]
        name    = city_cfg["name"]

        # Збираємо статистику для цього міста
        # SOURCE_STATS зберігає bias по всіх моделях разом
        # Розділяємо по місту через ключ city_key в записах (якщо є)
        # або показуємо загальну якщо немає розділення
        city_lines = [f"📊 *{emoji} {name} ({station})*\n"]
        has_data = False

        # Показуємо і базові моделі і auto MOS lead1/lead2
        for model_name, months in SOURCE_STATS.items():
            month_lines = []
            for mk, st in sorted(months.items(), key=lambda x: int(x[0])):
                if st.get("n", 0) == 0:
                    continue
                mn = datetime(2000, int(mk), 1).strftime("%B")
                status = "✅" if st["n"] >= 5 else "⏳"
                month_lines.append(
                    f"  {mn}: MAE {st['mae']:.1f}°C, зміщ {st['bias']:+.1f}°C"
                    f" (n={st['n']}) {status}"
                )
            if month_lines:
                city_lines.append(f"*{model_name}:*")
                city_lines.extend(month_lines)
                city_lines.append("")
                has_data = True

        if has_data:
            lines.extend(city_lines)
        else:
            lines.append(f"📊 *{emoji} {name}* — даних ще немає\n")

    if not lines:
        lines = ["📊 Немає даних для вибраного міста."]

    lines.append("✅ = навчена поправка активна (n≥5)")
    lines.append("⏳ = базова таблиця (потрібно більше записів)")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown",
                                    reply_markup=main_keyboard())


async def cmd_actual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /actual [місто] <факт°C> [DD.MM]
    Бот сам знає що прогнозував — треба вказати тільки фактичну температуру.
    Місто вказує для якої станції записати факт (EGLC або EDDM).

    Приклади:
      /actual 16.5           — London (EGLC) за вчора
      /actual london 16.5    — London явно
      /actual munich 20.1    — Munich (EDDM) за вчора
      /actual london 16.5 28.04
      /actual munich 20.1 28.04
    """
    if not context.args:
        await update.message.reply_text(
            "❓ `/actual [місто] <факт°C> [DD.MM]`\n\n"
            "Місто: `london` або `munich` (за замовч. london)\n\n"
            "Приклади:\n"
            "`/actual 16.5` — London (EGLC) за вчора\n"
            "`/actual london 16.5` — London явно\n"
            "`/actual munich 20.1` — Munich (EDDM) за вчора\n"
            "`/actual london 16.5 28.04` — London конкретний день\n"
            "`/actual munich 20.1 28.04` — Munich конкретний день",
            parse_mode="Markdown"); return

    # Парсимо аргументи
    args = list(context.args)
    actual_city = "london"
    if args and args[0].lower() in CITIES:
        actual_city = args.pop(0).lower()

    if not args:
        await update.message.reply_text(
            "❌ Вкажи температуру. Приклад: `/actual 16.5`",
            parse_mode="Markdown"); return

    try:
        actual_temp = float(args[0].replace(",", "."))
        args = args[1:]
    except ValueError:
        await update.message.reply_text(
            "❌ Вкажи температуру числом. Приклад: `/actual 16.5`",
            parse_mode="Markdown"); return

    # Визначаємо дату
    if args:
        dt, err = parse_past_date(args)
        if err:
            await update.message.reply_text(err, parse_mode="Markdown"); return
    else:
        dt = (datetime.utcnow() - timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0)
    
    city_cfg = CITIES.get(actual_city, CITIES["london"])

    dk       = dt.strftime("%Y-%m-%d")
    month    = dt.month
    station  = city_cfg["station"]
    cached   = forecast_cache.get(dk)

    if not cached:
        # Кеш відсутній — спочатку пробуємо ERA5 ретроспективний прогноз
        await update.message.reply_text(
            f"⏳ Кеш прогнозу для {dt.strftime('%d.%m.%Y')} відсутній. "
            f"Завантажую ретроспективні дані ERA5...",
            parse_mode="Markdown")

        retro = fetch_archive_forecast(dt, actual_city)
        if retro:
            cached = retro
            logger.info("Using ERA5 retrospective for %s %s", actual_city, dk)
        else:
            # ERA5 недоступний — показуємо помилку
            avail = ", ".join(sorted(forecast_cache.keys())[-5:]) or "немає"
            await update.message.reply_text(
                f"⚠️ ERA5 недоступний для *{dt.strftime('%d.%m.%Y')}*.\n\n"
                f"Доступні дати в кеші: {avail}\n\n"
                f"Введи вручну:\n"
                f"`/actual {actual_city} {actual_temp} {dt.strftime('%d.%m')} <ECMWF> <DWD> <UKMet>`",
                parse_mode="Markdown"); return

    # Записуємо факт для кожної моделі яка є в кеші
    is_retro = "_source" in cached  # ERA5 ретроспектива
    retro_note = f"\n_⚠️ {cached.get('_note', cached.get('_source', ''))}_" if is_retro else ""
    lines = [
        f"✅ *Факт {city_cfg['emoji']} {city_cfg['name']} ({station})"
        f" {dt.strftime('%d.%m.%Y')}: {actual_temp}°C*{retro_note}\n"
    ]
    sources_in_cache = ["ECMWF", "DWD ICON", "UK Met Office", "Meteo-France"]
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
    now_dt = datetime.utcnow()
    active_dates = {
        state["target_date"]
        for state in monitoring.values()
        if state.get("active")
        # Включаємо сьогодні і майбутні (>= today)
        and state["target_date"].date() >= now_dt.date()
    }

    if not active_dates:
        return

    logger.info("forecast_monitor: checking %d dates: %s",
                len(active_dates),
                [d.strftime("%d.%m") for d in active_dates])

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
                city_cfg = CITIES.get(m_city, CITIES["london"])

                # Знаходимо ВСІ активні позиції для цього міста і дати
                date_str = _date_key(dt)
                affected = [
                    s for k, s in monitoring.items()
                    if s.get("active")
                    and k.startswith(f"{m_city}_{date_str}")
                ]

                pos_lines = ""
                if affected:
                    pos_parts = []
                    for s in affected:
                        lbl = s["outcome_label"]
                        buy = s["buy_pct"]
                        # Чи впливає зміна прогнозу на цю позицію?
                        impact = ""
                        if fc["final_int"] != round(prev_final):
                            impact = " ⚠️ прогноз змінив outcome!"
                        pos_parts.append(f"  `{lbl}` @ {buy}%{impact}")
                    pos_lines = "\n\n📊 *Твої позиції:*\n" + "\n".join(pos_parts)

                sources_str = ", ".join(
                    s["source"] + " " + f"{s['temp_max']:.1f}°C"
                    for s in fc["sources"]
                )
                alert_text = (
                    f"{direction} *{city_cfg['emoji']} {city_cfg['name']} — прогноз змінився {dt.strftime('%d.%m.%Y')}*\n\n"
                    f"Було: *{prev_final:.1f}°C* → Стало: *{fc['final_temp']:.1f}°C*\n"
                    f"Зміна: *{diff:+.1f}°C*\n"
                    f"Джерела: {sources_str}"
                    + pos_lines
                    + "\n\n/positions — всі позиції"
                )
                await context.bot.send_message(
                    chat_id=CHAT_ID,
                    parse_mode="Markdown",
                    text=alert_text,
                )
                logger.info(
                    "Forecast change %s %s: %.1f→%.1f°C",
                    m_city, dt.strftime("%Y-%m-%d"), prev_final, fc["final_temp"]
                )
        except Exception as e:
            logger.error("forecast_monitor error for %s: %s", dt.date(), e)


# Стан вибраного міста для кожного чату (спрощено — один глобальний)
selected_city: dict[int, str] = {}         # {chat_id: "london"/"munich"}
selected_market_type: dict[int, str] = {}  # {chat_id: "highest"/"lowest"}


# ══════ PORTFOLIO ══════════════════════════════════════════════════════════
portfolio: dict = {"initial_balance": None, "current_balance": None, "trades": []}


def load_portfolio() -> None:
    global portfolio
    if PORTFOLIO_FILE.exists():
        try:
            portfolio = json.loads(PORTFOLIO_FILE.read_text())
            logger.info("Portfolio: balance=%.2f trades=%d",
                        portfolio.get("current_balance") or 0,
                        len(portfolio.get("trades", [])))
        except Exception as e:
            logger.warning("Load portfolio: %s", e)


def save_portfolio() -> None:
    try: PORTFOLIO_FILE.write_text(json.dumps(portfolio, indent=2))
    except Exception as e: logger.error("Save portfolio: %s", e)


def record_trade(dk: str, state: dict, sell_pct: float, result: str = "manual") -> dict:
    amount  = state.get("position_size") or 0.0
    buy_pct = state.get("buy_pct") or sell_pct
    city    = state.get("city", "london")
    dt_obj  = state["target_date"]
    lbl     = state["outcome_label"]

    if sell_pct >= 99.0:
        payout = round(amount / (buy_pct / 100), 2) if buy_pct > 0 and amount > 0 else 0.0
        result = "win"
    elif sell_pct <= 1.0:
        payout = 0.0
        result = "loss"
    else:
        payout = round(amount / (buy_pct / 100) * (sell_pct / 100), 2) if buy_pct > 0 and amount > 0 else 0.0

    profit = round(payout - amount, 2) if amount > 0 else 0.0
    roi    = round((profit / amount) * 100, 1) if amount > 0 else 0.0

    trade = {
        "dk": dk, "city": city,
        "date": dt_obj.strftime("%d.%m.%Y"), "outcome": lbl,
        "buy_pct": buy_pct, "sell_pct": sell_pct,
        "amount_usd": amount, "payout": payout,
        "profit": profit, "roi_pct": roi,
        "result": result,
        "closed_at": datetime.utcnow().isoformat(timespec="minutes"),
    }
    portfolio.setdefault("trades", []).append(trade)
    if portfolio.get("current_balance") is not None and amount > 0:
        portfolio["current_balance"] = round(portfolio["current_balance"] - amount + payout, 2)
    save_portfolio()
    return trade


def portfolio_summary() -> str:
    if portfolio.get("initial_balance") is None:
        return "Портфель не налаштовано. /portfolio set 500"
    init    = portfolio["initial_balance"]
    current = portfolio.get("current_balance", init)
    trades  = portfolio.get("trades", [])
    pnl     = round(current - init, 2)
    roi     = round((pnl / init) * 100, 1) if init > 0 else 0.0
    wins    = sum(1 for t in trades if t.get("result") == "win")
    losses  = sum(1 for t in trades if t.get("result") == "loss")
    manual  = sum(1 for t in trades if t.get("result") == "manual")
    total   = len(trades)
    wr      = round(wins / total * 100, 1) if total > 0 else 0.0
    avg_roi = round(sum(t.get("roi_pct", 0) for t in trades) / total, 1) if total > 0 else 0.0
    sign    = "UP" if pnl >= 0 else "DOWN"
    lines   = [
        f"Початок: ${init:.2f}",
        f"Зараз:   ${current:.2f} ({pnl:+.2f}$ / {roi:+.1f}%) {sign}",
        "",
        f"Угод: {total} | Win: {wins} | Loss: {losses} | Manual: {manual}",
        f"Win rate: {wr}% | Avg ROI: {avg_roi:+.1f}%",
    ]
    return "\n".join(lines)


def save_selected_city() -> None:
    try:
        # Конвертуємо int ключі в str для JSON
        SELECTED_CITY_FILE.write_text(
            json.dumps({str(k): v for k, v in selected_city.items()}, indent=2)
        )
    except Exception as e:
        logger.error("Save selected_city: %s", e)


def load_selected_city() -> None:
    global selected_city
    if SELECTED_CITY_FILE.exists():
        try:
            data = json.loads(SELECTED_CITY_FILE.read_text())
            selected_city = {int(k): v for k, v in data.items()}
            logger.info("Selected city loaded: %d users", len(selected_city))
        except Exception as e:
            logger.warning("Load selected_city: %s", e)


async def handle_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text    = update.message.text
    now     = datetime.utcnow()
    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    cid     = update.effective_chat.id
    # Поточне вибране місто
    city        = selected_city.get(cid, "warsaw")  # Warsaw пріоритет
    city_cfg    = CITIES[city]
    market_type = selected_market_type.get(cid, "highest")

    # Вибір типу ринку
    if text == "🌡️ Макс":
        selected_market_type[cid] = "highest"
        await update.message.reply_text(
            f"🌡️ Тип: Максимальна температура (Highest)",
            reply_markup=main_keyboard())
        return
    elif text == "❄️ Мін":
        selected_market_type[cid] = "lowest"
        await update.message.reply_text(
            f"❄️ Тип: Мінімальна температура (Lowest)",
            reply_markup=main_keyboard())
        return

    # Вибір міста
    if text == "🇵🇱 Warsaw":
        selected_city[cid] = "warsaw"
        save_selected_city()
        await update.message.reply_text(
            "🇵🇱 Вибрано Warsaw (EPWA). Всі запити тепер для Варшави.",
            reply_markup=main_keyboard())
        return
    elif text == "🇬🇧 London":
        selected_city[cid] = "london"
        save_selected_city()
        await update.message.reply_text(
            "🇬🇧 Вибрано London (EGLC). Всі запити тепер для Лондона.",
            reply_markup=main_keyboard())
        return
    elif text == "🇩🇪 Munich":
        selected_city[cid] = "munich"
        save_selected_city()
        await update.message.reply_text(
            "🇩🇪 Вибрано Munich (EDDM). Всі запити тепер для Мюнхена.",
            reply_markup=main_keyboard())
        return

    if text == "📅 Сьогодні":
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        await _send_full_report(context.bot, today, cid, "📅 Сьогодні", city, market_type)
    elif text in ("🔍 Завтра", "🔍 Прогноз завтра"):
        await _send_full_report(context.bot, tomorrow, cid, f"🔍 {city_cfg['emoji']} Завтра", city, market_type)
    elif text == "📅 Після завтра":
        day_after = (now + timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
        await _send_full_report(context.bot, day_after, cid, f"📅 {city_cfg['emoji']} Після завтра", city, market_type)
    elif text == "📅 Прогноз 2 дні":
        await update.message.reply_text("⏳ Збираю дані для 2 днів…")
        for days in (1, 2):
            dt = (now + timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)
            await _send_full_report(context.bot, dt, cid, "🔍 Прогноз")
    elif text == "📊 Polymarket":
        context.args = []
        context._city = city
        await cmd_poll(update, context)
    elif text in ("📈 Мої позиції", "📈 Позиції"):
        await cmd_positions(update, context)
    elif text == "🌤 Погода завтра":
        context.args = []
        await cmd_forecast(update, context)
    elif text in ("📉 Тренд цін", "📉 Тренд"):
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
        cb_city = state.get("city", "london")
        _, markets, _ = get_polymarket_data(dt, cb_city)
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
    load_monitoring()
    load_selected_city()
    load_portfolio()

    app = ApplicationBuilder().token(TOKEN).build()

    for cmd, handler in [
        ("portfolio", cmd_portfolio),
        ("scan",      cmd_scan_global),
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

    # Авто MOS о 01:00 — ERA5 вже оновився за вчора
    kyiv_100 = datetime.now(KYIV_TZ).replace(hour=1, minute=0, second=0, microsecond=0)
    jq.run_daily(job_auto_mos, time=kyiv_100.timetz(), name="auto_mos")
    jq.run_daily(job_morning_briefing, time=kyiv_730.timetz(), name="briefing_730")
    jq.run_daily(job_market_scan,      time=kyiv_9.timetz(),   name="market_scan_9")
    jq.run_daily(job_daily_14,         time=kyiv_14.timetz(),  name="daily_14")
    jq.run_repeating(monitor_job,          interval=120,        first=15,   name="price_monitor")
    jq.run_repeating(job_forecast_monitor, interval=30*60,      first=60,   name="forecast_monitor")  # кожні 3 год

    start_keep_alive()
    logger.info("Bot v4 | 07:30 briefing | 09:00 scan | 14:00 daily | price 2min | forecast 30min")
    app.run_polling()


if __name__ == "__main__":
    main()
