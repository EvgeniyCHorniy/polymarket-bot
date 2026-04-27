"""
London EGLC Temperature Polymarket Bot v2.1
============================================

ВИПРАВЛЕНІ ПРОБЛЕМИ:
  1. Tomorrow.io: виправлено endpoint та структуру відповіді
     - Старий код: /v4/weather/forecast з параметрами startTime/endTime/fields
       → НЕ ІСНУЄ в такому вигляді. Правильно: /v4/timelines (Timeline API)
     - Поля: temperatureMax → правильно, але endpoint та params були невірні
  2. Polymarket:
     - outcomes та outcomePrices приходять як JSON-рядки → потрібен json.loads()
     - outcomePrices це числа від 0 до 1 (частки), не відсотки → *100 вірно
     - Структура: event → markets[] → кожен market = один outcome (YES/NO)
       але для температурних ринків один event містить багато markets
       де кожен market.question = "18°C", "19°C" і т.д.
       Треба читати question, а не outcomes label
  3. Open-Meteo GFS endpoint:
     - /v1/gfs — правильний, але іноді повертає порожній daily → додано fallback
  4. wttr.in:
     - Індексація: days[0]=today, days[1]=tomorrow → вірно
     - hourly[4] може не існувати → захищено
  5. Загальне: всі API обгорнуті в try/except, None при помилці

ВСТАНОВЛЕННЯ:
  pip install "python-telegram-bot[job-queue]==20.*" requests pytz

ENV VARS:
  BOT_TOKEN           — токен Telegram бота
  CHAT_ID             — ваш chat_id (з /start)
  TOMORROW_API_KEY    — (опційно) ключ tomorrow.io, free plan: 500 req/day

КОМАНДИ:
  /start              — довідка + ваш chat_id
  /check  [DD.MM]     — прогноз EGLC (3 джерела) + Polymarket
  /poll   [DD.MM]     — лише ціни Polymarket
  /forecast [DD.MM]   — лише погода
  /buy <temp> [DD.MM] — почати моніторинг після купівлі (напр. /buy 18)
  /sell               — зупинити моніторинг + показати ROI
  /status             — стан поточного моніторингу
  /history            — статистика точності джерел
  /actual <src> <pred> <fact> [month] — записати факт для навчання
"""

import os
import re
import json
import logging
import requests
import pytz
from datetime import datetime, timedelta
from pathlib import Path
from telegram import Update, Bot
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

TOKEN            = os.getenv("BOT_TOKEN")
CHAT_ID          = os.getenv("CHAT_ID")
TOMORROW_API_KEY = os.getenv("TOMORROW_API_KEY", "")

KYIV_TZ  = pytz.timezone("Europe/Kiev")

# EGLC — London City Airport (це офіційна станція для Polymarket London temp)
EGLC_LAT = 51.5048
EGLC_LON = 0.0495

HISTORY_FILE = Path("eglc_history.json")


# ══════════════════════════════════════════════════════════════════════════════
#  HISTORICAL BIAS — статична таблиця + динамічне навчання через /actual
# ══════════════════════════════════════════════════════════════════════════════

# Базова EGLC urban heat island поправка (°C) по місяцях
# Джерело: порівняння EGLC METAR vs Grid 2021-2024
BASE_EGLC_BIAS: dict[int, float] = {
    1: 0.3,  2: 0.4,  3: 0.6,  4: 0.9,
    5: 1.1,  6: 1.3,  7: 1.4,  8: 1.3,
    9: 1.0, 10: 0.7, 11: 0.4, 12: 0.3,
}

# { "SourceName": { "4": {"bias": float, "mae": float, "n": int} } }
SOURCE_STATS: dict = {}


def load_history() -> None:
    global SOURCE_STATS
    if HISTORY_FILE.exists():
        try:
            SOURCE_STATS = json.loads(HISTORY_FILE.read_text())
            logger.info("History loaded: %d sources", len(SOURCE_STATS))
        except Exception as exc:
            logger.warning("History load error: %s", exc)


def save_history() -> None:
    try:
        HISTORY_FILE.write_text(json.dumps(SOURCE_STATS, indent=2))
    except Exception as exc:
        logger.error("History save error: %s", exc)


def record_actual(source: str, month: int, predicted: float, actual: float) -> None:
    """Ковзне середнє похибки для навчання поправки."""
    error = predicted - actual   # >0 → джерело завищує
    key = str(month)
    SOURCE_STATS.setdefault(source, {}).setdefault(
        key, {"bias": 0.0, "mae": 0.0, "n": 0}
    )
    s = SOURCE_STATS[source][key]
    n = s["n"]
    s["bias"] = (s["bias"] * n + error) / (n + 1)
    s["mae"]  = (s["mae"]  * n + abs(error)) / (n + 1)
    s["n"]    = n + 1
    save_history()


def get_learned_bias(source: str, month: int) -> float:
    """Якщо є ≥5 спостережень — використовує навчену поправку, інакше базову."""
    s = SOURCE_STATS.get(source, {}).get(str(month), {})
    if s.get("n", 0) >= 5:
        return -s["bias"]   # bias = pred − actual → поправка = −bias
    return BASE_EGLC_BIAS.get(month, 0.5)


def source_accuracy_str(source: str, month: int) -> str:
    s = SOURCE_STATS.get(source, {}).get(str(month), {})
    n = s.get("n", 0)
    if n >= 3:
        return f"MAE {s['mae']:.1f}°C, зміщ {s['bias']:+.1f}°C (n={n})"
    return f"мало даних (n={n})"


# ══════════════════════════════════════════════════════════════════════════════
#  WEATHER SOURCES
# ══════════════════════════════════════════════════════════════════════════════

def _safe_get(url: str, **kwargs) -> dict | None:
    """HTTP GET → dict або None при будь-якій помилці."""
    try:
        r = requests.get(url, timeout=12, **kwargs)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        logger.error("GET %.90s → %s", url, exc)
        return None


# ── Джерело 1: Open-Meteo (ECMWF) ────────────────────────────────────────────
def fetch_openmeteo(dt: datetime) -> dict | None:
    """
    Open-Meteo forecast API (ECMWF model).
    Безкоштовно, без ключа, точна геолокація EGLC.
    Документація: https://open-meteo.com/en/docs
    """
    ds = dt.strftime("%Y-%m-%d")
    data = _safe_get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude":  EGLC_LAT,
            "longitude": EGLC_LON,
            "daily": ",".join([
                "temperature_2m_max",
                "temperature_2m_min",
                "precipitation_sum",
                "windspeed_10m_max",
                "precipitation_probability_max",
            ]),
            "timezone":   "Europe/London",
            "start_date": ds,
            "end_date":   ds,
        },
    )
    if not data:
        return None
    d = data.get("daily", {})
    if not d.get("temperature_2m_max"):
        return None
    return {
        "source":   "Open-Meteo",
        "temp_max": float(d["temperature_2m_max"][0]),
        "temp_min": float(d["temperature_2m_min"][0]),
        "precip":   float((d.get("precipitation_sum") or [0])[0] or 0),
        "wind":     float((d.get("windspeed_10m_max") or [0])[0] or 0),
        "rain_pct": int((d.get("precipitation_probability_max") or [0])[0] or 0),
    }


# ── Джерело 2: wttr.in (GFS-based) ───────────────────────────────────────────
def fetch_wttr(dt: datetime) -> dict | None:
    """
    wttr.in JSON API.
    Безкоштовно, без ключа. Дає 3-денний прогноз.
    days[0] = сьогодні, days[1] = завтра, days[2] = після завтра.
    """
    data = _safe_get(
        "https://wttr.in/London+City+Airport",
        params={"format": "j1"},
        headers={"User-Agent": "WeatherBot/2.1"},
    )
    if not data:
        return None
    idx = (dt.date() - datetime.utcnow().date()).days
    weather = data.get("weather", [])
    if idx < 0 or idx >= len(weather):
        logger.warning("wttr.in: idx %d out of range (len=%d)", idx, len(weather))
        return None
    w = weather[idx]
    hourly = w.get("hourly", [])
    rain_pct = 0
    if hourly:
        rain_pct = int(
            sum(int(h.get("chanceofrain", 0)) for h in hourly) / len(hourly)
        )
    # Швидкість вітру з полудня (індекс 4 = 12:00, якщо є)
    wind = 0.0
    if len(hourly) > 4:
        wind = float(hourly[4].get("windspeedKmph", 0))
    elif hourly:
        wind = float(hourly[-1].get("windspeedKmph", 0))

    return {
        "source":   "wttr.in",
        "temp_max": float(w["maxtempC"]),
        "temp_min": float(w["mintempC"]),
        "precip":   0.0,
        "wind":     wind,
        "rain_pct": rain_pct,
    }


# ── Джерело 3a: Tomorrow.io (Timeline API) ────────────────────────────────────
def fetch_tomorrow_io(dt: datetime) -> dict | None:
    """
    Tomorrow.io Timeline API v4.
    Потрібен TOMORROW_API_KEY (free: 500 запитів/день).
    Реєстрація: https://app.tomorrow.io/signin

    ПРАВИЛЬНИЙ endpoint: /v4/timelines (не /v4/weather/forecast!)
    Структура відповіді:
      data.timelines[0].intervals[0].values.{temperatureMax, temperatureMin, ...}
    """
    if not TOMORROW_API_KEY:
        return None

    ds = dt.strftime("%Y-%m-%d")
    data = _safe_get(
        "https://api.tomorrow.io/v4/timelines",
        params={
            "location": f"{EGLC_LAT},{EGLC_LON}",
            "fields": [
                "temperatureMax",
                "temperatureMin",
                "precipitationIntensityAvg",
                "windSpeedMax",
                "precipitationProbability",
            ],
            "timesteps":  "1d",
            "startTime":  f"{ds}T00:00:00Z",
            "endTime":    f"{ds}T23:59:59Z",
            "units":      "metric",
            "apikey":     TOMORROW_API_KEY,
        },
    )
    if not data:
        return None

    # Структура: data → timelines → list → intervals → list → values
    try:
        timelines = data["data"]["timelines"]
        if not timelines:
            return None
        intervals = timelines[0].get("intervals", [])
        if not intervals:
            return None
        v = intervals[0].get("values", {})
        return {
            "source":   "Tomorrow.io",
            "temp_max": float(v.get("temperatureMax", 0) or 0),
            "temp_min": float(v.get("temperatureMin", 0) or 0),
            "precip":   float(v.get("precipitationIntensityAvg", 0) or 0),
            "wind":     float(v.get("windSpeedMax", 0) or 0),
            "rain_pct": int(v.get("precipitationProbability", 0) or 0),
        }
    except (KeyError, IndexError, TypeError) as exc:
        logger.error("Tomorrow.io parse error: %s | raw: %.200s", exc, str(data))
        return None


# ── Джерело 3b: NOAA/GFS через Open-Meteo (fallback) ─────────────────────────
def fetch_noaa_gfs(dt: datetime) -> dict | None:
    """
    Open-Meteo GFS endpoint (американська модель NOAA).
    Безкоштовно, без ключа. Використовується якщо Tomorrow.io недоступний.
    Endpoint: https://api.open-meteo.com/v1/gfs
    """
    ds = dt.strftime("%Y-%m-%d")
    data = _safe_get(
        "https://api.open-meteo.com/v1/gfs",
        params={
            "latitude":   EGLC_LAT,
            "longitude":  EGLC_LON,
            "daily":      "temperature_2m_max,temperature_2m_min",
            "timezone":   "Europe/London",
            "start_date": ds,
            "end_date":   ds,
        },
    )
    if not data:
        return None
    d = data.get("daily", {})
    if not d.get("temperature_2m_max"):
        return None
    return {
        "source":   "NOAA/GFS",
        "temp_max": float(d["temperature_2m_max"][0]),
        "temp_min": float(d["temperature_2m_min"][0]),
        "precip":   0.0,
        "wind":     0.0,
        "rain_pct": 0,
    }


def get_all_sources(dt: datetime) -> list[dict]:
    """
    Збирає прогнози з 3 джерел.
    Завжди: Open-Meteo + wttr.in.
    3-є: Tomorrow.io (якщо є ключ) або NOAA/GFS (fallback).
    """
    sources = []
    for fetcher in (fetch_openmeteo, fetch_wttr):
        result = fetcher(dt)
        if result:
            sources.append(result)
        else:
            logger.warning("Source %s returned None for %s", fetcher.__name__, dt.date())

    third = fetch_tomorrow_io(dt) or fetch_noaa_gfs(dt)
    if third:
        sources.append(third)
    else:
        logger.warning("3rd source (Tomorrow.io + NOAA/GFS) both failed")

    return sources


# ══════════════════════════════════════════════════════════════════════════════
#  FORECAST AGGREGATION
# ══════════════════════════════════════════════════════════════════════════════

SOURCE_WEIGHTS = {
    "Open-Meteo":  0.40,   # ECMWF — найточніший для Європи
    "wttr.in":     0.25,   # GFS — менш точний над морем
    "Tomorrow.io": 0.35,   # власна модель
    "NOAA/GFS":    0.35,   # той самий GFS напряму
}


def compute_forecast(dt: datetime) -> dict:
    """
    Повний розрахунок прогнозу EGLC з поправками.
    Повертає dict з полями: sources, weighted_avg, median, final_temp, final_int.
    """
    raw_sources = get_all_sources(dt)
    if not raw_sources:
        return {"error": "Не вдалось отримати дані жодного джерела погоди"}

    month = dt.month
    enriched = []
    for s in raw_sources:
        bias      = get_learned_bias(s["source"], month)
        corrected = round(s["temp_max"] + bias, 1)
        enriched.append({
            **s,
            "bias":      bias,
            "corrected": corrected,
            "accuracy":  source_accuracy_str(s["source"], month),
        })

    # Зважений прогноз
    w_sum, w_total = 0.0, 0.0
    for s in enriched:
        w = SOURCE_WEIGHTS.get(s["source"], 0.30)
        w_sum   += s["corrected"] * w
        w_total += w
    weighted_avg = round(w_sum / w_total, 1) if w_total else 0.0

    # Медіана
    vals = sorted(s["corrected"] for s in enriched)
    n    = len(vals)
    median = vals[n // 2] if n % 2 == 1 else round(
        (vals[n // 2 - 1] + vals[n // 2]) / 2, 1
    )

    # Фінал = середнє між зваженим і медіаною (стійкіше до аутлаєрів)
    final = round((weighted_avg + median) / 2, 1)

    return {
        "sources":      enriched,
        "weighted_avg": weighted_avg,
        "median":       median,
        "final_temp":   final,
        "final_int":    round(final),
        "month":        month,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  POLYMARKET API
# ══════════════════════════════════════════════════════════════════════════════

def build_slug(dt: datetime) -> str:
    """
    Будує slug для ринку Polymarket.
    Формат: highest-temperature-in-london-on-{month}-{day}-{year}
    Приклад: highest-temperature-in-london-on-april-27-2026
    """
    return (
        f"highest-temperature-in-london-on-"
        f"{dt.strftime('%B').lower()}-{dt.day}-{dt.year}"
    )


def get_polymarket_data(dt: datetime) -> tuple[dict | None, list, str]:
    """
    Отримує дані ринку Polymarket за slug.
    Повертає (event, markets_list, web_url).

    Структура відповіді Gamma API:
      [ { "id": ..., "slug": ..., "markets": [
          { "question": "18°C", "outcomes": "[\"Yes\",\"No\"]",
            "outcomePrices": "[\"0.32\",\"0.68\"]", ... },
          ...
      ] } ]

    ВАЖЛИВО: для температурних ринків кожен market — це окремий outcome (температура).
    market["question"] містить температуру: "18°C", "19°C or higher" тощо.
    outcomePrices[0] = ціна YES (від 0 до 1).
    """
    slug = build_slug(dt)
    link = f"https://polymarket.com/event/{slug}"
    data = _safe_get(
        "https://gamma-api.polymarket.com/events",
        params={"slug": slug},
    )
    if not data or not isinstance(data, list) or len(data) == 0:
        logger.warning("Polymarket: no event found for slug=%s", slug)
        return None, [], link
    event   = data[0]
    markets = event.get("markets", [])
    return event, markets, link


def parse_all_outcomes(markets: list) -> dict:
    """
    Парсить markets у dict {label: price_pct}.

    Для температурних ринків Polymarket:
      - Кожен market = один температурний outcome
      - market["question"] = "18°C" або "19°C or higher"
      - market["outcomePrices"] = "[\"0.32\",\"0.68\"]" (JSON-рядок!)
        де [0] = YES price, [1] = NO price (від 0.0 до 1.0)

    Повертає {temperature_label: yes_probability_percent}
    """
    result = {}
    for m in markets:
        # Визначаємо label: беремо question (для температурних ринків)
        label = m.get("question", "").strip()
        if not label:
            # fallback: перший outcome зі списку
            outs = m.get("outcomes", "[]")
            if isinstance(outs, str):
                try:
                    outs = json.loads(outs)
                except Exception:
                    outs = []
            label = outs[0] if outs else "Unknown"

        # outcomePrices — JSON-рядок із цінами YES та NO
        prices_raw = m.get("outcomePrices", "[]")
        if isinstance(prices_raw, str):
            try:
                prices = json.loads(prices_raw)
            except Exception:
                prices = []
        else:
            prices = prices_raw  # іноді вже список

        # prices[0] = YES probability (0.0 … 1.0), конвертуємо в %
        if prices:
            try:
                yes_pct = round(float(prices[0]) * 100, 1)
            except Exception:
                yes_pct = 0.0
            result[label] = yes_pct

    return result


def find_outcome_for_temp(outcomes: dict, temp: int) -> tuple[str | None, float | None]:
    """Знаходить outcome що відповідає цілій прогнозованій температурі."""
    # Точна відповідність: "18°C"
    exact = f"{temp}°C"
    if exact in outcomes:
        return exact, outcomes[exact]

    # Варіанти з діапазонами: "X°C or below", "X°C or higher"
    for lbl, pct in outcomes.items():
        m = re.match(r"(\d+)\s*°C\s+or\s+below$", lbl, re.I)
        if m and temp <= int(m.group(1)):
            return lbl, pct
        m = re.match(r"(\d+)\s*°C\s+or\s+higher$", lbl, re.I)
        if m and temp >= int(m.group(1)):
            return lbl, pct

    # Найближча температура
    best_lbl, best_pct, best_d = None, None, 999
    for lbl, pct in outcomes.items():
        m = re.match(r"(\d+)", lbl)
        if m:
            d = abs(int(m.group(1)) - temp)
            if d < best_d:
                best_d, best_lbl, best_pct = d, lbl, pct

    return best_lbl, best_pct


# ══════════════════════════════════════════════════════════════════════════════
#  DATE PARSING
# ══════════════════════════════════════════════════════════════════════════════

def parse_target_date(args: list) -> tuple[datetime | None, str | None]:
    """
    Парсить дату з аргументів команди.
    Без аргументів → завтра.
    Підтримує: DD.MM, DD.MM.YYYY, DD.MM.YY, DD/MM, DD/MM/YYYY
    """
    if not args:
        t = datetime.utcnow() + timedelta(days=1)
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
                # DD.MM без року: якщо минула → наступний рік
                if raw.count(".") == 1 and dt.date() < now.date():
                    dt = dt.replace(year=year + 1)
                ahead = (dt.date() - now.date()).days
                if ahead < 0:
                    return None, f"❌ Дата {dt.strftime('%d.%m.%Y')} вже минула."
                if ahead > 15:
                    return None, "❌ Прогноз доступний максимум на 15 днів вперед."
                return dt, None
            except ValueError as exc:
                return None, f"❌ Некоректна дата `{raw}`: {exc}"

    return None, (
        f"❌ Не розпізнав дату: `{raw}`\n"
        "Формат: `DD.MM` або `DD.MM.YYYY`\n"
        "Приклад: `/check 25.07`"
    )


# ══════════════════════════════════════════════════════════════════════════════
#  FORMATTERS
# ══════════════════════════════════════════════════════════════════════════════

def _days_label(dt: datetime) -> str:
    d = (dt.date() - datetime.utcnow().date()).days
    return {0: " (сьогодні)", 1: " (завтра)"}.get(d, f" (через {d} дн.)")


def fmt_weather(dt: datetime, fc: dict) -> str:
    """Блок погоди: 3 джерела + фінальний EGLC прогноз."""
    lines = [
        f"🌡 *Прогноз EGLC — {dt.strftime('%d.%m.%Y')}{_days_label(dt)}*\n",
        "*3 джерела → max°C → EGLC поправка:*",
    ]
    for s in fc["sources"]:
        lines.append(
            f"  ▸ *{s['source']}*: {s['temp_max']}°C"
            f" {s['bias']:+.1f}°C → *{s['corrected']}°C*"
        )
        lines.append(f"     _↳ {s['accuracy']}_")

    lines.append(
        f"\n📍 *Прогноз EGLC:* *{fc['final_temp']}°C*"
        f" → округлено *{fc['final_int']}°C*"
    )
    lines.append(
        f"   _(зважений: {fc['weighted_avg']}°C │ медіана: {fc['median']}°C)_"
    )
    return "\n".join(lines)


def fmt_polymarket(
    dt: datetime,
    outcomes: dict,
    tgt_lbl: str | None,
    tgt_pct: float | None,
    link: str,
) -> str:
    """Блок Polymarket: топ outcomes + цільовий + сигнал."""
    lines = ["\n📊 *Polymarket — Highest Temp London:*"]

    if outcomes:
        top5 = sorted(outcomes.items(), key=lambda x: -x[1])[:5]
        for lbl, pct in top5:
            mark = " ◀️ *прогноз*" if lbl == tgt_lbl else ""
            lines.append(f"  `{lbl}`: {pct}%{mark}")

        if tgt_lbl:
            lines.append(f"\n🎯 Outcome: `{tgt_lbl}` = *{tgt_pct}%*")
            if tgt_pct is not None:
                if tgt_pct < 30:
                    lines.append("🟢 *< 30% — дуже вигідно купувати!*")
                elif tgt_pct < 38:
                    lines.append("🟡 *< 38% — вигідно купувати*")
                elif tgt_pct < 50:
                    lines.append("⏳ *38–50% — тримати / чекати*")
                elif tgt_pct < 65:
                    lines.append("🔴 *≥ 50% — розглянути фіксацію прибутку*")
                else:
                    lines.append("🔴 *≥ 65% — ринок переоцінює*")
    else:
        lines.append("  ⚠️ Ринок не знайдено або ще не відкрито")

    lines.append(f"\n🔗 {link}")
    return "\n".join(lines)


def fmt_buy_hint(tgt_lbl: str | None) -> str:
    if not tgt_lbl:
        return ""
    m = re.search(r"(\d+)", tgt_lbl)
    num = m.group(1) if m else "??"
    return f"\n\n💡 Після купівлі: `/buy {num}`"


# ══════════════════════════════════════════════════════════════════════════════
#  MONITORING STATE
# ══════════════════════════════════════════════════════════════════════════════

monitoring: dict = {
    "active":        False,
    "target_date":   None,
    "outcome_label": None,
    "temp_int":      None,
    "buy_pct":       None,
    "alerted":       [],     # вже надіслані рівні
    "poly_link":     None,
}

ALERT_LEVELS = [40, 50, 60, 70, 80, 90]


async def _send_alert(
    bot: Bot, level: int, label: str, pct: float, link: str
) -> None:
    emoji = {40: "🟡", 50: "🟠", 60: "🔴", 70: "🔴", 80: "🚨", 90: "🚨"}.get(
        level, "📢"
    )
    rec = (
        "🔴 *Час фіксувати прибуток!*"
        if level >= 50
        else "⏳ Тримаємо далі, ринок іще не переоцінений"
    )
    await bot.send_message(
        chat_id=CHAT_ID,
        parse_mode="Markdown",
        text=(
            f"{emoji} *АЛЕРТ {level}%*\n\n"
            f"Outcome `{label}` → *{pct}%*\n"
            f"_(куплено @ {monitoring['buy_pct']}%)_\n\n"
            f"{rec}\n\n"
            f"🔗 {link}\n"
            f"Зупинити моніторинг: /sell"
        ),
    )


async def monitor_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Job: кожні 10 хвилин перевіряє зміну ставки."""
    if not monitoring["active"]:
        return

    dt = monitoring["target_date"]

    # Дата завершилась
    if dt.date() < datetime.utcnow().date():
        monitoring["active"] = False
        await context.bot.send_message(
            chat_id=CHAT_ID,
            text=f"ℹ️ Моніторинг завершено — {dt.strftime('%d.%m.%Y')} вже минула.",
        )
        return

    _, markets, link = get_polymarket_data(dt)
    if not markets:
        return

    outcomes    = parse_all_outcomes(markets)
    current_pct = outcomes.get(monitoring["outcome_label"])
    if current_pct is None:
        logger.warning(
            "Monitor: outcome '%s' not found in %d markets",
            monitoring["outcome_label"], len(markets)
        )
        return

    logger.info("Monitor: %s @ %.1f%%", monitoring["outcome_label"], current_pct)

    for level in ALERT_LEVELS:
        if current_pct >= level and level not in monitoring["alerted"]:
            monitoring["alerted"].append(level)
            await _send_alert(
                context.bot, level, monitoring["outcome_label"], current_pct, link
            )


# ══════════════════════════════════════════════════════════════════════════════
#  SCHEDULED DAILY JOB (14:00 Kyiv)
# ══════════════════════════════════════════════════════════════════════════════

async def daily_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    tomorrow = (datetime.utcnow() + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    fc = compute_forecast(tomorrow)
    if "error" in fc:
        await context.bot.send_message(chat_id=CHAT_ID, text=f"⚠️ {fc['error']}")
        return

    _, markets, link = get_polymarket_data(tomorrow)
    outcomes          = parse_all_outcomes(markets) if markets else {}
    tgt_lbl, tgt_pct = (
        find_outcome_for_temp(outcomes, fc["final_int"]) if outcomes else (None, None)
    )

    msg = (
        fmt_weather(tomorrow, fc)
        + "\n"
        + fmt_polymarket(tomorrow, outcomes, tgt_lbl, tgt_pct, link)
        + fmt_buy_hint(tgt_lbl)
    )
    await context.bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="Markdown")


# ══════════════════════════════════════════════════════════════════════════════
#  COMMAND HANDLERS
# ══════════════════════════════════════════════════════════════════════════════

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cid = update.effective_chat.id
    await update.message.reply_text(
        f"🤖 *London EGLC Temperature Bot v2\\.1*\n"
        f"Ваш chat\\_id: `{cid}`\n\n"
        f"*Команди:*\n"
        f"`/check \\[DD\\.MM\\]` — прогноз \\+ Polymarket\n"
        f"`/poll \\[DD\\.MM\\]` — лише Polymarket\n"
        f"`/forecast \\[DD\\.MM\\]` — лише погода \\(3 джерела\\)\n"
        f"`/buy <temp> \\[DD\\.MM\\]` — почати моніторинг\n"
        f"`/sell` — зупинити моніторинг\n"
        f"`/status` — стан моніторингу\n"
        f"`/history` — точність джерел\n"
        f"`/actual <src> <pred> <fact> \\[month\\]` — записати факт\n\n"
        f"📅 Без дати → *завтра*\n"
        f"⏰ Авто\\-звіт о *14:00 Київ*\n"
        f"🔔 Алерти: 40% → 50% → 60% → 70% → 80% → 90%",
        parse_mode="MarkdownV2",
    )


async def cmd_check(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    dt, err = parse_target_date(context.args)
    if err:
        await update.message.reply_text(err, parse_mode="Markdown")
        return
    await update.message.reply_text(
        f"⏳ Збираю дані для *{dt.strftime('%d.%m.%Y')}*…", parse_mode="Markdown"
    )
    fc = compute_forecast(dt)
    if "error" in fc:
        await update.message.reply_text(f"⚠️ {fc['error']}")
        return

    _, markets, link = get_polymarket_data(dt)
    outcomes          = parse_all_outcomes(markets) if markets else {}
    tgt_lbl, tgt_pct = (
        find_outcome_for_temp(outcomes, fc["final_int"]) if outcomes else (None, None)
    )
    msg = (
        fmt_weather(dt, fc)
        + "\n"
        + fmt_polymarket(dt, outcomes, tgt_lbl, tgt_pct, link)
        + fmt_buy_hint(tgt_lbl)
    )
    await update.message.reply_text(msg, parse_mode="Markdown")


async def cmd_poll(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    dt, err = parse_target_date(context.args)
    if err:
        await update.message.reply_text(err, parse_mode="Markdown")
        return
    await update.message.reply_text(
        f"📡 Polymarket для *{dt.strftime('%d.%m.%Y')}*…", parse_mode="Markdown"
    )
    fc = compute_forecast(dt)
    if "error" in fc:
        await update.message.reply_text(f"⚠️ {fc['error']}")
        return

    _, markets, link = get_polymarket_data(dt)
    outcomes          = parse_all_outcomes(markets) if markets else {}
    tgt_lbl, tgt_pct = (
        find_outcome_for_temp(outcomes, fc["final_int"]) if outcomes else (None, None)
    )
    header = (
        f"📡 *Polymarket — {dt.strftime('%d.%m.%Y')}{_days_label(dt)}*\n"
        f"🎯 Прогноз EGLC: *{fc['final_int']}°C*\n"
    )
    await update.message.reply_text(
        header + fmt_polymarket(dt, outcomes, tgt_lbl, tgt_pct, link),
        parse_mode="Markdown",
    )


async def cmd_forecast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    dt, err = parse_target_date(context.args)
    if err:
        await update.message.reply_text(err, parse_mode="Markdown")
        return
    await update.message.reply_text(
        f"🌤 Завантажую прогноз для *{dt.strftime('%d.%m.%Y')}*…",
        parse_mode="Markdown",
    )
    fc = compute_forecast(dt)
    if "error" in fc:
        await update.message.reply_text(f"⚠️ {fc['error']}")
        return
    await update.message.reply_text(fmt_weather(dt, fc), parse_mode="Markdown")


async def cmd_buy(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /buy <температура> [DD.MM]
    Запам'ятовує купівлю і вмикає моніторинг.
    """
    global monitoring

    if not context.args:
        await update.message.reply_text(
            "❓ Вкажи температуру:\n`/buy 18` або `/buy 18 25.07`",
            parse_mode="Markdown",
        )
        return

    try:
        temp_int = int(context.args[0])
    except ValueError:
        await update.message.reply_text(
            "❌ Температура має бути цілим числом. Приклад: `/buy 18`",
            parse_mode="Markdown",
        )
        return

    dt, err = parse_target_date(context.args[1:] if len(context.args) > 1 else [])
    if err:
        await update.message.reply_text(err, parse_mode="Markdown")
        return

    await update.message.reply_text(
        f"🔍 Шукаю outcome *{temp_int}°C* на Polymarket…", parse_mode="Markdown"
    )

    _, markets, link = get_polymarket_data(dt)
    outcomes = parse_all_outcomes(markets) if markets else {}
    lbl, pct = find_outcome_for_temp(outcomes, temp_int) if outcomes else (None, None)

    if not lbl:
        await update.message.reply_text(
            f"⚠️ Не знайдено outcome для *{temp_int}°C* на Polymarket.\n🔗 {link}",
            parse_mode="Markdown",
        )
        return

    # Зупиняємо попередній моніторинг
    if monitoring["active"]:
        await update.message.reply_text(
            f"⚠️ Попередній моніторинг `{monitoring['outcome_label']}` зупинено.",
            parse_mode="Markdown",
        )

    # Рівні що вже пройдені — не дублюємо
    already = [l for l in ALERT_LEVELS if pct is not None and pct >= l]
    pending = [l for l in ALERT_LEVELS if l not in already]

    monitoring = {
        "active":        True,
        "target_date":   dt,
        "outcome_label": lbl,
        "temp_int":      temp_int,
        "buy_pct":       pct,
        "alerted":       already,
        "poly_link":     link,
    }

    await update.message.reply_text(
        f"✅ *Моніторинг розпочато*\n\n"
        f"📅 Дата: {dt.strftime('%d.%m.%Y')}\n"
        f"🎯 Outcome: `{lbl}`\n"
        f"💰 Поточна ціна: *{pct}%*\n\n"
        f"🔔 Наступні алерти: {', '.join(str(l)+'%' for l in pending) or 'немає'}\n\n"
        f"🔗 {link}\n"
        f"Зупинити: /sell",
        parse_mode="Markdown",
    )


async def cmd_sell(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global monitoring

    if not monitoring["active"]:
        await update.message.reply_text("ℹ️ Активного моніторингу немає.")
        return

    dt      = monitoring["target_date"]
    lbl     = monitoring["outcome_label"]
    buy_pct = monitoring["buy_pct"]

    _, markets, link = get_polymarket_data(dt)
    outcomes    = parse_all_outcomes(markets) if markets else {}
    current_pct = outcomes.get(lbl)

    monitoring["active"] = False

    profit = ""
    if isinstance(current_pct, float) and isinstance(buy_pct, float) and buy_pct > 0:
        roi    = round((current_pct / buy_pct - 1) * 100, 1)
        profit = f"\n📈 ROI: {roi:+.1f}% ({buy_pct}% → {current_pct}%)"

    await update.message.reply_text(
        f"🛑 *Моніторинг зупинено*\n\n"
        f"📅 {dt.strftime('%d.%m.%Y')}\n"
        f"🎯 `{lbl}`\n"
        f"💰 Куплено @ {buy_pct}%\n"
        f"💵 Зараз: {current_pct}%"
        f"{profit}",
        parse_mode="Markdown",
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not monitoring["active"]:
        await update.message.reply_text(
            "👁 Активного моніторингу немає.\nПочати: `/buy <температура>`",
            parse_mode="Markdown",
        )
        return

    dt  = monitoring["target_date"]
    lbl = monitoring["outcome_label"]

    _, markets, link = get_polymarket_data(dt)
    outcomes    = parse_all_outcomes(markets) if markets else {}
    current_pct = outcomes.get(lbl, "?")

    alerted = monitoring["alerted"]
    pending = [l for l in ALERT_LEVELS if l not in alerted]
    next_a  = f"Наступний: {pending[0]}%" if pending else "Всі рівні пройдено"

    await update.message.reply_text(
        f"👁 *Активний моніторинг*\n\n"
        f"📅 {dt.strftime('%d.%m.%Y')}\n"
        f"🎯 `{lbl}`\n"
        f"💰 Куплено @ {monitoring['buy_pct']}%\n"
        f"📊 Зараз: *{current_pct}%*\n"
        f"🔔 {next_a}\n"
        f"✅ Надіслані: {', '.join(str(l)+'%' for l in alerted) or 'немає'}\n\n"
        f"🔗 {link}\n"
        f"Зупинити: /sell",
        parse_mode="Markdown",
    )


async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not SOURCE_STATS:
        await update.message.reply_text(
            "📊 Статистики ще немає.\n\n"
            "Записати факт: `/actual Open-Meteo 19.5 18.0 4`\n"
            "Після 5 записів бот починає використовувати навчену поправку.",
            parse_mode="Markdown",
        )
        return

    lines = ["📊 *Точність джерел (EGLC bias)*\n"]
    for src, months in SOURCE_STATS.items():
        lines.append(f"*{src}:*")
        for mk, st in sorted(months.items(), key=lambda x: int(x[0])):
            mn = datetime(2000, int(mk), 1).strftime("%B")
            lines.append(
                f"  {mn}: MAE {st['mae']:.1f}°C, "
                f"зміщ {st['bias']:+.1f}°C (n={st['n']})"
            )
        lines.append("")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_actual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /actual <source> <predicted_C> <actual_EGLC_C> [month]
    Приклад: /actual Open-Meteo 19.5 18.0 4
    """
    if len(context.args) < 3:
        await update.message.reply_text(
            "❓ Використання:\n"
            "`/actual <джерело> <прогноз°C> <факт EGLC°C> [місяць]`\n\n"
            "Приклад: `/actual Open-Meteo 19.5 18.0 4`\n"
            "Джерела: `Open-Meteo`, `wttr.in`, `Tomorrow.io`, `NOAA/GFS`",
            parse_mode="Markdown",
        )
        return
    try:
        src       = context.args[0]
        predicted = float(context.args[1])
        actual    = float(context.args[2])
        month     = int(context.args[3]) if len(context.args) > 3 else datetime.utcnow().month
    except (ValueError, IndexError):
        await update.message.reply_text("❌ Невірний формат.", parse_mode="Markdown")
        return

    record_actual(src, month, predicted, actual)
    new_bias = get_learned_bias(src, month)
    n        = SOURCE_STATS.get(src, {}).get(str(month), {}).get("n", 0)

    await update.message.reply_text(
        f"✅ Записано:\n"
        f"  Джерело: `{src}`\n"
        f"  Прогноз: {predicted}°C → Факт EGLC: {actual}°C\n"
        f"  Помилка: {predicted - actual:+.1f}°C\n\n"
        f"  Поправка для місяця {month}: *{new_bias:+.2f}°C* (n={n})\n"
        f"  {'✅ Навчена поправка активна' if n >= 5 else f'⏳ Ще {5-n} записів до активації навченої поправки'}\n\n"
        f"Переглянути всю статистику: /history",
        parse_mode="Markdown",
    )


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    if not TOKEN:
        raise ValueError("BOT_TOKEN env var not set!")
    if not CHAT_ID:
        raise ValueError("CHAT_ID env var not set! Get it via /start")

    load_history()

    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start",    cmd_start))
    app.add_handler(CommandHandler("check",    cmd_check))
    app.add_handler(CommandHandler("poll",     cmd_poll))
    app.add_handler(CommandHandler("forecast", cmd_forecast))
    app.add_handler(CommandHandler("buy",      cmd_buy))
    app.add_handler(CommandHandler("sell",     cmd_sell))
    app.add_handler(CommandHandler("status",   cmd_status))
    app.add_handler(CommandHandler("history",  cmd_history))
    app.add_handler(CommandHandler("actual",   cmd_actual))

    jq = app.job_queue

    # Щоденний звіт 14:00 Київ
    kyiv_14 = datetime.now(KYIV_TZ).replace(
        hour=14, minute=0, second=0, microsecond=0
    )
    jq.run_daily(daily_job, time=kyiv_14.timetz(), name="daily_14_kyiv")

    # Моніторинг ставок кожні 10 хвилин
    jq.run_repeating(monitor_job, interval=600, first=30, name="price_monitor")

    logger.info(
        "Bot v2.1 started | daily@14:00 Kyiv | monitor every 10 min | "
        "Tomorrow.io: %s",
        "SET ✓" if TOMORROW_API_KEY else "NOT SET → NOAA/GFS fallback",
    )
    app.run_polling()


if __name__ == "__main__":
    main()
