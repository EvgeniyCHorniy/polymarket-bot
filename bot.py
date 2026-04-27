"""
London EGLC Temperature Polymarket Bot v3
==========================================

НОВИНИ v3:
  1. Відкидання аутлаєрів — якщо джерело відхиляється > OUTLIER_THRESHOLD від
     медіани інших → його вага падає до 0.05 (практично ігнорується).
     Причина: wttr.in іноді дає 13°C коли інші 17°C — це псує прогноз.

  2. Паралельний мультидатний моніторинг — можна відкрити /buy для кількох
     дат одночасно. monitoring[] → словник {date_key: {...}}

  3. Авто-BUY сигнал — щоденний о 14:00 Kyiv та вранці о 8:00 Kyiv бот
     сам надсилає "🟢 КУПУЙ" якщо outcome < 38%

  4. Ранковий уточнений прогноз о 8:00 Kyiv (прогноз точніший вранці дня)

  5. /positions — список всіх активних моніторингів

  6. /sell <DD.MM> — закрити конкретну дату, /sell all — всі

  7. Консенсус з Polymarket — показує різницю між прогнозом і ринком

ВСТАНОВЛЕННЯ:
  pip install "python-telegram-bot[job-queue]==20.*" requests pytz

ENV VARS:
  BOT_TOKEN           — токен Telegram бота
  CHAT_ID             — chat_id куди слати алерти (з /start)
  TOMORROW_API_KEY    — (опційно) ключ tomorrow.io

КОМАНДИ:
  /start                  — довідка
  /check  [DD.MM]         — прогноз EGLC + Polymarket (default: завтра)
  /check2                 — прогноз для завтра І після завтра разом
  /poll   [DD.MM]         — лише ціни Polymarket
  /forecast [DD.MM]       — лише погода (3 джерела)
  /buy <temp> [DD.MM]     — відкрити моніторинг (напр. /buy 17 28.04)
  /sell [DD.MM|all]       — закрити моніторинг для дати або всі
  /positions              — всі активні моніторинги + поточні ціни
  /status [DD.MM]         — деталі одного моніторингу
  /history                — точність джерел (накопичена статистика)
  /actual <src> <pred> <fact> [month] — записати факт для навчання
"""

import os
import re
import json
import logging
import pytz
import requests
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

# EGLC — London City Airport (офіційна станція Polymarket)
EGLC_LAT = 51.5048
EGLC_LON = 0.0495

# Якщо джерело відхиляється від медіани інших більше ніж на X°C — аутлаєр
OUTLIER_THRESHOLD = 2.0

HISTORY_FILE = Path("eglc_history.json")

BUY_SIGNAL_MAX_PCT  = 38.0   # нижче — сигнал "КУПУЙ"
SELL_SIGNAL_MIN_PCT = 50.0   # вище — сигнал "ПРОДАВАЙ"
ALERT_LEVELS = [40, 50, 60, 70, 80, 90]


# ══════════════════════════════════════════════════════════════════════════════
#  HISTORICAL BIAS
# ══════════════════════════════════════════════════════════════════════════════

BASE_EGLC_BIAS: dict[int, float] = {
    1: 0.3,  2: 0.4,  3: 0.6,  4: 0.9,
    5: 1.1,  6: 1.3,  7: 1.4,  8: 1.3,
    9: 1.0, 10: 0.7, 11: 0.4, 12: 0.3,
}

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
    error = predicted - actual
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
    s = SOURCE_STATS.get(source, {}).get(str(month), {})
    if s.get("n", 0) >= 5:
        return -s["bias"]
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
    try:
        r = requests.get(url, timeout=12, **kwargs)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        logger.error("GET %.90s → %s", url, exc)
        return None


def fetch_openmeteo(dt: datetime) -> dict | None:
    """Open-Meteo ECMWF — найточніша модель для Європи, без ключа."""
    ds = dt.strftime("%Y-%m-%d")
    data = _safe_get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude":  EGLC_LAT,
            "longitude": EGLC_LON,
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,"
                     "windspeed_10m_max,precipitation_probability_max",
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


def fetch_wttr(dt: datetime) -> dict | None:
    """wttr.in GFS-based, безкоштовно. Дає 3 дні."""
    data = _safe_get(
        "https://wttr.in/London+City+Airport",
        params={"format": "j1"},
        headers={"User-Agent": "WeatherBot/3.0"},
    )
    if not data:
        return None
    idx = (dt.date() - datetime.utcnow().date()).days
    weather = data.get("weather", [])
    if idx < 0 or idx >= len(weather):
        return None
    w = weather[idx]
    hourly   = w.get("hourly", [])
    rain_pct = 0
    if hourly:
        rain_pct = int(sum(int(h.get("chanceofrain", 0)) for h in hourly) / len(hourly))
    wind = float(hourly[4]["windspeedKmph"]) if len(hourly) > 4 else (
        float(hourly[-1]["windspeedKmph"]) if hourly else 0.0
    )
    return {
        "source":   "wttr.in",
        "temp_max": float(w["maxtempC"]),
        "temp_min": float(w["mintempC"]),
        "precip":   0.0,
        "wind":     wind,
        "rain_pct": rain_pct,
    }


def fetch_tomorrow_io(dt: datetime) -> dict | None:
    """Tomorrow.io Timeline API. Потрібен TOMORROW_API_KEY."""
    if not TOMORROW_API_KEY:
        return None
    ds = dt.strftime("%Y-%m-%d")
    data = _safe_get(
        "https://api.tomorrow.io/v4/timelines",
        params={
            "location":  f"{EGLC_LAT},{EGLC_LON}",
            "fields":    ["temperatureMax", "temperatureMin",
                          "precipitationIntensityAvg", "windSpeedMax",
                          "precipitationProbability"],
            "timesteps": "1d",
            "startTime": f"{ds}T00:00:00Z",
            "endTime":   f"{ds}T23:59:59Z",
            "units":     "metric",
            "apikey":    TOMORROW_API_KEY,
        },
    )
    if not data:
        return None
    try:
        intervals = data["data"]["timelines"][0]["intervals"]
        v = intervals[0]["values"]
        return {
            "source":   "Tomorrow.io",
            "temp_max": float(v.get("temperatureMax", 0) or 0),
            "temp_min": float(v.get("temperatureMin", 0) or 0),
            "precip":   float(v.get("precipitationIntensityAvg", 0) or 0),
            "wind":     float(v.get("windSpeedMax", 0) or 0),
            "rain_pct": int(v.get("precipitationProbability", 0) or 0),
        }
    except (KeyError, IndexError, TypeError) as exc:
        logger.error("Tomorrow.io parse: %s", exc)
        return None


def fetch_noaa_gfs(dt: datetime) -> dict | None:
    """NOAA GFS через Open-Meteo — fallback якщо Tomorrow.io немає."""
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
    sources = []
    for fetcher in (fetch_openmeteo, fetch_wttr):
        r = fetcher(dt)
        if r:
            sources.append(r)
    third = fetch_tomorrow_io(dt) or fetch_noaa_gfs(dt)
    if third:
        sources.append(third)
    return sources


# ══════════════════════════════════════════════════════════════════════════════
#  OUTLIER DETECTION + FORECAST AGGREGATION
# ══════════════════════════════════════════════════════════════════════════════

SOURCE_WEIGHTS = {
    "Open-Meteo":  0.40,
    "wttr.in":     0.25,
    "Tomorrow.io": 0.35,
    "NOAA/GFS":    0.35,
}


def _median(vals: list[float]) -> float:
    s = sorted(vals)
    n = len(s)
    return s[n // 2] if n % 2 == 1 else round((s[n//2-1] + s[n//2]) / 2, 1)


def detect_outliers(sources: list[dict]) -> list[dict]:
    """
    Знаходить аутлаєри серед прогнозів.
    Якщо джерело відхиляється від медіани ІНШИХ на > OUTLIER_THRESHOLD —
    позначає як аутлаєр і зменшує вагу до 0.05.

    Це вирішує проблему типу wttr.in=13°C vs Open-Meteo=17°C, NOAA=16°C.
    """
    if len(sources) < 2:
        for s in sources:
            s["outlier"] = False
            s["outlier_delta"] = 0.0
        return sources

    for s in sources:
        others = [o["temp_max"] for o in sources if o["source"] != s["source"]]
        if not others:
            s["outlier"] = False
            s["outlier_delta"] = 0.0
            continue
        med_others = _median(others)
        delta = abs(s["temp_max"] - med_others)
        s["outlier"]       = delta > OUTLIER_THRESHOLD
        s["outlier_delta"] = round(delta, 1)

    return sources


def compute_forecast(dt: datetime) -> dict:
    """Агрегація прогнозів з детекцією аутлаєрів і EGLC-поправкою."""
    raw_sources = get_all_sources(dt)
    if not raw_sources:
        return {"error": "Не вдалось отримати дані жодного джерела погоди"}

    raw_sources = detect_outliers(raw_sources)
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

    # Ваги з урахуванням аутлаєрів
    w_sum, w_total = 0.0, 0.0
    for s in enriched:
        w = SOURCE_WEIGHTS.get(s["source"], 0.30)
        if s.get("outlier"):
            w = 0.05   # майже ігноруємо аутлаєр
        w_sum   += s["corrected"] * w
        w_total += w

    weighted_avg = round(w_sum / w_total, 1) if w_total else 0.0

    # Медіана скоригованих (стійка до аутлаєрів)
    corrected_vals = sorted(s["corrected"] for s in enriched)
    median_val = _median(corrected_vals)

    # Фінал: зважений + медіана
    final     = round((weighted_avg + median_val) / 2, 1)
    final_int = round(final)

    # Довіра: якщо є аутлаєр — знижуємо; якщо всі збігаються — підвищуємо
    max_spread = max(s["corrected"] for s in enriched) - min(s["corrected"] for s in enriched)
    n_outliers = sum(1 for s in enriched if s.get("outlier"))
    if n_outliers > 0:
        confidence = "⚠️ низька (аутлаєр)"
    elif max_spread <= 0.5:
        confidence = "🟢 висока (всі збігаються)"
    elif max_spread <= 1.5:
        confidence = "🟡 середня"
    else:
        confidence = "🟠 помірна (розкид)"

    return {
        "sources":      enriched,
        "weighted_avg": weighted_avg,
        "median":       median_val,
        "final_temp":   final,
        "final_int":    final_int,
        "month":        month,
        "max_spread":   round(max_spread, 1),
        "confidence":   confidence,
        "n_outliers":   n_outliers,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  POLYMARKET API
# ══════════════════════════════════════════════════════════════════════════════

def build_slug(dt: datetime) -> str:
    return (
        f"highest-temperature-in-london-on-"
        f"{dt.strftime('%B').lower()}-{dt.day}-{dt.year}"
    )


def get_polymarket_data(dt: datetime) -> tuple[dict | None, list, str]:
    slug = build_slug(dt)
    link = f"https://polymarket.com/event/{slug}"
    data = _safe_get("https://gamma-api.polymarket.com/events", params={"slug": slug})
    if not data or not isinstance(data, list) or not data:
        return None, [], link
    return data[0], data[0].get("markets", []), link


def parse_all_outcomes(markets: list) -> dict:
    """
    {temperature_label: yes_probability_%}
    outcomePrices приходить як JSON-рядок "[\"0.32\",\"0.68\"]"
    prices[0] = YES (0..1), конвертуємо в %.
    """
    result = {}
    for m in markets:
        label = m.get("question", "").strip()
        if not label:
            outs = m.get("outcomes", "[]")
            if isinstance(outs, str):
                try:
                    outs = json.loads(outs)
                except Exception:
                    outs = []
            label = outs[0] if outs else "Unknown"

        prices_raw = m.get("outcomePrices", "[]")
        if isinstance(prices_raw, str):
            try:
                prices = json.loads(prices_raw)
            except Exception:
                prices = []
        else:
            prices = prices_raw

        if prices:
            try:
                result[label] = round(float(prices[0]) * 100, 1)
            except Exception:
                result[label] = 0.0
    return result


def find_outcome_for_temp(outcomes: dict, temp: int) -> tuple[str | None, float | None]:
    exact = f"{temp}°C"
    if exact in outcomes:
        return exact, outcomes[exact]
    for lbl, pct in outcomes.items():
        m = re.match(r"(\d+)\s*°C\s+or\s+below$", lbl, re.I)
        if m and temp <= int(m.group(1)):
            return lbl, pct
        m = re.match(r"(\d+)\s*°C\s+or\s+higher$", lbl, re.I)
        if m and temp >= int(m.group(1)):
            return lbl, pct
    best_lbl, best_pct, best_d = None, None, 999
    for lbl, pct in outcomes.items():
        m = re.match(r"(\d+)", lbl)
        if m:
            d = abs(int(m.group(1)) - temp)
            if d < best_d:
                best_d, best_lbl, best_pct = d, lbl, pct
    return best_lbl, best_pct


def polymarket_consensus(outcomes: dict, forecast_temp: int) -> str:
    """
    Порівнює прогноз бота з ринковим консенсусом Polymarket.
    Ринковий консенсус = температура з найвищою ймовірністю.
    """
    if not outcomes:
        return ""
    top_lbl, top_pct = max(outcomes.items(), key=lambda x: x[1])
    m = re.search(r"(\d+)", top_lbl)
    if not m:
        return ""
    market_temp = int(m.group(1))
    diff = forecast_temp - market_temp

    if diff == 0:
        return f"✅ Прогноз збігається з ринком ({market_temp}°C @ {top_pct}%)"
    elif abs(diff) == 1:
        return (
            f"🟡 Прогноз {forecast_temp}°C, ринок ставить на {market_temp}°C @ {top_pct}%"
            f" (різниця 1°C — в межах норми)"
        )
    else:
        direction = "вище" if diff > 0 else "нижче"
        return (
            f"⚠️ Прогноз {forecast_temp}°C, ринок ставить на {market_temp}°C @ {top_pct}%"
            f" (прогноз на {abs(diff)}°C {direction} — перевір джерела!)"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  DATE PARSING
# ══════════════════════════════════════════════════════════════════════════════

def parse_target_date(args: list) -> tuple[datetime | None, str | None]:
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
        "Формат: `DD.MM` або `DD.MM.YYYY`"
    )


# ══════════════════════════════════════════════════════════════════════════════
#  FORMATTERS
# ══════════════════════════════════════════════════════════════════════════════

def _days_label(dt: datetime) -> str:
    d = (dt.date() - datetime.utcnow().date()).days
    return {0: " (сьогодні)", 1: " (завтра)", 2: " (після завтра)"}.get(
        d, f" (через {d} дн.)"
    )


def fmt_weather(dt: datetime, fc: dict) -> str:
    lines = [
        f"🌡 *Прогноз EGLC — {dt.strftime('%d.%m.%Y')}{_days_label(dt)}*\n",
        "*3 джерела → max°C → EGLC поправка:*",
    ]
    for s in fc["sources"]:
        outlier_mark = f" ⚠️ аутлаєр (Δ{s['outlier_delta']}°C)" if s.get("outlier") else ""
        lines.append(
            f"  ▸ *{s['source']}*: {s['temp_max']}°C"
            f" {s['bias']:+.1f}°C → *{s['corrected']}°C*{outlier_mark}"
        )
        lines.append(f"     _↳ {s['accuracy']}_")

    lines.append(
        f"\n📍 *Прогноз EGLC:* *{fc['final_temp']}°C* → округлено *{fc['final_int']}°C*"
    )
    lines.append(
        f"   _(зважений: {fc['weighted_avg']}°C │ медіана: {fc['median']}°C"
        f" │ розкид: {fc['max_spread']}°C)_"
    )
    lines.append(f"   _Довіра: {fc['confidence']}_")
    return "\n".join(lines)


def fmt_polymarket(
    dt: datetime,
    outcomes: dict,
    tgt_lbl: str | None,
    tgt_pct: float | None,
    link: str,
    forecast_temp: int | None = None,
) -> str:
    lines = ["\n📊 *Polymarket — Highest Temp London:*"]
    if outcomes:
        top5 = sorted(outcomes.items(), key=lambda x: -x[1])[:5]
        for lbl, pct in top5:
            mark = " ◀️ *прогноз*" if lbl == tgt_lbl else ""
            lines.append(f"  `{lbl}`: {pct}%{mark}")

        if tgt_lbl:
            lines.append(f"\n🎯 Outcome: `{tgt_lbl}` = *{tgt_pct}%*")
            if tgt_pct is not None:
                if tgt_pct < 20:
                    lines.append("🟢 *< 20% — ДУЖЕ вигідно! Сильний сигнал купівлі*")
                elif tgt_pct < 38:
                    lines.append(f"🟢 *< 38% — Сигнал BUY* → `/buy {re.search(r'(\\d+)', tgt_lbl).group(1) if re.search(r'(\\d+)', tgt_lbl) else '??'}`")
                elif tgt_pct < 50:
                    lines.append("⏳ *38–50% — тримати / чекати*")
                elif tgt_pct < 65:
                    lines.append("🔴 *≥ 50% — розглянути фіксацію прибутку*")
                else:
                    lines.append("🔴 *≥ 65% — ринок переоцінює, ризик зростає*")

        # Консенсус прогнозу з ринком
        if forecast_temp is not None:
            consensus = polymarket_consensus(outcomes, forecast_temp)
            if consensus:
                lines.append(f"\n_{consensus}_")
    else:
        lines.append("  ⚠️ Ринок не знайдено або ще не відкрито")

    lines.append(f"\n🔗 {link}")
    return "\n".join(lines)


def fmt_buy_hint(tgt_lbl: str | None, dt: datetime | None = None) -> str:
    if not tgt_lbl:
        return ""
    m = re.search(r"(\d+)", tgt_lbl)
    num = m.group(1) if m else "??"
    date_part = f" {dt.strftime('%d.%m')}" if dt else ""
    return f"\n\n💡 Після купівлі: `/buy {num}{date_part}`"


# ══════════════════════════════════════════════════════════════════════════════
#  MULTI-DATE MONITORING STATE
# ══════════════════════════════════════════════════════════════════════════════
# monitoring[date_key] = {active, target_date, outcome_label, temp_int,
#                         buy_pct, alerted, poly_link}
monitoring: dict[str, dict] = {}


def _date_key(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def _get_or_none(date_key: str) -> dict | None:
    m = monitoring.get(date_key)
    return m if m and m.get("active") else None


async def _send_alert(
    bot: Bot, level: int, label: str, pct: float, link: str,
    date_str: str, buy_pct: float
) -> None:
    emoji = {40: "🟡", 50: "🟠", 60: "🔴", 70: "🔴", 80: "🚨", 90: "🚨"}.get(level, "📢")
    rec   = "🔴 *Час фіксувати прибуток!*" if level >= 50 else "⏳ Тримаємо далі"
    await bot.send_message(
        chat_id=CHAT_ID,
        parse_mode="Markdown",
        text=(
            f"{emoji} *АЛЕРТ {level}% — {date_str}*\n\n"
            f"Outcome `{label}` → *{pct}%*\n"
            f"_(куплено @ {buy_pct}%)_\n\n"
            f"{rec}\n\n"
            f"🔗 {link}\n"
            f"Закрити: /sell {date_str}"
        ),
    )


async def monitor_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Кожні 10 хв перевіряє всі активні позиції."""
    now_date = datetime.utcnow().date()
    to_close = []

    for dk, state in list(monitoring.items()):
        if not state.get("active"):
            continue

        dt = state["target_date"]

        # Дата минула — закриваємо
        if dt.date() < now_date:
            to_close.append(dk)
            await context.bot.send_message(
                chat_id=CHAT_ID,
                text=f"ℹ️ Моніторинг {dt.strftime('%d.%m.%Y')} завершено — дата минула.",
            )
            continue

        _, markets, link = get_polymarket_data(dt)
        if not markets:
            continue

        outcomes    = parse_all_outcomes(markets)
        current_pct = outcomes.get(state["outcome_label"])
        if current_pct is None:
            continue

        logger.info("Monitor %s: %s @ %.1f%%", dk, state["outcome_label"], current_pct)

        for level in ALERT_LEVELS:
            if current_pct >= level and level not in state["alerted"]:
                state["alerted"].append(level)
                await _send_alert(
                    context.bot, level, state["outcome_label"],
                    current_pct, link, dt.strftime("%d.%m"), state["buy_pct"]
                )

    for dk in to_close:
        monitoring[dk]["active"] = False


# ══════════════════════════════════════════════════════════════════════════════
#  AUTO BUY/MORNING SIGNAL
# ══════════════════════════════════════════════════════════════════════════════

async def _send_auto_check(bot: Bot, dt: datetime, label: str = "📅 Авто-звіт") -> None:
    """Надсилає повний звіт із BUY-сигналом якщо outcome < 38%."""
    fc = compute_forecast(dt)
    if "error" in fc:
        await bot.send_message(chat_id=CHAT_ID, text=f"⚠️ {fc['error']}")
        return

    _, markets, link = get_polymarket_data(dt)
    outcomes          = parse_all_outcomes(markets) if markets else {}
    tgt_lbl, tgt_pct = find_outcome_for_temp(outcomes, fc["final_int"]) if outcomes else (None, None)

    msg = (
        f"*{label} — {dt.strftime('%d.%m.%Y')}{_days_label(dt)}*\n\n"
        + fmt_weather(dt, fc)
        + "\n"
        + fmt_polymarket(dt, outcomes, tgt_lbl, tgt_pct, link, fc["final_int"])
        + fmt_buy_hint(tgt_lbl, dt)
    )

    # Якщо сигнал BUY — виділяємо
    if tgt_pct is not None and tgt_pct < BUY_SIGNAL_MAX_PCT:
        m = re.search(r"(\d+)", tgt_lbl or "")
        num = m.group(1) if m else "??"
        msg += (
            f"\n\n{'='*30}\n"
            f"🟢 *BUY СИГНАЛ!*\n"
            f"Outcome `{tgt_lbl}` = {tgt_pct}% < {BUY_SIGNAL_MAX_PCT}%\n"
            f"Після купівлі: `/buy {num} {dt.strftime('%d.%m')}`"
        )

    await bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="Markdown")


async def daily_job_14(context: ContextTypes.DEFAULT_TYPE) -> None:
    """14:00 Kyiv — прогноз на завтра."""
    tomorrow = (datetime.utcnow() + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    await _send_auto_check(context.bot, tomorrow, "⏰ 14:00 Kyiv")


async def daily_job_8(context: ContextTypes.DEFAULT_TYPE) -> None:
    """08:00 Kyiv — уточнений ранковий прогноз на сьогодні і завтра."""
    today    = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow = today + timedelta(days=1)

    # Завтра — основне
    await _send_auto_check(context.bot, tomorrow, "🌅 Ранковий прогноз")

    # Якщо є активні позиції — надіслати поточні ціни
    active = {dk: s for dk, s in monitoring.items() if s.get("active")}
    if active:
        lines = ["📊 *Поточні ціни по позиціях:*"]
        for dk, state in active.items():
            dt = state["target_date"]
            _, markets, link = get_polymarket_data(dt)
            outcomes    = parse_all_outcomes(markets) if markets else {}
            current_pct = outcomes.get(state["outcome_label"], "?")
            lines.append(
                f"  {dt.strftime('%d.%m')}: `{state['outcome_label']}` "
                f"куп. {state['buy_pct']}% → зараз {current_pct}%"
            )
        await context.bot.send_message(
            chat_id=CHAT_ID, text="\n".join(lines), parse_mode="Markdown"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  COMMAND HANDLERS
# ══════════════════════════════════════════════════════════════════════════════

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cid = update.effective_chat.id
    await update.message.reply_text(
        f"🤖 *London EGLC Temp Bot v3*\n"
        f"chat\\_id: `{cid}`\n\n"
        f"*Команди:*\n"
        f"`/check \\[DD\\.MM\\]` — прогноз \\+ Polymarket\n"
        f"`/check2` — завтра \\+ після завтра разом\n"
        f"`/poll \\[DD\\.MM\\]` — лише Polymarket\n"
        f"`/forecast \\[DD\\.MM\\]` — лише погода\n"
        f"`/buy <temp> \\[DD\\.MM\\]` — відкрити позицію\n"
        f"`/sell \\[DD\\.MM|all\\]` — закрити позицію\n"
        f"`/positions` — всі активні позиції\n"
        f"`/status \\[DD\\.MM\\]` — деталі позиції\n"
        f"`/history` — точність джерел\n"
        f"`/actual <src> <pred> <fact> \\[m\\]` — записати факт\n\n"
        f"⏰ Авто\\-звіти: *08:00* і *14:00* Київ\n"
        f"🔔 Алерти: 40→50→60→70→80→90%\n"
        f"🟢 BUY сигнал при outcome < {BUY_SIGNAL_MAX_PCT}%\n"
        f"🔴 SELL сигнал при outcome ≥ {SELL_SIGNAL_MIN_PCT}%",
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
    await _send_auto_check(context.bot, dt, "🔍 Запит")


async def cmd_check2(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Прогноз одразу для завтра і після завтра."""
    now = datetime.utcnow()
    tomorrow  = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    day_after = (now + timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)

    await update.message.reply_text("⏳ Збираю дані для 2 днів…")
    for dt in (tomorrow, day_after):
        await _send_auto_check(context.bot, dt, "🔍 Запит")


async def cmd_poll(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    dt, err = parse_target_date(context.args)
    if err:
        await update.message.reply_text(err, parse_mode="Markdown")
        return
    fc = compute_forecast(dt)
    if "error" in fc:
        await update.message.reply_text(f"⚠️ {fc['error']}")
        return
    _, markets, link = get_polymarket_data(dt)
    outcomes          = parse_all_outcomes(markets) if markets else {}
    tgt_lbl, tgt_pct = find_outcome_for_temp(outcomes, fc["final_int"]) if outcomes else (None, None)
    header = (
        f"📡 *Polymarket — {dt.strftime('%d.%m.%Y')}{_days_label(dt)}*\n"
        f"🎯 Прогноз EGLC: *{fc['final_int']}°C* ({fc['confidence']})\n"
    )
    await update.message.reply_text(
        header + fmt_polymarket(dt, outcomes, tgt_lbl, tgt_pct, link, fc["final_int"]),
        parse_mode="Markdown",
    )


async def cmd_forecast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    dt, err = parse_target_date(context.args)
    if err:
        await update.message.reply_text(err, parse_mode="Markdown")
        return
    await update.message.reply_text(
        f"🌤 Завантажую прогноз для *{dt.strftime('%d.%m.%Y')}*…", parse_mode="Markdown"
    )
    fc = compute_forecast(dt)
    if "error" in fc:
        await update.message.reply_text(f"⚠️ {fc['error']}")
        return
    await update.message.reply_text(fmt_weather(dt, fc), parse_mode="Markdown")


async def cmd_buy(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /buy <температура> [DD.MM]
    Відкриває нову позицію для моніторингу.
    Можна мати кілька позицій на різні дати.
    """
    if not context.args:
        await update.message.reply_text(
            "❓ `/buy 17` або `/buy 17 28.04`", parse_mode="Markdown"
        )
        return
    try:
        temp_int = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Температура — ціле число. Приклад: `/buy 17`", parse_mode="Markdown")
        return

    dt, err = parse_target_date(context.args[1:] if len(context.args) > 1 else [])
    if err:
        await update.message.reply_text(err, parse_mode="Markdown")
        return

    await update.message.reply_text(
        f"🔍 Шукаю outcome *{temp_int}°C* на {dt.strftime('%d.%m.%Y')}…",
        parse_mode="Markdown"
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

    dk = _date_key(dt)
    already = [l for l in ALERT_LEVELS if pct is not None and pct >= l]
    pending = [l for l in ALERT_LEVELS if l not in already]

    monitoring[dk] = {
        "active":        True,
        "target_date":   dt,
        "outcome_label": lbl,
        "temp_int":      temp_int,
        "buy_pct":       pct,
        "alerted":       already,
        "poly_link":     link,
    }

    total_active = sum(1 for s in monitoring.values() if s.get("active"))

    await update.message.reply_text(
        f"✅ *Позицію відкрито*\n\n"
        f"📅 {dt.strftime('%d.%m.%Y')}\n"
        f"🎯 Outcome: `{lbl}`\n"
        f"💰 Поточна ціна: *{pct}%*\n\n"
        f"🔔 Наступні алерти: {', '.join(str(l)+'%' for l in pending) or 'всі вже пройдені'}\n"
        f"📊 Всього активних позицій: {total_active}\n\n"
        f"🔗 {link}\n"
        f"Закрити: `/sell {dt.strftime('%d.%m')}`",
        parse_mode="Markdown",
    )


async def cmd_sell(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /sell          — закрити єдину активну (або останню)
    /sell 28.04    — закрити конкретну дату
    /sell all      — закрити всі
    """
    active = {dk: s for dk, s in monitoring.items() if s.get("active")}

    if not active:
        await update.message.reply_text("ℹ️ Немає активних позицій.")
        return

    # Визначаємо які закривати
    arg = context.args[0].lower() if context.args else ""

    if arg == "all":
        to_close = list(active.keys())
    elif arg:
        # Спробуємо розпізнати як дату
        dt_parsed, err = parse_target_date(context.args)
        if err or dt_parsed is None:
            await update.message.reply_text(
                f"❌ Не розпізнав дату: `{arg}`\n"
                "Формат: `/sell 28.04` або `/sell all`",
                parse_mode="Markdown"
            )
            return
        dk = _date_key(dt_parsed)
        if dk not in active:
            await update.message.reply_text(
                f"⚠️ Немає активної позиції на {dt_parsed.strftime('%d.%m.%Y')}.\n"
                f"Активні: {', '.join(s['target_date'].strftime('%d.%m') for s in active.values())}",
                parse_mode="Markdown"
            )
            return
        to_close = [dk]
    else:
        # Без аргументу — якщо одна позиція, закриваємо її; якщо більше — просимо уточнити
        if len(active) == 1:
            to_close = list(active.keys())
        else:
            dates = ", ".join(s["target_date"].strftime("%d.%m") for s in active.values())
            await update.message.reply_text(
                f"❓ Кілька активних позицій: {dates}\n"
                f"Вкажи дату: `/sell 28.04` або `/sell all`",
                parse_mode="Markdown"
            )
            return

    # Закриваємо і формуємо звіт
    lines = []
    for dk in to_close:
        state = monitoring[dk]
        dt    = state["target_date"]
        lbl   = state["outcome_label"]
        buy   = state["buy_pct"]

        _, markets, _ = get_polymarket_data(dt)
        outcomes    = parse_all_outcomes(markets) if markets else {}
        current_pct = outcomes.get(lbl)

        state["active"] = False

        profit = ""
        if isinstance(current_pct, float) and isinstance(buy, float) and buy > 0:
            roi    = round((current_pct / buy - 1) * 100, 1)
            profit = f" │ ROI: {roi:+.1f}%"

        lines.append(
            f"🛑 *{dt.strftime('%d.%m.%Y')}*: `{lbl}`\n"
            f"   Куп. {buy}% → зараз {current_pct}%{profit}"
        )

    await update.message.reply_text("\n\n".join(lines), parse_mode="Markdown")


async def cmd_positions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Всі активні позиції з поточними цінами."""
    active = {dk: s for dk, s in monitoring.items() if s.get("active")}
    if not active:
        await update.message.reply_text(
            "📊 Немає активних позицій.\n\nВідкрити: `/buy <temp> [DD.MM]`",
            parse_mode="Markdown"
        )
        return

    lines = [f"📊 *Активні позиції ({len(active)}):*\n"]
    for dk, state in sorted(active.items()):
        dt  = state["target_date"]
        lbl = state["outcome_label"]
        buy = state["buy_pct"]

        _, markets, link = get_polymarket_data(dt)
        outcomes    = parse_all_outcomes(markets) if markets else {}
        current_pct = outcomes.get(lbl, "?")

        alerted = state["alerted"]
        pending = [l for l in ALERT_LEVELS if l not in alerted]

        profit = ""
        if isinstance(current_pct, float) and isinstance(buy, float) and buy > 0:
            roi    = round((current_pct / buy - 1) * 100, 1)
            sign   = "📈" if roi >= 0 else "📉"
            profit = f" {sign} {roi:+.1f}%"

        lines.append(
            f"*{dt.strftime('%d.%m.%Y')}* {_days_label(dt)}\n"
            f"  `{lbl}` │ куп. {buy}% → *{current_pct}%*{profit}\n"
            f"  Наст. алерт: {pending[0]}%" if pending else "  Всі алерти надіслано"
        )
        lines.append(f"  [Polymarket]({link})\n")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Деталі однієї позиції."""
    dt, err = parse_target_date(context.args)
    if err:
        # Якщо одна позиція — показуємо її
        active = {dk: s for dk, s in monitoring.items() if s.get("active")}
        if len(active) == 1:
            dk    = list(active.keys())[0]
            state = active[dk]
            dt    = state["target_date"]
        else:
            await update.message.reply_text(
                f"❓ Вкажи дату: `/status 28.04`\n"
                f"Або перегляньте всі: /positions",
                parse_mode="Markdown"
            )
            return

    dk    = _date_key(dt)
    state = _get_or_none(dk)
    if not state:
        await update.message.reply_text(
            f"⚠️ Немає активної позиції на {dt.strftime('%d.%m.%Y')}.",
            parse_mode="Markdown"
        )
        return

    lbl = state["outcome_label"]
    _, markets, link = get_polymarket_data(dt)
    outcomes    = parse_all_outcomes(markets) if markets else {}
    current_pct = outcomes.get(lbl, "?")

    alerted = state["alerted"]
    pending = [l for l in ALERT_LEVELS if l not in alerted]

    await update.message.reply_text(
        f"👁 *Позиція {dt.strftime('%d.%m.%Y')}*\n\n"
        f"🎯 `{lbl}`\n"
        f"💰 Куплено @ {state['buy_pct']}%\n"
        f"📊 Зараз: *{current_pct}%*\n"
        f"🔔 Наступний алерт: {pending[0]}%" if pending else "🔔 Всі алерти надіслано"
        + f"\n✅ Надіслані: {', '.join(str(l)+'%' for l in alerted) or 'немає'}\n\n"
        f"🔗 {link}\n"
        f"Закрити: `/sell {dt.strftime('%d.%m')}`",
        parse_mode="Markdown",
    )


async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not SOURCE_STATS:
        await update.message.reply_text(
            "📊 Статистики ще немає.\n\n"
            "Записати факт: `/actual Open-Meteo 17.9 16.5 4`\n"
            "Після 5 записів бот використовує навчену поправку замість базової.",
            parse_mode="Markdown",
        )
        return
    lines = ["📊 *Точність джерел (EGLC bias)*\n"]
    for src, months in SOURCE_STATS.items():
        lines.append(f"*{src}:*")
        for mk, st in sorted(months.items(), key=lambda x: int(x[0])):
            mn = datetime(2000, int(mk), 1).strftime("%B")
            lines.append(
                f"  {mn}: MAE {st['mae']:.1f}°C, зміщ {st['bias']:+.1f}°C (n={st['n']})"
            )
        lines.append("")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_actual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /actual <source> <predicted> <actual_EGLC> [month]
    Приклад: /actual Open-Meteo 17.9 16.5 4
    """
    if len(context.args) < 3:
        await update.message.reply_text(
            "❓ `/actual <джерело> <прогноз°C> <факт EGLC°C> [місяць]`\n\n"
            "Приклад: `/actual Open-Meteo 17.9 16.5 4`\n"
            "Джерела: `Open-Meteo` `wttr.in` `Tomorrow.io` `NOAA/GFS`",
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
    n = SOURCE_STATS.get(src, {}).get(str(month), {}).get("n", 0)

    await update.message.reply_text(
        f"✅ Записано:\n"
        f"  `{src}` — прогноз {predicted}°C, факт EGLC {actual}°C\n"
        f"  Помилка: {predicted-actual:+.1f}°C\n\n"
        f"  Нова поправка (місяць {month}): *{new_bias:+.2f}°C* (n={n})\n"
        f"  {'✅ Навчена поправка активна' if n >= 5 else f'⏳ Ще {5-n} записів до активації'}\n\n"
        f"/history — вся статистика",
        parse_mode="Markdown",
    )


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    if not TOKEN:
        raise ValueError("BOT_TOKEN env var not set!")
    if not CHAT_ID:
        raise ValueError("CHAT_ID env var not set!")

    load_history()

    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start",     cmd_start))
    app.add_handler(CommandHandler("check",     cmd_check))
    app.add_handler(CommandHandler("check2",    cmd_check2))
    app.add_handler(CommandHandler("poll",      cmd_poll))
    app.add_handler(CommandHandler("forecast",  cmd_forecast))
    app.add_handler(CommandHandler("buy",       cmd_buy))
    app.add_handler(CommandHandler("sell",      cmd_sell))
    app.add_handler(CommandHandler("positions", cmd_positions))
    app.add_handler(CommandHandler("status",    cmd_status))
    app.add_handler(CommandHandler("history",   cmd_history))
    app.add_handler(CommandHandler("actual",    cmd_actual))

    jq = app.job_queue

    kyiv_8  = datetime.now(KYIV_TZ).replace(hour=8,  minute=0, second=0, microsecond=0)
    kyiv_14 = datetime.now(KYIV_TZ).replace(hour=14, minute=0, second=0, microsecond=0)

    jq.run_daily(daily_job_8,  time=kyiv_8.timetz(),  name="morning_8_kyiv")
    jq.run_daily(daily_job_14, time=kyiv_14.timetz(), name="daily_14_kyiv")
    jq.run_repeating(monitor_job, interval=600, first=30, name="price_monitor")

    logger.info(
        "Bot v3 started | 08:00 + 14:00 Kyiv | monitor every 10 min | "
        "Tomorrow.io: %s | outlier threshold: %.1f°C",
        "SET ✓" if TOMORROW_API_KEY else "NOT SET → NOAA/GFS",
        OUTLIER_THRESHOLD,
    )
    app.run_polling()


if __name__ == "__main__":
    main()
