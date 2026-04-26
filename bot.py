"""
London Temperature Polymarket Bot
==================================
Логіка:
  14:00 Kyiv (UTC+3) → прогноз погоди EGLC на завтра + аналіз Polymarket
  Якщо ймовірність потрібного outcome < 38% → алерт "КУПУЙ"
  Моніторинг кожні 15 хв → якщо ≥ 50% → алерт "ПРОДАВАЙ"

Встановлення:
  pip install python-telegram-bot==20.* requests pytz

Env vars:
  BOT_TOKEN   — токен Telegram бота
  CHAT_ID     — ваш chat_id (отримати через /start)
"""

import os
import re
import logging
import requests
import pytz
from datetime import datetime, timedelta
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

TOKEN   = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")   # числовий chat_id куди слати алерти

KYIV_TZ = pytz.timezone("Europe/Kiev")

# EGLC coords (London City Airport)
EGLC_LAT = 51.5048
EGLC_LON = 0.0495

# ─────────────────────────────────────────────
# SLUG helper
# ─────────────────────────────────────────────
def build_slug(target_date: datetime) -> str:
    """Будує slug для Polymarket на вказану дату."""
    month = target_date.strftime("%B").lower()
    day   = target_date.day
    year  = target_date.year
    return f"highest-temperature-in-london-on-{month}-{day}-{year}"


# ─────────────────────────────────────────────
# POLYMARKET
# ─────────────────────────────────────────────
def get_polymarket_event(target_date: datetime):
    """Повертає (event_dict, markets_list, url) або (None, None, None)."""
    slug = build_slug(target_date)
    url  = f"https://gamma-api.polymarket.com/events?slug={slug}"
    try:
        r = requests.get(url, timeout=10)
        data = r.json()
        if not data:
            return None, None, None
        event = data[0]
        markets = event.get("markets", [])
        link = f"https://polymarket.com/event/{slug}"
        return event, markets, link
    except Exception as e:
        logger.error("Polymarket API error: %s", e)
        return None, None, None


def parse_outcome_price(markets: list, temp_c: int) -> dict:
    """
    Шукає outcome для конкретної температури (ціле °C) або діапазону.
    Повертає dict {outcome_label: price_pct} для всіх outcomes.
    """
    result = {}
    for m in markets:
        # outcome — зазвичай label типу "18°C", "15°C or below", "25°C or higher"
        outcomes  = m.get("outcomes", "[]")
        prices    = m.get("outcomePrices", "[]")

        # може бути рядок JSON
        if isinstance(outcomes, str):
            import json
            try:
                outcomes = json.loads(outcomes)
                prices   = json.loads(prices)
            except Exception:
                continue

        for label, price in zip(outcomes, prices):
            try:
                pct = round(float(price) * 100, 1)
            except Exception:
                pct = 0.0
            result[label] = pct
    return result


def find_best_outcome(outcome_prices: dict, predicted_temp: int) -> tuple:
    """
    Знаходить outcome, що відповідає прогнозованій температурі.
    Повертає (label, price_pct).
    """
    # Точна відповідність "18°C"
    exact = f"{predicted_temp}°C"
    if exact in outcome_prices:
        return exact, outcome_prices[exact]

    # Перевіряємо "or below" і "or higher"
    for label, pct in outcome_prices.items():
        # "15°C or below"
        m = re.match(r"(\d+)°C or below", label)
        if m and predicted_temp <= int(m.group(1)):
            return label, pct
        # "25°C or higher"
        m = re.match(r"(\d+)°C or higher", label)
        if m and predicted_temp >= int(m.group(1)):
            return label, pct

    # Найближчий
    best_label, best_pct = None, None
    best_diff = 999
    for label, pct in outcome_prices.items():
        m = re.match(r"(\d+)", label)
        if m:
            diff = abs(int(m.group(1)) - predicted_temp)
            if diff < best_diff:
                best_diff = diff
                best_label = label
                best_pct   = pct
    return best_label, best_pct


# ─────────────────────────────────────────────
# WEATHER — Open-Meteo (безкоштовно, без ключа)
# ─────────────────────────────────────────────
def fetch_openmeteo(target_date: datetime) -> dict | None:
    """
    Повертає прогноз Open-Meteo для EGLC на target_date.
    Включає temperature_2m_max для конкретного дня.
    """
    date_str = target_date.strftime("%Y-%m-%d")
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={EGLC_LAT}&longitude={EGLC_LON}"
        f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max"
        f"&timezone=Europe/London"
        f"&start_date={date_str}&end_date={date_str}"
    )
    try:
        r = requests.get(url, timeout=10)
        data = r.json()
        daily = data.get("daily", {})
        if not daily.get("temperature_2m_max"):
            return None
        return {
            "source":   "Open-Meteo",
            "temp_max": daily["temperature_2m_max"][0],
            "temp_min": daily["temperature_2m_min"][0],
            "precip":   daily["precipitation_sum"][0],
            "wind":     daily["windspeed_10m_max"][0],
        }
    except Exception as e:
        logger.error("Open-Meteo error: %s", e)
        return None


def fetch_wttr(target_date: datetime) -> dict | None:
    """
    wttr.in — ще одне безкоштовне джерело погоди.
    Використовуємо JSON API.
    """
    # wttr.in не дає прогноз на конкретний завтрашній день точно,
    # але дає 3-денний прогноз
    try:
        r = requests.get(
            "https://wttr.in/London+City+Airport?format=j1",
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        data = r.json()
        # weather[0]=сьогодні, weather[1]=завтра
        tomorrow_idx = (target_date.date() - datetime.utcnow().date()).days
        if tomorrow_idx < 0 or tomorrow_idx >= len(data.get("weather", [])):
            return None
        w = data["weather"][tomorrow_idx]
        return {
            "source":   "wttr.in",
            "temp_max": float(w["maxtempC"]),
            "temp_min": float(w["mintempC"]),
        }
    except Exception as e:
        logger.error("wttr.in error: %s", e)
        return None


# ─────────────────────────────────────────────
# EGLC HISTORICAL BIAS CORRECTION
# ─────────────────────────────────────────────
# EGLC (London City Airport) — міський острів тепла.
# За спостереженнями, EGLC зазвичай на ~0.5–1.5°C тепліше ніж сітковий
# прогноз для Лондона (залежно від сезону та ситуації).
# Тут використовуємо консервативну поправку +0.8°C (весна/літо).
EGLC_BIAS = {
    1: +0.3,   # Січень
    2: +0.3,
    3: +0.5,
    4: +0.8,   # Квітень
    5: +1.0,
    6: +1.2,
    7: +1.3,
    8: +1.3,
    9: +1.0,
    10: +0.7,
    11: +0.4,
    12: +0.3,
}

def apply_eglc_bias(raw_temp: float, month: int) -> float:
    """Застосовує статистичну поправку на EGLC urban heat island."""
    bias = EGLC_BIAS.get(month, 0.5)
    return raw_temp + bias


# ─────────────────────────────────────────────
# FORECAST AGGREGATION
# ─────────────────────────────────────────────
def get_forecast(target_date: datetime) -> dict:
    """
    Збирає прогнози з кількох джерел, застосовує EGLC-поправку,
    повертає зважений прогноз.
    """
    sources = []
    om = fetch_openmeteo(target_date)
    if om:
        sources.append(om)

    wt = fetch_wttr(target_date)
    if wt:
        sources.append(wt)

    if not sources:
        return {"error": "No weather data available"}

    month = target_date.month
    temps_raw      = [s["temp_max"] for s in sources]
    temps_corrected = [round(apply_eglc_bias(t, month), 1) for t in temps_raw]

    avg_raw  = round(sum(temps_raw) / len(temps_raw), 1)
    avg_corr = round(sum(temps_corrected) / len(temps_corrected), 1)
    predicted_int = round(avg_corr)   # EGLC фіксує цілі °C

    return {
        "sources":        sources,
        "avg_raw":        avg_raw,
        "eglc_bias":      EGLC_BIAS.get(month, 0.5),
        "avg_corrected":  avg_corr,
        "predicted_int":  predicted_int,   # саме це порівнюємо з Polymarket
    }


# ─────────────────────────────────────────────
# FORMAT MESSAGE
# ─────────────────────────────────────────────
def format_forecast_message(
    target_date: datetime,
    forecast: dict,
    outcome_prices: dict,
    best_label: str,
    best_pct: float,
    poly_link: str,
    alert_type: str = None,  # "BUY", "SELL", None
) -> str:
    date_str = target_date.strftime("%d %B %Y")
    lines = [f"🌡️ *London EGLC — {date_str}*\n"]

    # Джерела погоди
    for s in forecast.get("sources", []):
        lines.append(
            f"• {s['source']}: {s['temp_max']}°C → EGLC ≈"
            f" {round(apply_eglc_bias(s['temp_max'], target_date.month), 1)}°C"
        )

    lines.append(
        f"\n📍 *Прогноз EGLC (з поправкою +{forecast['eglc_bias']}°C):*"
        f" *{forecast['avg_corrected']}°C* → округлено *{forecast['predicted_int']}°C*"
    )

    # Polymarket
    if outcome_prices:
        lines.append(f"\n📊 *Polymarket — цільовий outcome:* `{best_label}` = *{best_pct}%*")

        # Топ-3 outcomes за ймовірністю
        top3 = sorted(outcome_prices.items(), key=lambda x: -x[1])[:4]
        lines.append("*Топ outcomes:*")
        for lbl, pct in top3:
            marker = " ◀️" if lbl == best_label else ""
            lines.append(f"  `{lbl}`: {pct}%{marker}")
    else:
        lines.append("\n⚠️ Polymarket: дані відсутні")

    lines.append(f"\n🔗 {poly_link}")

    # Алерти
    if alert_type == "BUY":
        lines.append(
            f"\n🟢 *СИГНАЛ: КУПУЙ YES на `{best_label}`*\n"
            f"Ймовірність {best_pct}% < 38% — вигідно купувати!"
        )
    elif alert_type == "SELL":
        lines.append(
            f"\n🔴 *СИГНАЛ: МОЖНА ПРОДАВАТИ `{best_label}`*\n"
            f"Ймовірність досягла {best_pct}% ≥ 50%!"
        )

    return "\n".join(lines)


# ─────────────────────────────────────────────
# CORE CHECK LOGIC
# ─────────────────────────────────────────────
async def run_daily_check(bot: Bot, force: bool = False, reply_chat_id: str = None):
    """
    Основна перевірка: прогноз + Polymarket.
    Надсилає алерт якщо ймовірність < 38%.
    reply_chat_id — куди відповідати. Якщо None — використовує CHAT_ID з env.
    """
    send_to = reply_chat_id or CHAT_ID

    tomorrow = datetime.utcnow() + timedelta(days=1)
    tomorrow = tomorrow.replace(hour=0, minute=0, second=0, microsecond=0)

    forecast = get_forecast(tomorrow)
    if "error" in forecast:
        await bot.send_message(chat_id=send_to, text=f"⚠️ {forecast['error']}")
        return

    event, markets, link = get_polymarket_event(tomorrow)
    if not event:
        await bot.send_message(
            chat_id=send_to,
            text=f"⚠️ Polymarket ринок не знайдено для {build_slug(tomorrow)}"
        )
        return

    outcome_prices = parse_outcome_price(markets, forecast["predicted_int"])
    best_label, best_pct = find_best_outcome(outcome_prices, forecast["predicted_int"])

    if best_pct is None:
        await bot.send_message(chat_id=send_to, text="⚠️ Не вдалось знайти відповідний outcome на Polymarket")
        return

    # Визначаємо алерт
    alert_type = None
    if best_pct < 38.0:
        alert_type = "BUY"

    msg = format_forecast_message(
        tomorrow, forecast, outcome_prices, best_label, best_pct, link, alert_type
    )

    await bot.send_message(chat_id=send_to, text=msg, parse_mode="Markdown")
    logger.info("Check sent to %s. Outcome: %s @ %.1f%%", send_to, best_label, best_pct)

    # Зберігаємо стан для моніторингу продажу
    return best_label, best_pct


# ─────────────────────────────────────────────
# SELL MONITOR (кожні 15 хвилин після купівлі)
# ─────────────────────────────────────────────
# Зберігаємо глобально що ми "тримаємо"
watching: dict = {}  # {target_date_str: {"label": ..., "buy_pct": ...}}

async def monitor_sell(context: ContextTypes.DEFAULT_TYPE):
    """Job: кожні 15 хвилин перевіряє чи пора продавати."""
    bot = context.bot
    tomorrow = datetime.utcnow() + timedelta(days=1)
    tomorrow = tomorrow.replace(hour=0, minute=0, second=0, microsecond=0)
    date_key = tomorrow.strftime("%Y-%m-%d")

    if date_key not in watching:
        return

    watch = watching[date_key]
    target_label = watch["label"]

    _, markets, link = get_polymarket_event(tomorrow)
    if not markets:
        return

    outcome_prices = parse_outcome_price(markets, 0)
    current_pct = outcome_prices.get(target_label)
    if current_pct is None:
        return

    logger.info("Monitor: %s @ %.1f%%", target_label, current_pct)

    if current_pct >= 50.0:
        msg = format_forecast_message(
            tomorrow, {"sources": [], "avg_corrected": 0, "eglc_bias": 0, "predicted_int": 0},
            outcome_prices, target_label, current_pct, link, alert_type="SELL"
        )
        await bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="Markdown")
        # Видаляємо з watch після сигналу
        del watching[date_key]


# ─────────────────────────────────────────────
# SCHEDULED JOB: 14:00 Kyiv
# ─────────────────────────────────────────────
async def daily_job(context: ContextTypes.DEFAULT_TYPE):
    """Запускається о 14:00 за Києвом."""
    bot = context.bot
    result = await run_daily_check(bot)
    if result:
        best_label, best_pct = result
        if best_pct < 38.0:
            tomorrow = datetime.utcnow() + timedelta(days=1)
            date_key = tomorrow.strftime("%Y-%m-%d")
            watching[date_key] = {"label": best_label, "buy_pct": best_pct}
            logger.info("Started watching %s for sell signal", best_label)


# ─────────────────────────────────────────────
# TELEGRAM COMMANDS
# ─────────────────────────────────────────────
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await update.message.reply_text(
        f"👋 Бот запущено!\n"
        f"Ваш chat_id: `{chat_id}`\n\n"
        f"*Команди:*\n"
        f"/check — 🔍 повний прогноз EGLC + Polymarket зараз\n"
        f"/poll — 📡 перевірити поточну ціну outcome (без прогнозу)\n"
        f"/status — 👁 що зараз моніториться для продажу\n"
        f"/watch — ➕ додати поточний прогноз у моніторинг продажу\n\n"
        f"⏰ Автоматична перевірка щодня о 14:00 Київ",
        parse_mode="Markdown",
    )


async def cmd_poll(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Швидка перевірка поточної ціни outcome на Polymarket без прогнозу погоди."""
    await update.message.reply_text("📡 Перевіряю ціни на Polymarket...")

    tomorrow = datetime.utcnow() + timedelta(days=1)
    tomorrow = tomorrow.replace(hour=0, minute=0, second=0, microsecond=0)

    # Беремо прогноз тільки щоб знати цільову температуру
    forecast = get_forecast(tomorrow)
    if "error" in forecast:
        await update.message.reply_text(f"⚠️ Прогноз погоди недоступний: {forecast['error']}")
        return

    _, markets, link = get_polymarket_event(tomorrow)
    if not markets:
        await update.message.reply_text("⚠️ Polymarket ринок не знайдено")
        return

    outcome_prices = parse_outcome_price(markets, forecast["predicted_int"])
    best_label, best_pct = find_best_outcome(outcome_prices, forecast["predicted_int"])
    date_str = tomorrow.strftime("%d %B %Y")

    # Топ outcomes
    top = sorted(outcome_prices.items(), key=lambda x: -x[1])[:5]
    lines = [f"📡 *Polymarket — {date_str}*\n"]
    lines.append(f"🎯 Прогноз EGLC: *{forecast['predicted_int']}°C*\n")
    lines.append("*Поточні ймовірності:*")
    for lbl, pct in top:
        marker = " ◀️ *ціль*" if lbl == best_label else ""
        lines.append(f"  `{lbl}`: *{pct}%*{marker}")
    lines.append(f"\n🔗 {link}")

    if best_pct is not None:
        if best_pct < 38.0:
            lines.append(f"\n🟢 *{best_pct}% < 38% — можна купувати!*")
        elif best_pct >= 50.0:
            lines.append(f"\n🔴 *{best_pct}% ≥ 50% — можна продавати!*")
        else:
            lines.append(f"\n⏳ *{best_pct}% — чекаємо (38–50%)*")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ручна перевірка — відповідає прямо в чат звідки прийшла команда.
    Завжди додає знайдений outcome у моніторинг продажу."""
    user_chat_id = str(update.effective_chat.id)
    await update.message.reply_text("🔍 Перевіряю прогноз і Polymarket...")
    result = await run_daily_check(context.bot, force=True, reply_chat_id=user_chat_id)
    if result:
        best_label, best_pct = result
        tomorrow = datetime.utcnow() + timedelta(days=1)
        date_key = tomorrow.strftime("%Y-%m-%d")
        watching[date_key] = {"label": best_label, "buy_pct": best_pct}
        logger.info("Manual /check: watching %s @ %.1f%%", best_label, best_pct)
        await update.message.reply_text(
            f"👁 Моніторинг увімкнено: `{best_label}`\n"
            f"Сигнал продажу надійде при ≥ 50%",
            parse_mode="Markdown",
        )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not watching:
        await update.message.reply_text("👁 Нічого не моніторю зараз.")
        return
    lines = ["👁 *Моніторинг:*"]
    for date_key, w in watching.items():
        lines.append(f"• {date_key}: `{w['label']}` (куплено @ {w['buy_pct']}%)")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_watch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Примусово додати поточний outcome до моніторингу продажу."""
    tomorrow = datetime.utcnow() + timedelta(days=1)
    tomorrow = tomorrow.replace(hour=0, minute=0, second=0, microsecond=0)
    forecast = get_forecast(tomorrow)
    if "error" in forecast:
        await update.message.reply_text("❌ Помилка прогнозу")
        return
    _, markets, _ = get_polymarket_event(tomorrow)
    if not markets:
        await update.message.reply_text("❌ Ринок не знайдено")
        return
    outcome_prices = parse_outcome_price(markets, forecast["predicted_int"])
    best_label, best_pct = find_best_outcome(outcome_prices, forecast["predicted_int"])
    date_key = tomorrow.strftime("%Y-%m-%d")
    watching[date_key] = {"label": best_label, "buy_pct": best_pct}
    await update.message.reply_text(
        f"✅ Додано до моніторингу:\n`{best_label}` @ {best_pct}%\nСигнал продажу при ≥ 50%",
        parse_mode="Markdown",
    )


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    if not TOKEN:
        raise ValueError("BOT_TOKEN env var not set!")
    if not CHAT_ID:
        raise ValueError("CHAT_ID env var not set! Run /start to get your chat_id")

    app = ApplicationBuilder().token(TOKEN).build()

    # Commands
    app.add_handler(CommandHandler("start",  cmd_start))
    app.add_handler(CommandHandler("check",  cmd_check))
    app.add_handler(CommandHandler("poll",   cmd_poll))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("watch",  cmd_watch))

    # Scheduled jobs
    jq = app.job_queue

    # 14:00 Kyiv = 11:00 UTC (взимку) або 11:00 UTC (UTC+3)
    # Використовуємо timezone-aware time
    kyiv_14 = datetime.now(KYIV_TZ).replace(hour=14, minute=0, second=0, microsecond=0)
    jq.run_daily(
        daily_job,
        time=kyiv_14.timetz(),
        name="daily_14_kyiv",
    )

    # Моніторинг продажу кожні 15 хвилин
    jq.run_repeating(
        monitor_sell,
        interval=900,   # 15 хв
        first=60,
        name="sell_monitor",
    )

    logger.info("Bot started. Daily check at 14:00 Kyiv. Sell monitor every 15 min.")
    app.run_polling()


if __name__ == "__main__":
    main()
