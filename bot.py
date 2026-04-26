import os
import requests
import re
from datetime import datetime, timedelta

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

TOKEN = os.getenv("BOT_TOKEN")


# =========================
# DATE
# =========================

def get_date(mode="today"):
    now = datetime.utcnow()
    if mode == "tomorrow":
        now += timedelta(days=1)
    return now


def get_date_str(mode="today"):
    d = get_date(mode)
    return d.strftime("%B %d").replace(" 0", " ")  # April 6


def get_event_slug(mode="today"):
    d = get_date(mode)

    day = d.strftime("%-d") if "%" in "%-d" else str(int(d.strftime("%d")))
    month = d.strftime("%B").lower()

    return f"highest-temperature-in-london-on-{month}-{day}-2026"


def get_polymarket_url(mode="today"):
    return f"https://polymarket.com/event/{get_event_slug(mode)}"


# =========================
# FORECAST (simple stable)
# =========================

def get_forecast():
    try:
        url = "https://api.met.no/weatherapi/locationforecast/2.0/compact"
        params = {"lat": 51.5, "lon": -0.1}
        headers = {"User-Agent": "bot"}

        data = requests.get(url, params=params, headers=headers, timeout=5).json()

        temps = []
        for item in data["properties"]["timeseries"][:10]:
            temps.append(item["data"]["instant"]["details"]["air_temperature"])

        return round(max(temps))
    except:
        return None


# =========================
# POLYMARKET (STABLE)
# =========================

def extract_temp(q):
    match = re.search(r'(\d+)\s*°', q)
    if match:
        return int(match.group(1))
    return None


def get_prices(event_slug):
    url = f"https://gamma-api.polymarket.com/events?slug={event_slug}"

    try:
        data = requests.get(url, timeout=5).json()
    except:
        return []

    results = []

    # 🔥 підтримка ОБОХ форматів (це ключ)
    if isinstance(data, list) and len(data) > 0:
        if "markets" in data[0]:
            items = data[0]["markets"]
        else:
            items = data
    else:
        return []

    for item in items:
        q = item.get("question", "")

        temp = extract_temp(q)
        if temp is None:
            continue

        try:
            prices = item.get("outcomePrices", [])
            if len(prices) < 2:
                continue

            buy = float(prices[0]) * 100
            sell = float(prices[1]) * 100
        except:
            continue

        results.append({
            "temp": temp,
            "buy": buy,
            "sell": sell,
            "spread": abs(buy - sell),
            "liq": float(item.get("liquidity", 0))
        })

    return sorted(results, key=lambda x: x["temp"])


# =========================
# MESSAGE BUILDER
# =========================

def build_message(mode):
    date_str = get_date(mode).strftime("%Y-%m-%d")
    url = get_polymarket_url(mode)

    forecast = get_forecast()

    prices = get_prices(get_event_slug(mode))

    if not prices:
        return "❌ No market data"

    msg = f"📅 {mode.upper()} → {date_str}\n🔗 {url}\n\n"

    msg += "🌤 Forecast:\n"
    msg += f"MetNo: {forecast}\n\n"

    if forecast is None:
        return msg + "❌ No forecast"

    msg += f"🎯 Target (±1 from {forecast}°C):\n\n"

    targets = [forecast - 1, forecast, forecast + 1]

    found = False

    for t in targets:
        for p in prices:
            if p["temp"] == t:
                found = True
                msg += f"{t}°C → BUY {p['buy']:.1f}% | SELL {p['sell']:.1f}% | spread {p['spread']:.1f}%\n"

    if not found:
        msg += "⚠️ No matching temps in market\n"

    # best entry (мінімальний buy)
    candidates = [p for p in prices if p["temp"] in targets]

    if candidates:
        best = min(candidates, key=lambda x: x["buy"])
        msg += f"\n🔥 BEST: {best['temp']}°C ({best['buy']:.1f}%)"

    return msg


# =========================
# HANDLERS
# =========================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Use /today or /tomorrow")


async def today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = build_message("today")
    await update.message.reply_text(msg)


async def tomorrow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = build_message("tomorrow")
    await update.message.reply_text(msg)


# =========================
# MAIN
# =========================

def main():
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("today", today))
    app.add_handler(CommandHandler("tomorrow", tomorrow))

    app.run_polling()


if __name__ == "__main__":
    main()
