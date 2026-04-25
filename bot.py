import os
import json
import asyncio
import requests
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

BOT_TOKEN = os.getenv("BOT_TOKEN")

EVENT_SLUG = "highest-temperature-in-london-on-april-27-2026"

# =========================
# WEATHER (3 SOURCES)
# =========================

def get_weather():
    temps = []

    try:
        # Open-Meteo
        r = requests.get(
            "https://api.open-meteo.com/v1/forecast?latitude=51.5&longitude=0.0&daily=temperature_2m_max&timezone=UTC"
        ).json()
        temps.append(r["daily"]["temperature_2m_max"][1])
    except:
        pass

    try:
        # Met.no
        r = requests.get(
            "https://api.met.no/weatherapi/locationforecast/2.0/compact?lat=51.5&lon=0.0"
        ).json()
        temps.append(r["properties"]["timeseries"][12]["data"]["instant"]["details"]["air_temperature"])
    except:
        pass

    try:
        # wttr.in (fallback)
        r = requests.get("https://wttr.in/London?format=j1").json()
        temps.append(float(r["weather"][1]["maxtempC"]))
    except:
        pass

    if not temps:
        return None

    # Weighted (ECMWF-like bias)
    avg = sum(temps) / len(temps)

    # EGLC bias (-1°C)
    return round(avg - 1, 1)


# =========================
# POLYMARKET
# =========================

def get_event():
    url = f"https://gamma-api.polymarket.com/events?slug={EVENT_SLUG}"
    r = requests.get(url)
    return r.json()[0]


def get_prices(event):
    result = {}

    for m in event["markets"]:
        name = m.get("groupItemTitle")

        bid = m.get("bestBid")
        ask = m.get("bestAsk")
        liq = m.get("liquidityNum", 0)

        if not name or bid is None or ask is None:
            continue

        if bid == 0 or ask == 0:
            continue

        spread = ask - bid

        result[name] = {
            "buy": round(ask * 100, 1),
            "sell": round(bid * 100, 1),
            "spread": round(spread * 100, 2),
            "liq": liq
        }

    return result


# =========================
# PROBABILISTIC PICK
# =========================

def pick_market(prices, forecast):
    best = None
    best_score = 999

    for temp_str, data in prices.items():
        try:
            temp = int(temp_str.replace("°C", ""))
        except:
            continue

        distance = abs(temp - forecast)

        # 🔥 спред лише впливає, але не блокує
        spread_penalty = data["spread"] * 0.3

        # ліквідність — бонус
        liq_bonus = -1 if data["liq"] > 3000 else 0

        score = distance + spread_penalty + liq_bonus

        if score < best_score:
            best_score = score
            best = (temp_str, data)

    return best


# =========================
# TELEGRAM COMMANDS
# =========================

active_trade = None


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    forecast = get_weather()
    event = get_event()
    prices = get_prices(event)

    if not forecast:
        await update.message.reply_text("❌ Weather error")
        return

    if not prices:
        await update.message.reply_text("❌ No prices")
        return

    market = pick_market(prices, forecast)

    if not market:
        await update.message.reply_text("❌ No valid market")
        return

    temp, data = market

msg = f"""
🌤 Forecast (EGLC adj): {forecast}°C

🎯 Best Market: {temp}
BUY: {data['buy']}%
SELL: {data['sell']}%
Spread: {data['spread']}%
Liquidity: {round(data['liq'])}
"""

    if data["buy"] < 38:
        msg += "\n🔥 SIGNAL: BUY (<38%)"

    await update.message.reply_text(msg)


# =========================
# BUY + MONITOR
# =========================

async def buy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global active_trade

    forecast = get_weather()
    event = get_event()
    prices = get_prices(event)

    market = pick_market(prices, forecast)

    if not market:
        await update.message.reply_text("❌ No market")
        return

    temp, data = market

    active_trade = {
        "temp": temp
    }

    await update.message.reply_text(f"✅ Tracking {temp}")


async def monitor(context: ContextTypes.DEFAULT_TYPE):
    global active_trade

    if not active_trade:
        return

    event = get_event()
    prices = get_prices(event)

    temp = active_trade["temp"]

    if temp not in prices:
        return

    price = prices[temp]["sell"]

    if price >= 50:
        await context.bot.send_message(
            chat_id=context.job.chat_id,
            text=f"💰 SELL SIGNAL {temp} @ {price}%"
        )
        active_trade = None


# =========================
# MAIN
# =========================

async def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("buy", buy))

    app.job_queue.run_repeating(monitor, interval=60, first=10)

    print("🚀 Bot started")
    await app.run_polling()


if __name__ == "__main__":
    asyncio.run(main())
