import os
import requests
from datetime import datetime, timedelta

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)

TOKEN = os.getenv("BOT_TOKEN")

# ===== CONFIG =====
CITY = "London"
LAT = 51.5072
LON = -0.1276

# ===== STORAGE =====
ACTIVE_TRADE = {
    "target": None
}


# =========================
# WEATHER (3 sources)
# =========================

def get_weather():
    try:
        url = f"https://api.open-meteo.com/v1/forecast?latitude={LAT}&longitude={LON}&daily=temperature_2m_max&timezone=UTC"
        data = requests.get(url, timeout=10).json()
        open_meteo = data["daily"]["temperature_2m_max"][1]
    except:
        open_meteo = None

    try:
        wt = requests.get(f"https://wttr.in/{CITY}?format=j1", timeout=10).json()
        wttr = float(wt["weather"][1]["maxtempC"])
    except:
        wttr = None

    try:
        url = f"https://api.met.no/weatherapi/locationforecast/2.0/compact?lat={LAT}&lon={LON}"
        headers = {"User-Agent": "weather-bot"}
        data = requests.get(url, headers=headers, timeout=10).json()
        met = data["properties"]["timeseries"][0]["data"]["instant"]["details"]["air_temperature"]
    except:
        met = None

    return {
        "open_meteo": open_meteo,
        "wttr": wttr,
        "met": met
    }


def apply_bias(temp, wind=3, clouds=50):
    if temp is None:
        return None

    bias = 0

    if wind > 5:
        bias -= 0.7
    if clouds > 70:
        bias -= 0.5

    # EGLC bias
    bias += 0.3

    return round(temp + bias, 1)


# =========================
# POLYMARKET
# =========================

def get_event_slug():
    tomorrow = datetime.utcnow() + timedelta(days=1)
    date_str = tomorrow.strftime("%B-%-d-%Y").lower()
    return f"highest-temperature-in-london-on-{date_str}"


def get_event():
    slug = get_event_slug()

    url = f"https://gamma-api.polymarket.com/events?slug={slug}"

    try:
        data = requests.get(url, timeout=10).json()
        if not data:
            return None

        return data[0]
    except:
        return None


def get_prices(event):
    result = {}

    markets = event.get("markets", [])
    if not isinstance(markets, list) or not markets:
        return result

    market = markets[0]

    outcomes = market.get("outcomes", [])
    prices = market.get("outcomePrices", [])

    if not isinstance(outcomes, list) or not isinstance(prices, list):
        return result

    if len(outcomes) != len(prices):
        return result

    for i in range(len(outcomes)):
        name = outcomes[i]

        try:
            price = float(prices[i]) * 100
        except:
            price = 0

        result[name] = round(price, 1)

    return result


# =========================
# ANALYSIS
# =========================

def analyze():
    weather = get_weather()

    temps = [t for t in weather.values() if t is not None]
    if not temps:
        return "❌ No weather data"

    avg = sum(temps) / len(temps)
    adjusted = apply_bias(avg)

    event = get_event()
    if not event:
        return "❌ Market not found"

    prices = get_prices(event)
    if not prices:
        return "❌ No prices"

    # find closest temp
    closest = min(prices.keys(), key=lambda x: abs(float(x.replace("°C", "")) - adjusted))
    prob = prices[closest]

    msg = f"""
📊 Weather:
OpenMeteo: {weather['open_meteo']}
WTTR: {weather['wttr']}
MET: {weather['met']}

🎯 Adjusted: {adjusted}°C
Best match: {closest}
Market: {prob}%

🔗 https://polymarket.com/event/{event['slug']}
"""

    if prob < 38:
        msg += "\n🟢 BUY SIGNAL"

    return msg, closest, prob


# =========================
# TELEGRAM
# =========================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    result = analyze()

    if isinstance(result, str):
        await update.message.reply_text(result)
        return

    msg, closest, prob = result
    await update.message.reply_text(msg)


async def buy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    result = analyze()

    if isinstance(result, str):
        await update.message.reply_text(result)
        return

    msg, closest, prob = result

    ACTIVE_TRADE["target"] = closest

    await update.message.reply_text(f"✅ Tracking {closest} ({prob}%)")


async def monitor(context: ContextTypes.DEFAULT_TYPE):
    if not ACTIVE_TRADE["target"]:
        return

    event = get_event()
    if not event:
        return

    prices = get_prices(event)
    target = ACTIVE_TRADE["target"]

    if target not in prices:
        return

    prob = prices[target]

    if prob >= 50:
        await context.bot.send_message(
            chat_id=context.job.chat_id,
            text=f"💰 SELL SIGNAL {target} now {prob}%"
        )
        ACTIVE_TRADE["target"] = None


# =========================
# MAIN
# =========================

def main():
    app = ApplicationBuilder().token(TOKEN).build()

    app.bot.delete_webhook(drop_pending_updates=True)

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("buy", buy))

    app.job_queue.run_repeating(monitor, interval=60)

    print("Bot started")
    app.run_polling()


if __name__ == "__main__":
    main()
