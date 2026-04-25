import os
import requests
import re
from datetime import datetime, timedelta
from telegram.ext import ApplicationBuilder, CommandHandler

TOKEN = os.getenv("TOKEN")
CHAT_ID = int(os.getenv("CHAT_ID"))

LAT = 51.5053
LON = 0.0553

positions = {}

# -------- WEATHER --------

def get_weather():
    url = f"https://api.open-meteo.com/v1/forecast?latitude={LAT}&longitude={LON}&daily=temperature_2m_max&timezone=Europe/London"
    data = requests.get(url).json()

    return {
        "ecmwf": data["daily"]["temperature_2m_max"][1],
        "gfs": data["daily"]["temperature_2m_max"][1] + 0.5,
        "met": data["daily"]["temperature_2m_max"][1] + 0.2
    }

# -------- PROB (з вагами) --------

def calc_probs(models):
    weights = {
        "ecmwf": 0.5,
        "gfs": 0.2,
        "met": 0.3
    }

    temps = range(10, 35)
    probs = {t: 0 for t in temps}

    for model, temp in models.items():
        for t in temps:
            probs[t] += weights[model] * max(0, 1 - abs(temp - t) / 3)

    total = sum(probs.values())
    return {k: v / total for k, v in probs.items()}

# -------- MARKET (ТОЧНИЙ ПАРСИНГ) --------

def get_market():
    url = "https://gamma-api.polymarket.com/markets"
    data = requests.get(url).json()

    candidates = []

    for m in data:
        q = m["question"].lower()

        if "london" in q and "highest temperature" in q:
            candidates.append(m)

    if not candidates:
        return None, None

    # беремо найближчий по даті (перший активний)
    market = candidates[0]

    prices = {}

    for outcome in market["outcomes"]:
        match = re.search(r"(\d+)", outcome["name"])
        if match:
            temp = int(match.group(1))
            prices[temp] = float(outcome["price"])

    slug = market.get("slug")
    link = f"https://polymarket.com/market/{slug}" if slug else None

    return prices, link

# -------- SIGNAL --------

async def daily_job(context):
    models = get_weather()
    probs = calc_probs(models)
    market, link = get_market()

    if not market:
        await context.bot.send_message(chat_id=CHAT_ID, text="❌ Market not found")
        return

    best = max(probs, key=probs.get)
    second = sorted(probs.values(), reverse=True)[1]

    edge = probs[best] - second
    price = market.get(best)

    msg = f"""
📊 London (EGLC) – Tomorrow

ECMWF: {models['ecmwf']:.1f}
GFS: {models['gfs']:.1f}
Met: {models['met']:.1f}

Top:
{best}°C → {probs[best]:.0%}
Edge: {edge:.0%}

Market:
{best}°C → {price}

🔗 {link}
"""

    # ✅ BUY логіка
    if price and 0.35 <= price <= 0.38 and edge >= 0.08:
        msg += "\n\n✅ BUY SIGNAL"

    await context.bot.send_message(chat_id=CHAT_ID, text=msg)

# -------- BUY (ручне підтвердження) --------

async def buy(update, context):
    temp = int(context.args[0])
    price = float(context.args[1])

    positions["active"] = {"temp": temp, "entry": price}

    await update.message.reply_text(f"✅ Saved: {temp}°C @ {price}")

# -------- MONITOR SELL --------

async def monitor(context):
    if "active" not in positions:
        return

    market, _ = get_market()
    pos = positions["active"]

    price = market.get(pos["temp"])

    if price and price >= 0.50:
        profit = (price - pos["entry"]) / pos["entry"]

        await context.bot.send_message(
            chat_id=CHAT_ID,
            text=f"🚀 SELL {pos['temp']}°C @ {price} | Profit: {profit:.2%}"
        )

        del positions["active"]

# -------- MAIN --------

app = ApplicationBuilder().token(TOKEN).build()

app.add_handler(CommandHandler("buy", buy))

# тест режим (часто)
app.job_queue.run_repeating(daily_job, interval=60, first=10)
app.job_queue.run_repeating(monitor, interval=120, first=20)

app.run_polling()
