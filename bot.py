import os
import requests
import re
from telegram.ext import ApplicationBuilder, CommandHandler
from apscheduler.schedulers.asyncio import AsyncIOScheduler

TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

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

# -------- PROB --------

def calc_probs(models):
    temps = range(15, 30)
    probs = {t: 0 for t in temps}

    for temp in models.values():
        for t in temps:
            probs[t] += max(0, 1 - abs(temp - t) / 3)

    total = sum(probs.values())
    return {k: v / total for k, v in probs.items()}

# -------- MARKET --------

def get_market():
    url = "https://gamma-api.polymarket.com/markets"
    data = requests.get(url).json()

    prices = {}

    for m in data:
        q = m["question"].lower()

        if "london" in q and "temperature" in q:
            for outcome in m["outcomes"]:
                match = re.search(r"(\d+)", outcome["name"])
                if match:
                    temp = int(match.group(1))
                    prices[temp] = float(outcome["price"])

    return prices

# -------- SIGNAL --------

async def daily_job(app):
    models = get_weather()
    probs = calc_probs(models)
    market = get_market()

    best = max(probs, key=probs.get)
    second = sorted(probs.values(), reverse=True)[1]

    price = market.get(best, None)

    msg = f"""
📊 London (EGLC) – Tomorrow

ECMWF: {models['ecmwf']:.1f}
GFS: {models['gfs']:.1f}
Met: {models['met']:.1f}

Top:
{best}°C → {probs[best]:.0%}

Market:
{best}°C → {price}
"""

    if price and price <= 0.38 and (probs[best] - second) >= 0.10:
        msg += "\n✅ BUY SIGNAL"

    await app.bot.send_message(chat_id=CHAT_ID, text=msg)

# -------- BUY --------

async def buy(update, context):
    temp = int(context.args[0])
    price = float(context.args[1])

    positions["active"] = {"temp": temp, "entry": price}

    await update.message.reply_text("✅ Position saved")

# -------- MONITOR --------

async def monitor(app):
    if "active" not in positions:
        return

    market = get_market()
    pos = positions["active"]

    price = market.get(pos["temp"], None)

    if price and price >= 0.50:
        profit = (price - pos["entry"]) / pos["entry"]

        await app.bot.send_message(
            chat_id=CHAT_ID,
            text=f"🚀 SELL {pos['temp']}°C @ {price} | Profit: {profit:.2%}"
        )

        del positions["active"]

# -------- MAIN --------

app = ApplicationBuilder().token(TOKEN).build()
app.add_handler(CommandHandler("buy", buy))

scheduler = AsyncIOScheduler()

# тест режим (щохвилини)
scheduler.add_job(daily_job, "interval", minutes=1, args=[app])
scheduler.add_job(monitor, "interval", minutes=2, args=[app])

scheduler.start()

app.run_polling()
