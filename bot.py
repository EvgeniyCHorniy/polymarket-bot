import os
import requests
from datetime import datetime, timedelta

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
)

# =====================
# ENV
# =====================
TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

# =====================
# WEATHER MOCK (твоя логіка)
# =====================
def get_weather():
    return {
        "ECMWF": 19.6,
        "GFS": 20.1,
        "MET": 19.8
    }

def calc_probs(models):
    probs = {}
    for k, v in models.items():
        rounded = round(v)
        probs[rounded] = probs.get(rounded, 0) + 1/len(models)
    return probs

# =====================
# POLYMARKET API
# =====================
def build_slug():
    tomorrow = datetime.utcnow() + timedelta(days=1)
    day = tomorrow.day
    month = tomorrow.strftime("%B").lower()
    year = tomorrow.year

    return f"highest-temperature-in-london-on-{month}-{day}-{year}"

def get_market():
    url = "https://gamma-api.polymarket.com/events?active=true&closed=false"

    try:
        r = requests.get(url, timeout=10)
        data = r.json()

        tomorrow = datetime.utcnow() + timedelta(days=1)
        day = str(tomorrow.day)
        month = tomorrow.strftime("%B")
        year = str(tomorrow.year)

        target_words = [
            "london",
            "highest temperature",
            month.lower(),
            day,
            year
        ]

        for event in data:
            title = (event.get("title") or "").lower()

            if all(word in title for word in target_words):
                markets = event.get("markets", [])
                if not markets:
                    continue

                market = markets[0]

                outcomes = market.get("outcomes", [])
                prices = market.get("outcomePrices", [])

                result = {}
                for i in range(len(outcomes)):
                    result[outcomes[i]] = float(prices[i])

                slug = event.get("slug")
                link = f"https://polymarket.com/event/{slug}"

                return result, link

        return None, None

    except Exception as e:
        print("API ERROR:", e)
        return None, None

# =====================
# JOB
# =====================
async def daily_job(context: ContextTypes.DEFAULT_TYPE):
    models = get_weather()
    probs = calc_probs(models)

    market, link = get_market()

    if market is None:
        text = "❌ Market not found"
    else:
        best = max(probs, key=probs.get)
        prob = probs[best]
        price = market.get(str(best), "N/A")

        text = f"""
📊 London (Tomorrow)

ECMWF: {models['ECMWF']}
GFS: {models['GFS']}
MET: {models['MET']}

🎯 Top:
{best}°C → {round(prob*100)}%

💰 Market:
{best}°C → {price}

🔗 {link}
"""

    await context.bot.send_message(chat_id=CHAT_ID, text=text)

# =====================
# MAIN
# =====================
def main():
    app = ApplicationBuilder().token(TOKEN).build()

    # кожні 10 хв
    app.job_queue.run_repeating(daily_job, interval=600, first=5)

    print("Bot started")

    app.run_polling()

# =====================
if __name__ == "__main__":
    main()
