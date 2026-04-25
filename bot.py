import os
import requests
from datetime import datetime, timedelta

from telegram.ext import ApplicationBuilder, ContextTypes

TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")


# =====================
# WEATHER (твоя логіка)
# =====================
def get_weather():
    return {
        "ECMWF": 19.6,
        "GFS": 20.1,
        "MET": 19.8
    }


def calc_probs(models):
    probs = {}
    for v in models.values():
        rounded = round(v)
        probs[rounded] = probs.get(rounded, 0) + 1 / len(models)
    return probs


# =====================
# SLUG
# =====================
def build_slug():
    tomorrow = datetime.utcnow() + timedelta(days=1)
    return f"highest-temperature-in-london-on-{tomorrow.strftime('%B').lower()}-{tomorrow.day}-{tomorrow.year}"


# =====================
# 1. TRY CLOB (основний)
# =====================
def get_from_clob(slug):
    try:
        url = "https://clob.polymarket.com/markets"
        data = requests.get(url, timeout=10).json()

        for m in data:
            if m.get("market_slug") == slug:
                outcomes = m.get("outcomes", [])
                prices = m.get("outcomePrices", [])

                result = {}
                for i in range(len(outcomes)):
                    result[outcomes[i]] = float(prices[i])

                return result

        return None

    except Exception as e:
        print("CLOB ERROR:", e)
        return None


# =====================
# 2. FALLBACK GAMMA
# =====================
def get_from_gamma():
    try:
        url = "https://gamma-api.polymarket.com/events?limit=1000"
        data = requests.get(url, timeout=10).json()

        tomorrow = datetime.utcnow() + timedelta(days=1)
        day = str(tomorrow.day)
        month = tomorrow.strftime("%B").lower()

        for event in data:
            title = (event.get("title") or "").lower()

            if (
                "london" in title
                and "temperature" in title
                and month in title
                and day in title
            ):
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
                return result, slug

        return None, None

    except Exception as e:
        print("GAMMA ERROR:", e)
        return None, None


# =====================
# MAIN MARKET LOGIC
# =====================
def get_market():
    slug = build_slug()

    # 1. пробуємо CLOB
    market = get_from_clob(slug)
    if market:
        return market, f"https://polymarket.com/event/{slug}"

    # 2. fallback GAMMA
    market, gamma_slug = get_from_gamma()
    if market:
        link = f"https://polymarket.com/event/{gamma_slug}"
        return market, link

    return None, None


# =====================
# JOB
# =====================
async def daily_job(context: ContextTypes.DEFAULT_TYPE):
    models = get_weather()
    probs = calc_probs(models)

    market, link = get_market()

    if market is None:
        text = "❌ Market not found (API lag or blocked)"
    else:
        best = max(probs, key=probs.get)
        prob = probs[best]
        price = market.get(str(best), "N/A")

        text = f"""📊 London (Tomorrow)

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

    app.job_queue.run_repeating(daily_job, interval=600, first=5)

    print("Bot started")

    app.run_polling()


if __name__ == "__main__":
    main()
