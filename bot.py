import requests
import re
from datetime import datetime, timedelta
import asyncio
from telegram import Bot
from telegram.ext import ApplicationBuilder, ContextTypes
import os

# ================= CONFIG =================

TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

# ================= MARKET PARSER =================

def get_market():
    url = "https://gamma-api.polymarket.com/events?active=true&closed=false"

    try:
        data = requests.get(url, timeout=10).json()
    except:
        return None

    today = datetime.utcnow().date()
    tomorrow = today + timedelta(days=1)
    target = tomorrow.strftime("%B %d").lower()

    for event in data:
        for m in event.get("markets", []):
            q = m.get("question", "").lower()

            if "highest temperature in london" not in q:
                continue

            if target not in q:
                continue

            prices = {}

            for o in m.get("outcomes", []):
                t = re.search(r"(\d+)", o.get("name", ""))
                if t:
                    prices[int(t.group(1))] = float(o.get("price", 0))

            if not prices:
                continue

            slug = m.get("slug", "")
            link = f"https://polymarket.com/event/{slug}" if slug else ""

            return prices, link, q

    return None

# ================= SIGNAL LOGIC =================

def format_signal(prices):
    if not prices:
        return "❌ No prices"

    best_temp = max(prices, key=prices.get)
    best_price = prices[best_temp]

    msg = "📊 London (EGLC) – Tomorrow\n\n"

    for t in sorted(prices.keys()):
        msg += f"{t}°C → {round(prices[t]*100)}%\n"

    msg += "\n🎯 Top:\n"
    msg += f"{best_temp}°C → {round(best_price*100)}%\n"

    if 0.35 <= best_price <= 0.38:
        msg += "\n🟢 BUY zone (35–38%)"
    elif best_price >= 0.50:
        msg += "\n💰 SELL zone (50%+)"
    else:
        msg += "\n⏳ No action"

    return msg

# ================= BOT JOB =================

LAST_SENT = None

async def daily_job(context: ContextTypes.DEFAULT_TYPE):
    global LAST_SENT

    result = get_market()

    if not result:
        if LAST_SENT != "no_market":
            await context.bot.send_message(
                chat_id=CHAT_ID,
                text="⏳ Market not found yet"
            )
            LAST_SENT = "no_market"
        return

    prices, link, q = result

    signal = format_signal(prices)
    text = f"{signal}\n\n🔗 {link}"

    if text != LAST_SENT:
        await context.bot.send_message(
            chat_id=CHAT_ID,
            text=text
        )
        LAST_SENT = text

# ================= MAIN =================

async def main():
    app = ApplicationBuilder().token(TOKEN).build()

    # запуск кожні 10 хв
    app.job_queue.run_repeating(daily_job, interval=600, first=5)

    print("Bot started")

    await app.run_polling()

if __name__ == "__main__":
    asyncio.run(main())
