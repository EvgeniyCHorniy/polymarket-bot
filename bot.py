import os

TOKEN = os.getenv("BOT_TOKEN")

import requests
import json
import asyncio
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes


EVENT_SLUG = "highest-temperature-in-london-on-april-27-2026"

# =========================
# WEATHER (простий baseline)
# =========================
def get_forecast():
    # тут можна підключити 3 API, зараз baseline
    return 20  # заглушка (можеш замінити)

# =========================
# GET PRICES (FIXED)
# =========================
def get_prices():
    url = f"https://gamma-api.polymarket.com/events?slug={EVENT_SLUG}"
    r = requests.get(url)

    if r.status_code != 200:
        return None, None

    data = r.json()
    if not data:
        return None, None

    markets = data[0].get("markets", [])

    results = []

    for m in markets:
        question = m.get("question", "")
        bid = m.get("bestBid")
        ask = m.get("bestAsk")

        if bid is None or ask is None:
            continue

        # визначаємо температуру
        temp = None
        for t in range(10, 40):
            if f"{t}°C" in question:
                temp = t
                break

        if temp is None:
            continue

        buy = round(ask * 100, 1)
        sell = round(bid * 100, 1)
        spread = round(buy - sell, 2)
        liquidity = float(m.get("liquidity", 0))

        results.append({
            "temp": temp,
            "buy": buy,
            "sell": sell,
            "spread": spread,
            "liq": liquidity
        })

    if not results:
        return None, None

    # беремо найбільш ймовірний (max buy)
    best = max(results, key=lambda x: x["buy"])

    return best, results

# =========================
# MONITOR AFTER BUY
# =========================
async def monitor(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.data["chat_id"]
    target_temp = context.job.data["temp"]

    best, all_markets = get_prices()

    if not all_markets:
        return

    for m in all_markets:
        if m["temp"] == target_temp:
            if m["buy"] >= 50:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"🚀 SELL SIGNAL\nTemp {target_temp}°C reached {m['buy']}%"
                )
                context.job.schedule_removal()
            break

# =========================
# /start
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    forecast = get_forecast()
    best, all_markets = get_prices()

    if not best:
        await update.message.reply_text("❌ No prices found")
        return

    msg = f"""
🌤 Forecast (EGLC adj): {forecast}°C

🎯 Best Market: {best['temp']}°C
BUY: {best['buy']}%
SELL: {best['sell']}%
Spread: {best['spread']}%
Liquidity: {round(best['liq'])}
"""

    if best["buy"] < 38:
        msg += "\n🔥 SIGNAL: BUY (<38%)\nНапиши BUY для моніторингу"

    context.user_data["last_temp"] = best["temp"]

    await update.message.reply_text(msg)

# =========================
# BUY COMMAND
# =========================
async def buy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    temp = context.user_data.get("last_temp")

    if not temp:
        await update.message.reply_text("❌ Спочатку /start")
        return

    context.job_queue.run_repeating(
        monitor,
        interval=60,
        first=5,
        data={
            "chat_id": update.effective_chat.id,
            "temp": temp
        }
    )

    await update.message.reply_text(f"👀 Monitoring {temp}°C... (sell ≥50%)")

# =========================
# MAIN
# =========================
def main():
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("buy", buy))

    print("🚀 Bot started")

    app.run_polling()

if __name__ == "__main__":
    main()
