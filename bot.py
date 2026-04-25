import os
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

TOKEN = os.getenv("BOT_TOKEN")

# =========================
# DATE (London timezone)
# =========================
def get_event_info(day="tomorrow"):
    now = datetime.now(ZoneInfo("Europe/London"))

    if day == "today":
        target = now
    else:
        target = now + timedelta(days=1)

    slug = f"highest-temperature-in-london-on-{target.strftime('%B').lower()}-{target.day}-{target.year}"
    date_str = target.strftime("%Y-%m-%d")
    url = f"https://polymarket.com/event/{slug}"

    return slug, date_str, url


# =========================
# WEATHER (REAL)
# =========================
def get_forecast_all():
    sources = {}

    # Met.no
    try:
        r = requests.get(
            "https://api.met.no/weatherapi/locationforecast/2.0/compact?lat=51.5&lon=0.05",
            headers={"User-Agent": "weather-bot"},
            timeout=10
        )
        data = r.json()

        temps = [
            t["data"]["instant"]["details"]["air_temperature"]
            for t in data["properties"]["timeseries"][:24]
        ]

        sources["MetNo"] = round(max(temps))
    except:
        sources["MetNo"] = None

    # VisualCrossing
    try:
        r = requests.get(
            "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/London/tomorrow?unitGroup=metric&include=days",
            timeout=10
        )
        data = r.json()
        sources["VisualCrossing"] = round(data["days"][0]["tempmax"])
    except:
        sources["VisualCrossing"] = None

    valid = [v for v in sources.values() if v is not None]

    if not valid:
        return {"sources": sources, "avg": None, "final": None}

    avg = round(sum(valid) / len(valid), 1)

    # EGLC bias
    final = round(avg - 0.5)

    return {
        "sources": sources,
        "avg": avg,
        "final": final
    }


# =========================
# POLYMARKET
# =========================
def get_prices(slug):
    url = f"https://gamma-api.polymarket.com/events?slug={slug}"

    try:
        r = requests.get(url, timeout=10)
        data = r.json()

        if not data:
            return None

        markets = data[0].get("markets", [])
        results = []

        for m in markets:
            q = m.get("question", "")
            bid = float(m.get("bestBid", 0))
            ask = float(m.get("bestAsk", 0))

            if bid == 0 and ask == 0:
                continue

            temp = None
            for t in range(5, 40):
                if f"{t}°C" in q:
                    temp = t
                    break

            if temp is None:
                continue

            results.append({
                "temp": temp,
                "buy": round(ask * 100, 1),
                "sell": round(bid * 100, 1),
                "spread": round((ask - bid) * 100, 2),
                "liq": float(m.get("liquidity", 0))
            })

        return results

    except:
        return None


# =========================
# STRATEGY (forecast ±1)
# =========================
def select_trade(results, forecast):
    if not results or forecast is None:
        return None, []

    targets = [forecast - 1, forecast, forecast + 1]

    selected = [r for r in results if r["temp"] in targets]

    if not selected:
        return None, []

    selected = sorted(selected, key=lambda x: x["temp"])

    best = min(selected, key=lambda x: x["buy"])

    return best, selected


# =========================
# MONITOR
# =========================
async def monitor(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.data["chat_id"]
    temp = context.job.data["temp"]
    slug = context.job.data["slug"]
    day = context.job.data["day"]

    results = get_prices(slug)
    if not results:
        return

    for r in results:
        if r["temp"] == temp and r["buy"] >= 50:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"🚀 SELL SIGNAL {temp}°C ({day}) → {r['buy']}%"
            )
            context.job.schedule_removal()


# =========================
# CORE LOGIC
# =========================
async def run(update, context, day):
    slug, date_str, url = get_event_info(day)

    forecast_data = get_forecast_all()
    forecast = forecast_data["final"]

    results = get_prices(slug)

    if not results:
        await update.message.reply_text("❌ No market data")
        return

    best, selected = select_trade(results, forecast)

    msg = f"""
📅 {day.upper()} → {date_str}
🔗 {url}

🌤 Forecast:
MetNo: {forecast_data['sources'].get('MetNo')}
VisualCrossing: {forecast_data['sources'].get('VisualCrossing')}

📊 Avg: {forecast_data['avg']}
🧭 EGLC: {forecast}

🎯 Target (±1):
"""

    for r in selected:
        mark = "👉" if best and r["temp"] == best["temp"] else ""
        msg += f"\n{mark}{r['temp']}°C → BUY {r['buy']}% | SELL {r['sell']}%"

    if best:
        msg += f"\n\n🔥 BEST: {best['temp']}°C ({best['buy']}%)"

        if best["buy"] < 38:
            msg += "\n💰 BUY SIGNAL → /buy"

        context.user_data["temp"] = best["temp"]
        context.user_data["slug"] = slug
        context.user_data["day"] = day

    await update.message.reply_text(msg)


# =========================
# COMMANDS
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await run(update, context, "tomorrow")


async def today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await run(update, context, "today")


async def tomorrow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await run(update, context, "tomorrow")


async def buy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    temp = context.user_data.get("temp")
    slug = context.user_data.get("slug")
    day = context.user_data.get("day")

    if not temp:
        await update.message.reply_text("❌ Спочатку /today або /tomorrow")
        return

    context.job_queue.run_repeating(
        monitor,
        interval=60,
        first=5,
        data={
            "chat_id": update.effective_chat.id,
            "temp": temp,
            "slug": slug,
            "day": day
        }
    )

    await update.message.reply_text(f"👀 Monitoring {temp}°C ({day})")


# =========================
# MAIN
# =========================
def main():
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("today", today))
    app.add_handler(CommandHandler("tomorrow", tomorrow))
    app.add_handler(CommandHandler("buy", buy))

    print("🚀 Bot started")
    app.run_polling()


if __name__ == "__main__":
    main()
