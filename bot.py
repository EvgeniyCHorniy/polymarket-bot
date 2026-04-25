import os
import requests
from datetime import datetime, timedelta

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise ValueError("❌ BOT_TOKEN not set")

# =========================
# DATE → ALWAYS TOMORROW
# =========================
def get_event_info():
    d = datetime.utcnow() + timedelta(days=1)
    slug = f"highest-temperature-in-london-on-{d.strftime('%B').lower()}-{d.day}-{d.year}"
    date_str = d.strftime("%Y-%m-%d")
    url = f"https://polymarket.com/event/{slug}"
    return slug, date_str, url


# =========================
# WEATHER (REAL DATA)
# =========================

def get_forecast_all():
    sources = {}

    # =========================
    # 1. Met.no (основа)
    # =========================
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

    # =========================
    # 2. VisualCrossing (часто ближче до реальності)
    # =========================
    try:
        r = requests.get(
            "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/London/tomorrow?unitGroup=metric&include=days",
            timeout=10
        )
        data = r.json()
        sources["VisualCrossing"] = round(data["days"][0]["tempmax"])
    except:
        sources["VisualCrossing"] = None

    # =========================
    # 3. WeatherAPI (безкоштовний ключ потрібен)
    # =========================
    try:
        API_KEY = os.getenv("WEATHERAPI_KEY")
        if API_KEY:
            r = requests.get(
                f"http://api.weatherapi.com/v1/forecast.json?key={API_KEY}&q=London&days=1",
                timeout=10
            )
            data = r.json()
            sources["WeatherAPI"] = round(data["forecast"]["forecastday"][0]["day"]["maxtemp_c"])
        else:
            sources["WeatherAPI"] = None
    except:
        sources["WeatherAPI"] = None

    # =========================
    # FILTER + AVG
    # =========================
    valid = [v for v in sources.values() if v is not None]

    if not valid:
        return {"sources": sources, "avg": None, "final": None}

    # 👉 важливо: прибираємо екстремуми (як OpenMeteo косячив)
    if len(valid) >= 3:
        valid.sort()
        valid = valid[1:-1]  # обрізаємо min/max

    avg = round(sum(valid) / len(valid), 1)

    # EGLC bias (ключ!)
    final = round(avg - 0.5)

    return {
        "sources": sources,
        "avg": avg,
        "final": final
    }

# =========================
# POLYMARKET (bestBid/Ask)
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
            bid = m.get("bestBid") or 0
            ask = m.get("bestAsk") or 0

            # пропускаємо пусті
            if bid == 0 and ask == 0:
                continue

            # дістаємо температуру з питання
            temp = None
            for t in range(5, 40):
                if f"{t}°C" in q:
                    temp = t
                    break
            if temp is None:
                continue

            results.append({
                "temp": temp,
                "buy": round(ask * 100, 1),   # купуєш по ask
                "sell": round(bid * 100, 1),  # продаєш по bid
                "spread": round((ask - bid) * 100, 2),
                "liq": float(m.get("liquidity", 0))
            })

        return results if results else None

    except Exception as e:
        print("API error:", e)
        return None


# =========================
# STRATEGY (t-1, t, t+1 → найдешевший)
# =========================
def select_trade(results, forecast):
    if not results or forecast is None:
        return None, []

    temps = [r["temp"] for r in results]
    nearest = min(temps, key=lambda t: abs(t - forecast))

    candidates = []
    for t in [nearest - 1, nearest, nearest + 1]:
        for r in results:
            if r["temp"] == t:
                candidates.append(r)

    if not candidates:
        return None, []

    # найдешевший buy поруч із прогнозом
    best = min(candidates, key=lambda x: x["buy"])
    return best, sorted(candidates, key=lambda x: x["temp"])


# =========================
# MONITOR (SELL ≥ 50%)
# =========================
async def monitor(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.data["chat_id"]
    temp = context.job.data["temp"]
    slug = context.job.data["slug"]

    results = get_prices(slug)
    if not results:
        return

    for r in results:
        if r["temp"] == temp and r["buy"] >= 50:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"🚀 SELL SIGNAL\n{temp}°C → {r['buy']}%"
            )
            context.job.schedule_removal()
            break


# =========================
# /start
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    slug, date_str, poly_url = get_event_info()

    f = get_forecast_all()
    results = get_prices(slug)

    if not results:
        await update.message.reply_text("❌ No market data (можливо маркет ще не відкрився)")
        return

    best, nearby = select_trade(results, f["final"])

    # формуємо повідомлення
    msg = f"""
📅 Date: {date_str}
🔗 {poly_url}

🌤 Forecast:
OpenMeteo: {f['sources'].get('OpenMeteo')}
MetNo: {f['sources'].get('MetNo')}
VisualCrossing: {f['sources'].get('VisualCrossing')}

📊 Avg: {f['avg']}
🧭 EGLC adj: {f['final']}

🎯 Target zone:
"""

    for r in nearby:
        mark = "👉" if best and r["temp"] == best["temp"] else "  "
        msg += f"\n{mark} {r['temp']}°C → BUY {r['buy']}% | SELL {r['sell']}% | spread {r['spread']}%"

    if best:
        msg += f"\n\n🔥 BEST ENTRY: {best['temp']}°C ({best['buy']}%)"
        if best["buy"] < 38:
            msg += "\n💰 BUY SIGNAL (<38%) → /buy"
        context.user_data["last_temp"] = best["temp"]
        context.user_data["slug"] = slug

    await update.message.reply_text(msg)


# =========================
# /buy
# =========================
async def buy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    temp = context.user_data.get("last_temp")
    slug = context.user_data.get("slug")

    if not temp or not slug:
        await update.message.reply_text("❌ Спочатку /start")
        return

    context.job_queue.run_repeating(
        monitor,
        interval=60,
        first=5,
        data={
            "chat_id": update.effective_chat.id,
            "temp": temp,
            "slug": slug
        }
    )

    await update.message.reply_text(f"👀 Monitoring {temp}°C (sell ≥50%)")


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
