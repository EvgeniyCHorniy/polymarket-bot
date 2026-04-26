import os
import requests
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

TOKEN = os.getenv("BOT_TOKEN")

# =========================
# DATE + SLUG
# =========================
def get_target_date(mode="today"):
    now = datetime.utcnow()
    if mode == "tomorrow":
        now += timedelta(days=1)

    date_str = now.strftime("%Y-%m-%d")
    day = str(int(now.strftime("%d")))
    month = now.strftime("%B").lower()

    slug = f"highest-temperature-in-london-on-{month}-{day}-2026"

    return date_str, slug


# =========================
# FORECAST (MET.NO)
# =========================
def get_forecast():
    try:
        url = "https://api.met.no/weatherapi/locationforecast/2.0/compact"
        headers = {"User-Agent": "weather-bot"}

        params = {"lat": 51.5074, "lon": 0.0553}

        r = requests.get(url, headers=headers, params=params, timeout=5)
        data = r.json()

        temps = []
        for item in data["properties"]["timeseries"][:10]:
            temps.append(item["data"]["instant"]["details"]["air_temperature"])

        return round(sum(temps) / len(temps))
    except:
        return None


# =========================
# POLYMARKET
# =========================
def get_markets(slug):
    try:
        url = f"https://gamma-api.polymarket.com/events?slug={slug}"
        r = requests.get(url, timeout=5)
        data = r.json()

        if not data or len(data) == 0:
            return []

        markets = data[0].get("markets", [])

        parsed = []

        for m in markets:
            try:
                question = m.get("question", "")
                if "°C" not in question:
                    continue

                temp = int(question.split("be ")[1].split("°")[0])

                outcomes = m.get("outcomes", [])
                if len(outcomes) < 2:
                    continue

                yes = outcomes[0]

                buy = float(yes.get("price", 0)) * 100
                sell = float(yes.get("price", 0)) * 100  # fallback

                spread = abs(buy - sell)

                parsed.append({
                    "temp": temp,
                    "buy": round(buy, 1),
                    "sell": round(sell, 1),
                    "spread": round(spread, 1),
                    "liq": float(m.get("liquidity", 0))
                })

            except:
                continue

        return parsed

    except Exception as e:
        return []


# =========================
# MESSAGE BUILDER
# =========================
def build_message(mode="today"):
    date_str, slug = get_target_date(mode)
    url = f"https://polymarket.com/event/{slug}"

    forecast = get_forecast()
    markets = get_markets(slug)

    if not markets:
        return f"❌ No market data\n{url}"

    msg = f"📅 {mode.upper()} → {date_str}\n🔗 {url}\n\n"

    msg += "🌤 Forecast:\n"
    msg += f"MetNo: {forecast}\n\n"

    if forecast is None:
        return msg + "❌ No forecast"

    msg += f"🎯 Target (±1 around {forecast}°C):\n\n"

    targets = [forecast - 1, forecast, forecast + 1]

    found = False

    for t in targets:
        m = next((x for x in markets if x["temp"] == t), None)

        if not m:
            continue

        found = True

        marker = "👉" if t == forecast else "•"

        msg += (
            f"{marker} {t}°C → "
            f"BUY {m['buy']}% | "
            f"SELL {m['sell']}% | "
            f"spread {m['spread']}% | "
            f"liq {int(m['liq'])}\n"
        )

    if not found:
        msg += "❌ No matching temps found\n"

    # BEST ENTRY
    best = None
    for m in markets:
        if abs(m["temp"] - forecast) <= 1:
            if not best or m["buy"] < best["buy"]:
                best = m

    if best:
        msg += f"\n🔥 BEST ENTRY: {best['temp']}°C ({best['buy']}%)"

    return msg


# =========================
# HANDLERS
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Use /today or /tomorrow")


async def today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        msg = build_message("today")
        await update.message.reply_text(msg)
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")


async def tomorrow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        msg = build_message("tomorrow")
        await update.message.reply_text(msg)
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")


# =========================
# MAIN
# =========================
def main():
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("today", today))
    app.add_handler(CommandHandler("tomorrow", tomorrow))

    print("🚀 Bot started")
    app.run_polling()


if __name__ == "__main__":
    main()
