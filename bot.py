import os
import requests
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

TOKEN = os.getenv("BOT_TOKEN")

# =========================
# DATE + SLUG
# =========================
def get_target(mode="today"):
    now = datetime.utcnow()
    if mode == "tomorrow":
        now += timedelta(days=1)

    date_str = now.strftime("%Y-%m-%d")

    day = str(int(now.strftime("%d")))
    month = now.strftime("%B").lower()

    slug = f"highest-temperature-in-london-on-{month}-{day}-2026"

    url = f"https://polymarket.com/event/{slug}"

    return date_str, slug, url


# =========================
# FORECAST (MetNo)
# =========================
def get_forecast():
    try:
        url = "https://api.met.no/weatherapi/locationforecast/2.0/compact"
        headers = {"User-Agent": "bot"}

        r = requests.get(
            url,
            headers=headers,
            params={"lat": 51.5074, "lon": 0.0553},
            timeout=5
        )

        data = r.json()

        temps = [
            t["data"]["instant"]["details"]["air_temperature"]
            for t in data["properties"]["timeseries"][:10]
        ]

        return round(sum(temps) / len(temps))

    except:
        return None


# =========================
# MARKETS (СТАРИЙ РОБОЧИЙ ENDPOINT)
# =========================
def get_markets(slug):
    try:
        url = f"https://gamma-api.polymarket.com/events?slug={slug}"
        r = requests.get(url, timeout=5)
        data = r.json()

        if not data:
            return []

        event = data[0]

        # 🔥 ключ — беремо markets як раніше
        markets = event.get("markets") or event.get("childMarkets") or []

        parsed = []

        for m in markets:
            try:
                q = m.get("question", "")

                if "°C" not in q:
                    continue

                temp = int(q.split("be ")[1].split("°")[0])

                outcomes = m.get("outcomes", [])
                if not outcomes:
                    continue

                yes = outcomes[0]

                price = float(yes.get("price", 0)) * 100

                parsed.append({
                    "temp": temp,
                    "buy": round(price, 1),
                    "sell": round(price, 1),
                    "spread": 0,
                    "liq": float(m.get("liquidity", 0))
                })

            except:
                continue

        return parsed

    except Exception as e:
        print("MARKET ERROR:", e)
        return []


# =========================
# MESSAGE
# =========================
def build_message(mode="today"):
    date_str, slug, url = get_target(mode)

    forecast = get_forecast()
    markets = get_markets(slug)

    if not markets:
        return f"❌ No market data\n{url}"

    msg = f"📅 {mode.upper()} → {date_str}\n🔗 {url}\n\n"

    # forecast
    msg += "🌤 Forecast:\n"
    msg += f"MetNo: {forecast}\n\n"

    if forecast is None:
        return msg + "❌ No forecast"

    # target
    msg += f"🎯 Target (±1 around {forecast}°C):\n\n"

    targets = [forecast - 1, forecast, forecast + 1]

    found = False

    for t in targets:
        m = next((x for x in markets if x["temp"] == t), None)

        if not m:
            continue

        found = True
        mark = "👉" if t == forecast else "•"

        msg += (
            f"{mark} {t}°C → "
            f"BUY {m['buy']}% | "
            f"liq {int(m['liq'])}\n"
        )

    if not found:
        msg += "❌ No matching temps\n"

    # best entry
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
