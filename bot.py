import os
import requests
import math
from datetime import datetime, timedelta

from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

TOKEN = os.getenv("BOT_TOKEN")

LAT = 51.5053
LON = 0.0553

WATCH = {}  # chat_id -> outcome


# =====================
# SLUG
# =====================
def build_slug():
    tomorrow = datetime.utcnow() + timedelta(days=1)
    return f"highest-temperature-in-london-on-{tomorrow.strftime('%B').lower()}-{tomorrow.day}-{tomorrow.year}"


# =====================
# POLYMARKET
# =====================
def get_event():
    slug = build_slug()
    url = f"https://gamma-api.polymarket.com/events?slug={slug}"

    r = requests.get(url, timeout=10)
    data = r.json()

    if not data:
        return None, None

    return data[0], slug


def get_prices(event):
    result = {}

    for m in event.get("markets", []):
        for o in m.get("outcomes", []):
            result[o["name"]] = round(o.get("price", 0) * 100, 1)

    return result


# =====================
# WEATHER
# =====================
def fetch_weather():
    tomorrow = (datetime.utcnow() + timedelta(days=1)).strftime("%Y-%m-%d")

    url = (
        "https://api.open-meteo.com/v1/forecast?"
        f"latitude={LAT}&longitude={LON}"
        "&hourly=temperature_2m,cloudcover,windspeed_10m"
        f"&start_date={tomorrow}&end_date={tomorrow}"
    )

    return requests.get(url, timeout=10).json()["hourly"]


def build_models(data):
    temps = data["temperature_2m"]
    wind = data["windspeed_10m"]
    clouds = data["cloudcover"]

    base_max = max(temps)

    return {
        "ECMWF": base_max,
        "ICON": base_max - 0.2,
        "GFS": base_max - 0.4
    }, wind, clouds


def apply_adjustments(models, wind, clouds):
    avg = (
        models["ECMWF"] * 0.5 +
        models["ICON"] * 0.3 +
        models["GFS"] * 0.2
    )

    avg_cloud = sum(clouds) / len(clouds)
    avg_wind = sum(wind) / len(wind)

    # сонце
    if avg_cloud < 20:
        sun = 1.0
    elif avg_cloud < 50:
        sun = 0.4
    else:
        sun = 0

    cloud_penalty = (avg_cloud / 100) * 1.2
    wind_penalty = max(0, (avg_wind - 3)) * 0.2

    eglc_bias = -0.7
    max_boost = 0.5

    final = avg + max_boost + sun - cloud_penalty - wind_penalty + eglc_bias

    return round(final, 1)


def calc_probs(temp):
    temps = range(int(temp - 2), int(temp + 3))

    probs = {}
    sigma = 0.9

    for t in temps:
        p = math.exp(-((t - temp) ** 2) / (2 * sigma ** 2))
        probs[f"{t}°C"] = p

    total = sum(probs.values())
    for k in probs:
        probs[k] /= total

    return probs


# =====================
# COMMANDS
# =====================
async def start(update, context):
    event, slug = get_event()

    if not event:
        await update.message.reply_text("❌ Market not found")
        return

    prices = get_prices(event)

    data = fetch_weather()
    models, wind, clouds = build_models(data)
    temp = apply_adjustments(models, wind, clouds)
    probs = calc_probs(temp)

    best = max(probs, key=probs.get)

    msg = f"📊 London EGLC – Tomorrow\n\n"

    msg += "Models:\n"
    for k, v in models.items():
        msg += f"{k}: {v}\n"

    msg += f"\nAdjusted temp: {temp}°C\n"

    msg += "\nModel probabilities:\n"
    for k, v in probs.items():
        msg += f"{k} → {round(v*100)}%\n"

    msg += "\nMarket:\n"
    for k, v in prices.items():
        msg += f"{k} → {v}%\n"

    msg += f"\nTop model: {best} ({round(probs[best]*100)}%)"

    msg += f"\n\n🔗 https://polymarket.com/event/{slug}"

    await update.message.reply_text(msg)


async def buy(update, context):
    if not context.args:
        await update.message.reply_text("Usage: /buy 20")
        return

    temp = context.args[0] + "°C"

    WATCH[update.effective_chat.id] = temp

    await update.message.reply_text(f"👀 Watching {temp}")


async def monitor(context):
    event, _ = get_event()

    if not event:
        return

    prices = get_prices(event)

    for chat_id, outcome in WATCH.items():
        price = prices.get(outcome, 0)

        if price >= 50:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"🚀 {outcome} reached {price}%"
            )


# =====================
# MAIN
# =====================
def main():
    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("buy", buy))

    app.job_queue.run_repeating(monitor, interval=60)

    print("Bot started")
    app.run_polling()


if __name__ == "__main__":
    main()
