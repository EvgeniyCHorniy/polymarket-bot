import os
import requests
from datetime import datetime, timedelta

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)

TOKEN = os.getenv("BOT_TOKEN")

# -------------------------
# BUILD SLUG (ключ всього)
# -------------------------
def build_slug():
    tomorrow = datetime.utcnow() + timedelta(days=1)

    month = tomorrow.strftime("%B").lower()
    day = tomorrow.day
    year = tomorrow.year

    return f"highest-temperature-in-london-on-{month}-{day}-{year}"


# -------------------------
# GET MARKET FROM API
# -------------------------
def get_market():
    slug = build_slug()

    url = f"https://gamma-api.polymarket.com/events?slug={slug}"

    try:
        r = requests.get(url, timeout=10)
        data = r.json()

        if not data:
            return None, None

        event = data[0]

        link = f"https://polymarket.com/event/{slug}"

        return event, link

    except Exception as e:
        print("API ERROR:", e)
        return None, None


# -------------------------
# TELEGRAM COMMAND
# -------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    event, link = get_market()

    if not event:
        await update.message.reply_text("❌ Market not found")
        return

    title = event.get("title", "No title")

    msg = f"📊 {title}\n\n🔗 {link}"

    await update.message.reply_text(msg)


# -------------------------
# MAIN
# -------------------------
def main():
    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))

    print("Bot started...")

    app.run_polling()


if __name__ == "__main__":
    main()
