from telethon import TelegramClient
import pandas as pd
from datetime import datetime, timezone
import json
import os
from argparse import ArgumentParser

# ── Argument Parser ────────────────────────────────────────────────────────────
parser = ArgumentParser()
parser.add_argument("--channel", default="PikudHaOref_all")
parser.add_argument("--output", default="PikudHaOref_alerts.csv")
# 2nd Iran war began on 2026-02-28, so we want to get messages from that date and later
parser.add_argument("--start_date", default="2026-02-28", help="Start date for scraping (YYYY-MM-DD)")
parser.add_argument("--end_date", default=None, help="End date for scraping (YYYY-MM-DD)")

if not os.path.exists("keys.json") or "telegram_api" not in json.load(open("keys.json", "r")):
    parser.add_argument("--api_id", required=True, help="Telegram API ID")
    parser.add_argument("--api_hash", required=True, help="Telegram API Hash")
args = parser.parse_args()

if not os.path.exists("keys.json") or "telegram_api" not in json.load(open("keys.json", "r")):
    api_id = args.api_id
    api_hash = args.api_hash
else:
    with open("keys.json", "r") as f:
        keys = json.load(f)
    api_id = keys["telegram_api"]["id"]
    api_hash = keys["telegram_api"]["hash"]

channel = args.channel
output_file = args.output

start_date = datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)

if args.end_date:
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
else:
    end_date = None

# ── Main Logic ───────────────────────────────────────────────────────────────
messages = []

async def main():
    async for msg in client.iter_messages(channel):
        if msg.date < start_date:
            break

        if end_date and msg.date > end_date:
            break

        if msg.text:
            messages.append({
                "date": msg.date,
                "text": msg.text
            })
# ── Run the Telegram client and execute main() ─────────────────────────────────
with TelegramClient("alerts_session", api_id, api_hash) as client:
    client.loop.run_until_complete(main())

# ── Save messages to CSV ─────────────────────────────────────────────────────────
df = pd.DataFrame(messages)
df["date"] = df["date"].dt.tz_convert("Asia/Jerusalem")

df.to_csv(output_file, index=False)
print(df.head())