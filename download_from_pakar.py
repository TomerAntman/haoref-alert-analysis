from telethon import TelegramClient
import pandas as pd
from datetime import datetime, timezone
import json

with open("keys.json", "r") as f:
    keys = json.load(f)
api_id = keys["telegram_api"]["id"]
api_hash = keys["telegram_api"]["hash"]

channel = "PikudHaOref_all"

# 2nd Iran war began on 2026-02-28, so we want to get messages from that date and later
start_date = datetime(2026, 2, 28, tzinfo=timezone.utc)

messages = []

async def main():
    async for msg in client.iter_messages(channel):
        if msg.date < start_date:
            break

        if msg.text:
            messages.append({
                "date": msg.date,
                "text": msg.text
            })

with TelegramClient("alerts_session", api_id, api_hash) as client:
    client.loop.run_until_complete(main())

df = pd.DataFrame(messages)
df["date"] = df["date"].dt.tz_convert("Asia/Jerusalem")
# save to csv
df.to_csv("PikudHaOref_alerts.csv", index=False)
print(df.head())