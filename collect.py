import asyncio
import websockets
import json
from datetime import datetime, timezone
import gzip

import os

URL = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"


def load_cursor():
    if os.path.exists("data/cursor.txt"):
        with open("data/cursor.txt") as c:
            return c.read().strip()
    return None


def current_filename():
    now = datetime.now(timezone.utc)
    return f"data/bronze/posts-{now:%Y-%m-%d-%H}.jsonl.gz"


async def listen():
    cursor = load_cursor()
    url = f"{URL}&cursor={cursor}" if cursor else URL
    async with websockets.connect(url) as ws:
        current = current_filename()
        f = gzip.open(current, "at")
        try:
            async for message in ws:
                name = current_filename()
                if name != current:
                    f.close()
                    current = name
                    f = gzip.open(current, "at")
                f.write(message + "\n")
                data = json.loads(message)
                with open("data/cursor.txt", "w") as c:
                    c.write(str(data["cursor"]))
        finally:
            f.close()


async def main():
    while True:
        try:
            await listen()
        except Exception as e:
            print("connection lost:", e)
            await asyncio.sleep(5)


asyncio.run(main())
