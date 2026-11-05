import asyncio
import websockets
import json
from datetime import datetime, timezone
import gzip

URL = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"


def current_filename():
    now = datetime.now(timezone.utc)
    return f"data/bronze/posts-{now:%Y-%m-%d-%H}.jsonl.gz"


async def main():
    async with websockets.connect(URL) as ws:
        current = current_filename()
        f = gzip.open(current, "at")
        async for message in ws:
            name = current_filename()
            if name != current:
                f.close()
                current = name
                f = open(current, "a")
            f.write(message + "\n")


asyncio.run(main())
