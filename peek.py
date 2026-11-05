import asyncio
import websockets
import json

URL = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"


async def main():
    async with websockets.connect(URL) as ws:
        for i in range(5):
            message = await ws.recv()
            data = json.loads(message)
            print(json.dumps(data, indent=2))
            print()


asyncio.run(main())
