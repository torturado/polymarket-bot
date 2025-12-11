"""
Test WebSocket connection to Polymarket for real-time market data.
"""
import asyncio
import json
import logging
from websocket_client import PolymarketWebSocketClient
from config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_websocket():
    """Test WebSocket connection and listen for market updates."""
    client = PolymarketWebSocketClient(config)

    print("Connecting to Polymarket WebSocket...")
    print(f"URL: {client.WS_URL}")

    if await client.connect():
        print("Connected! Subscribing to markets...")

        # This will fetch active market tokens and subscribe
        success = await client.subscribe_to_markets()

        if not success:
            print("Failed to subscribe to any markets")
            await client.close()
            return

        print("\nListening for market updates...")
        print("You should see book updates, price changes, and trades.")
        print("Press Ctrl+C to stop\n")

        try:
            # Listen for 60 seconds
            await asyncio.wait_for(client.listen(), timeout=60.0)
        except asyncio.TimeoutError:
            print("\n" + "="*60)
            print("Timeout reached. Summary:")
            print("="*60)

            cached = client.get_cached_markets()
            print(f"\nCached {len(cached)} market order books")

            for asset_id, data in list(cached.items())[:5]:
                print(f"\n  Asset: {asset_id[:40]}...")
                if data.get("bids"):
                    print(f"    Best bid: {data['bids'][0] if data['bids'] else 'N/A'}")
                if data.get("asks"):
                    print(f"    Best ask: {data['asks'][0] if data['asks'] else 'N/A'}")
                if data.get("last_price"):
                    print(f"    Last price: {data['last_price']}")

        await client.close()
        print("\nWebSocket closed.")
    else:
        print("Failed to connect to WebSocket")


if __name__ == "__main__":
    asyncio.run(test_websocket())
