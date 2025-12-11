"""
Real-Time Data Streaming (RTDS) client for Polymarket.
Based on the official RTDS documentation and real-time-data-client.

See: https://docs.polymarket.com/developers/RTDS/RTDS-overview
     https://github.com/Polymarket/real-time-data-client
"""
import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Callable, Optional, List, Dict, Any
import websockets

from config import BotConfig
from models import Market, MarketPair, MarketSide, Quote

logger = logging.getLogger(__name__)


class PolymarketRTDSClient:
	"""
    Real-Time Data Streaming (RTDS) client for Polymarket.

    Uses the official RTDS WebSocket endpoint for:
    - clob_market: Real-time orderbook, prices, and trades
    - clob_user: User orders and trades (authenticated)

    Reference: https://docs.polymarket.com/developers/RTDS/RTDS-overview
    """

	# Official RTDS WebSocket URL
	WS_URL = "wss://ws-live-data.polymarket.com"

	# Available topics
	TOPIC_CLOB_MARKET = "clob_market"
	TOPIC_CLOB_USER = "clob_user"
	TOPIC_CRYPTO_PRICES = "crypto_prices_binance"

	def __init__(self, config: BotConfig):
		"""Initialize the RTDS client with optional API credentials."""
		self.config = config
		self.websocket = None
		self.running = False
		self.markets_cache: Dict[str,
		                         Dict[str,
		                              Any]] = {}  # asset_id -> market data
		self.subscribed_assets: List[str] = []
		self.callbacks: List[Callable] = []

		# Auth credentials from config (for clob_user channel)
		self.api_key = config.api_key
		self.api_secret = config.api_secret
		self.api_passphrase = config.passphrase
		self.has_credentials = all(
		    [self.api_key, self.api_secret, self.api_passphrase])

		if self.has_credentials:
			logger.info(
			    "🔑 RTDS: API credentials configured for authenticated streams")
		else:
			logger.info(
			    "📖 RTDS: Running in read-only mode (no API credentials)")

	def _get_clob_auth(self) -> Optional[Dict[str, str]]:
		"""Get CLOB authentication object for RTDS subscription."""
		if not self.has_credentials:
			return None
		return {
		    "key": self.api_key,
		    "secret": self.api_secret,
		    "passphrase": self.api_passphrase
		}

	async def connect(self) -> bool:
		"""Connect to the RTDS WebSocket."""
		try:
			self.websocket = await websockets.connect(self.WS_URL,
			                                          ping_interval=30,
			                                          ping_timeout=10,
			                                          close_timeout=5)
			logger.info(f"✅ Connected to RTDS: {self.WS_URL}")
			return True
		except asyncio.TimeoutError:
			logger.error("RTDS connection timeout")
			return False
		except Exception as e:
			logger.error(f"Failed to connect to RTDS: {e}")
			return False

	async def subscribe_to_markets(self, asset_ids: List[str] = None) -> bool:
		"""
        Subscribe to clob_market topic for real-time orderbook and price updates.

        Args:
            asset_ids: List of token IDs to subscribe to. If None, fetches BTC market tokens.
        """
		# Ensure connection
		try:
			is_closed = self.websocket is None or self.websocket.state.name != "OPEN"
		except (AttributeError, Exception):
			is_closed = self.websocket is None

		if is_closed:
			logger.info("RTDS disconnected, reconnecting...")
			connected = await self.connect()
			if not connected:
				logger.error("Failed to reconnect RTDS")
				return False

		# Fetch asset IDs if not provided
		if not asset_ids:
			asset_ids = await self._fetch_market_tokens()

		if not asset_ids:
			logger.warning("No asset IDs to subscribe to")
			return False

		self.subscribed_assets = asset_ids

		# Build RTDS subscription message
		# Format: filters = comma-separated asset IDs
		filters = ",".join(asset_ids[:100])  # Limit to 100 for safety

		subscribe_msg = {
		    "action":
		    "subscribe",
		    "subscriptions": [{
		        "topic": self.TOPIC_CLOB_MARKET,
		        "type":
		        "*",  # All message types (price_changes, agg_orderbook, last_trade_price)
		        "filters": filters
		    }]
		}

		# Add authentication if available (improves rate limits)
		clob_auth = self._get_clob_auth()
		if clob_auth:
			subscribe_msg["subscriptions"][0]["clob_auth"] = clob_auth
			logger.debug("Using authenticated RTDS subscription")

		try:
			await self.websocket.send(json.dumps(subscribe_msg))
			logger.info(
			    f"📡 RTDS: Subscribed to {len(asset_ids)} market tokens")
			return True
		except Exception as e:
			logger.error(f"Failed to subscribe to RTDS: {e}")
			self.websocket = None
			return False

	async def listen(self, callback: Optional[Callable] = None):
		"""Listen for RTDS messages and process them."""
		self.running = True

		if callback:
			self.callbacks.append(callback)

		try:
			async for message in self.websocket:
				try:
					# Skip empty messages
					if not message or not message.strip():
						continue

					data = json.loads(message)
					await self._process_message(data)
				except json.JSONDecodeError as e:
					if message and message.strip():
						logger.warning(f"Failed to parse RTDS message: {e}")
				except Exception as e:
					logger.error(f"Error processing RTDS message: {e}")
		except websockets.exceptions.ConnectionClosed:
			logger.warning("RTDS connection closed")
		except Exception as e:
			logger.error(f"RTDS error: {e}")
		finally:
			self.running = False

	async def _process_message(self, data: Dict[str, Any]):
		"""
        Process incoming RTDS message.

        RTDS message format:
        {
            "topic": "clob_market",
            "type": "price_changes" | "agg_orderbook" | "last_trade_price",
            "timestamp": 1234567890123,
            "payload": { ... }
        }
        """
		topic = data.get("topic")
		msg_type = data.get("type")
		payload = data.get("payload", {})

		if topic != self.TOPIC_CLOB_MARKET:
			return

		if msg_type == "price_changes":
			await self._handle_price_changes(payload)
		elif msg_type == "agg_orderbook":
			await self._handle_orderbook(payload)
		elif msg_type == "last_trade_price":
			await self._handle_trade(payload)

	async def _handle_price_changes(self, payload: Dict[str, Any]):
		"""
        Handle price_changes message from RTDS.

        Compact format:
        {
            "m": "condition_id",  # market
            "pc": [               # price_changes
                {
                    "a": "asset_id",
                    "p": "0.5",       # price
                    "s": "BUY",       # side
                    "si": "100",      # size
                    "ba": "0.52",     # best_ask
                    "bb": "0.48"      # best_bid
                }
            ],
            "t": "1234567890123"  # timestamp
        }
        """
		market = payload.get("m") or payload.get("market")
		price_changes = payload.get("pc") or payload.get("price_changes", [])

		for pc in price_changes:
			asset_id = pc.get("a") or pc.get("asset_id")
			if not asset_id:
				continue

			best_bid = pc.get("bb") or pc.get("best_bid")
			best_ask = pc.get("ba") or pc.get("best_ask")

			if asset_id not in self.markets_cache:
				self.markets_cache[asset_id] = {
				    "asset_id": asset_id,
				    "market": market,
				    "bids": [],
				    "asks": [],
				    "best_bid": None,
				    "best_ask": None,
				    "last_price": None,
				}

			if best_bid:
				self.markets_cache[asset_id]["best_bid"] = float(best_bid)
			if best_ask:
				self.markets_cache[asset_id]["best_ask"] = float(best_ask)

	async def _handle_orderbook(self, payload: Dict[str, Any]):
		"""
        Handle agg_orderbook message from RTDS.

        Format:
        {
            "asset_id": "...",
            "market": "...",
            "bids": [{"price": "0.48", "size": "100"}, ...],
            "asks": [{"price": "0.52", "size": "50"}, ...],
            "timestamp": 1234567890123
        }
        """
		asset_id = payload.get("asset_id")
		if not asset_id:
			return

		bids = payload.get("bids", [])
		asks = payload.get("asks", [])

		if asset_id not in self.markets_cache:
			self.markets_cache[asset_id] = {
			    "asset_id": asset_id,
			    "market": payload.get("market"),
			    "bids": [],
			    "asks": [],
			    "best_bid": None,
			    "best_ask": None,
			    "last_price": None,
			}

		self.markets_cache[asset_id]["bids"] = bids
		self.markets_cache[asset_id]["asks"] = asks

		# Update best bid/ask
		if bids:
			sorted_bids = sorted(bids,
			                     key=lambda x: float(x.get("price", 0)),
			                     reverse=True)
			self.markets_cache[asset_id]["best_bid"] = float(
			    sorted_bids[0].get("price", 0))
		if asks:
			sorted_asks = sorted(asks,
			                     key=lambda x: float(x.get("price", 999)))
			self.markets_cache[asset_id]["best_ask"] = float(
			    sorted_asks[0].get("price", 0))

	async def _handle_trade(self, payload: Dict[str, Any]):
		"""Handle last_trade_price message from RTDS."""
		asset_id = payload.get("asset_id")
		price = payload.get("price")

		if asset_id and price:
			if asset_id not in self.markets_cache:
				self.markets_cache[asset_id] = {
				    "asset_id": asset_id,
				    "market": payload.get("market"),
				    "bids": [],
				    "asks": [],
				    "best_bid": None,
				    "best_ask": None,
				    "last_price": None,
				}
			self.markets_cache[asset_id]["last_price"] = float(price)

	async def _fetch_market_tokens(self) -> List[str]:
		"""Fetch up/down market token IDs for all supported tokens from Gamma API."""
		import aiohttp

		try:
			# Get supported tokens from config
			supported_tokens = getattr(self.config, 'supported_tokens',
			                           ['BTC', 'ETH', 'SOL', 'XRP'])
			token_patterns = {}
			for token in supported_tokens:
				token_lower = token.lower()
				patterns = [token_lower]
				# Add common name variations
				if token == "BTC":
					patterns.extend(["bitcoin"])
				elif token == "ETH":
					patterns.extend(["ethereum"])
				elif token == "SOL":
					patterns.extend(["solana"])
				elif token == "XRP":
					patterns.extend(["ripple"])
				token_patterns[token] = patterns

			timeout = aiohttp.ClientTimeout(total=15, connect=5)
			async with aiohttp.ClientSession(timeout=timeout) as session:
				url = "https://gamma-api.polymarket.com/events"
				params = {
				    "order": "id",
				    "ascending": "false",
				    "closed": "false",
				    "limit": 100
				}

				async with session.get(url, params=params) as response:
					if response.status != 200:
						logger.warning(
						    f"Failed to fetch events: {response.status}")
						return []

					events = await response.json()

					token_ids = []
					for event in events:
						title = (event.get("title") or "").lower()
						slug = (event.get("slug") or "").lower()

						# Check if this event matches any supported token pattern
						matches_token = False
						matched_token = None
						for token, patterns in token_patterns.items():
							for pattern in patterns:
								if pattern in title or pattern in slug:
									if "up" in title or "down" in title or "updown" in slug:
										matches_token = True
										matched_token = token
										break
							if matches_token:
								break

						if matches_token:
							markets = event.get("markets", [])
							for market in markets:
								clob_token_ids = market.get("clobTokenIds", "")
								if isinstance(clob_token_ids, str):
									try:
										token_list = json.loads(clob_token_ids)
										token_ids.extend(token_list)
									except:
										pass
								elif isinstance(clob_token_ids, list):
									token_ids.extend(clob_token_ids)

					tokens_str = ", ".join(supported_tokens)
					logger.info(
					    f"Found {len(token_ids)} market tokens for {tokens_str}"
					)
					return token_ids[:100]

		except Exception as e:
			logger.error(f"Error fetching market tokens: {e}")
			return []

	async def _fetch_btc_market_tokens(self) -> List[str]:
		"""Legacy method for backward compatibility."""
		return await self._fetch_market_tokens()

	def get_cached_markets(self) -> Dict[str, Any]:
		"""Get all cached market data from RTDS."""
		return self.markets_cache

	def get_best_ask(self, asset_id: str) -> Optional[float]:
		"""Get the best ask price (lowest sell price) for an asset."""
		data = self.markets_cache.get(asset_id)
		if not data:
			return None

		# Try best_ask first
		if data.get("best_ask"):
			return data["best_ask"]

		# Fallback to asks array
		asks = data.get("asks", [])
		if asks:
			sorted_asks = sorted(asks,
			                     key=lambda x: float(x.get("price", 999)))
			return float(sorted_asks[0].get("price", 0))

		# Fallback to last_price
		if data.get("last_price"):
			return data["last_price"]

		return None

	def get_best_bid(self, asset_id: str) -> Optional[float]:
		"""Get the best bid price (highest buy price) for an asset."""
		data = self.markets_cache.get(asset_id)
		if not data:
			return None

		# Try best_bid first
		if data.get("best_bid"):
			return data["best_bid"]

		# Fallback to bids array
		bids = data.get("bids", [])
		if bids:
			sorted_bids = sorted(bids,
			                     key=lambda x: float(x.get("price", 0)),
			                     reverse=True)
			return float(sorted_bids[0].get("price", 0))

		return None

	def get_instant_quote(self, asset_id: str) -> Optional[tuple]:
		"""
        Get instant bid/ask quote from RTDS cache.
        Returns (best_bid, best_ask) or None if no data.
        """
		bid = self.get_best_bid(asset_id)
		ask = self.get_best_ask(asset_id)

		if bid is not None and ask is not None:
			return (bid, ask)

		# Fallback to last_price if no order book
		data = self.markets_cache.get(asset_id)
		if data and data.get("last_price"):
			price = data["last_price"]
			return (price, price)

		return None

	async def close(self):
		"""Close the RTDS connection."""
		self.running = False
		if self.websocket:
			# Send unsubscribe before closing
			try:
				unsubscribe_msg = {"action": "unsubscribe"}
				await self.websocket.send(json.dumps(unsubscribe_msg))
			except:
				pass

			await self.websocket.close()
			logger.info("RTDS connection closed")


# Alias for backwards compatibility
PolymarketWebSocketClient = PolymarketRTDSClient


async def test_rtds():
	"""Test RTDS connection."""
	from config import config

	client = PolymarketRTDSClient(config)

	if await client.connect():
		await client.subscribe_to_markets()

		print("Listening for RTDS messages (30 seconds)...")
		print("Press Ctrl+C to stop")

		try:
			await asyncio.wait_for(client.listen(), timeout=30.0)
		except asyncio.TimeoutError:
			print("\nTimeout reached. Cached markets:")
			for asset_id, data in client.get_cached_markets().items():
				print(
				    f"  {asset_id[:20]}... bid={data.get('best_bid')} ask={data.get('best_ask')}"
				)

		await client.close()


if __name__ == "__main__":
	asyncio.run(test_rtds())
