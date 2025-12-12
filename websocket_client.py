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
		self.subscribed_without_filters: bool = False  # Track if we subscribed without filters
		self.callbacks: List[Callable] = []
		self.last_update_timestamps: Dict[str,
		                                  float] = {}  # asset_id -> timestamp
		self.connection_time: Optional[float] = None  # When we connected

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
			self.connection_time = time.time()
			logger.info(f"✅ Connected to RTDS: {self.WS_URL}")
			# Small delay to ensure connection is fully established
			await asyncio.sleep(0.1)
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
		logger.info(
		    f"🔔 subscribe_to_markets llamado con {len(asset_ids) if asset_ids else 0} asset_ids"
		)

		# If we already subscribed without filters, we don't need to resubscribe
		# Without filters, we receive ALL market data, so resubscribing is unnecessary
		# Check this FIRST before any async operations to prevent concurrent subscriptions
		if self.subscribed_without_filters and self.websocket:
			logger.info(
			    "✅ Already subscribed without filters - no need to resubscribe (receiving all market data)"
			)
			# Update tracked assets but don't resubscribe
			if asset_ids:
				self.subscribed_assets = asset_ids
			return True

		# Mark that we're about to subscribe without filters IMMEDIATELY to prevent concurrent calls
		# This prevents race conditions where multiple calls try to subscribe simultaneously
		# We always subscribe without filters now, so set this flag early
		if self.websocket:
			self.subscribed_without_filters = True

		# Ensure connection
		try:
			is_closed = self.websocket is None or self.websocket.state.name != "OPEN"
			logger.info(
			    f"🔍 WebSocket estado: websocket={self.websocket is not None}, is_closed={is_closed}"
			)
		except (AttributeError, Exception) as e:
			is_closed = self.websocket is None
			logger.warning(f"⚠️ Error checking WebSocket state: {e}")

		if is_closed:
			logger.info("RTDS disconnected, reconnecting...")
			connected = await self.connect()
			if not connected:
				logger.error("Failed to reconnect RTDS")
				return False

		# Fetch asset IDs if not provided
		if not asset_ids:
			asset_ids = await self._fetch_market_tokens()

			# Check again after fetching tokens - another call might have subscribed in the meantime
			if self.subscribed_without_filters and self.websocket:
				logger.info(
				    "✅ Already subscribed without filters (checked after fetching tokens) - no need to resubscribe"
				)
				if asset_ids:
					self.subscribed_assets = asset_ids
				return True

		if not asset_ids:
			logger.warning("No asset IDs to subscribe to")
			return False

		# Unsubscribe from previous subscriptions before subscribing to new ones
		# RTDS requires unsubscribing before changing subscription filters
		# But if we're already subscribed without filters, we don't need to unsubscribe/resubscribe
		if self.subscribed_assets and self.websocket and not self.subscribed_without_filters:
			try:
				unsubscribe_msg = {"action": "unsubscribe"}
				await self.websocket.send(json.dumps(unsubscribe_msg))
				# Reset flag when unsubscribing
				self.subscribed_without_filters = False
				# Small delay to ensure unsubscribe is processed
				await asyncio.sleep(0.1)
			except Exception as e:
				logger.warning(
				    f"Failed to unsubscribe before resubscribe: {e}")

		self.subscribed_assets = asset_ids

		# Build RTDS subscription message
		# Since filters are optional per RTDS docs, try WITHOUT filters
		# If no filters, we only need ONE subscription (not multiple batches)
		# This avoids "Invalid request body" errors from too many subscriptions
		# We'll receive all market data and filter client-side for the tokens we need

		# Create single subscription without filters
		subscriptions = [{
		    "topic": self.TOPIC_CLOB_MARKET,
		    "type": "agg_orderbook"  # Order book data (bids/asks)
		    # No filters - RTDS will send all market data, we filter client-side
		}]

		# Add authentication if available (improves rate limits)
		clob_auth = self._get_clob_auth()
		if clob_auth:
			for sub in subscriptions:
				sub["clob_auth"] = clob_auth
			logger.debug("Using authenticated RTDS subscription")

		subscribe_msg = {"action": "subscribe", "subscriptions": subscriptions}

		try:
			subscription_json = json.dumps(subscribe_msg)
			# Flag already set earlier to prevent concurrent subscriptions
			await self.websocket.send(subscription_json)

			logger.info(
			    f"📡 RTDS: Subscribed to all markets (no filters) - will filter client-side for {len(asset_ids)} tokens"
			)

			# Log asset IDs for debugging
			if asset_ids:
				logger.info(
				    f"  📋 Tracking {len(asset_ids)} asset IDs client-side")
				logger.info(f"  📋 Sample IDs: {asset_ids[:5]}")

			return True
		except Exception as e:
			logger.error(f"Failed to subscribe to RTDS: {e}")
			# Reset flag on error so we can retry
			self.subscribed_without_filters = False
			self.websocket = None
			return False

	async def listen(self, callback: Optional[Callable] = None):
		"""Listen for RTDS messages and process them."""
		self.running = True

		if callback:
			self.callbacks.append(callback)

		try:
			# Send periodic PING to keep connection alive (every 10 seconds as per Polymarket docs)
			async def ping_task():
				while self.running and self.websocket:
					try:
						await asyncio.sleep(10)
						if self.websocket and self.websocket.open:
							await self.websocket.ping()
							logger.debug(
							    "📡 Sent PING to keep connection alive")
					except Exception as e:
						logger.debug(f"PING error: {e}")
						break

			# Start ping task
			ping_task_handle = asyncio.create_task(ping_task())

			async for message in self.websocket:
				try:
					# Skip empty messages
					if not message or not message.strip():
						continue

					data = json.loads(message)

					# Log raw messages at DEBUG level with configurable limits
					# Keep counter logic (initialize if missing and increment)
					if not hasattr(self, '_raw_message_count'):
						self._raw_message_count = 0
					self._raw_message_count += 1

					# Only log if DEBUG is enabled and conditions are met
					if logger.isEnabledFor(logging.DEBUG):
						should_log = False
						limit = self.config.raw_message_log_limit
						sample_interval = self.config.raw_message_log_sample_interval

						if limit > 0:
							# Log first N messages
							if self._raw_message_count <= limit:
								should_log = True
							# After limit, sample every Mth message if sampling enabled
							elif sample_interval > 0 and self._raw_message_count % sample_interval == 0:
								should_log = True
						elif limit == -1:
							# Unlimited logging (for debugging)
							should_log = True

						if should_log:
							logger.debug(
							    f"📥 Raw message #{self._raw_message_count}: {str(message)[:500]}..."
							)

					await self._process_message(data)
				except json.JSONDecodeError as e:
					if message and message.strip():
						logger.warning(f"Failed to parse RTDS message: {e}")
				except Exception as e:
					logger.error(f"Error processing RTDS message: {e}")

			# Cancel ping task when done
			ping_task_handle.cancel()
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

        Initial snapshot format (NO topic field):
        {
            "connection_id": "...",
            "payload": [ ... ]  # List of orderbook snapshots
        }

        Also handles subscription confirmations and errors.
        """
		# Check for subscription confirmation or error messages
		if "action" in data:
			action = data.get("action")
			if action == "subscribed":
				logger.info(
				    f"✅ RTDS subscription confirmed: {json.dumps(data, indent=2)}"
				)
				return
			elif action == "error":
				logger.error(
				    f"❌ RTDS subscription error: {json.dumps(data, indent=2)}")
				return
			elif action == "unsubscribed":
				logger.info(
				    f"ℹ️ RTDS unsubscribed: {json.dumps(data, indent=2)}")
				return

		# Also check for "message" field which might indicate errors
		if "message" in data:
			msg = data.get("message")
			if "Invalid" in msg or "error" in msg.lower():
				logger.error(
				    f"❌ RTDS error message: {json.dumps(data, indent=2)}")
				return

		topic = data.get("topic")
		msg_type = data.get("type")
		payload = data.get("payload")

		# Initialize message counter if needed
		if not hasattr(self, '_message_count'):
			self._message_count = 0
		self._message_count += 1

		# CRITICAL FIX: Handle initial snapshot message that has NO topic field
		# According to RTDS protocol (see https://github.com/Polymarket/real-time-data-client):
		# When connection is established and a filter is used, the server sends an initial data dump.
		# Initial snapshot format: {"connection_id": "...", "payload": [...]}  # payload is a LIST
		# NOTE: Regular messages also have connection_id, but they have topic field OR payload is a dict with 'pc' (price_changes)
		# Reference: https://github.com/Polymarket/real-time-data-client#initial-data-dump-on-connection
		# Check for connection_id AND payload as list (true initial snapshot)
		if "connection_id" in data and topic is None and isinstance(
		    payload, list):
			if self._message_count <= 5:
				logger.info(
				    f"  📦 Initial snapshot detected: connection_id={data.get('connection_id')[:20]}..., payload is a list with {len(payload)} items"
				)

			# Process each item in the list as an orderbook snapshot
			if payload:
				for item in payload:
					if isinstance(item, dict):
						# Initial snapshots don't have topic/type, assume clob_market/agg_orderbook
						effective_topic = self.TOPIC_CLOB_MARKET
						effective_type = msg_type or "agg_orderbook"
						await self._process_single_payload(
						    effective_topic, effective_type, item)
			else:
				logger.debug("  ⚠️ Initial snapshot has empty payload list")
			return

		# Fallback: Handle payload as list even without connection_id (some RTDS variants)
		if isinstance(payload, list):
			if self._message_count <= 5:
				logger.info(
				    f"  📦 Payload is a list with {len(payload)} items (snapshot format)"
				)
			if payload:
				for item in payload:
					if isinstance(item, dict):
						# Use topic from message or default to clob_market
						effective_topic = topic or self.TOPIC_CLOB_MARKET
						effective_type = msg_type or "agg_orderbook"
						await self._process_single_payload(
						    effective_topic, effective_type, item)
			return

		# Default payload to empty dict if None
		if payload is None:
			payload = {}

		# Log first 20 messages in detail, then less frequently
		if self._message_count <= 20 or self._message_count % 50 == 0:
			logger.info(
			    f"📨 RTDS msg #{self._message_count}: topic={topic}, type={msg_type}, cache_size={len(self.markets_cache)}"
			)
			if payload and isinstance(payload, dict):
				# Log payload structure for first few messages
				if self._message_count <= 10:
					logger.info(
					    f"  📦 Payload keys: {list(payload.keys())[:10]}")
					# For price_change messages, asset_id is inside pc array, not in root
					if "pc" in payload:
						pc_list = payload.get("pc", [])
						if pc_list and isinstance(pc_list,
						                          list) and len(pc_list) > 0:
							first_pc = pc_list[0] if isinstance(
							    pc_list[0], dict) else {}
							asset_id = first_pc.get("a") or first_pc.get(
							    "asset_id")
							if asset_id:
								logger.info(
								    f"  ✅ Price change message with {len(pc_list)} updates, first asset_id: {asset_id[:50]}..."
								)
							else:
								logger.debug(
								    f"  📊 Price change message with {len(pc_list)} updates (asset_id in pc array)"
								)
					else:
						# For other message types, check root level
						asset_id = payload.get("asset_id") or payload.get("a")
						if asset_id:
							logger.info(
							    f"  ✅ Asset ID recibido: {asset_id[:50]}...")
						elif self._message_count <= 10:
							logger.debug(
							    f"  📊 Message type {msg_type} (asset_id may be nested)"
							)

		# Process single payload
		await self._process_single_payload(topic, msg_type, payload)

	async def _process_single_payload(self, topic: str, msg_type: str,
	                                  payload: Dict[str, Any]):
		"""Process a single payload dictionary."""

		# Ensure payload is a dict
		if not isinstance(payload, dict):
			if self._message_count <= 10:
				logger.warning(
				    f"  ⚠️ Payload is not a dict, type: {type(payload)}, value: {str(payload)[:100]}"
				)
			return

		# If topic is None or empty, assume it's clob_market (for initial snapshots)
		# If topic is None or empty, assume it's clob_market (for initial snapshots)
		if topic is None or topic == "":
			topic = self.TOPIC_CLOB_MARKET
		elif topic != self.TOPIC_CLOB_MARKET:
			if self._message_count <= 10:
				logger.info(f"  ⚠️ Skipping non-clob_market topic: {topic}")
			return

		# Handle different message types from clob_market topic
		# RTDS sends: price_change (singular), agg_orderbook, last_trade_price, etc.
		# If msg_type is None or empty, check payload structure to determine type
		if not msg_type:
			# Check if payload has price_changes structure (pc field indicates price_change)
			if "pc" in payload or "price_changes" in payload:
				msg_type = "price_change"  # Use singular form as per RTDS protocol
			# Check if payload has orderbook structure (bids/asks)
			elif "bids" in payload or "asks" in payload:
				msg_type = "agg_orderbook"
			elif "price" in payload and "asset_id" in payload:
				msg_type = "last_trade_price"
			else:
				# Default to orderbook for snapshots
				msg_type = "agg_orderbook"

		if msg_type == "price_change" or msg_type == "price_changes":  # Support both for compatibility
			await self._handle_price_changes(payload)
		elif msg_type == "agg_orderbook":
			await self._handle_orderbook(payload)
		elif msg_type == "last_trade_price":
			await self._handle_trade(payload)
		else:
			if self._message_count <= 10:
				payload_keys = list(payload.keys())[:5] if isinstance(
				    payload, dict) else 'not a dict'
				logger.info(
				    f"  ⚠️ Unknown message type: {msg_type} (payload keys: {payload_keys})"
				)

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

		if not price_changes:
			if self._message_count <= 10:
				logger.debug("  ⚠️ Price change message has empty pc array")
			return

		processed_count = 0
		for pc in price_changes:
			if not isinstance(pc, dict):
				continue

			# Asset ID is stored as "a" in compact format (not "asset_id")
			asset_id = pc.get("a") or pc.get("asset_id")
			if not asset_id:
				if self._message_count <= 10:
					logger.debug(
					    f"  ⚠️ Price change entry missing asset_id, keys: {list(pc.keys())}"
					)
				continue

			# Best bid/ask are stored as "bb" and "ba" in compact format
			best_bid = pc.get("bb") or pc.get("best_bid")
			best_ask = pc.get("ba") or pc.get("best_ask")

			# Initialize cache entry if needed
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

			# Update best bid/ask
			if best_bid:
				self.markets_cache[asset_id]["best_bid"] = float(best_bid)
			if best_ask:
				self.markets_cache[asset_id]["best_ask"] = float(best_ask)

			# Track last update time
			self.last_update_timestamps[asset_id] = time.time()
			processed_count += 1

		# Log successful processing for first few messages
		if self._message_count <= 5 and processed_count > 0:
			logger.info(
			    f"  ✅ Processed {processed_count}/{len(price_changes)} price changes successfully"
			)

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
			# Log for debugging when asset_id is missing
			if self._message_count <= 10:
				logger.warning(
				    f"  ⚠️ Orderbook payload missing asset_id, keys: {list(payload.keys())[:10]}"
				)
			return

		# Filter: Only process orderbooks for assets we're tracking (current up/down markets)
		# Since we subscribe without filters, we receive ALL market data, but we only want current markets
		if self.subscribed_assets and asset_id not in self.subscribed_assets:
			# Skip this orderbook - it's not one of our tracked assets (might be old/future market)
			if self._message_count <= 20:
				logger.debug(
				    f"  ⏭️ Skipping orderbook for untracked asset_id: {asset_id[:20]}... (not in subscribed_assets)"
				)
			return

		# Log successful processing for first few items
		if self._message_count <= 5:
			logger.info(
			    f"  ✅ Processing orderbook for asset_id: {asset_id[:20]}... (tracked asset)"
			)

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

		# Track last update time
		self.last_update_timestamps[asset_id] = time.time()

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
			# Track last update time
			self.last_update_timestamps[asset_id] = time.time()

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
									except (json.JSONDecodeError, TypeError):
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

	def is_connected(self) -> bool:
		"""Check if WebSocket is connected and active."""
		if not self.websocket:
			return False
		try:
			# Check WebSocket state
			state = self.websocket.state.name if hasattr(
			    self.websocket, 'state') else None
			return state == "OPEN"
		except (AttributeError, Exception):
			return False

	def is_data_fresh(self,
	                  asset_id: str,
	                  max_age_seconds: float = 60.0) -> bool:
		"""
        Check if cached data for an asset is fresh (updated within max_age_seconds).

        Default is 60 seconds to account for low-liquidity periods where markets
        may not update frequently. RTDS sends initial snapshots, but updates may
        be sparse during quiet periods.
        """
		if asset_id not in self.last_update_timestamps:
			return False
		age = time.time() - self.last_update_timestamps[asset_id]
		return age <= max_age_seconds

	def get_instant_quote(self,
	                      asset_id: str,
	                      require_fresh: bool = True) -> Optional[tuple]:
		"""
        Get instant bid/ask quote from RTDS cache.
        Returns (best_bid, best_ask) or None if no data.

        Args:
            asset_id: The asset token ID
            require_fresh: If True, only return data updated within last 60 seconds (default freshness window)
        """
		# Check if data is fresh (if required)
		if require_fresh and not self.is_data_fresh(asset_id):
			return None

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
