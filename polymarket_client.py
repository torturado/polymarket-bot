"""
Polymarket API client using the official py-clob-client SDK.
"""
import logging
import time
import requests
from datetime import datetime, timedelta, timezone
from typing import Optional

try:
	from py_clob_client.client import ClobClient
	from py_clob_client.clob_types import BookParams
	CLOB_CLIENT_AVAILABLE = True
except ImportError:
	CLOB_CLIENT_AVAILABLE = False
	logger = logging.getLogger(__name__)
	logger.warning(
	    "py-clob-client not installed. Install with: pip install py-clob-client"
	)

from config import BotConfig
from models import Market, MarketPair, MarketSide, Quote

logger = logging.getLogger(__name__)


def robust_http_get(
    url: str,
    params: dict = None,
    headers: dict = None,
    max_retries: int = 3,
    timeout: tuple = (5, 15)) -> Optional[requests.Response]:
	"""
    Perform HTTP GET with retry logic and aggressive timeout.

    Args:
        url: URL to fetch
        params: Query parameters
        headers: Optional HTTP headers (e.g., for authentication)
        max_retries: Maximum number of retry attempts
        timeout: Tuple of (connect_timeout, read_timeout) in seconds

    Returns:
        Response object or None if all retries failed
    """
	# Default headers
	default_headers = {
	    "User-Agent": "PolymarketBot/1.0",
	    "Accept": "application/json"
	}
	if headers:
		default_headers.update(headers)

	for attempt in range(max_retries):
		try:
			response = requests.get(url,
			                        params=params,
			                        headers=default_headers,
			                        timeout=timeout)
			response.raise_for_status()
			return response
		except requests.exceptions.Timeout:
			logger.warning(
			    f"HTTP timeout (attempt {attempt + 1}/{max_retries}): {url}")
			if attempt < max_retries - 1:
				time.sleep(1)  # Brief pause before retry
		except requests.exceptions.ConnectionError as e:
			logger.warning(
			    f"Connection error (attempt {attempt + 1}/{max_retries}): {e}")
			if attempt < max_retries - 1:
				time.sleep(2)  # Longer pause for connection issues
		except requests.exceptions.RequestException as e:
			logger.error(
			    f"HTTP error (attempt {attempt + 1}/{max_retries}): {e}")
			if attempt < max_retries - 1:
				time.sleep(1)

	logger.error(f"All {max_retries} HTTP attempts failed for: {url}")
	return None


class PolymarketClient:
	"""Client for fetching Polymarket market data using the official SDK and WebSocket."""

	def __init__(self, config: BotConfig):
		"""Initialize the client with configuration."""
		if not CLOB_CLIENT_AVAILABLE:
			raise ImportError(
			    "py-clob-client is required. Install with: pip install py-clob-client"
			)

		self.config = config

		# Initialize the CLOB client (read-only for market data)
		self.clob_client = ClobClient(self.config.polymarket_api_base)

		# Set Builder API credentials if available
		if config.api_key and config.api_secret and config.passphrase:
			try:
				from py_clob_client.clob_types import ApiCreds
				creds = ApiCreds(api_key=config.api_key,
				                 api_secret=config.api_secret,
				                 api_passphrase=config.passphrase)
				self.clob_client.set_api_creds(creds)
				logger.info("Polymarket Builder API credentials configured")
			except Exception as e:
				logger.warning(f"Failed to set API credentials: {e}")
		else:
			logger.debug("No Builder API credentials - using read-only access")

		# WebSocket client for real-time up/down markets
		self.ws_client = None
		try:
			import websockets
			from websocket_client import PolymarketWebSocketClient
			self.ws_client = PolymarketWebSocketClient(config)
			supported_tokens = getattr(config, 'supported_tokens',
			                           ['BTC', 'ETH', 'SOL', 'XRP'])
			tokens_str = ", ".join(supported_tokens)
			logger.info(
			    f"WebSocket client initialized for real-time {tokens_str} up/down markets"
			)
		except ImportError:
			logger.warning(
			    "WebSocket client not available. Install websockets: pip install websockets"
			)
		except Exception as e:
			logger.warning(f"Failed to initialize WebSocket client: {e}")

	def list_markets(self) -> list[Market]:
		"""
        Fetch 15-minute up/down markets for all supported tokens.

        Uses the Gamma API to get market data.
        WebSocket is used for real-time price updates, not for market discovery.
        """
		# Use API for market discovery
		return self._list_markets_from_api()

	def list_btc_markets(self) -> list[Market]:
		"""
        Legacy method for backward compatibility.
        Fetches markets for all supported tokens (not just BTC).
        """
		return self.list_markets()

	def _list_markets_from_api(self) -> list[Market]:
		"""
        Fetch 15-minute up/down markets for all supported tokens using Gamma Events API.

        Uses Gamma Events API to get full market details including question/title.
        Events endpoint provides better results for active markets.
        """
		try:
			import requests
			import json

			# Use events endpoint for better active market discovery
			events_url = "https://gamma-api.polymarket.com/events"
			markets_url = "https://gamma-api.polymarket.com/markets"

			all_markets = []
			seen_condition_ids = set()

			# Strategy 1: Fetch active events (newest first)
			logger.info("Fetching active events from Gamma API...")
			try:
				for offset in [0, 100, 200]:
					params = {
					    "order": "id",
					    "ascending": "false",  # Newest first
					    "closed": "false",
					    "limit": 100,
					    "offset": offset
					}

					response = robust_http_get(events_url, params=params)
					if not response:
						break
					events = response.json()

					if not events:
						break

					# Extract markets from events
					for event in events:
						event_markets = event.get("markets", [])
						for market in event_markets:
							cid = market.get("conditionId") or market.get(
							    "condition_id")
							if cid and cid not in seen_condition_ids:
								# Add event info to market for context
								market["_event_title"] = event.get("title", "")
								market["_event_slug"] = event.get("slug", "")
								all_markets.append(market)
								seen_condition_ids.add(cid)

					logger.debug(
					    f"Fetched {len(events)} events (offset {offset}), total markets: {len(all_markets)}"
					)

					if len(events) < 100:
						break

			except Exception as e:
				logger.warning(f"Error fetching events: {e}")

			# Strategy 2: Fetch up/down markets directly by searching for all supported tokens
			# This catches markets that might be "closed" for new positions but still active
			try:
				supported_tokens = getattr(self.config, 'supported_tokens',
				                           ['BTC', 'ETH', 'SOL', 'XRP'])
				for token in supported_tokens:
					token_lower = token.lower()
					search_params = {
					    "limit": 100,
					    "_q": f"{token_lower}-updown-15m"  # Search query
					}

					response = robust_http_get(markets_url,
					                           params=search_params)
					if not response:
						continue
					direct_markets = response.json()

					for market in direct_markets:
						cid = market.get("conditionId") or market.get(
						    "condition_id")
						if cid and cid not in seen_condition_ids:
							all_markets.append(market)
							seen_condition_ids.add(cid)

					logger.debug(
					    f"Added {len(direct_markets)} {token} markets from search endpoint"
					)

			except Exception as e:
				logger.warning(f"Error fetching direct markets: {e}")

			# Strategy 3: Calculate and fetch the CURRENT market directly by slug for all tokens
			try:
				supported_tokens = getattr(self.config, 'supported_tokens',
				                           ['BTC', 'ETH', 'SOL', 'XRP'])
				now_ts = int(datetime.now(timezone.utc).timestamp())
				current_window_start = (
				    now_ts // 900) * 900  # Round down to 15-min boundary

				for token in supported_tokens:
					token_lower = token.lower()
					current_slug = f"{token_lower}-updown-15m-{current_window_start}"

					# Try to fetch the current market directly
					response = robust_http_get(
					    f"{markets_url}?slug={current_slug}")
					if response and response.status_code == 200:
						current_markets = response.json()
						if current_markets:
							for market in (current_markets if isinstance(
							    current_markets, list) else [current_markets]):
								cid = market.get("conditionId") or market.get(
								    "condition_id")
								if cid and cid not in seen_condition_ids:
									all_markets.append(market)
									seen_condition_ids.add(cid)
									logger.info(
									    f"✅ Found current {token} market: {current_slug}"
									)

					# Also try next market
					next_window_start = current_window_start + 900
					next_slug = f"{token_lower}-updown-15m-{next_window_start}"
					response = robust_http_get(
					    f"{markets_url}?slug={next_slug}")
					if response and response.status_code == 200:
						next_markets = response.json()
						if next_markets:
							for market in (next_markets if isinstance(
							    next_markets, list) else [next_markets]):
								cid = market.get("conditionId") or market.get(
								    "condition_id")
								if cid and cid not in seen_condition_ids:
									all_markets.append(market)
									seen_condition_ids.add(cid)

			except Exception as e:
				logger.debug(f"Error fetching current markets by slug: {e}")

			# Skip fetching closed markets - focus on active ones for paper trading
			# The events endpoint already gives us active markets

			markets_data = all_markets

			if not markets_data:
				logger.warning("No markets returned from Gamma API")
				return []

			logger.debug(
			    f"Received {len(markets_data)} total markets from Gamma API")

			# Filter markets by date - only include recent markets (last 30 days or future)
			now = datetime.now(timezone.utc)
			thirty_days_ago = now - timedelta(days=30)
			recent_markets = []

			for market in markets_data:
				# Get end date
				end_date_str = (market.get("endDateISO")
				                or market.get("end_date_iso")
				                or market.get("endDate")
				                or market.get("end_date"))

				if end_date_str:
					try:
						# Parse date (handle different formats)
						if isinstance(end_date_str, str):
							end_date_str = end_date_str.replace("Z", "+00:00")
							end_date = datetime.fromisoformat(end_date_str)
							# Ensure timezone-aware
							if end_date.tzinfo is None:
								end_date = end_date.replace(
								    tzinfo=timezone.utc)
						else:
							continue

						# Only include markets that end in the future or within last 30 days
						if end_date >= thirty_days_ago:
							recent_markets.append(market)
					except (ValueError, AttributeError):
						# If we can't parse the date, include it if it's active and not resolved
						if market.get("active") and not market.get(
						    "resolved", False):
							recent_markets.append(market)
				else:
					# If no end date, include if active and not resolved
					if market.get("active") and not market.get(
					    "resolved", False):
						recent_markets.append(market)

			# Sort markets by end date (most recent first)
			def get_end_date(market):
				end_date_str = (market.get("endDateISO")
				                or market.get("end_date_iso")
				                or market.get("endDate")
				                or market.get("end_date"))
				if end_date_str:
					try:
						if isinstance(end_date_str, str):
							end_date_str = end_date_str.replace("Z", "+00:00")
							end_date = datetime.fromisoformat(end_date_str)
							# Ensure timezone-aware
							if end_date.tzinfo is None:
								end_date = end_date.replace(
								    tzinfo=timezone.utc)
							return end_date
					except (ValueError, AttributeError):
						pass
				return datetime.min.replace(
				    tzinfo=timezone.utc
				)  # Put markets without dates at the end

			recent_markets.sort(key=get_end_date, reverse=True)
			markets_data = recent_markets
			logger.debug(f"Markets after date filtering: {len(markets_data)}")

			# Filter markets by keywords and parse
			markets = []
			for market_data in markets_data:
				try:
					# Gamma API fields: question, slug, conditionId, outcomes, etc.
					question = market_data.get("question", "").lower()
					slug = market_data.get(
					    "slug", "").lower() if market_data.get("slug") else ""
					title = market_data.get(
					    "title",
					    "").lower() if market_data.get("title") else ""

					# Check for up/down 15-minute markets for all supported tokens
					# These have slugs like "btc-updown-15m-1765358100", "eth-updown-15m-1765443600", etc.
					slug = market_data.get("slug", "").lower()

					# Get supported tokens from config
					supported_tokens = getattr(self.config, 'supported_tokens',
					                           ['BTC', 'ETH', 'SOL', 'XRP'])
					token_patterns = [
					    f"{token.lower()}-updown-15m"
					    for token in supported_tokens
					]

					# Also check for common token name variations
					token_variations = {
					    "btc": ["bitcoin"],
					    "eth": ["ethereum"],
					    "sol": ["solana"],
					    "xrp": ["ripple"]
					}
					for token_lower in [t.lower() for t in supported_tokens]:
						if token_lower in token_variations:
							for variation in token_variations[token_lower]:
								token_patterns.append(
								    f"{variation}-updown-15m")

					# STRICT filter: Only match the specific 15-minute up/down markets for supported tokens
					is_supported_updown_15m = False
					matched_token = None
					for pattern in token_patterns:
						if pattern in slug:
							is_supported_updown_15m = True
							# Extract token from pattern
							matched_token = pattern.split("-")[0].upper()
							break

					# Also check generic pattern: token + updown + 15m
					if not is_supported_updown_15m:
						for token_lower in [
						    t.lower() for t in supported_tokens
						]:
							if token_lower in slug and "updown" in slug and "15m" in slug:
								is_supported_updown_15m = True
								matched_token = token_lower.upper()
								break

					# Log for debugging
					if is_supported_updown_15m and len(markets) < 10:
						logger.debug(
						    f"✅ Found {matched_token} 15m market - Slug: {slug} | "
						    f"Question: {market_data.get('question', '')[:50]}..."
						)

					# ONLY accept 15-minute up/down markets for supported tokens
					if not is_supported_updown_15m:
						continue

					# Get condition ID (Gamma API structure)
					condition_id = market_data.get("conditionId",
					                               "") or market_data.get(
					                                   "condition_id", "")

					# Parse outcomes - they might be a JSON string or array
					outcomes_raw = market_data.get("outcomes", [])
					outcomes = []
					if isinstance(outcomes_raw, str):
						try:
							outcomes = json.loads(outcomes_raw)
						except:
							outcomes = []
					elif isinstance(outcomes_raw, list):
						outcomes = outcomes_raw

					# Extract token IDs from clobTokenIds (Gamma API uses this field)
					token_ids = []
					clob_token_ids_raw = market_data.get("clobTokenIds", "")
					if isinstance(clob_token_ids_raw, str):
						try:
							token_ids = json.loads(clob_token_ids_raw)
						except:
							pass
					elif isinstance(clob_token_ids_raw, list):
						token_ids = clob_token_ids_raw

					# Fallback: try to get from outcomes if they're dicts
					if not token_ids and outcomes:
						if isinstance(outcomes[0], dict):
							token_ids = [
							    o.get("token_id") or o.get("tokenId", "")
							    for o in outcomes
							    if o.get("token_id") or o.get("tokenId")
							]

					# Determine side from outcomes
					# For "Bitcoin Up or Down" markets, outcomes are typically ["Up", "Down"]
					side = None
					outcome_name = None
					token_id = None

					# Check outcomes to determine side
					if outcomes:
						if isinstance(outcomes[0], dict):
							# Look for "Up" or "Down" in outcome names
							for idx, outcome in enumerate(outcomes):
								outcome_name_lower = (
								    outcome.get("name", "")
								    or outcome.get("title", "")).lower()
								if "up" in outcome_name_lower and "down" not in outcome_name_lower:
									side = MarketSide.UP
									outcome_name = outcome.get(
									    "name") or outcome.get("title")
									if idx < len(token_ids):
										token_id = token_ids[idx]
									break
								elif "down" in outcome_name_lower:
									side = MarketSide.DOWN
									outcome_name = outcome.get(
									    "name") or outcome.get("title")
									if idx < len(token_ids):
										token_id = token_ids[idx]
									break
						elif isinstance(outcomes[0], str):
							# Outcomes are strings (parsed from JSON string)
							for idx, outcome_str in enumerate(outcomes):
								outcome_str_lower = outcome_str.lower()
								if "up" in outcome_str_lower and "down" not in outcome_str_lower:
									side = MarketSide.UP
									outcome_name = outcome_str
									if idx < len(token_ids):
										token_id = token_ids[idx]
									break
								elif "down" in outcome_str_lower:
									side = MarketSide.DOWN
									outcome_name = outcome_str
									if idx < len(token_ids):
										token_id = token_ids[idx]
									break

					# Fallback: determine from question/title
					if side is None:
						if "down" in question and "up" not in question:
							side = MarketSide.DOWN
						elif "up" in question:
							side = MarketSide.UP
						else:
							# Default to UP if we can't determine (will be paired later)
							side = MarketSide.UP
						# Use first token as fallback
						if token_ids:
							token_id = token_ids[0]

					# Parse end date if available (Gamma API uses endDateISO or end_date_iso)
					end_date = None
					end_date_str = (market_data.get("endDateISO")
					                or market_data.get("end_date_iso")
					                or market_data.get("end_date"))
					if end_date_str:
						try:
							end_date = datetime.fromisoformat(
							    end_date_str.replace("Z", "+00:00"))
						except (ValueError, AttributeError):
							pass

					# Fallback 1: Parse timestamp from slug (most reliable)
					# Format: {token}-updown-15m-{timestamp} -> timestamp is START time, add 900 for end
					if not end_date and slug:
						import re
						# Match any supported token pattern
						supported_tokens = getattr(
						    self.config, 'supported_tokens',
						    ['BTC', 'ETH', 'SOL', 'XRP'])
						token_patterns = "|".join(
						    [token.lower() for token in supported_tokens])
						slug_match = re.search(
						    rf'({token_patterns})-updown-15m-(\d+)', slug)
						if slug_match:
							try:
								start_timestamp = int(slug_match.group(2))
								# End time is start + 15 minutes (900 seconds)
								end_timestamp = start_timestamp + 900
								end_date = datetime.fromtimestamp(
								    end_timestamp, tz=timezone.utc)
							except Exception:
								pass

					# Fallback 2: parse end time from question/title
					# Format: "Bitcoin Up or Down - December 11, 4:15AM-4:30AM ET"
					if not end_date:
						import re
						question_text = market_data.get(
						    "question", "") or market_data.get("title", "")
						# Pattern: "December 11, 4:15AM-4:30AM ET" -> extract end time (4:30AM)
						time_match = re.search(
						    r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d+),?\s+[\d:]+[AP]M-(\d+):(\d+)([AP]M)\s+ET',
						    question_text)
						if time_match:
							try:
								month_name = time_match.group(1)
								day = int(time_match.group(2))
								end_hour = int(time_match.group(3))
								end_minute = int(time_match.group(4))
								am_pm = time_match.group(5)

								# Convert to 24h
								if am_pm == "PM" and end_hour != 12:
									end_hour += 12
								elif am_pm == "AM" and end_hour == 12:
									end_hour = 0

								# Get month number
								months = {
								    "January": 1,
								    "February": 2,
								    "March": 3,
								    "April": 4,
								    "May": 5,
								    "June": 6,
								    "July": 7,
								    "August": 8,
								    "September": 9,
								    "October": 10,
								    "November": 11,
								    "December": 12
								}
								month = months.get(month_name, 1)

								# Assume current year, ET timezone (UTC-5)
								from zoneinfo import ZoneInfo
								et_tz = ZoneInfo("America/New_York")
								now = datetime.now(timezone.utc)
								year = now.year

								# Create datetime in ET, then convert to UTC
								end_date_et = datetime(year,
								                       month,
								                       day,
								                       end_hour,
								                       end_minute,
								                       tzinfo=et_tz)
								end_date = end_date_et.astimezone(timezone.utc)
							except Exception:
								pass

					# Use token_id as market_id for CLOB API, fallback to condition_id
					# Token ID fallback if not set earlier
					if not token_id and token_ids:
						if len(token_ids) >= 2:
							token_id = token_ids[
							    0] if side == MarketSide.UP else token_ids[1]
						elif len(token_ids) == 1:
							token_id = token_ids[0]
					market_id = token_id or condition_id or ""

					if not market_id:
						continue  # Skip if we can't identify the market

					# For BTC up/down markets, we need to create entries for both sides
					# if the question contains both "up" and "down"
					# Otherwise, create a single entry for the detected side

					# Create market entry
					# Get question/title for the market
					market_question = market_data.get(
					    "question") or market_data.get("title", "")

					market = Market(
					    market_id=market_id,
					    question=market_question,
					    condition_id=condition_id,
					    token_id=token_id,
					    side=side,
					    end_date=end_date,
					    is_resolved=market_data.get("resolved", False)
					    or market_data.get("closed", False),
					    resolution=None  # Will be set if resolved
					)
					markets.append(market)

					# For BTC up/down markets, create entry for the opposite side if we have both outcomes
					if outcomes and len(outcomes) >= 2 and condition_id:
						opposite_side = MarketSide.DOWN if side == MarketSide.UP else MarketSide.UP

						# Find token_id for opposite side
						opposite_token_id = None
						if isinstance(outcomes[0], dict):
							for outcome in outcomes:
								outcome_name_lower = (
								    outcome.get("name", "")
								    or outcome.get("title", "")).lower()
								outcome_token_id = outcome.get(
								    "token_id") or outcome.get("tokenId")

								if opposite_side == MarketSide.UP and "up" in outcome_name_lower and "down" not in outcome_name_lower:
									opposite_token_id = outcome_token_id
									break
								elif opposite_side == MarketSide.DOWN and "down" in outcome_name_lower:
									opposite_token_id = outcome_token_id
									break
						elif isinstance(outcomes[0],
						                str) and len(token_ids) >= 2:
							# Outcomes are strings, use index
							for idx, outcome_str in enumerate(outcomes):
								outcome_str_lower = outcome_str.lower()
								if idx < len(token_ids):
									if opposite_side == MarketSide.UP and "up" in outcome_str_lower and "down" not in outcome_str_lower:
										opposite_token_id = token_ids[idx]
										break
									elif opposite_side == MarketSide.DOWN and "down" in outcome_str_lower:
										opposite_token_id = token_ids[idx]
										break

						# Fallback: use opposite index
						if not opposite_token_id and len(token_ids) >= 2:
							opposite_token_id = token_ids[
							    1] if side == MarketSide.UP else token_ids[0]

						if opposite_token_id:
							opposite_market_id = opposite_token_id or condition_id

							# Only add if we don't already have this side
							if not any(m.condition_id == condition_id
							           and m.side == opposite_side
							           for m in markets):
								opposite_market = Market(
								    market_id=opposite_market_id,
								    question=market_question,
								    condition_id=condition_id,
								    token_id=opposite_token_id,
								    side=opposite_side,
								    end_date=end_date,
								    is_resolved=market_data.get(
								        "resolved", False)
								    or market_data.get("closed", False),
								    resolution=None)
								markets.append(opposite_market)
				except Exception as e:
					logger.warning(f"Failed to parse market: {e}",
					               exc_info=True)
					continue

			supported_tokens = getattr(self.config, 'supported_tokens',
			                           ['BTC', 'ETH', 'SOL', 'XRP'])
			tokens_str = ", ".join(supported_tokens)
			logger.info(
			    f"Successfully fetched {len(markets)} markets for {tokens_str}"
			)
			return markets
		except Exception as e:
			logger.error(f"Failed to fetch markets: {e}", exc_info=True)
			return []

	def get_market_quote(self, market_id: str,
	                     side: MarketSide) -> Optional[Quote]:
		"""
        Get current price quote for a market using token_id.

        Uses get_price() or get_midpoint() from the CLOB API.
        """
		try:
			# market_id should be a token_id for CLOB API
			token_id = market_id

			# Get price based on side
			# For UP side, get BUY price (buying YES shares)
			# For DOWN side, get SELL price (buying NO shares)
			side_str = "BUY" if side == MarketSide.UP else "SELL"

			try:
				price_data = self.clob_client.get_price(token_id,
				                                        side=side_str)
				price = float(price_data.get("price", 0))
			except Exception:
				# Fallback to midpoint if specific side price unavailable
				midpoint_data = self.clob_client.get_midpoint(token_id)
				price = float(midpoint_data.get("mid", 0))

			if price <= 0 or price > 1:
				logger.debug(f"Invalid price {price} for {token_id}")
				return None

			return Quote(market_id=market_id,
			             side=side,
			             price=price,
			             timestamp=datetime.now())
		except Exception as e:
			logger.warning(f"Failed to fetch quote for {market_id}: {e}")
			return None

	def get_market_by_condition_id(self, condition_id: str) -> Optional[dict]:
		"""
        Get a specific market by condition ID using the CLOB API.

        Useful for fetching details of a specific BTC up/down market.
        """
		try:
			market = self.clob_client.get_market(condition_id)
			return market
		except Exception as e:
			logger.error(f"Failed to fetch market {condition_id}: {e}")
			return None

	def find_market_pairs(self, only_current: bool = True) -> list[MarketPair]:
		"""
        Find and pair UP/DOWN markets for all supported tokens' 15-minute markets.

        Args:
            only_current: If True, only return the market pair ending soonest (current active).
                         If False, return all market pairs.
        """
		markets = self.list_markets()

		# Group markets by condition_id (markets with same condition_id are UP/DOWN pairs)
		market_groups = {}

		for market in markets:
			condition_id = market.condition_id
			if not condition_id:
				continue

			if condition_id not in market_groups:
				market_groups[condition_id] = {
				    "up": None,
				    "down": None,
				    "end_date": market.end_date
				}

			if market.side == MarketSide.UP:
				market_groups[condition_id]["up"] = market
			else:
				market_groups[condition_id]["down"] = market

			# Update end_date if this market has one
			if market.end_date:
				market_groups[condition_id]["end_date"] = market.end_date

		# Create pairs
		pairs = []
		now = datetime.now(timezone.utc)

		for condition_id, markets_dict in market_groups.items():
			up_market = markets_dict["up"]
			down_market = markets_dict["down"]
			end_date = markets_dict.get("end_date")

			if up_market and down_market:
				# Skip if already resolved
				if up_market.is_resolved or down_market.is_resolved:
					continue

				# Skip if end_date is in the past
				if end_date and end_date < now:
					continue

				pair_id = f"{condition_id}"
				pair = MarketPair(up_market=up_market,
				                  down_market=down_market,
				                  pair_id=pair_id)
				# Store end_date for sorting
				pair._end_date = end_date
				pairs.append(pair)

		# Calculate what the current markets SHOULD be based on timestamp
		now_ts = int(now.timestamp())
		current_window_start = (now_ts //
		                        900) * 900  # Round down to 15-min boundary
		current_window_end = current_window_start + 900
		supported_tokens = getattr(self.config, 'supported_tokens',
		                           ['BTC', 'ETH', 'SOL', 'XRP'])
		expected_slugs = [
		    f"{token.lower()}-updown-15m-{current_window_start}"
		    for token in supported_tokens
		]
		expected_end = datetime.fromtimestamp(current_window_end,
		                                      tz=timezone.utc)

		logger.info(
		    f"📍 Current time: {now.strftime('%H:%M:%S UTC')} | Expected markets: {', '.join(expected_slugs)} (ends {expected_end.strftime('%H:%M:%S UTC')})"
		)

		# Sort by end_date (soonest first) - only consider markets ending in the future
		valid_pairs = []
		for p in pairs:
			if hasattr(p, '_end_date') and p._end_date:
				# Only include if ends in the future (with 30 second buffer for execution)
				if p._end_date > now - timedelta(seconds=30):
					valid_pairs.append(p)
			else:
				# No end_date, include anyway but will be sorted to end
				valid_pairs.append(p)

		pairs = valid_pairs
		pairs.sort(key=lambda p: p._end_date if hasattr(p, '_end_date') and p.
		           _end_date else datetime.max.replace(tzinfo=timezone.utc))

		logger.info(f"Found {len(pairs)} active market pairs")

		# Debug: show first 3 pairs with their end times
		if pairs and logger.isEnabledFor(logging.DEBUG):
			for i, p in enumerate(pairs[:3]):
				ed = p._end_date if hasattr(p, '_end_date') else None
				ed_str = ed.strftime(
				    "%Y-%m-%d %H:%M UTC") if ed else "No end date"
				logger.debug(
				    f"  #{i+1}: {p.up_market.question[:50]}... | End: {ed_str}"
				)

		# If only_current, filter to just the market ending soonest
		if only_current and pairs:
			current_pair = pairs[0]
			end_time = current_pair._end_date if hasattr(
			    current_pair, '_end_date') else None

			if end_time:
				time_remaining = end_time - now
				minutes_left = int(time_remaining.total_seconds() // 60)
				seconds_left = int(time_remaining.total_seconds() % 60)

				# If market has ended, try next one
				if time_remaining.total_seconds() < 0:
					logger.info(f"⏰ Market ended, looking for next...")
					for p in pairs[1:]:
						p_end = p._end_date if hasattr(p,
						                               '_end_date') else None
						if p_end and p_end > now:
							current_pair = p
							time_remaining = p_end - now
							minutes_left = int(
							    time_remaining.total_seconds() // 60)
							seconds_left = int(time_remaining.total_seconds() %
							                   60)
							break

				# Extract token from market question/slug for better logging
				market_question = current_pair.up_market.question
				token_info = ""
				for token in getattr(self.config, 'supported_tokens',
				                     ['BTC', 'ETH', 'SOL', 'XRP']):
					if token.lower() in market_question.lower() or token.lower(
					) in (current_pair.up_market.market_id or "").lower():
						token_info = f"[{token}] "
						break

				logger.info(
				    f"🎯 TRACKING {token_info}{current_pair.up_market.question[:50]}... | "
				    f"Ends in {minutes_left}m {seconds_left}s | "
				    f"Condition: {current_pair.pair_id[:16]}...")
			else:
				logger.info(
				    f"🎯 TRACKING: {current_pair.up_market.question[:60]}... (no end date)"
				)

			return [current_pair]

		return pairs

	def get_pair_quotes(self,
	                    pair: MarketPair) -> Optional[tuple[Quote, Quote]]:
		"""Get quotes for both sides of a market pair using CLOB API."""
		up_quote = self.get_market_quote(pair.up_market.market_id,
		                                 MarketSide.UP)
		down_quote = self.get_market_quote(pair.down_market.market_id,
		                                   MarketSide.DOWN)

		if up_quote and down_quote:
			return (up_quote, down_quote)

		return None
