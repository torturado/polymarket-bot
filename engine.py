"""
Paper-trading engine that orchestrates data fetching, strategy execution, and PnL tracking.
"""
import asyncio
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Optional
import threading

try:
	import websockets
	import websockets.exceptions
except ImportError:
	websockets = None

from config import BotConfig
from models import (
    MarketPair,
    PairQuote,
    Position,
    PositionStatus,
    MarketSide,
    PnLRecord,
    Quote,
)
from polymarket_client import PolymarketClient
from strategy import LegInStrategy
from reporting import Reporter

logger = logging.getLogger(__name__)


class PaperTradingEngine:
	"""Main engine for paper-trading simulation."""

	def __init__(self, config: BotConfig):
		"""Initialize the engine with configuration."""
		self.config = config
		self.client = PolymarketClient(config)
		self.strategy = LegInStrategy(config)
		self.reporter = Reporter(config)

		# State
		self.positions: dict[str, Position] = {}  # position_id -> Position
		self.market_pairs: list[MarketPair] = []
		self.pnl_history: list[PnLRecord] = []
		self.running = False

		# Statistics
		self.total_trades = 0
		self.total_realized_pnl = 0.0
		self._iteration_count = 0

	def initialize(self):
		"""Initialize the engine by fetching market pairs and starting WebSocket."""
		logger.info("Initializing engine...")

		# Start WebSocket client for real-time up/down markets
		if self.client.ws_client:
			self._start_websocket()

		try:
			# Get ALL active market pairs to trade on all tokens simultaneously
			self.market_pairs = self.client.find_market_pairs(
			    only_current=False)

			if not self.market_pairs:
				logger.warning(
				    "No active market pairs found. Waiting for next market to start..."
				)
			else:
				# CRITICAL: Subscribe WebSocket to tokens from loaded market pairs
				# This ensures we're subscribed to the correct, current tokens
				if self.client.ws_client:
					logger.info(
					    f"Subscribing WebSocket to tokens from {len(self.market_pairs)} market pairs..."
					)
					self._resubscribe_websocket_all_pairs(self.market_pairs)
		except Exception as e:
			logger.error(f"Failed to initialize market pairs: {e}",
			             exc_info=True)
			self.market_pairs = []

	def _start_websocket(self):
		"""Start WebSocket client in a separate thread with auto-reconnect."""

		async def ws_loop():
			reconnect_delay = 5  # Start with 5 seconds
			max_reconnect_delay = 60  # Max 60 seconds

			while self.running:
				try:
					# Connect
					connected = await self.client.ws_client.connect()
					if not connected:
						logger.warning(
						    f"WebSocket connection failed, retrying in {reconnect_delay}s..."
						)
						await asyncio.sleep(reconnect_delay)
						reconnect_delay = min(reconnect_delay * 2,
						                      max_reconnect_delay)
						continue

					# Reset reconnect delay on successful connection
					reconnect_delay = 5

					# Don't subscribe initially - wait for market pairs to be loaded
					# Initial subscription will happen in initialize() after market_pairs are loaded
					logger.info(
					    "WebSocket connected, waiting for market pairs to subscribe..."
					)
					success = True  # Connection successful, subscription happens later
					if success:
						logger.info(
						    "✅ WebSocket subscribed to markets successfully")
						await self.client.ws_client.listen()
					else:
						logger.warning(
						    "Failed to subscribe to WebSocket markets")
						await asyncio.sleep(reconnect_delay)
						reconnect_delay = min(reconnect_delay * 2,
						                      max_reconnect_delay)
						continue

				except Exception as e:
					error_type = type(e).__name__
					if websockets and isinstance(
					    e, websockets.exceptions.ConnectionClosed):
						logger.warning(
						    f"WebSocket connection closed, reconnecting in {reconnect_delay}s..."
						)
					else:
						logger.error(f"WebSocket error ({error_type}): {e}",
						             exc_info=True)
					await asyncio.sleep(reconnect_delay)
					reconnect_delay = min(reconnect_delay * 2,
					                      max_reconnect_delay)

		def run_ws():
			loop = asyncio.new_event_loop()
			asyncio.set_event_loop(loop)
			loop.run_until_complete(ws_loop())

		self.ws_thread = threading.Thread(target=run_ws, daemon=True)
		self.ws_thread.start()
		logger.info(
		    "WebSocket client started for real-time market data (with auto-reconnect)"
		)

	def run(self):
		"""Run the main trading loop."""
		self.running = True
		self.initialize()

		logger.info("Starting paper-trading loop...")

		while self.running:
			try:
				self._iteration()
			except Exception as e:
				logger.error(f"Error in trading loop: {e}", exc_info=True)

			# Sleep before next iteration
			time.sleep(self.config.polling_interval_seconds)

	def _iteration(self):
		"""
        Execute one iteration of the trading loop - SIMPLIFIED.

        Only refreshes markets when:
        - No markets loaded (startup)
        - Current market expired
        - Current market about to expire (within 5 seconds)

        This avoids rate limiting by not polling the API constantly.
        WebSocket provides real-time price data, so we only need to refresh
        market metadata when markets change (every 15 minutes or hourly).
        """
		now = datetime.now(timezone.utc)

		# Check if we need to refresh markets
		should_refresh = False

		if not self.market_pairs:
			# No markets loaded - refresh immediately
			should_refresh = True
		else:
			current_pair = self.market_pairs[0]

			# Check if current market expired or is about to expire
			if hasattr(current_pair, '_end_date') and current_pair._end_date:
				if current_pair._end_date < now:
					logger.info("⏰ Mercado expirado, buscando siguiente...")
					should_refresh = True
				else:
					# Check if market is about to expire (within 5 seconds)
					# This ensures we refresh right before the market ends to catch the next one
					time_left = (current_pair._end_date - now).total_seconds()
					if time_left <= 5:
						logger.info(
						    f"⏰ Mercado expira en {time_left:.0f}s, refrescando..."
						)
						should_refresh = True

		# Refresh markets if needed
		if should_refresh:
			try:
				new_pairs = self.client.find_market_pairs(only_current=False)
				if new_pairs:
					# Check if token IDs changed (simple comparison)
					old_tokens = set()
					for p in self.market_pairs:
						if p.up_market.token_id:
							old_tokens.add(p.up_market.token_id)
						if p.down_market.token_id:
							old_tokens.add(p.down_market.token_id)
						# Also check market_id as fallback
						if not p.up_market.token_id and p.up_market.market_id:
							old_tokens.add(p.up_market.market_id)
						if not p.down_market.token_id and p.down_market.market_id:
							old_tokens.add(p.down_market.market_id)

					new_tokens = set()
					for p in new_pairs:
						if p.up_market.token_id:
							new_tokens.add(p.up_market.token_id)
						if p.down_market.token_id:
							new_tokens.add(p.down_market.token_id)
						# Also check market_id as fallback
						if not p.up_market.token_id and p.up_market.market_id:
							new_tokens.add(p.up_market.market_id)
						if not p.down_market.token_id and p.down_market.market_id:
							new_tokens.add(p.down_market.market_id)

					# If tokens changed OR if we had no markets before, resubscribe WebSocket
					if old_tokens != new_tokens or not self.market_pairs:
						if old_tokens != new_tokens:
							logger.info(
							    f"🔄 Cambio de mercado detectado: {len(new_pairs)} pairs activos"
							)
							logger.info(
							    f"   Tokens antiguos: {len(old_tokens)}, Tokens nuevos: {len(new_tokens)}"
							)
						else:
							logger.info(
							    f"🔌 Suscribiendo WebSocket a {len(new_pairs)} pairs (primera vez)"
							)

						if self.client.ws_client:
							self._resubscribe_websocket_all_pairs(new_pairs)

					self.market_pairs = new_pairs
			except Exception as e:
				logger.error(f"Error refrescando mercados: {e}", exc_info=True)

		# Trading logic (uses WebSocket cache only)
		self._update_positions()
		self._check_entry_opportunities()
		self._check_resolved_markets()
		self._record_pnl_snapshot()

		self._iteration_count += 1

	def _resubscribe_websocket_all_pairs(self, pairs: list[MarketPair]):
		"""
        SIMPLIFIED: Just collect token IDs from pairs and resubscribe WebSocket.

        CRITICAL: Includes "grace period" for recently expired markets to avoid
        race condition where API hasn't indexed new market yet. This keeps the
        WebSocket subscribed during market transitions.
        """
		try:
			now = datetime.now(timezone.utc)
			# Grace period: keep listening to expired markets for 10 minutes
			# This prevents empty subscriptions during market transitions when
			# API hasn't indexed the new market yet
			grace_period_seconds = 600  # 10 minutes
			grace_cutoff = now - timedelta(seconds=grace_period_seconds)

			# Filter: include active pairs + recently expired pairs (grace period)
			active_pairs = []
			grace_period_pairs = []
			for pair in pairs:
				if pair.up_market.is_resolved or pair.down_market.is_resolved:
					continue
				if hasattr(pair, '_end_date') and pair._end_date:
					# Include if market hasn't expired OR expired within grace period
					if pair._end_date < grace_cutoff:
						# Market expired too long ago, skip it
						continue
					elif pair._end_date < now:
						# Market expired but within grace period - include for WebSocket but mark it
						grace_period_pairs.append(pair)
						active_pairs.append(pair)
						continue
				active_pairs.append(pair)

			# Log if we're including grace period markets
			if grace_period_pairs:
				logger.info(
				    f"⏳ Incluyendo {len(grace_period_pairs)} mercados en periodo de gracia "
				    f"(expiraron hace <{grace_period_seconds//60}min) para mantener suscripción durante transición"
				)

			# Sort by end_date (soonest first)
			active_pairs.sort(
			    key=lambda p: p._end_date if hasattr(p, '_end_date') and p.
			    _end_date else datetime.max.replace(tzinfo=timezone.utc))

			# Limit to 20 pairs (40 tokens max)
			current_pairs = active_pairs[:20]

			# Collect token IDs - SIMPLE!
			all_tokens = []
			for pair in current_pairs:
				up_token = pair.up_market.token_id or pair.up_market.market_id
				down_token = pair.down_market.token_id or pair.down_market.market_id
				if up_token:
					all_tokens.append(up_token)
				if down_token:
					all_tokens.append(down_token)

			# Remove duplicates
			all_tokens = list(set(all_tokens))

			if not all_tokens:
				logger.warning(
				    f"⚠️ No tokens found in {len(current_pairs)} pairs!")
				return

			logger.info(
			    f"🔌 Resuscribiendo WebSocket a {len(all_tokens)} tokens de {len(current_pairs)} markets"
			)

			# Resubscribe - wait for WebSocket to be connected
			if self.client.ws_client:
				loop = asyncio.new_event_loop()

				async def resubscribe():
					try:
						# Wait for WebSocket to be connected (max 10 seconds)
						max_wait = 10
						waited = 0
						while waited < max_wait:
							if (self.client.ws_client.websocket and hasattr(
							    self.client.ws_client.websocket, 'state')
							    and self.client.ws_client.websocket.state.name
							    == "OPEN"):
								break
							await asyncio.sleep(0.5)
							waited += 0.5

						if waited >= max_wait:
							logger.warning(
							    "⚠️ WebSocket not connected after waiting, attempting to connect..."
							)
							# Try to connect if not connected
							connected = await self.client.ws_client.connect()
							if not connected:
								logger.warning(
								    "⚠️ Failed to connect WebSocket")
								return

						# Now subscribe
						success = await self.client.ws_client.subscribe_to_markets(
						    all_tokens)
						if success:
							logger.info(
							    f"✅ WebSocket resubscribed to {len(all_tokens)} tokens"
							)
						else:
							logger.warning(
							    f"⚠️ WebSocket resubscription failed")
					except Exception as e:
						logger.error(f"Error in resubscribe: {e}",
						             exc_info=True)

				def run_async():
					asyncio.set_event_loop(loop)
					loop.run_until_complete(resubscribe())

				thread = threading.Thread(target=run_async, daemon=True)
				thread.start()
			else:
				logger.warning("⚠️ WebSocket client not initialized")

		except Exception as e:
			logger.error(f"Error resuscribiendo WebSocket: {e}")

	def _update_positions(self):
		"""Update unrealized PnL for open positions."""
		for position in self.positions.values():
			if position.status != PositionStatus.OPEN:
				continue

			# Get current quotes
			pair = self._find_pair_by_id(position.pair_id)
			if not pair:
				continue

			quotes = self.client.get_pair_quotes(pair)
			if not quotes:
				continue

			up_quote, down_quote = quotes
			self.strategy.update_position_unrealized_pnl(
			    position, up_quote.price, down_quote.price)

	def _get_instant_quotes(self, pair: MarketPair) -> Optional[tuple]:
		"""
        SIMPLIFIED: Get quotes ONLY from WebSocket cache (no HTTP fallback).
        WebSocket provides real-time data - if it's not available, wait for it.
        """
		if not self.client.ws_client or not self.client.ws_client.is_connected(
		):
			return None

		up_token = pair.up_market.token_id or pair.up_market.market_id
		down_token = pair.down_market.token_id or pair.down_market.market_id

		# Get fresh data from WebSocket cache (relax freshness requirement slightly)
		# Try with fresh first, then without freshness requirement as fallback
		up_data = self.client.ws_client.get_instant_quote(up_token,
		                                                  require_fresh=True)
		down_data = self.client.ws_client.get_instant_quote(down_token,
		                                                    require_fresh=True)

		# If no fresh data, try without freshness requirement (data might be slightly old but valid)
		if not up_data:
			up_data = self.client.ws_client.get_instant_quote(
			    up_token, require_fresh=False)
		if not down_data:
			down_data = self.client.ws_client.get_instant_quote(
			    down_token, require_fresh=False)

		# Debug: Log when we can't get quotes (only occasionally to avoid spam)
		if (not up_data or not down_data) and self._iteration_count % 30 == 0:
			cache_size = len(self.client.ws_client.markets_cache
			                 ) if self.client.ws_client else 0
			up_in_cache = up_token in (self.client.ws_client.markets_cache
			                           if self.client.ws_client else {})
			down_in_cache = down_token in (self.client.ws_client.markets_cache
			                               if self.client.ws_client else {})
			# Show sample of cached asset IDs for debugging
			sample_ids = list(self.client.ws_client.markets_cache.keys())[:3] if self.client.ws_client and self.client.ws_client.markets_cache else []

			logger.debug(
			    f"⚠️ No quotes: UP token={up_token[:20]}... in_cache={up_in_cache}, "
			    f"DOWN token={down_token[:20]}... in_cache={down_in_cache}, "
			    f"cache_size={cache_size}, sample_ids={sample_ids}")

		if up_data and down_data:
			up_bid, up_ask = up_data
			down_bid, down_ask = down_data

			up_quote = Quote(market_id=pair.up_market.market_id,
			                 side=MarketSide.UP,
			                 price=up_ask,
			                 timestamp=datetime.now())
			down_quote = Quote(market_id=pair.down_market.market_id,
			                   side=MarketSide.DOWN,
			                   price=down_ask,
			                   timestamp=datetime.now())
			return (up_quote, down_quote)

		return None

	def _check_entry_opportunities(self):
		"""Check for entry opportunities using Leg-In strategy."""
		# Categorize positions
		pending_positions = [
		    p for p in self.positions.values() if p.status in
		    [PositionStatus.PENDING_UP, PositionStatus.PENDING_DOWN]
		]
		open_positions = [
		    p for p in self.positions.values()
		    if p.status == PositionStatus.OPEN
		]
		all_active = pending_positions + open_positions

		total_capital_deployed = sum(p.total_cost_usd for p in all_active)
		available_capital = self.config.max_capital - total_capital_deployed

		best_quote = None

		# Filter to only ACTIVE markets (not expired, not resolved)
		# NOTE: We keep expired markets in WebSocket subscription (grace period),
		# but we don't trade on them - only listen for resolution
		now = datetime.now(timezone.utc)

		active_pairs = []
		for pair in self.market_pairs:
			# Skip if already resolved
			if pair.up_market.is_resolved or pair.down_market.is_resolved:
				continue

			# Skip if end_date is in the past (don't trade on expired markets)
			# But keep them in WebSocket subscription for resolution data
			if hasattr(pair, '_end_date') and pair._end_date:
				if pair._end_date < now:
					continue  # Don't trade, but WebSocket still subscribed (grace period)

			active_pairs.append(pair)

		# Sort by end_date (soonest first) to prioritize urgent markets
		active_pairs.sort(
		    key=lambda p: p._end_date if hasattr(p, '_end_date') and p.
		    _end_date else datetime.max.replace(tzinfo=timezone.utc))

		# Check ALL active pairs to find the best opportunity
		# Don't limit - we want to find the best price across all markets
		pairs_to_check = active_pairs if active_pairs else self.market_pairs[:4]

		# Log progress every 10 iterations
		if self._iteration_count % 10 == 0:
			ws_status = "disconnected"
			if self.client.ws_client:
				ws_status = "connected" if self.client.ws_client.is_connected(
				) else "disconnected"
				cache_size = len(
				    self.client.ws_client.markets_cache
				) if self.client.ws_client.markets_cache else 0
				ws_status = f"{ws_status} ({cache_size} tokens)"
			#logger.info(
			#    f"🔄 Iteration {self._iteration_count}: Checking {len(pairs_to_check)}/{len(self.market_pairs)} pairs | WS: {ws_status} | Positions: {len(self.positions)}"
			#)

		quotes_found = 0
		quotes_missing = 0

		# FIRST PASS: Get quotes for ALL pairs and sort by best opportunity (combined_price)
		# This ensures we process the best opportunities first (e.g., 0.230 vs 0.510)
		all_pair_quotes = []  # Store (pair, pair_quote) for sorting

		for pair in pairs_to_check:
			# Get quotes - WebSocket first, then HTTP fallback
			quotes = self._get_instant_quotes(pair)
			if not quotes:
				quotes_missing += 1
				continue

			quotes_found += 1
			up_quote, down_quote = quotes
			pair_quote = PairQuote.create(pair, up_quote, down_quote)
			all_pair_quotes.append((pair, pair_quote))

			# Track best quote for logging
			if best_quote is None or pair_quote.combined_price < best_quote.combined_price:
				best_quote = pair_quote

		# Sort by combined_price (best opportunities first - lowest combined = best)
		all_pair_quotes.sort(key=lambda x: x[1].combined_price)

		# Log best opportunities found (every 10 iterations)
		if all_pair_quotes and self._iteration_count % 10 == 0:
			best_5 = all_pair_quotes[:5]
			logger.info(
			    f"🏆 Top 5 opportunities (de {len(all_pair_quotes)} con quotes):"
			)
			for i, (p, pq) in enumerate(best_5, 1):
				logger.info(
				    f"  {i}. Combined=${pq.combined_price:.3f} | "
				    f"UP=${pq.up_quote.price:.3f} DOWN=${pq.down_quote.price:.3f} | "
				    f"Pair={p.pair_id[:8]}...")

		# SECOND PASS: Process pairs starting with the BEST opportunities
		for pair, pair_quote in all_pair_quotes:
			# Check SCRATCH logic for pending positions FIRST (emergency exit)
			self._process_scratch_logic(pair, pair_quote)

			# Refresh pending positions list after potential scratches
			pending_positions = [
			    p for p in self.positions.values() if p.status in
			    [PositionStatus.PENDING_UP, PositionStatus.PENDING_DOWN]
			]

			# Check bail-out for pending positions (use smart scratch logic)
			for position in pending_positions:
				if position.pair_id == pair.pair_id:
					if self.strategy.should_bail_out(position, pair_quote):
						logger.warning(
						    f"🚨 BAIL OUT - Position {position.position_id[:8]}... | "
						    f"UP: ${pair_quote.up_quote.price:.3f} DOWN: ${pair_quote.down_quote.price:.3f}"
						)
						# Use smart scratch execution (recovers real market value)
						self._execute_scratch(position,
						                      pair_quote,
						                      reason="BAIL_OUT_CRASH")
						continue

			# Step 1: Check if any pending position can be completed
			for position in pending_positions:
				if position.pair_id == pair.pair_id:
					if self.strategy.should_complete_position(
					    position, pair_quote):
						if self.strategy.complete_position(
						    position, pair_quote, available_capital):
							self.total_trades += 1
							self.reporter.log_trade(position, "COMPLETE")
							# Update available capital
							total_capital_deployed = sum(
							    p.total_cost_usd
							    for p in self.positions.values()
							    if p.status != PositionStatus.RESOLVED)
							available_capital = self.config.max_capital - total_capital_deployed

			# Step 2: Check if we should buy first leg
			# Only skip if we have a PENDING position in this pair (needs capital to complete)
			# Allow new entries even if we have OPEN positions (already completed, don't need capital)
			if any(p.pair_id == pair.pair_id and p.status in
			       [PositionStatus.PENDING_UP, PositionStatus.PENDING_DOWN]
			       for p in self.positions.values()):
				continue

			# Calculate time remaining for this market
			time_remaining = None
			if hasattr(pair, '_end_date') and pair._end_date:
				now = datetime.now(timezone.utc)
				time_remaining = (pair._end_date - now).total_seconds()

			side_to_buy = self.strategy.should_buy_first_leg(
			    pair_quote, all_active, total_capital_deployed, time_remaining)

			if side_to_buy:
				# Get market end date for auto-resolution
				market_end_date = getattr(pair, '_end_date', None)

				position = self.strategy.create_first_leg_position(
				    pair, pair_quote, side_to_buy, available_capital,
				    market_end_date)
				if position:
					self.positions[position.position_id] = position
					self.total_trades += 1
					self.reporter.log_trade(position, "FIRST_LEG")
					# Update tracking
					all_active.append(position)
					total_capital_deployed += position.total_cost_usd
					available_capital = self.config.max_capital - total_capital_deployed

		# Log status every 6 iterations (~1.2 seconds with 0.2s polling)
		self._iteration_count += 1
		if self._iteration_count % 6 == 0 and self.market_pairs:
			# Show info about first pair (or any active pair)
			current_pair = self.market_pairs[0] if self.market_pairs else None
			time_info = ""
			if hasattr(current_pair, '_end_date') and current_pair._end_date:
				now = datetime.now(timezone.utc)
				remaining = current_pair._end_date - now
				mins = int(remaining.total_seconds() // 60)
				secs = int(remaining.total_seconds() % 60)
				time_info = f"⏱️ {mins}m {secs}s"

			# Count positions
			n_pending = len([
			    p for p in self.positions.values() if p.status in
			    [PositionStatus.PENDING_UP, PositionStatus.PENDING_DOWN]
			])
			n_open = len([
			    p for p in self.positions.values()
			    if p.status == PositionStatus.OPEN
			])

			# Check WebSocket status with connection verification
			ws_status = "🔴 HTTP"
			cache_size = 0
			fresh_count = 0
			if self.client.ws_client:
				if self.client.ws_client.is_connected():
					cache_size = len(self.client.ws_client.markets_cache)
					fresh_count = sum(
					    1 for asset_id in
					    self.client.ws_client.markets_cache.keys()
					    if self.client.ws_client.is_data_fresh(asset_id))
					ws_status = f"🟢 WS({cache_size}, {fresh_count}fresh)"
				else:
					ws_status = "🟡 WS(disconnected)"

			if best_quote:
				up_p = best_quote.up_quote.price
				down_p = best_quote.down_quote.price
				spread = abs(up_p - down_p)

				# Show spread and what we need
				spread_info = f"Spread: {spread*100:.1f}¢"

				logger.info(
				    f"{ws_status} | UP: ${up_p:.3f} DOWN: ${down_p:.3f} | "
				    f"{spread_info} | {n_pending}P {n_open}O | {time_info}")
			else:
				# Show diagnostic info when no quotes available
				logger.warning(
				    f"{ws_status} | ⚠️ No quotes available | "
				    f"Pairs checked: {len(pairs_to_check)}, Found: {quotes_found}, Missing: {quotes_missing} | "
				    f"{n_pending}P {n_open}O | {time_info}")

	def _check_resolved_markets(self):
		"""
        Check for resolved/expired markets and close positions.

        For COMPLETE positions (both legs bought), resolution is deterministic:
        - Payout = shares × $1.00 (guaranteed, regardless of winner)
        - PnL = payout - total_cost

        This auto-resolves positions when the market expires (time-based).
        Uses position.market_end_date to work even after market pair changes.
        """
		now = datetime.now(timezone.utc)

		for position in self.positions.values():
			# Skip already resolved positions
			if position.status in [
			    PositionStatus.RESOLVED, PositionStatus.CLOSED,
			    PositionStatus.SCRATCHED
			]:
				continue

			# Check if market has expired using position's stored end date
			market_expired = False

			# First try position's stored end date (works even after market change)
			if position.market_end_date:
				market_expired = now > position.market_end_date
			else:
				# Fallback: try to find the pair (only works if pair still exists)
				pair = self._find_pair_by_id(position.pair_id)
				if pair and hasattr(pair, '_end_date') and pair._end_date:
					market_expired = now > pair._end_date

			if not market_expired:
				continue

			# === MARKET EXPIRED - RESOLVE POSITION ===

			if position.status == PositionStatus.OPEN:
				# COMPLETE POSITION: Both legs bought = Guaranteed arbitrage
				# Calculate payout: shares × $1.00
				shares = position.up_size_usd / position.entry_up_price if position.entry_up_price > 0 else 0
				payout = shares * 1.0  # $1.00 per share guaranteed

				# PnL = Payout - Total Cost (no fees on Polymarket)
				realized_pnl = payout - position.total_cost_usd

				position.realized_pnl_usd = realized_pnl
				position.status = PositionStatus.RESOLVED
				position.exit_timestamp = now

				self.total_realized_pnl += realized_pnl

				self.reporter.log_trade(position, "RESOLVED_ARBITRAGE",
				                        realized_pnl)
				logger.info(
				    f"🎯 Position {position.position_id[:8]}... RESOLVED (Arbitrage) | "
				    f"Shares: {shares:.0f} | Payout: ${payout:.2f} | "
				    f"Cost: ${position.total_cost_usd:.2f} | PnL: ${realized_pnl:.2f}"
				)

			elif position.status in [
			    PositionStatus.PENDING_UP, PositionStatus.PENDING_DOWN
			]:
				# PENDING POSITION: Only one leg - market expired before completion
				# This is a LOSS scenario - we didn't complete the hedge
				# For paper trading, assume worst case: the side we hold went to $0

				side = "UP" if position.status == PositionStatus.PENDING_UP else "DOWN"

				# Total loss = cost of the leg we bought
				realized_pnl = -position.total_cost_usd

				position.realized_pnl_usd = realized_pnl
				position.status = PositionStatus.RESOLVED
				position.exit_timestamp = now

				self.total_realized_pnl += realized_pnl

				self.reporter.log_trade(position, f"EXPIRED_PENDING_{side}",
				                        realized_pnl)
				logger.warning(
				    f"💀 Position {position.position_id[:8]}... EXPIRED (Pending {side}) | "
				    f"Never completed hedge | Loss: ${abs(realized_pnl):.2f}")

	def _record_pnl_snapshot(self):
		"""Record a PnL snapshot."""
		open_positions = [
		    p for p in self.positions.values()
		    if p.status == PositionStatus.OPEN
		]
		closed_positions = [
		    p for p in self.positions.values()
		    if p.status != PositionStatus.OPEN
		]

		total_realized = sum(p.realized_pnl_usd for p in closed_positions)
		total_unrealized = sum(p.unrealized_pnl_usd for p in open_positions)
		total_pnl = total_realized + total_unrealized
		total_deployed = sum(p.total_cost_usd for p in open_positions)

		record = PnLRecord(timestamp=datetime.now(),
		                   total_realized_pnl_usd=total_realized,
		                   total_unrealized_pnl_usd=total_unrealized,
		                   total_pnl_usd=total_pnl,
		                   open_positions_count=len(open_positions),
		                   total_capital_deployed=total_deployed)

		self.pnl_history.append(record)

		# Log summary periodically
		if len(self.pnl_history) % 10 == 0:
			self.reporter.log_summary(record)

	def _find_pair_by_id(self, pair_id: str) -> Optional[MarketPair]:
		"""Find a market pair by ID."""
		for pair in self.market_pairs:
			if pair.pair_id == pair_id:
				return pair
		return None

	def _process_scratch_logic(self, pair: MarketPair, pair_quote: PairQuote):
		"""
        Check and execute scratch (emergency sell) for pending positions.

        Scratch = selling a pending position at a loss when:
        - Completing would guarantee bigger loss
        - Time is running out
        - Position crashed beyond recovery
        """
		# Calculate time remaining for this market
		time_remaining = None
		if hasattr(pair, '_end_date') and pair._end_date:
			time_remaining = (pair._end_date -
			                  datetime.now(timezone.utc)).total_seconds()

		# Find pending positions for this pair
		pending = [
		    p for p in self.positions.values()
		    if p.pair_id == pair.pair_id and p.status in
		    [PositionStatus.PENDING_UP, PositionStatus.PENDING_DOWN]
		]

		for position in pending:
			scratch_reason = self.strategy.check_scratch_signal(
			    position, pair_quote, time_remaining)
			if scratch_reason:
				self._execute_scratch(position, pair_quote, scratch_reason)

	def _execute_scratch(self,
	                     position: Position,
	                     pair_quote: PairQuote,
	                     reason: str = "UNKNOWN"):
		"""
        Execute scratch (emergency sell) of a pending position.

        CRITICAL: Use BID price for selling, not ASK!
        - ASK = price to buy (what we pay when entering)
        - BID = price to sell (what we get when exiting)

        In a crashing market, the spread widens. If we simulate selling at ASK,
        results will be falsely optimistic.

        Args:
            position: The position to scratch
            pair_quote: Current market quotes
            reason: Why we're scratching (for analysis)
        """
		# Determine which side we're holding
		was_pending_up = position.status == PositionStatus.PENDING_UP

		# Get the correct token ID
		token_id = position.up_market_id if was_pending_up else position.down_market_id
		entry_price = position.entry_up_price if was_pending_up else position.entry_down_price
		position_size = position.up_size_usd if was_pending_up else position.down_size_usd
		side = "UP" if was_pending_up else "DOWN"

		# --- CRITICAL FIX: Get BID price for selling, not ASK ---
		exit_price = 0.0
		price_source = "UNKNOWN"

		if self.client.ws_client:
			# Get the real BID price from WebSocket order book
			best_bid = self.client.ws_client.get_best_bid(token_id)

			if best_bid is not None and best_bid > 0:
				exit_price = best_bid
				price_source = "WS_BID"
				logger.debug(f"Scratch using WebSocket BID: ${exit_price:.4f}")
			else:
				# Fallback: No buyers in order book (illiquid market)
				# Apply severe penalty - assume 10% worse than ASK
				ask_price = pair_quote.up_quote.price if was_pending_up else pair_quote.down_quote.price
				exit_price = ask_price * 0.90
				price_source = "PENALIZED_ASK_10%"
				logger.warning(
				    f"No BID available, using penalized ASK: ${exit_price:.4f}"
				)
		else:
			# Fallback HTTP: Apply 5% spread penalty
			ask_price = pair_quote.up_quote.price if was_pending_up else pair_quote.down_quote.price
			exit_price = ask_price * 0.95
			price_source = "HTTP_PENALIZED_5%"
			logger.warning(
			    f"No WebSocket, using penalized price: ${exit_price:.4f}")

		# Calculate shares we hold
		# Shares = Size USD / Entry Price
		shares = position_size / entry_price if entry_price > 0 else 0

		# Calculate recovery amount (what we get back)
		# Recovery = Shares * Exit Price (no fees on Polymarket)
		gross_recovery = shares * exit_price
		net_recovery = gross_recovery  # No fees to deduct

		# Calculate PnL (negative = loss)
		pnl = net_recovery - position.total_cost_usd

		# Update position status
		position.status = PositionStatus.SCRATCHED
		position.realized_pnl_usd = pnl
		position.exit_timestamp = datetime.now()

		# Update engine totals
		self.total_realized_pnl += pnl

		# Log the scratch with reason for analysis
		action = f"SCRATCH:{reason}"
		self.reporter.log_trade(position, action, pnl)

		logger.info(
		    f"✂️ SCRATCHED {side} Position {position.position_id[:8]}... | "
		    f"Entry: ${entry_price:.3f} → Exit: ${exit_price:.3f} ({price_source}) | "
		    f"Recovered: ${net_recovery:.2f} | PnL: ${pnl:.2f} | Reason: {reason}"
		)

	def shutdown(self):
		"""Shutdown the engine gracefully."""
		logger.info("Shutting down engine...")
		self.running = False

		# Final PnL report
		open_positions = [
		    p for p in self.positions.values()
		    if p.status == PositionStatus.OPEN
		]
		closed_positions = [
		    p for p in self.positions.values()
		    if p.status != PositionStatus.OPEN
		]

		total_realized = sum(p.realized_pnl_usd for p in closed_positions)
		total_unrealized = sum(p.unrealized_pnl_usd for p in open_positions)
		total_pnl = total_realized + total_unrealized

		logger.info("=" * 60)
		logger.info("FINAL SUMMARY")
		logger.info("=" * 60)
		logger.info(f"Total trades: {self.total_trades}")
		logger.info(f"Open positions: {len(open_positions)}")
		logger.info(f"Closed positions: {len(closed_positions)}")
		logger.info(f"Total realized PnL: ${total_realized:.2f}")
		logger.info(f"Total unrealized PnL: ${total_unrealized:.2f}")
		logger.info(f"Total PnL: ${total_pnl:.2f}")
		logger.info("=" * 60)

		# Save final report
		self.reporter.save_final_report(self.positions, self.pnl_history)
