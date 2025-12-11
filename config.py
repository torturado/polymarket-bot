"""
Configuration settings for the Polymarket BTC paper-trading bot.
"""
import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class BotConfig:
	"""Main configuration for the paper-trading bot."""

	# API Configuration
	polymarket_api_base: str = "https://clob.polymarket.com"
	polymarket_graphql_endpoint: str = "https://api.polymarket.com/graphql"

	# API Credentials (from environment variables or direct configuration)
	api_key: Optional[str] = None  # Polymarket Builder API key
	api_secret: Optional[str] = None  # Polymarket Builder API secret
	passphrase: Optional[str] = None  # Polymarket Builder passphrase
	bearer_token: Optional[
	    str] = None  # Bearer token for authentication (alternative)

	# Market Filters
	market_keywords: list[
	    str] = None  # Keywords to filter BTC markets (e.g., ["BTC", "Bitcoin"])
	market_duration_minutes: int = 15  # Target market duration in minutes
	market_slug_pattern: Optional[
	    str] = None  # Optional slug pattern (e.g., "btc-updown-15m")

	# Strategy Parameters - Arbitrage Detection
	# ═══════════════════════════════════════════════════════════════════════
	# CRITICAL: The bot ONLY enters if UP + DOWN <= complete_threshold
	# This ensures arbitrage profit is possible BEFORE buying the first leg.
	# ═══════════════════════════════════════════════════════════════════════

	# Step 1: Entry conditions
	first_leg_threshold: float = 0.30  # Buy UP or DOWN when price <= this
	min_spread_to_enter: float = 0.40  # Minimum spread to consider entry
	min_safe_price: float = 0.15  # Don't buy below this (market already decided winner)

	# Step 2: Complete position when spread compresses
	complete_threshold: float = 0.96  # Complete when UP + DOWN <= this (4% guaranteed profit)

	# Bail-out parameters (legacy - mostly handled by scratch now)
	bail_threshold: float = 0.10
	bail_loss_ratio: float = 0.50

	# Scratch parameters (emergency exit for pending positions)
	# More conservative: give the market time to swing back before exiting
	scratch_time_threshold: int = 45  # Scratch if less than 45s remaining
	scratch_loss_threshold: float = 0.35  # Scratch if lost >35% of entry value
	scratch_min_recovery: float = 0.03  # Only scratch if at least $0.03 to recover

	# Risk Controls
	# NOTE: max_position_size = TARGET SHARES = TARGET PAYOUT
	# e.g., 100 = 100 shares = $100 payout guaranteed if completed
	# Max cost = 100 shares × $0.96 × 1.02 fee = ~$98
	max_capital: float = 100.0  # Capital for one full arbitrage + margin
	max_position_size: float = 40.0  # Target shares (100 shares = $100 payout)
	max_concurrent_pairs: int = 1  # Maximum number of concurrent positions

	# Fee Assumptions
	trading_fee_percent: float = 2.0  # Trading fee percentage (2% taker fee)

	# Execution Settings
	# Faster polling = faster reaction to price changes (WebSocket cache is instant)
	polling_interval_seconds: float = 0.2  # Ultra-fast polling (was 0.5), no API limits with WebSocket cache
	min_time_remaining_seconds: int = 240  # Don't enter with less than 3 minutes left

	# Reporting
	log_to_csv: bool = True
	csv_output_path: str = "paper_trades.csv"

	def __post_init__(self):
		"""Set default values for mutable fields."""
		if self.market_keywords is None:
			self.market_keywords = [
			    "BTC", "Bitcoin", "btc", "bitcoin", "15m", "15 min",
			    "15-minute", "updown", "up down", "up or down"
			]

		# Load credentials from environment variables if not explicitly set
		if self.api_key is None:
			self.api_key = os.getenv("POLYMARKET_API_KEY") or os.getenv(
			    "POLY_BUILDER_API_KEY")
		if self.api_secret is None:
			self.api_secret = os.getenv("POLYMARKET_API_SECRET") or os.getenv(
			    "POLY_BUILDER_SECRET")
		if self.passphrase is None:
			self.passphrase = os.getenv("POLYMARKET_PASSPHRASE") or os.getenv(
			    "POLY_BUILDER_PASSPHRASE")
		if self.bearer_token is None:
			self.bearer_token = os.getenv("POLYMARKET_BEARER_TOKEN")


# Global config instance
config = BotConfig()
