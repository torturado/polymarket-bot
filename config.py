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
	    str] = None  # Keywords to filter markets (e.g., ["BTC", "ETH", "SOL", "XRP"])
	market_duration_minutes: int = 15  # Target market duration in minutes
	market_slug_pattern: Optional[
	    str] = None  # Optional slug pattern (e.g., "btc-updown-15m")
	supported_tokens: list[
	    str] = None  # List of tokens to track (e.g., ["BTC", "ETH", "SOL", "XRP"])

	# Strategy Parameters - Arbitrage Detection (MODO PRODUCCIÓN/SNIPER)
	# ═══════════════════════════════════════════════════════════════════════

	# Step 1: Entry conditions
	# Volvemos a 0.25 para asegurar que compramos gangas y tenemos margen de caída
	first_leg_threshold: float = 0.25
	min_spread_to_enter: float = 0.45  # El mercado debe estar desequilibrado
	min_safe_price: float = 0.05  # Seguridad

	# Step 2: Complete position
	# 0.95 garantiza un 5% de margen bruto (excluyendo slippage).
	# Como los fees son 0%, esto es 5% neto teórico.
	complete_threshold: float = 0.95

	# RESTAURAR LÓGICA ESTRICTA: Nunca entrar si la suma > 1.00
	max_entry_threshold: float = 0.96

	# Exigir un beneficio mínimo del 2% al proyectar el cierre
	min_profit_pct: float = 2.0

	# Bail-out parameters (legacy - mostly handled by scratch now)
	bail_threshold: float = 0.10
	bail_loss_ratio: float = 0.50

	# Scratch parameters (Salida de emergencia)
	scratch_time_threshold: int = 45
	scratch_loss_threshold: float = 0.25  # Cortar si perdemos 25% ($10)
	scratch_min_recovery: float = 0.03

	# Risk Controls - CUENTA $100
	max_capital: float = 100.0

	# 40 acciones = $40 payout objetivo.
	# Coste aprox: $38. Deja $62 libres para emergencias o fees de gas si hubiera.
	max_position_size: float = 40.0

	# Con $100, mejor concéntrate en 1 buena operación a la vez para no quedarte sin liquidez
	max_concurrent_pairs: int = 1

	# Fee Assumptions
	trading_fee_percent: float = 0.0  # Correcto para Polymarket

	# Execution Settings
	# Faster polling = faster reaction to price changes (WebSocket cache is instant)
	polling_interval_seconds: float = 0.2  # Ultra-fast polling (was 0.5), no API limits with WebSocket cache
	min_time_remaining_seconds: int = 240  # Don't enter with less than 3 minutes left

	# Debug/Logging Settings
	raw_message_log_limit: int = 10  # Log first N raw WebSocket messages at DEBUG level (0 = disabled, -1 = unlimited)
	raw_message_log_sample_interval: int = 100  # After limit, log every Nth message (0 = no sampling)

	# Reporting
	log_to_csv: bool = True
	csv_output_path: str = "paper_trades.csv"

	def __post_init__(self):
		"""Set default values for mutable fields."""
		if self.supported_tokens is None:
			self.supported_tokens = ["BTC", "ETH", "SOL", "XRP"]

		if self.market_keywords is None:
			# Build keywords from supported tokens
			token_keywords = []
			for token in self.supported_tokens:
				token_lower = token.lower()
				token_keywords.extend([token, token_lower])
				# Add common names
				if token == "BTC":
					token_keywords.extend(["Bitcoin", "bitcoin"])
				elif token == "ETH":
					token_keywords.extend(["Ethereum", "ethereum"])
				elif token == "SOL":
					token_keywords.extend(["Solana", "solana"])
				elif token == "XRP":
					token_keywords.extend(["Ripple", "ripple"])

			# Add common market keywords
			token_keywords.extend([
			    "15m", "15 min", "15-minute", "updown", "up down", "up or down"
			])
			self.market_keywords = token_keywords

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
