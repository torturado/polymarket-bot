"""
Main entrypoint for the Polymarket BTC paper-trading bot.
"""
import argparse
import logging
import sys

# Load environment variables from .env file (if present)
try:
	from dotenv import load_dotenv
	load_dotenv()
except ImportError:
	pass  # dotenv not installed, rely on system environment variables

from engine import PaperTradingEngine
from config import config

# Configure logging - INFO level for cleaner output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

# Silence noisy loggers
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("hpack").setLevel(logging.WARNING)
logging.getLogger("websockets").setLevel(logging.WARNING)
logging.getLogger("websocket_client").setLevel(
    logging.WARNING)  # Only show warnings/errors from WS
logging.getLogger("polymarket_client").setLevel(logging.INFO)  # Normal logging


def run_paper_bot():
	"""Run the paper-trading bot."""
	supported_tokens = getattr(config, 'supported_tokens',
	                           ['BTC', 'ETH', 'SOL', 'XRP'])
	tokens_str = ", ".join(supported_tokens)
	print(
	    f"Starting Polymarket Multi-Token Paper-Trading Bot ({tokens_str})...")
	print()

	# Show API credentials status
	has_creds = all([config.api_key, config.api_secret, config.passphrase])
	if has_creds:
		print(f"  API Credentials: ✅ Configured (Builder API)")
		print(
		    f"  API Key: {config.api_key[:8]}...{config.api_key[-4:] if len(config.api_key) > 12 else ''}"
		)
	else:
		print(f"  API Credentials: ⚠️  Not configured (read-only mode)")
		print(
		    f"  Set POLYMARKET_API_KEY, POLYMARKET_API_SECRET, POLYMARKET_PASSPHRASE"
		)
	print()

	print(f"Configuration (Leg-In Strategy):")
	print(
	    f"  First leg threshold: ${config.first_leg_threshold:.2f} (buy when UP or DOWN ≤ this)"
	)
	print(
	    f"  Complete threshold: ${config.complete_threshold:.2f} (complete when total ≤ this)"
	)
	print(f"  Max capital: ${config.max_capital:.2f}")
	print(f"  Max position size: ${config.max_position_size:.2f}")
	print(f"  Trading fee: {config.trading_fee_percent}%")
	print(f"  Polling interval: {config.polling_interval_seconds}s")
	print()

	engine = PaperTradingEngine(config)

	try:
		engine.run()
	except KeyboardInterrupt:
		print("\nBot stopped by user.")
		engine.shutdown()
		sys.exit(0)
	except Exception as e:
		print(f"\nError: {e}")
		engine.shutdown()
		sys.exit(1)


def main():
	"""CLI entrypoint."""
	parser = argparse.ArgumentParser(
	    description="Polymarket Multi-Token Paper-Trading Bot")
	parser.add_argument("command",
	                    choices=["run-paper-bot"],
	                    help="Command to execute")

	args = parser.parse_args()

	if args.command == "run-paper-bot":
		run_paper_bot()
	else:
		parser.print_help()
		sys.exit(1)


if __name__ == "__main__":
	main()
