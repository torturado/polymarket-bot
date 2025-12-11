# Polymarket BTC Up/Down Paper-Trading Bot

A Python paper-trading bot that simulates the Polymarket BTC 15-minute up/down trading strategy. The bot watches for opportunities where buying both sides of a market (UP and DOWN) costs less than $1.00, creating a near-risk-free arbitrage opportunity.

## Strategy Overview

The bot implements a simple but effective strategy:

1. **Monitor BTC 15-minute markets**: Watches for Bitcoin up/down markets with 15-minute durations
2. **Detect arbitrage opportunities**: When `price_up + price_down < threshold` (default: 0.99), enter a position
3. **Buy both sides**: Purchase equal dollar amounts of UP and DOWN shares
4. **Hold to resolution**: Wait for market resolution and collect guaranteed payout

### Example Trade

-   Buy UP at 23 cents
-   Buy DOWN at 70 cents
-   Total cost: 93 cents
-   Guaranteed payout: 100 cents
-   Profit: 7 cents (7% return)

## Installation

1. **Clone or download this repository**

2. **Install dependencies**:

```bash
pip install -r requirements.txt
```

**Note**: The bot uses the official `py-clob-client` SDK from Polymarket. Make sure it's installed correctly.

## Configuration

### API Credentials (Polymarket Builder)

The bot supports Polymarket Builder API credentials (apiKey, secret, and passphrase).

**Option 1: Using `.env` file (Recommended)**

A `.env` file has been created with your Builder credentials. The bot will automatically load them from environment variables.

If you need to update credentials, edit `.env`:

```bash
POLYMARKET_API_KEY=your_api_key_here
POLYMARKET_API_SECRET=your_api_secret_here
POLYMARKET_PASSPHRASE=your_passphrase_here
```

**Option 2: Direct configuration in `config.py`**

You can also set credentials directly:

```python
config = BotConfig(
    api_key="your_api_key_here",
    api_secret="your_api_secret_here",
    passphrase="your_passphrase_here"
)
```

**Security Note**: The `.env` file is already in `.gitignore` and will not be committed to git. Keep your credentials secure!

### Bot Parameters

Edit `config.py` to customize bot behavior:

-   `entry_threshold`: Combined price threshold (default: 0.99)
-   `max_capital`: Maximum capital to deploy (default: $10,000)
-   `max_position_size`: Maximum position size per pair (default: $100)
-   `max_concurrent_pairs`: Maximum concurrent positions (default: 10)
-   `trading_fee_percent`: Trading fee percentage (default: 2%)
-   `polling_interval_seconds`: How often to check markets (default: 5 seconds)
-   `market_keywords`: Keywords to filter BTC markets (default: ["BTC", "Bitcoin", "15m", "15 min"])

## Usage

Run the paper-trading bot:

```bash
python main.py run-paper-bot
```

The bot will:

-   Fetch BTC 15-minute up/down markets from Polymarket
-   Monitor prices and identify entry opportunities
-   Simulate trades (no real orders are placed)
-   Track positions and PnL
-   Log trades to console and CSV (`paper_trades.csv`)
-   Generate a final report on shutdown (`final_report.json`)

## Important Notes

### API Configuration

The bot includes placeholder implementations for Polymarket API endpoints. You'll need to:

1. **Update API endpoints** in `config.py` if Polymarket's API structure differs
2. **Adapt GraphQL queries** in `polymarket_client.py` to match Polymarket's actual API schema
3. **Test API connectivity** before running the bot

The current implementation uses example GraphQL queries that may need adjustment based on Polymarket's actual API documentation.

### Paper Trading Only

**This bot does NOT place real orders.** It simulates trades for strategy testing and analysis. To use with real trading:

1. Implement order execution in a separate module
2. Add authentication/API keys for Polymarket
3. Thoroughly test with small amounts first
4. Understand the risks involved

### Edge Cases Handled

-   Insufficient capital
-   Maximum concurrent positions limit
-   Market resolution detection
-   Fee calculations
-   Illiquid markets (missing quotes)

## Project Structure

```
.
├── config.py              # Bot configuration
├── models.py              # Data models (Market, Position, Quote, etc.)
├── polymarket_client.py   # API client for fetching market data
├── strategy.py            # Trading strategy logic
├── engine.py              # Main simulation engine
├── reporting.py           # Logging and CSV reporting
├── main.py                # CLI entrypoint
├── requirements.txt       # Python dependencies
└── README.md              # This file
```

## Output Files

-   `paper_trades.csv`: Log of all trades with timestamps, prices, sizes, and PnL
-   `final_report.json`: Final summary with all positions and PnL history

## Future Enhancements

-   Real order execution (requires Polymarket API integration)
-   Backtesting on historical data
-   Early exit strategies (exit when combined price widens)
-   WebSocket support for real-time price updates
-   Database persistence for positions and history
-   Advanced risk management (position sizing based on edge)

## Disclaimer

This bot is for educational and research purposes. Trading on prediction markets involves risk. Always:

-   Test thoroughly before using real capital
-   Understand the markets and strategy
-   Monitor positions actively
-   Use appropriate risk management

## License

This project is provided as-is for educational purposes.
