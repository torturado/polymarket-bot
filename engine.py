"""
Paper-trading engine that orchestrates data fetching, strategy execution, and PnL tracking.
"""
import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Optional
import threading

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
            self.market_pairs = self.client.find_market_pairs(only_current=True)

            if not self.market_pairs:
                logger.warning(
                    "No active market pairs found. Waiting for next market to start..."
                )
        except Exception as e:
            logger.error(f"Failed to initialize market pairs: {e}", exc_info=True)
            self.market_pairs = []

    def _start_websocket(self):
        """Start WebSocket client in a separate thread."""
        async def ws_loop():
            try:
                await self.client.ws_client.connect()
                # Use the new subscribe_to_markets method
                success = await self.client.ws_client.subscribe_to_markets()
                if success:
                    logger.info("WebSocket subscribed to markets successfully")
                    await self.client.ws_client.listen()
                else:
                    logger.warning("Failed to subscribe to WebSocket markets")
            except Exception as e:
                logger.error(f"WebSocket error: {e}", exc_info=True)

        def run_ws():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(ws_loop())

        self.ws_thread = threading.Thread(target=run_ws, daemon=True)
        self.ws_thread.start()
        logger.info("WebSocket client started for real-time market data")

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
        """Execute one iteration of the trading loop."""
        # 1. Determinar si necesitamos refrescar la lista de mercados
        # OPTIMIZACIÓN: Solo consultamos Gamma API cuando es estrictamente necesario
        # (al inicio o cuando el mercado actual termina), NO constantemente.
        should_refresh = False
        now = datetime.now(timezone.utc)

        if not self.market_pairs:
            # Caso A: No tenemos mercados cargados (inicio o error previo)
            should_refresh = True
        else:
            current_pair = self.market_pairs[0]

            # Verificar si el mercado actual ya expiró
            if hasattr(current_pair, '_end_date') and current_pair._end_date:
                time_left = (current_pair._end_date - now).total_seconds()

                if time_left <= 0:
                    # Mercado terminado - buscar siguiente
                    logger.info("⏰ El mercado actual ha terminado. Buscando el siguiente...")
                    should_refresh = True
                elif time_left <= 30 and self._iteration_count % 20 == 0:
                    # Caso B: Faltan 30s. Refrescamos preventivamente para tener el ID del
                    # siguiente mercado listo en cuanto este cierre. (Baja frecuencia)
                    logger.info("⚠️ Preparando transición al siguiente mercado...")
                    should_refresh = True

        # 2. Ejecutar el refresco SOLO si es necesario
        # ELIMINADO: "or self._iteration_count % 12 == 0" <- Esto causaba spam a la API
        if should_refresh:
            try:
                new_pairs = self.client.find_market_pairs(only_current=True)

                if new_pairs:
                    # Detectar si hemos cambiado de mercado
                    old_pair_id = self.market_pairs[0].pair_id if self.market_pairs else None
                    new_pair_id = new_pairs[0].pair_id

                    if old_pair_id != new_pair_id:
                        logger.info(f"🔄 Cambio de mercado detectado: {new_pairs[0].up_market.question[:60]}...")

                        # IMPORTANTE: Suscribir WebSocket a los nuevos tokens
                        if self.client.ws_client:
                            self._resubscribe_websocket(new_pairs[0])

                    self.market_pairs = new_pairs

            except Exception as e:
                logger.error(f"Error refrescando mercados: {e}", exc_info=True)

        # 3. Lógica de Trading (usa WebSocket cache, no HTTP)
        self._update_positions()
        self._check_entry_opportunities()
        self._check_resolved_markets()
        self._record_pnl_snapshot()

        self._iteration_count += 1

    def _resubscribe_websocket(self, new_pair: MarketPair):
        """
        Resubscribe WebSocket to new market tokens when market changes.

        Cada 15 minutos los Token IDs cambian. Sin esto, el WebSocket
        seguiría enviando precios del mercado viejo.
        """
        try:
            up_token = new_pair.up_market.token_id or new_pair.up_market.market_id
            down_token = new_pair.down_market.token_id or new_pair.down_market.market_id

            logger.info(f"🔌 Resuscribiendo WebSocket a nuevos tokens: UP={up_token[:20]}..., DOWN={down_token[:20]}...")

            # Run subscription in the WebSocket's event loop
            if self.client.ws_client.websocket:
                loop = asyncio.new_event_loop()

                async def resubscribe():
                    await self.client.ws_client.subscribe_to_markets([up_token, down_token])

                # Run in separate thread to avoid blocking
                def run_async():
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(resubscribe())

                thread = threading.Thread(target=run_async, daemon=True)
                thread.start()

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
                position,
                up_quote.price,
                down_quote.price
            )

    def _get_instant_quotes(self, pair: MarketPair) -> Optional[tuple]:
        """
        Get quotes from WebSocket cache first (instant), fallback to HTTP.
        Returns (up_quote, down_quote) or None.
        """
        # Try WebSocket cache first (INSTANT - no latency)
        if self.client.ws_client:
            up_token = pair.up_market.token_id or pair.up_market.market_id
            down_token = pair.down_market.token_id or pair.down_market.market_id

            up_data = self.client.ws_client.get_instant_quote(up_token)
            down_data = self.client.ws_client.get_instant_quote(down_token)

            if up_data and down_data:
                # Use best ASK for buying (what we'd pay)
                up_bid, up_ask = up_data
                down_bid, down_ask = down_data

                up_quote = Quote(
                    market_id=pair.up_market.market_id,
                    side=MarketSide.UP,
                    price=up_ask,  # Use ASK for buying
                    timestamp=datetime.now()
                )
                down_quote = Quote(
                    market_id=pair.down_market.market_id,
                    side=MarketSide.DOWN,
                    price=down_ask,  # Use ASK for buying
                    timestamp=datetime.now()
                )
                return (up_quote, down_quote)

        # Fallback to HTTP (slower but more reliable)
        return self.client.get_pair_quotes(pair)

    def _check_entry_opportunities(self):
        """Check for entry opportunities using Leg-In strategy."""
        # Categorize positions
        pending_positions = [
            p for p in self.positions.values()
            if p.status in [PositionStatus.PENDING_UP, PositionStatus.PENDING_DOWN]
        ]
        open_positions = [p for p in self.positions.values() if p.status == PositionStatus.OPEN]
        all_active = pending_positions + open_positions

        total_capital_deployed = sum(p.total_cost_usd for p in all_active)
        available_capital = self.config.max_capital - total_capital_deployed

        best_quote = None

        for pair in self.market_pairs:
            # Get quotes - WebSocket first, then HTTP fallback
            quotes = self._get_instant_quotes(pair)
            if not quotes:
                continue

            up_quote, down_quote = quotes
            pair_quote = PairQuote.create(pair, up_quote, down_quote)

            # Check SCRATCH logic for pending positions FIRST (emergency exit)
            self._process_scratch_logic(pair, pair_quote)

            # Refresh pending positions list after potential scratches
            pending_positions = [
                p for p in self.positions.values()
                if p.status in [PositionStatus.PENDING_UP, PositionStatus.PENDING_DOWN]
            ]

            # Check bail-out for pending positions (use smart scratch logic)
            for position in pending_positions:
                if position.pair_id == pair.pair_id:
                    if self.strategy.should_bail_out(position, pair_quote):
                        logger.warning(
                            f"🚨 BAIL OUT - Position {position.position_id[:8]}... | "
                            f"UP: ${up_quote.price:.3f} DOWN: ${down_quote.price:.3f}"
                        )
                        # Use smart scratch execution (recovers real market value)
                        self._execute_scratch(position, pair_quote, reason="BAIL_OUT_CRASH")
                        continue

            if best_quote is None or pair_quote.combined_price < best_quote.combined_price:
                best_quote = pair_quote

            # Step 1: Check if any pending position can be completed
            for position in pending_positions:
                if position.pair_id == pair.pair_id:
                    if self.strategy.should_complete_position(position, pair_quote):
                        if self.strategy.complete_position(position, pair_quote, available_capital):
                            self.total_trades += 1
                            self.reporter.log_trade(position, "COMPLETE")
                            # Update available capital
                            total_capital_deployed = sum(
                                p.total_cost_usd for p in self.positions.values()
                                if p.status != PositionStatus.RESOLVED
                            )
                            available_capital = self.config.max_capital - total_capital_deployed

            # Step 2: Check if we should buy first leg
            # Skip if we already have any position in this pair
            if any(p.pair_id == pair.pair_id for p in all_active):
                continue

            # Calculate time remaining for this market
            time_remaining = None
            if hasattr(pair, '_end_date') and pair._end_date:
                now = datetime.now(timezone.utc)
                time_remaining = (pair._end_date - now).total_seconds()

            side_to_buy = self.strategy.should_buy_first_leg(
                pair_quote, all_active, total_capital_deployed, time_remaining
            )

            if side_to_buy:
                # Get market end date for auto-resolution
                market_end_date = getattr(pair, '_end_date', None)

                position = self.strategy.create_first_leg_position(
                    pair, pair_quote, side_to_buy, available_capital, market_end_date
                )
                if position:
                    self.positions[position.position_id] = position
                    self.total_trades += 1
                    self.reporter.log_trade(position, "FIRST_LEG")
                    # Update tracking
                    all_active.append(position)
                    total_capital_deployed += position.total_cost_usd
                    available_capital = self.config.max_capital - total_capital_deployed

        # Log status every 6 iterations (~30 seconds)
        self._iteration_count += 1
        if self._iteration_count % 6 == 0 and self.market_pairs:
            current_pair = self.market_pairs[0]
            time_info = ""
            if hasattr(current_pair, '_end_date') and current_pair._end_date:
                now = datetime.now(timezone.utc)
                remaining = current_pair._end_date - now
                mins = int(remaining.total_seconds() // 60)
                secs = int(remaining.total_seconds() % 60)
                time_info = f"⏱️ {mins}m {secs}s"

            # Count positions
            n_pending = len([p for p in self.positions.values() if p.status in [PositionStatus.PENDING_UP, PositionStatus.PENDING_DOWN]])
            n_open = len([p for p in self.positions.values() if p.status == PositionStatus.OPEN])

            # Check WebSocket status
            ws_status = "🔴 HTTP"
            if self.client.ws_client and self.client.ws_client.markets_cache:
                ws_status = f"🟢 WS({len(self.client.ws_client.markets_cache)})"

            if best_quote:
                up_p = best_quote.up_quote.price
                down_p = best_quote.down_quote.price
                spread = abs(up_p - down_p)

                # Show spread and what we need
                spread_info = f"Spread: {spread*100:.1f}¢"

                logger.info(
                    f"{ws_status} | UP: ${up_p:.3f} DOWN: ${down_p:.3f} | "
                    f"{spread_info} | {n_pending}P {n_open}O | {time_info}"
                )

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
            if position.status in [PositionStatus.RESOLVED, PositionStatus.CLOSED, PositionStatus.SCRATCHED]:
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

                # PnL = Payout - Total Cost (fees already in cost)
                realized_pnl = payout - position.total_cost_usd

                position.realized_pnl_usd = realized_pnl
                position.status = PositionStatus.RESOLVED
                position.exit_timestamp = now

                self.total_realized_pnl += realized_pnl

                self.reporter.log_trade(position, "RESOLVED_ARBITRAGE", realized_pnl)
                logger.info(
                    f"🎯 Position {position.position_id[:8]}... RESOLVED (Arbitrage) | "
                    f"Shares: {shares:.0f} | Payout: ${payout:.2f} | "
                    f"Cost: ${position.total_cost_usd:.2f} | PnL: ${realized_pnl:.2f}"
                )

            elif position.status in [PositionStatus.PENDING_UP, PositionStatus.PENDING_DOWN]:
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

                self.reporter.log_trade(position, f"EXPIRED_PENDING_{side}", realized_pnl)
                logger.warning(
                    f"💀 Position {position.position_id[:8]}... EXPIRED (Pending {side}) | "
                    f"Never completed hedge | Loss: ${abs(realized_pnl):.2f}"
                )

    def _record_pnl_snapshot(self):
        """Record a PnL snapshot."""
        open_positions = [p for p in self.positions.values() if p.status == PositionStatus.OPEN]
        closed_positions = [p for p in self.positions.values() if p.status != PositionStatus.OPEN]

        total_realized = sum(p.realized_pnl_usd for p in closed_positions)
        total_unrealized = sum(p.unrealized_pnl_usd for p in open_positions)
        total_pnl = total_realized + total_unrealized
        total_deployed = sum(p.total_cost_usd for p in open_positions)

        record = PnLRecord(
            timestamp=datetime.now(),
            total_realized_pnl_usd=total_realized,
            total_unrealized_pnl_usd=total_unrealized,
            total_pnl_usd=total_pnl,
            open_positions_count=len(open_positions),
            total_capital_deployed=total_deployed
        )

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
            time_remaining = (pair._end_date - datetime.now(timezone.utc)).total_seconds()

        # Find pending positions for this pair
        pending = [
            p for p in self.positions.values()
            if p.pair_id == pair.pair_id and p.status in [PositionStatus.PENDING_UP, PositionStatus.PENDING_DOWN]
        ]

        for position in pending:
            scratch_reason = self.strategy.check_scratch_signal(position, pair_quote, time_remaining)
            if scratch_reason:
                self._execute_scratch(position, pair_quote, scratch_reason)

    def _execute_scratch(self, position: Position, pair_quote: PairQuote, reason: str = "UNKNOWN"):
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
                logger.warning(f"No BID available, using penalized ASK: ${exit_price:.4f}")
        else:
            # Fallback HTTP: Apply 5% spread penalty
            ask_price = pair_quote.up_quote.price if was_pending_up else pair_quote.down_quote.price
            exit_price = ask_price * 0.95
            price_source = "HTTP_PENALIZED_5%"
            logger.warning(f"No WebSocket, using penalized price: ${exit_price:.4f}")

        # Calculate shares we hold
        # Shares = Size USD / Entry Price
        shares = position_size / entry_price if entry_price > 0 else 0

        # Calculate recovery amount (what we get back)
        # Recovery = (Shares * Exit Price) - Fees
        gross_recovery = shares * exit_price
        fee_amount = gross_recovery * (self.config.trading_fee_percent / 100.0)
        net_recovery = gross_recovery - fee_amount

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
        open_positions = [p for p in self.positions.values() if p.status == PositionStatus.OPEN]
        closed_positions = [p for p in self.positions.values() if p.status != PositionStatus.OPEN]

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
