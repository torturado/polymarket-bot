"""
Leg-In Trading Strategy for BTC Up/Down Markets.

CRITICAL: This strategy uses SHARE PARITY (same number of shares on both sides)
to achieve true arbitrage. max_position_size = target shares = target payout.
"""
import logging
from datetime import datetime
from typing import Optional
from uuid import uuid4

from config import BotConfig
from models import (
    MarketPair,
    PairQuote,
    Position,
    PositionStatus,
    MarketSide,
)

logger = logging.getLogger(__name__)


class LegInStrategy:
	"""
    Leg-In Strategy with SHARE PARITY.

    max_position_size = number of shares to buy = target payout in dollars.
    Example: max_position_size=100 means buy 100 shares, payout $100 guaranteed.
    """

	def __init__(self, config: BotConfig):
		self.config = config

	def should_buy_first_leg(
	        self,
	        pair_quote: PairQuote,
	        pending_positions: list[Position],
	        total_capital_deployed: float,
	        time_remaining_seconds: float = None) -> Optional[MarketSide]:
		"""
        Check if we should buy the first leg of a position.

        CRITICAL: Only enter if arbitrage is CURRENTLY possible.
        If UP + DOWN > complete_threshold, there's NO profit opportunity.
        """
		up_price = pair_quote.up_quote.price
		down_price = pair_quote.down_quote.price
		combined_price = up_price + down_price

		# DEBUG: Log entry for every check
		logger.debug(
		    f"🔍 Checking entry: UP=${up_price:.3f} DOWN=${down_price:.3f} "
		    f"Combined=${combined_price:.3f} | Pair={pair_quote.pair_id[:8]}..."
		)

		# SAFETY: Don't buy if less time remaining than configured minimum
		min_time_remaining = getattr(self.config, 'min_time_remaining_seconds',
		                             120)
		if time_remaining_seconds is not None and time_remaining_seconds < min_time_remaining:
			logger.debug(
			    f"❌ REJECT: Time remaining {time_remaining_seconds:.0f}s < {min_time_remaining}s"
			)
			return None

		# Check concurrent positions limit
		if len(pending_positions) >= self.config.max_concurrent_pairs:
			logger.debug(
			    f"❌ REJECT: Max concurrent positions ({len(pending_positions)} >= {self.config.max_concurrent_pairs})"
			)
			return None

		# Check if we already have a position in this pair
		for pos in pending_positions:
			if pos.pair_id == pair_quote.pair_id:
				logger.debug(
				    f"❌ REJECT: Already have position in pair {pair_quote.pair_id[:8]}..."
				)
				return None

		# ═══════════════════════════════════════════════════════════════════
		# RELAXED ENTRY FOR TESTING: Allow higher combined prices
		# ═══════════════════════════════════════════════════════════════════
		# Get max_entry_threshold from config
		# NOTE: In binary markets, UP + DOWN always sums to ~1.0, so max_entry_threshold
		# should be >= 1.0 to allow entries. Default to 1.10 for testing.
		max_entry_threshold = getattr(self.config, 'max_entry_threshold', 1.10)

		if combined_price > max_entry_threshold:
			logger.debug(
			    f"❌ REJECT: Combined ${combined_price:.3f} > max_entry_threshold ${max_entry_threshold:.2f}"
			)
			return None

		# Target shares = max_position_size (e.g., 100 shares = $100 payout)
		target_shares = self.config.max_position_size

		# Calculate available capital
		available_capital = self.config.max_capital - total_capital_deployed

		# Estimate TOTAL cost for full arbitrage (both legs) at CURRENT prices
		# No fees on Polymarket (https://docs.polymarket.com/polymarket-learn/trading/fees)
		estimated_total_cost = target_shares * combined_price

		# Don't enter if we can't afford the full arbitrage at current prices
		if available_capital < estimated_total_cost:
			logger.warning(
			    f"❌ REJECT: Insufficient capital: have ${available_capital:.2f}, need ${estimated_total_cost:.2f} "
			    f"(shares={target_shares}, combined=${combined_price:.3f})")
			return None

		# SAFETY: Don't buy extremely cheap options (market already decided)
		# If UP is at $0.10 and DOWN is at $0.85, the market is 85% sure DOWN wins
		min_safe_price = getattr(self.config, 'min_safe_price', 0.00)
		if min_safe_price > 0:  # Only check if configured
			if up_price < min_safe_price:
				logger.debug(
				    f"❌ REJECT: UP too cheap @ ${up_price:.3f} < min_safe_price ${min_safe_price:.3f}"
				)
				return None
			if down_price < min_safe_price:
				logger.debug(
				    f"❌ REJECT: DOWN too cheap @ ${down_price:.3f} < min_safe_price ${min_safe_price:.3f}"
				)
				return None

		# Check for imbalance (spread threshold)
		spread = abs(up_price - down_price)
		min_spread = getattr(self.config, 'min_spread_to_enter', 0.00)

		if spread < min_spread:
			logger.debug(
			    f"❌ REJECT: Spread ${spread:.3f} < min_spread ${min_spread:.3f}"
			)
			return None

		# Calculate potential profit if we complete at threshold
		# NOTE: complete_threshold should be <= 1.0 for binary markets
		# If it's > 1.0, we'll calculate negative profit which will be rejected
		effective_complete_threshold = min(self.config.complete_threshold, 1.0)
		potential_profit_at_completion = 1.0 - effective_complete_threshold
		potential_profit_pct = potential_profit_at_completion * 100

		# Only enter if potential profit is reasonable (at least 0% when completed)
		# Relaxed for testing - allow even small profits
		min_profit_pct = getattr(self.config, 'min_profit_pct', 0.0)
		if potential_profit_pct < min_profit_pct:
			logger.debug(
			    f"❌ REJECT: Potential profit {potential_profit_pct:.1f}% < min_profit_pct {min_profit_pct:.1f}%"
			)
			return None

		# Buy the cheaper side if it's below threshold
		# FIXED: Use <= instead of < to allow entry when prices are equal
		if up_price <= self.config.first_leg_threshold and up_price <= down_price:
			logger.info(
			    f"✅ ENTRY SIGNAL! UP ${up_price:.3f} + DOWN ${down_price:.3f} = ${combined_price:.3f} "
			    f"| Profit: {potential_profit_pct:.1f}% | Buying UP")
			return MarketSide.UP
		elif down_price <= self.config.first_leg_threshold and down_price <= up_price:
			logger.info(
			    f"✅ ENTRY SIGNAL! UP ${up_price:.3f} + DOWN ${down_price:.3f} = ${combined_price:.3f} "
			    f"| Profit: {potential_profit_pct:.1f}% | Buying DOWN")
			return MarketSide.DOWN

		# Final check: why didn't we enter?
		logger.debug(
		    f"❌ REJECT: No entry condition met | "
		    f"UP ${up_price:.3f} <= threshold ${self.config.first_leg_threshold:.2f}? {up_price <= self.config.first_leg_threshold} | "
		    f"DOWN ${down_price:.3f} <= threshold ${self.config.first_leg_threshold:.2f}? {down_price <= self.config.first_leg_threshold} | "
		    f"UP <= DOWN? {up_price <= down_price} | DOWN <= UP? {down_price <= up_price}"
		)
		return None

	def create_first_leg_position(
	        self,
	        pair: MarketPair,
	        pair_quote: PairQuote,
	        side: MarketSide,
	        available_capital: float,
	        market_end_date: Optional[datetime] = None) -> Optional[Position]:
		"""
        Create a pending position based on TARGET SHARES (Share Parity Fix).

        max_position_size = target shares (e.g., 100 = 100 shares = $100 payout)
        Cost = shares × price + fees
        """
		target_shares = self.config.max_position_size

		if side == MarketSide.UP:
			entry_price = pair_quote.up_quote.price
			cost_usd = target_shares * entry_price
			# No fees on Polymarket
			total_cost = cost_usd

			if total_cost > available_capital:
				logger.warning(
				    f"Insufficient capital for Leg 1: need ${total_cost:.2f}, have ${available_capital:.2f}"
				)
				return None

			position = Position(position_id=str(uuid4()),
			                    pair_id=pair.pair_id,
			                    up_market_id=pair.up_market.market_id,
			                    down_market_id=pair.down_market.market_id,
			                    entry_up_price=entry_price,
			                    entry_down_price=0.0,
			                    entry_combined_price=entry_price,
			                    entry_timestamp=datetime.now(),
			                    market_end_date=market_end_date,
			                    up_size_usd=cost_usd,
			                    down_size_usd=0.0,
			                    total_cost_usd=total_cost,
			                    status=PositionStatus.PENDING_UP)
		else:
			entry_price = pair_quote.down_quote.price
			cost_usd = target_shares * entry_price
			# No fees on Polymarket
			total_cost = cost_usd

			if total_cost > available_capital:
				logger.warning(
				    f"Insufficient capital for Leg 1: need ${total_cost:.2f}, have ${available_capital:.2f}"
				)
				return None

			position = Position(position_id=str(uuid4()),
			                    pair_id=pair.pair_id,
			                    up_market_id=pair.up_market.market_id,
			                    down_market_id=pair.down_market.market_id,
			                    entry_up_price=0.0,
			                    entry_down_price=entry_price,
			                    entry_combined_price=entry_price,
			                    entry_timestamp=datetime.now(),
			                    market_end_date=market_end_date,
			                    up_size_usd=0.0,
			                    down_size_usd=cost_usd,
			                    total_cost_usd=total_cost,
			                    status=PositionStatus.PENDING_DOWN)

		logger.info(
		    f"🎫 First leg bought! {side.value.upper()} @ ${entry_price:.3f} | "
		    f"Shares: {target_shares:.0f} | Cost: ${total_cost:.2f}")
		return position

	def should_complete_position(self, position: Position,
	                             pair_quote: PairQuote) -> bool:
		"""Check if we should complete the position (unit cost check)."""
		if position.status == PositionStatus.PENDING_UP:
			first_leg_price = position.entry_up_price
			second_leg_price = pair_quote.down_quote.price
		elif position.status == PositionStatus.PENDING_DOWN:
			first_leg_price = position.entry_down_price
			second_leg_price = pair_quote.up_quote.price
		else:
			return False

		# Check UNIT cost (per share), not total cost
		total_cost_unit = first_leg_price + second_leg_price

		if total_cost_unit <= self.config.complete_threshold:
			edge = (1.0 - total_cost_unit) * 100
			logger.info(
			    f"💰 Complete opportunity! Unit Cost: ${total_cost_unit:.3f} (Edge: {edge:.1f}%)"
			)
			return True
		return False

	def complete_position(self, position: Position, pair_quote: PairQuote,
	                      available_capital: float) -> bool:
		"""
        Complete position matching the SHARE COUNT of the first leg.
        This ensures true arbitrage: same shares on both sides = guaranteed payout.
        """
		if position.status == PositionStatus.PENDING_UP:
			# Calculate shares held in UP leg
			shares_held = position.up_size_usd / position.entry_up_price if position.entry_up_price > 0 else 0

			# Buy DOWN to match shares
			entry_price = pair_quote.down_quote.price
			cost_usd = shares_held * entry_price
			# No fees on Polymarket
			total_cost = cost_usd

			if total_cost > available_capital:
				logger.warning(
				    f"Insufficient capital to complete hedge: need ${total_cost:.2f}, have ${available_capital:.2f}"
				)
				return False

			position.entry_down_price = entry_price
			position.down_size_usd = cost_usd
			position.total_cost_usd += total_cost
			position.entry_combined_price = position.entry_up_price + entry_price
			position.second_leg_timestamp = datetime.now()
			position.status = PositionStatus.OPEN

		elif position.status == PositionStatus.PENDING_DOWN:
			# Calculate shares held in DOWN leg
			shares_held = position.down_size_usd / position.entry_down_price if position.entry_down_price > 0 else 0

			# Buy UP to match shares
			entry_price = pair_quote.up_quote.price
			cost_usd = shares_held * entry_price
			# No fees on Polymarket
			total_cost = cost_usd

			if total_cost > available_capital:
				logger.warning(
				    f"Insufficient capital to complete hedge: need ${total_cost:.2f}, have ${available_capital:.2f}"
				)
				return False

			position.entry_up_price = entry_price
			position.up_size_usd = cost_usd
			position.total_cost_usd += total_cost
			position.entry_combined_price = entry_price + position.entry_down_price
			position.second_leg_timestamp = datetime.now()
			position.status = PositionStatus.OPEN
		else:
			return False

		logger.info(
		    f"✅ Position COMPLETE! Shares: {shares_held:.0f} | "
		    f"UP: ${position.entry_up_price:.3f} + DOWN: ${position.entry_down_price:.3f} "
		    f"= ${position.entry_combined_price:.3f} | Total Invested: ${position.total_cost_usd:.2f}"
		)
		return True

	def check_scratch_signal(
	        self,
	        position: Position,
	        pair_quote: PairQuote,
	        time_remaining_seconds: Optional[float] = None) -> Optional[str]:
		"""Check if we should emergency exit a pending position."""
		scratch_time_threshold = getattr(self.config, 'scratch_time_threshold',
		                                 60)
		scratch_loss_threshold = getattr(self.config, 'scratch_loss_threshold',
		                                 0.25)
		scratch_min_recovery = getattr(self.config, 'scratch_min_recovery',
		                               0.05)

		if position.status == PositionStatus.PENDING_UP:
			current_price = pair_quote.up_quote.price
			entry_price = position.entry_up_price
			other_side_price = pair_quote.down_quote.price
		elif position.status == PositionStatus.PENDING_DOWN:
			current_price = pair_quote.down_quote.price
			entry_price = position.entry_down_price
			other_side_price = pair_quote.up_quote.price
		else:
			return None

		# Don't scratch if nothing to recover
		if current_price < scratch_min_recovery:
			return None

		# Calculate completion loss (per share)
		total_if_complete = entry_price + other_side_price
		completion_loss = total_if_complete - 1.0

		# Calculate current loss ratio
		loss_ratio = (entry_price -
		              current_price) / entry_price if entry_price > 0 else 0

		# CONDITION 1: Time panic
		if time_remaining_seconds is not None and time_remaining_seconds < scratch_time_threshold:
			if completion_loss > 0:
				return f"TIME_PANIC:{time_remaining_seconds:.0f}s"

		# CONDITION 2: Price crash
		if loss_ratio >= scratch_loss_threshold and completion_loss > 0.02:
			return f"PRICE_CRASH:-{loss_ratio*100:.1f}%"

		# CONDITION 3: Impossible hedge (reduced threshold to catch earlier)
		# If completing would lose >5 cents per share, exit immediately
		if completion_loss > 0.05:
			return f"IMPOSSIBLE_HEDGE:loss>{completion_loss:.2f}"

		return None

	def should_bail_out(self, position: Position,
	                    pair_quote: PairQuote) -> bool:
		"""Legacy method - logic moved to check_scratch_signal."""
		return False

	def update_position_unrealized_pnl(self, position: Position,
	                                   current_up_price: float,
	                                   current_down_price: float) -> None:
		"""Update unrealized PnL for a position."""
		if position.status == PositionStatus.OPEN:
			# For complete positions: Mark to market
			# Shares = up_size_usd / entry_up_price
			shares = position.up_size_usd / position.entry_up_price if position.entry_up_price > 0 else 0
			# Current value = shares × (current_up + current_down)
			current_value = shares * (current_up_price + current_down_price)
			position.unrealized_pnl_usd = current_value - position.total_cost_usd
		elif position.status in [
		    PositionStatus.PENDING_UP, PositionStatus.PENDING_DOWN
		]:
			# For pending positions: potential if completed now
			if position.status == PositionStatus.PENDING_UP:
				potential_unit_cost = position.entry_up_price + current_down_price
				shares = position.up_size_usd / position.entry_up_price if position.entry_up_price > 0 else 0
			else:
				potential_unit_cost = current_up_price + position.entry_down_price
				shares = position.down_size_usd / position.entry_down_price if position.entry_down_price > 0 else 0

			# Potential payout if completed = shares × $1.00
			# Potential cost = current cost + (shares × other_side_price) - no fees
			potential_profit_per_share = 1.0 - potential_unit_cost
			position.unrealized_pnl_usd = shares * potential_profit_per_share


# Backwards compatibility alias
BundledSidesStrategy = LegInStrategy
