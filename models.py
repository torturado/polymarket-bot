"""
Data models for markets, quotes, positions, and PnL records.
"""
from datetime import datetime
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field


class MarketSide(str, Enum):
    """Market side enumeration."""
    UP = "up"
    DOWN = "down"


class PositionStatus(str, Enum):
    """Position status enumeration."""
    PENDING_UP = "pending_up"      # Only UP leg bought, waiting for DOWN
    PENDING_DOWN = "pending_down"  # Only DOWN leg bought, waiting for UP
    OPEN = "open"                  # Both legs bought, waiting for resolution
    CLOSED = "closed"
    RESOLVED = "resolved"
    SCRATCHED = "scratched"        # Emergency exit - sold pending position at loss


class Market(BaseModel):
    """Represents a Polymarket market."""
    market_id: str = Field(..., description="Unique market identifier (token_id for CLOB)")
    question: str = Field(..., description="Market question/title")
    condition_id: str = Field(..., description="Condition ID for this market")
    token_id: Optional[str] = Field(None, description="Token ID for this specific side (UP or DOWN)")
    side: MarketSide = Field(..., description="Whether this is UP or DOWN side")
    end_date: Optional[datetime] = Field(None, description="Market end/resolution date")
    is_resolved: bool = Field(False, description="Whether the market has been resolved")
    resolution: Optional[MarketSide] = Field(None, description="Resolution outcome if resolved")

    class Config:
        use_enum_values = True


class MarketPair(BaseModel):
    """Represents a pair of UP and DOWN markets."""
    up_market: Market = Field(..., description="UP side market")
    down_market: Market = Field(..., description="DOWN side market")
    pair_id: str = Field(..., description="Unique identifier for this pair")

    def get_combined_price(self, up_price: float, down_price: float) -> float:
        """Calculate combined price for both sides."""
        return up_price + down_price


class Quote(BaseModel):
    """Represents a price quote for a market."""
    market_id: str = Field(..., description="Market ID this quote is for")
    side: MarketSide = Field(..., description="Market side")
    price: float = Field(..., ge=0.0, le=1.0, description="Current price (0.0 to 1.0)")
    timestamp: datetime = Field(default_factory=datetime.now, description="Quote timestamp")

    class Config:
        use_enum_values = True


class PairQuote(BaseModel):
    """Represents quotes for both sides of a market pair."""
    pair_id: str = Field(..., description="Market pair ID")
    up_quote: Quote = Field(..., description="UP side quote")
    down_quote: Quote = Field(..., description="DOWN side quote")
    combined_price: float = Field(..., ge=0.0, le=2.0, description="Combined price (up + down)")
    edge_cents: float = Field(..., description="Profit edge in cents (100 - combined_price * 100)")

    @classmethod
    def create(cls, pair: MarketPair, up_quote: Quote, down_quote: Quote) -> "PairQuote":
        """Create a PairQuote from a MarketPair and two quotes."""
        combined = up_quote.price + down_quote.price
        edge_cents = max(0.0, (1.0 - combined) * 100.0)
        return cls(
            pair_id=pair.pair_id,
            up_quote=up_quote,
            down_quote=down_quote,
            combined_price=combined,
            edge_cents=edge_cents
        )


class Position(BaseModel):
    """Represents a paper-trading position."""
    position_id: str = Field(..., description="Unique position identifier")
    pair_id: str = Field(..., description="Market pair ID")
    up_market_id: str = Field(..., description="UP market ID")
    down_market_id: str = Field(..., description="DOWN market ID")

    # Entry details - can be 0 for pending positions (only one leg)
    entry_up_price: float = Field(default=0.0, ge=0.0, le=1.0, description="Entry price for UP side")
    entry_down_price: float = Field(default=0.0, ge=0.0, le=1.0, description="Entry price for DOWN side")
    entry_combined_price: float = Field(default=0.0, description="Combined entry price")
    entry_timestamp: datetime = Field(default_factory=datetime.now, description="Entry timestamp")
    second_leg_timestamp: Optional[datetime] = Field(None, description="When second leg was bought")

    # Market expiration (stored to enable auto-resolution after market change)
    market_end_date: Optional[datetime] = Field(None, description="When the market expires")

    # Position sizing - can be 0 for pending positions
    up_size_usd: float = Field(default=0.0, ge=0.0, description="Position size in USD for UP side")
    down_size_usd: float = Field(default=0.0, ge=0.0, description="Position size in USD for DOWN side")
    total_cost_usd: float = Field(default=0.0, ge=0.0, description="Total cost in USD")

    # Status
    status: PositionStatus = Field(default=PositionStatus.OPEN, description="Position status")
    exit_timestamp: Optional[datetime] = Field(None, description="Exit/resolution timestamp")

    # PnL
    realized_pnl_usd: float = Field(default=0.0, description="Realized PnL in USD")
    unrealized_pnl_usd: float = Field(default=0.0, description="Unrealized PnL in USD")

    class Config:
        use_enum_values = True

    def calculate_unrealized_pnl(self, current_up_price: float, current_down_price: float, fee_percent: float) -> float:
        """Calculate unrealized PnL based on current prices."""
        # Value if UP wins: up_size_usd * (1.0 / entry_up_price) - fees
        # Value if DOWN wins: down_size_usd * (1.0 / entry_down_price) - fees
        # Expected value: weighted average

        up_payout = (self.up_size_usd / self.entry_up_price) * (1.0 - fee_percent / 100.0)
        down_payout = (self.down_size_usd / self.entry_down_price) * (1.0 - fee_percent / 100.0)

        # Current market-implied probability
        up_prob = current_up_price
        down_prob = current_down_price

        # Normalize probabilities
        total_prob = up_prob + down_prob
        if total_prob > 0:
            up_prob /= total_prob
            down_prob /= total_prob
        else:
            up_prob = down_prob = 0.5

        expected_value = (up_payout * up_prob) + (down_payout * down_prob)
        return expected_value - self.total_cost_usd

    def resolve(self, winning_side: MarketSide, fee_percent: float) -> float:
        """
        Resolve the position and calculate realized PnL.

        CRITICAL: Payout is shares * $1.00 (no fee on payout).
        Fees are only applied when buying, which is already in total_cost_usd.
        """
        if winning_side == MarketSide.UP:
            # Calculate shares: USD invested / entry price
            shares = self.up_size_usd / self.entry_up_price if self.entry_up_price > 0 else 0
            # Payout: $1.00 per share (no fee on payout)
            payout = shares * 1.0
        else:
            # Calculate shares: USD invested / entry price
            shares = self.down_size_usd / self.entry_down_price if self.entry_down_price > 0 else 0
            # Payout: $1.00 per share (no fee on payout)
            payout = shares * 1.0

        # PnL = Payout - Total Cost (cost already includes fees)
        self.realized_pnl_usd = payout - self.total_cost_usd
        self.status = PositionStatus.RESOLVED
        self.exit_timestamp = datetime.now()

        return self.realized_pnl_usd


class PnLRecord(BaseModel):
    """Record of PnL at a point in time."""
    timestamp: datetime = Field(default_factory=datetime.now, description="Record timestamp")
    total_realized_pnl_usd: float = Field(default=0.0, description="Total realized PnL")
    total_unrealized_pnl_usd: float = Field(default=0.0, description="Total unrealized Pnl")
    total_pnl_usd: float = Field(default=0.0, description="Total PnL (realized + unrealized)")
    open_positions_count: int = Field(default=0, description="Number of open positions")
    total_capital_deployed: float = Field(default=0.0, description="Total capital currently deployed")
