import asyncio
import time

from src.config import Config
from src.leg_in_bot import LegInBot
from src.utils.market_utils import MarketUpdate, PriceData


def test_arb_flow_paper_trading(tmp_path):
    cfg = Config(
        paper_trading=True,
        paper_trades_log=str(tmp_path / "paper_trades.csv"),
        arb_enabled=True,
        arb_max_entry_exit_cost=0.99,
        arb_budget_usdc=99.0,
        market_specs=[
            {
                "condition_id": "c1",
                "yes_token_id": "yes1",
                "no_token_id": "no1",
            }
        ],
    )
    bot = LegInBot(cfg)

    now = time.time()
    update = MarketUpdate(
        condition_id="c1",
        yes_token_id="yes1",
        no_token_id="no1",
        prices={
            "YES": PriceData(best_bid=0.48, best_ask=0.49, timestamp=now),
            "NO": PriceData(best_bid=0.48, best_ask=0.49, timestamp=now),
        },
        received_at=now,
    )

    async def _run():
        await bot._handle_update(update)
        task = bot._tasks_by_condition.get("c1")
        assert task is not None
        await task
        assert "c1" not in bot.positions

    asyncio.run(_run())

