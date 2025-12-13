import asyncio

from src.config import Config
from src.execution_engine import ExecutionEngine


class _DummyClient:
    pass


def test_execute_burst_buy_splits_and_vwap():
    cfg = Config(
        paper_trading=True,
        use_burst_execution=True,
        burst_min_chunk_shares=10.0,
        burst_max_chunk_shares=10.0,
        burst_max_orders=10,
        burst_min_delay_ms=0,
        burst_max_delay_ms=0,
    )
    eng = ExecutionEngine(_DummyClient(), cfg)

    filled, vwap, sent = asyncio.run(
        eng.execute_burst_buy(
            token_id="t1",
            total_size=25.0,
            reference_price=0.2,
        )
    )
    assert filled == 25.0
    assert sent == 3
    assert abs(vwap - 0.2) < 1e-12


def test_execute_burst_buy_respects_max_orders():
    cfg = Config(
        paper_trading=True,
        use_burst_execution=True,
        burst_min_chunk_shares=10.0,
        burst_max_chunk_shares=10.0,
        burst_max_orders=5,
        burst_min_delay_ms=0,
        burst_max_delay_ms=0,
    )
    eng = ExecutionEngine(_DummyClient(), cfg)

    filled, _vwap, sent = asyncio.run(
        eng.execute_burst_buy(
            token_id="t1",
            total_size=100.0,
            reference_price=0.2,
        )
    )
    assert sent == 5
    assert filled == 50.0


def test_execute_burst_buy_respects_max_price():
    cfg = Config(
        paper_trading=True,
        use_burst_execution=True,
        burst_min_chunk_shares=10.0,
        burst_max_chunk_shares=10.0,
        burst_max_orders=5,
        burst_min_delay_ms=0,
        burst_max_delay_ms=0,
        burst_max_price_slippage=0.0,
    )
    eng = ExecutionEngine(_DummyClient(), cfg)

    def price_fn():
        return 0.6

    filled, _vwap, sent = asyncio.run(
        eng.execute_burst_buy(
            token_id="t1",
            total_size=10.0,
            reference_price=0.5,
            max_price=0.55,
            price_fn=price_fn,
        )
    )
    assert sent == 0
    assert filled == 0.0

