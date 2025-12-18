from src.utils.market_utils import extract_strike_price


def test_extract_strike_price_keywords():
    text = "Price to beat: $90,123.45 at settlement."
    assert extract_strike_price(text) == 90123.45


def test_extract_strike_price_dollar_prefixed():
    text = "Bitcoin reference price is $65000."
    assert extract_strike_price(text) == 65000.0


def test_extract_strike_price_fallback_commas():
    text = "Closing value 92,000 will be used."
    assert extract_strike_price(text) == 92000.0


def test_extract_strike_price_ignores_year_like_numbers():
    text = "Bitcoin Up or Down - December 13, 2025 2:30AM-2:45AM ET"
    assert extract_strike_price(text) is None


def test_extract_strike_price_ignores_epoch_suffix():
    text = "btc-updown-15m-1765694700"
    assert extract_strike_price(text) is None


def test_extract_strike_price_prefers_strike_over_epoch():
    text = "btc-updown-15m-1765694700 Price to beat: $90,000"
    assert extract_strike_price(text) == 90000.0


def test_extract_strike_price_empty():
    assert extract_strike_price("") is None
