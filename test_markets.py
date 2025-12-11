"""
Test script to inspect what markets are returned from Polymarket API.
"""
import json
import logging
from datetime import datetime, timedelta, timezone
from py_clob_client.client import ClobClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_markets():
    """Test fetching and inspecting markets."""
    import requests

    print("Fetching markets from Gamma API...")

    # Use events endpoint for better active market discovery
    events_url = "https://gamma-api.polymarket.com/events"
    markets_url = "https://gamma-api.polymarket.com/markets"

    all_markets = []

    # Strategy 1: Fetch active events (better for recent markets)
    print("Fetching active events (newest first)...")
    try:
        params = {
            "order": "id",
            "ascending": "false",  # Newest first
            "closed": "false",
            "limit": 100
        }

        for offset in [0, 100, 200]:  # Get first 300 events
            params["offset"] = offset
            response = requests.get(events_url, params=params, timeout=30)
            response.raise_for_status()
            events = response.json()

            if not events:
                break

            # Extract markets from events
            for event in events:
                event_markets = event.get("markets", [])
                for market in event_markets:
                    # Add event info to market
                    market["_event_title"] = event.get("title", "")
                    market["_event_slug"] = event.get("slug", "")
                    all_markets.append(market)

            print(f"  Fetched {len(events)} events (offset {offset}), total markets: {len(all_markets)}")

            if len(events) < 100:
                break

    except Exception as e:
        print(f"Error fetching events: {e}")

    # Strategy 2: Also try direct markets endpoint
    print("Fetching markets directly...")
    try:
        params = {
            "closed": "false",
            "limit": 100,
            "offset": 0
        }

        response = requests.get(markets_url, params=params, timeout=30)
        response.raise_for_status()
        direct_markets = response.json()

        # Deduplicate by conditionId
        seen_ids = {m.get("conditionId") or m.get("condition_id") for m in all_markets if m.get("conditionId") or m.get("condition_id")}

        for market in direct_markets:
            cid = market.get("conditionId") or market.get("condition_id")
            if cid and cid not in seen_ids:
                all_markets.append(market)
                seen_ids.add(cid)

        print(f"  Added {len(direct_markets)} from markets endpoint, total: {len(all_markets)}")

    except Exception as e:
        print(f"Error fetching direct markets: {e}")

    markets_data = all_markets
    print(f"\nTotal markets received: {len(markets_data)}")

    # Filter markets by date - only include recent markets (last 30 days or future)
    print("Filtering markets by date (last 30 days or future)...")
    now = datetime.now(timezone.utc)
    thirty_days_ago = now - timedelta(days=30)
    recent_markets = []

    for market in markets_data:
        # Get end date
        end_date_str = (
            market.get("endDateISO") or
            market.get("end_date_iso") or
            market.get("endDate") or
            market.get("end_date")
        )

        if end_date_str:
            try:
                # Parse date (handle different formats)
                if isinstance(end_date_str, str):
                    end_date_str = end_date_str.replace("Z", "+00:00")
                    end_date = datetime.fromisoformat(end_date_str)
                    # Ensure timezone-aware
                    if end_date.tzinfo is None:
                        end_date = end_date.replace(tzinfo=timezone.utc)
                else:
                    continue

                # Only include markets that end in the future or within last 30 days
                if end_date >= thirty_days_ago:
                    recent_markets.append(market)
            except (ValueError, AttributeError) as e:
                # If we can't parse the date, include it if it's active and not resolved
                if market.get("active") and not market.get("resolved", False):
                    recent_markets.append(market)
        else:
            # If no end date, check other date fields or exclude old markets
            # Check if market is resolved or closed (old markets)
            is_resolved = market.get("resolved", False) or market.get("closed", False)
            is_active = market.get("active", False)

            # Only include if active and not resolved
            # Also check creation date or other date fields
            created_date_str = (
                market.get("createdAt") or
                market.get("created_at") or
                market.get("createdDateISO") or
                market.get("created_date_iso")
            )

            if created_date_str:
                try:
                    if isinstance(created_date_str, str):
                        created_date_str = created_date_str.replace("Z", "+00:00")
                        created_date = datetime.fromisoformat(created_date_str)
                        if created_date.tzinfo is None:
                            created_date = created_date.replace(tzinfo=timezone.utc)
                        # Only include if created within last 30 days
                        if created_date >= thirty_days_ago and is_active and not is_resolved:
                            recent_markets.append(market)
                except (ValueError, AttributeError):
                    # If we can't parse creation date, only include if active and not resolved
                    if is_active and not is_resolved:
                        recent_markets.append(market)
            else:
                # No date info at all - only include if active and not resolved
                # But be more strict - exclude if question contains "2020" or earlier years
                question = market.get("question", "").lower()
                if is_active and not is_resolved and "2020" not in question and "2019" not in question:
                    recent_markets.append(market)

    # Sort markets by end date (most recent first)
    def get_end_date(market):
        end_date_str = (
            market.get("endDateISO") or
            market.get("end_date_iso") or
            market.get("endDate") or
            market.get("end_date")
        )
        if end_date_str:
            try:
                if isinstance(end_date_str, str):
                    end_date_str = end_date_str.replace("Z", "+00:00")
                    end_date = datetime.fromisoformat(end_date_str)
                    # Ensure timezone-aware
                    if end_date.tzinfo is None:
                        end_date = end_date.replace(tzinfo=timezone.utc)
                    return end_date
            except (ValueError, AttributeError):
                pass
        return datetime.min.replace(tzinfo=timezone.utc)  # Put markets without dates at the end

    recent_markets.sort(key=get_end_date, reverse=True)
    markets_data = recent_markets
    print(f"Markets after date filtering: {len(markets_data)}")

    # Show sample of what markets we got (for debugging)
    if markets_data:
        print("\nSample of recent markets found:")
        for i, market in enumerate(markets_data[:5], 1):
            question = market.get('question', 'N/A')[:60]
            slug = market.get('slug', 'N/A')[:40]
            end_date = market.get('endDateISO', market.get('end_date_iso', 'N/A'))
            print(f"  {i}. {question}... | Slug: {slug}... | End: {end_date}")

    # Look for BTC-related markets
    btc_markets = []
    up_or_down_markets = []

    for market in markets_data:
        question = market.get("question", "").lower()
        title = market.get("title", "").lower() if market.get("title") else ""
        slug = market.get("slug", "").lower() if market.get("slug") else ""
        search_text = f"{question} {title} {slug}".lower()

        if "btc" in search_text or "bitcoin" in search_text:
            btc_markets.append(market)

        # Specifically look for "up or down" pattern or slug pattern
        slug = market.get("slug", "").lower()
        has_updown_pattern = (
            "up or down" in search_text or
            "up/down" in search_text or
            "updown" in search_text or
            "up-down" in search_text or
            "btc-updown" in slug or
            "bitcoin-updown" in slug
        )

        if has_updown_pattern:
            if "bitcoin" in search_text or "btc" in search_text or "btc" in slug:
                up_or_down_markets.append(market)

    print(f"\nFound {len(btc_markets)} BTC-related markets:")
    print(f"Found {len(up_or_down_markets)} BTC 'Up or Down' markets:")

    if up_or_down_markets:
        print("\n" + "="*60)
        print("BTC Up or Down Markets Found:")
        print("="*60)
        for i, market in enumerate(up_or_down_markets[:10], 1):
            print(f"\n{i}. Question: {market.get('question', 'N/A')}")
            print(f"   Slug: {market.get('slug', 'N/A')}")
            print(f"   Condition ID: {market.get('conditionId', market.get('condition_id', 'N/A'))}")
            outcomes = market.get('outcomes', [])
            if outcomes:
                if isinstance(outcomes[0], dict):
                    print(f"   Outcomes: {[o.get('name', o.get('title', '')) for o in outcomes]}")
                else:
                    print(f"   Outcomes: {outcomes}")
            print(f"   Active: {market.get('active', 'N/A')}")
            print(f"   Closed: {market.get('closed', 'N/A')}")

    print(f"\nShowing first 10 of {len(btc_markets)} BTC-related markets:")

    for i, market in enumerate(btc_markets[:10], 1):  # Show first 10
        print(f"\n{i}. Question: {market.get('question', 'N/A')}")
        print(f"   Title: {market.get('title', 'N/A')}")
        print(f"   Slug: {market.get('slug', 'N/A')}")
        print(f"   Condition ID: {market.get('conditionId', market.get('condition_id', 'N/A'))}")
        outcomes = market.get('outcomes', [])
        # Handle different outcome formats
        if outcomes:
            if isinstance(outcomes[0], dict):
                print(f"   Outcomes: {[o.get('name', o.get('title', '')) for o in outcomes]}")
                print(f"   Token IDs: {[o.get('token_id', o.get('tokenId', '')) for o in outcomes]}")
            else:
                print(f"   Outcomes: {outcomes}")
        else:
            print(f"   Outcomes: None")
        end_date_str = market.get('endDateISO', market.get('end_date_iso', 'N/A'))
        print(f"   End Date: {end_date_str}")
        print(f"   Active: {market.get('active', 'N/A')}")
        print(f"   Resolved: {market.get('resolved', market.get('closed', False))}")

        # Show if market is recent
        if end_date_str != 'N/A':
            try:
                if isinstance(end_date_str, str):
                    end_date_str_parsed = end_date_str.replace("Z", "+00:00")
                    end_date = datetime.fromisoformat(end_date_str_parsed)
                    # Ensure timezone-aware
                    if end_date.tzinfo is None:
                        end_date = end_date.replace(tzinfo=timezone.utc)
                    now = datetime.now(timezone.utc)
                    days_until = (end_date - now).days
                    if days_until > 0:
                        print(f"   ⏰ Ends in {days_until} days")
                    elif days_until == 0:
                        print(f"   ⏰ Ends today")
                    else:
                        print(f"   ⏰ Ended {abs(days_until)} days ago")
            except (ValueError, AttributeError):
                pass
        # Print full market structure for first market
        if i == 1:
            print(f"\n   Full market structure (first 500 chars):")
            print(json.dumps(market, indent=2, default=str)[:500])

    # Look specifically for up/down 15m markets
    print("\n" + "="*60)
    print("Looking for BTC up/down 15m markets...")
    print("="*60)

    updown_15m = []
    for market in markets_data:
        question = market.get("question", "").lower()
        title = market.get("title", "").lower() if market.get("title") else ""
        slug = market.get("slug", "").lower() if market.get("slug") else ""
        search_text = f"{question} {title} {slug}".lower()

        has_btc = "btc" in search_text or "bitcoin" in search_text
        has_updown = "updown" in search_text or "up down" in search_text or "up or down" in search_text
        has_15m = "15m" in search_text or "15" in search_text or "15 min" in search_text or "15-minute" in search_text

        if has_btc and has_updown and has_15m:
            updown_15m.append(market)

    print(f"\nFound {len(updown_15m)} BTC up/down 15m markets:")
    for i, market in enumerate(updown_15m[:5], 1):
        print(f"\n{i}. {market.get('question', market.get('title', 'N/A'))}")
        print(f"   Slug: {market.get('slug', 'N/A')}")
        print(f"   Condition ID: {market.get('conditionId', market.get('condition_id', 'N/A'))}")
        outcomes = market.get('outcomes', [])
        if outcomes:
            if isinstance(outcomes[0], dict):
                print(f"   Outcomes: {[o.get('name', o.get('title', '')) for o in outcomes]}")
                print(f"   Token IDs: {[o.get('token_id', o.get('tokenId', '')) for o in outcomes]}")
            else:
                print(f"   Outcomes: {outcomes}")
        else:
            print(f"   Outcomes: None")

    # Save sample to file for inspection
    if markets_data:
        sample = {
            "total": len(markets_data),
            "sample_markets": markets_data[:5],
            "btc_markets": btc_markets[:5],
            "updown_15m": updown_15m[:5]
        }
        with open("market_sample.json", "w") as f:
            json.dump(sample, f, indent=2)
        print(f"\nSaved sample to market_sample.json")

if __name__ == "__main__":
    test_markets()
