#!/usr/bin/env python
from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
import sys
import urllib.parse
import urllib.request
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

from py_clob_client.client import ClobClient

# Ensure repo root is on sys.path when running from elsewhere.
ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.config import Config  # noqa: E402


DEFAULT_OUTPUT = "market_specs.json"
DEFAULT_GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
DEFAULT_MARKET_TIMEZONE = "America/New_York"
DEFAULT_WINDOW_MINUTES = 15

_EPOCH_SUFFIX_RE = re.compile(r"-(\d{9,})$")
_TAG_MINUTES_RE = re.compile(r"^(\d+)\s*[mM]$")


@dataclass(frozen=True)
class SelectedMarket:
    label: str
    condition_id: str
    yes_token_id: str
    no_token_id: str


@dataclass
class ScanStats:
    scanned: int = 0
    active: int = 0
    regex_matched: int = 0
    window_matched: int = 0
    selected: int = 0
    missing_condition_id: int = 0
    missing_tokens: int = 0
    sample_labels: List[str] = field(default_factory=list)
    regex_matches: List[str] = field(default_factory=list)
    regex_matches_missing_data: List[str] = field(default_factory=list)
    window_start_local: Optional[str] = None
    window_end_local: Optional[str] = None
    window_start_epoch_utc: Optional[int] = None
    window_tz: Optional[str] = None
    window_minutes: Optional[int] = None
    window_minutes_env: Optional[int] = None
    window_minutes_inferred: Optional[int] = None
    gamma_tag_slug: Optional[str] = None


def _iter_pages(
    client: ClobClient, *, source: str, max_pages: int
) -> Iterable[Dict[str, Any]]:
    next_cursor: Optional[str] = "MA=="
    for _ in range(max_pages):
        if source == "sampling_simplified":
            resp = client.get_sampling_simplified_markets(next_cursor)
        elif source == "markets":
            resp = client.get_markets(next_cursor)
        else:
            resp = client.get_simplified_markets(next_cursor)

        if isinstance(resp, list):
            data = resp
            next_cursor = None
        elif isinstance(resp, dict):
            data = resp.get("data") or resp.get("markets") or []
            next_cursor = (
                resp.get("next_cursor")
                or resp.get("nextCursor")
                or resp.get("next")
                or None
            )
        else:
            data = []
            next_cursor = None

        for m in data:
            if isinstance(m, dict):
                yield m

        if not next_cursor or next_cursor in {"END", "END_CURSOR"}:
            break


def _compute_current_window(
    *,
    tz_name: str,
    window_minutes: int,
    now: Optional[_dt.datetime] = None,
) -> tuple[_dt.datetime, _dt.datetime, int]:
    tz = ZoneInfo(tz_name)
    now_local = now or _dt.datetime.now(tz)
    if now_local.tzinfo is None:
        now_local = now_local.replace(tzinfo=tz)

    if window_minutes <= 0 or 60 % window_minutes != 0:
        raise ValueError("window_minutes must be a divisor of 60 (e.g. 15)")

    minute_bucket = (now_local.minute // window_minutes) * window_minutes
    window_start_local = now_local.replace(
        minute=minute_bucket, second=0, microsecond=0
    )
    window_end_local = window_start_local + _dt.timedelta(minutes=window_minutes)
    window_start_epoch_utc = int(
        window_start_local.astimezone(_dt.timezone.utc).timestamp()
    )
    return window_start_local, window_end_local, window_start_epoch_utc


def _extract_epoch_suffix(value: Any) -> Optional[int]:
    if not isinstance(value, str):
        return None
    m = _EPOCH_SUFFIX_RE.search(value.strip())
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _parse_iso8601_to_epoch(value: Any) -> Optional[int]:
    if not isinstance(value, str) or not value.strip():
        return None
    s = value.strip()
    try:
        dt = _dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_dt.timezone.utc)
    return int(dt.timestamp())


def _infer_window_minutes_from_tag_slug(tag_slug: Any) -> Optional[int]:
    if not isinstance(tag_slug, str):
        return None
    m = _TAG_MINUTES_RE.match(tag_slug.strip())
    if not m:
        return None
    try:
        minutes = int(m.group(1))
    except Exception:
        return None
    if minutes <= 0 or 60 % minutes != 0:
        return None
    return minutes


def _fetch_json(url: str, *, timeout_s: int = 30) -> Any:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "polymarket-bot/market-specs-updater",
            "Accept": "application/json",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        raw = resp.read()
    return json.loads(raw.decode("utf-8"))


def _iter_gamma_events(
    *,
    base_url: str,
    tag_slug: str,
    limit: int,
    active: bool,
    archived: bool,
    closed: bool,
    order: str,
    ascending: bool,
    offset: int,
    max_pages: int,
) -> Iterable[Dict[str, Any]]:
    for _ in range(max_pages):
        query = {
            "limit": str(limit),
            "active": "true" if active else "false",
            "archived": "true" if archived else "false",
            "tag_slug": tag_slug,
            "closed": "true" if closed else "false",
            "order": order,
            "ascending": "true" if ascending else "false",
            "offset": str(offset),
        }
        url = f"{base_url.rstrip('/')}/events/pagination?{urllib.parse.urlencode(query)}"
        payload = _fetch_json(url)
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, list) or not data:
            break
        for event in data:
            if isinstance(event, dict):
                yield event
        if len(data) < limit:
            break
        offset += limit


def _market_text_fields(market: Dict[str, Any]) -> Iterable[str]:
    for key in (
        "event_slug",
        "eventSlug",
        "slug",
        "market_slug",
        "marketSlug",
        "series_slug",
        "seriesSlug",
        "question",
        "title",
        "name",
    ):
        val = market.get(key)
        if isinstance(val, str):
            yield val


def _is_active(market: Dict[str, Any]) -> bool:
    for key in ("active", "is_active", "isActive"):
        if key in market:
            return bool(market.get(key))
    for key in ("closed", "is_closed", "isClosed"):
        if key in market:
            return not bool(market.get(key))
    return True


def _extract_condition_id(market: Dict[str, Any]) -> Optional[str]:
    for key in ("condition_id", "conditionId", "conditionID"):
        val = market.get(key)
        if val:
            return str(val)
    condition = market.get("condition")
    if isinstance(condition, dict):
        for key in ("id", "condition_id", "conditionId"):
            val = condition.get(key)
            if val:
                return str(val)
    return None


def _market_label(market: Dict[str, Any]) -> str:
    for key in (
        "event_slug",
        "eventSlug",
        "slug",
        "market_slug",
        "marketSlug",
        "series_slug",
        "seriesSlug",
        "question",
        "title",
        "name",
    ):
        val = market.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()

    condition_id = _extract_condition_id(market)
    if condition_id:
        return condition_id

    for key in ("market", "id"):
        val = market.get(key)
        if val:
            return str(val)

    return "<unknown>"


def _event_label(event: Dict[str, Any]) -> str:
    for key in ("ticker", "slug", "title", "name"):
        val = event.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    eid = event.get("id")
    return str(eid) if eid is not None else "<unknown-event>"


def _extract_yes_no_tokens(market: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    for yes_key, no_key in (
        ("yes_token_id", "no_token_id"),
        ("yesTokenId", "noTokenId"),
    ):
        yes = market.get(yes_key)
        no = market.get(no_key)
        if yes and no:
            return str(yes), str(no)

    tokens = (
        market.get("tokens")
        or market.get("outcomes")
        or market.get("assets")
        or market.get("outcome_tokens")
    )
    if isinstance(tokens, list):
        yes_id = None
        no_id = None
        for t in tokens:
            if not isinstance(t, dict):
                continue
            tid = (
                t.get("token_id")
                or t.get("tokenId")
                or t.get("asset_id")
                or t.get("assetId")
                or t.get("id")
            )
            outcome = t.get("outcome") or t.get("name") or t.get("side")
            if tid and outcome:
                o = str(outcome).strip().lower()
                if o == "yes":
                    yes_id = str(tid)
                elif o == "no":
                    no_id = str(tid)
        if yes_id and no_id:
            return yes_id, no_id

        if len(tokens) == 2:
            t0 = tokens[0] if isinstance(tokens[0], dict) else {}
            t1 = tokens[1] if isinstance(tokens[1], dict) else {}
            tid0 = (
                t0.get("token_id")
                or t0.get("tokenId")
                or t0.get("asset_id")
                or t0.get("assetId")
                or t0.get("id")
            )
            tid1 = (
                t1.get("token_id")
                or t1.get("tokenId")
                or t1.get("asset_id")
                or t1.get("assetId")
                or t1.get("id")
            )
            if tid0 and tid1:
                return str(tid0), str(tid1)

    return None


def _extract_gamma_market_condition_id(market: Dict[str, Any]) -> Optional[str]:
    for key in ("conditionId", "condition_id", "conditionID"):
        val = market.get(key)
        if val:
            return str(val)
    return None


def _extract_gamma_clob_token_ids(market: Dict[str, Any]) -> Optional[List[str]]:
    val = market.get("clobTokenIds") or market.get("clob_token_ids")
    if val is None:
        return None
    if isinstance(val, list):
        return [str(v) for v in val if v]
    if isinstance(val, str) and val.strip():
        try:
            parsed = json.loads(val)
        except json.JSONDecodeError:
            return None
        if isinstance(parsed, list):
            return [str(v) for v in parsed if v]
    return None


def build_market_specs(
    client: ClobClient,
    *,
    filter_regex: Optional[re.Pattern],
    source: str,
    max_pages: int,
    limit: int,
    sample_size: int = 0,
    regex_print_limit: int = 0,
) -> tuple[List[SelectedMarket], ScanStats]:
    selected: List[SelectedMarket] = []
    stats = ScanStats()
    seen_samples = set()

    if source == "gamma_15m":
        gamma_base_url = os.getenv("GAMMA_API_BASE_URL", DEFAULT_GAMMA_BASE_URL)
        tag_slug = os.getenv("GAMMA_TAG_SLUG", "15M")
        gamma_limit = int(os.getenv("GAMMA_LIMIT", "100"))
        gamma_offset = int(os.getenv("GAMMA_OFFSET", "0"))
        gamma_order = os.getenv("GAMMA_ORDER", "volume24hr")
        gamma_ascending = os.getenv("GAMMA_ASCENDING", "false").lower() in {"1", "true", "yes"}
        gamma_active = os.getenv("GAMMA_ACTIVE", "true").lower() in {"1", "true", "yes"}
        gamma_archived = os.getenv("GAMMA_ARCHIVED", "false").lower() in {"1", "true", "yes"}
        gamma_closed = os.getenv("GAMMA_CLOSED", "false").lower() in {"1", "true", "yes"}
        filter_current_window = os.getenv("MARKET_FILTER_CURRENT_WINDOW", "true").lower() in {
            "1",
            "true",
            "yes",
            "y",
            "on",
        }
        window_tz = os.getenv("MARKET_TIMEZONE", DEFAULT_MARKET_TIMEZONE)
        window_minutes_env = int(os.getenv("MARKET_WINDOW_MINUTES", str(DEFAULT_WINDOW_MINUTES)))
        window_minutes_inferred = _infer_window_minutes_from_tag_slug(tag_slug)
        window_minutes = window_minutes_inferred or window_minutes_env
        window_start_epoch_utc: Optional[int] = None
        if filter_current_window:
            ws, we, epoch = _compute_current_window(
                tz_name=window_tz,
                window_minutes=window_minutes,
            )
            stats.window_start_local = ws.isoformat()
            stats.window_end_local = we.isoformat()
            stats.window_start_epoch_utc = epoch
            stats.window_tz = window_tz
            stats.window_minutes = window_minutes
            stats.window_minutes_env = window_minutes_env
            stats.window_minutes_inferred = window_minutes_inferred
            stats.gamma_tag_slug = tag_slug
            window_start_epoch_utc = epoch

        for event in _iter_gamma_events(
            base_url=gamma_base_url,
            tag_slug=tag_slug,
            limit=gamma_limit,
            active=gamma_active,
            archived=gamma_archived,
            closed=gamma_closed,
            order=gamma_order,
            ascending=gamma_ascending,
            offset=gamma_offset,
            max_pages=max_pages,
        ):
            stats.scanned += 1
            stats.active += 1  # gamma already filtered
            event_lbl = _event_label(event)
            if sample_size > 0 and len(stats.sample_labels) < sample_size and event_lbl not in seen_samples:
                stats.sample_labels.append(event_lbl)
                seen_samples.add(event_lbl)

            if filter_current_window and window_start_epoch_utc is not None:
                event_start_epoch = (
                    _extract_epoch_suffix(event.get("ticker"))
                    or _extract_epoch_suffix(event.get("slug"))
                    or _parse_iso8601_to_epoch(event.get("startTime"))
                )
                if event_start_epoch != window_start_epoch_utc:
                    continue
                stats.window_matched += 1

            if filter_regex is not None:
                hay = " ".join(
                    str(v)
                    for v in (
                        event.get("ticker"),
                        event.get("slug"),
                        event.get("title"),
                        event.get("description"),
                    )
                    if isinstance(v, str)
                )
                if not filter_regex.search(hay):
                    continue
                stats.regex_matched += 1
                if regex_print_limit <= 0 or len(stats.regex_matches) < regex_print_limit:
                    stats.regex_matches.append(event_lbl)

            markets = event.get("markets") if isinstance(event.get("markets"), list) else []
            for m in markets:
                if not isinstance(m, dict):
                    continue
                condition_id = _extract_gamma_market_condition_id(m)
                token_ids = _extract_gamma_clob_token_ids(m)
                if not condition_id:
                    stats.missing_condition_id += 1
                    if filter_regex is not None and (
                        regex_print_limit <= 0
                        or len(stats.regex_matches_missing_data) < regex_print_limit
                    ):
                        stats.regex_matches_missing_data.append(
                            f"{event_lbl} (missing conditionId)"
                        )
                    continue
                if not token_ids or len(token_ids) < 2:
                    stats.missing_tokens += 1
                    if filter_regex is not None and (
                        regex_print_limit <= 0
                        or len(stats.regex_matches_missing_data) < regex_print_limit
                    ):
                        stats.regex_matches_missing_data.append(
                            f"{event_lbl} (missing clobTokenIds)"
                        )
                    continue

                yes_token_id, no_token_id = token_ids[0], token_ids[1]
                selected.append(
                    SelectedMarket(
                        label=event_lbl,
                        condition_id=condition_id,
                        yes_token_id=yes_token_id,
                        no_token_id=no_token_id,
                    )
                )
                stats.selected += 1
                if limit and len(selected) >= limit:
                    return selected, stats

        return selected, stats

    for market in _iter_pages(client, source=source, max_pages=max_pages):
        stats.scanned += 1
        if not _is_active(market):
            continue
        stats.active += 1

        label = _market_label(market)
        if sample_size > 0 and len(stats.sample_labels) < sample_size and label not in seen_samples:
            stats.sample_labels.append(label)
            seen_samples.add(label)

        if filter_regex is not None:
            text = " ".join(_market_text_fields(market))
            if not filter_regex.search(text):
                continue
            stats.regex_matched += 1
            if regex_print_limit <= 0 or len(stats.regex_matches) < regex_print_limit:
                stats.regex_matches.append(label)

        condition_id = _extract_condition_id(market)
        tokens = _extract_yes_no_tokens(market)
        if not condition_id:
            stats.missing_condition_id += 1
            if filter_regex is not None and (
                regex_print_limit <= 0
                or len(stats.regex_matches_missing_data) < regex_print_limit
            ):
                stats.regex_matches_missing_data.append(f"{label} (missing condition_id)")
            continue
        if not tokens:
            stats.missing_tokens += 1
            if filter_regex is not None and (
                regex_print_limit <= 0
                or len(stats.regex_matches_missing_data) < regex_print_limit
            ):
                stats.regex_matches_missing_data.append(f"{label} (missing YES/NO token ids)")
            continue

        yes_token_id, no_token_id = tokens
        selected.append(
            SelectedMarket(
                label=_market_label(market),
                condition_id=condition_id,
                yes_token_id=yes_token_id,
                no_token_id=no_token_id,
            )
        )
        stats.selected += 1
        if limit and len(selected) >= limit:
            break

    return selected, stats


def write_specs(path: str, specs: List[Dict[str, str]]) -> None:
    out = Path(path)
    tmp = out.with_suffix(out.suffix + ".tmp")
    tmp.write_text(json.dumps(specs, indent=2))
    tmp.replace(out)


def main() -> None:
    # Load `.env` early so argparse defaults can read values like
    # MARKET_UPDATE_INTERVAL_S without requiring the user to export them.
    config = Config.from_env()

    parser = argparse.ArgumentParser(description="Update market_specs.json for LegInBot.")
    parser.add_argument(
        "--once", action="store_true", help="Run one update and exit."
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=int(os.getenv("MARKET_UPDATE_INTERVAL_S", "900")),
        help="Seconds between updates (default 900).",
    )
    parser.add_argument(
        "--output",
        default=os.getenv("MARKET_SPECS_FILE", DEFAULT_OUTPUT),
        help="Path to write specs JSON.",
    )
    parser.add_argument(
        "--filter-regex",
        default=os.getenv("MARKET_FILTER_REGEX", ""),
        help="Regex to match market slug/question.",
    )
    parser.add_argument(
        "--source",
        default=os.getenv("MARKET_SOURCE", "simplified"),
        choices=["gamma_15m", "simplified", "sampling_simplified", "markets"],
        help="Which source to use.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=int(os.getenv("MARKET_MAX_PAGES", "5")),
        help="Max pages to scan (default 5).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=int(os.getenv("MARKET_LIMIT", "10")),
        help="Max specs to write (default 10).",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print scan summary and sample labels (useful to craft regex).",
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=int(os.getenv("MARKET_SAMPLE", "20")),
        help="How many sample labels to print in debug/--once (default 20).",
    )
    parser.add_argument(
        "--regex-print-limit",
        type=int,
        default=int(os.getenv("MARKET_REGEX_PRINT_LIMIT", "25")),
        help="Max regex matches to print (default 25).",
    )
    args = parser.parse_args()

    filter_re = re.compile(args.filter_regex, re.IGNORECASE) if args.filter_regex else None
    client = ClobClient(host=config.host, chain_id=config.chain_id)

    while True:
        selected, stats = build_market_specs(
            client,
            filter_regex=filter_re,
            source=args.source,
            max_pages=args.max_pages,
            limit=args.limit,
            sample_size=args.sample if (args.debug or args.once) else 0,
            regex_print_limit=args.regex_print_limit if args.filter_regex else 0,
        )
        specs = [
            {
                "condition_id": m.condition_id,
                "yes_token_id": m.yes_token_id,
                "no_token_id": m.no_token_id,
            }
            for m in selected
        ]

        if args.filter_regex:
            print(
                f"Regex matched {stats.regex_matched} active entries (showing up to {args.regex_print_limit}):"
            )
            for label in stats.regex_matches:
                print(f"- {label}")
            if stats.regex_matches_missing_data:
                print("Regex matched but could not extract IDs (showing up to limit):")
                for label in stats.regex_matches_missing_data:
                    print(f"- {label}")

        if (args.debug or args.once) and stats.window_start_local and stats.window_end_local:
            print(
                "Current window (ET): "
                f"{stats.window_start_local} -> {stats.window_end_local} "
                f"(start_epoch_utc={stats.window_start_epoch_utc}, window_matched={stats.window_matched})"
            )
            if (
                stats.window_minutes_inferred is not None
                and stats.window_minutes_env is not None
                and stats.window_minutes_inferred != stats.window_minutes_env
            ):
                print(
                    f"NOTE: MARKET_WINDOW_MINUTES={stats.window_minutes_env} pero GAMMA_TAG_SLUG={stats.gamma_tag_slug}. "
                    f"Usando {stats.window_minutes_inferred}m por el tag."
                )
            if (
                stats.gamma_tag_slug
                and stats.window_minutes
                and stats.gamma_tag_slug.upper() == "15M"
                and stats.window_minutes != 15
            ):
                print(
                    f"WARNING: GAMMA_TAG_SLUG={stats.gamma_tag_slug} pero MARKET_WINDOW_MINUTES={stats.window_minutes}. "
                    "Para 15M debería ser 15."
                )

        if specs:
            write_specs(args.output, specs)
            print(f"Wrote {len(specs)} market specs to {args.output}")
            print("Selected markets:")
            for m in selected:
                print(f"- {m.label} (condition_id={m.condition_id})")
        else:
            print("No matching markets found; leaving file unchanged")
            if args.debug or args.once:
                print(
                    f"Scan summary: scanned={stats.scanned} active={stats.active} "
                    f"regex_matched={stats.regex_matched} window_matched={stats.window_matched} selected={stats.selected} "
                    f"missing_condition_id={stats.missing_condition_id} missing_tokens={stats.missing_tokens} "
                    f"source={args.source} max_pages={args.max_pages}"
                )
                if stats.sample_labels:
                    print(f"Sample labels (first {len(stats.sample_labels)}):")
                    for label in stats.sample_labels:
                        print(f"- {label}")

        if args.once:
            break

        # Scheduler:
        # - Always respect `--interval` / MARKET_UPDATE_INTERVAL_S as the maximum
        #   time between updates.
        # - If we're filtering Gamma 15m markets to the "current window", wake
        #   up at the next window boundary (so the file updates immediately when
        #   the ticker epoch changes).
        sleep_s = float(args.interval)
        if args.source == "gamma_15m" and stats.window_end_local:
            grace_s = float(os.getenv("MARKET_WINDOW_GRACE_S", "2"))
            try:
                window_end = _dt.datetime.fromisoformat(stats.window_end_local)
                next_boundary_at = window_end.timestamp() + grace_s
                if sleep_s <= 0:
                    sleep_until = next_boundary_at
                else:
                    sleep_until = min(time.time() + sleep_s, next_boundary_at)
                sleep_s = max(0.0, sleep_until - time.time())
            except Exception:
                sleep_s = max(0.0, sleep_s)
        elif sleep_s < 0:
            sleep_s = 0.0

        if args.debug:
            print(f"Next update in {sleep_s:.1f}s")

        time.sleep(sleep_s)


if __name__ == "__main__":
    main()
