from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
import time
from pathlib import Path
from typing import Any, Dict, Optional

from aiohttp import web

# Allow running as a script: `python src/dashboard.py`
if __package__ in (None, ""):
    import sys as _sys

    _ROOT = Path(__file__).resolve().parents[1]
    if str(_ROOT) not in _sys.path:
        _sys.path.insert(0, str(_ROOT))

from src.config import Config
from src.leg_in_bot import LegInBot
from src.telemetry import Telemetry


INDEX_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Polymarket Bot Dashboard</title>
  <style>
    :root { color-scheme: dark; }
    body { margin: 0; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, "Noto Sans", "Liberation Sans", sans-serif; background: #0b1020; color: #e6e8ef; }
    header { padding: 14px 16px; border-bottom: 1px solid rgba(255,255,255,0.08); display:flex; gap:12px; align-items:center; justify-content:space-between; position: sticky; top:0; background:#0b1020; z-index:10; }
    .title { font-weight: 700; letter-spacing: 0.2px; }
    .status { font-size: 12px; opacity: 0.9; }
    .wrap { padding: 16px; display:grid; gap: 16px; grid-template-columns: 1fr; }
    .grid2 { display:grid; gap: 16px; grid-template-columns: 1fr; }
    @media (min-width: 1100px) { .grid2 { grid-template-columns: 1.2fr 0.8fr; } }
    .card { border: 1px solid rgba(255,255,255,0.08); border-radius: 12px; background: rgba(255,255,255,0.03); }
    .card h2 { margin: 0; padding: 12px 14px; border-bottom: 1px solid rgba(255,255,255,0.08); font-size: 13px; text-transform: uppercase; letter-spacing: 0.08em; opacity: 0.9; }
    .card .content { padding: 12px 14px; }
    .kpis { display:flex; gap: 16px; flex-wrap: wrap; }
    .kpi { padding: 10px 12px; border-radius: 10px; border: 1px solid rgba(255,255,255,0.08); background: rgba(0,0,0,0.18); }
    .kpi .label { font-size: 11px; opacity: 0.75; text-transform: uppercase; letter-spacing: 0.06em; }
    .kpi .value { font-size: 18px; margin-top: 4px; font-variant-numeric: tabular-nums; }
    table { width: 100%; border-collapse: collapse; font-variant-numeric: tabular-nums; }
    th, td { padding: 8px 8px; border-bottom: 1px solid rgba(255,255,255,0.08); text-align: left; font-size: 12px; }
    th { opacity: 0.75; font-weight: 600; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace; }
    .pill { display:inline-block; padding: 2px 8px; border-radius: 999px; background: rgba(255,255,255,0.08); border: 1px solid rgba(255,255,255,0.10); font-size: 11px; }
    .pos { color: #74f7c6; }
    .neg { color: #ff6b81; }
    .log { height: 340px; overflow: auto; padding: 10px 12px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace; font-size: 11px; line-height: 1.35; }
    .log .line { padding: 2px 0; border-bottom: 1px dashed rgba(255,255,255,0.06); }
    .muted { opacity: 0.75; }
  </style>
</head>
<body>
  <header>
    <div class="title">Polymarket Bot Dashboard</div>
    <div class="status">Status: <span id="conn" class="pill">connecting</span></div>
  </header>

  <div class="wrap">
    <div class="card">
      <h2>Summary</h2>
      <div class="content">
        <div class="kpis">
          <div class="kpi">
            <div class="label">Wallet Balance (USDC)</div>
            <div id="walletBal" class="value" style="color:#ffd36a;">0.0000</div>
          </div>
          <div class="kpi">
            <div class="label">Daily PnL (USDC)</div>
            <div id="dailyPnl" class="value">0.0000</div>
          </div>
          <div class="kpi">
            <div class="label">Open Positions</div>
            <div id="openPositions" class="value">0</div>
          </div>
          <div class="kpi">
            <div class="label">Markets Seen</div>
            <div id="marketsSeen" class="value">0</div>
          </div>
          <div class="kpi">
            <div class="label">Last Snapshot</div>
            <div id="lastSnap" class="value">—</div>
          </div>
        </div>
      </div>
    </div>

    <div class="grid2">
      <div class="card">
        <h2>Markets</h2>
        <div class="content">
          <table>
            <thead>
              <tr>
                <th>Condition</th>
                <th>YES bid/ask</th>
                <th>NO bid/ask</th>
                <th>sum ask</th>
              </tr>
            </thead>
            <tbody id="marketsTbody"></tbody>
          </table>
        </div>
      </div>

      <div class="card">
        <h2>Positions</h2>
        <div class="content">
          <table>
            <thead>
              <tr>
                <th>Condition</th>
                <th>State</th>
                <th>Side</th>
                <th>Entry</th>
                <th>Size</th>
                <th>Exit cost</th>
                <th>Mark PnL</th>
              </tr>
            </thead>
            <tbody id="posTbody"></tbody>
          </table>
        </div>
      </div>
    </div>

    <div class="card">
      <h2>Events</h2>
      <div class="log" id="log"></div>
    </div>
  </div>

  <script>
    const elConn = document.getElementById('conn');
    const elDaily = document.getElementById('dailyPnl');
    const elWallet = document.getElementById('walletBal');
    const elOpen = document.getElementById('openPositions');
    const elMarketsSeen = document.getElementById('marketsSeen');
    const elLastSnap = document.getElementById('lastSnap');
    const marketsTbody = document.getElementById('marketsTbody');
    const posTbody = document.getElementById('posTbody');
    const logEl = document.getElementById('log');

    function shortId(s) {
      if (!s) return '';
      if (s.length <= 14) return s;
      return s.slice(0, 8) + '…' + s.slice(-6);
    }

    function fmt(n, digits=4) {
      if (n === null || n === undefined || Number.isNaN(n)) return '—';
      return Number(n).toFixed(digits);
    }

    function fmtInt(n) {
      if (n === null || n === undefined || Number.isNaN(n)) return '—';
      return String(Math.trunc(Number(n)));
    }

    function fmtTs(ts) {
      if (!ts) return '—';
      const d = new Date(ts * 1000);
      return d.toLocaleTimeString();
    }

    function setConn(state) {
      elConn.textContent = state;
      elConn.style.background = state === 'connected' ? 'rgba(116,247,198,0.12)' : 'rgba(255,255,255,0.08)';
      elConn.style.borderColor = state === 'connected' ? 'rgba(116,247,198,0.35)' : 'rgba(255,255,255,0.10)';
    }

    function appendEvent(ev) {
      const line = document.createElement('div');
      line.className = 'line';
      const ts = fmtTs(ev.ts);
      const t = ev.type || 'event';
      let msg = '';
      try { msg = JSON.stringify(ev.data || {}); } catch { msg = String(ev.data || ''); }
      line.textContent = `[${ts}] ${t} ${msg}`;
      logEl.prepend(line);
      while (logEl.childNodes.length > 400) logEl.removeChild(logEl.lastChild);
    }

    function renderMarkets(markets) {
      marketsTbody.innerHTML = '';
      for (const m of markets || []) {
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td class="mono">${shortId(m.condition_id)}<div class="muted">${m.label ? String(m.label).slice(0, 42) : ''}</div></td>
          <td>${fmt(m.yes?.best_bid)} / ${fmt(m.yes?.best_ask)}</td>
          <td>${fmt(m.no?.best_bid)} / ${fmt(m.no?.best_ask)}</td>
          <td>${fmt(m.sum_ask)}<div class="muted">strike: ${fmt(m.strike_price, 2)}</div></td>
        `;
        marketsTbody.appendChild(tr);
      }
    }

    function renderPositions(positions) {
      posTbody.innerHTML = '';
      for (const p of positions || []) {
        const pnl = p.mark_pnl;
        const pnlClass = pnl === null || pnl === undefined ? '' : (pnl >= 0 ? 'pos' : 'neg');
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td class="mono">${shortId(p.condition_id)}</td>
          <td><span class="pill">${p.state || ''}</span></td>
          <td>${p.side || ''}</td>
          <td>${fmt(p.entry_price)}</td>
          <td>${fmt(p.size, 4)}</td>
          <td>${fmt(p.exit_cost)}</td>
          <td class="${pnlClass}">${fmt(pnl)}</td>
        `;
        posTbody.appendChild(tr);
      }
    }

    function renderSnapshot(s) {
      if (s.paper_balance !== undefined) {
        elWallet.textContent = fmt(s.paper_balance);
      }
      elDaily.textContent = fmt(s.daily_pnl_usdc);
      elDaily.className = 'value ' + ((s.daily_pnl_usdc || 0) >= 0 ? 'pos' : 'neg');
      elOpen.textContent = fmtInt(s.open_positions);
      elMarketsSeen.textContent = fmtInt(s.markets_seen);
      elLastSnap.textContent = fmtTs(s.now);
      renderMarkets(s.markets);
      renderPositions(s.positions);
    }

    async function boot() {
      try {
        const resp = await fetch('/api/state');
        const state = await resp.json();
        renderSnapshot(state.snapshot);
        for (const ev of (state.events || [])) appendEvent(ev);
      } catch (e) {
        console.warn('state fetch failed', e);
      }

      const es = new EventSource('/events');
      es.addEventListener('open', () => setConn('connected'));
      es.onerror = () => setConn('error');
      es.addEventListener('event', (e) => appendEvent(JSON.parse(e.data)));
      es.addEventListener('snapshot', (e) => renderSnapshot(JSON.parse(e.data)));
    }
    boot();
  </script>
</body>
</html>
"""


def _serialize_position(telemetry: Telemetry, pos) -> Dict[str, Any]:
    cid = pos.condition_id
    market = telemetry.get_market(cid) or {}
    side = str(pos.leg_1_side).upper()
    my = market.get("yes") if side == "YES" else market.get("no")
    opp = market.get("no") if side == "YES" else market.get("yes")
    opp_ask = float(opp.get("best_ask")) if isinstance(opp, dict) and opp.get("best_ask") else None
    my_bid = float(my.get("best_bid")) if isinstance(my, dict) and my.get("best_bid") else None
    exit_cost = (float(pos.leg_1_entry_price) + float(opp_ask)) if opp_ask is not None else None
    mark_pnl = (
        (float(my_bid) - float(pos.leg_1_entry_price)) * float(pos.leg_1_size)
        if my_bid is not None
        else None
    )
    return {
        "condition_id": cid,
        "state": pos.state.value,
        "side": side,
        "entry_price": float(pos.leg_1_entry_price),
        "size": float(pos.leg_1_size),
        "scalein_count": int(getattr(pos, "scalein_count", 0) or 0),
        "exit_cost": exit_cost,
        "mark_pnl": mark_pnl,
    }


def _build_snapshot(bot: LegInBot, telemetry: Telemetry) -> Dict[str, Any]:
    markets = telemetry.get_markets()
    positions = [_serialize_position(telemetry, p) for p in bot.positions.values()]
    positions.sort(key=lambda p: (p.get("condition_id") or ""))
    return {
        "now": time.time(),
        "daily_pnl_usdc": float(getattr(bot, "_daily_pnl_usdc", 0.0) or 0.0),
        "paper_balance": float(getattr(bot, "_paper_balance", 0.0) or 0.0),
        "open_positions": int(len(bot.positions)),
        "markets_seen": int(len(markets)),
        "positions": positions,
        "markets": markets,
    }


async def _send_sse(resp: web.StreamResponse, *, event: str, data: Any) -> None:
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    msg = f"event: {event}\ndata: {payload}\n\n"
    await resp.write(msg.encode("utf-8"))


async def index(_request: web.Request) -> web.Response:
    return web.Response(text=INDEX_HTML, content_type="text/html")


async def api_state(request: web.Request) -> web.Response:
    bot: LegInBot = request.app["bot"]
    telemetry: Telemetry = request.app["telemetry"]
    snapshot = _build_snapshot(bot, telemetry)
    events = telemetry.get_events(limit=100)
    return web.json_response({"snapshot": snapshot, "events": events})


async def events_sse(request: web.Request) -> web.StreamResponse:
    bot: LegInBot = request.app["bot"]
    telemetry: Telemetry = request.app["telemetry"]
    snapshot_interval_s: float = float(request.app["snapshot_interval_s"])

    resp = web.StreamResponse(
        status=200,
        reason="OK",
        headers={
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
        },
    )
    await resp.prepare(request)

    q = telemetry.subscribe()
    try:
        await _send_sse(resp, event="snapshot", data=_build_snapshot(bot, telemetry))
        for ev in telemetry.get_events(limit=50):
            await _send_sse(resp, event="event", data=ev)

        while True:
            try:
                ev = await asyncio.wait_for(q.get(), timeout=snapshot_interval_s)
                await _send_sse(resp, event="event", data={"id": ev.id, "ts": ev.ts, "type": ev.type, "data": ev.data})
            except asyncio.TimeoutError:
                await _send_sse(resp, event="snapshot", data=_build_snapshot(bot, telemetry))
            except (asyncio.CancelledError, ConnectionResetError, BrokenPipeError):
                break
    finally:
        telemetry.unsubscribe(q)
        with contextlib.suppress(Exception):
            await resp.write_eof()

    return resp


async def main() -> None:
    host = os.getenv("DASHBOARD_HOST", "127.0.0.1")
    port = int(os.getenv("DASHBOARD_PORT", "8000"))
    snapshot_interval_s = float(os.getenv("DASHBOARD_SNAPSHOT_INTERVAL_S", "1.0"))

    config = Config.from_env()
    telemetry = Telemetry(max_events=int(os.getenv("DASHBOARD_MAX_EVENTS", "500")))
    bot = LegInBot(config, telemetry=telemetry)

    app = web.Application()
    app["telemetry"] = telemetry
    app["bot"] = bot
    app["snapshot_interval_s"] = snapshot_interval_s

    app.router.add_get("/", index)
    app.router.add_get("/api/state", api_state)
    app.router.add_get("/events", events_sse)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=host, port=port)
    await site.start()
    print(f"Dashboard running on http://{host}:{port}")

    stop = asyncio.Event()

    def _stop() -> None:
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (getattr(signal, "SIGINT", None), getattr(signal, "SIGTERM", None)):
        if sig is None:
            continue
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _stop)

    bot_task = asyncio.create_task(bot.run())
    try:
        await stop.wait()
    finally:
        bot_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await bot_task
        await runner.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
