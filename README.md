Polymarket leg‑in bot (MVP)

**Qué hay aquí**
- `src/leg_in_bot.py`: orquestador del bot.
- `src/market_monitor.py`: monitor WS CLOB con best_bid/best_ask.
- `update_market_specs.py`: actualiza `market_specs.json` periódicamente.

**1) Instalación**
```bash
python -m venv venv
venv/bin/pip install -r requirements.txt
```

**2) Configuración**
Edita `.env` (mira `.env.example`). Imprescindible:
- `USE_WEBSOCKET=true`
- `MARKET_SPECS_FILE=market_specs.json`
- `MARKET_REFRESH_INTERVAL_S=30` (o similar)
- `MARKET_SOURCE=gamma_15m` (para mercados 15m por Gamma API)
- `MARKET_FILTER_REGEX` si quieres filtrar, ej. `(?i)xrp-updown-15m-\\d+`.
- `MARKET_FILTER_CURRENT_WINDOW=true` para quedarte solo con la ventana 15m actual (ET).
- Para evitar loops de re‑entry: `MAX_ENTRIES_PER_CONDITION`, `REENTRY_COOLDOWN_S`, `ENTRY_SIGNAL_MIN_INTERVAL_MS`.
- Para filtrar entradas “muertas”: `MAX_OPPOSITE_ASK_FOR_ENTRY` (o alias `NO_ENTRY_IF_EITHER_ASK_ABOVE`), y opcional `MAX_ENTRY_EXIT_COST`.
- Filtros conservadores extra: `MIN_OPPOSITE_ASK`, `MIN_BOOK_DEPTH_USDC`, `MIN_SPREAD_BASIS_POINTS` (requiere book snapshots del WS).
- Circuit breaker: `MAX_DAILY_LOSS` (bloquea nuevas entradas hasta el próximo día UTC).
- `MAX_POSITION_SIZE` se interpreta como presupuesto USDC por patita (`size = budget / price`).
- `POLYMARKET_WS_HEADERS` / `POLYMARKET_WS_COOKIES` se pasan como JSON (mira `.env.example`).

**3) Auto‑actualizar markets**
Este script consulta Gamma API (tag `15M`) y escribe `market_specs.json` cada 15m (por defecto).
```bash
venv/bin/python update_market_specs.py
```
Opciones útiles:
- `--once` actualiza una vez y sale.
- `--interval 900` cambia el periodo.
- `--source gamma_15m` fuerza Gamma (si no usas `.env`).
- `--filter-regex "(?i)xrp-updown-15m-\\d+"` override rápido.

**4) Ejecutar el bot**
```bash
venv/bin/python src/leg_in_bot.py
```

Notas:
- El subscribe al WS se genera automáticamente a partir de `market_specs.json`.
- `merge_tokens()` para live trading sigue pendiente (stub).
