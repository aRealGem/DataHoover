# Twelve Data Financial Market Connector

## Overview

The Twelve Data connector fetches time series financial market data (stocks, ETFs, forex, crypto) using the [Twelve Data API](https://twelvedata.com/).

## Setup

### 1. Get API Key

Sign up for a free API key at https://twelvedata.com/

Free tier includes:
- 800 API credits per day
- 1 API request per second
- Access to all core endpoints

### 2. Set Environment Variable

```bash
export TWELVEDATA_API_KEY='your_api_key_here'
```

Add this to your `~/.bashrc` or `~/.zshrc` to persist.

### 3. Configure Sources

Edit `sources.toml` to customize the symbols you want to track:

```toml
[[sources]]
name = "twelvedata_watchlist_daily"
kind = "twelvedata_time_series"
description = "Twelve Data daily time series for watchlist symbols."

[sources.twelvedata_watchlist_daily]
symbols = ["SPY", "QQQ", "IWM", "DIA", "TLT", "EUR/USD", "BTC/USD"]
interval = "1day"
outputsize = 30
```

**Configuration options:**
- `symbols`: List of tickers/symbols (stocks, ETFs, forex pairs, crypto)
- `interval`: Time interval (e.g., `1day`, `1h`, `15min`, `1week`)
- `outputsize`: Number of data points to fetch (default 30)

## Usage

### Ingest Market Data

```bash
# Using PYTHONPATH
PYTHONPATH=src python -m datahoover.cli ingest-twelvedata --source twelvedata_watchlist_daily

# Or with the script
bash run-ingest.sh ingest-twelvedata --source twelvedata_watchlist_daily
```

Output example:
```
[twelvedata_watchlist_daily] fetched=210 inserted_or_updated=210 raw=2026-02-01T12-34-56Z.json
```

### Compute Market Move Signals

After ingesting data, compute signals for significant market moves:

```bash
bash run-compute-signals.sh --since 7d
```

This will create `market_move` signals for:
- Daily returns > 2% (in either direction)
- Severity scaled to 0-1 (5% move = 0.5, 10% move = 1.0)
- Tracked per symbol with direction (gain/loss)

### View Alerts

```bash
bash run-alert.sh --since 7d --limit 10
```

Example output:
```
== market_move (3 signals)
  0.85 | BTC/USD 8.50% gain
  0.72 | SPY 7.20% loss
  0.45 | TLT 4.50% gain
```

## Data Schema

Data is stored in the `twelvedata_time_series` table:

```sql
CREATE TABLE twelvedata_time_series (
  source       VARCHAR,      -- Source name from config
  symbol       VARCHAR,      -- Ticker symbol
  interval     VARCHAR,      -- Time interval (1day, etc.)
  ts           TIMESTAMP,    -- Timestamp of the data point
  open         DOUBLE,       -- Opening price
  high         DOUBLE,       -- High price
  low          DOUBLE,       -- Low price
  close        DOUBLE,       -- Closing price
  volume       BIGINT,       -- Trading volume (nullable)
  currency     VARCHAR,      -- Currency (USD, etc.)
  exchange     VARCHAR,      -- Exchange name
  ingested_at  TIMESTAMP,    -- When data was ingested
  raw_path     VARCHAR       -- Path to raw JSON snapshot
);
```

**Indexes:**
- `(source, symbol, interval, ts)` - Primary upsert key
- `(source, symbol, ts DESC)` - Query performance

## API Credits

Each symbol in a request consumes **1 credit**. With the free tier's 800 credits/day:
- 7 symbols × 1 request/day = 7 credits/day
- Can run ~114 times per day with default config
- For hourly updates: reduce `symbols` or upgrade plan

## Supported Symbols

**Stocks & ETFs:** `AAPL`, `TSLA`, `SPY`, `QQQ`, `VTI`
**Forex:** `EUR/USD`, `GBP/USD`, `USD/JPY` (use slash format)
**Crypto:** `BTC/USD`, `ETH/USD`, `BTC/EUR`
**Commodities:** `GOLD`, `SILVER`, `OIL`

Search available symbols at: https://twelvedata.com/symbol-search

## Troubleshooting

### Error: "TWELVEDATA_API_KEY environment variable is required"

Make sure you've exported the API key:
```bash
export TWELVEDATA_API_KEY='your_key'
```

### Error: "Twelve Data API error for {symbol}: Invalid symbol"

The symbol doesn't exist or isn't supported. Check the symbol search tool.

### Error: "Rate limit exceeded"

Free tier limits: 1 request/second, 800/day. Wait or upgrade your plan.

### No data returned

Some symbols have limited historical data. Try:
- Different symbols
- Smaller `outputsize`
- Different `interval` (e.g., `1week` instead of `1day`)

## Testing

Run offline tests:
```bash
bash run-all-no-network.sh
```

Run integration test (requires API key):
```bash
TWELVEDATA_API_KEY='your_key' python3 -m pytest tests/test_twelvedata_parse.py::test_twelvedata_integration_live -v
```

## Files

- **Connector:** `src/datahoover/connectors/twelvedata_time_series.py`
- **Schema:** `src/datahoover/storage/duckdb_store.py` (lines for table + upsert)
- **Signals:** `src/datahoover/signals.py` (`_market_move_signals`)
- **Tests:** `tests/test_twelvedata_parse.py`
- **Fixtures:** `tests/fixtures/twelvedata_time_series_*.json`

## Next Steps

- **Add more symbols** to `sources.toml`
- **Adjust outputsize** to control lookback window
- **Create additional signals** for volatility, correlations, etc.
- **Set up cron job** for automatic daily updates
