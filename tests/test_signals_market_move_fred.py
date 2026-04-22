"""Tests for FRED integration into _market_move_signals with crypto-only dedupe."""
from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import duckdb

from datahoover.signals import _market_move_signals
from datahoover.storage.duckdb_store import (
    init_db,
    upsert_fred_series_observations,
    upsert_twelvedata_time_series,
)


INGESTED_AT = datetime(2026, 2, 1, tzinfo=timezone.utc)
COMPUTED_AT = datetime(2026, 2, 1, tzinfo=timezone.utc)
CUTOFF = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _ts(year: int, month: int, day: int, hour: int = 21) -> datetime:
    """Naive UTC timestamp (matches how TD connector stores close times in DuckDB TIMESTAMP)."""
    return datetime(year, month, day, hour, 0, 0)


def _td_row(symbol: str, ts: datetime, close: float, source: str = "twelvedata_watchlist_daily") -> dict:
    return {
        "source": source,
        "symbol": symbol,
        "interval": "1day",
        "series_group": "primary",
        "ts": ts,
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": 0,
        "currency": "USD",
        "exchange": "",
        "ingested_at": INGESTED_AT,
        "raw_path": f"raw/{symbol.replace('/', '_')}.json",
    }


def _fred_row(series_id: str, observation_date: date, value: float, source: str = "fred_crypto_fx") -> dict:
    return {
        "source": source,
        "series_id": series_id,
        "observation_date": observation_date,
        "value": value,
        "realtime_start": None,
        "realtime_end": None,
        "units": "USD",
        "ingested_at": INGESTED_AT,
        "raw_path": f"raw/fred_{series_id}.json",
    }


def _seed_td(db_path: Path, *, symbol: str, day1: float, day2: float, ts1=None, ts2=None) -> None:
    ts1 = ts1 or _ts(2026, 1, 30)
    ts2 = ts2 or _ts(2026, 1, 31)
    upsert_twelvedata_time_series(
        db_path, [_td_row(symbol, ts1, day1), _td_row(symbol, ts2, day2)]
    )


def _seed_fred(db_path: Path, *, series_id: str, day1: float, day2: float, d1=None, d2=None, source: str = "fred_crypto_fx") -> None:
    d1 = d1 or date(2026, 1, 30)
    d2 = d2 or date(2026, 1, 31)
    upsert_fred_series_observations(
        db_path,
        [
            _fred_row(series_id, d1, day1, source=source),
            _fred_row(series_id, d2, day2, source=source),
        ],
    )


def _run(db_path: Path):
    con = duckdb.connect(str(db_path))
    try:
        return _market_move_signals(con, cutoff=CUTOFF, computed_at=COMPUTED_AT)
    finally:
        con.close()


def test_crypto_collapses_to_single_td_winning_signal(tmp_path):
    db = tmp_path / "x.duckdb"
    init_db(db)
    _seed_td(db, symbol="BTC/USD", day1=40000.0, day2=44000.0)  # +10.00%
    _seed_fred(db, series_id="CBBTCUSD", day1=40000.0, day2=44200.0)  # +10.50%

    signals = _run(db)

    btc_signals = [s for s in signals if s["entity_id"] == "BTC/USD"]
    assert len(btc_signals) == 1
    assert btc_signals[0]["source"] == "twelvedata_watchlist_daily"
    assert btc_signals[0]["entity_type"] == "symbol"
    assert abs(btc_signals[0]["severity_score"] - 1.0) < 1e-9


def test_index_vs_etf_stays_as_two_signals(tmp_path):
    db = tmp_path / "x.duckdb"
    init_db(db)
    _seed_td(db, symbol="SPY", day1=500.0, day2=515.0)  # +3.00%
    _seed_fred(db, series_id="SP500", day1=5000.0, day2=5150.0, source="fred_macro_watchlist")  # +3.00%

    signals = _run(db)

    entity_ids = sorted(s["entity_id"] for s in signals)
    assert entity_ids == ["SP500", "SPY"]
    spy_sources = {s["source"] for s in signals if s["entity_id"] == "SPY"}
    sp500_sources = {s["source"] for s in signals if s["entity_id"] == "SP500"}
    assert spy_sources == {"twelvedata_watchlist_daily"}
    assert sp500_sources == {"fred_macro_watchlist"}


def test_empty_fred_table_matches_td_only_behavior(tmp_path):
    db = tmp_path / "x.duckdb"
    init_db(db)
    _seed_td(db, symbol="QQQ", day1=400.0, day2=410.0)  # +2.50%

    signals = _run(db)

    assert len(signals) == 1
    assert signals[0]["entity_id"] == "QQQ"
    assert signals[0]["source"] == "twelvedata_watchlist_daily"


def test_fred_only_crypto_emits_one_fred_signal(tmp_path):
    db = tmp_path / "x.duckdb"
    init_db(db)
    _seed_fred(db, series_id="CBETHUSD", day1=3000.0, day2=3150.0)  # +5.00%

    signals = _run(db)

    assert len(signals) == 1
    s = signals[0]
    assert s["entity_id"] == "ETH/USD"
    assert s["source"] == "fred_crypto_fx"


def test_cutoff_excludes_old_observations(tmp_path):
    db = tmp_path / "x.duckdb"
    init_db(db)
    _seed_td(
        db,
        symbol="BTC/USD",
        day1=40000.0,
        day2=44000.0,
        ts1=_ts(2025, 1, 1),
        ts2=_ts(2025, 1, 2),
    )
    _seed_fred(
        db,
        series_id="CBBTCUSD",
        day1=40000.0,
        day2=44200.0,
        d1=date(2025, 1, 1),
        d2=date(2025, 1, 2),
    )

    signals = _run(db)

    assert signals == []


def test_different_days_do_not_collapse(tmp_path):
    db = tmp_path / "x.duckdb"
    init_db(db)
    _seed_td(
        db,
        symbol="BTC/USD",
        day1=40000.0,
        day2=44000.0,
        ts1=_ts(2026, 1, 30),
        ts2=_ts(2026, 1, 31),
    )
    _seed_fred(
        db,
        series_id="CBBTCUSD",
        day1=40000.0,
        day2=44200.0,
        d1=date(2026, 1, 29),
        d2=date(2026, 1, 30),
    )

    signals = _run(db)

    btc_signals = [s for s in signals if s["entity_id"] == "BTC/USD"]
    assert len(btc_signals) == 2
    sources = {s["source"] for s in btc_signals}
    assert sources == {"twelvedata_watchlist_daily", "fred_crypto_fx"}
