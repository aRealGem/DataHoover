"""Tests for the Twelve Data connector's min_interval_seconds throttle."""
from __future__ import annotations

from pathlib import Path

import pytest


def test_throttler_no_op_when_min_interval_is_zero(monkeypatch):
    from datahoover.connectors import twelvedata_time_series as module

    sleeps: list[float] = []
    monkeypatch.setattr(module.time, "sleep", lambda s: sleeps.append(s))

    t = module._Throttler(min_interval_seconds=0.0)
    t.wait()
    t.wait()
    t.wait()

    assert sleeps == [], "Default zero interval must never call sleep"


def test_throttler_sleeps_with_positive_interval(monkeypatch):
    from datahoover.connectors import twelvedata_time_series as module

    fake_now = {"t": 1000.0}

    def fake_monotonic() -> float:
        return fake_now["t"]

    sleeps: list[float] = []

    def fake_sleep(s: float) -> None:
        sleeps.append(s)
        fake_now["t"] += s

    monkeypatch.setattr(module.time, "monotonic", fake_monotonic)
    monkeypatch.setattr(module.time, "sleep", fake_sleep)

    t = module._Throttler(min_interval_seconds=2.0)

    t.wait()
    fake_now["t"] += 0.1
    t.wait()
    fake_now["t"] += 0.1
    t.wait()

    assert len(sleeps) == 2, f"Expected 2 sleep calls, got {sleeps}"
    for s in sleeps:
        assert s >= 1.5, f"Each throttled wait should sleep >=1.5s, got {s}"


def test_ingest_paces_back_to_back_calls(monkeypatch, tmp_path: Path):
    """Two configured symbols with min_interval_seconds=2 must produce a sleep
    of >=1.5s between the two HTTP fetches."""
    from datahoover.connectors import twelvedata_time_series as module

    payload = {
        "meta": {"symbol": "SPY", "interval": "1day", "currency": "USD", "exchange_name": "NYSE ARCA"},
        "values": [
            {"datetime": "2026-04-28", "open": "500", "high": "510", "low": "499", "close": "505", "volume": "1000"},
        ],
        "status": "ok",
    }

    fake_now = {"t": 1000.0}

    def fake_monotonic() -> float:
        return fake_now["t"]

    sleeps: list[float] = []

    def fake_sleep(s: float) -> None:
        sleeps.append(s)
        fake_now["t"] += s

    def fake_fetch_time_series(symbol: str, *, api_key: str, interval: str, outputsize: int, timeout_s: float = 30.0):
        # Each fetch advances the clock a tiny bit (e.g. 0.05s of network).
        fake_now["t"] += 0.05
        return module.FetchResult(status_code=200, symbol=symbol, data=payload, raw_bytes=b"{}")

    monkeypatch.setattr(module.time, "monotonic", fake_monotonic)
    monkeypatch.setattr(module.time, "sleep", fake_sleep)
    monkeypatch.setattr(module, "fetch_time_series", fake_fetch_time_series)
    monkeypatch.setattr(module, "fetch_with_retry", lambda fn: fn())
    monkeypatch.setenv("TWELVEDATA_API_KEY", "test-key")

    config = tmp_path / "sources.toml"
    config.write_text(
        """[[sources]]
name = "test_twelvedata"
kind = "twelvedata_time_series"
description = "test"
symbols = ["SPY", "QQQ"]
interval = "1day"
outputsize = 5
min_interval_seconds = 2.0
""",
        encoding="utf-8",
    )

    data_dir = tmp_path / "data"
    db_path = tmp_path / "warehouse.duckdb"

    module.ingest_twelvedata_time_series(
        config_path=config,
        source_name="test_twelvedata",
        data_dir=data_dir,
        db_path=db_path,
    )

    paced_sleeps = [s for s in sleeps if s > 0.0]
    assert paced_sleeps, f"Expected at least one paced sleep, got sleeps={sleeps}"
    assert max(paced_sleeps) >= 1.5, f"Largest sleep should be near min_interval=2.0, got {paced_sleeps}"
