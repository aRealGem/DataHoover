import json
from datetime import datetime, timezone
from pathlib import Path

import feedparser

from datahoover.connectors.caida_ioda import _normalize_events as normalize_ioda
from datahoover.connectors.gdacs_rss import _normalize_entries as normalize_gdacs
from datahoover.connectors.ooni_measurements import _normalize_measurements as normalize_ooni
from datahoover.connectors.usgs_fdsn import _normalize_feature as normalize_usgs_feature
from datahoover.connectors.worldbank_indicator import _normalize_entries as normalize_worldbank
from datahoover.connectors.worldbank_indicator import _ensure_macro_fiscal_view
from datahoover.signals import compute_signals, alert_signals
from datahoover.sources import Source
from datahoover.storage.duckdb_store import (
    init_db,
    log_run,
    upsert_usgs_events,
    upsert_gdacs_alerts,
    upsert_ioda_events,
    upsert_ooni_measurements,
    upsert_worldbank_indicators,
)


FIXTURES = Path(__file__).parent / "fixtures"


def _load_json(name: str):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _seed_db(db_path: Path) -> None:
    init_db(db_path)
    ingested_at = datetime(2026, 1, 30, tzinfo=timezone.utc)

    usgs_source = Source(
        name="usgs_all_day",
        kind="usgs_earthquakes_geojson",
        url="https://example.test/usgs",
    )
    usgs_payload = _load_json("usgs_fdsn_events.json")
    usgs_rows = [normalize_usgs_feature(usgs_source, f, ingested_at) for f in usgs_payload["features"]]
    upsert_usgs_events(db_path, usgs_rows)
    log_run(
        db_path,
        run_id="usgs-run",
        source=usgs_source.name,
        feed_url=usgs_source.url,
        started_at=ingested_at,
        ended_at=ingested_at,
        status="ok",
        n_total=len(usgs_rows),
        n_new=len(usgs_rows),
        message="stored raw=usgs_fdsn_events.json",
    )

    gdacs_source = Source(
        name="gdacs_alerts",
        kind="gdacs_rss",
        url="https://example.test/gdacs",
    )
    gdacs_xml = (FIXTURES / "gdacs_alerts.xml").read_bytes()
    gdacs_feed = feedparser.parse(gdacs_xml)
    gdacs_rows = normalize_gdacs(gdacs_source, gdacs_feed.entries, ingested_at)
    upsert_gdacs_alerts(db_path, gdacs_rows)
    log_run(
        db_path,
        run_id="gdacs-run",
        source=gdacs_source.name,
        feed_url=gdacs_source.url,
        started_at=ingested_at,
        ended_at=ingested_at,
        status="ok",
        n_total=len(gdacs_rows),
        n_new=len(gdacs_rows),
        message="stored raw=gdacs_alerts.xml",
    )

    ioda_source = Source(
        name="caida_ioda_recent",
        kind="caida_ioda",
        url="https://example.test/ioda",
    )
    ioda_payload = _load_json("caida_ioda_events.json")
    ioda_rows = normalize_ioda(ioda_source, ioda_payload["events"], ingested_at, ioda_source.url)
    upsert_ioda_events(db_path, ioda_rows)
    log_run(
        db_path,
        run_id="ioda-run",
        source=ioda_source.name,
        feed_url=ioda_source.url,
        started_at=ingested_at,
        ended_at=ingested_at,
        status="ok",
        n_total=len(ioda_rows),
        n_new=len(ioda_rows),
        message="stored raw=caida_ioda_events.json",
    )

    ooni_source = Source(
        name="ooni_us_recent",
        kind="ooni_measurements",
        url="https://example.test/ooni",
    )
    ooni_payload = _load_json("ooni_measurements.json")
    measurements = ooni_payload["results"]
    # Expand measurements to exceed spike thresholds deterministically.
    for i in range(10):
        measurements.append(
            {
                "measurement_id": f"m-extra-{i}",
                "test_name": "web_connectivity",
                "probe_cc": "US",
                "measurement_start_time": "2026-01-29T12:00:00Z",
                "input": "http://example.com",
                "anomaly": True,
                "confirmed": True,
                "scores": {"blocking_general": 0.9},
            }
        )
    ooni_rows = normalize_ooni(ooni_source, measurements, ingested_at)
    upsert_ooni_measurements(db_path, ooni_rows)
    log_run(
        db_path,
        run_id="ooni-run",
        source=ooni_source.name,
        feed_url=ooni_source.url,
        started_at=ingested_at,
        ended_at=ingested_at,
        status="ok",
        n_total=len(ooni_rows),
        n_new=len(ooni_rows),
        message="stored raw=ooni_measurements.json",
    )

    wb_source = Source(
        name="worldbank_macro_fiscal",
        kind="worldbank_macro_fiscal",
        url="https://example.test/worldbank",
    )
    wb_payload = _load_json("worldbank_macro_fiscal.json")
    wb_rows = normalize_worldbank(wb_source, wb_payload[1], ingested_at)
    upsert_worldbank_indicators(db_path, wb_rows)
    _ensure_macro_fiscal_view(db_path)
    log_run(
        db_path,
        run_id="wb-run",
        source=wb_source.name,
        feed_url=wb_source.url,
        started_at=ingested_at,
        ended_at=ingested_at,
        status="ok",
        n_total=len(wb_rows),
        n_new=len(wb_rows),
        message="stored raw=worldbank_macro_fiscal.json",
    )


def test_compute_signals_from_fixtures(tmp_path):
    db_path = tmp_path / "signals.duckdb"
    _seed_db(db_path)
    computed_at = datetime(2026, 2, 1, tzinfo=timezone.utc)
    inserted = compute_signals(
        db_path=str(db_path),
        since="10000d",
        min_magnitude=5.0,
        gdacs_min_severity=0.6,
        computed_at=computed_at,
    )
    assert inserted > 0

    import duckdb

    con = duckdb.connect(str(db_path))
    try:
        counts = dict(
            con.execute(
                "SELECT signal_type, COUNT(*) FROM signals GROUP BY signal_type"
            ).fetchall()
        )
        assert counts["earthquake"] >= 1
        assert counts["alert"] >= 1
        assert counts["internet_outage"] >= 1
        assert counts["censorship_spike"] >= 1
        assert counts["fiscal_stress"] >= 1

        row = con.execute(
            """
            SELECT severity_score
            FROM signals
            WHERE signal_type = 'earthquake' AND summary LIKE 'M5.1%'
            LIMIT 1
            """
        ).fetchone()
    finally:
        con.close()

    assert row is not None
    assert abs(row[0] - 0.025) < 0.0001


def test_alert_signals_output(tmp_path, capsys):
    db_path = tmp_path / "signals.duckdb"
    _seed_db(db_path)
    compute_signals(db_path=str(db_path), since="10000d", computed_at=datetime(2026, 2, 1, tzinfo=timezone.utc))
    alert_signals(db_path=str(db_path), since="10000d", limit=1)
    output = capsys.readouterr().out
    assert "earthquake" in output
    assert "raw=" in output
