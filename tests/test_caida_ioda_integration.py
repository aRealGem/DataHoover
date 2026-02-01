from pathlib import Path

import duckdb
import pytest

from datahoover.connectors.caida_ioda import fetch_ioda_json, ingest_ioda_events, _build_request
from datahoover.sources import load_sources


@pytest.mark.network
def test_caida_ioda_recent_ingest(tmp_path):
    config_path = Path(__file__).resolve().parents[1] / "sources.toml"
    sources = load_sources(config_path)
    source = sources["caida_ioda_recent"]

    base_url, params = _build_request(source)
    result = fetch_ioda_json(base_url, params=params)
    assert isinstance(result.data, dict)

    data_dir = tmp_path / "data"
    db_path = tmp_path / "hoover.duckdb"
    ingest_ioda_events(
        config_path=config_path,
        source_name="caida_ioda_recent",
        data_dir=data_dir,
        db_path=db_path,
    )

    con = duckdb.connect(str(db_path))
    try:
        row = con.execute(
            """
            SELECT n_new
            FROM ingest_runs
            WHERE source = ?
            ORDER BY ended_at DESC
            LIMIT 1
            """,
            ["caida_ioda_recent"],
        ).fetchone()
    finally:
        con.close()

    assert row is not None
    assert row[0] > 0
