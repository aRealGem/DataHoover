from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .connectors.usgs_earthquakes import ingest_usgs_geojson
from .connectors.usgs_fdsn import ingest_usgs_fdsn_events
from .connectors.eurostat_stats import ingest_eurostat_stats
from .connectors.openfema_disaster_declarations import ingest_openfema_disaster_declarations
from .connectors.nws_alerts import ingest_nws_alerts
from .connectors.gdacs_rss import ingest_gdacs_rss
from .connectors.worldbank_indicator import ingest_worldbank_indicator
from .connectors.ckan_catalog import ingest_ckan_catalog
from .connectors.socrata_soda import ingest_socrata_soda
from .connectors.opendatasoft_explore import ingest_opendatasoft_explore
from .connectors.gdelt_doc_query import ingest_gdelt_doc_query
from .connectors.ooni_measurements import ingest_ooni_measurements
from .connectors.caida_ioda import ingest_ioda_events
from .connectors.ripe_ris_live import ingest_ripe_ris_live
from .connectors.ripe_atlas_probes import ingest_ripe_atlas_probes
from .connectors.twelvedata_time_series import ingest_twelvedata_time_series
from .connectors.fred_series import ingest_fred_series
from .connectors.eia_v2 import ingest_eia_v2
from .connectors.bls_timeseries import ingest_bls_timeseries
from .connectors.census_acs import ingest_census_acs
from .connectors.alternative_me_fng import ingest_alternative_me_fng
from .connectors.cnn_fear_greed import ingest_cnn_fear_greed
from .storage.duckdb_store import show_latest
from .signals import compute_signals, alert_signals
from .snapshot import snapshot_zip, snapshot_parquet, default_snapshot_stamp
from .publish import main as publish_main


DEFAULT_CONFIG = Path("sources.toml")
DEFAULT_DATA_DIR = Path("data")
DEFAULT_DB = DEFAULT_DATA_DIR / "warehouse.duckdb"


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="hoover", description="Data Hoover CLI")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_ingest = sub.add_parser("ingest-usgs", help="Ingest USGS earthquakes GeoJSON feed into DuckDB")
    p_ingest.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Path to sources.toml")
    p_ingest.add_argument("--source", type=str, default="usgs_all_day", help="Source name from sources.toml")
    p_ingest.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="Data directory (raw/state/db)")
    p_ingest.add_argument("--db", type=Path, default=DEFAULT_DB, help="DuckDB database path")

    p_fdsn = sub.add_parser("ingest-usgs-fdsn", help="Ingest USGS FDSN event service into DuckDB")
    p_fdsn.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Path to sources.toml")
    p_fdsn.add_argument("--source", type=str, default="usgs_catalog_m45_day", help="Source name from sources.toml")
    p_fdsn.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="Data directory (raw/state/db)")
    p_fdsn.add_argument("--db", type=Path, default=DEFAULT_DB, help="DuckDB database path")

    p_euro = sub.add_parser("ingest-eurostat", help="Ingest Eurostat Statistics API JSON into DuckDB")
    p_euro.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Path to sources.toml")
    p_euro.add_argument("--source", type=str, default="eurostat_gdp", help="Source name from sources.toml")
    p_euro.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="Data directory (raw/state/db)")
    p_euro.add_argument("--db", type=Path, default=DEFAULT_DB, help="DuckDB database path")

    p_fema = sub.add_parser("ingest-openfema", help="Ingest OpenFEMA DisasterDeclarationsSummaries into DuckDB")
    p_fema.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Path to sources.toml")
    p_fema.add_argument("--source", type=str, default="openfema_disaster_declarations", help="Source name from sources.toml")
    p_fema.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="Data directory (raw/state/db)")
    p_fema.add_argument("--db", type=Path, default=DEFAULT_DB, help="DuckDB database path")

    p_nws = sub.add_parser("ingest-nws", help="Ingest NWS active alerts into DuckDB")
    p_nws.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Path to sources.toml")
    p_nws.add_argument("--source", type=str, default="nws_alerts_active", help="Source name from sources.toml")
    p_nws.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="Data directory (raw/state/db)")
    p_nws.add_argument("--db", type=Path, default=DEFAULT_DB, help="DuckDB database path")

    p_gdacs = sub.add_parser("ingest-gdacs", help="Ingest GDACS alerts RSS into DuckDB")
    p_gdacs.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Path to sources.toml")
    p_gdacs.add_argument("--source", type=str, default="gdacs_alerts", help="Source name from sources.toml")
    p_gdacs.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="Data directory (raw/state/db)")
    p_gdacs.add_argument("--db", type=Path, default=DEFAULT_DB, help="DuckDB database path")

    p_wb = sub.add_parser("ingest-worldbank", help="Ingest World Bank indicator data into DuckDB")
    p_wb.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Path to sources.toml")
    p_wb.add_argument("--source", type=str, default="worldbank_gdp_usa", help="Source name from sources.toml")
    p_wb.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="Data directory (raw/state/db)")
    p_wb.add_argument("--db", type=Path, default=DEFAULT_DB, help="DuckDB database path")

    p_ckan = sub.add_parser("ingest-ckan", help="Ingest CKAN package_search results into DuckDB")
    p_ckan.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Path to sources.toml")
    p_ckan.add_argument("--source", type=str, default="datagov_catalog_climate", help="Source name from sources.toml")
    p_ckan.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="Data directory (raw/state/db)")
    p_ckan.add_argument("--db", type=Path, default=DEFAULT_DB, help="DuckDB database path")

    p_socrata = sub.add_parser("ingest-socrata", help="Ingest Socrata SODA JSON into DuckDB")
    p_socrata.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Path to sources.toml")
    p_socrata.add_argument("--source", type=str, default="socrata_example", help="Source name from sources.toml")
    p_socrata.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="Data directory (raw/state/db)")
    p_socrata.add_argument("--db", type=Path, default=DEFAULT_DB, help="DuckDB database path")

    p_ods = sub.add_parser("ingest-opendatasoft", help="Ingest Opendatasoft Explore API records into DuckDB")
    p_ods.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Path to sources.toml")
    p_ods.add_argument("--source", type=str, default="opendatasoft_example", help="Source name from sources.toml")
    p_ods.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="Data directory (raw/state/db)")
    p_ods.add_argument("--db", type=Path, default=DEFAULT_DB, help="DuckDB database path")

    p_gdelt = sub.add_parser("ingest-gdelt", help="Ingest GDELT doc query results into DuckDB")
    p_gdelt.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Path to sources.toml")
    p_gdelt.add_argument("--source", type=str, default="gdelt_democracy_24h", help="Source name from sources.toml")
    p_gdelt.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="Data directory (raw/state/db)")
    p_gdelt.add_argument("--db", type=Path, default=DEFAULT_DB, help="DuckDB database path")

    p_ooni = sub.add_parser("ingest-ooni", help="Ingest OONI measurement metadata into DuckDB")
    p_ooni.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Path to sources.toml")
    p_ooni.add_argument("--source", type=str, default="ooni_us_recent", help="Source name from sources.toml")
    p_ooni.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="Data directory (raw/state/db)")
    p_ooni.add_argument("--db", type=Path, default=DEFAULT_DB, help="DuckDB database path")

    p_ioda = sub.add_parser("ingest-ioda", help="Ingest CAIDA IODA events into DuckDB")
    p_ioda.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Path to sources.toml")
    p_ioda.add_argument("--source", type=str, default="caida_ioda_recent", help="Source name from sources.toml")
    p_ioda.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="Data directory (raw/state/db)")
    p_ioda.add_argument("--db", type=Path, default=DEFAULT_DB, help="DuckDB database path")

    p_ris = sub.add_parser("ingest-ripe-ris", help="Capture RIPE RIS Live messages into DuckDB")
    p_ris.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Path to sources.toml")
    p_ris.add_argument("--source", type=str, default="ripe_ris_live_10s", help="Source name from sources.toml")
    p_ris.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="Data directory (raw/state/db)")
    p_ris.add_argument("--db", type=Path, default=DEFAULT_DB, help="DuckDB database path")

    p_atlas = sub.add_parser("ingest-ripe-atlas", help="Ingest RIPE Atlas probe metadata into DuckDB")
    p_atlas.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Path to sources.toml")
    p_atlas.add_argument("--source", type=str, default="ripe_atlas_probes", help="Source name from sources.toml")
    p_atlas.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="Data directory (raw/state/db)")
    p_atlas.add_argument("--db", type=Path, default=DEFAULT_DB, help="DuckDB database path")

    p_twelvedata = sub.add_parser("ingest-twelvedata", help="Ingest Twelve Data time series into DuckDB")
    p_twelvedata.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Path to sources.toml")
    p_twelvedata.add_argument("--source", type=str, default="twelvedata_watchlist_daily", help="Source name from sources.toml")
    p_twelvedata.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="Data directory (raw/state/db)")
    p_twelvedata.add_argument("--db", type=Path, default=DEFAULT_DB, help="DuckDB database path")

    p_fred = sub.add_parser("ingest-fred", help="Ingest FRED series observations into DuckDB")
    p_fred.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Path to sources.toml")
    p_fred.add_argument("--source", type=str, default="fred_macro_watchlist", help="Source name from sources.toml")
    p_fred.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="Data directory (raw/state/db)")
    p_fred.add_argument("--db", type=Path, default=DEFAULT_DB, help="DuckDB database path")

    p_eia = sub.add_parser("ingest-eia", help="Ingest EIA Open Data v2 series into DuckDB")
    p_eia.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Path to sources.toml")
    p_eia.add_argument("--source", type=str, default="eia_petroleum_wpsr_weekly", help="Source name from sources.toml")
    p_eia.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="Data directory (raw/state/db)")
    p_eia.add_argument("--db", type=Path, default=DEFAULT_DB, help="DuckDB database path")

    p_bls = sub.add_parser("ingest-bls", help="Ingest BLS Public Data API timeseries into DuckDB")
    p_bls.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Path to sources.toml")
    p_bls.add_argument("--source", type=str, default="bls_truthbot_watchlist", help="Source name from sources.toml")
    p_bls.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="Data directory (raw/state/db)")
    p_bls.add_argument("--db", type=Path, default=DEFAULT_DB, help="DuckDB database path")

    p_census = sub.add_parser("ingest-census", help="Ingest U.S. Census ACS API observations into DuckDB")
    p_census.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Path to sources.toml")
    p_census.add_argument("--source", type=str, default="census_acs_state_basic", help="Source name from sources.toml")
    p_census.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="Data directory (raw/state/db)")
    p_census.add_argument("--db", type=Path, default=DEFAULT_DB, help="DuckDB database path")

    p_alt_fng = sub.add_parser("ingest-altme-fng", help="Ingest Alternative.me Crypto Fear & Greed Index into DuckDB")
    p_alt_fng.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Path to sources.toml")
    p_alt_fng.add_argument("--source", type=str, default="alternative_me_fng_daily", help="Source name from sources.toml")
    p_alt_fng.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="Data directory (raw/state/db)")
    p_alt_fng.add_argument("--db", type=Path, default=DEFAULT_DB, help="DuckDB database path")

    p_cnn_fg = sub.add_parser("ingest-cnn-fg", help="Ingest CNN Fear & Greed Index into DuckDB")
    p_cnn_fg.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Path to sources.toml")
    p_cnn_fg.add_argument("--source", type=str, default="cnn_fear_greed_daily", help="Source name from sources.toml")
    p_cnn_fg.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="Data directory (raw/state/db)")
    p_cnn_fg.add_argument("--db", type=Path, default=DEFAULT_DB, help="DuckDB database path")

    p_signals = sub.add_parser("compute-signals", help="Compute derived signals from ingested data")
    p_signals.add_argument("--db", type=Path, default=DEFAULT_DB, help="DuckDB database path")
    p_signals.add_argument("--since", type=str, default="24h", help="Lookback window (e.g., 24h, 7d)")
    p_signals.add_argument("--usgs-min-mag", type=float, default=5.0, help="Minimum USGS magnitude")
    p_signals.add_argument(
        "--gdacs-min-severity", type=float, default=0.6, help="Minimum GDACS severity (0-1)"
    )

    p_alert = sub.add_parser("alert", help="Print highest severity signals by type")
    p_alert.add_argument("--db", type=Path, default=DEFAULT_DB, help="DuckDB database path")
    p_alert.add_argument("--since", type=str, default="24h", help="Lookback window (e.g., 24h, 7d)")
    p_alert.add_argument("--limit", type=int, default=5, help="Max signals per type")

    p_latest = sub.add_parser("show-latest", help="Show latest ingested earthquake events")
    p_latest.add_argument("--db", type=Path, default=DEFAULT_DB, help="DuckDB database path")
    p_latest.add_argument("--limit", type=int, default=10, help="Number of rows to show")

    p_snapshot = sub.add_parser("snapshot", help="Create a local snapshot (zip/parquet) for backup")
    p_snapshot.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="Data directory (raw/state/db)")
    p_snapshot.add_argument("--db", type=Path, default=DEFAULT_DB, help="DuckDB database path")
    p_snapshot.add_argument("--format", choices=["zip", "parquet", "both"], default="zip", help="Snapshot format")
    p_snapshot.add_argument("--output", type=Path, default=None, help="Output directory or .zip path")

    return p


def main(argv: list[str] | None = None) -> int:
    raw = sys.argv[1:] if argv is None else list(argv)
    if raw and raw[0] == "publish":
        return publish_main(raw[1:])

    args = build_parser().parse_args(argv)

    if args.cmd == "ingest-usgs":
        ingest_usgs_geojson(
            config_path=args.config,
            source_name=args.source,
            data_dir=args.data_dir,
            db_path=args.db,
        )
        return 0

    if args.cmd == "ingest-usgs-fdsn":
        ingest_usgs_fdsn_events(
            config_path=args.config,
            source_name=args.source,
            data_dir=args.data_dir,
            db_path=args.db,
        )
        return 0

    if args.cmd == "ingest-eurostat":
        ingest_eurostat_stats(
            config_path=args.config,
            source_name=args.source,
            data_dir=args.data_dir,
            db_path=args.db,
        )
        return 0

    if args.cmd == "ingest-openfema":
        ingest_openfema_disaster_declarations(
            config_path=args.config,
            source_name=args.source,
            data_dir=args.data_dir,
            db_path=args.db,
        )
        return 0

    if args.cmd == "ingest-nws":
        ingest_nws_alerts(
            config_path=args.config,
            source_name=args.source,
            data_dir=args.data_dir,
            db_path=args.db,
        )
        return 0

    if args.cmd == "ingest-gdacs":
        ingest_gdacs_rss(
            config_path=args.config,
            source_name=args.source,
            data_dir=args.data_dir,
            db_path=args.db,
        )
        return 0

    if args.cmd == "ingest-worldbank":
        ingest_worldbank_indicator(
            config_path=args.config,
            source_name=args.source,
            data_dir=args.data_dir,
            db_path=args.db,
        )
        return 0

    if args.cmd == "ingest-ckan":
        ingest_ckan_catalog(
            config_path=args.config,
            source_name=args.source,
            data_dir=args.data_dir,
            db_path=args.db,
        )
        return 0

    if args.cmd == "ingest-socrata":
        ingest_socrata_soda(
            config_path=args.config,
            source_name=args.source,
            data_dir=args.data_dir,
            db_path=args.db,
        )
        return 0

    if args.cmd == "ingest-opendatasoft":
        ingest_opendatasoft_explore(
            config_path=args.config,
            source_name=args.source,
            data_dir=args.data_dir,
            db_path=args.db,
        )
        return 0

    if args.cmd == "ingest-gdelt":
        ingest_gdelt_doc_query(
            config_path=args.config,
            source_name=args.source,
            data_dir=args.data_dir,
            db_path=args.db,
        )
        return 0

    if args.cmd == "ingest-ooni":
        ingest_ooni_measurements(
            config_path=args.config,
            source_name=args.source,
            data_dir=args.data_dir,
            db_path=args.db,
        )
        return 0

    if args.cmd == "ingest-ioda":
        ingest_ioda_events(
            config_path=args.config,
            source_name=args.source,
            data_dir=args.data_dir,
            db_path=args.db,
        )
        return 0

    if args.cmd == "ingest-ripe-ris":
        ingest_ripe_ris_live(
            config_path=args.config,
            source_name=args.source,
            data_dir=args.data_dir,
            db_path=args.db,
        )
        return 0

    if args.cmd == "ingest-ripe-atlas":
        ingest_ripe_atlas_probes(
            config_path=args.config,
            source_name=args.source,
            data_dir=args.data_dir,
            db_path=args.db,
        )
        return 0

    if args.cmd == "ingest-twelvedata":
        ingest_twelvedata_time_series(
            config_path=args.config,
            source_name=args.source,
            data_dir=args.data_dir,
            db_path=args.db,
        )
        return 0

    if args.cmd == "ingest-fred":
        ingest_fred_series(
            config_path=args.config,
            source_name=args.source,
            data_dir=args.data_dir,
            db_path=args.db,
        )
        return 0

    if args.cmd == "ingest-eia":
        ingest_eia_v2(
            config_path=args.config,
            source_name=args.source,
            data_dir=args.data_dir,
            db_path=args.db,
        )
        return 0

    if args.cmd == "ingest-bls":
        ingest_bls_timeseries(
            config_path=args.config,
            source_name=args.source,
            data_dir=args.data_dir,
            db_path=args.db,
        )
        return 0

    if args.cmd == "ingest-census":
        ingest_census_acs(
            config_path=args.config,
            source_name=args.source,
            data_dir=args.data_dir,
            db_path=args.db,
        )
        return 0

    if args.cmd == "ingest-altme-fng":
        ingest_alternative_me_fng(
            config_path=args.config,
            source_name=args.source,
            data_dir=args.data_dir,
            db_path=args.db,
        )
        return 0

    if args.cmd == "ingest-cnn-fg":
        ingest_cnn_fear_greed(
            config_path=args.config,
            source_name=args.source,
            data_dir=args.data_dir,
            db_path=args.db,
        )
        return 0

    if args.cmd == "compute-signals":
        inserted = compute_signals(
            db_path=str(args.db),
            since=args.since,
            min_magnitude=args.usgs_min_mag,
            gdacs_min_severity=args.gdacs_min_severity,
        )
        print(f"Computed signals inserted_or_updated={inserted}")
        return 0

    if args.cmd == "alert":
        alert_signals(db_path=str(args.db), since=args.since, limit=args.limit)
        return 0

    if args.cmd == "show-latest":
        show_latest(db_path=args.db, limit=args.limit)
        return 0

    if args.cmd == "snapshot":
        stamp = default_snapshot_stamp()
        base = args.output or (args.data_dir / "snapshots")

        if args.format in {"parquet", "both"} and base.suffix:
            raise SystemExit("--output must be a directory for parquet snapshots.")

        if args.format in {"zip", "both"}:
            zip_path = base if base.suffix == ".zip" else base / f"snapshot-{stamp}.zip"
            snapshot_zip(data_dir=args.data_dir, db_path=args.db, output_path=zip_path)
            print(f"ZIP snapshot: {zip_path}")

        if args.format in {"parquet", "both"}:
            parquet_dir = base / f"snapshot-{stamp}"
            snapshot_parquet(db_path=args.db, output_dir=parquet_dir)
            print(f"Parquet snapshot: {parquet_dir}")
        return 0

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
