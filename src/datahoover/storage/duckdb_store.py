from __future__ import annotations

import duckdb
from datetime import datetime
from pathlib import Path
from typing import Iterable, Dict, Any


def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS usgs_earthquakes (
              source       VARCHAR,
              feed_url      VARCHAR,
              event_id      VARCHAR,
              magnitude     DOUBLE,
              place         VARCHAR,
              time_utc      TIMESTAMP,
              updated_utc   TIMESTAMP,
              url           VARCHAR,
              detail        VARCHAR,
              tsunami       INTEGER,
              status        VARCHAR,
              event_type    VARCHAR,
              longitude     DOUBLE,
              latitude      DOUBLE,
              depth_km      DOUBLE,
              raw_json      VARCHAR,
              ingested_at   TIMESTAMP
            );
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS ingest_runs (
              run_id     VARCHAR,
              source     VARCHAR,
              feed_url   VARCHAR,
              started_at TIMESTAMP,
              ended_at   TIMESTAMP,
              status     VARCHAR,
              n_total    INTEGER,
              n_new      INTEGER,
              message    VARCHAR
            );
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS signals (
              signal_id      VARCHAR,
              signal_type    VARCHAR,
              source         VARCHAR,
              entity_type    VARCHAR,
              entity_id      VARCHAR,
              ts_start       TIMESTAMP,
              ts_end         TIMESTAMP,
              severity_score DOUBLE,
              summary        VARCHAR,
              details_json   VARCHAR,
              ingested_at    TIMESTAMP,
              computed_at    TIMESTAMP,
              raw_paths      VARCHAR
            );
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS eurostat_stats (
              source       VARCHAR,
              dataset_id   VARCHAR,
              freq         VARCHAR,
              unit         VARCHAR,
              na_item      VARCHAR,
              geo          VARCHAR,
              time_period  VARCHAR,
              value        DOUBLE,
              extra_dims   VARCHAR,
              ingested_at  TIMESTAMP
            );
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS openfema_disaster_declarations (
              source             VARCHAR,
              declaration_id     VARCHAR,
              disaster_number    INTEGER,
              state              VARCHAR,
              declaration_type   VARCHAR,
              declaration_date   TIMESTAMP,
              incident_type      VARCHAR,
              declaration_title  VARCHAR,
              incident_begin_date TIMESTAMP,
              incident_end_date  TIMESTAMP,
              raw_json           VARCHAR,
              ingested_at        TIMESTAMP
            );
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS nws_alerts (
              source        VARCHAR,
              feed_url      VARCHAR,
              alert_id      VARCHAR,
              sent          TIMESTAMP,
              effective     TIMESTAMP,
              expires       TIMESTAMP,
              severity      VARCHAR,
              urgency       VARCHAR,
              certainty     VARCHAR,
              event         VARCHAR,
              headline      VARCHAR,
              area_desc     VARCHAR,
              instruction   VARCHAR,
              sender_name   VARCHAR,
              alert_source  VARCHAR,
              bbox_min_lon  DOUBLE,
              bbox_min_lat  DOUBLE,
              bbox_max_lon  DOUBLE,
              bbox_max_lat  DOUBLE,
              centroid_lon  DOUBLE,
              centroid_lat  DOUBLE,
              raw_json      VARCHAR,
              ingested_at   TIMESTAMP
            );
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS gdacs_alerts (
              source       VARCHAR,
              feed_url     VARCHAR,
              entry_id     VARCHAR,
              title        VARCHAR,
              published    TIMESTAMP,
              updated      TIMESTAMP,
              link         VARCHAR,
              summary      VARCHAR,
              event_type   VARCHAR,
              raw_json     VARCHAR,
              ingested_at  TIMESTAMP
            );
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS worldbank_indicators (
              source        VARCHAR,
              feed_url      VARCHAR,
              series_id     VARCHAR,
              country_id    VARCHAR,
              country_name  VARCHAR,
              year          VARCHAR,
              value         DOUBLE,
              unit          VARCHAR,
              raw_json      VARCHAR,
              ingested_at   TIMESTAMP
            );
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS ckan_packages (
              source             VARCHAR,
              feed_url           VARCHAR,
              package_id         VARCHAR,
              name               VARCHAR,
              title              VARCHAR,
              organization       VARCHAR,
              metadata_created   TIMESTAMP,
              metadata_modified  TIMESTAMP,
              license_id         VARCHAR,
              num_resources      INTEGER,
              tags               VARCHAR,
              raw_json           VARCHAR,
              ingested_at        TIMESTAMP
            );
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS socrata_records (
              source        VARCHAR,
              feed_url      VARCHAR,
              record_hash   VARCHAR,
              retrieved_at  TIMESTAMP,
              raw_json      VARCHAR,
              ingested_at   TIMESTAMP
            );
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS opendatasoft_records (
              source        VARCHAR,
              feed_url      VARCHAR,
              record_id     VARCHAR,
              field_summary VARCHAR,
              raw_json      VARCHAR,
              ingested_at   TIMESTAMP
            );
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS gdelt_docs (
              source            VARCHAR,
              feed_url          VARCHAR,
              document_id       VARCHAR,
              url               VARCHAR,
              title             VARCHAR,
              seendate          VARCHAR,
              source_country    VARCHAR,
              source_collection VARCHAR,
              tone              VARCHAR,
              raw_json          VARCHAR,
              ingested_at       TIMESTAMP
            );
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS ooni_measurements (
              source                 VARCHAR,
              feed_url               VARCHAR,
              measurement_id         VARCHAR,
              test_name              VARCHAR,
              probe_cc               VARCHAR,
              measurement_start_time TIMESTAMP,
              input                  VARCHAR,
              anomaly                BOOLEAN,
              confirmed              BOOLEAN,
              scores                 VARCHAR,
              raw_json               VARCHAR,
              ingested_at            TIMESTAMP
            );
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS caida_ioda_events (
              source        VARCHAR,
              feed_url      VARCHAR,
              event_id      VARCHAR,
              start_time    TIMESTAMP,
              end_time      TIMESTAMP,
              country       VARCHAR,
              asn           VARCHAR,
              signal_type   VARCHAR,
              severity      VARCHAR,
              raw_json      VARCHAR,
              ingested_at   TIMESTAMP
            );
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS ripe_ris_messages (
              source        VARCHAR,
              feed_url      VARCHAR,
              msg_id        VARCHAR,
              timestamp     TIMESTAMP,
              prefix        VARCHAR,
              asn           VARCHAR,
              path          VARCHAR,
              message_type  VARCHAR,
              raw_json      VARCHAR,
              ingested_at   TIMESTAMP
            );
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS ripe_atlas_probes (
              source          VARCHAR,
              feed_url        VARCHAR,
              probe_id        VARCHAR,
              country_code    VARCHAR,
              status          VARCHAR,
              asn_v4          VARCHAR,
              asn_v6          VARCHAR,
              latitude        DOUBLE,
              longitude       DOUBLE,
              first_connected TIMESTAMP,
              last_connected  TIMESTAMP,
              raw_json        VARCHAR,
              ingested_at     TIMESTAMP
            );
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS twelvedata_time_series (
              source       VARCHAR,
              symbol       VARCHAR,
              interval     VARCHAR,
              series_group VARCHAR,
              ts           TIMESTAMP,
              open         DOUBLE,
              high         DOUBLE,
              low          DOUBLE,
              close        DOUBLE,
              volume       BIGINT,
              currency     VARCHAR,
              exchange     VARCHAR,
              ingested_at  TIMESTAMP,
              raw_path     VARCHAR
            );
            """
        )
        con.execute(
            "ALTER TABLE twelvedata_time_series ADD COLUMN IF NOT EXISTS series_group VARCHAR DEFAULT 'primary';"
        )
        con.execute(
            "UPDATE twelvedata_time_series SET series_group = 'primary' WHERE series_group IS NULL;"
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS fred_series_observations (
              source           VARCHAR,
              series_id        VARCHAR,
              observation_date DATE,
              value            DOUBLE,
              realtime_start   DATE,
              realtime_end     DATE,
              units            VARCHAR,
              ingested_at      TIMESTAMP,
              raw_path         VARCHAR
            );
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS bls_timeseries_observations (
              source       VARCHAR,
              series_id    VARCHAR,
              year         INTEGER,
              period       VARCHAR,
              period_name  VARCHAR,
              value        DOUBLE,
              footnotes    VARCHAR,
              ingested_at  TIMESTAMP,
              raw_path     VARCHAR
            );
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS census_observations (
              source       VARCHAR,
              dataset      VARCHAR,
              year         INTEGER,
              geo_type     VARCHAR,
              geo_id       VARCHAR,
              variable     VARCHAR,
              value        DOUBLE,
              label        VARCHAR,
              ingested_at  TIMESTAMP,
              raw_path     VARCHAR
            );
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS eia_v2_observations (
              source      VARCHAR,
              route       VARCHAR,
              frequency   VARCHAR,
              series_id   VARCHAR,
              period      VARCHAR,
              value       DOUBLE,
              units       VARCHAR,
              ingested_at TIMESTAMP,
              raw_path    VARCHAR
            );
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS alternative_me_fng (
              source           VARCHAR,
              observation_date DATE,
              ts_utc           TIMESTAMP,
              value            INTEGER,
              classification   VARCHAR,
              ingested_at      TIMESTAMP,
              raw_path         VARCHAR
            );
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS cnn_fear_greed (
              source           VARCHAR,
              component        VARCHAR,
              observation_date DATE,
              ts_utc           TIMESTAMP,
              score            DOUBLE,
              rating           VARCHAR,
              ingested_at      TIMESTAMP,
              raw_path         VARCHAR
            );
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS gdelt_gkg (
              source             VARCHAR,
              feed_url           VARCHAR,
              gkg_record_id      VARCHAR,
              v21_date           TIMESTAMP,
              source_collection  VARCHAR,
              source_common_name VARCHAR,
              document_url       VARCHAR,
              v2_themes          VARCHAR,
              v2_tone            VARCHAR,
              v2_tone_avg        DOUBLE,
              v2_tone_pos        DOUBLE,
              v2_tone_neg        DOUBLE,
              v2_tone_polarity   DOUBLE,
              v2_word_count      INTEGER,
              raw_row_json       VARCHAR,
              ingested_at        TIMESTAMP
            );
            """
        )
        # Create indexes for performance
        con.execute("CREATE INDEX IF NOT EXISTS idx_signals_signal_id ON signals(signal_id);")
        con.execute("CREATE INDEX IF NOT EXISTS idx_signals_severity ON signals(severity_score DESC, computed_at DESC);")
        con.execute("CREATE INDEX IF NOT EXISTS idx_usgs_earthquakes_key ON usgs_earthquakes(source, event_id);")
        con.execute("CREATE INDEX IF NOT EXISTS idx_usgs_earthquakes_query ON usgs_earthquakes(magnitude, time_utc);")
        con.execute("CREATE INDEX IF NOT EXISTS idx_ooni_measurements_key ON ooni_measurements(source, measurement_id);")
        con.execute("CREATE INDEX IF NOT EXISTS idx_caida_ioda_events_key ON caida_ioda_events(source, event_id);")
        con.execute("CREATE INDEX IF NOT EXISTS idx_ingest_runs_lookup ON ingest_runs(source, status, ended_at);")
        con.execute("CREATE INDEX IF NOT EXISTS idx_twelvedata_time_series_key ON twelvedata_time_series(source, symbol, interval, series_group, ts);")
        con.execute("CREATE INDEX IF NOT EXISTS idx_twelvedata_time_series_query ON twelvedata_time_series(source, symbol, ts DESC);")
        con.execute("CREATE INDEX IF NOT EXISTS idx_fred_series_key ON fred_series_observations(source, series_id, observation_date, realtime_start, realtime_end);")
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_bls_timeseries_key ON bls_timeseries_observations(source, series_id, year, period);"
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_census_obs_key ON census_observations(source, dataset, year, geo_type, geo_id, variable);"
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_eia_v2_obs_key ON eia_v2_observations(source, series_id, period);"
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_alt_me_fng_key ON alternative_me_fng(source, observation_date);"
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_cnn_fg_key ON cnn_fear_greed(source, component, ts_utc);"
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_gdelt_gkg_key ON gdelt_gkg(source, gkg_record_id);"
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_gdelt_gkg_query ON gdelt_gkg(source, v21_date DESC);"
        )
    finally:
        con.close()


def upsert_usgs_events(db_path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    """Insert new rows; overwrite existing (source,event_id) by delete+insert.

    Returns count of inserted/updated rows.
    """
    con = duckdb.connect(str(db_path))
    inserted = 0
    try:
        for r in rows:
            con.execute(
                "DELETE FROM usgs_earthquakes WHERE source = ? AND event_id = ?",
                [r["source"], r["event_id"]],
            )
            con.execute(
                """
                INSERT INTO usgs_earthquakes VALUES
                  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    r["source"],
                    r["feed_url"],
                    r["event_id"],
                    r["magnitude"],
                    r["place"],
                    r["time_utc"],
                    r["updated_utc"],
                    r["url"],
                    r["detail"],
                    r["tsunami"],
                    r["status"],
                    r["event_type"],
                    r["longitude"],
                    r["latitude"],
                    r["depth_km"],
                    r["raw_json"],
                    r["ingested_at"],
                ],
            )
            inserted += 1
    finally:
        con.close()
    return inserted


def upsert_eurostat_stats(db_path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    """Insert new rows; overwrite existing by dimensional key."""
    con = duckdb.connect(str(db_path))
    inserted = 0
    try:
        for r in rows:
            con.execute(
                """
                DELETE FROM eurostat_stats
                WHERE source = ? AND dataset_id = ? AND freq IS NOT DISTINCT FROM ?
                  AND unit IS NOT DISTINCT FROM ? AND na_item IS NOT DISTINCT FROM ?
                  AND geo IS NOT DISTINCT FROM ? AND time_period IS NOT DISTINCT FROM ?
                  AND extra_dims IS NOT DISTINCT FROM ?
                """,
                [
                    r["source"],
                    r["dataset_id"],
                    r.get("freq"),
                    r.get("unit"),
                    r.get("na_item"),
                    r.get("geo"),
                    r.get("time_period"),
                    r.get("extra_dims"),
                ],
            )
            con.execute(
                """
                INSERT INTO eurostat_stats VALUES
                  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    r["source"],
                    r["dataset_id"],
                    r.get("freq"),
                    r.get("unit"),
                    r.get("na_item"),
                    r.get("geo"),
                    r.get("time_period"),
                    r.get("value"),
                    r.get("extra_dims"),
                    r["ingested_at"],
                ],
            )
            inserted += 1
    finally:
        con.close()
    return inserted


def upsert_openfema_disaster_declarations(db_path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    """Insert new rows; overwrite existing by declaration id."""
    con = duckdb.connect(str(db_path))
    inserted = 0
    try:
        for r in rows:
            con.execute(
                "DELETE FROM openfema_disaster_declarations WHERE source = ? AND declaration_id = ?",
                [r["source"], r["declaration_id"]],
            )
            con.execute(
                """
                INSERT INTO openfema_disaster_declarations VALUES
                  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    r["source"],
                    r["declaration_id"],
                    r.get("disaster_number"),
                    r.get("state"),
                    r.get("declaration_type"),
                    r.get("declaration_date"),
                    r.get("incident_type"),
                    r.get("declaration_title"),
                    r.get("incident_begin_date"),
                    r.get("incident_end_date"),
                    r.get("raw_json"),
                    r["ingested_at"],
                ],
            )
            inserted += 1
    finally:
        con.close()
    return inserted


def upsert_nws_alerts(db_path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    """Insert new rows; overwrite existing by alert id."""
    con = duckdb.connect(str(db_path))
    inserted = 0
    try:
        for r in rows:
            con.execute(
                "DELETE FROM nws_alerts WHERE source = ? AND alert_id = ?",
                [r["source"], r["alert_id"]],
            )
            con.execute(
                """
                INSERT INTO nws_alerts VALUES
                  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    r["source"],
                    r["feed_url"],
                    r["alert_id"],
                    r.get("sent"),
                    r.get("effective"),
                    r.get("expires"),
                    r.get("severity"),
                    r.get("urgency"),
                    r.get("certainty"),
                    r.get("event"),
                    r.get("headline"),
                    r.get("area_desc"),
                    r.get("instruction"),
                    r.get("sender_name"),
                    r.get("alert_source"),
                    r.get("bbox_min_lon"),
                    r.get("bbox_min_lat"),
                    r.get("bbox_max_lon"),
                    r.get("bbox_max_lat"),
                    r.get("centroid_lon"),
                    r.get("centroid_lat"),
                    r.get("raw_json"),
                    r["ingested_at"],
                ],
            )
            inserted += 1
    finally:
        con.close()
    return inserted


def upsert_gdacs_alerts(db_path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    """Insert new rows; overwrite existing by entry id."""
    con = duckdb.connect(str(db_path))
    inserted = 0
    try:
        for r in rows:
            con.execute(
                "DELETE FROM gdacs_alerts WHERE source = ? AND entry_id = ?",
                [r["source"], r["entry_id"]],
            )
            con.execute(
                """
                INSERT INTO gdacs_alerts VALUES
                  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    r["source"],
                    r["feed_url"],
                    r["entry_id"],
                    r.get("title"),
                    r.get("published"),
                    r.get("updated"),
                    r.get("link"),
                    r.get("summary"),
                    r.get("event_type"),
                    r.get("raw_json"),
                    r["ingested_at"],
                ],
            )
            inserted += 1
    finally:
        con.close()
    return inserted


def upsert_worldbank_indicators(db_path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    """Insert new rows; overwrite existing by series/country/year."""
    con = duckdb.connect(str(db_path))
    inserted = 0
    try:
        for r in rows:
            con.execute(
                """
                DELETE FROM worldbank_indicators
                WHERE source = ? AND series_id = ? AND country_id = ? AND year = ?
                """,
                [r["source"], r["series_id"], r["country_id"], r["year"]],
            )
            con.execute(
                """
                INSERT INTO worldbank_indicators VALUES
                  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    r["source"],
                    r["feed_url"],
                    r["series_id"],
                    r["country_id"],
                    r.get("country_name"),
                    r.get("year"),
                    r.get("value"),
                    r.get("unit"),
                    r.get("raw_json"),
                    r["ingested_at"],
                ],
            )
            inserted += 1
    finally:
        con.close()
    return inserted


def upsert_ckan_packages(db_path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    """Insert new rows; overwrite existing by package id."""
    con = duckdb.connect(str(db_path))
    inserted = 0
    try:
        for r in rows:
            con.execute(
                "DELETE FROM ckan_packages WHERE source = ? AND package_id = ?",
                [r["source"], r["package_id"]],
            )
            con.execute(
                """
                INSERT INTO ckan_packages VALUES
                  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    r["source"],
                    r["feed_url"],
                    r["package_id"],
                    r.get("name"),
                    r.get("title"),
                    r.get("organization"),
                    r.get("metadata_created"),
                    r.get("metadata_modified"),
                    r.get("license_id"),
                    r.get("num_resources"),
                    r.get("tags"),
                    r.get("raw_json"),
                    r["ingested_at"],
                ],
            )
            inserted += 1
    finally:
        con.close()
    return inserted


def upsert_socrata_records(db_path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    """Insert new rows; overwrite existing by record hash."""
    con = duckdb.connect(str(db_path))
    inserted = 0
    try:
        for r in rows:
            con.execute(
                "DELETE FROM socrata_records WHERE source = ? AND record_hash = ?",
                [r["source"], r["record_hash"]],
            )
            con.execute(
                """
                INSERT INTO socrata_records VALUES
                  (?, ?, ?, ?, ?, ?)
                """,
                [
                    r["source"],
                    r["feed_url"],
                    r["record_hash"],
                    r.get("retrieved_at"),
                    r.get("raw_json"),
                    r["ingested_at"],
                ],
            )
            inserted += 1
    finally:
        con.close()
    return inserted


def upsert_opendatasoft_records(db_path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    """Insert new rows; overwrite existing by record id."""
    con = duckdb.connect(str(db_path))
    inserted = 0
    try:
        for r in rows:
            con.execute(
                "DELETE FROM opendatasoft_records WHERE source = ? AND record_id = ?",
                [r["source"], r["record_id"]],
            )
            con.execute(
                """
                INSERT INTO opendatasoft_records VALUES
                  (?, ?, ?, ?, ?, ?)
                """,
                [
                    r["source"],
                    r["feed_url"],
                    r["record_id"],
                    r.get("field_summary"),
                    r.get("raw_json"),
                    r["ingested_at"],
                ],
            )
            inserted += 1
    finally:
        con.close()
    return inserted


def upsert_gdelt_docs(db_path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    """Insert new rows; overwrite existing by document id."""
    con = duckdb.connect(str(db_path))
    inserted = 0
    try:
        for r in rows:
            con.execute(
                "DELETE FROM gdelt_docs WHERE source = ? AND document_id = ?",
                [r["source"], r["document_id"]],
            )
            con.execute(
                """
                INSERT INTO gdelt_docs VALUES
                  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    r["source"],
                    r["feed_url"],
                    r["document_id"],
                    r.get("url"),
                    r.get("title"),
                    r.get("seendate"),
                    r.get("source_country"),
                    r.get("source_collection"),
                    r.get("tone"),
                    r.get("raw_json"),
                    r["ingested_at"],
                ],
            )
            inserted += 1
    finally:
        con.close()
    return inserted


def upsert_ooni_measurements(db_path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    """Insert new rows; overwrite existing by measurement id."""
    con = duckdb.connect(str(db_path))
    inserted = 0
    try:
        for r in rows:
            con.execute(
                "DELETE FROM ooni_measurements WHERE source = ? AND measurement_id = ?",
                [r["source"], r["measurement_id"]],
            )
            con.execute(
                """
                INSERT INTO ooni_measurements VALUES
                  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    r["source"],
                    r["feed_url"],
                    r["measurement_id"],
                    r.get("test_name"),
                    r.get("probe_cc"),
                    r.get("measurement_start_time"),
                    r.get("input"),
                    r.get("anomaly"),
                    r.get("confirmed"),
                    r.get("scores"),
                    r.get("raw_json"),
                    r["ingested_at"],
                ],
            )
            inserted += 1
    finally:
        con.close()
    return inserted


def upsert_ioda_events(db_path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    """Insert new rows; overwrite existing by event id."""
    con = duckdb.connect(str(db_path))
    inserted = 0
    try:
        for r in rows:
            con.execute(
                "DELETE FROM caida_ioda_events WHERE source = ? AND event_id = ?",
                [r["source"], r["event_id"]],
            )
            con.execute(
                """
                INSERT INTO caida_ioda_events VALUES
                  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    r["source"],
                    r["feed_url"],
                    r["event_id"],
                    r.get("start_time"),
                    r.get("end_time"),
                    r.get("country"),
                    r.get("asn"),
                    r.get("signal_type"),
                    r.get("severity"),
                    r.get("raw_json"),
                    r["ingested_at"],
                ],
            )
            inserted += 1
    finally:
        con.close()
    return inserted


def upsert_ripe_ris_messages(db_path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    """Insert new rows; overwrite existing by msg id."""
    con = duckdb.connect(str(db_path))
    inserted = 0
    try:
        for r in rows:
            con.execute(
                "DELETE FROM ripe_ris_messages WHERE source = ? AND msg_id = ?",
                [r["source"], r["msg_id"]],
            )
            con.execute(
                """
                INSERT INTO ripe_ris_messages VALUES
                  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    r["source"],
                    r["feed_url"],
                    r["msg_id"],
                    r.get("timestamp"),
                    r.get("prefix"),
                    r.get("asn"),
                    r.get("path"),
                    r.get("message_type"),
                    r.get("raw_json"),
                    r["ingested_at"],
                ],
            )
            inserted += 1
    finally:
        con.close()
    return inserted


def upsert_ripe_atlas_probes(db_path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    """Insert new rows; overwrite existing by probe id."""
    con = duckdb.connect(str(db_path))
    inserted = 0
    try:
        for r in rows:
            con.execute(
                "DELETE FROM ripe_atlas_probes WHERE source = ? AND probe_id = ?",
                [r["source"], r["probe_id"]],
            )
            con.execute(
                """
                INSERT INTO ripe_atlas_probes VALUES
                  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    r["source"],
                    r["feed_url"],
                    r["probe_id"],
                    r.get("country_code"),
                    r.get("status"),
                    r.get("asn_v4"),
                    r.get("asn_v6"),
                    r.get("latitude"),
                    r.get("longitude"),
                    r.get("first_connected"),
                    r.get("last_connected"),
                    r.get("raw_json"),
                    r["ingested_at"],
                ],
            )
            inserted += 1
    finally:
        con.close()
    return inserted


def upsert_signals(db_path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    """Insert new rows; overwrite existing (signal_id) by delete+insert.

    Returns count of inserted/updated rows.
    """
    con = duckdb.connect(str(db_path))
    inserted = 0
    try:
        for r in rows:
            con.execute("DELETE FROM signals WHERE signal_id = ?", [r["signal_id"]])
            con.execute(
                """
                INSERT INTO signals VALUES
                  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    r["signal_id"],
                    r["signal_type"],
                    r["source"],
                    r.get("entity_type"),
                    r.get("entity_id"),
                    r.get("ts_start"),
                    r.get("ts_end"),
                    r.get("severity_score"),
                    r.get("summary"),
                    r.get("details_json"),
                    r.get("ingested_at"),
                    r.get("computed_at"),
                    r.get("raw_paths"),
                ],
            )
            inserted += 1
    finally:
        con.close()
    return inserted

def upsert_fred_series_observations(db_path: Path, rows: list[dict]) -> int:
    """Upsert FRED series observations keyed by source, series, date, and realtime window."""
    con = duckdb.connect(str(db_path))
    inserted = 0
    try:
        for r in rows:
            con.execute(
                """
                DELETE FROM fred_series_observations
                WHERE source = ? AND series_id = ?
                  AND observation_date IS NOT DISTINCT FROM ?
                  AND realtime_start IS NOT DISTINCT FROM ?
                  AND realtime_end IS NOT DISTINCT FROM ?
                """
                ,
                [
                    r["source"],
                    r["series_id"],
                    r.get("observation_date"),
                    r.get("realtime_start"),
                    r.get("realtime_end"),
                ],
            )
            con.execute(
                """
                INSERT INTO fred_series_observations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                ,
                [
                    r["source"],
                    r["series_id"],
                    r.get("observation_date"),
                    r.get("value"),
                    r.get("realtime_start"),
                    r.get("realtime_end"),
                    r.get("units"),
                    r.get("ingested_at"),
                    r.get("raw_path"),
                ],
            )
            inserted += 1
    finally:
        con.close()
    return inserted


def upsert_eia_v2_observations(db_path: Path, rows: list[dict]) -> int:
    """Upsert EIA v2 data rows keyed by source, route, frequency, series_id, and period."""
    con = duckdb.connect(str(db_path))
    inserted = 0
    try:
        for r in rows:
            con.execute(
                """
                DELETE FROM eia_v2_observations
                WHERE source = ? AND route = ? AND frequency = ? AND series_id = ? AND period = ?
                """
                ,
                [
                    r["source"],
                    r["route"],
                    r["frequency"],
                    r["series_id"],
                    r["period"],
                ],
            )
            con.execute(
                """
                INSERT INTO eia_v2_observations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                ,
                [
                    r["source"],
                    r["route"],
                    r["frequency"],
                    r["series_id"],
                    r["period"],
                    r.get("value"),
                    r.get("units"),
                    r.get("ingested_at"),
                    r.get("raw_path"),
                ],
            )
            inserted += 1
    finally:
        con.close()
    return inserted


def upsert_alternative_me_fng(db_path: Path, rows: list[dict]) -> int:
    """Upsert Alternative.me Fear & Greed rows keyed by source and observation_date."""
    con = duckdb.connect(str(db_path))
    inserted = 0
    try:
        for r in rows:
            con.execute(
                """
                DELETE FROM alternative_me_fng
                WHERE source = ? AND observation_date IS NOT DISTINCT FROM ?
                """,
                [r["source"], r.get("observation_date")],
            )
            con.execute(
                """
                INSERT INTO alternative_me_fng VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    r["source"],
                    r.get("observation_date"),
                    r.get("ts_utc"),
                    r.get("value"),
                    r.get("classification"),
                    r.get("ingested_at"),
                    r.get("raw_path"),
                ],
            )
            inserted += 1
    finally:
        con.close()
    return inserted


def upsert_cnn_fear_greed(db_path: Path, rows: list[dict]) -> int:
    """Upsert CNN Fear & Greed rows keyed by source, component, and ts_utc."""
    con = duckdb.connect(str(db_path))
    inserted = 0
    try:
        for r in rows:
            con.execute(
                """
                DELETE FROM cnn_fear_greed
                WHERE source = ? AND component = ? AND ts_utc IS NOT DISTINCT FROM ?
                """,
                [r["source"], r["component"], r.get("ts_utc")],
            )
            con.execute(
                """
                INSERT INTO cnn_fear_greed VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    r["source"],
                    r["component"],
                    r.get("observation_date"),
                    r.get("ts_utc"),
                    r.get("score"),
                    r.get("rating"),
                    r.get("ingested_at"),
                    r.get("raw_path"),
                ],
            )
            inserted += 1
    finally:
        con.close()
    return inserted


def upsert_gdelt_gkg(db_path: Path, rows: list[dict]) -> int:
    """Upsert GDELT GKG rows keyed by (source, gkg_record_id)."""
    con = duckdb.connect(str(db_path))
    inserted = 0
    try:
        for r in rows:
            con.execute(
                "DELETE FROM gdelt_gkg WHERE source = ? AND gkg_record_id = ?",
                [r["source"], r["gkg_record_id"]],
            )
            con.execute(
                """
                INSERT INTO gdelt_gkg VALUES
                  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    r["source"],
                    r.get("feed_url"),
                    r["gkg_record_id"],
                    r.get("v21_date"),
                    r.get("source_collection"),
                    r.get("source_common_name"),
                    r.get("document_url"),
                    r.get("v2_themes"),
                    r.get("v2_tone"),
                    r.get("v2_tone_avg"),
                    r.get("v2_tone_pos"),
                    r.get("v2_tone_neg"),
                    r.get("v2_tone_polarity"),
                    r.get("v2_word_count"),
                    r.get("raw_row_json"),
                    r.get("ingested_at"),
                ],
            )
            inserted += 1
    finally:
        con.close()
    return inserted


def upsert_bls_timeseries_observations(db_path: Path, rows: list[dict]) -> int:
    """Upsert BLS timeseries rows keyed by source, series_id, year, and period."""
    con = duckdb.connect(str(db_path))
    inserted = 0
    try:
        for r in rows:
            con.execute(
                """
                DELETE FROM bls_timeseries_observations
                WHERE source = ? AND series_id = ? AND year = ? AND period = ?
                """,
                [r["source"], r["series_id"], r["year"], r["period"]],
            )
            con.execute(
                """
                INSERT INTO bls_timeseries_observations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    r["source"],
                    r["series_id"],
                    r["year"],
                    r["period"],
                    r.get("period_name"),
                    r.get("value"),
                    r.get("footnotes"),
                    r.get("ingested_at"),
                    r.get("raw_path"),
                ],
            )
            inserted += 1
    finally:
        con.close()
    return inserted


def upsert_census_observations(db_path: Path, rows: list[dict]) -> int:
    """Upsert Census ACS rows keyed by source, dataset, year, geo, and variable."""
    con = duckdb.connect(str(db_path))
    inserted = 0
    try:
        for r in rows:
            con.execute(
                """
                DELETE FROM census_observations
                WHERE source = ? AND dataset = ? AND year = ?
                  AND geo_type = ? AND geo_id = ? AND variable = ?
                """,
                [
                    r["source"],
                    r["dataset"],
                    r["year"],
                    r["geo_type"],
                    r["geo_id"],
                    r["variable"],
                ],
            )
            con.execute(
                """
                INSERT INTO census_observations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    r["source"],
                    r["dataset"],
                    r["year"],
                    r["geo_type"],
                    r["geo_id"],
                    r["variable"],
                    r.get("value"),
                    r.get("label"),
                    r.get("ingested_at"),
                    r.get("raw_path"),
                ],
            )
            inserted += 1
    finally:
        con.close()
    return inserted


def log_run(
    db_path: Path,
    *,
    run_id: str,
    source: str,
    feed_url: str,
    started_at: datetime,
    ended_at: datetime,
    status: str,
    n_total: int,
    n_new: int,
    message: str,
) -> None:
    con = duckdb.connect(str(db_path))
    try:
        con.execute(
            """
            INSERT INTO ingest_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [run_id, source, feed_url, started_at, ended_at, status, n_total, n_new, message],
        )
    finally:
        con.close()


def show_latest(*, db_path: Path, limit: int = 10) -> None:
    con = duckdb.connect(str(db_path))
    try:
        rows = con.execute(
            """
            SELECT
              time_utc,
              magnitude,
              place,
              url
            FROM usgs_earthquakes
            ORDER BY time_utc DESC NULLS LAST
            LIMIT ?
            """,
            [limit],
        ).fetchall()

        if not rows:
            print("No rows found yet. Run: hoover ingest-usgs")
            return

        for (t, mag, place, url) in rows:
            print(f"{t} | M{mag} | {place} | {url}")
    finally:
        con.close()


def upsert_twelvedata_time_series(db_path: Path, rows: list[dict]) -> int:
    """Upsert Twelve Data time series records (idempotent on source+symbol+interval+series_group+ts)."""
    con = duckdb.connect(str(db_path))
    inserted = 0
    try:
        for r in rows:
            existing = con.execute(
                """
                SELECT COUNT(*) FROM twelvedata_time_series
                WHERE source = ? AND symbol = ? AND interval = ? AND series_group = ? AND ts = ?
                """
                ,
                [r["source"], r["symbol"], r["interval"], r.get("series_group"), r["ts"]],
            ).fetchone()
            if existing and existing[0] > 0:
                continue
            con.execute(
                """
                INSERT INTO twelvedata_time_series
                  (source, symbol, interval, series_group, ts, open, high, low,
                   close, volume, currency, exchange, ingested_at, raw_path)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                ,
                [
                    r["source"],
                    r["symbol"],
                    r["interval"],
                    r.get("series_group"),
                    r["ts"],
                    r["open"],
                    r["high"],
                    r["low"],
                    r["close"],
                    r["volume"],
                    r["currency"],
                    r["exchange"],
                    r["ingested_at"],
                    r["raw_path"],
                ],
            )
            inserted += 1
    finally:
        con.close()
    return inserted
