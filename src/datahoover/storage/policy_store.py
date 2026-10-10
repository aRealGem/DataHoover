"""DuckDB tables for the policy sources added by DH-PULLS-001.

Kept out of duckdb_store.init_db on purpose: these tables are created only when
a policy connector runs, so the weekly pipeline's existing schema is untouched
until a policy step is actually scheduled.

Pattern per source:
  <x>            current row per identity (upsert); first_seen_at never moves
  <x>_versions   one row per distinct content hash -- the revision history
  raw_responses  ledger of every raw file written: sha256, bytes, query, time
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List

import duckdb

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS raw_responses (
        raw_sha256 VARCHAR,
        source VARCHAR,
        endpoint VARCHAR,
        query VARCHAR,
        fetched_at TIMESTAMP,
        http_status INTEGER,
        n_bytes BIGINT,
        raw_path VARCHAR,
        PRIMARY KEY (raw_sha256, raw_path)
    )
    """,
    # ---- Federal Register: public inspection ------------------------------
    """
    CREATE TABLE IF NOT EXISTS fr_public_inspection (
        document_number VARCHAR PRIMARY KEY,
        filed_at_raw VARCHAR,
        filed_at_utc TIMESTAMP,
        filed_at_tz_offset VARCHAR,
        filed_at_precision VARCHAR,
        pdf_updated_at_raw VARCHAR,
        pdf_updated_at_utc TIMESTAMP,
        pdf_updated_at_tz_offset VARCHAR,
        pdf_updated_at_precision VARCHAR,
        publication_date DATE,
        last_public_inspection_issue DATE,
        filing_type VARCHAR,
        doc_type VARCHAR,
        title VARCHAR,
        agency_names VARCHAR,
        docket_numbers VARCHAR,
        editorial_note VARCHAR,
        num_pages INTEGER,
        pdf_url VARCHAR,
        html_url VARCHAR,
        content_sha256 VARCHAR,
        n_versions INTEGER,
        first_seen_at TIMESTAMP,
        last_seen_at TIMESTAMP,
        raw_sha256 VARCHAR,
        raw_path VARCHAR,
        raw_json VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fr_public_inspection_versions (
        document_number VARCHAR,
        content_sha256 VARCHAR,
        pdf_updated_at_raw VARCHAR,
        first_seen_at TIMESTAMP,
        raw_sha256 VARCHAR,
        raw_path VARCHAR,
        raw_json VARCHAR,
        PRIMARY KEY (document_number, content_sha256)
    )
    """,
    # ---- Federal Register: published documents ----------------------------
    """
    CREATE TABLE IF NOT EXISTS fr_documents (
        document_number VARCHAR PRIMARY KEY,
        publication_date DATE,
        effective_on DATE,
        effective_on_raw VARCHAR,
        signing_date DATE,
        comments_close_on DATE,
        dates_text VARCHAR,
        doc_type VARCHAR,
        subtype VARCHAR,
        title VARCHAR,
        action VARCHAR,
        abstract VARCHAR,
        agency_names VARCHAR,
        citation VARCHAR,
        start_page INTEGER,
        end_page INTEGER,
        correction_of VARCHAR,
        corrections VARCHAR,
        docket_ids VARCHAR,
        regulation_id_numbers VARCHAR,
        cfr_references VARCHAR,
        significant BOOLEAN,
        executive_order_number VARCHAR,
        presidential_document_number VARCHAR,
        disposition_notes VARCHAR,
        not_received_for_publication VARCHAR,
        pdf_url VARCHAR,
        public_inspection_pdf_url VARCHAR,
        html_url VARCHAR,
        json_url VARCHAR,
        content_sha256 VARCHAR,
        n_versions INTEGER,
        first_seen_at TIMESTAMP,
        last_seen_at TIMESTAMP,
        raw_sha256 VARCHAR,
        raw_path VARCHAR,
        raw_json VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fr_documents_versions (
        document_number VARCHAR,
        content_sha256 VARCHAR,
        first_seen_at TIMESTAMP,
        raw_sha256 VARCHAR,
        raw_path VARCHAR,
        raw_json VARCHAR,
        PRIMARY KEY (document_number, content_sha256)
    )
    """,
    # PI <-> published linkage by document identity. Each clock stays its own column.
    """
    CREATE OR REPLACE VIEW fr_document_timeline AS
    SELECT
        COALESCE(d.document_number, p.document_number) AS document_number,
        p.filed_at_utc            AS pi_filed_at_utc,
        p.filed_at_raw            AS pi_filed_at_raw,
        p.pdf_updated_at_utc      AS pi_pdf_updated_at_utc,
        p.publication_date        AS pi_scheduled_publication_date,
        d.publication_date        AS publication_date,
        d.effective_on            AS effective_on,
        d.correction_of           AS correction_of,
        d.corrections             AS corrections,
        LEAST(p.first_seen_at, d.first_seen_at) AS local_first_seen_at,
        p.document_number IS NOT NULL AS seen_in_public_inspection,
        d.document_number IS NOT NULL AS seen_published
    FROM fr_documents d
    FULL OUTER JOIN fr_public_inspection p USING (document_number)
    """,
    # ---- USITC HTS --------------------------------------------------------
    # Edition metadata as USITC's archive page states it. Two different dates
    # appear there and neither is a legal effective date, so both are kept raw
    # and effective_date stays NULL unless USITC itself states one.
    """
    CREATE TABLE IF NOT EXISTS hts_editions (
        edition_name VARCHAR PRIMARY KEY,
        edition_label VARCHAR,
        archive_published_date_raw VARCHAR,
        archive_published_date DATE,
        release_date_raw VARCHAR,
        release_date DATE,
        effective_date DATE,
        effective_date_status VARCHAR,
        modification_sources VARCHAR,
        json_url VARCHAR,
        current_snapshot_sha256 VARCHAR,
        first_seen_at TIMESTAMP,
        last_seen_at TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS hts_snapshots (
        snapshot_sha256 VARCHAR PRIMARY KEY,
        edition_name VARCHAR,
        n_bytes BIGINT,
        n_lines INTEGER,
        http_last_modified_raw VARCHAR,
        raw_path VARCHAR,
        first_seen_at TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS hts_lines (
        snapshot_sha256 VARCHAR,
        line_no INTEGER,
        line_key VARCHAR,
        htsno VARCHAR,
        indent INTEGER,
        description VARCHAR,
        units VARCHAR,
        general_rate VARCHAR,
        special_rate VARCHAR,
        other_rate VARCHAR,
        footnotes VARCHAR,
        additional_duties VARCHAR,
        quota_quantity VARCHAR,
        line_sha256 VARCHAR,
        PRIMARY KEY (snapshot_sha256, line_no)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS hts_edition_diffs (
        from_snapshot_sha256 VARCHAR,
        to_snapshot_sha256 VARCHAR,
        from_edition VARCHAR,
        to_edition VARCHAR,
        line_key VARCHAR,
        change_type VARCHAR,
        htsno VARCHAR,
        changed_fields VARCHAR,
        before_json VARCHAR,
        after_json VARCHAR,
        computed_at TIMESTAMP,
        PRIMARY KEY (from_snapshot_sha256, to_snapshot_sha256, line_key)
    )
    """,
]


def init_policy_tables(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    try:
        for stmt in _DDL:
            con.execute(stmt)
    finally:
        con.close()


def record_raw_response(db_path: Path, row: Dict[str, Any]) -> None:
    con = duckdb.connect(str(db_path))
    try:
        con.execute(
            "INSERT OR IGNORE INTO raw_responses VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [row["raw_sha256"], row["source"], row["endpoint"], row["query"], row["fetched_at"],
             row["http_status"], row["n_bytes"], row["raw_path"]],
        )
    finally:
        con.close()


def _upsert_versioned(
    db_path: Path,
    table: str,
    versions_table: str,
    key: str,
    rows: Iterable[Dict[str, Any]],
    version_cols: List[str],
) -> Dict[str, int]:
    """Upsert current rows, keep first_seen_at, append a version row per new hash.

    Returns counts: inserted (new identity), revised (known identity, new hash),
    unchanged (same hash seen again).
    """
    counts = {"inserted": 0, "revised": 0, "unchanged": 0}
    con = duckdb.connect(str(db_path))
    try:
        cols = [c[0] for c in con.execute(f"DESCRIBE {table}").fetchall()]
        for row in rows:
            ident = row[key]
            prev = con.execute(
                f"SELECT content_sha256, first_seen_at, n_versions FROM {table} WHERE {key} = ?", [ident]
            ).fetchone()
            vrow = {c: row.get(c) for c in version_cols}
            vrow["first_seen_at"] = row["last_seen_at"]
            con.execute(
                f"INSERT OR IGNORE INTO {versions_table} ({', '.join(vrow)}) VALUES ({', '.join('?' for _ in vrow)})",
                list(vrow.values()),
            )
            if prev is None:
                row = {**row, "first_seen_at": row["last_seen_at"], "n_versions": 1}
                counts["inserted"] += 1
            elif prev[0] == row["content_sha256"]:
                row = {**row, "first_seen_at": prev[1], "n_versions": prev[2]}
                counts["unchanged"] += 1
            else:
                n = con.execute(
                    f"SELECT COUNT(*) FROM {versions_table} WHERE {key} = ?", [ident]
                ).fetchone()[0]
                row = {**row, "first_seen_at": prev[1], "n_versions": n}
                counts["revised"] += 1
            values = [row.get(c) for c in cols]
            con.execute(
                f"INSERT OR REPLACE INTO {table} ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})",
                values,
            )
    finally:
        con.close()
    return counts


def upsert_fr_public_inspection(db_path: Path, rows: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    return _upsert_versioned(
        db_path, "fr_public_inspection", "fr_public_inspection_versions", "document_number", rows,
        ["document_number", "content_sha256", "pdf_updated_at_raw", "raw_sha256", "raw_path", "raw_json"],
    )


def upsert_fr_documents(db_path: Path, rows: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    return _upsert_versioned(
        db_path, "fr_documents", "fr_documents_versions", "document_number", rows,
        ["document_number", "content_sha256", "raw_sha256", "raw_path", "raw_json"],
    )
