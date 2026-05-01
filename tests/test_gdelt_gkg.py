from __future__ import annotations

import io
import json
import zipfile
from datetime import datetime, timezone

import duckdb
import pytest

from datahoover.connectors import gdelt_gkg as gkg
from datahoover.sources import Source
from datahoover.storage.duckdb_store import init_db, upsert_gdelt_gkg


SAMPLE_LASTUPDATE = (
    "12345 abc123 http://data.gdeltproject.org/gdeltv2/20260501120000.export.CSV.zip\n"
    "67890 def456 http://data.gdeltproject.org/gdeltv2/20260501120000.mentions.CSV.zip\n"
    "11111 ghi789 http://data.gdeltproject.org/gdeltv2/20260501120000.gkg.csv.zip\n"
)


def _gkg_row(record_id: str, *, tone_csv: str = "-3.5,4.2,7.7,11.9,1.2,0.8,420") -> str:
    """Build a tab-delimited GKG CSV row with the right column count."""
    cells = [
        record_id,
        "20260501120000",  # V21DATE
        "1",  # V2SOURCECOLLECTIONIDENTIFIER
        "cnbc.com",  # V2SOURCECOMMONNAME
        "https://example.test/article",  # V2DOCUMENTIDENTIFIER
        "",  # V1COUNTS
        "",  # V21COUNTS
        "DEMOCRACY;ECON_*",  # V1THEMES
        "ECON_TAXATION,123;MOOD_ANGER,456",  # V2ENHANCEDTHEMES
        "",  # V1LOCATIONS
        "",  # V2ENHANCEDLOCATIONS
        "",  # V1PERSONS
        "",  # V2ENHANCEDPERSONS
        "",  # V1ORGANIZATIONS
        "",  # V2ENHANCEDORGANIZATIONS
        tone_csv,  # V2TONE
        "",  # V21ENHANCEDDATES
        "",  # V2GCAM
        "",  # V21SHARINGIMAGE
        "",  # V21RELATEDIMAGES
        "",  # V21SOCIALIMAGEEMBEDS
        "",  # V21SOCIALVIDEOEMBEDS
        "",  # V21QUOTATIONS
        "",  # V21ALLNAMES
        "",  # V21AMOUNTS
        "",  # V21TRANSLATIONINFO
        "",  # V2EXTRASXML
    ]
    return "\t".join(cells)


def _source() -> Source:
    return Source(
        name="gdelt_gkg_15min",
        kind="gdelt_gkg",
        url="http://data.gdeltproject.org/gdeltv2/lastupdate.txt",
    )


def test_parse_lastupdate_picks_gkg_url():
    url = gkg.parse_lastupdate(SAMPLE_LASTUPDATE)
    assert url == "http://data.gdeltproject.org/gdeltv2/20260501120000.gkg.csv.zip"


def test_parse_lastupdate_returns_none_when_no_gkg_line():
    body = "12345 abc123 http://data.gdeltproject.org/gdeltv2/20260501120000.export.CSV.zip\n"
    assert gkg.parse_lastupdate(body) is None


def test_parse_tone_csv_extracts_first_three_floats():
    parsed = gkg._parse_tone_csv("-3.5,4.2,7.7,11.9,1.2,0.8,420")
    assert parsed["avg"] == pytest.approx(-3.5)
    assert parsed["pos"] == pytest.approx(4.2)
    assert parsed["neg"] == pytest.approx(7.7)
    assert parsed["polarity"] == pytest.approx(11.9)
    assert parsed["word_count"] == pytest.approx(420.0)


def test_parse_tone_csv_handles_empty_and_malformed():
    assert gkg._parse_tone_csv("")["avg"] is None
    bad = gkg._parse_tone_csv("not,a,number,here,,,")
    assert bad["avg"] is None
    assert bad["word_count"] is None


def test_parse_v21_date_round_trips_utc():
    ts = gkg._parse_v21_date("20260501120000")
    assert ts == datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)


def test_parse_v21_date_returns_none_on_garbage():
    assert gkg._parse_v21_date("") is None
    assert gkg._parse_v21_date("not-a-date") is None


def test_normalize_csv_rows_extracts_expected_fields():
    csv_text = "\n".join(
        [
            _gkg_row("20260501120000-0"),
            _gkg_row("20260501120000-1", tone_csv=""),  # missing tone tolerated
        ]
    )
    at = datetime(2026, 5, 1, 12, 30, tzinfo=timezone.utc)
    rows = gkg._normalize_csv_rows(_source(), csv_text, ingested_at=at, max_records=None)
    assert len(rows) == 2
    r0 = rows[0]
    assert r0["gkg_record_id"] == "20260501120000-0"
    assert r0["v21_date"] == datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
    assert r0["source_common_name"] == "cnbc.com"
    assert r0["v2_tone_avg"] == pytest.approx(-3.5)
    assert r0["v2_word_count"] == 420
    assert "MOOD_ANGER" in r0["v2_themes"]
    # raw_row_json is a parseable JSON dict.
    parsed = json.loads(r0["raw_row_json"])
    assert parsed["GKGRECORDID"] == "20260501120000-0"

    # Empty-tone row is included but with None tone fields.
    r1 = rows[1]
    assert r1["v2_tone_avg"] is None
    assert r1["v2_word_count"] is None


def test_normalize_csv_rows_respects_max_records():
    csv_text = "\n".join(_gkg_row(f"id-{i}") for i in range(10))
    at = datetime(2026, 5, 1, tzinfo=timezone.utc)
    rows = gkg._normalize_csv_rows(_source(), csv_text, ingested_at=at, max_records=3)
    assert len(rows) == 3


def test_normalize_csv_rows_skips_blank_record_id():
    csv_text = _gkg_row("")  # empty GKGRECORDID
    at = datetime(2026, 5, 1, tzinfo=timezone.utc)
    rows = gkg._normalize_csv_rows(_source(), csv_text, ingested_at=at, max_records=None)
    assert rows == []


def test_normalize_csv_rows_pads_short_rows():
    short_row = "\t".join(["id-1", "20260501120000", "1", "x.com"])  # only 4 cells
    at = datetime(2026, 5, 1, tzinfo=timezone.utc)
    rows = gkg._normalize_csv_rows(_source(), short_row, ingested_at=at, max_records=None)
    assert len(rows) == 1
    assert rows[0]["gkg_record_id"] == "id-1"


def test_extract_csv_from_zip_round_trip():
    payload = b"hello\tworld\n"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("20260501120000.gkg.csv", payload)
    text = gkg._extract_csv_from_zip(buf.getvalue())
    assert text == payload.decode("utf-8")


def test_extract_csv_from_zip_rejects_zip_without_csv():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("readme.txt", b"no csv here")
    with pytest.raises(ValueError, match="did not contain a .csv member"):
        gkg._extract_csv_from_zip(buf.getvalue())


def test_upsert_gdelt_gkg_idempotent(tmp_path):
    db_path = tmp_path / "warehouse.duckdb"
    init_db(db_path)
    at = datetime(2026, 5, 1, tzinfo=timezone.utc)
    rows = [
        {
            "source": "gdelt_gkg_15min",
            "feed_url": "http://example.test",
            "gkg_record_id": "20260501120000-0",
            "v21_date": datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc),
            "source_collection": "1",
            "source_common_name": "cnbc.com",
            "document_url": "https://example.test/article",
            "v2_themes": "ECON_TAXATION",
            "v2_tone": "-3.5,4.2,7.7,11.9,1.2,0.8,420",
            "v2_tone_avg": -3.5,
            "v2_tone_pos": 4.2,
            "v2_tone_neg": 7.7,
            "v2_tone_polarity": 11.9,
            "v2_word_count": 420,
            "raw_row_json": "{}",
            "ingested_at": at,
        },
    ]
    assert upsert_gdelt_gkg(db_path, rows) == 1
    assert upsert_gdelt_gkg(db_path, rows) == 1
    con = duckdb.connect(str(db_path))
    try:
        n = con.execute("SELECT COUNT(*) FROM gdelt_gkg").fetchone()[0]
        assert n == 1
    finally:
        con.close()
