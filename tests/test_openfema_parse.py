from datetime import datetime, timezone
import json
from pathlib import Path

from datahoover.connectors.openfema_disaster_declarations import _normalize_records
from datahoover.sources import Source


def _load_fixture() -> dict:
    path = Path("tests/fixtures/openfema_disaster_declarations.json")
    return json.loads(path.read_text(encoding="utf-8"))


def test_openfema_normalization_schema_and_rows():
    data = _load_fixture()
    source = Source(
        name="openfema_disaster_declarations",
        kind="openfema_disaster_declarations",
        url="https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries",
    )
    ingested_at = datetime(2026, 1, 29, tzinfo=timezone.utc)
    rows = _normalize_records(source, data["DisasterDeclarationsSummaries"], ingested_at)

    assert rows, "Expected at least one normalized row"
    row = rows[0]
    assert set(row.keys()) == {
        "source",
        "declaration_id",
        "disaster_number",
        "state",
        "declaration_type",
        "declaration_date",
        "incident_type",
        "declaration_title",
        "incident_begin_date",
        "incident_end_date",
        "raw_json",
        "ingested_at",
    }


def test_openfema_primary_key_stability():
    data = _load_fixture()
    source = Source(
        name="openfema_disaster_declarations",
        kind="openfema_disaster_declarations",
        url="https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries",
    )
    ingested_at = datetime(2026, 1, 29, tzinfo=timezone.utc)

    rows_a = _normalize_records(source, data["DisasterDeclarationsSummaries"], ingested_at)
    rows_b = _normalize_records(source, data["DisasterDeclarationsSummaries"], ingested_at)

    key_a = (rows_a[0]["declaration_id"], rows_a[0]["declaration_date"])
    key_b = (rows_b[0]["declaration_id"], rows_b[0]["declaration_date"])
    assert key_a == key_b
