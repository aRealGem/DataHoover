"""Connector for the GDELT 2.0 Global Knowledge Graph (GKG) feed.

GDELT publishes a 15-minute drop of three CSV-in-ZIP files (export, mentions,
gkg). The GKG file is the richest sentiment payload: per-article V2Tone (7
floats: tone / pos / neg / polarity / activity / self-ref / wordcount),
V2Themes (semicolon-delimited theme tags including MOOD_*, ECON_*, WB_*),
plus locations / persons / organisations.

The latest URL is advertised in `lastupdate.txt`, formatted as one
`<size> <md5> <url>` line per file. We pick the line whose URL ends in
`.gkg.csv.zip` and conditionally fetch only when its filename advances past
the last successfully ingested one (state file).

Volume: ~50-150 MB unzipped per drop, ~50-200k rows. Keeps the raw zip in
`data/raw/<source>/`.

License: GDELT 2.0 publishes under CC-BY-NC-SA 4.0 — non-commercial only.
This connector tags `gdelt_gkg_15min` accordingly in `sources.toml`; downstream
consumers that need a commercial-safe lane should filter the source out.
"""
from __future__ import annotations

import csv
import io
import json
import uuid
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import httpx

from ..sources import Source, load_sources
from ._retry import fetch_with_retry

DEFAULT_LASTUPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
USER_AGENT = "data-hoover/0.1 (+local-first; contact: you@example.com)"
HTTP_TIMEOUT_S = 60.0

# GKG 2.1 column order (tab-delimited). Index used so a future column shift
# would surface as a tuple length mismatch instead of silent miss-mapping.
GKG_COLUMNS: tuple[str, ...] = (
    "GKGRECORDID",
    "V21DATE",
    "V2SOURCECOLLECTIONIDENTIFIER",
    "V2SOURCECOMMONNAME",
    "V2DOCUMENTIDENTIFIER",
    "V1COUNTS",
    "V21COUNTS",
    "V1THEMES",
    "V2ENHANCEDTHEMES",
    "V1LOCATIONS",
    "V2ENHANCEDLOCATIONS",
    "V1PERSONS",
    "V2ENHANCEDPERSONS",
    "V1ORGANIZATIONS",
    "V2ENHANCEDORGANIZATIONS",
    "V2TONE",
    "V21ENHANCEDDATES",
    "V2GCAM",
    "V21SHARINGIMAGE",
    "V21RELATEDIMAGES",
    "V21SOCIALIMAGEEMBEDS",
    "V21SOCIALVIDEOEMBEDS",
    "V21QUOTATIONS",
    "V21ALLNAMES",
    "V21AMOUNTS",
    "V21TRANSLATIONINFO",
    "V2EXTRASXML",
)


@dataclass(frozen=True)
class FetchResult:
    status_code: int
    gkg_url: str
    zip_bytes: bytes


def _state_path(data_dir: Path, source_name: str) -> Path:
    return data_dir / "state" / f"{source_name}.json"


def _raw_path(data_dir: Path, source_name: str, gkg_filename: str) -> Path:
    return data_dir / "raw" / source_name / gkg_filename


def _load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _save_state(path: Path, state: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def parse_lastupdate(text: str) -> Optional[str]:
    """Return the GKG zip URL from the lastupdate.txt body, or None.

    Each line is `<size> <md5> <url>`. We pick the URL ending in `.gkg.csv.zip`.
    Returns None if no such line is present.
    """
    for line in text.splitlines():
        parts = line.strip().split()
        if len(parts) < 3:
            continue
        url = parts[-1]
        if url.endswith(".gkg.csv.zip"):
            return url
    return None


def fetch_gdelt_gkg_lastupdate(
    url: str,
    *,
    timeout_s: float = HTTP_TIMEOUT_S,
) -> str:
    headers = {"User-Agent": USER_AGENT, "Accept": "text/plain"}
    with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
        response = client.get(url, headers=headers)
    response.raise_for_status()
    gkg_url = parse_lastupdate(response.text)
    if not gkg_url:
        raise ValueError(f"GDELT lastupdate.txt did not include a .gkg.csv.zip URL: {response.text[:200]!r}")
    return gkg_url


def fetch_gdelt_gkg_zip(
    gkg_url: str,
    *,
    timeout_s: float = HTTP_TIMEOUT_S,
) -> FetchResult:
    headers = {"User-Agent": USER_AGENT}
    with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
        response = client.get(gkg_url, headers=headers)
    response.raise_for_status()
    return FetchResult(status_code=response.status_code, gkg_url=gkg_url, zip_bytes=response.content)


def _parse_v21_date(value: str) -> Optional[datetime]:
    """V2.1DATE is YYYYMMDDHHMMSS in UTC."""
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _parse_tone_csv(value: str) -> Dict[str, Optional[float]]:
    """Parse V2TONE's 7-tuple: tone, pos, neg, polarity, actref, selfref, wordcount.

    Returns a dict with keys avg, pos, neg, polarity, word_count. Missing /
    malformed values become None.
    """
    out: Dict[str, Optional[float]] = {
        "avg": None,
        "pos": None,
        "neg": None,
        "polarity": None,
        "word_count": None,
    }
    if not value:
        return out
    parts = value.split(",")
    def _f(idx: int) -> Optional[float]:
        if idx >= len(parts):
            return None
        try:
            return float(parts[idx])
        except (TypeError, ValueError):
            return None

    out["avg"] = _f(0)
    out["pos"] = _f(1)
    out["neg"] = _f(2)
    out["polarity"] = _f(3)
    wc = _f(6)
    out["word_count"] = wc
    return out


def _normalize_csv_rows(
    source: Source,
    csv_text: str,
    *,
    ingested_at: datetime,
    max_records: Optional[int],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    reader = csv.reader(io.StringIO(csv_text), delimiter="\t", quoting=csv.QUOTE_NONE)
    for raw in reader:
        if not raw:
            continue
        # Tolerate column-count drift: pad short rows, trim long ones.
        if len(raw) < len(GKG_COLUMNS):
            raw = list(raw) + [""] * (len(GKG_COLUMNS) - len(raw))
        elif len(raw) > len(GKG_COLUMNS):
            raw = raw[: len(GKG_COLUMNS)]
        record = dict(zip(GKG_COLUMNS, raw))
        gkg_id = record.get("GKGRECORDID") or ""
        if not gkg_id:
            continue
        tone = _parse_tone_csv(record.get("V2TONE", ""))
        wc_int = int(tone["word_count"]) if isinstance(tone["word_count"], float) else None
        rows.append(
            {
                "source": source.name,
                "feed_url": source.url,
                "gkg_record_id": gkg_id,
                "v21_date": _parse_v21_date(record.get("V21DATE", "")),
                "source_collection": record.get("V2SOURCECOLLECTIONIDENTIFIER") or None,
                "source_common_name": record.get("V2SOURCECOMMONNAME") or None,
                "document_url": record.get("V2DOCUMENTIDENTIFIER") or None,
                "v2_themes": record.get("V2ENHANCEDTHEMES") or record.get("V1THEMES") or None,
                "v2_tone": record.get("V2TONE") or None,
                "v2_tone_avg": tone["avg"],
                "v2_tone_pos": tone["pos"],
                "v2_tone_neg": tone["neg"],
                "v2_tone_polarity": tone["polarity"],
                "v2_word_count": wc_int,
                "raw_row_json": json.dumps(record, separators=(",", ":"), ensure_ascii=False),
                "ingested_at": ingested_at,
            }
        )
        if max_records is not None and len(rows) >= max_records:
            break
    return rows


def _extract_csv_from_zip(zip_bytes: bytes) -> str:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = [n for n in zf.namelist() if n.endswith(".csv") or n.endswith(".CSV")]
        if not names:
            raise ValueError("GDELT GKG zip did not contain a .csv member")
        with zf.open(names[0]) as fh:
            data = fh.read()
    # GDELT files are Latin-1 / UTF-8 mixed; decode permissively.
    return data.decode("utf-8", errors="replace")


def ingest_gdelt_gkg(
    *, config_path: Path, source_name: str, data_dir: Path, db_path: Path
) -> None:
    from ..storage.duckdb_store import init_db, log_run, upsert_gdelt_gkg

    sources = load_sources(config_path)
    if source_name not in sources:
        raise SystemExit(
            f"Unknown source '{source_name}'. Available: {', '.join(sorted(sources.keys()))}"
        )
    source = sources[source_name]
    extra = source.extra or {}
    max_records = extra.get("max_records")
    if max_records is not None:
        max_records = int(max_records)
    feed_url = source.url or DEFAULT_LASTUPDATE_URL

    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "raw" / source.name).mkdir(parents=True, exist_ok=True)
    (data_dir / "state").mkdir(parents=True, exist_ok=True)

    state_file = _state_path(data_dir, source.name)
    state = _load_state(state_file)

    started_at = datetime.now(timezone.utc)
    run_id = str(uuid.uuid4())

    try:
        gkg_url = fetch_with_retry(lambda: fetch_gdelt_gkg_lastupdate(feed_url))
        gkg_filename = gkg_url.rsplit("/", 1)[-1]

        if state.get("last_gkg_filename") == gkg_filename:
            init_db(db_path)
            log_run(
                db_path,
                run_id=run_id,
                source=source.name,
                feed_url=feed_url,
                started_at=started_at,
                ended_at=datetime.now(timezone.utc),
                status="no_change",
                n_total=0,
                n_new=0,
                message=f"already-ingested gkg={gkg_filename}",
            )
            print(f"[{source.name}] No change (already ingested {gkg_filename}).")
            return

        result = fetch_with_retry(lambda: fetch_gdelt_gkg_zip(gkg_url))

        ingested_at = datetime.now(timezone.utc)
        raw_path = _raw_path(data_dir, source.name, gkg_filename)
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_bytes(result.zip_bytes)

        csv_text = _extract_csv_from_zip(result.zip_bytes)
        rows = _normalize_csv_rows(source, csv_text, ingested_at=ingested_at, max_records=max_records)

        init_db(db_path)
        n_new = upsert_gdelt_gkg(db_path, rows)

        state.update(
            {
                "last_gkg_filename": gkg_filename,
                "last_gkg_url": gkg_url,
                "last_success_at": ingested_at.isoformat(),
                "last_raw_path": str(raw_path),
            }
        )
        _save_state(state_file, state)

        log_run(
            db_path,
            run_id=run_id,
            source=source.name,
            feed_url=feed_url,
            started_at=started_at,
            ended_at=datetime.now(timezone.utc),
            status="ok",
            n_total=len(rows),
            n_new=n_new,
            message=f"stored raw={raw_path.name}",
        )
        print(
            f"[{source.name}] fetched={len(rows)} inserted_or_updated={n_new} raw={raw_path}"
        )
    except Exception as exc:
        try:
            init_db(db_path)
            log_run(
                db_path,
                run_id=run_id,
                source=source.name,
                feed_url=feed_url,
                started_at=started_at,
                ended_at=datetime.now(timezone.utc),
                status="error",
                n_total=0,
                n_new=0,
                message=str(exc),
            )
        except Exception:
            pass
        raise
