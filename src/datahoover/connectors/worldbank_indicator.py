from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, List, Tuple
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import httpx

from ..sources import load_sources, Source
from ._retry import fetch_with_retry


@dataclass(frozen=True)
class FetchResult:
    status_code: int
    etag: Optional[str]
    last_modified: Optional[str]
    data: Optional[List[Any]]


MACRO_FISCAL_INDICATORS = [
    "NY.GDP.MKTP.CN",
    "NY.GDP.MKTP.PP.CD",
    "PA.NUS.PPP",
    "GC.DOD.TOTL.GD.ZS",
    "GC.NLD.TOTL.GD.ZS",
    "GC.REV.XGRT.GD.ZS",
    "GC.REV.XGRT.CN",
    "GC.XPN.INTP.CN",
]


def _state_path(data_dir: Path, source_name: str) -> Path:
    return data_dir / "state" / f"{source_name}.json"


def _raw_path(data_dir: Path, source_name: str, ts: datetime) -> Path:
    safe_ts = ts.strftime("%Y-%m-%dT%H-%M-%SZ")
    return data_dir / "raw" / source_name / f"{safe_ts}.json"


def _load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _save_state(path: Path, state: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def _with_page(url: str, page: int) -> str:
    parts = urlparse(url)
    qs = parse_qs(parts.query)
    qs["page"] = [str(page)]
    query = urlencode(qs, doseq=True)
    return urlunparse(parts._replace(query=query))


def build_multi_indicator_url(
    *,
    country: str,
    indicators: List[str],
    per_page: int = 20000,
    source_id: int = 2,
    format: str = "json",
) -> str:
    if not indicators:
        raise ValueError("Indicators list cannot be empty")
    indicator_list = ";".join(indicators)
    base = f"https://api.worldbank.org/v2/country/{country}/indicator/{indicator_list}"
    query = urlencode({"format": format, "per_page": per_page, "source": source_id})
    return f"{base}?{query}"


def _parse_worldbank_response(payload: Any) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    if not isinstance(payload, list) or len(payload) < 2:
        raise ValueError("Unexpected World Bank response shape")
    meta = payload[0] or {}
    rows = payload[1] or []
    if not isinstance(meta, dict) or not isinstance(rows, list):
        raise ValueError("Unexpected World Bank response shape")
    return meta, rows


def fetch_worldbank_json(
    url: str,
    *,
    page: int = 1,
    etag: str | None = None,
    last_modified: str | None = None,
    timeout_s: float = 60.0,
) -> FetchResult:
    headers: Dict[str, str] = {
        "User-Agent": "data-hoover/0.1 (+local-first; contact: you@example.com)"
    }
    if page == 1:
        if etag:
            headers["If-None-Match"] = etag
        if last_modified and not etag:
            headers["If-Modified-Since"] = last_modified

    page_url = _with_page(url, page)
    with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
        r = client.get(page_url, headers=headers)

    if r.status_code == 304:
        return FetchResult(status_code=304, etag=etag, last_modified=last_modified, data=None)

    r.raise_for_status()
    new_etag = r.headers.get("ETag") if page == 1 else etag
    new_last_modified = r.headers.get("Last-Modified") if page == 1 else last_modified
    data = r.json()
    return FetchResult(status_code=r.status_code, etag=new_etag, last_modified=new_last_modified, data=data)


def _normalize_entries(
    source: Source, entries: List[Dict[str, Any]], ingested_at: datetime
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for entry in entries:
        indicator = entry.get("indicator") or {}
        country = entry.get("country") or {}
        series_id = indicator.get("id") or "unknown"
        country_id = country.get("id") or entry.get("countryiso3code") or "unknown"
        year = entry.get("date")
        rows.append(
            {
                "source": source.name,
                "feed_url": source.url,
                "series_id": series_id,
                "country_id": country_id,
                "country_name": country.get("value"),
                "year": year,
                "value": entry.get("value"),
                "unit": entry.get("unit"),
                "raw_json": json.dumps(entry, separators=(",", ":"), ensure_ascii=False),
                "ingested_at": ingested_at,
            }
        )
    return rows


def _compute_interest_payments_pct_gdp(interest_lcu: float | None, gdp_lcu: float | None) -> float | None:
    if interest_lcu is None or gdp_lcu in (None, 0):
        return None
    return 100.0 * (float(interest_lcu) / float(gdp_lcu))


def ingest_worldbank_indicator(
    *, config_path: Path, source_name: str, data_dir: Path, db_path: Path
) -> None:
    """Fetch World Bank indicator JSON and store it locally."""
    from ..storage.duckdb_store import init_db, upsert_worldbank_indicators, log_run

    sources = load_sources(config_path)
    if source_name not in sources:
        raise SystemExit(f"Unknown source '{source_name}'. Available: {', '.join(sorted(sources.keys()))}")

    source = sources[source_name]
    fetch_url = source.url
    if source.kind == "worldbank_macro_fiscal":
        fetch_url = build_multi_indicator_url(country="all", indicators=MACRO_FISCAL_INDICATORS)
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "raw" / source.name).mkdir(parents=True, exist_ok=True)
    (data_dir / "state").mkdir(parents=True, exist_ok=True)

    state_file = _state_path(data_dir, source.name)
    state = _load_state(state_file)

    started_at = datetime.now(timezone.utc)
    run_id = str(uuid.uuid4())

    try:
        fr = fetch_with_retry(
            lambda: fetch_worldbank_json(
                fetch_url, page=1, etag=state.get("etag"), last_modified=state.get("last_modified")
            )
        )
        init_db(db_path)

        if fr.status_code == 304:
            log_run(
                db_path,
                run_id=run_id,
                source=source.name,
                feed_url=source.url,
                started_at=started_at,
                ended_at=datetime.now(timezone.utc),
                status="no_change",
                n_total=0,
                n_new=0,
                message="HTTP 304 Not Modified",
            )
            print(f"[{source.name}] No change (HTTP 304).")
            return

        if fr.data is None:
            raise ValueError("World Bank response missing data")

        meta, rows = _parse_worldbank_response(fr.data)
        pages = int(meta.get("pages") or 1)
        pages_payload = [fr.data]

        for page in range(2, pages + 1):
            pr = fetch_with_retry(lambda p=page: fetch_worldbank_json(fetch_url, page=p))
            if pr.data is None:
                continue
            pages_payload.append(pr.data)
            _, page_rows = _parse_worldbank_response(pr.data)
            rows.extend(page_rows)

        ingested_at = datetime.now(timezone.utc)
        raw_path = _raw_path(data_dir, source.name, ingested_at)
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_text(
            json.dumps({"pages": pages_payload}, indent=2, ensure_ascii=False), encoding="utf-8"
        )

        normalized = _normalize_entries(source, rows, ingested_at)
        n_new = upsert_worldbank_indicators(db_path, normalized)

        if source.kind == "worldbank_macro_fiscal":
            _ensure_macro_fiscal_view(db_path)

        state.update(
            {
                "etag": fr.etag,
                "last_modified": fr.last_modified,
                "last_success_at": ingested_at.isoformat(),
                "last_status": fr.status_code,
                "last_raw_path": str(raw_path),
            }
        )
        _save_state(state_file, state)

        log_run(
            db_path,
            run_id=run_id,
            source=source.name,
            feed_url=fetch_url,
            started_at=started_at,
            ended_at=datetime.now(timezone.utc),
            status="ok",
            n_total=len(normalized),
            n_new=n_new,
            message=f"stored raw={raw_path.name}",
        )

        print(f"[{source.name}] fetched={len(normalized)} inserted_or_updated={n_new} raw={raw_path}")
    except Exception as e:
        try:
            init_db(db_path)
            log_run(
                db_path,
                run_id=run_id,
                source=source.name,
                feed_url=source.url,
                started_at=started_at,
                ended_at=datetime.now(timezone.utc),
                status="error",
                n_total=0,
                n_new=0,
                message=str(e),
            )
        except Exception:
            pass
        raise


def _ensure_macro_fiscal_view(db_path: Path) -> None:
    import duckdb

    con = duckdb.connect(str(db_path))
    try:
        con.execute(
            """
            CREATE OR REPLACE VIEW worldbank_macro_fiscal_wide AS
            SELECT
              country_id AS iso3,
              year,
              MAX(CASE WHEN series_id = 'NY.GDP.MKTP.CN' THEN value END) AS gdp_current_lcu,
              MAX(CASE WHEN series_id = 'NY.GDP.MKTP.PP.CD' THEN value END) AS gdp_ppp_current_intl_usd,
              MAX(CASE WHEN series_id = 'PA.NUS.PPP' THEN value END) AS ppp_conversion_factor,
              MAX(CASE WHEN series_id = 'GC.DOD.TOTL.GD.ZS' THEN value END) AS debt_pct_gdp,
              MAX(CASE WHEN series_id = 'GC.NLD.TOTL.GD.ZS' THEN value END) AS net_lending_pct_gdp,
              MAX(CASE WHEN series_id = 'GC.REV.XGRT.GD.ZS' THEN value END) AS revenue_ex_grants_pct_gdp,
              MAX(CASE WHEN series_id = 'GC.REV.XGRT.CN' THEN value END) AS revenue_ex_grants_lcu,
              MAX(CASE WHEN series_id = 'GC.XPN.INTP.CN' THEN value END) AS interest_payments_lcu,
              CASE
                WHEN MAX(CASE WHEN series_id = 'NY.GDP.MKTP.CN' THEN value END) IS NULL
                  OR MAX(CASE WHEN series_id = 'NY.GDP.MKTP.CN' THEN value END) = 0
                  OR MAX(CASE WHEN series_id = 'GC.XPN.INTP.CN' THEN value END) IS NULL
                THEN NULL
                ELSE 100.0
                  * MAX(CASE WHEN series_id = 'GC.XPN.INTP.CN' THEN value END)
                  / MAX(CASE WHEN series_id = 'NY.GDP.MKTP.CN' THEN value END)
              END AS interest_payments_pct_gdp
            FROM worldbank_indicators
            WHERE source = 'worldbank_macro_fiscal'
            GROUP BY country_id, year;
            """
        )
    finally:
        con.close()
