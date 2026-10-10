"""Provenance helpers shared by the policy connectors (DH-PULLS-001).

Every policy source keeps several clocks apart and must never collapse them:

  * first-public time   -- when the source says the record became public
                           (e.g. Federal Register ``filed_at`` for public inspection)
  * publication time    -- the source's own publication/edition date
  * effective time      -- the legal effective date, ONLY as the source states it
  * revision time       -- when the source says it changed the record
                           (e.g. ``pdf_updated_at``)
  * local first_seen_at -- when THIS machine first saw the record

Source timestamps are kept verbatim (``*_raw``) next to a parsed UTC value plus
the original UTC offset and a precision flag, so a reader can always tell a
real 00:00 from a date-only field and a stated offset from an assumed one.
Unknown stays NULL; nothing here infers a missing value.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

USER_AGENT = "DataHoover/policy-pulls (+https://github.com/aRealGem/DataHoover)"

_TS_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2})[T ](\d{2}):(\d{2})(?::(\d{2})(\.\d+)?)?\s*(Z|[+-]\d{2}:?\d{2})?$"
)


@dataclass(frozen=True)
class ParsedTs:
    raw: Optional[str]
    utc: Optional[datetime]  # naive UTC (DuckDB TIMESTAMP), None when unknown/unparseable
    tz_offset: Optional[str]  # e.g. "-04:00"; None when the source gave no offset
    precision: Optional[str]  # "ms" | "us" | "s" | "min" | None


def parse_source_ts(value: Any) -> ParsedTs:
    """Parse an ISO-8601 source timestamp without guessing.

    A timestamp with no offset is NOT assumed to be UTC or Eastern: ``utc`` stays
    None and ``tz_offset`` None, with the raw string retained for a human to read.
    """
    if value is None or value == "":
        return ParsedTs(None, None, None, None)
    raw = str(value).strip()
    m = _TS_RE.match(raw)
    if not m:
        return ParsedTs(raw, None, None, None)
    _, _, _, sec, frac, off = m.groups()
    if frac:
        precision = "ms" if len(frac) <= 4 else "us"
    elif sec is not None:
        precision = "s"
    else:
        precision = "min"
    if not off:
        return ParsedTs(raw, None, None, precision)
    norm_off = "+00:00" if off == "Z" else (off if ":" in off else f"{off[:3]}:{off[3:]}")
    iso = raw.replace(" ", "T", 1)
    if iso.endswith("Z"):
        iso = iso[:-1] + "+00:00"
    elif ":" not in off:
        iso = iso[: -len(off)] + norm_off
    dt = datetime.fromisoformat(iso)
    return ParsedTs(raw, dt.astimezone(timezone.utc).replace(tzinfo=None), norm_off, precision)


def parse_source_date(value: Any) -> Optional[date]:
    """Parse a date-only source field (YYYY-MM-DD). Anything else -> None."""
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10]) if re.match(r"^\d{4}-\d{2}-\d{2}$", str(value)) else None
    except ValueError:
        return None


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def content_hash(record: Dict[str, Any], *, exclude: Iterable[str] = ()) -> str:
    """Stable hash of a record's substantive content (volatile keys excluded)."""
    skip = set(exclude)
    payload = {k: v for k, v in record.items() if k not in skip}
    return sha256_bytes(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))


def write_immutable(path: Path, data: bytes) -> str:
    """Write raw bytes once. An existing file is never overwritten.

    Returns the sha256. If the path already exists with different bytes that is a
    provenance error, not something to paper over.
    """
    digest = sha256_bytes(data)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if sha256_bytes(path.read_bytes()) != digest:
            raise RuntimeError(f"refusing to overwrite immutable raw file with different bytes: {path}")
        return digest
    with open(path, "xb") as fh:
        fh.write(data)
    return digest


def utc_now() -> datetime:
    """Naive UTC now (the warehouse stores naive-UTC TIMESTAMPs)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)
