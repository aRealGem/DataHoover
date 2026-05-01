from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

try:
    import tomllib
except ImportError:  # pragma: no cover - Python < 3.11 fallback
    import tomli as tomllib


# Hardcoded defaults matching the pre-externalization behavior.
# `load_signal_thresholds` merges these with any `[signals.<type>]` block
# present in sources.toml so omitting the section yields byte-identical output.
SIGNAL_THRESHOLD_DEFAULTS: Dict[str, Dict[str, Any]] = {
    "earthquake": {"min_magnitude": 5.0},
    "gdacs": {"min_severity": 0.6},
    "ooni": {"min_total": 10, "min_current_ratio": 0.5, "min_ratio_delta": 0.3},
    "market_move": {"min_abs_return": 0.02, "severity_denominator": 0.10},
    "sentiment_tone": {"min_articles": 5, "min_abs_avg_tone": 1.0, "severity_denominator": 5.0},
}


# Allowed values for the `license` field on a [[sources]] block. SPDX-style IDs
# where one exists; otherwise a `proprietary-<vendor>` or `mixed-<provider>`
# tag. `per-package` means the licence is encoded on each ingested record (e.g.
# CKAN catalogs whose member datasets each carry their own licence).
LICENSE_TAGS: frozenset[str] = frozenset(
    {
        "PD-USGov",
        "CC0-1.0",
        "CC-BY-4.0",
        "CC-BY-SA-4.0",
        "CC-BY-NC-4.0",
        "CC-BY-NC-SA-4.0",
        "ODC-BY-1.0",
        "ODbL-1.0",
        "eu-commission-reuse",
        "nyc-open-data",
        "mixed-fred",
        "proprietary-twelvedata",
        "proprietary-gdacs",
        "proprietary-caida",
        "proprietary-altme",
        "proprietary-cnn",
        "per-package",
    }
)

# Allowed values for the `redistribute` field. Operational answer to "can the
# raw rows or signals derived from them be republished?".
#   public-domain   — no restrictions.
#   with-attribution — CC-BY-style: republish freely if you attribute.
#   share-alike     — CC-BY-SA / ODbL: derived data inherits the licence.
#   non-commercial  — CC-BY-NC / CC-BY-NC-SA: not for commercial products.
#   display-only    — vendor permits showing values to users but not bulk
#                     redistribution of the raw series (e.g. Twelve Data).
#   per-package     — depends on the underlying record (CKAN catalogs).
#   no              — explicit prohibition; ingest for internal use only.
REDISTRIBUTE_TAGS: frozenset[str] = frozenset(
    {
        "public-domain",
        "with-attribution",
        "share-alike",
        "non-commercial",
        "display-only",
        "per-package",
        "no",
    }
)


@dataclass(frozen=True)
class Source:
    name: str
    kind: str
    url: str
    description: str | None = None
    license: str | None = None
    redistribute: str | None = None
    extra: Dict[str, any] | None = None


CATALOGS_FILENAME = "catalogs.toml"

_RESERVED_KEYS = {"name", "kind", "url", "description", "license", "redistribute"}


def _parse_source_blocks(raw_blocks: List[dict]) -> Dict[str, Source]:
    out: Dict[str, Source] = {}
    for s in raw_blocks:
        extra = {k: v for k, v in s.items() if k not in _RESERVED_KEYS}
        src = Source(
            name=s["name"],
            kind=s["kind"],
            url=s.get("url", ""),
            description=s.get("description"),
            license=s.get("license"),
            redistribute=s.get("redistribute"),
            extra=extra if extra else None,
        )
        out[src.name] = src
    return out


def load_sources(path: Path) -> Dict[str, Source]:
    """Load `[[sources]]` from `path` and auto-merge any sibling `catalogs.toml`."""
    if not path.exists():
        raise FileNotFoundError(f"Missing config: {path}")

    data = tomllib.loads(path.read_text(encoding="utf-8"))
    blocks: List[dict] = list(data.get("sources", []))

    catalogs_path = path.parent / CATALOGS_FILENAME
    if catalogs_path.exists():
        catalogs_data = tomllib.loads(catalogs_path.read_text(encoding="utf-8"))
        blocks.extend(catalogs_data.get("sources", []))

    return _parse_source_blocks(blocks)


def load_signal_thresholds(path: Path | None) -> Dict[str, Dict[str, Any]]:
    """Return merged `[signals.*]` table from `path`, overlaying `SIGNAL_THRESHOLD_DEFAULTS`."""
    merged: Dict[str, Dict[str, Any]] = {k: dict(v) for k, v in SIGNAL_THRESHOLD_DEFAULTS.items()}
    if path is None or not path.exists():
        return merged
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    section = data.get("signals", {})
    if not isinstance(section, dict):
        return merged
    for signal_type, overrides in section.items():
        if not isinstance(overrides, dict):
            continue
        merged.setdefault(signal_type, {}).update(overrides)
    return merged
