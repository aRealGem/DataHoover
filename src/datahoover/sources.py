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
}


@dataclass(frozen=True)
class Source:
    name: str
    kind: str
    url: str
    description: str | None = None
    extra: Dict[str, any] | None = None


def load_sources(path: Path) -> Dict[str, Source]:
    if not path.exists():
        raise FileNotFoundError(f"Missing config: {path}")

    data = tomllib.loads(path.read_text(encoding="utf-8"))
    sources: List[dict] = data.get("sources", [])
    out: Dict[str, Source] = {}
    for s in sources:
        # Extract extra fields (everything not in the base Source fields)
        extra = {k: v for k, v in s.items() if k not in {"name", "kind", "url", "description"}}
        src = Source(
            name=s["name"],
            kind=s["kind"],
            url=s.get("url", ""),  # URL is optional for some sources like twelvedata
            description=s.get("description"),
            extra=extra if extra else None,
        )
        out[src.name] = src
    return out


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
