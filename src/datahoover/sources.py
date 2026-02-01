from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

try:
    import tomllib
except ImportError:  # pragma: no cover - Python < 3.11 fallback
    import tomli as tomllib


@dataclass(frozen=True)
class Source:
    name: str
    kind: str
    url: str
    description: str | None = None


def load_sources(path: Path) -> Dict[str, Source]:
    if not path.exists():
        raise FileNotFoundError(f"Missing config: {path}")

    data = tomllib.loads(path.read_text(encoding="utf-8"))
    sources: List[dict] = data.get("sources", [])
    out: Dict[str, Source] = {}
    for s in sources:
        src = Source(
            name=s["name"],
            kind=s["kind"],
            url=s["url"],
            description=s.get("description"),
        )
        out[src.name] = src
    return out
