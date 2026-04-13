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
