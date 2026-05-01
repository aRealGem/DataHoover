"""Lane resolver: map per-source redistribute tags into a publication lane.

Used by the publish pipeline (`hoover publish`) to split rendered canvases
into a 'commercial-safe' index and a 'personal-use' index. The split mirrors
the lane semantics in `docs/licensing.md`.
"""

from __future__ import annotations

from typing import Iterable

from .sources import Source

COMMERCIAL_SAFE: frozenset[str] = frozenset(
    {"public-domain", "with-attribution", "share-alike"}
)


def lane_for_redistribute(value: str | None) -> str:
    """Map a single redistribute tag to its publication lane.

    Returns ``'commercial-safe'`` for tags in :data:`COMMERCIAL_SAFE`, otherwise
    ``'personal-use'``. ``None``, unknown values, ``'non-commercial'``,
    ``'display-only'``, ``'per-package'``, and ``'no'`` all collapse to
    ``'personal-use'`` — the conservative default for republication contexts.
    """
    return "commercial-safe" if value in COMMERCIAL_SAFE else "personal-use"


def lane_for_publication(
    source_names: Iterable[str], srcs: dict[str, Source]
) -> str:
    """Worst-case lane across all sources used by a publication.

    Unknown source names are silently ignored. If no listed source is known,
    returns ``'personal-use'`` — there's no evidence of commercial-safety.
    """
    known = [srcs[n] for n in source_names if n in srcs]
    if not known:
        return "personal-use"
    lanes = {lane_for_redistribute(s.redistribute) for s in known}
    return "personal-use" if "personal-use" in lanes else "commercial-safe"


def attribution_block(
    source_names: Iterable[str], srcs: dict[str, Source]
) -> str:
    """Render a multi-line footer attribution string for a publication.

    Format::

        Sources:
          <name>: <license> (<redistribute>)
          ...

    Unknown source names are silently dropped; entries are sorted by name for
    stable output.
    """
    lines: list[str] = []
    for name in sorted(set(source_names)):
        if name in srcs:
            s = srcs[name]
            lines.append(
                f"  {s.name}: {s.license or 'unknown'} ({s.redistribute or 'unknown'})"
            )
    if not lines:
        return "Sources: (none)"
    return "Sources:\n" + "\n".join(lines)
