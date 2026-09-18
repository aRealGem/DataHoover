"""Fixtures must never carry credential material. This repo is PUBLIC.

Ruling DH-CRUDE-001 round 4. Recorded responses and cassettes are easy to
capture with a real key in the request headers or the query string, and a
fixture is exactly the kind of file nobody re-reads before committing. This
test is the backstop.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"

# Header and query-parameter forms of an API key, plus the obvious bearer case.
# Deliberately matches the KEY NAME, not a value pattern: we cannot know what a
# provider's keys look like, but we know what the field is called.
FORBIDDEN = [
    (re.compile(r"x-api-key", re.I), "X-Api-Key header"),
    (re.compile(r"\bapi[_-]?key\b", re.I), "api_key / apikey parameter"),
    (re.compile(r"\bauthorization\b\s*[:=]", re.I), "Authorization header"),
    (re.compile(r"\bbearer\s+[A-Za-z0-9._\-]{12,}", re.I), "bearer token"),
]

# DEMO_KEY is the documented public sandbox key for api.data.gov. It is not a
# secret, but a fixture should not imply automation uses it either, so it is
# called out separately rather than silently allowed.
DEMO_KEY = re.compile(r"\bDEMO_KEY\b")


def _fixture_files() -> list[Path]:
    if not FIXTURES.is_dir():
        return []
    return [p for p in sorted(FIXTURES.rglob("*")) if p.is_file()]


def test_fixtures_directory_is_present():
    assert _fixture_files(), "no fixtures found - has the path moved?"


@pytest.mark.parametrize("path", _fixture_files(), ids=lambda p: p.name)
def test_fixture_carries_no_credential_material(path: Path):
    text = path.read_text(encoding="utf-8", errors="replace")
    hits = [label for pattern, label in FORBIDDEN if pattern.search(text)]
    assert not hits, (
        f"{path.relative_to(FIXTURES.parent)} contains {', '.join(hits)}. "
        "This repo is public. Scrub the request side of the recording before "
        "committing it: capture the response body only, or redact the field."
    )


@pytest.mark.parametrize("path", _fixture_files(), ids=lambda p: p.name)
def test_fixture_does_not_embed_demo_key(path: Path):
    text = path.read_text(encoding="utf-8", errors="replace")
    assert not DEMO_KEY.search(text), (
        f"{path.relative_to(FIXTURES.parent)} mentions DEMO_KEY. It is a public "
        "sandbox key, not a secret, but it must not appear in a fixture - that "
        "reads as though automation uses it, which is explicitly disallowed."
    )
