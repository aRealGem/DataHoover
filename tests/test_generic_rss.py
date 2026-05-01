from __future__ import annotations

from datetime import datetime, timezone

import duckdb
import pytest

from datahoover.connectors import generic_rss as rss
from datahoover.sources import Source
from datahoover.storage.duckdb_store import init_db, upsert_rss_items


RSS_2_0_SAMPLE = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Sample feed</title>
    <link>https://example.test/</link>
    <description>example</description>
    <item>
      <title>FOMC statement released</title>
      <link>https://example.test/fomc-2026-05-01</link>
      <description>The Federal Open Market Committee decided to maintain rates...</description>
      <pubDate>Thu, 01 May 2026 18:00:00 GMT</pubDate>
      <guid>https://example.test/fomc-2026-05-01</guid>
      <author>press@federalreserve.gov</author>
    </item>
    <item>
      <title>Speech: Powell on financial conditions</title>
      <link>https://example.test/powell-2026-04-30</link>
      <description>...</description>
      <pubDate>Wed, 30 Apr 2026 14:30:00 GMT</pubDate>
      <guid isPermaLink="false">speech-2026-04-30</guid>
    </item>
  </channel>
</rss>"""

ATOM_SAMPLE = b"""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Sample atom feed</title>
  <link href="https://example.test/atom" rel="self"/>
  <id>https://example.test/atom</id>
  <updated>2026-05-01T18:00:00Z</updated>
  <entry>
    <title>Atom entry one</title>
    <link href="https://example.test/atom/1" rel="alternate"/>
    <id>tag:example.test,2026:1</id>
    <updated>2026-05-01T18:00:00Z</updated>
    <published>2026-05-01T18:00:00Z</published>
    <summary>First entry summary</summary>
    <author><name>Author A</name></author>
  </entry>
  <entry>
    <title>Atom entry two</title>
    <link href="https://example.test/atom/2"/>
    <id>tag:example.test,2026:2</id>
    <updated>2026-04-30T12:00:00Z</updated>
    <content>Inline content fallback</content>
  </entry>
</feed>"""


def _source() -> Source:
    return Source(
        name="fed_press_releases_rss",
        kind="generic_rss",
        url="https://www.federalreserve.gov/feeds/press_all.xml",
    )


def test_parse_feed_rss_2_0_extracts_items():
    entries = rss.parse_feed(RSS_2_0_SAMPLE)
    assert len(entries) == 2
    e0 = entries[0]
    assert e0["title"] == "FOMC statement released"
    assert e0["link"] == "https://example.test/fomc-2026-05-01"
    assert e0["guid"] == "https://example.test/fomc-2026-05-01"
    assert e0["author"] == "press@federalreserve.gov"
    assert e0["published_at"] == datetime(2026, 5, 1, 18, 0, tzinfo=timezone.utc)
    # Second entry: pubDate via different time, no author, non-URL guid.
    assert entries[1]["guid"] == "speech-2026-04-30"
    assert entries[1]["author"] is None


def test_parse_feed_atom_extracts_entries():
    entries = rss.parse_feed(ATOM_SAMPLE)
    assert len(entries) == 2
    e0 = entries[0]
    assert e0["title"] == "Atom entry one"
    assert e0["link"] == "https://example.test/atom/1"
    assert e0["guid"] == "tag:example.test,2026:1"
    assert e0["author"] == "Author A"
    assert e0["published_at"] == datetime(2026, 5, 1, 18, 0, tzinfo=timezone.utc)
    # Second entry: summary falls back to <content>.
    assert entries[1]["summary"] == "Inline content fallback"


def test_parse_feed_rejects_unknown_root():
    with pytest.raises(ValueError, match="Unrecognized feed root"):
        rss.parse_feed(b"<not-a-feed/>")


def test_parse_feed_rejects_malformed_xml():
    with pytest.raises(ValueError, match="Could not parse RSS/Atom XML"):
        rss.parse_feed(b"<rss><channel><item></rss>")


def test_parse_pubdate_handles_rfc822_and_iso():
    assert rss._parse_pubdate("Thu, 01 May 2026 18:00:00 GMT") == datetime(
        2026, 5, 1, 18, 0, tzinfo=timezone.utc
    )
    assert rss._parse_pubdate("2026-05-01T18:00:00Z") == datetime(
        2026, 5, 1, 18, 0, tzinfo=timezone.utc
    )
    # Naive ISO is treated as UTC.
    assert rss._parse_pubdate("2026-05-01T18:00:00") == datetime(
        2026, 5, 1, 18, 0, tzinfo=timezone.utc
    )
    assert rss._parse_pubdate("garbage") is None
    assert rss._parse_pubdate(None) is None


def test_normalize_entries_uses_guid_link_or_title():
    src = _source()
    at = datetime(2026, 5, 1, tzinfo=timezone.utc)
    entries = [
        {"guid": "g1", "title": "t1", "link": "https://x/1", "summary": "s1", "author": "a", "published_at": at},
        {"guid": None, "title": "t2", "link": "https://x/2", "summary": None, "author": None, "published_at": None},
        # No guid/link/title: dropped.
        {"guid": None, "title": None, "link": None, "summary": "lonely", "author": None, "published_at": None},
    ]
    rows = rss._normalize_entries(src, entries, ingested_at=at, raw_path="/r.xml")
    assert len(rows) == 2
    assert rows[0]["guid"] == "g1"
    # When guid is missing, fallback is the link.
    assert rows[1]["guid"] == "https://x/2"


def test_fetch_handles_304(monkeypatch):
    class FakeResponse:
        status_code = 304
        headers = {}

        def raise_for_status(self):
            return None

    class FakeClient:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get(self, *a, **k):
            return FakeResponse()

    monkeypatch.setattr(rss.httpx, "Client", FakeClient)
    result = rss.fetch_rss("https://x.test/feed", etag="W/abc")
    assert result.status_code == 304
    assert result.raw_bytes is None
    assert result.etag == "W/abc"


def test_upsert_rss_items_idempotent(tmp_path):
    db_path = tmp_path / "warehouse.duckdb"
    init_db(db_path)
    at = datetime(2026, 5, 1, tzinfo=timezone.utc)
    rows = [
        {
            "source": "fed_press_releases_rss",
            "feed_url": "https://www.federalreserve.gov/feeds/press_all.xml",
            "guid": "https://example.test/fomc",
            "title": "FOMC",
            "link": "https://example.test/fomc",
            "summary": "...",
            "author": None,
            "published_at": datetime(2026, 5, 1, 18, 0, tzinfo=timezone.utc),
            "raw_xml_path": "/a.xml",
            "ingested_at": at,
        }
    ]
    assert upsert_rss_items(db_path, rows) == 1
    assert upsert_rss_items(db_path, rows) == 1
    con = duckdb.connect(str(db_path))
    try:
        n = con.execute("SELECT COUNT(*) FROM rss_items").fetchone()[0]
        assert n == 1
    finally:
        con.close()
