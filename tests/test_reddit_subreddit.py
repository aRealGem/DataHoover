from __future__ import annotations

from datetime import datetime, timezone

import duckdb
import pytest

from datahoover.connectors import reddit_subreddit as reddit
from datahoover.sources import Source
from datahoover.storage.duckdb_store import init_db, upsert_reddit_posts


SAMPLE_LISTING = {
    "kind": "Listing",
    "data": {
        "after": "t3_zzz",
        "children": [
            {
                "kind": "t3",
                "data": {
                    "id": "1abc234",
                    "name": "t3_1abc234",
                    "title": "Big news for SPY",
                    "selftext": "Speculation here",
                    "author": "trader1",
                    "subreddit": "wallstreetbets",
                    "score": 42,
                    "num_comments": 12,
                    "upvote_ratio": 0.95,
                    "created_utc": 1714521600.0,  # 2024-05-01 00:00 UTC
                    "permalink": "/r/wallstreetbets/comments/1abc234/big_news/",
                    "url": "https://reddit.com/r/wallstreetbets/comments/1abc234/big_news/",
                    "domain": "self.wallstreetbets",
                    "link_flair_text": "DD",
                    "is_self": True,
                    "over_18": False,
                },
            },
            {
                "kind": "t3",
                "data": {
                    "id": "1def567",
                    "name": "t3_1def567",
                    "title": "Another post",
                    "score": "not-an-int",  # malformed; should land as None.
                    "created_utc": 1714435200.0,
                    "is_self": False,
                },
            },
            # Malformed entry without `name` should be skipped.
            {"kind": "t3", "data": {"title": "no id"}},
            # Non-dict entries tolerated.
            "junk",
        ],
    },
}


def _source() -> Source:
    return Source(
        name="reddit_sentiment_subs",
        kind="reddit_subreddit_json",
        url="https://www.reddit.com",
        extra={"subreddits": ["wallstreetbets"]},
    )


def test_normalize_listing_extracts_one_row_per_post():
    at = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
    rows = reddit._normalize_listing(_source(), "wallstreetbets", SAMPLE_LISTING, ingested_at=at)
    assert len(rows) == 2
    r0 = rows[0]
    assert r0["post_id"] == "t3_1abc234"
    assert r0["reddit_id"] == "1abc234"
    assert r0["score"] == 42
    assert r0["upvote_ratio"] == 0.95
    assert r0["created_utc"].isoformat() == "2024-05-01T00:00:00+00:00"
    assert r0["is_self"] is True
    # Malformed score becomes None rather than blowing up.
    assert rows[1]["score"] is None


def test_normalize_listing_handles_missing_data_block():
    at = datetime(2026, 5, 1, tzinfo=timezone.utc)
    rows = reddit._normalize_listing(_source(), "wallstreetbets", {}, ingested_at=at)
    assert rows == []


def test_fetch_reddit_listing_sends_user_agent(monkeypatch):
    captured: dict = {}

    class FakeResponse:
        status_code = 200
        content = b"{}"

        def raise_for_status(self):
            return None

        def json(self):
            return {"kind": "Listing", "data": {"children": []}}

    class FakeClient:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get(self, url, headers=None, params=None):
            captured["url"] = url
            captured["headers"] = dict(headers or {})
            captured["params"] = dict(params or {})
            return FakeResponse()

    monkeypatch.setattr(reddit.httpx, "Client", FakeClient)
    reddit.fetch_reddit_listing("https://www.reddit.com", subreddit="stocks", listing="new", limit=50)
    assert captured["url"] == "https://www.reddit.com/r/stocks/new.json"
    # Reddit requires a UA in `<platform>:<app id>:<version> (by /u/<user>)` form;
    # bot-generic UAs get 403'd. Confirm the default matches that shape.
    ua = captured["headers"]["User-Agent"]
    assert ua.startswith("python:datahoover:")
    assert "(by /u/" in ua
    assert captured["params"]["limit"] == "50"
    assert captured["params"]["raw_json"] == "1"


def test_fetch_reddit_listing_honors_user_agent_override(monkeypatch):
    captured: dict = {}

    class FakeResponse:
        status_code = 200
        content = b"{}"

        def raise_for_status(self):
            return None

        def json(self):
            return {"kind": "Listing", "data": {"children": []}}

    class FakeClient:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get(self, url, headers=None, params=None):
            captured["headers"] = dict(headers or {})
            return FakeResponse()

    monkeypatch.setattr(reddit.httpx, "Client", FakeClient)
    reddit.fetch_reddit_listing(
        "https://www.reddit.com",
        subreddit="stocks",
        user_agent="python:my-tool:2.3 (by /u/realhandle)",
    )
    assert captured["headers"]["User-Agent"] == "python:my-tool:2.3 (by /u/realhandle)"


def test_fetch_reddit_listing_rejects_non_object_response(monkeypatch):
    class FakeResponse:
        status_code = 200
        content = b"[]"

        def raise_for_status(self):
            return None

        def json(self):
            return []

    class FakeClient:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get(self, *a, **k):
            return FakeResponse()

    monkeypatch.setattr(reddit.httpx, "Client", FakeClient)
    with pytest.raises(ValueError, match="did not return a JSON object"):
        reddit.fetch_reddit_listing("https://www.reddit.com", subreddit="stocks")


def test_upsert_reddit_posts_idempotent(tmp_path):
    db_path = tmp_path / "warehouse.duckdb"
    init_db(db_path)
    at = datetime(2026, 5, 1, tzinfo=timezone.utc)
    rows = [
        {
            "source": "reddit_sentiment_subs",
            "subreddit": "wallstreetbets",
            "post_id": "t3_1abc234",
            "reddit_id": "1abc234",
            "title": "Big news",
            "selftext": "...",
            "author": "trader1",
            "score": 42,
            "num_comments": 12,
            "upvote_ratio": 0.95,
            "created_utc": datetime(2024, 5, 1, tzinfo=timezone.utc),
            "permalink": "/r/wallstreetbets/comments/1abc234/big_news/",
            "url": "https://reddit.com/...",
            "domain": "self.wallstreetbets",
            "flair": "DD",
            "is_self": True,
            "over_18": False,
            "raw_json": "{}",
            "raw_path": "/a.json",
            "ingested_at": at,
        }
    ]
    assert upsert_reddit_posts(db_path, rows) == 1
    assert upsert_reddit_posts(db_path, rows) == 1
    con = duckdb.connect(str(db_path))
    try:
        n = con.execute("SELECT COUNT(*) FROM reddit_posts").fetchone()[0]
        assert n == 1
    finally:
        con.close()
