import httpx

from datahoover.connectors.usgs_earthquakes import fetch_geojson


def test_fetch_geojson_sends_etag(monkeypatch):
    captured = {}

    class MockClient:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def get(self, url, headers):
            captured["headers"] = headers
            return httpx.Response(
                200,
                headers={"ETag": "etag-1"},
                json={"features": []},
                request=httpx.Request("GET", url),
            )

    monkeypatch.setattr("datahoover.connectors.usgs_earthquakes.httpx.Client", MockClient)

    result = fetch_geojson("https://example.test/feed", etag="etag-0")

    assert captured["headers"]["If-None-Match"] == "etag-0"
    assert result.status_code == 200
    assert result.etag == "etag-1"


def test_fetch_geojson_304(monkeypatch):
    class MockClient:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def get(self, url, headers):
            return httpx.Response(304, headers={}, request=httpx.Request("GET", url))

    monkeypatch.setattr("datahoover.connectors.usgs_earthquakes.httpx.Client", MockClient)

    result = fetch_geojson("https://example.test/feed", etag="etag-0")

    assert result.status_code == 304
    assert result.data is None
