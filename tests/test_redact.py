"""API keys must never reach logs, reports, tracebacks or ingest_runs.

Regression for 2026-10-17: the FRED key was found 16 times in plain text in
data/logs/datahoover-weekly.log and in data/snapshots/last-pipeline-run.txt,
because httpx puts the full request URL (with ``api_key=``) into every
HTTPStatusError message and the FRED connector printed that message. This repo
is public; the fake key below is shaped like a real FRED key so the test
exercises the same code path, and it is not a credential.
"""
from __future__ import annotations

import io
import logging
import os
import re
import subprocess
import sys
import textwrap
from pathlib import Path

import duckdb
import httpx
import pytest

from datahoover import redact

REPO = Path(__file__).resolve().parents[1]
SRC = REPO / "src"
FAKE_KEY = "0123456789abcdef0123456789abcdef"


@pytest.fixture(autouse=True)
def _clean_registry(monkeypatch):
    monkeypatch.setattr(redact, "_registered", set())


def test_redact_masks_registered_secret_anywhere():
    redact.register_secret(FAKE_KEY)
    out = redact.redact(f"boom: token {FAKE_KEY} in prose")
    assert FAKE_KEY not in out
    assert redact.MASK in out


def test_redact_masks_secret_from_credential_named_env_var(monkeypatch):
    monkeypatch.setenv("SOMETHING_API_KEY", FAKE_KEY)
    assert FAKE_KEY not in redact.redact(f"x {FAKE_KEY} y")


@pytest.mark.parametrize(
    "param",
    ["api_key", "apikey", "key", "registrationkey", "access_token", "API_KEY"],
)
def test_redact_masks_key_shaped_query_params_even_when_value_unknown(param):
    url = f"https://example.org/x?series_id=SP500&{param}=zzzzzzzzzzzzzzzz&limit=5"
    out = redact.redact_url(url)
    assert "zzzzzzzzzzzzzzzz" not in out
    assert "series_id=SP500" in out and "limit=5" in out


def test_redact_leaves_ordinary_text_alone():
    text = "[fred_macro_watchlist] SP500: rows=120 raw=series_SP500.json"
    assert redact.redact(text) == text


def _status_error_response(url: str, status: int = 400) -> httpx.Response:
    return httpx.Response(status, request=httpx.Request("GET", url))


def test_raw_httpx_message_does_leak_the_key():
    """Negative control: proves the scenario the redaction guards is real."""
    response = _status_error_response(
        f"https://api.stlouisfed.org/fred/series/observations?api_key={FAKE_KEY}"
    )
    with pytest.raises(httpx.HTTPStatusError) as info:
        response.raise_for_status()
    assert FAKE_KEY in str(info.value)


def test_raise_for_status_masks_key_and_keeps_status_and_type():
    response = _status_error_response(
        f"https://api.stlouisfed.org/fred/series/observations?api_key={FAKE_KEY}", 503
    )
    with pytest.raises(httpx.HTTPStatusError) as info:
        redact.raise_for_status(response)
    exc = info.value
    assert FAKE_KEY not in str(exc)
    assert exc.response.status_code == 503
    # `from None`: the original, unmasked message must not ride along as the cause.
    assert exc.__cause__ is None and exc.__suppress_context__


def test_raise_for_status_is_silent_on_success():
    redact.raise_for_status(httpx.Response(200, request=httpx.Request("GET", "https://x")))


def test_logging_filter_masks_message_args_and_traceback(monkeypatch):
    redact.register_secret(FAKE_KEY)
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.addFilter(redact.RedactingFilter())
    log = logging.getLogger("datahoover.test_redact")
    log.addHandler(handler)
    log.propagate = False
    try:
        try:
            raise RuntimeError(f"url?api_key={FAKE_KEY}")
        except RuntimeError:
            log.exception("failed with %s", FAKE_KEY)
    finally:
        log.removeHandler(handler)
    out = buf.getvalue()
    assert FAKE_KEY not in out
    assert "RuntimeError" in out


def test_stream_catches_secret_split_across_writes():
    redact.register_secret(FAKE_KEY)
    inner = io.StringIO()
    stream = redact.RedactingStream(inner)
    stream.write("before " + FAKE_KEY[:10])
    stream.write(FAKE_KEY[10:] + " after\n")
    stream.write("tail " + FAKE_KEY)
    stream.flush()
    assert FAKE_KEY not in inner.getvalue()
    assert inner.getvalue().count(redact.MASK) == 2


def test_get_secret_registers_the_value(monkeypatch):
    from datahoover.env import get_secret

    monkeypatch.setenv("DH_TEST_CREDENTIAL_X", FAKE_KEY)  # name not credential-shaped
    assert get_secret("DH_TEST_CREDENTIAL_X") == FAKE_KEY
    assert FAKE_KEY not in redact.redact(f"leak {FAKE_KEY}")


def test_fred_http_error_never_reaches_output_or_ingest_runs(tmp_path):
    """End to end, the way the weekly timer runs it: `hoover ingest-fred` in a
    child process, stdout+stderr captured as the pipeline tees them, FRED
    answering 400 so the connector prints its warning, logs the run and exits
    with an uncaught traceback. The key must appear in none of it."""
    config = tmp_path / "sources.toml"
    config.write_text(
        textwrap.dedent(
            """
            [[sources]]
            name = "fred_test"
            kind = "fred_series"
            url = "https://api.stlouisfed.org/fred/series/observations"
            series_ids = ["SP500", "DJIA"]
            """
        ),
        encoding="utf-8",
    )
    db = tmp_path / "warehouse.duckdb"
    runner = tmp_path / "runner.py"
    runner.write_text(
        textwrap.dedent(
            """
            import sys
            import httpx

            def handler(request):
                return httpx.Response(400, json={"error_message": "Bad Request"})

            _Client = httpx.Client

            def client(*args, **kwargs):
                kwargs["transport"] = httpx.MockTransport(handler)
                return _Client(*args, **kwargs)

            httpx.Client = client

            from datahoover.cli import main
            sys.exit(main(sys.argv[1:]))
            """
        ),
        encoding="utf-8",
    )
    env = {
        **os.environ,
        "PYTHONPATH": str(SRC),
        "FRED_API_KEY": FAKE_KEY,
        "DATAHOOVER_ENV_FILE": str(tmp_path / "absent.env"),
    }
    proc = subprocess.run(
        [
            sys.executable,
            str(runner),
            "ingest-fred",
            "--config", str(config),
            "--source", "fred_test",
            "--data-dir", str(tmp_path / "data"),
            "--db", str(db),
        ],
        capture_output=True,
        text=True,
        env=env,
        cwd=tmp_path,
        timeout=120,
    )
    output = proc.stdout + proc.stderr

    # The failure path really ran: per-series warnings, then the uncaught error.
    assert proc.returncode != 0, output
    assert "SP500" in output and "400 Bad Request" in output, output
    assert "No FRED series fetched successfully" in output, output

    assert FAKE_KEY not in output
    assert not re.search(r"api_key=(?!\[REDACTED\])", output), output

    con = duckdb.connect(str(db))
    try:
        rows = con.execute("SELECT feed_url, message FROM ingest_runs").fetchall()
    finally:
        con.close()
    assert rows, "the failed run must still be logged"
    for feed_url, message in rows:
        assert FAKE_KEY not in (feed_url or "")
        assert FAKE_KEY not in (message or "")

    for path in (tmp_path / "data").rglob("*"):
        if path.is_file():
            assert FAKE_KEY not in path.name
            assert FAKE_KEY not in path.read_text(encoding="utf-8", errors="replace")


def test_uncaught_traceback_is_masked(tmp_path):
    script = textwrap.dedent(
        f"""
        from datahoover import redact
        redact.install()
        raise RuntimeError("GET https://api.example/x?api_key={FAKE_KEY}")
        """
    )
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": str(SRC)},
        cwd=tmp_path,
        timeout=60,
    )
    assert "RuntimeError" in proc.stderr
    assert FAKE_KEY not in proc.stderr


CONNECTORS = SRC / "datahoover" / "connectors"


@pytest.mark.parametrize(
    "path",
    sorted(p for p in CONNECTORS.glob("*.py") if "get_secret" in p.read_text(encoding="utf-8")),
    ids=lambda p: p.name,
)
def test_keyed_connectors_use_the_masking_raise_for_status(path: Path):
    """A connector that holds a key must not call httpx's own
    raise_for_status: its message embeds the full URL, key included."""
    text = path.read_text(encoding="utf-8")
    assert ".raise_for_status()" not in text, (
        f"{path.name} calls response.raise_for_status() directly; use "
        "datahoover.redact.raise_for_status(response) so the key is masked."
    )
