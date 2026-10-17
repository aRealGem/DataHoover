"""Keep API keys out of logs, reports, exceptions and the warehouse.

Why this exists: on 2026-10-17 the FRED key was found in plain text 16 times in
data/logs/datahoover-weekly.log and in data/snapshots/last-pipeline-run.txt.
FRED, EIA, Census and Twelve Data take the key as a URL query parameter, and
httpx puts the full request URL into every HTTPStatusError message, so a
single 4xx/5xx printed with ``f"{exc}"`` wrote the key to stdout, which the
pipeline tees into both files. This repo is public and those files reach
backups, so the key must never be in them.

Two layers, so a new connector cannot reintroduce the leak by accident:

  * At the source: ``raise_for_status`` raises an HTTPStatusError whose message
    has the key masked, and ``redact_url`` masks a URL before it is stored.
  * At the sinks: ``install`` wraps sys.stdout / sys.stderr and the root logger
    so anything printed, logged or dumped as a traceback is scrubbed of every
    known secret value and of any key-shaped query parameter.

Known secret values are every value ``env.get_secret`` has returned, plus the
current value of any environment variable whose name looks like a credential
(``*_API_KEY``, ``*_TOKEN``, ``*_SECRET``, ``*_PASSWORD``).
"""
from __future__ import annotations

import io
import logging
import os
import re
import sys
from typing import Iterable

import httpx

MASK = "[REDACTED]"

# Values shorter than this are not treated as secrets: masking "1" or "ok"
# everywhere would corrupt ordinary output and protect nothing.
_MIN_SECRET_LEN = 8

_SECRET_ENV_NAME = re.compile(
    r"(?:API_?KEY|_KEY|TOKEN|SECRET|PASSWORD|PASSWD)$", re.IGNORECASE
)

# Query/form parameter names that carry credentials for the APIs we call
# (FRED/EIA ``api_key``, Twelve Data ``apikey``, Census ``key``, BLS
# ``registrationkey``) plus the common generic ones.
_SECRET_PARAM = re.compile(
    r"(?P<name>\b(?:api[_-]?key|apikey|key|registration[_-]?key|access[_-]?token|"
    r"token|auth|password|secret)=)(?P<value>[^&\s'\"<>#]+)",
    re.IGNORECASE,
)

_registered: set[str] = set()


def register_secret(value: str | None) -> None:
    """Remember a secret value so every sink masks it."""
    if value and len(value) >= _MIN_SECRET_LEN:
        _registered.add(value)


def _env_secrets() -> Iterable[str]:
    for name, value in os.environ.items():
        if _SECRET_ENV_NAME.search(name) and value and len(value) >= _MIN_SECRET_LEN:
            yield value


def known_secrets() -> list[str]:
    # Longest first, so a secret that contains another is masked whole.
    return sorted(set(_registered) | set(_env_secrets()), key=len, reverse=True)


def redact(text: object) -> str:
    """Return ``text`` with every known secret and key-shaped parameter masked."""
    s = text if isinstance(text, str) else str(text)
    if not s:
        return s
    for secret in known_secrets():
        if secret in s:
            s = s.replace(secret, MASK)
    return _SECRET_PARAM.sub(lambda m: m.group("name") + MASK, s)


def redact_url(url: object) -> str:
    """A URL safe to log or store: credential query parameters masked."""
    return redact(url)


def raise_for_status(response: httpx.Response) -> None:
    """``response.raise_for_status()``, with the key masked in the message.

    httpx's message embeds the full request URL. The replacement keeps the
    same type, request and response, so retry logic that reads
    ``exc.response.status_code`` behaves exactly as before. ``from None``
    stops Python printing the original (unmasked) message as the cause.
    """
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        raise httpx.HTTPStatusError(
            redact(str(exc)), request=exc.request, response=exc.response
        ) from None


class RedactingFilter(logging.Filter):
    """Logging filter that masks secrets in the fully formatted message."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except Exception:
            return True
        record.msg = redact(message)
        record.args = None
        if record.exc_info and not record.exc_text:
            record.exc_text = redact(
                logging.Formatter().formatException(record.exc_info)
            )
        elif record.exc_text:
            record.exc_text = redact(record.exc_text)
        return True


class RedactingStream(io.TextIOBase):
    """Text stream wrapper that masks secrets before they reach ``inner``.

    Writes are buffered to the end of each line, so a secret split across two
    ``write`` calls (as ``print`` and traceback output both do) is still caught.
    """

    def __init__(self, inner):
        self._inner = inner
        self._pending = ""

    def write(self, s: str) -> int:
        self._pending += s
        if "\n" in self._pending:
            complete, self._pending = self._pending.rsplit("\n", 1)
            self._inner.write(redact(complete) + "\n")
        return len(s)

    def flush(self) -> None:
        if self._pending:
            self._inner.write(redact(self._pending))
            self._pending = ""
        self._inner.flush()

    def isatty(self) -> bool:
        return self._inner.isatty()

    def fileno(self) -> int:
        return self._inner.fileno()

    @property
    def encoding(self):
        return getattr(self._inner, "encoding", "utf-8")

    def writable(self) -> bool:
        return True


def install() -> None:
    """Scrub secrets from stdout, stderr and the root logger. Idempotent."""
    if not isinstance(sys.stdout, RedactingStream):
        sys.stdout = RedactingStream(sys.stdout)
    if not isinstance(sys.stderr, RedactingStream):
        sys.stderr = RedactingStream(sys.stderr)
    root = logging.getLogger()
    if not any(isinstance(f, RedactingFilter) for f in root.filters):
        root.addFilter(RedactingFilter())
    for handler in root.handlers:
        if not any(isinstance(f, RedactingFilter) for f in handler.filters):
            handler.addFilter(RedactingFilter())
