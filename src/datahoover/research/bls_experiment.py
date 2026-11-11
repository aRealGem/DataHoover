"""Run an offline descriptive experiment on retained BLS v1 capture files.

This is an explicit consumer of the research transforms, not an adapter for the
live DataHoover BLS v2 connector. It never fetches data or writes a database.
"""

import argparse
import json
from pathlib import Path
import sys

from .bls_features import transform
from .bls_normalize import captured_rows, load_json


INPUT_SCHEMA = "bls-v1-capture"
MAX_INPUT_BYTES = 2_000_000


def read_bounded(path):
    """Read one named local file, rejecting oversized inputs before processing."""
    with Path(path).open("rb") as stream:
        raw = stream.read(MAX_INPUT_BYTES + 1)
    if len(raw) > MAX_INPUT_BYTES:
        raise ValueError("input exceeds offline experiment size bound")
    return raw


def run(raw, capture_raw, request_raw):
    """Return deterministic descriptive results; preserve all absence evidence."""
    capture = load_json(capture_raw)
    rows, absent, annual, coverage = captured_rows(raw, capture, request_raw)
    return {
        "input_schema": INPUT_SCHEMA,
        "experiment": "exact_calendar_bls_changes",
        "historical_asof_eligible": False,
        "production_integration": False,
        "coverage": coverage,
        "absent_slots": absent,
        "excluded_annual_records": annual,
        "features": transform(rows),
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-schema", required=True, choices=[INPUT_SCHEMA])
    parser.add_argument("--raw", required=True, type=Path)
    parser.add_argument("--capture", required=True, type=Path)
    parser.add_argument("--request", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        result = run(*(read_bounded(path) for path in
                       (args.raw, args.capture, args.request)))
        output = json.dumps(result, sort_keys=True, ensure_ascii=False,
                            allow_nan=False)
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        # Do not echo untrusted receipt contents or file paths in an error.
        print("Invalid or unreadable BLS v1 experiment inputs.", file=sys.stderr)
        return 2
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
