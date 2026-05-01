#!/usr/bin/env python3
"""Thin shim around `datahoover.publish:main`.

Use `hoover publish` for normal operation; this script is here so the
publishing pipeline can be invoked without installing the `hoover` console
script (e.g. from a fresh clone before `pip install -e .`).
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC = REPO_ROOT / "src"
if SRC.is_dir() and str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from datahoover.publish import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
