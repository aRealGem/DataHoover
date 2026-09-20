#!/usr/bin/env python3
"""Fetch the pinned Natural Earth basemap inputs and verify them by sha256.

Ruling DH-CRUDE-002-R2 D1. A clean checkout has no Natural Earth: data/raw/ is
gitignored, and ~3.7 MB of third-party GeoJSON does not belong in the repo.
This is the documented step that gets it back, and it FAILS on a hash mismatch
rather than quietly building a map from something else.

    python3 scripts/fetch_naturalearth.py          # fetch + verify
    python3 scripts/fetch_naturalearth.py --check  # verify what is on disk

Pinned to a tag, not a branch: Natural Earth reshapes borders between releases,
so an unpinned pull would silently move coastlines under a published map.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data" / "raw" / "naturalearth"
UA = "DataHoover/crude-map (+https://github.com/aRealGem/DataHoover)"

NE_TAG = "v5.1.2"
BASE = f"https://raw.githubusercontent.com/nvkelso/natural-earth-vector/{NE_TAG}/geojson"

# name -> (sha256, purpose). Recorded when the map was first built; a mismatch
# means the upstream file changed under a tag that should be immutable.
FILES: dict[str, tuple[str, str]] = {
    "ne_110m_admin_0_countries.geojson": (
        "6866c877d39cba9c357620878839b336d569f8c662d3cfab4cb1dbe2d39c977f",
        "basemap polygons",
    ),
    "ne_50m_admin_0_countries.geojson": (
        "3e458fc036ad0a66411f2c1e6cac49c5d7bfb81cb1123bc513b22511a2b7fdeb",
        "LABEL_X/LABEL_Y arrow endpoints (110m omits small states)",
    ),
}


def sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true",
                    help="verify what is already on disk; fetch nothing")
    args = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)

    prov = {"natural_earth_version": NE_TAG,
            "licence": "public domain (Natural Earth terms of use)",
            "pinned_to": "git tag, not a branch",
            "files": {}}
    failed = False

    for name, (want, why) in FILES.items():
        dest, url = OUT / name, f"{BASE}/{name}"
        if dest.exists():
            raw = dest.read_bytes()
            action = "on disk"
        elif args.check:
            print(f"  MISSING  {name}")
            failed = True
            continue
        else:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=300) as r:
                raw = r.read()
            dest.write_bytes(raw)
            action = "fetched"

        got = sha256(raw)
        ok = got == want
        failed |= not ok
        print(f"  {'OK  ' if ok else 'FAIL'}  {name}  ({action}, {len(raw)/1024/1024:.2f} MB)")
        if not ok:
            print(f"        expected {want}\n        got      {got}")
        prov["files"][name] = {"url": url, "sha256": got, "sha256_expected": want,
                               "bytes": len(raw), "purpose": why, "verified": ok}

    (OUT / "provenance.json").write_text(json.dumps(prov, indent=2), encoding="utf-8")
    if failed:
        print("\nHASH MISMATCH OR MISSING FILE — not safe to build a map from this.")
        return 1
    print(f"\nverified {len(FILES)} files against {NE_TAG}; provenance in {OUT}/provenance.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
