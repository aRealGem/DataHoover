"""Exercise the actual offline CLI with fabricated, byte-bound capture inputs."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from datahoover.research import bls_experiment


class ExperimentTests(unittest.TestCase):
    def fixture(self, directory, url=None):
        request = json.dumps({"seriesid": ["CUUR0000SA0", "LNS14000000"],
                              "startyear": "2001", "endyear": "2001"}).encode()
        series = []
        for sid in ("CUUR0000SA0", "LNS14000000"):
            series.append({"seriesID": sid, "data": [
                {"year": "2001", "period": "M01", "periodName": "January",
                 "value": "100", "footnotes": []},
                {"year": "2001", "period": "M02", "periodName": "February",
                 "value": "110", "footnotes": []}]})
        raw = json.dumps({"status": "REQUEST_SUCCEEDED",
                          "Results": {"series": series}}).encode()
        capture = {"http_status": 200, "complete_body": True, "method": "POST",
                   "url": url or "https://api.bls.gov/publicAPI/v1/timeseries/data/",
                   "response_bytes": len(raw),
                   "response_sha256": hashlib.sha256(raw).hexdigest(),
                   "request_sha256": hashlib.sha256(request).hexdigest(),
                   "started_utc": "2001-03-01T00:00:00Z",
                   "finished_utc": "2001-03-01T00:00:01Z"}
        files = {"raw": raw, "request": request,
                 "capture": json.dumps(capture).encode()}
        for name, body in files.items():
            (directory / name).write_bytes(body)
        return files

    def invoke(self, directory):
        return subprocess.run(
            [sys.executable, "-m", "datahoover.research.bls_experiment",
             "--input-schema", "bls-v1-capture", "--raw", str(directory / "raw"),
             "--capture", str(directory / "capture"),
             "--request", str(directory / "request")],
            env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")},
            capture_output=True, text=True, check=False)

    def test_cli_consumes_capture_and_preserves_evidence(self):
        with tempfile.TemporaryDirectory() as folder:
            directory = Path(folder)
            originals = self.fixture(directory)
            first = self.invoke(directory)
            self.assertEqual(first.returncode, 0, first.stderr)
            result = json.loads(first.stdout)
            self.assertFalse(result["historical_asof_eligible"])
            self.assertFalse(result["production_integration"])
            self.assertEqual(len(result["features"]), 8)
            self.assertEqual(len(result["absent_slots"]), 20)
            self.assertEqual(result["features"][2]["exact_rational"],
                             {"numerator": "10", "denominator": "1"})
            self.assertEqual(first.stdout, self.invoke(directory).stdout)
            self.assertEqual({p.name: p.read_bytes() for p in directory.iterdir()}, originals)

    def test_tampered_bytes_fail_without_result(self):
        with tempfile.TemporaryDirectory() as folder:
            directory = Path(folder)
            self.fixture(directory)
            with (directory / "raw").open("ab") as stream:
                stream.write(b" ")
            result = self.invoke(directory)
            self.assertEqual(result.returncode, 2)
            self.assertEqual(result.stdout, "")

    def test_does_not_impersonate_v2_connector_adapter(self):
        with tempfile.TemporaryDirectory() as folder:
            directory = Path(folder)
            self.fixture(directory, "https://api.bls.gov/publicAPI/v2/timeseries/data/")
            result = self.invoke(directory)
            self.assertEqual(result.returncode, 2)
            self.assertEqual(result.stdout, "")

    def test_input_size_bound(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "oversized"
            path.write_bytes(b"x" * (bls_experiment.MAX_INPUT_BYTES + 1))
            with self.assertRaises(ValueError):
                bls_experiment.read_bounded(path)
