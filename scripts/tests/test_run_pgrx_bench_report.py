"""Regression tests for the single-command pgrx benchmark HTML report."""

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).parents[2]
RUNNER = ROOT / "scripts" / "run-pgrx-bench.sh"


class PgrxBenchReportTest(unittest.TestCase):
    def test_report_has_environment_percentiles_comparison_and_history_columns(self) -> None:
        summary = {
            "group_name": "current-run",
            "compare_group_name": None,
            "benchmarks": [
                {
                    "bench_name": "plain_heap_pk_lookup",
                    "status": "ok",
                    "primary_estimate": {
                        "point_estimate_ns": 100.0,
                        "ci_lower_bound_ns": 90.0,
                        "ci_upper_bound_ns": 110.0,
                    },
                    "comparison": None,
                },
                {
                    "bench_name": "managed_hot_pk_lookup",
                    "status": "ok",
                    "primary_estimate": {
                        "point_estimate_ns": 125.0,
                        "ci_lower_bound_ns": 120.0,
                        "ci_upper_bound_ns": 130.0,
                    },
                    "comparison": None,
                },
            ],
            "missing_from_current": [],
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            fake_bin = temp / "bin"
            fake_bin.mkdir()
            fake_cargo = fake_bin / "cargo"
            fake_cargo.write_text(
                "#!/bin/sh\n"
                "cat <<'JSON'\n"
                f"{json.dumps(summary)}\n"
                "JSON\n",
                encoding="utf-8",
            )
            fake_cargo.chmod(0o755)

            output_dir = temp / "results"
            archive_dir = temp / "archive"
            previous_dir = archive_dir / "pg16" / "2026-08-01T12"
            previous_dir.mkdir(parents=True)
            (previous_dir / "report-data.json").write_text(
                json.dumps({
                    "metadata": {"created_at": "2026-08-01T12:00:00Z"},
                    "summary": {
                        "group_name": "previous-run",
                        "benchmarks": [
                            {
                                "bench_name": "plain_heap_pk_lookup",
                                "status": "ok",
                                "primary_estimate": {"point_estimate_ns": 80.0},
                            },
                            {
                                "bench_name": "managed_hot_pk_lookup",
                                "status": "ok",
                                "primary_estimate": {"point_estimate_ns": 100.0},
                            },
                        ],
                    },
                }),
                encoding="utf-8",
            )
            env = os.environ.copy()
            env["PATH"] = f"{fake_bin}{os.pathsep}{os.defpath}"
            env["KOLDSTORE_BENCH_REPO_RESULTS_DIR"] = str(archive_dir)
            completed = subprocess.run(
                [str(RUNNER), "16", "--output-dir", str(output_dir)],
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            report = next(output_dir.glob("run-*/report.html")).read_text(encoding="utf-8")

            completed_again = subprocess.run(
                [str(RUNNER), "16", "--output-dir", str(output_dir)],
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(completed_again.returncode, 0, completed_again.stderr)
            self.assertEqual(len(list((archive_dir / "pg16").iterdir())), 2)
            self.assertTrue((archive_dir / "index.html").is_file())
            self.assertTrue((archive_dir / "index.json").is_file())
            archived_report_data = list((archive_dir / "pg16").glob("*/report-data.json"))
            self.assertEqual(len(archived_report_data), 2)

        self.assertIn("Machine", report)
        self.assertIn("p50", report)
        self.assertIn("p90", report)
        self.assertIn("p99", report)
        self.assertIn("PG only", report)
        self.assertIn("KoldStore vs PG", report)
        self.assertIn("100.00 ns", report)
        self.assertIn("+25.00%", report)
        self.assertIn("baseline", report)
        self.assertIn("Prev1: 2026-08-01 12:00:00", report)
        self.assertIn("Prev1", report)
        self.assertIn("Prev2", report)
        self.assertIn("Prev3", report)


if __name__ == "__main__":
    unittest.main()
