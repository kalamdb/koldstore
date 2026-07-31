"""Tests for repeated storage-comparison result aggregation."""

import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "render-storage-comparison-results.py"
SPEC = importlib.util.spec_from_file_location("storage_results_renderer", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
renderer = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(renderer)


def report(update_speed: int, *, dirty: bool = False) -> dict:
    return {
        "mode": "async",
        "generated_at": "2026-07-20T12:00:00+00:00",
        "git_commit": "abc123",
        "git_dirty": dirty,
        "rows": 100_000,
        "hot_limit": 10_000,
        "dml_sample": 50_000,
        "insert_batch_rows": 100_000,
        "max_rows_per_file": 1_000_000,
        "warmup_rows": 100_000,
        "main": [],
        "detail": [
            {
                "metric": "update speed†",
                "postgres_only": "—",
                "koldstore": f"{update_speed} ops/s (19 µs/op)",
            }
        ],
    }


def mode_report(mode: str, sample_count: int = 6) -> dict:
    value = report(52_000)
    value["mode"] = mode
    value["sample_count"] = sample_count
    return value


def full_side_report(mode: str) -> dict:
    """Minimal main/detail cells for glance-table rendering."""
    if mode == "pg":
        return {
            "mode": "pg",
            "generated_at": "2026-07-31T12:00:00+00:00",
            "git_commit": "abc123",
            "git_dirty": False,
            "rows": 100_000,
            "hot_limit": 10_000,
            "dml_sample": 50_000,
            "insert_batch_rows": 100_000,
            "max_rows_per_file": 1_000_000,
            "warmup_rows": 100_000,
            "sample_count": 1,
            "main": [
                {
                    "metric": "total hot+cold storage",
                    "postgres_only": "5.85 GiB",
                    "koldstore": "—",
                },
                {
                    "metric": "local PostgreSQL storage",
                    "postgres_only": "5.85 GiB",
                    "koldstore": "—",
                },
                {
                    "metric": "VACUUM duration",
                    "postgres_only": "227.02 s",
                    "koldstore": "—",
                },
            ],
            "detail": [
                {
                    "metric": "index storage (hot + __cl)",
                    "postgres_only": "414.86 MiB",
                    "koldstore": "—",
                },
                {
                    "metric": "└ cold Parquet",
                    "postgres_only": "—",
                    "koldstore": "—",
                },
            ],
        }
    return {
        "mode": "async",
        "generated_at": "2026-07-31T12:10:00+00:00",
        "git_commit": "abc123",
        "git_dirty": False,
        "rows": 100_000,
        "hot_limit": 10_000,
        "dml_sample": 50_000,
        "insert_batch_rows": 100_000,
        "max_rows_per_file": 1_000_000,
        "warmup_rows": 100_000,
        "sample_count": 1,
        "main": [
            {
                "metric": "total hot+cold storage",
                "postgres_only": "—",
                "koldstore": "670.75 MiB",
            },
            {
                "metric": "local PostgreSQL storage",
                "postgres_only": "—",
                "koldstore": "72.23 MiB",
            },
            {
                "metric": "VACUUM duration",
                "postgres_only": "—",
                "koldstore": "3.21 s",
            },
        ],
        "detail": [
            {
                "metric": "index storage (hot + __cl)",
                "postgres_only": "—",
                "koldstore": "11.45 MiB",
            },
            {
                "metric": "└ cold Parquet",
                "postgres_only": "—",
                "koldstore": "598.52 MiB",
            },
        ],
    }


class AggregateReportsTest(unittest.TestCase):
    def test_load_report_rejects_a_missing_requested_sample(self) -> None:
        missing = Path("/definitely/missing/storage-comparison.json")

        with self.assertRaisesRegex(FileNotFoundError, "storage-comparison"):
            renderer.load_report([missing])

    def test_uses_metric_median_and_records_range(self) -> None:
        aggregate = renderer.aggregate_reports(
            [report(52_000), report(48_000), report(55_000)]
        )

        self.assertEqual(aggregate["sample_count"], 3)
        self.assertEqual(
            aggregate["detail"][0]["koldstore"], "52000 ops/s (19 µs/op)"
        )
        self.assertEqual(
            aggregate["sample_dispersion"]["detail.update speed†.koldstore"],
            {"min": "48000 ops/s (19 µs/op)", "max": "55000 ops/s (19 µs/op)"},
        )

    def test_rejects_dirty_or_mismatched_samples(self) -> None:
        with self.assertRaisesRegex(ValueError, "dirty"):
            renderer.aggregate_reports([report(52_000), report(48_000, dirty=True)])

        mismatched = report(48_000)
        mismatched["rows"] = 200_000
        with self.assertRaisesRegex(ValueError, "rows"):
            renderer.aggregate_reports([report(52_000), mismatched])

    def test_comparison_rejects_mismatched_counts_and_metadata(self) -> None:
        pg = mode_report("pg")
        async_report = mode_report("async", sample_count=12)
        with self.assertRaisesRegex(ValueError, "sample_count"):
            renderer.validate_comparison_reports(pg, async_report)

        async_report["sample_count"] = 6
        async_report["rows"] = 200_000
        with self.assertRaisesRegex(ValueError, "rows"):
            renderer.validate_comparison_reports(pg, async_report)

    def test_storage_wins_glance_computes_tradeoffs(self) -> None:
        glance = renderer.render_storage_wins_glance(
            full_side_report("pg"), full_side_report("async")
        )
        self.assertIn("5.85 GiB → 670.75 MiB", glance)
        self.assertIn("**89% smaller**", glance)
        self.assertIn("**71× faster**", glance)
        self.assertIn("599 MiB", glance)


if __name__ == "__main__":
    unittest.main()
