#!/usr/bin/env python3
"""Merge async/strict storage-comparison JSON snapshots into RESULTS.md."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


MISSING = "—"


def load_report(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def cell(report: dict[str, Any] | None, section: str, metric: str, field: str) -> str:
    if report is None:
        return MISSING
    for row in report.get(section, []):
        if row.get("metric") == metric:
            value = row.get(field, MISSING)
            return MISSING if value in (None, "") else str(value)
    return MISSING


def ordered_metrics(async_report: dict[str, Any] | None, strict_report: dict[str, Any] | None, section: str) -> list[str]:
    seen: list[str] = []
    for report in (async_report, strict_report):
        if report is None:
            continue
        for row in report.get(section, []):
            metric = row.get("metric")
            if metric and metric not in seen:
                seen.append(metric)
    return seen


def pick_pg(async_report: dict[str, Any] | None, strict_report: dict[str, Any] | None, section: str, metric: str) -> str:
    reports = [r for r in (async_report, strict_report) if r is not None]
    reports.sort(key=lambda r: r.get("generated_at", ""), reverse=True)
    for report in reports:
        value = cell(report, section, metric, "postgres_only")
        if value != MISSING:
            return value
    return MISSING


def render_table(
    label: str,
    section: str,
    async_report: dict[str, Any] | None,
    strict_report: dict[str, Any] | None,
) -> str:
    lines = [
        f"| {label} | PostgreSQL only | PG + KoldStore (async) | PG + KoldStore (strict) |",
        "| --- | --- | --- | --- |",
    ]
    for metric in ordered_metrics(async_report, strict_report, section):
        pg = pick_pg(async_report, strict_report, section, metric)
        async_val = cell(async_report, section, metric, "koldstore")
        strict_val = cell(strict_report, section, metric, "koldstore")
        lines.append(f"| {metric} | {pg} | {async_val} | {strict_val} |")
    return "\n".join(lines)


def run_meta(async_report: dict[str, Any] | None, strict_report: dict[str, Any] | None) -> str:
    source = async_report or strict_report or {}
    rows = source.get("rows", "?")
    hot = source.get("hot_limit", "?")
    dml = source.get("dml_sample", "?")
    batch = source.get("insert_batch_rows", "?")
    max_rows = source.get("max_rows_per_file", "?")
    modes = []
    if async_report is not None:
        modes.append("async")
    if strict_report is not None:
        modes.append("strict")
    mode_text = " + ".join(modes) if modes else "none"
    return (
        f"**Run:** {rows} rows · `hot_row_limit = {hot}` · `max_rows_per_file = {max_rows}` "
        f"· `--dml-sample {dml}` · `insert_batch_rows = {batch}` · zstd Parquet · "
        f"modes measured: **{mode_text}**"
    )


def render(async_report: dict[str, Any] | None, strict_report: dict[str, Any] | None) -> str:
    parts = [
        "# Latest benchmark results",
        "",
        "Published numbers from the most recent storage comparison run(s). Re-run",
        "`scripts/run-storage-comparison.sh --update-results` (once per mode, or",
        "with `--both-modes`) to refresh this file. Methodology:",
        "[README.md](README.md).",
        "",
        run_meta(async_report, strict_report),
        "",
        "Managed PostgreSQL sizes include hot heap + `koldstore.<table>__cl` + mirror",
        "indexes. Cold Parquet is outside the PostgreSQL data directory. Columns are",
        "**PostgreSQL only**, **PG + KoldStore (async)**, and **PG + KoldStore (strict)**.",
        "",
        "## Main comparison",
        "",
        render_table("Metric", "main", async_report, strict_report),
        "",
        "‡ Hot+cold PK lookups open matching Parquet segments; footer open + merge-scan",
        "setup can dominate vs a pure B-tree probe at large segment sizes. See",
        "[performance](../performance.md).",
        "",
        "## Detail (throughput and storage)",
        "",
        render_table("Operation", "detail", async_report, strict_report),
        "",
        "† Strict DML updates the change-log mirror in the foreground. Async DML",
        "records heap WAL in the foreground; catch-up rows appear only in the async",
        "column.",
        "",
    ]
    return "\n".join(parts)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--async-json", type=Path, default=None)
    parser.add_argument("--strict-json", type=Path, default=None)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()

    async_report = load_report(args.async_json)
    strict_report = load_report(args.strict_json)
    if async_report is None and strict_report is None:
        raise SystemExit("at least one of --async-json / --strict-json must exist")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(render(async_report, strict_report), encoding="utf-8")
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
