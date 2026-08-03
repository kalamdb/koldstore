#!/usr/bin/env python3
"""Archive one pgrx benchmark report per PostgreSQL/date-hour bucket."""

from __future__ import annotations

import argparse
import html
import json
import re
import shutil
from pathlib import Path


ARCHIVED_FILES = ("report.html", "report-data.json", "results.json", "history.json")
ARCHIVE_KEY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}$")


def read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def archive_run(source_dir: Path, archive_root: Path, pg_version: str, archive_key: str) -> Path:
    if not ARCHIVE_KEY_RE.fullmatch(archive_key):
        raise ValueError(f"archive key must be YYYY-MM-DDTHH, got {archive_key!r}")

    target_dir = archive_root / f"pg{pg_version}" / archive_key
    target_dir.mkdir(parents=True, exist_ok=True)
    for filename in ARCHIVED_FILES:
        source = source_dir / filename
        if source.is_file():
            shutil.copy2(source, target_dir / filename)
    return target_dir


def build_index(archive_root: Path) -> None:
    runs = []
    for pg_dir in sorted(archive_root.glob("pg*")):
        if not pg_dir.is_dir():
            continue
        for run_dir in sorted(pg_dir.iterdir(), reverse=True):
            report_data = run_dir / "report-data.json"
            if not run_dir.is_dir() or not report_data.is_file():
                continue
            data = read_json(report_data)
            metadata = data.get("metadata") or {}
            summary = data.get("summary") or {}
            benchmarks = summary.get("benchmarks") or []
            statuses = {benchmark.get("status") for benchmark in benchmarks}
            if statuses == {"ok"}:
                status = "ok"
            elif not benchmarks:
                status = "empty"
            else:
                status = "partial"
            runs.append(
                {
                    "pg_version": pg_dir.name.removeprefix("pg"),
                    "archive_key": run_dir.name,
                    "created_at": metadata.get("created_at") or run_dir.name,
                    "hostname": metadata.get("hostname") or "—",
                    "git_commit": metadata.get("git_commit") or "—",
                    "status": status,
                    "benchmark_count": len(benchmarks),
                    "report": f"{pg_dir.name}/{run_dir.name}/report.html",
                }
            )

    runs.sort(key=lambda run: (run["archive_key"], run["pg_version"]), reverse=True)
    archive_root.mkdir(parents=True, exist_ok=True)
    (archive_root / "index.json").write_text(
        json.dumps(runs, indent=2) + "\n", encoding="utf-8"
    )

    rows = []
    for run in runs:
        rows.append(
            "<tr>"
            f"<td>{html.escape(run['archive_key'])}</td>"
            f"<td>{html.escape(run['pg_version'])}</td>"
            f"<td>{html.escape(run['status'])}</td>"
            f"<td>{run['benchmark_count']}</td>"
            f"<td>{html.escape(run['hostname'])}</td>"
            f"<td><code>{html.escape(run['git_commit'])}</code></td>"
            f"<td><a href=\"{html.escape(run['report'])}\">report</a></td>"
            "</tr>"
        )
    if not rows:
        rows.append('<tr><td colspan="7">No archived benchmark runs yet.</td></tr>')

    body = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>pg_koldstore benchmark archive</title>
<style>
body {{ font: 15px system-ui, sans-serif; margin: 2rem; color: #202124; }}
table {{ border-collapse: collapse; }}
th, td {{ border: 1px solid #dadce0; padding: .55rem .7rem; text-align: left; }}
th {{ background: #f1f3f4; }}
code {{ background: #f1f3f4; padding: .1rem .25rem; }}
</style></head><body>
<h1>pg_koldstore benchmark archive</h1>
<p>One result bucket is retained per PostgreSQL version and UTC date-hour. Re-running within the same hour replaces that bucket.</p>
<table><thead><tr><th>Date-hour (UTC)</th><th>PostgreSQL</th><th>Status</th><th>Benchmarks</th><th>Machine</th><th>Commit</th><th>Report</th></tr></thead>
<tbody>{''.join(rows)}</tbody></table>
</body></html>
"""
    (archive_root / "index.html").write_text(body, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--archive-root", type=Path, required=True)
    parser.add_argument("--pg-version", required=True)
    parser.add_argument("--archive-key", required=True)
    args = parser.parse_args()

    archive_run(args.source_dir, args.archive_root, args.pg_version, args.archive_key)
    build_index(args.archive_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
