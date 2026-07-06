#!/usr/bin/env python3
"""Generate pgKalam benchmark JSON, Markdown, and HTML reports."""

from __future__ import annotations

import argparse
import datetime as dt
import glob
import html
import json
import math
import os
import re
from pathlib import Path
from typing import Any


MODES = ["baseline", "extension-hot", "extension-hot-cold", "extension-cold-only"]
MODE_LABELS = {
    "baseline": "Baseline",
    "extension-hot": "Extension hot-only",
    "extension-hot-cold": "Extension hot+cold",
    "extension-cold-only": "Extension cold-only",
}
BENCHMARK_LABELS = {
    "single_hot_query": "Single hot query",
    "batch_hot_query": "Batch hot query",
    "hot_cold_query": "Hot+cold query",
    "cold_only_query": "Cold-only query",
    "cold_miss_query": "Cold miss query",
    "single_insert": "Single insert",
    "batch_insert_100": "Batch insert 100",
    "batch_insert_500": "Batch insert 500",
    "batch_insert_1000": "Batch insert 1000",
    "single_update": "Single update",
    "batch_update": "Batch update",
    "single_delete": "Single delete",
    "batch_delete": "Batch delete",
    "mixed_20_clients": "Mixed 20 clients",
}
ROW_ESTIMATES = {
    "single_hot_query": 50,
    "batch_hot_query": 200,
    "hot_cold_query": 500,
    "cold_only_query": 500,
    "cold_miss_query": 100,
    "single_insert": 1,
    "batch_insert_100": 100,
    "batch_insert_500": 500,
    "batch_insert_1000": 1000,
    "single_update": 1,
    "batch_update": 40,
    "single_delete": 1,
    "batch_delete": 40,
    "mixed_20_clients": 50,
}


def benchmark_version() -> str:
    env_version = os.environ.get("KOLDSTORE_BENCH_VERSION")
    if env_version:
        return env_version

    cargo_toml = Path(__file__).resolve().parents[2] / "Cargo.toml"
    if not cargo_toml.exists():
        return "unknown-version"

    in_workspace_package = False
    for raw_line in cargo_toml.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line.startswith("[") and line.endswith("]"):
            in_workspace_package = line == "[workspace.package]"
            continue
        if in_workspace_package and line.startswith("version"):
            match = re.search(r'"([^"]+)"', line)
            if match:
                return match.group(1)
    return "unknown-version"


def safe_filename_part(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-")
    return safe or "unknown-version"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-dir", default="benchmarks/results")
    args = parser.parse_args()

    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)

    generated_at = dt.datetime.now(dt.timezone.utc)
    version = benchmark_version()
    timestamp = generated_at.strftime("%Y%m%dT%H%M%SZ")
    timestamped_html_name = f"{safe_filename_part(version)}-{timestamp}.html"
    benchmark_results = load_benchmark_results(results_dir)
    sizes = load_size_stats(results_dir)
    system = load_system_stats(results_dir)
    summary = {
        "suite": "pgKalam PostgreSQL extension benchmarks",
        "version": version,
        "generated_at": generated_at.isoformat(),
        "html_report": timestamped_html_name,
        "notes": [
            "pgbench latency percentiles are derived from pgbench per-transaction logs.",
            "Rows processed are estimates based on each benchmark's LIMIT or batch size.",
            "Memory and CPU fields are approximate process measurements from ps/pgrep.",
            "Baseline and extension hot-only run the full pgbench workload suite.",
            "Extension hot+cold and cold-only are storage-focused modes: the harness verifies flush output, "
            "prunes flushed hot rows for the size snapshot, and skips long pgbench workloads.",
            "Cold storage snapshots reflect benchmark-managed prune-after-flush, so they measure storage savings "
            "instead of catalog-check overhead on still-hot rows.",
            "DML benchmarks are marked N/A for cold-only mode when workloads are enabled; in storage-only modes they are not run.",
            "Size snapshots are taken after setup and compaction. For flushed storage modes this means "
            "seed + indexes + flush + verified prune + compaction.",
            "Hot heap/table/index sizes come from PostgreSQL pg_relation_size, pg_table_size, and pg_indexes_size; "
            "only cold storage size comes from the local cold-storage directory.",
            "GitHub Actions results are useful for trend checks, not absolute machine performance numbers.",
        ],
        "benchmarks": benchmark_results,
        "sizes": sizes,
        "system": system,
    }

    (results_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    (results_dir / "report.md").write_text(render_markdown(summary), encoding="utf-8")
    html_report = render_html(summary)
    (results_dir / "report.html").write_text(html_report, encoding="utf-8")
    (results_dir / timestamped_html_name).write_text(html_report, encoding="utf-8")
    print(f"wrote HTML report: {results_dir / timestamped_html_name}")


def load_benchmark_results(results_dir: Path) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for metadata_path in sorted((results_dir / "raw").glob("*/*.json")):
        if metadata_path.name.startswith("db.") or ".system." in metadata_path.name:
            continue
        metadata = read_json(metadata_path)
        if not isinstance(metadata, dict) or "benchmark" not in metadata:
            continue
        seen.add((metadata["mode"], metadata["benchmark"]))
        if metadata.get("status") != "completed":
            entries.append(
                {
                    "mode": metadata["mode"],
                    "benchmark": metadata["benchmark"],
                    "label": BENCHMARK_LABELS.get(metadata["benchmark"], metadata["benchmark"]),
                    "status": metadata.get("status", "unknown"),
                    "reason": metadata.get("reason", metadata.get("status", "unknown")),
                    "stdout_path": relative_to_results(metadata.get("stdout"), results_dir),
                    "stderr_path": relative_to_results(metadata.get("stderr"), results_dir),
                }
            )
            continue

        latencies = read_pgbench_latencies(Path(metadata["log_prefix"]))
        stdout = Path(metadata["stdout"]).read_text(encoding="utf-8", errors="replace")
        seconds = float(metadata["seconds"])
        transactions = len(latencies)
        avg_ms = parse_float(stdout, r"latency average =\s+([0-9.]+)") or average(latencies)
        tps = parse_float(stdout, r"tps =\s+([0-9.]+)") or (transactions / seconds if seconds > 0 else 0.0)
        row_estimate = ROW_ESTIMATES.get(metadata["benchmark"], 1)
        entries.append(
            {
                "mode": metadata["mode"],
                "benchmark": metadata["benchmark"],
                "label": BENCHMARK_LABELS.get(metadata["benchmark"], metadata["benchmark"]),
                "status": "completed",
                "transactions": transactions,
                "latency_avg_ms": avg_ms,
                "latency_p50_ms": percentile(latencies, 0.50),
                "latency_p95_ms": percentile(latencies, 0.95),
                "latency_p99_ms": percentile(latencies, 0.99),
                "transactions_per_second": tps,
                "rows_processed_estimate": transactions * row_estimate,
                "clients": metadata["clients"],
                "jobs": metadata["jobs"],
                "seconds": metadata["seconds"],
                "plan_path": relative_to_results(metadata.get("plan"), results_dir),
                "stdout_path": relative_to_results(metadata.get("stdout"), results_dir),
                "stderr_path": relative_to_results(metadata.get("stderr"), results_dir),
            }
        )
    entries.extend(load_orphan_failed_results(results_dir, seen))
    return entries


def load_orphan_failed_results(
    results_dir: Path,
    seen: set[tuple[str, str]],
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for stderr_path in sorted((results_dir / "raw").glob("*/*.err")):
        mode = stderr_path.parent.name
        benchmark = stderr_path.stem
        if (mode, benchmark) in seen:
            continue
        stderr_text = stderr_path.read_text(encoding="utf-8", errors="replace")
        if "error:" not in stderr_text.lower():
            continue
        stdout_path = stderr_path.with_suffix(".out")
        entries.append(
            {
                "mode": mode,
                "benchmark": benchmark,
                "label": BENCHMARK_LABELS.get(benchmark, benchmark),
                "status": "failed",
                "reason": "pgbench failed before metadata was written; see stderr_path",
                "stdout_path": relative_to_results(str(stdout_path), results_dir)
                if stdout_path.exists()
                else None,
                "stderr_path": relative_to_results(str(stderr_path), results_dir),
            }
        )
    return entries


def load_size_stats(results_dir: Path) -> list[dict[str, Any]]:
    """Load size stats using db.before.json (post-setup, pre-benchmark).

    Using the pre-benchmark snapshot gives a fair cross-mode comparison: every
    mode starts with the same 100k-row seed, so the only differences are the
    extension setup cost and cold storage written by flush_table.  db.after.json
    reflects DML-bloated tables which vary wildly between modes.
    """
    sizes = []
    for mode in MODES:
        path = results_dir / "raw" / mode / "db.before.json"
        if not path.exists():
            path = results_dir / "raw" / mode / "db.after.json"
        if path.exists():
            payload = read_json(path)
            if isinstance(payload, dict):
                payload.setdefault(
                    "measurement_note",
                    "Size snapshot taken after setup. Flushed storage modes include verified prune + compaction before the snapshot.",
                )
                sizes.append(payload)
    return sizes


def load_system_stats(results_dir: Path) -> list[dict[str, Any]]:
    by_mode = []
    for mode in MODES:
        records = []
        for path in sorted((results_dir / "raw" / mode).glob("*.system.after.json")):
            payload = read_json(path)
            if isinstance(payload, dict):
                records.append(payload)
        if not records:
            continue
        rss_values = [int(record.get("approx_postgres_rss_bytes") or 0) for record in records]
        cpu_values = [
            float(record["approx_postgres_cpu_seconds"])
            for record in records
            if isinstance(record.get("approx_postgres_cpu_seconds"), (int, float))
        ]
        by_mode.append(
            {
                "mode": mode,
                "avg_memory_bytes": int(sum(rss_values) / len(rss_values)) if rss_values else 0,
                "peak_memory_bytes": max(rss_values) if rss_values else 0,
                "cpu_approx_seconds": max(cpu_values) if cpu_values else None,
                "measurement_note": "Approximate process RSS and CPU time from ps/pgrep.",
            }
        )
    return by_mode


def read_pgbench_latencies(log_prefix: Path) -> list[float]:
    latencies = []
    prefix_name = log_prefix.name + "."
    for path_name in glob.glob(str(log_prefix) + ".*"):
        path = Path(path_name)
        suffix = path.name.removeprefix(prefix_name)
        if not suffix.isdigit():
            continue
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            parts = line.split()
            if len(parts) >= 3:
                try:
                    latencies.append(float(parts[2]) / 1000.0)
                except ValueError:
                    pass
    return latencies


def render_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# pgKalam Benchmark Report",
        "",
        f"Generated: `{summary['generated_at']}`",
        "",
        "## PostgreSQL pgbench Comparison",
        "",
        "| Benchmark | Baseline TPS | Hot-only TPS | Hot+cold TPS | Cold-only TPS | Hot-only p95 change |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    by_benchmark = index_benchmarks(summary["benchmarks"])
    for benchmark, label in BENCHMARK_LABELS.items():
        baseline = by_benchmark.get((benchmark, "baseline"))
        hot = by_benchmark.get((benchmark, "extension-hot"))
        hot_cold = by_benchmark.get((benchmark, "extension-hot-cold"))
        cold = by_benchmark.get((benchmark, "extension-cold-only"))
        lines.append(
            "| {} | {} | {} | {} | {} | {} |".format(
                label,
                baseline_tps_cell(baseline),
                tps_comparison_cell(baseline, hot),
                tps_comparison_cell(baseline, hot_cold),
                tps_comparison_cell(baseline, cold),
                latency_comparison_cell(baseline, hot),
            )
        )

    lines.extend(
        [
            "",
            "## Size Comparison",
            "",
            "*(snapshot taken after setup and compaction; flushed storage modes include verified prune-before-snapshot)*",
            "",
            "| Mode | Hot heap (PG) | Hot table total (PG) | Hot indexes (PG) | Cold storage | Dead tuples est. | Extension metadata |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    sizes_by_mode = {size["mode"]: size for size in summary["sizes"]}
    for mode in MODES:
        size = sizes_by_mode.get(mode)
        lines.append(
            "| {} | {} | {} | {} | {} | {} | {} |".format(
                MODE_LABELS.get(mode, mode),
                bytes_cell(
                    size.get("heap_size_bytes", size.get("table_size_bytes")) if size else None
                ),
                bytes_cell(size.get("table_size_bytes") if size else None),
                bytes_cell(size.get("index_size_bytes") if size else None),
                bytes_cell(size.get("cold_storage_size_bytes") if size else None),
                integer_cell(size.get("dead_tuples_estimate") if size else None),
                bytes_cell(size.get("extension_metadata_size_bytes") if size else None),
            )
        )

    lines.extend(
        [
            "",
            "## System Comparison",
            "",
            "| Mode | Avg memory | Peak memory | CPU approx |",
            "|---|---:|---:|---:|",
        ]
    )
    system_by_mode = {system["mode"]: system for system in summary["system"]}
    for mode in MODES:
        system = system_by_mode.get(mode)
        lines.append(
            "| {} | {} | {} | {} |".format(
                MODE_LABELS.get(mode, mode),
                bytes_cell(system.get("avg_memory_bytes") if system else None),
                bytes_cell(system.get("peak_memory_bytes") if system else None),
                seconds_cell(system.get("cpu_approx_seconds") if system else None),
            )
        )

    # n/a entries are expected architecture decisions; exclude them from the anomaly list.
    anomalies = [
        e for e in summary["benchmarks"]
        if e["status"] not in ("completed", "n/a")
    ]
    na_entries = [e for e in summary["benchmarks"] if e["status"] == "n/a"]
    if anomalies:
        lines.extend(["", "## Skipped or Failed", ""])
        for entry in anomalies:
            lines.append(
                f"- `{entry['mode']}` / `{entry['benchmark']}`: "
                f"{entry.get('status', 'unknown')} — {entry.get('reason', 'no reason recorded')}"
            )
    if na_entries:
        lines.extend(["", "## Not Applicable (cold-only archive mode)", ""])
        lines.append(
            "DML benchmarks are not run in cold-only mode because the archive tier "
            "is read-only by design. The following were skipped:"
        )
        for entry in na_entries:
            lines.append(f"- `{entry['benchmark']}`")

    lines.extend(["", "## Notes", ""])
    for note in summary["notes"]:
        lines.append(f"- {note}")
    lines.append("")
    return "\n".join(lines)


def render_html(summary: dict[str, Any]) -> str:
    markdown = render_markdown(summary)
    body = markdown_to_simple_html(markdown)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>pgKalam Benchmark Report</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 2rem; color: #172033; }}
    table {{ border-collapse: collapse; width: 100%; margin: 1rem 0 2rem; }}
    th, td {{ border: 1px solid #d8dee9; padding: 0.45rem 0.6rem; text-align: right; }}
    th:first-child, td:first-child {{ text-align: left; }}
    th {{ background: #f4f6f8; }}
    code {{ background: #f4f6f8; padding: 0.1rem 0.25rem; border-radius: 4px; }}
    li {{ margin: 0.25rem 0; }}
    .badge {{ border-radius: 999px; display: inline-block; font-size: 0.78rem; font-weight: 650; margin-left: 0.35rem; padding: 0.12rem 0.45rem; }}
    .badge-good {{ background: #d9f8df; color: #116329; }}
    .badge-bad {{ background: #ffe0e0; color: #a11919; }}
    .badge-neutral {{ background: #eceff3; color: #57606a; }}
  </style>
</head>
<body>
{body}
</body>
</html>
"""


def markdown_to_simple_html(markdown: str) -> str:
    lines = markdown.splitlines()
    html_lines: list[str] = []
    table: list[str] = []
    in_list = False
    for line in lines:
        if line.startswith("|"):
            table.append(line)
            continue
        if table:
            html_lines.append(render_markdown_table(table))
            table = []
        if in_list and not line.startswith("- "):
            html_lines.append("</ul>")
            in_list = False
        if line.startswith("# "):
            html_lines.append(f"<h1>{html.escape(line[2:])}</h1>")
        elif line.startswith("## "):
            html_lines.append(f"<h2>{html.escape(line[3:])}</h2>")
        elif line.startswith("- "):
            if not in_list:
                html_lines.append("<ul>")
                in_list = True
            html_lines.append(f"<li>{inline_markdown(line[2:])}</li>")
        elif line:
            html_lines.append(f"<p>{inline_markdown(line)}</p>")
    if table:
        html_lines.append(render_markdown_table(table))
    if in_list:
        html_lines.append("</ul>")
    return "\n".join(html_lines)


def render_markdown_table(rows: list[str]) -> str:
    parsed = [[cell.strip() for cell in row.strip("|").split("|")] for row in rows]
    if len(parsed) >= 2 and all(set(cell) <= {"-", ":"} for cell in parsed[1]):
        header = parsed[0]
        body = parsed[2:]
    else:
        header = parsed[0]
        body = parsed[1:]
    out = ["<table><thead><tr>"]
    out.extend(f"<th>{inline_markdown(cell)}</th>" for cell in header)
    out.append("</tr></thead><tbody>")
    for row in body:
        out.append("<tr>")
        out.extend(f"<td>{inline_markdown(cell)}</td>" for cell in row)
        out.append("</tr>")
    out.append("</tbody></table>")
    return "".join(out)


def inline_markdown(text: str) -> str:
    escaped = html.escape(text)
    escaped = re.sub(r"`([^`]+)`", r"<code>\1</code>", escaped)
    return re.sub(
        r"\[\[(good|bad|neutral):([^\]]+)\]\]",
        r'<span class="badge badge-\1">\2</span>',
        escaped,
    )


def index_benchmarks(entries: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    return {(entry["benchmark"], entry["mode"]): entry for entry in entries}


def baseline_tps_cell(entry: dict[str, Any] | None) -> str:
    if entry is None:
        return "not run [[neutral:—]]"
    status = entry.get("status", "unknown")
    if status == "n/a":
        return "N/A [[neutral:archive mode]]"
    if status == "skipped":
        return "skipped [[neutral:—]]"
    if status == "failed":
        return "failed [[bad:error]]"
    if status != "completed":
        return f"{status} [[neutral:—]]"
    return f"{entry.get('transactions_per_second', 0.0):.2f}"


def tps_comparison_cell(
    baseline: dict[str, Any] | None,
    extension: dict[str, Any] | None,
) -> str:
    if extension is None:
        return "not run [[neutral:—]]"
    status = extension.get("status", "unknown")
    if status == "n/a":
        return "N/A [[neutral:archive mode]]"
    if status == "skipped":
        return "skipped [[neutral:—]]"
    if status == "failed":
        return "failed [[bad:error]]"
    if status != "completed":
        return f"{status} [[neutral:—]]"
    tps = extension.get("transactions_per_second", 0.0)
    if not baseline or baseline.get("status") != "completed":
        return f"{tps:.2f} [[neutral:no baseline]]"
    base_tps = baseline.get("transactions_per_second") or 0
    if base_tps <= 0:
        return f"{tps:.2f} [[neutral:no baseline]]"
    change_pct = ((tps - base_tps) / base_tps) * 100.0
    return f"{tps:.2f} {change_badge(change_pct)}"


def latency_comparison_cell(
    baseline: dict[str, Any] | None,
    extension: dict[str, Any] | None,
) -> str:
    if extension is None:
        return "not run [[neutral:—]]"
    status = extension.get("status", "unknown")
    if status == "n/a":
        return "N/A [[neutral:archive mode]]"
    if status == "skipped":
        return "skipped [[neutral:—]]"
    if status == "failed":
        return "failed [[bad:error]]"
    if status != "completed":
        return f"{status} [[neutral:—]]"
    if not baseline or baseline.get("status") != "completed":
        return "no baseline [[neutral:—]]"
    base_p95 = baseline.get("latency_p95_ms") or 0
    ext_p95 = extension.get("latency_p95_ms") or 0
    if base_p95 <= 0:
        return "no baseline [[neutral:—]]"
    change_pct = ((base_p95 - ext_p95) / base_p95) * 100.0
    return f"{ext_p95:.3f} ms {change_badge(change_pct)}"


def change_badge(change_pct: float) -> str:
    label = f"{change_pct:+.1f}%"
    if change_pct > 0:
        return f"[[good:{label}]]"
    if change_pct < -20.0:
        return f"[[bad:{label}]]"
    return f"[[neutral:{label}]]"


def bytes_cell(value: Any) -> str:
    if value is None:
        return "not collected [[neutral:n/a]]"
    value = float(value)
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    unit = 0
    while value >= 1024 and unit < len(units) - 1:
        value /= 1024
        unit += 1
    return f"{value:.2f} {units[unit]}"


def seconds_cell(value: Any) -> str:
    if value is None:
        return "not collected [[neutral:n/a]]"
    return f"{float(value):.2f}s"


def integer_cell(value: Any) -> str:
    if value is None:
        return "not collected [[neutral:n/a]]"
    return f"{int(value):,}"


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    index = int(round((len(values) - 1) * pct))
    return values[index]


def average(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def parse_float(text: str, pattern: str) -> float | None:
    match = re.search(pattern, text)
    if not match:
        return None
    try:
        value = float(match.group(1))
    except ValueError:
        return None
    if math.isfinite(value):
        return value
    return None


def relative_to_results(path: str | None, results_dir: Path) -> str | None:
    if not path:
        return None
    try:
        return str(Path(path).resolve().relative_to(results_dir.resolve()))
    except ValueError:
        return path


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    main()
