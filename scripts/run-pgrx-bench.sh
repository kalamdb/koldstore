#!/usr/bin/env bash
# Run in-process #[pg_bench] benchmarks for pg_koldstore via cargo pgrx bench.
#
# Unlike tests/e2e (client SQL over a prepared cluster) and benchmarks/
# (pgbench / Criterion), these functions execute inside a Postgres backend with
# the extension loaded. See crates/pg_koldstore/src/pg_benches/.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

usage() {
  cat <<'EOF'
Usage: scripts/run-pgrx-bench.sh [PG_VERSION] [BENCH_NAME] [options]

Runs `cargo pgrx bench` for pg_koldstore with the project feature/profile
defaults. Normal runs write JSON, a log, and an HTML report under
target/bench-results/<timestamp>/. PG_VERSION defaults to
KOLDSTORE_BENCH_PGVERSION or KOLDSTORE_E2E_PGVERSION or 16.

Examples:
  scripts/run-pgrx-bench.sh
  scripts/run-pgrx-bench.sh 16
  scripts/run-pgrx-bench.sh 16 managed_hot_count_scan
  scripts/run-pgrx-bench.sh 16 changes_since
  scripts/run-pgrx-bench.sh 16 --list
  scripts/run-pgrx-bench.sh 16 --group-name before-opt
  scripts/run-pgrx-bench.sh 16 --compare-group before-opt --group-name after-opt
  scripts/run-pgrx-bench.sh 16 --wait 10
  scripts/run-pgrx-bench.sh 16 --output-dir /tmp/koldstore-bench-results

Environment:
  KOLDSTORE_BENCH_PGVERSION   PostgreSQL major (default: E2E version or 16)
  KOLDSTORE_PGRX_BENCH_DEBUG  Set to 1 to build with --debug instead of release-pg
  KOLDSTORE_PGRX_BENCH_EXTRA  Extra args appended to cargo pgrx bench
  KOLDSTORE_BENCH_OUTPUT_DIR  Results root (default: target/bench-results)
  KOLDSTORE_BENCH_DBNAME      Benchmark database (default: koldstore_benches)
  KOLDSTORE_BENCH_DB_PORT     pgrx port (default: 28800 + PG_VERSION)
  KOLDSTORE_BENCH_REPO_RESULTS_DIR
                              Tracked archive (default: benchmarks/results/pgrx)
EOF
}

PG_VERSION="${KOLDSTORE_BENCH_PGVERSION:-${KOLDSTORE_E2E_PGVERSION:-16}}"
BENCH_NAME=""
EXTRA_ARGS=()
OUTPUT_ROOT="${KOLDSTORE_BENCH_OUTPUT_DIR:-target/bench-results}"
BENCH_DB_NAME="${KOLDSTORE_BENCH_DBNAME:-koldstore_benches}"
BENCH_DB_PORT="${KOLDSTORE_BENCH_DB_PORT:-$((28800 + PG_VERSION))}"
REPO_RESULTS_ROOT="${KOLDSTORE_BENCH_REPO_RESULTS_DIR:-benchmarks/results/pgrx}"
RUN_BENCHMARKS=1
JSON_REQUESTED=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      EXTRA_ARGS+=("$@")
      break
      ;;
    --output-dir|--html-dir)
      if [[ $# -lt 2 ]]; then
        echo "error: $1 requires a directory" >&2
        exit 2
      fi
      OUTPUT_ROOT="$2"
      shift 2
      ;;
    --no-html)
      echo "error: --no-html is not supported; reports are always written" >&2
      exit 2
      ;;
    --json)
      JSON_REQUESTED=1
      shift
      ;;
    --list|--report)
      RUN_BENCHMARKS=0
      EXTRA_ARGS+=("$1")
      shift
      ;;
    -*)
      EXTRA_ARGS+=("$1")
      shift
      ;;
    *)
      if [[ "$1" =~ ^[0-9]+$ ]]; then
        PG_VERSION="$1"
      elif [[ -z "$BENCH_NAME" ]]; then
        BENCH_NAME="$1"
      else
        echo "error: unexpected positional argument '$1'" >&2
        usage >&2
        exit 2
      fi
      shift
      ;;
  esac
done

if [[ -n "${KOLDSTORE_PGRX_BENCH_EXTRA:-}" ]]; then
  # shellcheck disable=SC2206
  EXTRA_ARGS+=(${KOLDSTORE_PGRX_BENCH_EXTRA})
fi

PG_FEATURE="pg${PG_VERSION}"
FEATURES="${PG_FEATURE} s3 pg_bench"

ARGS=(
  -p pg_koldstore
  --no-default-features
  --features "${FEATURES}"
  --postgresql-conf wal_level=logical
  --postgresql-conf max_worker_processes=16
  --postgresql-conf shared_preload_libraries=koldstore
)

if [[ "${KOLDSTORE_PGRX_BENCH_DEBUG:-0}" == "1" || "${KOLDSTORE_PGRX_BENCH_DEBUG:-}" == "true" ]]; then
  ARGS+=(--debug)
else
  # release-pg: optimized + panic=unwind (plain --release uses panic=abort).
  ARGS+=(--profile release-pg)
fi

ARGS+=("pg${PG_VERSION}")
if [[ -n "$BENCH_NAME" ]]; then
  ARGS+=("$BENCH_NAME")
fi
if [[ ${#EXTRA_ARGS[@]} -gt 0 ]]; then
  ARGS+=("${EXTRA_ARGS[@]}")
fi

echo "running cargo pgrx bench (PostgreSQL ${PG_VERSION}, features: ${FEATURES})"

if [[ "$RUN_BENCHMARKS" -eq 0 ]]; then
  exec cargo pgrx bench "${ARGS[@]}"
fi

mkdir -p "$OUTPUT_ROOT"
RUN_STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_DIR="$(mktemp -d "${OUTPUT_ROOT%/}/run-${RUN_STAMP}-XXXXXX")"
LOG_FILE="$RUN_DIR/bench.log"
RESULTS_FILE="$RUN_DIR/results.json"
HTML_FILE="$RUN_DIR/report.html"
HISTORY_FILE="$RUN_DIR/history.json"
ARCHIVE_KEY="$(date -u +%Y-%m-%dT%H)"
if [[ "$REPO_RESULTS_ROOT" = /* ]]; then
  ARCHIVE_ROOT="$REPO_RESULTS_ROOT"
else
  ARCHIVE_ROOT="$ROOT_DIR/$REPO_RESULTS_ROOT"
fi

if [[ "$JSON_REQUESTED" -eq 0 ]]; then
  ARGS+=(--json)
fi

set +e
cargo pgrx bench "${ARGS[@]}" 2>&1 | tee "$RESULTS_FILE"
BENCH_STATUS="${PIPESTATUS[0]}"
set -e
cp "$RESULTS_FILE" "$LOG_FILE"

GROUP_NAME="$(python3 - "$RESULTS_FILE" <<'PY'
import json
import sys

raw = open(sys.argv[1], encoding="utf-8").read()
try:
    summary = json.loads(raw)
except json.JSONDecodeError:
    summary = None
    decoder = json.JSONDecoder()
    for offset, character in enumerate(raw):
        if character != "{":
            continue
        try:
            candidate, _ = decoder.raw_decode(raw[offset:])
        except json.JSONDecodeError:
            continue
        if isinstance(candidate, dict) and "benchmarks" in candidate:
            summary = candidate
print((summary or {}).get("group_name", ""))
PY
)"

if [[ -n "$GROUP_NAME" ]] && command -v psql >/dev/null 2>&1; then
  if ! PGHOST=localhost PGPORT="$BENCH_DB_PORT" PGUSER="${USER:-$(id -un)}" \
    psql -X -A -t -q -v ON_ERROR_STOP=1 -v current_group="$GROUP_NAME" \
    --dbname "$BENCH_DB_NAME" >"$HISTORY_FILE" <<'SQL'
WITH ranked_groups AS (
    SELECT
        id,
        group_name,
        created_at,
        row_number() OVER (ORDER BY created_at DESC, id DESC) AS recency_rank
    FROM pgrx_bench.run_group
    WHERE extname = 'koldstore'
      AND status = 'completed'
),
primary_estimate AS (
    SELECT DISTINCT ON (benchmark_run_id)
        benchmark_run_id,
        point_estimate_ns
    FROM pgrx_bench.benchmark_estimate
    ORDER BY benchmark_run_id,
        CASE WHEN estimate_kind = 'slope' THEN 0
             WHEN estimate_kind = 'mean' THEN 1
             ELSE 2 END,
        estimate_kind
),
sample_stats AS (
    SELECT
        benchmark_run_id,
        percentile_cont(0.50) WITHIN GROUP (ORDER BY elapsed_ns / NULLIF(iteration_count, 0)) AS p50_ns,
        percentile_cont(0.90) WITHIN GROUP (ORDER BY elapsed_ns / NULLIF(iteration_count, 0)) AS p90_ns,
        percentile_cont(0.99) WITHIN GROUP (ORDER BY elapsed_ns / NULLIF(iteration_count, 0)) AS p99_ns
    FROM pgrx_bench.benchmark_sample
    WHERE iteration_count > 0
    GROUP BY benchmark_run_id
),
current_group AS (
    SELECT id
    FROM ranked_groups
    WHERE group_name = :'current_group'
    LIMIT 1
),
metadata AS (
    SELECT json_build_object(
        'group_name', group_name,
        'created_at', created_at,
        'extversion', extversion,
        'pg_version_major', pg_version_major,
        'profile_name', profile_name,
        'cargo_features', cargo_features,
        'os', os,
        'arch', arch,
        'rustc_version', rustc_version,
        'cargo_version', cargo_version,
        'pgrx_version', pgrx_version,
        'cargo_pgrx_version', cargo_pgrx_version,
        'git_commit', git_commit,
        'git_branch', git_branch,
        'git_dirty', git_dirty,
        'git_describe', git_describe
    ) AS value
    FROM pgrx_bench.run_group
    WHERE group_name = :'current_group'
    LIMIT 1
),
metrics AS (
    SELECT COALESCE(json_agg(json_build_object(
        'bench_name', benchmark_case.bench_name,
        'p50_ns', sample_stats.p50_ns,
        'p90_ns', sample_stats.p90_ns,
        'p99_ns', sample_stats.p99_ns
    ) ORDER BY benchmark_case.bench_name), '[]'::json) AS value
    FROM pgrx_bench.benchmark_run
    JOIN pgrx_bench.benchmark_case
      ON benchmark_case.id = benchmark_run.case_id
    LEFT JOIN sample_stats
      ON sample_stats.benchmark_run_id = benchmark_run.id
    WHERE benchmark_run.group_id = (SELECT id FROM current_group)
),
history AS (
    SELECT COALESCE(json_agg(json_build_object(
        'bench_name', benchmark_case.bench_name,
        'group_name', ranked_groups.group_name,
        'created_at', ranked_groups.created_at,
        'recency_rank', ranked_groups.recency_rank,
        'status', benchmark_run.status,
        'point_estimate_ns', primary_estimate.point_estimate_ns
    ) ORDER BY ranked_groups.recency_rank, benchmark_case.bench_name), '[]'::json) AS value
    FROM ranked_groups
    JOIN pgrx_bench.benchmark_run
      ON benchmark_run.group_id = ranked_groups.id
    JOIN pgrx_bench.benchmark_case
      ON benchmark_case.id = benchmark_run.case_id
    LEFT JOIN primary_estimate
      ON primary_estimate.benchmark_run_id = benchmark_run.id
    WHERE ranked_groups.recency_rank <= 4
)
SELECT json_build_object(
    'metadata', COALESCE((SELECT value FROM metadata), '{}'::json),
    'metrics', (SELECT value FROM metrics),
    'history', (SELECT value FROM history)
)::text;
SQL
  then
    echo "warning: could not read pgrx benchmark history; report will omit previous runs" >&2
    : >"$HISTORY_FILE"
  fi
else
  echo "warning: psql unavailable; report will omit previous runs" >&2
  : >"$HISTORY_FILE"
fi

python3 - "$RESULTS_FILE" "$HTML_FILE" "$PG_VERSION" "$BENCH_NAME" "$BENCH_STATUS" "$HISTORY_FILE" "$ARCHIVE_ROOT" "$ARCHIVE_KEY" <<'PY'
import html
import json
import pathlib
import platform
import subprocess
import sys
from datetime import datetime, timezone

(
    results_path,
    html_path,
    pg_version,
    bench_name,
    process_status,
    history_path,
    archive_root,
    archive_key,
) = sys.argv[1:]
raw = pathlib.Path(results_path).read_text(encoding="utf-8")

def duration(value):
    if value is None:
        return "—"
    value = float(value)
    if value < 1_000:
        return f"{value:.2f} ns"
    if value < 1_000_000:
        return f"{value / 1_000:.2f} µs"
    if value < 1_000_000_000:
        return f"{value / 1_000_000:.2f} ms"
    return f"{value / 1_000_000_000:.2f} s"

def cell(value):
    return html.escape(str(value))

def date_label(value):
    if not value:
        return "—"
    return str(value).replace("T", " ")[:19]

def command_output(*args):
    try:
        return subprocess.check_output(args, text=True, stderr=subprocess.DEVNULL).strip()
    except (OSError, subprocess.CalledProcessError):
        return ""

def local_memory():
    value = command_output("sysctl", "-n", "hw.memsize")
    if value.isdigit():
        return int(value)
    try:
        for line in pathlib.Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            if line.startswith("MemTotal:"):
                return int(line.split()[1]) * 1024
    except OSError:
        pass
    return None

def memory_label(value):
    if not value:
        return "—"
    value = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{value:.1f} {unit}"
        value /= 1024

try:
    summary = json.loads(raw)
except json.JSONDecodeError:
    summary = None
    decoder = json.JSONDecoder()
    for offset, character in enumerate(raw):
        if character != "{":
            continue
        try:
            candidate, _ = decoder.raw_decode(raw[offset:])
        except json.JSONDecodeError:
            continue
        if isinstance(candidate, dict) and "benchmarks" in candidate:
            summary = candidate
if summary is None:
    summary = {"group_name": "benchmark failed", "benchmarks": []}

pathlib.Path(results_path).write_text(
    json.dumps(summary, indent=2) + "\n", encoding="utf-8"
)

try:
    history = json.loads(pathlib.Path(history_path).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    history = {"metadata": {}, "metrics": [], "history": []}

metadata = dict(history.get("metadata") or {})
system = platform.uname()
metadata.setdefault("os", system.system)
metadata.setdefault("arch", system.machine)
metadata["hostname"] = platform.node() or "—"
metadata["cpu_model"] = (
    command_output("sysctl", "-n", "machdep.cpu.brand_string")
    or command_output("sysctl", "-n", "hw.model")
    or "—"
)
metadata["memory_bytes"] = local_memory()
metadata.setdefault("pg_version_major", pg_version)
metadata.setdefault("profile_name", "release-pg")
metadata.setdefault("group_name", summary.get("group_name", "unknown"))
metadata.setdefault("created_at", datetime.now(timezone.utc).isoformat())

sample_metrics = {
    metric.get("bench_name"): metric
    for metric in history.get("metrics", [])
}
history_rows = history.get("history", [])
history_by_rank = {}
for row in history_rows:
    rank = row.get("recency_rank")
    if rank is not None:
        history_by_rank.setdefault(int(rank), {})[row.get("bench_name")] = row

previous_groups = []
for rank in (2, 3, 4):
    rows = [row for row in history_rows if row.get("recency_rank") == rank]
    previous_groups.append({
        "rank": rank,
        "label": date_label(rows[0].get("created_at")) if rows else "—",
    })

current_by_name = {
    benchmark.get("bench_name"): benchmark
    for benchmark in summary.get("benchmarks", [])
}

def primary_ns(benchmark):
    return (benchmark or {}).get("primary_estimate", {}).get("point_estimate_ns")

def archived_previous_runs():
    root = pathlib.Path(archive_root) / f"pg{pg_version}"
    if not root.is_dir():
        return []
    runs = []
    for run_dir in root.iterdir():
        if not run_dir.is_dir() or run_dir.name == archive_key:
            continue
        report_data = run_dir / "report-data.json"
        if not report_data.is_file():
            continue
        try:
            archived = json.loads(report_data.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        archived_summary = archived.get("summary") or {}
        archived_metadata = archived.get("metadata") or {}
        archived_rows = []
        for archived_benchmark in archived_summary.get("benchmarks", []):
            archived_rows.append({
                "bench_name": archived_benchmark.get("bench_name"),
                "group_name": archived_summary.get("group_name", run_dir.name),
                "created_at": archived_metadata.get("created_at") or run_dir.name,
                "status": archived_benchmark.get("status"),
                "point_estimate_ns": primary_ns(archived_benchmark),
            })
        if archived_rows:
            runs.append((run_dir.name, archived_rows))
    return sorted(runs, reverse=True)[:3]

def history_from_archived_runs(runs):
    rows = []
    for rank, (_, archived_rows) in enumerate(runs, start=2):
        for row in archived_rows:
            row = dict(row)
            row["recency_rank"] = rank
            rows.append(row)
    return rows

archived_runs = archived_previous_runs()
if archived_runs:
    history_rows = history_from_archived_runs(archived_runs)
    history_by_rank = {}
    for row in history_rows:
        history_by_rank.setdefault(row["recency_rank"], {})[row["bench_name"]] = row
    previous_groups = []
    for rank in (2, 3, 4):
        rows = [row for row in history_rows if row["recency_rank"] == rank]
        previous_groups.append({
            "rank": rank,
            "label": date_label(rows[0].get("created_at")) if rows else "—",
        })

def comparison_with_pg(benchmark):
    name = benchmark.get("bench_name", "")
    if name.startswith("managed_hot_"):
        plain = current_by_name.get("plain_heap_" + name.removeprefix("managed_hot_"))
        plain_ns = primary_ns(plain)
        current_ns = primary_ns(benchmark)
        if plain_ns and current_ns:
            return plain_ns, (current_ns - plain_ns) / plain_ns * 100.0
    if name.startswith("plain_heap_"):
        return primary_ns(benchmark), None
    return None, None

rows = []
for benchmark in summary.get("benchmarks", []):
    estimate = benchmark.get("primary_estimate") or {}
    comparison = benchmark.get("comparison") or {}
    change = comparison.get("point_pct")
    change_text = "—" if change is None else f"{float(change):+.2f}%"
    status = benchmark.get("status", "unknown")
    metric = sample_metrics.get(benchmark.get("bench_name"), {})
    pg_ns, pg_delta = comparison_with_pg(benchmark)
    pg_delta_text = "—" if pg_delta is None else f"{pg_delta:+.2f}%"
    if benchmark.get("bench_name", "").startswith("plain_heap_"):
        pg_delta_text = "baseline"
    previous_cells = []
    for group in previous_groups:
        previous = history_by_rank.get(group["rank"], {}).get(benchmark.get("bench_name"))
        previous_cells.append(cell(duration((previous or {}).get("point_estimate_ns"))))
    rows.append(
        "<tr>"
        f"<td>{cell(benchmark.get('bench_name', ''))}</td>"
        f"<td class=\"status-{cell(status)}\">{cell(status)}</td>"
        f"<td>{cell(duration(estimate.get('point_estimate_ns')))}</td>"
        f"<td>{cell(duration(estimate.get('ci_lower_bound_ns')))} – {cell(duration(estimate.get('ci_upper_bound_ns')))}</td>"
        f"<td>{cell(duration(metric.get('p50_ns')))}</td>"
        f"<td>{cell(duration(metric.get('p90_ns')))}</td>"
        f"<td>{cell(duration(metric.get('p99_ns')))}</td>"
        f"<td>{cell(change_text)}</td>"
        f"<td>{cell(duration(pg_ns))}</td>"
        f"<td>{cell(pg_delta_text)}</td>"
        + "".join(f"<td>{value}</td>" for value in previous_cells)
        + "</tr>"
    )

if not rows:
    rows.append(f"<tr><td colspan=13>No benchmark summary was produced (exit {cell(process_status)}).</td></tr>")

title = f"pg_koldstore benchmarks · PostgreSQL {pg_version}"
report_data = {
    "summary": summary,
    "metadata": metadata,
    "metrics": history.get("metrics", []),
    "history": history_rows,
}
report_data_path = pathlib.Path(html_path).with_name("report-data.json")
report_data_path.write_text(json.dumps(report_data, indent=2) + "\n", encoding="utf-8")
metadata_items = [
    ("Run date", date_label(metadata.get("created_at"))),
    ("Commit", metadata.get("git_commit") or "—"),
    ("Branch", metadata.get("git_branch") or "—"),
    ("Machine", metadata.get("hostname")),
    ("CPU", metadata.get("cpu_model")),
    ("Memory", memory_label(metadata.get("memory_bytes"))),
    ("OS", f"{metadata.get('os', '—')} / {metadata.get('arch', '—')}"),
    ("PostgreSQL", metadata.get("pg_version_major", pg_version)),
    ("Extension", metadata.get("extversion") or "—"),
    ("Profile", metadata.get("profile_name") or "—"),
    ("Features", ", ".join(metadata.get("cargo_features") or []) or "—"),
]
metadata_html = "".join(
    f"<div><strong>{cell(label)}</strong><br><code>{cell(value)}</code></div>"
    for label, value in metadata_items
)
history_headers = "".join(
    f"<th>Prev{group['rank'] - 1}: {cell(group['label'])}</th>"
    for group in previous_groups
)
body = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>{cell(title)}</title>
<style>
body {{ font: 15px system-ui, sans-serif; margin: 2rem; color: #202124; }}
table {{ border-collapse: collapse; min-width: 1800px; }}
th, td {{ border: 1px solid #dadce0; padding: .55rem .7rem; text-align: left; }}
th {{ background: #f1f3f4; }}
.status-ok {{ color: #137333; }} .status-failed {{ color: #c5221f; }}
code {{ background: #f1f3f4; padding: .1rem .25rem; }}
.metadata {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: .8rem; margin: 1rem 0 1.5rem; }}
.metadata div {{ border: 1px solid #dadce0; border-radius: .35rem; padding: .6rem; }}
.scroll {{ overflow-x: auto; }}
</style></head><body>
<h1>{cell(title)}</h1>
<p>Group: <code>{cell(summary.get('group_name', 'unknown'))}</code> · selector: <code>{cell(bench_name or 'all')}</code> · exit: <code>{cell(process_status)}</code></p>
<div class="metadata">{metadata_html}</div>
<div class="scroll"><table><thead><tr><th>Benchmark</th><th>Status</th><th>Estimate</th><th>Confidence interval</th><th>p50</th><th>p90</th><th>p99</th><th>Change</th><th>PG only</th><th>KoldStore vs PG</th>{history_headers}</tr></thead>
<tbody>{''.join(rows)}</tbody></table>
</div>
<p>Raw data: <a href="results.json">results.json</a> · report data: <a href="report-data.json">report-data.json</a> · log: <a href="bench.log">bench.log</a></p>
</body></html>
"""
pathlib.Path(html_path).write_text(body, encoding="utf-8")
PY

python3 "$ROOT_DIR/scripts/archive-pgrx-bench-results.py" \
  --source-dir "$RUN_DIR" \
  --archive-root "$ARCHIVE_ROOT" \
  --pg-version "$PG_VERSION" \
  --archive-key "$ARCHIVE_KEY"

echo "benchmark artifacts: $RUN_DIR" >&2
echo "tracked benchmark archive: $ARCHIVE_ROOT/pg${PG_VERSION}/${ARCHIVE_KEY}" >&2
exit "$BENCH_STATUS"
