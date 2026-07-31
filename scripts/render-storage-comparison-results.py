#!/usr/bin/env python3
"""Merge isolated pg/async storage-comparison JSON into RESULTS.md."""

from __future__ import annotations

import argparse
import copy
import json
import re
import statistics
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


MISSING = "—"
SAMPLE_METADATA_FIELDS = (
    "mode",
    "git_commit",
    "rows",
    "hot_limit",
    "dml_sample",
    "insert_batch_rows",
    "max_rows_per_file",
    "warmup_rows",
)
NUMBER_RE = re.compile(r"[-+]?\d+(?:\.\d+)?")


def load_report(paths: list[Path] | None) -> dict[str, Any] | None:
    if not paths:
        return None
    missing = [path for path in paths if not path.is_file()]
    if missing:
        raise FileNotFoundError(
            "missing requested storage-comparison sample(s): "
            + ", ".join(str(path) for path in missing)
        )
    reports = [json.loads(path.read_text(encoding="utf-8")) for path in paths]
    return aggregate_reports(reports)


def numeric_value(value: str) -> float | None:
    match = NUMBER_RE.search(value)
    if match is None:
        return None
    number = float(match.group(0))
    unit_match = re.match(r"\s*(GiB|MiB|KiB|bytes|µs|ms|s)\b", value[match.end() :])
    unit = unit_match.group(1) if unit_match else ""
    scale = {
        "GiB": 1024.0**3,
        "MiB": 1024.0**2,
        "KiB": 1024.0,
        "s": 1_000_000.0,
        "ms": 1_000.0,
        "µs": 1.0,
        "bytes": 1.0,
        "": 1.0,
    }[unit]
    return number * scale


def replace_first_number(template: str, value: float, scale: float) -> str:
    rendered = value / scale
    text = f"{rendered:.2f}".rstrip("0").rstrip(".")
    return NUMBER_RE.sub(text, template, count=1)


def aggregate_cell(values: list[str]) -> tuple[str, dict[str, str] | None]:
    parsed = [numeric_value(value) for value in values]
    if any(value is None for value in parsed):
        if len(set(values)) != 1:
            raise ValueError(f"non-numeric sample values disagree: {values}")
        return values[0], None
    numeric = [value for value in parsed if value is not None]
    median = statistics.median(numeric)
    minimum = min(range(len(numeric)), key=numeric.__getitem__)
    maximum = max(range(len(numeric)), key=numeric.__getitem__)
    template_index = min(range(len(numeric)), key=lambda index: abs(numeric[index] - median))
    template_number = NUMBER_RE.search(values[template_index])
    assert template_number is not None
    original = float(template_number.group(0))
    scale = numeric[template_index] / original if original != 0 else 1.0
    aggregated = replace_first_number(values[template_index], median, scale)
    dispersion = None
    if numeric[minimum] != numeric[maximum]:
        dispersion = {"min": values[minimum], "max": values[maximum]}
    return aggregated, dispersion


def aggregate_reports(reports: list[dict[str, Any]]) -> dict[str, Any]:
    if not reports:
        raise ValueError("at least one sample report is required")
    if any(report.get("git_dirty") for report in reports):
        raise ValueError("cannot aggregate a dirty benchmark sample")
    first = reports[0]
    for field in SAMPLE_METADATA_FIELDS:
        expected = first.get(field)
        if any(report.get(field) != expected for report in reports[1:]):
            raise ValueError(f"sample metadata mismatch for {field}")

    aggregate = copy.deepcopy(first)
    aggregate["sample_count"] = len(reports)
    stamps = [str(report.get("generated_at") or "") for report in reports]
    aggregate["generated_at_first"] = min(stamps)
    aggregate["generated_at"] = max(stamps)
    dispersion: dict[str, dict[str, str]] = {}
    for section in ("main", "detail"):
        expected_metrics = [row.get("metric") for row in first.get(section, [])]
        for report in reports[1:]:
            metrics = [row.get("metric") for row in report.get(section, [])]
            if metrics != expected_metrics:
                raise ValueError(f"sample metric mismatch for {section}")
        for row_index, row in enumerate(aggregate.get(section, [])):
            metric = str(row.get("metric") or "")
            for field in ("postgres_only", "koldstore"):
                values = [str(report[section][row_index].get(field, MISSING)) for report in reports]
                value, cell_dispersion = aggregate_cell(values)
                row[field] = value
                if cell_dispersion is not None:
                    dispersion[f"{section}.{metric}.{field}"] = cell_dispersion
    aggregate["sample_dispersion"] = dispersion
    return aggregate


def validate_comparison_reports(
    pg_report: dict[str, Any] | None,
    async_report: dict[str, Any] | None,
) -> None:
    reports = [
        ("pg", pg_report),
        ("async", async_report),
    ]
    present = [(expected_mode, report) for expected_mode, report in reports if report]
    for expected_mode, report in present:
        if report.get("mode") != expected_mode:
            raise ValueError(
                f"expected {expected_mode} report, got mode={report.get('mode')!r}"
            )
    if not present:
        return
    reference = present[0][1]
    for field in (*SAMPLE_METADATA_FIELDS[1:], "sample_count"):
        expected = reference.get(field, 1 if field == "sample_count" else None)
        for mode, report in present[1:]:
            actual = report.get(field, 1 if field == "sample_count" else None)
            if actual != expected:
                raise ValueError(
                    f"comparison metadata mismatch for {field}: "
                    f"expected {expected!r}, {mode} has {actual!r}"
                )


def cell(report: dict[str, Any] | None, section: str, metric: str, field: str) -> str:
    if report is None:
        return MISSING
    for row in report.get(section, []):
        if row.get("metric") == metric:
            value = row.get(field, MISSING)
            if value in (None, ""):
                return MISSING
            rendered = str(value)
            spread = report.get("sample_dispersion", {}).get(f"{section}.{metric}.{field}")
            if spread:
                rendered += f" [range: {spread['min']} – {spread['max']}]"
            return rendered
    return MISSING


def ordered_metrics(*reports: dict[str, Any] | None, section: str) -> list[str]:
    seen: list[str] = []
    for report in reports:
        if report is None:
            continue
        for row in report.get(section, []):
            metric = row.get("metric")
            if metric and metric not in seen:
                seen.append(metric)
    return seen


def render_table(
    label: str,
    section: str,
    pg_report: dict[str, Any] | None,
    async_report: dict[str, Any] | None,
) -> str:
    lines = [
        f"| {label} | PostgreSQL only | PG + KoldStore |",
        "| --- | --- | --- |",
    ]
    for metric in ordered_metrics(
        # managed first so catch-up rows sit under DML rather than at the end
        async_report, pg_report, section=section
    ):
        pg = cell(pg_report, section, metric, "postgres_only")
        managed = cell(async_report, section, metric, "koldstore")
        lines.append(f"| {metric} | {pg} | {managed} |")
    return "\n".join(lines)


def _plain_cell(
    report: dict[str, Any] | None, section: str, metric: str, field: str
) -> str:
    """Like cell(), but without sample-dispersion suffixes (for glance math)."""
    if report is None:
        return MISSING
    for row in report.get(section, []):
        if row.get("metric") == metric:
            value = row.get(field, MISSING)
            if value in (None, ""):
                return MISSING
            return str(value)
    return MISSING


def _shrink_pct(before: float, after: float) -> str:
    if before <= 0:
        return "n/a"
    pct = (1.0 - (after / before)) * 100.0
    return f"**{pct:.0f}% smaller**"


def _speedup(before_s: float, after_s: float) -> str:
    if after_s <= 0:
        return "n/a"
    return f"**{before_s / after_s:.0f}× faster**"


def render_storage_wins_glance(
    pg_report: dict[str, Any] | None,
    async_report: dict[str, Any] | None,
) -> str:
    """Build the glance table from published main/detail cells."""
    if pg_report is None or async_report is None:
        return (
            "KoldStore is a **storage lifecycle** tool. The durable wins after flush are heap\n"
            "size, index size, and VACUUM time — not universal DML/query acceleration.\n"
            "Glance table requires both pg and async JSON samples."
        )

    total_pg = _plain_cell(
        pg_report, "main", "total hot+cold storage", "postgres_only"
    )
    total_ks = _plain_cell(
        async_report, "main", "total hot+cold storage", "koldstore"
    )
    local_pg = _plain_cell(
        pg_report, "main", "local PostgreSQL storage", "postgres_only"
    )
    local_ks = _plain_cell(
        async_report, "main", "local PostgreSQL storage", "koldstore"
    )
    cold = _plain_cell(async_report, "detail", "└ cold Parquet", "koldstore")
    idx_pg = _plain_cell(
        pg_report, "detail", "index storage (hot + __cl)", "postgres_only"
    )
    idx_ks = _plain_cell(
        async_report, "detail", "index storage (hot + __cl)", "koldstore"
    )
    vac_pg = _plain_cell(pg_report, "main", "VACUUM duration", "postgres_only")
    vac_ks = _plain_cell(async_report, "main", "VACUUM duration", "koldstore")

    total_pg_n = numeric_value(total_pg)
    total_ks_n = numeric_value(total_ks)
    local_pg_n = numeric_value(local_pg)
    local_ks_n = numeric_value(local_ks)
    idx_pg_n = numeric_value(idx_pg)
    idx_ks_n = numeric_value(idx_ks)
    vac_pg_n = numeric_value(vac_pg)
    vac_ks_n = numeric_value(vac_ks)

    total_trade = (
        _shrink_pct(total_pg_n, total_ks_n)
        if total_pg_n is not None and total_ks_n is not None
        else "—"
    )
    local_trade = (
        _shrink_pct(local_pg_n, local_ks_n)
        if local_pg_n is not None and local_ks_n is not None
        else "—"
    )
    idx_trade = (
        _shrink_pct(idx_pg_n, idx_ks_n)
        if idx_pg_n is not None and idx_ks_n is not None
        else "—"
    )
    vac_trade = (
        _speedup(vac_pg_n, vac_ks_n)
        if vac_pg_n is not None and vac_ks_n is not None
        else "—"
    )

    cold_short = cold.split(" ")[0] + " " + cold.split(" ")[1] if cold.count(" ") >= 1 else cold
    # Prefer a compact cold label like "599 MiB" from "598.52 MiB"
    cold_n = numeric_value(cold)
    if cold_n is not None:
        cold_mib = cold_n / (1024.0**2)
        cold_label = f"{cold_mib:.0f} MiB"
    else:
        cold_label = cold_short

    lines = [
        "KoldStore is a **storage lifecycle** tool. The durable wins after flush are heap",
        "size, index size, and VACUUM time — not universal DML/query acceleration.",
        "Async column below (vs PostgreSQL-only).",
        "",
        "| Result | Before → after flush | Tradeoff |",
        "| --- | --- | --- |",
        f"| Total footprint (hot + cold) | {total_pg} → {total_ks} | {total_trade} |",
        f"| └ hot in PostgreSQL (heap + `__cl`) | {local_pg} → {local_ks} | {local_trade} |",
        f"| └ cold Parquet | — → {cold_label} | outside the database |",
        f"| Indexes (hot + `__cl`) | {idx_pg} → {idx_ks} | {idx_trade} |",
        f"| `VACUUM (FULL, ANALYZE)` | {vac_pg} → {vac_ks} | {vac_trade} |",
    ]
    return "\n".join(lines)


def parse_rfc3339(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def format_when(
    pg_report: dict[str, Any] | None,
    async_report: dict[str, Any] | None,
) -> str:
    stamps: list[tuple[str, datetime]] = []
    for label, report in (
        ("pg", pg_report),
        ("async", async_report),
    ):
        if report is None:
            continue
        dt = parse_rfc3339(str(report.get("generated_at") or ""))
        if dt is not None:
            stamps.append((label, dt.astimezone(timezone.utc)))
    if not stamps:
        return "unknown"
    if len(stamps) == 1:
        label, dt = stamps[0]
        return f"{dt.date().isoformat()} UTC ({label} @ {dt.strftime('%H:%M:%SZ')})"
    first = min(stamps, key=lambda x: x[1])[1]
    last = max(stamps, key=lambda x: x[1])[1]
    per_side = ", ".join(
        f"{label} {dt.strftime('%H:%M:%SZ')}" for label, dt in stamps
    )
    if first.date() == last.date():
        return f"{first.date().isoformat()} UTC ({per_side})"
    return (
        f"{first.date().isoformat()} → {last.date().isoformat()} UTC ({per_side})"
    )


def resolve_git_commit(
    *reports: dict[str, Any] | None,
    fallback: str | None,
) -> tuple[str, bool, str]:
    commits: list[str] = []
    dirty = False
    notes: list[str] = []
    for report in reports:
        if report is None:
            continue
        commit = str(report.get("git_commit") or "").strip()
        if commit and commit not in commits:
            commits.append(commit)
        if report.get("git_dirty"):
            dirty = True
        note = str(report.get("git_note") or "").strip()
        if note and note not in notes:
            notes.append(note)
    if commits:
        commit = (
            commits[0]
            if len(commits) == 1
            else " / ".join(commits) + " (sides disagree)"
        )
    elif fallback:
        commit = fallback
    else:
        try:
            commit = subprocess.check_output(
                ["git", "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL
            ).strip()
        except (OSError, subprocess.CalledProcessError):
            commit = "unknown"
    return commit, dirty, "; ".join(notes)


def short_commit(commit: str) -> str:
    if commit in ("unknown", "") or "sides disagree" in commit:
        return commit
    if " / " in commit:
        return commit
    return commit[:12] if len(commit) > 12 else commit


def run_meta(
    pg_report: dict[str, Any] | None,
    async_report: dict[str, Any] | None,
    git_commit: str,
    git_dirty: bool,
    git_note: str,
) -> str:
    source = pg_report or async_report or {}
    rows = source.get("rows", "?")
    hot = source.get("hot_limit", "?")
    dml = source.get("dml_sample", "?")
    batch = source.get("insert_batch_rows", "?")
    max_rows = source.get("max_rows_per_file", "?")
    warmup = source.get("warmup_rows", "?")
    modes = []
    if pg_report is not None:
        modes.append("pg")
    if async_report is not None:
        modes.append("async")
    mode_text = " + ".join(modes) if modes else "none"
    sample_counts = [
        int(report.get("sample_count", 1))
        for report in (pg_report, async_report)
        if report is not None
    ]
    sample_text = (
        f" · **{sample_counts[0]} samples per side (median + range)**"
        if sample_counts and len(set(sample_counts)) == 1 and sample_counts[0] > 1
        else " · **single sample per side**"
    )
    when = format_when(pg_report, async_report)
    git_line = f"**Git:** `{short_commit(git_commit)}`"
    if len(git_commit) > 12 and " " not in git_commit:
        git_line += f" (`{git_commit}`)"
    if git_dirty:
        git_line += " · dirty tree"
    if git_note:
        git_line += f" — {git_note}"
    return "\n".join(
        [
            f"**When:** {when}",
            git_line,
            f"**Run:** {rows} rows · `hot_row_limit = {hot}` · `max_rows_per_file = {max_rows}` "
            f"· `--dml-sample {dml}` · `insert_batch_rows = {batch}` · "
            f"`warmup_rows = {warmup}` · zstd Parquet · "
            f"**counterbalanced sequential** isolated wiped server per sample (not parallel) · "
            f"sides measured: **{mode_text}**{sample_text}",
        ]
    )


def render(
    pg_report: dict[str, Any] | None,
    async_report: dict[str, Any] | None,
    git_commit: str,
    git_dirty: bool = False,
    git_note: str = "",
) -> str:
    parts = [
        "# Latest benchmark results",
        "",
        "Published numbers from the most recent storage comparison run(s). Re-run",
        "`scripts/run-storage-comparison.sh --all-sides --repetitions 6 --update-results` to refresh",
        "this file. Each column is measured alone on a wiped + re-initdb pgrx PostgreSQL",
        "(stop → wipe `~/.pgrx/data-<ver>` → prepare → one side). Methodology:",
        "[README.md](README.md).",
        "",
        run_meta(pg_report, async_report, git_commit, git_dirty, git_note),
        "",
        "Managed PostgreSQL sizes include hot heap + `koldstore.<table>__cl` + mirror",
        "indexes. Cold Parquet is outside the PostgreSQL data directory. Columns are",
        "**PostgreSQL only** and **PG + KoldStore** (WAL-only capture).",
        "",
        "## Main comparison",
        "",
        render_table("Metric", "main", pg_report, async_report),
        "",
        "‡ **Hot+cold query** alternates newest hot PK (`id = <rows>`) and oldest",
        "cold PK (`id = 1`) after flush — **50/50** of the lookup loop.",
        "**Cold-only** repeatedly looks up only `id = 1` (Parquet on managed).",
        "**Hot-only** (before flush) repeatedly looks up `id = <rows>`.",
        "p99 insert = per insert-batch; update = per 1k-row batch; queries = per",
        "PK lookup (`QUERY_LOOPS = 400` after 40 discarded warm-up lookups). See [README.md](README.md).",
        "",
        "## Detail (throughput and storage)",
        "",
        render_table("Operation", "detail", pg_report, async_report),
        "",
        "† Managed DML records heap WAL in the foreground; catch-up rows appear",
        "separately after `wait_for_async_mirror()`.",
        "",
        "## Storage wins at a glance (this run)",
        "",
        render_storage_wins_glance(pg_report, async_report),
        "",
        "### Why was delete reported faster before — and is it?",
        "",
        "Foreground delete is a single `DELETE … WHERE id BETWEEN …` over",
        "`--dml-sample` rows **before flush**. Managed capture does **not** update the",
        "mirror in that window (catch-up is a separate row).",
        "",
        "Managed delete can still land below PostgreSQL-only: one-shot bulk DELETE has",
        "high variance across isolated sides, and the managed table still carries a",
        "logical publication. Do **not** publish “KoldStore makes DELETE faster” from a",
        "single sample.",
        "",
        "### Segment object-path layout",
        "",
        "Flush keys use `{namespace}/{table}/{folder:03}/segment-{NNNN}-{8hex}.parquet`",
        "(100 segments per folder). Manifest stores the table-relative path. This does",
        "**not** change DML, VACUUM, or Parquet byte size; it only improves listing",
        "hygiene vs a flat `batch-*` / full-UUID layout. Keep the short token for",
        "orphan-retry uniqueness; week/Hive folders are unnecessary while catalog stats",
        "prune reads.",
        "",
        "### Why does managed insert look faster than PostgreSQL only?",
        "",
        "It is **not** a KoldStore acceleration of `INSERT`. Both columns time the same",
        "kind of work: committed 100k-row batches into the user heap (+ indexes).",
        "Managed capture does **not** update `koldstore.<table>__cl` in that timed",
        "window — that cost is the separate **async insert mirror catch-up** row.",
        "",
        "After each timed seed the harness probes PK bounds and logs WAL bytes plus",
        "pre-flush heap/index size. When those footprints match and first batches are",
        "similar, a large late-batch gap is I/O / checkpoint variance — not skipped",
        "indexes or a smaller hot set. Sides run one after another on wiped clusters",
        "(`cargo pgrx stop` + wipe data dir + initdb). Do not treat managed >",
        "PostgreSQL-only insert as a product claim until repeated isolated runs agree.",
        "For end-to-end “row is mirrored” cost, add catch-up (or run with the",
        "background worker and measure lag).",
        "",
        "Lab note: the storage harness may set `koldstore.async_mirror_max_retained_bytes = 0`",
        "while the worker is off so 10M-row seeding can retain multi-GiB slot WAL until",
        "the post-insert fence. Production keeps the default 1 GiB health threshold;",
        "crossing it alerts but never blocks apply from draining retained WAL.",
        "",
    ]
    return "\n".join(parts)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pg-json", type=Path, action="append", default=None)
    parser.add_argument("--async-json", type=Path, action="append", default=None)
    parser.add_argument(
        "--git-commit",
        default=None,
        help="Fallback git SHA when JSON lacks git_commit (default: git rev-parse HEAD)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        required=True,
        help="Write rendered RESULTS.md here (also printed to stdout)",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Do not print the rendered markdown to stdout (still writes --out)",
    )
    args = parser.parse_args()

    pg_report = load_report(args.pg_json)
    async_report = load_report(args.async_json)
    if pg_report is None and async_report is None:
        raise SystemExit(
            "at least one of --pg-json / --async-json must exist"
        )
    validate_comparison_reports(pg_report, async_report)

    git_commit, git_dirty, git_note = resolve_git_commit(
        pg_report, async_report, fallback=args.git_commit
    )

    markdown = render(
        pg_report,
        async_report,
        git_commit,
        git_dirty=git_dirty,
        git_note=git_note,
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(markdown, encoding="utf-8")
    if not args.quiet:
        print(markdown, end="" if markdown.endswith("\n") else "\n")
    print(f"wrote {args.out}", flush=True)


if __name__ == "__main__":
    main()
