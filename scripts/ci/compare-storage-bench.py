#!/usr/bin/env python3
"""Compare CI storage-bench JSON (base vs current) into markdown + HTML tables."""

from __future__ import annotations

import argparse
import html
import json
import re
from pathlib import Path
from typing import Any


MISSING = "—"
NUMBER_RE = re.compile(r"[-+]?\d+(?:\.\d+)?")

# (section, metric, field_pg_report, field_async_report, higher_is_better)
# Isolated --side pg fills postgres_only; --side async fills koldstore.
FOCUS_METRICS: list[tuple[str, str, bool]] = [
    ("main", "foreground insert throughput", True),
    ("main", "insert p99 latency", False),
    ("main", "update p99 latency", False),
    ("main", "hot-query p99 latency", False),
    ("main", "cold-query p99 latency", False),
    ("main", "hot+cold query throughput", True),
    ("main", "cold-only query throughput", True),
    ("main", "peak RSS during flush", False),
    ("main", "flush duration", False),
    ("main", "VACUUM duration", False),
    ("main", "local PostgreSQL storage", False),
    ("main", "total hot+cold storage", False),
    ("detail", "insert speed†", True),
    ("detail", "update speed†", True),
    ("detail", "delete speed†", True),
    ("detail", "query hot only (before flush)", True),
    ("detail", "query with hot+cold (after flush)", True),
    ("detail", "query cold only (after flush)", True),
    ("detail", "index storage (hot + __cl)", False),
    ("detail", "table storage (hot + __cl)", False),
    ("detail", "└ cold Parquet", False),
]


def load_report(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def numeric_value(value: str) -> float | None:
    if not value or value.strip() in {MISSING, "TODO", "-"}:
        return None
    match = NUMBER_RE.search(value)
    if match is None:
        return None
    number = float(match.group(0))
    unit_match = re.match(r"\s*(GiB|MiB|KiB|bytes|ops/s|µs|ms|s)\b", value[match.end() :])
    unit = unit_match.group(1) if unit_match else ""
    scale = {
        "GiB": 1024.0**3,
        "MiB": 1024.0**2,
        "KiB": 1024.0,
        "s": 1_000_000.0,
        "ms": 1_000.0,
        "µs": 1.0,
        "bytes": 1.0,
        "ops/s": 1.0,
        "": 1.0,
    }[unit]
    return number * scale


def cell(report: dict[str, Any] | None, section: str, metric: str, field: str) -> str:
    if report is None:
        return MISSING
    for row in report.get(section, []):
        if row.get("metric") == metric:
            value = str(row.get(field, MISSING)).strip()
            return value if value else MISSING
    return MISSING


def side_value(report: dict[str, Any] | None, section: str, metric: str, side: str) -> str:
    field = "postgres_only" if side == "pg" else "koldstore"
    return cell(report, section, metric, field)


def format_delta(base: str, current: str, higher_is_better: bool) -> str:
    base_n = numeric_value(base)
    current_n = numeric_value(current)
    if base_n is None or current_n is None:
        if base == MISSING and current == MISSING:
            return MISSING
        if base == MISSING:
            return "new"
        if current == MISSING:
            return "gone"
        return "n/a"
    if base_n == 0:
        if current_n == 0:
            return "±0%"
        return "n/a"
    pct = ((current_n - base_n) / abs(base_n)) * 100.0
    improved = (pct > 0 and higher_is_better) or (pct < 0 and not higher_is_better)
    regressed = (pct < 0 and higher_is_better) or (pct > 0 and not higher_is_better)
    sign = f"{pct:+.1f}%"
    if abs(pct) < 0.05:
        return "±0%"
    if improved:
        return f"{sign} ✅"
    if regressed:
        return f"{sign} ⚠️"
    return sign


def short_sha(report: dict[str, Any] | None) -> str:
    if report is None:
        return MISSING
    commit = str(report.get("git_commit") or "").strip()
    if not commit:
        return MISSING
    return commit[:12]


def meta_line(report: dict[str, Any] | None) -> str:
    if report is None:
        return MISSING
    rows = report.get("rows", "?")
    hot = report.get("hot_limit", "?")
    return f"{rows} rows · hot_limit={hot}"


def build_rows(
    base_pg: dict[str, Any] | None,
    base_async: dict[str, Any] | None,
    current_pg: dict[str, Any] | None,
    current_async: dict[str, Any] | None,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for section, metric, higher_is_better in FOCUS_METRICS:
        b_pg = side_value(base_pg, section, metric, "pg")
        c_pg = side_value(current_pg, section, metric, "pg")
        b_as = side_value(base_async, section, metric, "async")
        c_as = side_value(current_async, section, metric, "async")
        # Skip entirely empty focus rows (TODO-only placeholders).
        values = {b_pg, c_pg, b_as, c_as}
        if values <= {MISSING, "TODO"}:
            continue
        rows.append(
            {
                "metric": metric,
                "base_pg": b_pg,
                "current_pg": c_pg,
                "delta_pg": format_delta(b_pg, c_pg, higher_is_better),
                "base_async": b_as,
                "current_async": c_as,
                "delta_async": format_delta(b_as, c_as, higher_is_better),
            }
        )
    return rows


def render_markdown(
    rows: list[dict[str, str]],
    *,
    has_baseline: bool,
    base_sha: str,
    current_sha: str,
    base_branch: str,
    meta: str,
) -> str:
    lines = [
        "<!-- koldstore-storage-bench -->",
        "## Storage bench (PG 16, 10k rows)",
        "",
    ]
    if has_baseline:
        lines.append(
            f"Comparison against base commit `{base_sha}` "
            f"(previous successful CI on `{base_branch}`)."
        )
    else:
        lines.append(
            "No prior successful CI storage-bench artifacts found; "
            "showing current run only (deltas marked n/a)."
        )
    lines.extend(
        [
            "",
            f"Results for commit `{current_sha}`. · {meta}",
            "",
            "| Metric | Base (PG) | Current (PG) | Δ | Base (Async) | Current (Async) | Δ |",
            "| --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for row in rows:
        lines.append(
            "| {metric} | {base_pg} | {current_pg} | {delta_pg} | "
            "{base_async} | {current_async} | {delta_async} |".format(**row)
        )
    lines.extend(["", "♻️ This comment has been updated with latest results.", ""])
    return "\n".join(lines)


def render_html(
    rows: list[dict[str, str]],
    *,
    has_baseline: bool,
    base_sha: str,
    current_sha: str,
    base_branch: str,
    meta: str,
) -> str:
    def esc(value: str) -> str:
        return html.escape(value)

    if has_baseline:
        subtitle = (
            f"Comparison against base commit <code>{esc(base_sha)}</code> "
            f"(previous successful CI on <code>{esc(base_branch)}</code>)."
        )
    else:
        subtitle = (
            "No prior successful CI storage-bench artifacts found; "
            "showing current run only."
        )
    body_rows = []
    for row in rows:
        body_rows.append(
            "<tr>"
            f"<td>{esc(row['metric'])}</td>"
            f"<td>{esc(row['base_pg'])}</td>"
            f"<td>{esc(row['current_pg'])}</td>"
            f"<td>{esc(row['delta_pg'])}</td>"
            f"<td>{esc(row['base_async'])}</td>"
            f"<td>{esc(row['current_async'])}</td>"
            f"<td>{esc(row['delta_async'])}</td>"
            "</tr>"
        )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Storage bench (PG 16)</title>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 1.5rem; color: #111; }}
    table {{ border-collapse: collapse; width: 100%; font-size: 0.9rem; }}
    th, td {{ border: 1px solid #ccc; padding: 0.4rem 0.55rem; text-align: left; }}
    th {{ background: #f4f4f5; }}
    code {{ font-size: 0.9em; }}
  </style>
</head>
<body>
  <h1>Storage bench (PG 16, 10k rows)</h1>
  <p>{subtitle}</p>
  <p>Results for commit <code>{esc(current_sha)}</code>. · {esc(meta)}</p>
  <table>
    <thead>
      <tr>
        <th>Metric</th>
        <th>Base (PG)</th>
        <th>Current (PG)</th>
        <th>Δ</th>
        <th>Base (Async)</th>
        <th>Current (Async)</th>
        <th>Δ</th>
      </tr>
    </thead>
    <tbody>
      {"".join(body_rows)}
    </tbody>
  </table>
</body>
</html>
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--current-pg", type=Path, required=True)
    parser.add_argument("--current-async", type=Path, required=True)
    parser.add_argument("--base-pg", type=Path, default=None)
    parser.add_argument("--base-async", type=Path, default=None)
    parser.add_argument("--base-branch", default="main")
    parser.add_argument("--base-sha", default="")
    parser.add_argument("--current-sha", default="")
    parser.add_argument("--html-out", type=Path, required=True)
    parser.add_argument("--md-out", type=Path, required=True)
    args = parser.parse_args()

    current_pg = load_report(args.current_pg)
    current_async = load_report(args.current_async)
    if current_pg is None and current_async is None:
        raise SystemExit("at least one current report is required")

    base_pg = load_report(args.base_pg)
    base_async = load_report(args.base_async)
    has_baseline = base_pg is not None or base_async is not None

    current_sha = args.current_sha or short_sha(current_pg) or short_sha(current_async)
    base_sha = args.base_sha or short_sha(base_pg) or short_sha(base_async)
    meta = meta_line(current_pg) if current_pg else meta_line(current_async)

    rows = build_rows(base_pg, base_async, current_pg, current_async)
    md = render_markdown(
        rows,
        has_baseline=has_baseline,
        base_sha=base_sha,
        current_sha=current_sha,
        base_branch=args.base_branch,
        meta=meta,
    )
    html_doc = render_html(
        rows,
        has_baseline=has_baseline,
        base_sha=base_sha,
        current_sha=current_sha,
        base_branch=args.base_branch,
        meta=meta,
    )

    args.md_out.parent.mkdir(parents=True, exist_ok=True)
    args.html_out.parent.mkdir(parents=True, exist_ok=True)
    args.md_out.write_text(md, encoding="utf-8")
    args.html_out.write_text(html_doc, encoding="utf-8")
    print(f"wrote {args.md_out}")
    print(f"wrote {args.html_out}")


if __name__ == "__main__":
    main()
