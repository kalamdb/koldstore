#!/usr/bin/env python3
"""Flush + EXPLAIN proof gates for deep HammerDB runs."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path


def psql(
    args: argparse.Namespace,
    sql: str,
    *,
    tuples_only: bool = False,
) -> str:
    cmd = [
        args.psql,
        "-h",
        args.host,
        "-p",
        str(args.port),
        "-d",
        args.database,
        "-v",
        "ON_ERROR_STOP=1",
    ]
    if tuples_only:
        cmd.append("-At")
    else:
        cmd.append("-q")
    cmd.extend(["-c", sql])
    proc = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(
            f"psql failed ({proc.returncode}): {sql[:200]!r}\n"
            f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        )
    return proc.stdout


def last_data_line(text: str) -> str:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    skip = {
        "LOAD",
        "CREATE TABLE",
        "CREATE",
        "DO",
        "INSERT",
        "DELETE",
        "SELECT",
        "SET",
        "FLUSH",
    }
    for line in reversed(lines):
        if line in skip or line.startswith("CREATE "):
            continue
        return line
    raise RuntimeError(f"no data line in psql output: {text!r}")


def scalar(args: argparse.Namespace, sql: str) -> str:
    return last_data_line(psql(args, sql, tuples_only=True))


def parquet_segments_opened(explain_text: str) -> int:
    m = re.search(r"Parquet Segments Opened:\s*(\d+)", explain_text)
    if m:
        return int(m.group(1))
    m = re.search(r"Cold segments: considered=(\d+),.*?opened=(\d+)", explain_text)
    if m:
        return int(m.group(2))
    return 0


def managed_tables(args: argparse.Namespace) -> list[str]:
    raw = psql(
        args,
        """
SELECT coalesce(string_agg(format('%s.%s', n.nspname, c.relname), ',' ORDER BY c.relname), '')
FROM koldstore.schemas s
JOIN pg_class c ON c.oid = s.table_oid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE s.active;
""",
        tuples_only=True,
    )
    line = last_data_line(raw)
    if not line:
        return []
    return [t for t in line.split(",") if t]


def flush_tables(args: argparse.Namespace, tables: list[str], log_path: Path) -> None:
    chunks: list[str] = ["LOAD 'koldstore';"]
    for table in tables:
        chunks.append(f"SELECT koldstore.flush_table('{table}'::regclass);")
    chunks.append(
        "SELECT id, job_type, status, left(coalesce(error_trace,''), 240) AS err "
        "FROM koldstore.jobs WHERE job_type='flush' ORDER BY created_at DESC LIMIT 5;"
    )
    chunks.append(
        "SELECT count(*) AS active_segments, coalesce(sum(row_count),0) AS cold_rows, "
        "coalesce(sum(byte_size),0) AS cold_bytes "
        "FROM koldstore.cold_segments WHERE status='active';"
    )
    out = psql(args, "\n".join(chunks))
    log_path.write_text(out, encoding="utf-8")
    segs = int(scalar(args, "SELECT count(*) FROM koldstore.cold_segments WHERE status='active';"))
    if segs < 1:
        raise SystemExit(f"error: flush produced 0 active cold segments; see {log_path}")


def prove(args: argparse.Namespace) -> dict:
    psql(args, "LOAD 'koldstore';")

    explain_hist = psql(
        args,
        "LOAD 'koldstore'; EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) "
        "SELECT * FROM history WHERE ks_id = 1;",
    )
    explain_cust = psql(
        args,
        "EXPLAIN (COSTS OFF) SELECT * FROM customer "
        "WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1;",
    )
    explain_text = (
        f"===== phase={args.phase} history PK =====\n{explain_hist}\n"
        f"===== phase={args.phase} customer PK =====\n{explain_cust}\n"
    )
    Path(args.explain_out).write_text(explain_text, encoding="utf-8")

    uses_merge = "Custom Scan (KoldMergeScan)" in explain_hist
    customer_uses_merge = "Custom Scan (KoldMergeScan)" in explain_cust
    opened = parquet_segments_opened(explain_hist)
    cold_segs = int(scalar(args, "SELECT count(*) FROM koldstore.cold_segments WHERE status='active';"))
    managed = managed_tables(args)

    if "public.history" not in managed and "history" not in {m.split(".")[-1] for m in managed}:
        raise SystemExit(f"error: history not actively managed at phase={args.phase}: {managed}")

    if args.expect_cold:
        if not uses_merge:
            raise SystemExit(
                f"error: phase={args.phase} expected KoldMergeScan on history PK:\n{explain_hist}"
            )
        if opened < 1:
            raise SystemExit(
                f"error: phase={args.phase} expected opened>=1 cold on history PK:\n{explain_hist}"
            )
        if cold_segs < 1:
            raise SystemExit(f"error: phase={args.phase} expected active cold segments")
    if customer_uses_merge:
        raise SystemExit(
            f"error: phase={args.phase} customer unexpectedly used KoldMergeScan:\n{explain_cust}"
        )

    result = {
        "phase": args.phase,
        "managed_tables": managed,
        "plan_history_pk_uses_merge_scan": uses_merge,
        "plan_history_pk_cold_segments_opened": opened,
        "plan_customer_pk_uses_merge_scan": customer_uses_merge,
        "cold_segments": cold_segs,
    }
    Path(args.json_out).write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", required=True, help="mid_run | post_run | …")
    parser.add_argument("--psql", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--flush", action="store_true")
    parser.add_argument("--expect-cold", action="store_true")
    parser.add_argument("--flush-log", default="")
    parser.add_argument("--json-out", required=True)
    parser.add_argument("--explain-out", required=True)
    args = parser.parse_args()

    tables = managed_tables(args)
    if args.flush:
        if not args.flush_log:
            raise SystemExit("--flush requires --flush-log")
        flush_tables(args, tables, Path(args.flush_log))
    prove(args)


if __name__ == "__main__":
    main()
