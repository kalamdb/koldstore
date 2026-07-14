#!/usr/bin/env python3
"""Read microbench + EXPLAIN proof for HammerDB compare arms."""

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
    single_txn: bool = False,
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
    if single_txn:
        cmd.append("-1")
    if tuples_only:
        cmd.append("-At")
    else:
        cmd.append("-q")
    cmd.extend(["-c", sql])
    proc = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(
            f"psql failed ({proc.returncode}): {sql[:180]!r}\n"
            f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        )
    return proc.stdout


def last_data_line(text: str) -> str:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    # Skip psql command tags mixed into -At output (LOAD/CREATE/DO/...).
    skip = {
        "LOAD",
        "CREATE TABLE",
        "CREATE",
        "DO",
        "INSERT",
        "DELETE",
        "SELECT",
        "SET",
    }
    for line in reversed(lines):
        if line in skip or line.startswith("CREATE "):
            continue
        return line
    raise RuntimeError(f"no data line in psql output: {text!r}")


def psql_scalar(args: argparse.Namespace, sql: str) -> str:
    return last_data_line(psql(args, sql, tuples_only=True))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--arm", required=True)
    parser.add_argument("--psql", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--iters", type=int, default=200)
    parser.add_argument("--expect-merge", action="store_true")
    parser.add_argument("--json-out", required=True)
    parser.add_argument("--explain-out", required=True)
    args = parser.parse_args()

    psql(args, "LOAD 'koldstore';")

    explain_hist = psql(
        args,
        "LOAD 'koldstore'; EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) "
        "SELECT * FROM history WHERE ks_id = 1;",
    )
    explain_count = psql(
        args, "LOAD 'koldstore'; EXPLAIN (COSTS OFF) SELECT count(*) FROM history;"
    )
    explain_cust = psql(
        args,
        "EXPLAIN (COSTS OFF) SELECT * FROM customer "
        "WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1;",
    )

    explain_text = (
        f"===== arm={args.arm} history PK lookup =====\n{explain_hist}\n"
        f"===== arm={args.arm} history count(*) =====\n{explain_count}\n"
        f"===== arm={args.arm} customer PK lookup =====\n{explain_cust}\n"
    )
    Path(args.explain_out).write_text(explain_text, encoding="utf-8")

    uses_merge = "Custom Scan (KoldMergeScan)" in explain_hist
    opened = 0
    m = re.search(r"Cold segments: considered=(\d+),.*?opened=(\d+)", explain_hist)
    if m:
        opened = int(m.group(2))
    cold_segs = int(psql_scalar(args, "SELECT count(*) FROM koldstore.cold_segments WHERE status='active';") or "0")

    if args.expect_merge:
        if not uses_merge:
            raise SystemExit(
                f"error: arm={args.arm} expected KoldMergeScan on history PK lookup, got:\n{explain_hist}"
            )
        if opened < 1:
            raise SystemExit(
                f"error: arm={args.arm} expected opened>=1 cold segment on history PK, plan:\n{explain_hist}"
            )

    max_ks = int(psql_scalar(args, "SELECT coalesce(max(ks_id), 1) FROM ONLY history;") or "1")

    timing_raw = psql(
        args,
        f"""
LOAD 'koldstore';
CREATE TEMP TABLE _ks_bench (hist_ms double precision, cust_ms double precision);
DO $$
DECLARE
  iters int := {args.iters};
  max_ks bigint := {max_ks};
  i int;
  probe bigint;
  t0 timestamptz;
  hist_ms double precision;
  cust_ms double precision;
BEGIN
  t0 := clock_timestamp();
  FOR i IN 1..iters LOOP
    IF i % 2 = 0 THEN
      probe := 1 + ((i - 1) % greatest(max_ks / 4, 1));
    ELSE
      probe := 1 + ((i - 1) % greatest(max_ks, 1));
    END IF;
    PERFORM * FROM history WHERE ks_id = probe;
  END LOOP;
  hist_ms := 1000.0 * extract(epoch FROM clock_timestamp() - t0);

  t0 := clock_timestamp();
  FOR i IN 1..iters LOOP
    PERFORM * FROM customer
    WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1 + ((i - 1) % 100);
  END LOOP;
  cust_ms := 1000.0 * extract(epoch FROM clock_timestamp() - t0);

  INSERT INTO _ks_bench VALUES (hist_ms, cust_ms);
END $$;
SELECT hist_ms::text || ' ' || cust_ms::text FROM _ks_bench;
""",
        tuples_only=True,
        single_txn=True,
    )
    timing_line = last_data_line(timing_raw)
    hist_ms_s, cust_ms_s = timing_line.split()
    hist_ms = float(hist_ms_s)
    cust_ms = float(cust_ms_s)

    hist_only = int(psql_scalar(args, "SELECT count(*) FROM ONLY history;"))
    hist_visible = int(psql_scalar(args, "LOAD 'koldstore'; SELECT count(*) FROM history;"))
    hist_heap = int(psql_scalar(args, "SELECT pg_total_relation_size('history'::regclass);"))
    cust_heap = int(psql_scalar(args, "SELECT pg_total_relation_size('customer'::regclass);"))
    cold_rows = int(psql_scalar(args, "SELECT coalesce(sum(row_count),0)::bigint FROM koldstore.cold_segments WHERE status='active';") or "0")
    cold_bytes = int(psql_scalar(args, "SELECT coalesce(sum(byte_size),0)::bigint FROM koldstore.cold_segments WHERE status='active';") or "0")

    result = {
        "arm": args.arm,
        "history_pk_ms": round(hist_ms, 2),
        "customer_pk_ms": round(cust_ms, 2),
        "history_pk_avg_ms": round(hist_ms / max(args.iters, 1), 4),
        "customer_pk_avg_ms": round(cust_ms / max(args.iters, 1), 4),
        "iters": args.iters,
        "plan_history_pk_uses_merge_scan": uses_merge,
        "plan_history_pk_cold_segments_opened": opened,
        "cold_segments": cold_segs,
        "cold_rows": cold_rows,
        "cold_bytes": cold_bytes,
        "history_heap_bytes": hist_heap,
        "customer_heap_bytes": cust_heap,
        "history_heap_rows": hist_only,
        "history_visible_rows": hist_visible,
    }
    Path(args.json_out).write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
