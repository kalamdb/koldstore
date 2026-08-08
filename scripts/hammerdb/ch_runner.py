#!/usr/bin/env python3
"""Run CH-benCHmark-style analytical queries against a HammerDB schema.

Modes:
  after       — one pass (or N loops) then exit
  concurrent  — loop until --duration-seconds or SIGTERM
  only        — same as after (caller skips TPROC-C)
"""

from __future__ import annotations

import argparse
import json
import random
import signal
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from ch_queries import CH_QUERIES  # noqa: E402

RANDOM_SEED = 123
_stop = False


def _on_signal(_signum: int, _frame: object) -> None:
    global _stop
    _stop = True


def psql_run(
    *,
    psql: str,
    host: str,
    port: str,
    database: str,
    user: str | None,
    query: str,
) -> tuple[int, float]:
    cmd = [
        psql,
        "-P",
        "pager=off",
        "-v",
        "ON_ERROR_STOP=1",
        "-h",
        host,
        "-p",
        str(port),
        "-d",
        database,
        "-c",
        query,
    ]
    if user:
        cmd.extend(["-U", user])
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, check=False, capture_output=True, text=True)
    ms = (time.perf_counter() - t0) * 1000.0
    if proc.returncode != 0:
        raise RuntimeError(
            f"CH query failed ({proc.returncode}):\n{proc.stderr or proc.stdout}"
        )
    return proc.returncode, ms


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("after", "concurrent", "only"), required=True)
    parser.add_argument("--psql", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--user", default="")
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--loops", type=int, default=1, help="full Q1–Q22 passes for after/only")
    parser.add_argument(
        "--duration-seconds",
        type=int,
        default=0,
        help="concurrent mode: run until this many seconds (0 = until SIGTERM)",
    )
    parser.add_argument("--json-out", required=True)
    parser.add_argument("--seed", type=int, default=RANDOM_SEED)
    args = parser.parse_args()

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    random.seed(args.seed)
    order = list(range(len(CH_QUERIES)))
    random.shuffle(order)

    latencies: list[dict[str, float | int]] = []
    started = time.time()
    completed = 0
    failures = 0

    def run_one(q_index: int) -> None:
        nonlocal completed, failures
        query = CH_QUERIES[q_index]
        try:
            _, ms = psql_run(
                psql=args.psql,
                host=args.host,
                port=args.port,
                database=args.database,
                user=args.user or None,
                query=query,
            )
            latencies.append({"q": q_index + 1, "ms": round(ms, 2)})
            completed += 1
        except Exception as exc:  # noqa: BLE001 — collect then fail closed
            failures += 1
            latencies.append({"q": q_index + 1, "ms": -1, "error": str(exc)[:400]})
            raise

    if args.mode in ("after", "only"):
        for _ in range(max(args.loops, 1)):
            for q_index in order:
                if _stop:
                    break
                run_one(q_index)
    else:
        # concurrent: cycle until duration or signal
        idx = 0
        while not _stop:
            if args.duration_seconds > 0 and (time.time() - started) >= args.duration_seconds:
                break
            run_one(order[idx % len(order)])
            idx += 1

    elapsed = max(time.time() - started, 1e-6)
    summary = {
        "mode": args.mode,
        "queries_defined": len(CH_QUERIES),
        "completed": completed,
        "failures": failures,
        "elapsed_seconds": round(elapsed, 3),
        "qph": round(3600.0 * completed / elapsed, 2) if completed else 0.0,
        "latencies": latencies,
    }
    Path(args.json_out).write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: summary[k] for k in summary if k != "latencies"}, indent=2))
    if failures:
        raise SystemExit(f"error: {failures} CH query failure(s); see {args.json_out}")


if __name__ == "__main__":
    main()
