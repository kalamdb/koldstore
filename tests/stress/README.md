# Chat penetration stress (`tests/stress`)

Manual / CI soak for PostgreSQL + `pg_koldstore` under a wide-row chat workload
with aggressive flush, history readers, and optional feature packs.

Design: [`docs/plans/2026-07-19-chat-penetration-stress-design.md`](../../docs/plans/2026-07-19-chat-penetration-stress-design.md)

## Run

```bash
# Default: packs=chat, 5 minutes
scripts/run-chat-penetration.sh

# Short local smoke
KOLDSTORE_STRESS_SOAK_SECONDS=30 \
  scripts/run-chat-penetration.sh --packs chat,cold_dml,multi_table,joins

# Full v1 packs + async mirror
scripts/run-chat-penetration.sh --packs chat,cold_dml,multi_table,joins,async
```

Cold Parquet lands under **`tmp/chat_penetration/`** in the repo (cleared at the start of
each run; kept after the process exits so you can inspect it).

Reports land under `target/stress/` (also uploaded by `.github/workflows/chat-penetration.yml`).

During the soak, progress lines print every `KOLDSTORE_STRESS_PROGRESS_INTERVAL_SECS`
(default 5s) with message counts and live p50/p95/p99 for insert/history/join/cold_update.

Defaults: `max_rows_per_file=2000`, writer delay `1ms` (~2× prior insert/update rate).

**Do not run a second** `scripts/run-chat-penetration.sh` / `cargo pgrx start` against the
same PG version while a soak is in progress — prepare force-stops the cluster and all
workers will see `connection closed`. The harness now aborts on that instead of spamming.
