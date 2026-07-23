# Jobs Control Plane Design (minimal)

**Date:** 2026-07-23  
**Status:** Draft for implementation planning  
**ADR:** [006-jobs-platform](../decisions/006-jobs-platform.md)  
**Related:** crate reorg plan (`koldstore-jobs` — **deferred**), segment publication ADR-004,
async flush prune fence, current watermark catch-up in flush waves

**Scope choice (accepted):** prefer **minimal, stable changes** on the current
inline flush/migrate design. Do **not** build a general-purpose job runner,
claim worker, or handler framework in v1. Add only what operators need:
live progress, cancel, DROP cleanup, and honest catalog/docs.

---

## 1. Goals

Make KoldStore’s **existing** `koldstore.jobs` + inline executors honest and
controllable:

| Goal | Meaning |
|------|---------|
| Traceable | Other sessions can see `status`, `phase`, and progress while work runs |
| Controllable | Cancel a job; DROP/unmanage cancels work and cleans cold data |
| Idempotent | Retry / resume never double-publishes cold data (keep ADR-004 fence) |
| Fail-safe | Crash or cancel leaves hot authoritative; pending objects recoverable |
| Honest | Remove or finish dead schema/docs/planners so the catalog matches code |

Non-goals for v1 (explicitly deferred — avoid maintenance cost):

- New `koldstore-jobs` crate / `JobHandler` trait / multi-claimer worker loop
- Always-enqueue + background-only execution
- Lease claim system, `priority`/`run_after` scheduling, auto-retry framework
- Real parallel async flushes across tables (needs apply-lock split)
- A separate UI app (SQL poll contract only)
- Perfect exactly-once object deletes on every cancel path

---

## 2. Current state (truth)

Today `koldstore.jobs` is a **durable audit + uniqueness spine** attached to
**inline executors**:

```text
enqueue / ensure  →  advisory table lock  →  run work in caller/worker xact  →  mark terminal
```

| Piece | Reality |
|-------|---------|
| Status CHECK | `pending`, `running`, `dry_run`, `completed`, `cancelled`, `error` |
| Uniqueness | One active flush per `(table, scope)`; one migrate per table; one flush\|migrate per table |
| Flush progress columns | Written, but usually same transaction as the whole job → **not visible live** |
| Lease columns / claim indexes | **Removed** from install SQL (no claimer) |
| `max_running_jobs` | **Removed** GUC |
| Cancel API | **None** yet; demigrate sets `cancelled` |
| DROP TABLE job teardown | Planner for `drop_table_cleanup` exists; **not hooked** |
| `recover_segments` | Live path recovers orphans; docs that say it “records recovery jobs” drift |
| Retry | No auto-retry; scheduler skips tables with flush `error` for ~60s |
| Resume | Checkpoints stored on flush; execution does not “continue job id” |

Flush **data-plane** fail-safe already exists and must be preserved:

- Pending segments are invisible until activate + generation CAS
- Crash before activate → orphan pending / objects → `recover_segments` + retry flush
- Hot remains authoritative if flush fails

The platform gap is the **control plane** (progress, cancel, lease, resume), not
the cold publish protocol.

---

## 3. Target architecture (minimal)

### 3.1 Keep current shape

```text
┌──────────────────────────────────────────────────────────────┐
│ pg_koldstore (unchanged ownership)                           │
│  SQL: flush_table, manage_table, cancel_*, list_jobs, DROP   │
│  Inline executors + existing database_worker scheduler       │
│  Small helpers for progress COMMIT + cancel checks           │
└─────────────────────────────┬────────────────────────────────┘
                              │
          ┌───────────────────┴───────────────────┐
          ▼                                       ▼
┌──────────────────────┐               ┌──────────────────────┐
│ koldstore-flush      │               │ koldstore-migrate    │
│ (domain unchanged)   │               │ (domain unchanged)   │
└──────────────────────┘               └──────────────────────┘
```

Still **one** table: `koldstore.jobs`. No second queue. No Redis.

`koldstore-jobs` crate extraction stays in the crate-reorg backlog until
there is a second real consumer beyond flush/migrate helpers — not v1.

### 3.2 Status machine (v1)

```text
pending ──start──► running ──► completed
                      │
                      ├── cancel request ──► cancelling ──► cancelled
                      │
                      └── failure ──► error
```

Rules:

- Keep existing uniqueness (one active flush per scope; one migrate; etc.).
- Add `cancelling` only if needed for UI; otherwise `cancel_requested_at` on
  `running` is enough for v1.
- Terminal states stay terminal except retention GC (P2 —
  https://github.com/kalamdb/koldstore/issues/54).

Deferred: lease reclaim, claim worker, auto-retry loops.

### 3.3 Progress contract (UI-ready, minimal)

| Field | Purpose |
|-------|---------|
| `status` | Badge / filter |
| `phase` | Current stage string (flush/migrate enums) |
| `progress_current` / `progress_total` / `progress_unit` | Bar |
| existing `rows_*` / `batches_completed` / checkpoints | Detail + resume |

**Visibility rule:** write `phase` / `progress_*` on the job row between waves
inside the flush transaction. Cross-session live mid-flush visibility is
**deferred (B.1)** — not worth PROCEDURE/autonomous complexity for v1. Peers see
progress when the flush statement commits.

Domain safety still comes from pending segments + generation CAS, not from
holding one giant catalog transaction.

### 3.4 Execution model (accepted)

- **`flush_table` / migrate stay inline** in the caller (and scheduler still
  calls flush inline). Enqueue-only background execution is deferred.
- Cancel is cooperative: between waves, re-read `cancel_requested_at` /
  status and stop at the next safe boundary.
- DROP/unmanage cancel + `drop_table_cleanup` (Delete) as already accepted.

---

## 4. Idempotency model

Idempotency is defined **per side-effect**, not “rerun the whole SQL function”.

### 4.1 Flush side-effects

| Stage | Side-effect | Idempotency key / rule |
|-------|-------------|-------------------------|
| Claim | Job → `running` | Unique active index; start marks running (no lease claimer in v1) |
| Select | Read-only | Watermark `catchup_upto_seq` pinned at job start (already) |
| Encode + upload | Object + `cold_segments` pending row | Deterministic object path per `(table, batch, content)` or per `(job_id, wave, batch)`; insert pending ON CONFLICT / path unique |
| Activate | Pending → active + generation CAS | CAS miss ⇒ conflict; safe to retry after re-read generation |
| Prune | DELETE mirror/hot `seq <= prune_max_seq` | Re-running same bound is a no-op / fewer rows |
| Counters | Row-count deltas | Must be tied to activate once; use job checkpoint so delta applied once |
| Complete | Job terminal | UPDATE … WHERE status IN ('running','cancelling') |

**Hard rule:** never activate the same logical segment twice. Pending-until-CAS
is the publish fence (ADR-004).

**Hard rule:** object keys for a retry must not collide with a previous
partial upload in a way that validates the wrong bytes. Prefer paths that
include `job_id` + monotonic `batch_number`, or content hash.

### 4.2 Migrate side-effects

| Stage | Idempotency |
|-------|-------------|
| Create mirror / triggers | IF NOT EXISTS / catalog version check |
| Backfill batches | Cursor in `payload` / `checkpoint_*`; skip already-copied PK ranges |
| Complete | Same terminal CAS as flush |

### 4.3 Control-plane side-effects

| Action | Idempotency |
|--------|-------------|
| `cancel_job` | Second call on terminal job is no-op success |
| DROP cleanup | Object deletes are idempotent (missing = success) |

Deferred: lease reclaim CAS, `error` → `pending` auto-retry enqueue.


---

## 5. Failure mid-run and resume (all cases)

Legend:

- **Safe** — hot authoritative; no wrong cold visibility
- **Resume** — how the next claim continues
- **GC** — what cleanup is required

### 5.1 Flush matrix

| Failure point | Catalog / objects | Safe? | Resume |
|---------------|-------------------|-------|--------|
| After claim, before any pending insert | Job `running` (if progress committed) or xact abort → prior state | Yes | Next start re-selects under watermark |
| Mid-encode / mid-upload | Partial object possible; no pending or incomplete pending | Yes | GC orphan object; new batch path; continue |
| Pending inserted, not activated | Pending rows + objects | Yes | Activate if complete set; else expire pending + re-encode |
| Activate CAS success, prune not done | Cold visible; hot may still hold copies | Yes (merge correct) | Resume prune with stored `prune_max_seq` / checkpoint |
| Prune done, complete not marked | Data done; job still `running` | Yes | Mark completed (idempotent) |
| Crash holding apply lock | Lock released on backend death | Yes | Worker resumes apply; job reclaim as above |
| Cancel before activate | Pending not published | Yes | Mark cancelled; expire/GC pending for this job |
| Cancel after activate, during prune | Cold already public | Yes | Finish prune for consistency; mark **`completed`**; set `payload.cancel_requested_after_publish=true` for audit |
| Concurrent second `flush_table` | Blocked by table lock / unique active job | Yes | Wait or skip (scheduler try-lock) |
| Generation CAS conflict | Other publisher won | Yes | Abort wave; job error or retry with fresh read |
| Async WAL apply lag | Selection watermark excludes post-start rows | Yes | Next job drains remainder (intentional) |

**Cancel vs activate (normative — accepted):**

1. If cancel observed **before** successful activate → do not activate; GC pending; `cancelled`.
2. If activate already committed → **do not roll back cold publish**; finish prune if required for merge consistency, then mark **`completed`**. Late cancel is recorded in payload (`cancel_requested_after_publish: true`) for audit, but status stays `completed` so operators are not told the publish was undone.

### 5.2 Migrate matrix

| Failure point | Safe? | Resume |
|---------------|-------|--------|
| Mid-backfill batch | Yes (mirror incomplete; manage not active / or dual-write rules as today) | Resume cursor from payload |
| After mirror ready, before triggers | Follow existing manage transactional boundaries | Restart manage or resume job |
| Demigrate with running flush | Cancel flush first (already partly true) | Then demigrate |

### 5.3 Stuck / timeout (v1 minimal)

| Event | Action |
|-------|--------|
| Operator `cancel_job` | Set `cancel_requested_at` (and optional `cancelling`); owner stops at next wave check |
| Backend crash mid-job | Transaction/locks release; pending segments recovered via existing `recover_segments`; job left non-terminal → next start/scheduler marks `error` or resumes from checkpoint **without** a lease claimer |
| Wall-clock timeout / lease reclaim / `max_attempts` auto-retry | **Deferred** — keep existing error cooldown skip for auto-flush |

### 5.4 DROP TABLE / unmanage

**Normative — accepted:** DROP must **cancel active jobs and delete cold data**, not leave orphans.

| Event | Action |
|-------|--------|
| `unmanage` / demigrate | Cancel all `pending\|running\|cancelling` jobs for `table_oid`; existing demigrate already drops mirror/local schema |
| `DROP TABLE` | Hook ProcessUtility **before** relation teardown: (1) cancel all active jobs for `table_oid`; (2) deactivate catalog metadata; (3) enqueue `drop_table_cleanup` with **Delete** policy to remove object-store artifacts; (4) allow DROP to proceed so Postgres heap/OID go away |
| `drop_table_cleanup` handler | Idempotent delete of cold segments/objects for that OID; mark job `completed`; safe if objects already gone |
| Orphan jobs for dropped OID | Never leave `running`; cancel+reclaim first, then cleanup job owns artifact GC |

`DropTableCleanupPolicy::Retain` / `Failed` stay for operator/recovery paths; **default DROP path is Delete**.

---

## 6. What to clean up (delete or finish)

| Item | Action |
|------|--------|
| Unwired `drop_table_cleanup` job planner | **Finish** (accepted): wire DROP → cancel jobs + Delete-policy cleanup job + implement handler |
| Docs: `recover_segments` “records recovery jobs” | **Fix docs** to match live orphan recovery |
| `max_running_jobs` / lease / claim columns | **Removed** (accepted) — no dormant GUCs or unused indexes |
| Migrate stays `pending` all backfill | **Mark `running` + phase + cursor progress** (same minimal pattern as flush) |
| Duplicate architecture claims (`koldstore-worker` owns leases) | **Correct** crate-architecture.md |
| Flush single-txn progress updates | **COMMIT job row between waves** (accepted); keep data safety via pending/CAS |
| Error cooldown-only “retry” | **Keep for v1**; no `run_after` column |
| Unbounded terminal job retention | Retention GC — P2 (https://github.com/kalamdb/koldstore/issues/54) |
| `koldstore-jobs` crate / JobHandler | **Deferred** until control helpers prove painful to duplicate |

Do not invent parallel abstractions (second queue table, Redis, per-type job tables).

---

## 7. Public SQL surface (proposed)

```sql
-- Progress / UI
SELECT * FROM koldstore.jobs
 WHERE status IN ('pending','running','cancelling')
 ORDER BY updated_at;

SELECT koldstore.list_jobs(
  statuses => ARRAY['running','pending'],
  job_types => NULL,
  table_name => NULL
);

-- Control
SELECT koldstore.cancel_job(job_id uuid);
SELECT koldstore.cancel_table_jobs(table_name regclass);

-- Existing
SELECT koldstore.flush_table(...);
SELECT koldstore.enqueue_flush_job(...);
SELECT koldstore.manage_table(...);
```

`describe_table` keeps embedding recent jobs; `list_jobs` is the dashboard entry.

GUC (honest for v1):

| GUC | Behavior |
|-----|----------|
| Existing flush/scheduler GUCs | Unchanged |
| `max_running_jobs` / lease GUCs | **Removed** |

Do not add timeout/retry GUCs in v1.


---

## 8. Flush handler stages (concrete)

Suggested `phase` values:

1. `claimed`
2. `selecting`
3. `encoding` (per wave)
4. `uploading`
5. `activating`
6. `pruning`
7. `finished` / `failed` / `cancelled`

Progress example for UI:

- `progress_unit = 'waves'` or `'rows'`
- `progress_current = rows_flushed`
- `progress_total = estimated rows at watermark` (fixed at claim)

Between waves: COMMIT job progress + cancel check.

Watermark catch-up (already landed) remains: one job drains `seq <= catchup_upto`
only, so resume/retry does not chase concurrent apply forever.

---

## 9. Phased delivery

### Phase A — Design lock (this doc + ADR)
- Accept minimal control-plane scope
- Accepted: cancel-after-publish → `completed`
- Accepted: DROP cancels + Delete cleanup
- Accepted: progress COMMITs between waves; keep inline `flush_table`

### Phase B — Progress visibility
- Typed flush phases (`claimed` / `selecting` / `writing` / `activating` / …)
- Job row progress fields (`progress_current` / `progress_total` / `progress_unit`)
- `list_jobs` SQL
- Tests: progress fields + `list_jobs` after flush
- **Status:** landed except mid-flush cross-session COMMIT (see B.1)

### Phase B.1 — Live cross-session progress (**deferred**)

Tracked in https://github.com/kalamdb/koldstore/issues/52

Mid-flush `COMMIT` from `SECURITY DEFINER` `flush_table` FATAL-asserts.
Do **not** add PROCEDURE/autonomous-writer complexity unless a product need
appears. Peers see progress when the flush statement ends; that is enough for v1.

### Phase C — Cancel + DROP/unmanage
- `cancel_job` / `cancel_table_jobs`
- Cooperative checks at wave boundaries (including after `after_select_rows`)
- ProcessUtility DROP: cancel → wait table-job lock → deactivate → delete cold objects → audit job
- Tests: cancel pending; cancel before activate; DROP cleans objects; DROP during flush
- **Status:** landed (no mid-flush live progress — see B.1 / #52). DROP during
  flush signals cancel then waits on the flush advisory lock so relation-lock
  deadlocks cannot occur.

### Phase D — Honesty / crash hygiene (still minimal)
- Fix docs drift (`recover_segments`) — **done**
- ~~Remove unused lease/`max_running_jobs`~~ (done with schema cleanup)
- Migrate: mark `running` + cursor/`progress_*` between backfill batches — **done**
- Stuck non-terminal after crash: abandon durable `running` flush jobs when the
  table-job lock is free (scheduler + `flush_table` claim) — **done**
- **Status:** landed

### Phase E — Deferred runner / leases / auto-retry (**only if needed later**)

Tracked in https://github.com/kalamdb/koldstore/issues/53

- Extract `koldstore-jobs`
- Lease claim/reclaim (would require new columns again — avoid unless necessary)
- Auto-retry scheduling
- Always-enqueue background execution
- Apply-lock split for parallel table uploads

---

## 10. Testing plan (minimum)

| Case | Proof |
|------|-------|
| Live progress | Session A flushes; session B polls `list_jobs` mid-wave |
| Cancel pre-activate | No new active segments; job `cancelled` |
| Cancel post-activate | Cold correct; job **`completed`** + `cancel_requested_after_publish` (tracked: https://github.com/kalamdb/koldstore/issues/57) |
| Crash mid-upload | recover_segments + retry; no duplicate active segment (hardening: https://github.com/kalamdb/koldstore/issues/58) |
| Crash post-activate pre-prune | Resume prune; merge remains correct |
| DROP during flush | Jobs cancelled/reclaimed; cold objects deleted; DROP succeeds |
| Watermark | Concurrent inserts during flush do not unbounded-wave the same job |
| Crash mid-upload | recover_segments + retry; no duplicate active segment |
| Crash post-activate pre-prune | Resume prune; merge remains correct |

Deferred tests (Phase E+): stale lease reclaim, retry exhausted, multi-claimer fairness.

Prefer pgrx `#[pg_test]` for lifecycle and e2e for DROP/crash.

---

## 11. Open decisions (resolve before Phase B coding)

1. ~~**Cancel after activate:**~~ **Accepted:** mark **`completed`**; audit via `payload.cancel_requested_after_publish`.
2. ~~**`drop_table_cleanup` job type:**~~ **Accepted:** finish + wire — DROP cancels jobs and cleans data (`Delete` policy).
3. ~~**Progress commits:**~~ **Accepted:** commit job row between existing waves/stages; keep one `flush_table` API; reuse worker-style `CommitTransactionCommand` — no multi-call flush redesign.
4. ~~**Inline vs enqueue-only:**~~ **Accepted:** keep synchronous inline execution; enqueue-only deferred.
5. ~~**Platform complexity:**~~ **Accepted:** minimal control plane only; defer `koldstore-jobs`, leases/claimer, auto-retry framework.
6. ~~**Unused lease / `max_running_jobs`:**~~ **Accepted:** delete GUC + lease/claim columns/indexes from install SQL (no deprecation).

---

## 12. Success criteria

- Operator can watch flush stages and a progress bar from SQL alone.
- Operator can cancel; DROP cancels jobs, removes cold artifacts, and never leaves a running job on a dead OID.
- Crash mid-flush is resumed or cleaned without duplicate cold publish.
- Schema/docs match running code (no fake claimer story).
- No new job framework crate required to ship v1.
