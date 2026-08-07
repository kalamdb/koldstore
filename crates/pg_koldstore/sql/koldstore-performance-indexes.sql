-- Worker/job hot-path indexes kept separate from the catalog bootstrap so the
-- durable schema remains readable while scheduler-specific access paths can
-- evolve independently.

-- One-shot flush executors select due pending work in this exact order. Keep the
-- JSON payload out of the index: force is fetched for at most a tiny bounded page.
CREATE INDEX IF NOT EXISTS jobs_flush_dispatch_idx
  ON koldstore.jobs (available_at, updated_at, id)
  INCLUDE (table_oid)
  WHERE job_type = 'flush' AND status = 'pending';

-- Retention deletes terminal jobs oldest-first in small SKIP LOCKED batches.
CREATE INDEX IF NOT EXISTS jobs_terminal_finished_idx
  ON koldstore.jobs (finished_at, id)
  WHERE status IN ('completed', 'cancelled', 'error')
    AND finished_at IS NOT NULL;
