//! Async mirror task that applies committed WAL once per worker tick.

use koldstore_worker::{DatabaseWorkerTask, TickResult};

use crate::async_mirror::apply::{apply_bounded, BoundedApplyRequest};

/// SPI-backed async mirror apply task for the shared database worker loop.
pub(crate) struct AsyncMirrorTask {
    database_oid: u32,
}

impl AsyncMirrorTask {
    /// Builds a task bound to one database OID (slot identity).
    #[must_use]
    pub(crate) const fn new(database_oid: u32) -> Self {
        Self { database_oid }
    }

    /// Runs one apply tick.
    ///
    /// When `advance_slot_on_empty` is false, an empty publication peek leaves
    /// `confirmed_flush` unchanged. Commit-wake retries use that mode so
    /// unrelated WAL cannot advance the slot before the watchdog.
    pub(crate) fn tick_with(&self, advance_slot_on_empty: bool) -> Result<TickResult, String> {
        let _ = self.database_oid;
        let started = std::time::Instant::now();
        let mut request = BoundedApplyRequest::available();
        request.advance_slot_on_empty = advance_slot_on_empty;
        let outcome = apply_bounded(request)?;
        let elapsed_ms = i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX);
        crate::observability::record_async_apply_tick(outcome.row_changes, elapsed_ms);
        if outcome.budget_exhausted {
            Ok(TickResult::ContinuePending)
        } else if outcome.row_changes == 0 {
            Ok(TickResult::ContinueIdle)
        } else {
            Ok(TickResult::Continue)
        }
    }
}

impl DatabaseWorkerTask for AsyncMirrorTask {
    fn name(&self) -> &'static str {
        "async_mirror_apply"
    }

    /// Peeks and applies available committed WAL for this database.
    ///
    /// Idempotent under crash: mirror upserts are PK `ON CONFLICT` and the slot
    /// advances only after a durable `applied_lsn` checkpoint.
    ///
    /// # Errors
    ///
    /// Returns an error when apply fails fatally (including armed failpoints).
    fn tick(&self) -> Result<TickResult, String> {
        self.tick_with(true)
    }
}
