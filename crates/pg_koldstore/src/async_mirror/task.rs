//! Async mirror task that applies committed WAL once per worker tick.

use koldstore_worker::{DatabaseWorkerTask, TickResult};

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
        let _ = self.database_oid;
        crate::async_mirror::apply::apply_available()?;
        Ok(TickResult::Continue)
    }
}
