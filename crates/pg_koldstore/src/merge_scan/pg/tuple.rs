//! Tuple slot and scan-owned Datum helpers for KoldMergeScan.
//!
//! Materialized rows live in a dedicated AllocSet created at BeginCustomScan.
//! EndCustomScan clears scan/result slots, then drops that context so Datums are
//! released only after PostgreSQL no longer aliases them.
//!
//! On portal ERROR, PostgreSQL skips `ExecutorEnd` / `EndCustomScan` and deletes
//! the portal memory tree (including this AllocSet). Abort scrub then
//! [`ScanMemory::disown`]s so Rust Drop does not call `MemoryContextDelete` a
//! second time (glibc double-free / backend Abort).

use pgrx::memcxt::PgMemoryContexts;
use pgrx::pg_sys;

/// One projected result row owned by the scan memory context.
#[derive(Debug)]
pub(super) struct MaterializedRow {
    pub(super) values: Vec<pg_sys::Datum>,
    pub(super) is_null: Vec<bool>,
}

/// Scan-local AllocSet that owns all materialized Datums for one CustomScan node.
#[derive(Debug)]
pub(super) struct ScanMemory {
    context: Option<PgMemoryContexts>,
}

impl ScanMemory {
    /// Creates a child AllocSet under `CurrentMemoryContext`.
    pub(super) fn create(name: &str) -> Self {
        Self {
            context: Some(PgMemoryContexts::new(name)),
        }
    }

    /// Runs `f` with allocations going into this scan context.
    pub(super) unsafe fn switch<T>(&mut self, f: impl FnOnce() -> T) -> T {
        let context = self
            .context
            .as_mut()
            .expect("ScanMemory::switch after disown");
        context.switch_to(|_| f())
    }

    /// Releases Datums from the previously emitted streamed row.
    ///
    /// # Safety
    ///
    /// Callers must clear any TupleTableSlot that still aliases Datums from this
    /// context before reset, and must not retain those Datums afterward.
    pub(super) unsafe fn reset(&mut self) {
        let context = self
            .context
            .as_mut()
            .expect("ScanMemory::reset after disown");
        context.reset();
    }

    /// Relinquish ownership when PostgreSQL already deleted the AllocSet.
    ///
    /// Portal abort deletes query memory children before Rust can run
    /// `EndCustomScan`. Forgetting the wrapper avoids a second
    /// `MemoryContextDelete` when the stale [`super::ScanExecutionState`] is
    /// later drained.
    pub(super) fn disown(&mut self) {
        if let Some(context) = self.context.take() {
            std::mem::forget(context);
        }
    }
}

pub(super) unsafe fn store_materialized_row(
    slot: *mut pg_sys::TupleTableSlot,
    row: &MaterializedRow,
    slot_indexes: &[usize],
    tuple_width: usize,
) {
    // Virtual slots only clear validity flags; they do not free external Datums.
    clear_slot(slot);

    let slot_natts = slot_attribute_count(slot).unwrap_or(tuple_width);
    let width = tuple_width.min(slot_natts);
    for index in 0..width {
        *(*slot).tts_values.add(index) = pg_sys::Datum::null();
        *(*slot).tts_isnull.add(index) = true;
    }
    for (slot_index, (value, is_null)) in slot_indexes
        .iter()
        .copied()
        .zip(row.values.iter().copied().zip(row.is_null.iter().copied()))
    {
        if slot_index >= width {
            continue;
        }
        *(*slot).tts_values.add(slot_index) = value;
        *(*slot).tts_isnull.add(slot_index) = is_null;
    }
    (*slot).tts_nvalid = width as pg_sys::AttrNumber;
    pg_sys::ExecStoreVirtualTuple(slot);
}

pub(super) unsafe fn clear_slot(slot: *mut pg_sys::TupleTableSlot) {
    if slot.is_null() {
        return;
    }
    if !(*slot).tts_ops.is_null() {
        if let Some(clear) = (*(*slot).tts_ops).clear {
            clear(slot);
        }
    }
}

/// Clears CustomScan slots that may still alias scan-owned Datums.
///
/// Must run before dropping [`ScanMemory`] so `ExecClearTuple` never frees
/// pointers that already vanished with the AllocSet.
pub(super) unsafe fn clear_custom_scan_slots(node: *mut pg_sys::CustomScanState) {
    if node.is_null() {
        return;
    }
    clear_slot((*node).ss.ss_ScanTupleSlot);
    clear_slot((*node).ss.ps.ps_ResultTupleSlot);
}

pub(super) unsafe fn slot_attribute_count(slot: *mut pg_sys::TupleTableSlot) -> Option<usize> {
    if slot.is_null() || (*slot).tts_tupleDescriptor.is_null() {
        return None;
    }
    usize::try_from((*(*slot).tts_tupleDescriptor).natts).ok()
}
