//! Tuple slot and scan-owned Datum helpers for KoldMergeScan.
//!
//! Materialized rows live in a dedicated AllocSet created at BeginCustomScan.
//! EndCustomScan drops that context, releasing all pass-by-ref Datums at once.

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
    context: PgMemoryContexts,
}

impl ScanMemory {
    /// Creates a child AllocSet under `CurrentMemoryContext`.
    pub(super) fn create(name: &str) -> Self {
        Self {
            context: PgMemoryContexts::new(name),
        }
    }

    /// Runs `f` with allocations going into this scan context.
    pub(super) unsafe fn switch<T>(&mut self, f: impl FnOnce() -> T) -> T {
        self.context.switch_to(|_| f())
    }
}

pub(super) unsafe fn store_materialized_row(
    slot: *mut pg_sys::TupleTableSlot,
    row: &MaterializedRow,
) {
    // Virtual slots only clear validity flags; they do not free external Datums.
    clear_slot(slot);

    let slot_natts = slot_attribute_count(slot).unwrap_or(row.values.len());
    let copied = row.values.len().min(slot_natts);
    for (index, (value, is_null)) in row
        .values
        .iter()
        .copied()
        .zip(row.is_null.iter().copied())
        .take(copied)
        .enumerate()
    {
        *(*slot).tts_values.add(index) = value;
        *(*slot).tts_isnull.add(index) = is_null;
    }
    (*slot).tts_nvalid = copied as pg_sys::AttrNumber;
    pg_sys::ExecStoreVirtualTuple(slot);
}

unsafe fn clear_slot(slot: *mut pg_sys::TupleTableSlot) {
    if !(*slot).tts_ops.is_null() {
        if let Some(clear) = (*(*slot).tts_ops).clear {
            clear(slot);
        }
    }
}

unsafe fn slot_attribute_count(slot: *mut pg_sys::TupleTableSlot) -> Option<usize> {
    if (*slot).tts_tupleDescriptor.is_null() {
        return None;
    }
    usize::try_from((*(*slot).tts_tupleDescriptor).natts).ok()
}
