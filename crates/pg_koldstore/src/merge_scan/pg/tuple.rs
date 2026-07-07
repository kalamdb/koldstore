//! Tuple slot and SPI datum ownership helpers for KoldMergeScan.

use pgrx::pg_sys;

#[derive(Debug)]
pub(super) struct MaterializedRow {
    pub(super) values: Vec<pg_sys::Datum>,
    pub(super) is_null: Vec<bool>,
}

pub(super) unsafe fn store_materialized_row(
    slot: *mut pg_sys::TupleTableSlot,
    row: &MaterializedRow,
) {
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

pub(super) unsafe fn copy_spi_datum(
    tupdesc: *mut pg_sys::TupleDescData,
    attr_index: usize,
    datum: pg_sys::Datum,
) -> pg_sys::Datum {
    #[cfg(feature = "pg18")]
    {
        let natts = (*tupdesc).natts as usize;
        let compact_attr = &(*tupdesc).compact_attrs.as_slice(natts)[attr_index];
        pg_sys::SPI_datumTransfer(datum, compact_attr.attbyval, i32::from(compact_attr.attlen))
    }
    #[cfg(not(feature = "pg18"))]
    {
        let attr = (*tupdesc).attrs.as_ptr().add(attr_index);
        pg_sys::SPI_datumTransfer(datum, (*attr).attbyval, i32::from((*attr).attlen))
    }
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
