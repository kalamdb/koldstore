//! Typed hot cursor over the native PostgreSQL child plan.
//!
//! For ordered progressive paths, hot candidates come from `ExecProcNode` on
//! the ordered index/seq child instead of SPI `to_jsonb` keyset paging. Reads
//! run under the relation-owner merge identity so RLS cannot hide a newer hot
//! winner before cold versions are masked; user `WHERE` / RLS still apply later
//! via `ExecScan`.

#![allow(unsafe_op_in_unsafe_fn)]

use koldstore_common::{HotRow, LogicalPk, PkColumn, SeqId};
use koldstore_merge::scan::HOT_SEQ_SENTINEL;
use koldstore_migrate::order::CatalogColumn;
use pgrx::pg_sys;

use super::hot::HotMergeBatchReader;
use super::literals::datum_to_json_value;
use super::{exec_proc_node, tuple_slot_is_empty, with_hook_disabled};

/// First adaptive page for native ordered hot merge under parent `Limit`.
pub(super) const NATIVE_HOT_FIRST_BATCH_ROWS: usize = 8;
/// Steady-state adaptive page after the first full native hot fetch.
pub(super) const NATIVE_HOT_BATCH_ROWS: usize = 64;

/// Hot merge source used by [`super::execute::MergeRowStream`].
#[derive(Debug)]
pub(super) enum HotMergeSource {
    /// Legacy mixed merge: SPI JSON keyset pages ordered by primary key.
    SpiJson(HotMergeBatchReader),
    /// Ordered progressive: native child slots under trusted merge identity.
    NativeChild(NativeHotCursor),
}

impl HotMergeSource {
    pub(super) fn empty(relation_owner: pg_sys::Oid) -> Self {
        Self::SpiJson(HotMergeBatchReader::empty(relation_owner))
    }

    pub(super) fn next_batch(&mut self) -> Result<Option<Vec<HotRow>>, String> {
        match self {
            Self::SpiJson(reader) => reader.next_batch(),
            Self::NativeChild(cursor) => cursor.next_batch(),
        }
    }

    pub(super) fn reset(&mut self) {
        match self {
            Self::SpiJson(reader) => reader.reset(),
            Self::NativeChild(cursor) => cursor.reset(),
        }
    }

    pub(super) fn first_page_sql(&self) -> Option<String> {
        match self {
            Self::SpiJson(reader) => reader.first_page_sql(),
            Self::NativeChild(_) => None,
        }
    }

    #[allow(dead_code)] // Useful for EXPLAIN / adaptive paging follow-ups.
    pub(super) fn is_native_child(&self) -> bool {
        matches!(self, Self::NativeChild(_))
    }
}

/// Pulls ordered hot candidates from a live child `PlanState`.
#[derive(Debug)]
pub(super) struct NativeHotCursor {
    child: *mut pg_sys::PlanState,
    relation_owner: pg_sys::Oid,
    pk_columns: Vec<PkColumn>,
    /// Catalog columns used to decode slot attributes into a row image.
    catalog_columns: Vec<CatalogColumn>,
    batch_size: usize,
    exhausted: bool,
}

impl NativeHotCursor {
    /// Builds a cursor over an already-initialized custom-scan child.
    ///
    /// # Safety
    /// `child` must remain valid for the CustomScan lifetime.
    pub(super) unsafe fn open(
        child: *mut pg_sys::PlanState,
        relation_owner: pg_sys::Oid,
        pk_columns: Vec<PkColumn>,
        catalog_columns: Vec<CatalogColumn>,
    ) -> Result<Self, String> {
        if child.is_null() {
            return Err("ordered hot cursor requires an initialized native child".to_string());
        }
        if relation_owner == pg_sys::InvalidOid {
            return Err("managed relation has no valid owner".to_string());
        }
        if pk_columns.is_empty() {
            return Err("ordered hot cursor requires primary-key columns".to_string());
        }
        Ok(Self {
            child,
            relation_owner,
            pk_columns,
            catalog_columns,
            batch_size: NATIVE_HOT_FIRST_BATCH_ROWS,
            exhausted: false,
        })
    }

    /// Fetches up to one adaptive page of hot rows under the merge identity.
    ///
    /// The first page is small so parent `Limit` can stop after a handful of
    /// `ExecProcNode` pulls; subsequent pages grow toward
    /// [`NATIVE_HOT_BATCH_ROWS`].
    pub(super) fn next_batch(&mut self) -> Result<Option<Vec<HotRow>>, String> {
        if self.exhausted {
            return Ok(None);
        }
        let child = self.child;
        let pk_columns = self.pk_columns.clone();
        let catalog_columns = self.catalog_columns.clone();
        let batch_size = self.batch_size;
        let rows =
            crate::catalog::owner::with_relation_owner_for_merge(self.relation_owner, || {
                with_hook_disabled(|| unsafe {
                    pull_hot_rows_from_child(child, &pk_columns, &catalog_columns, batch_size)
                })
            })?;
        if rows.len() < batch_size {
            self.exhausted = true;
        } else if self.batch_size < NATIVE_HOT_BATCH_ROWS {
            self.batch_size = NATIVE_HOT_BATCH_ROWS;
        }
        if rows.is_empty() {
            Ok(None)
        } else {
            Ok(Some(rows))
        }
    }

    pub(super) fn reset(&mut self) {
        self.exhausted = false;
        self.batch_size = NATIVE_HOT_FIRST_BATCH_ROWS;
    }
}

unsafe fn pull_hot_rows_from_child(
    child: *mut pg_sys::PlanState,
    pk_columns: &[PkColumn],
    catalog_columns: &[CatalogColumn],
    batch_size: usize,
) -> Result<Vec<HotRow>, String> {
    let mut rows = Vec::with_capacity(batch_size);
    while rows.len() < batch_size {
        let slot = exec_proc_node(child);
        if tuple_slot_is_empty(slot) {
            break;
        }
        rows.push(hot_row_from_slot(slot, pk_columns, catalog_columns)?);
    }
    Ok(rows)
}

unsafe fn hot_row_from_slot(
    slot: *mut pg_sys::TupleTableSlot,
    pk_columns: &[PkColumn],
    catalog_columns: &[CatalogColumn],
) -> Result<HotRow, String> {
    let max_attnum = catalog_columns
        .iter()
        .map(|column| column.column_id.get())
        .filter(|attnum| *attnum > 0)
        .max()
        .unwrap_or(0);
    if max_attnum > 0 {
        ensure_slot_attrs(slot, max_attnum);
    }

    let mut row_image = serde_json::Map::new();
    let mut pk_object = serde_json::Map::new();

    for column in catalog_columns {
        let attnum = column.column_id.get();
        if attnum <= 0 {
            continue;
        }
        let index = (attnum - 1) as usize;
        if index >= (*slot).tts_nvalid as usize {
            continue;
        }
        let is_null = *(*slot).tts_isnull.add(index);
        let value = if is_null {
            serde_json::Value::Null
        } else {
            let datum = *(*slot).tts_values.add(index);
            datum_to_json_value(datum, column).unwrap_or(serde_json::Value::Null)
        };
        if pk_columns
            .iter()
            .any(|pk| pk.as_str().eq_ignore_ascii_case(&column.name))
        {
            pk_object.insert(column.name.clone(), value.clone());
        }
        row_image.insert(column.name.clone(), value);
    }

    for pk in pk_columns {
        if pk_object.contains_key(pk.as_str()) {
            continue;
        }
        return Err(format!(
            "native hot cursor slot omitted primary-key column `{}`",
            pk.as_str()
        ));
    }

    let pk_json = serde_json::Value::Object(pk_object);
    let pk =
        LogicalPk::from_json_object(&pk_json, pk_columns).map_err(|error| error.to_string())?;
    let seq = SeqId::new(HOT_SEQ_SENTINEL).map_err(|error| error.to_string())?;
    Ok(HotRow {
        pk,
        scope_key: None,
        seq,
        deleted: false,
        row_image: serde_json::Value::Object(row_image),
    })
}

unsafe fn ensure_slot_attrs(slot: *mut pg_sys::TupleTableSlot, attnum: i16) {
    if slot.is_null() || attnum <= 0 {
        return;
    }
    // Already deformed / virtual projection already filled Datum arrays.
    if (*slot).tts_nvalid >= attnum {
        return;
    }
    // VirtualTTSOps::getsomeattrs elog(ERROR) if called; leave as-is.
    if (*slot).tts_ops == std::ptr::addr_of!(pg_sys::TTSOpsVirtual) {
        return;
    }
    pg_sys::slot_getsomeattrs_int(slot, i32::from(attnum));
}
