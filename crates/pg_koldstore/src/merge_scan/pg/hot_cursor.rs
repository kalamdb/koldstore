//! Typed hot cursor over the native PostgreSQL child plan.
//!
//! For ordered progressive / unordered hot-first paths, hot candidates come
//! from `ExecProcNode` on the native child instead of SPI `to_jsonb` keyset
//! paging. Reads run under the relation-owner merge identity so RLS cannot
//! hide a newer hot winner before cold versions are masked; user `WHERE` /
//! RLS still apply later via `ExecScan`.
//!
//! Projected IndexScan slots renumber TupleDesc attnums to `1..n` and often
//! leave `attname` empty, so decode maps slot indexes from the child's plan
//! `targetlist` Vars (`varattno` / `resname`). When the child tlist omits the
//! primary key (e.g. `count(*)`), open fails and callers fall back to SPI.

#![allow(unsafe_op_in_unsafe_fn)]

use koldstore_common::{HotRow, LogicalPk, PkColumn, SeqId};
use koldstore_merge::scan::HOT_SEQ_SENTINEL;
use koldstore_migrate::order::CatalogColumn;
use pgrx::pg_sys;

use super::hot::HotMergeBatchReader;
use super::literals::datum_to_json_value;
use super::pg_list::{list_len, list_nth_ptr};
use super::{exec_proc_node, tuple_slot_is_empty, with_hook_disabled};

/// First adaptive page for native ordered hot merge under parent `Limit`.
pub(super) const NATIVE_HOT_FIRST_BATCH_ROWS: usize = 8;
/// Steady-state adaptive page after the first full native hot fetch.
pub(super) const NATIVE_HOT_BATCH_ROWS: usize = 64;

/// Hot merge source used by [`super::execute::MergeRowStream`].
#[derive(Debug)]
pub(super) enum HotMergeSource {
    /// GeneralMerge / native-open fallback: SPI JSON keyset pages.
    SpiJson(HotMergeBatchReader),
    /// Ordered/unordered progressive: native child slots under merge identity.
    NativeChild(NativeHotCursor),
}

impl HotMergeSource {
    /// Empty hot source (cold-only point path); not a live SPI query.
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
}

/// Pulls ordered hot candidates from a live child `PlanState`.
#[derive(Debug)]
pub(super) struct NativeHotCursor {
    child: *mut pg_sys::PlanState,
    relation_owner: pg_sys::Oid,
    pk_columns: Vec<PkColumn>,
    catalog_columns: Vec<CatalogColumn>,
    /// Slot index → relation attnum from the child plan tlist.
    slot_attnums: Vec<i16>,
    batch_size: usize,
    exhausted: bool,
}

impl NativeHotCursor {
    /// Builds a cursor over an already-initialized custom-scan child.
    ///
    /// # Safety
    /// `child` must remain valid for the CustomScan lifetime.
    ///
    /// # Errors
    ///
    /// Returns an error when the child targetlist does not cover every primary
    /// key column (callers should fall back to SPI JSON).
    pub(super) unsafe fn open(
        child: *mut pg_sys::PlanState,
        relation_owner: pg_sys::Oid,
        _table_oid: pg_sys::Oid,
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
        let slot_attnums = child_slot_attnums(child, &catalog_columns)?;
        if !pk_columns_covered(&pk_columns, &catalog_columns, &slot_attnums) {
            return Err("native hot cursor child targetlist omits primary-key columns".to_string());
        }
        // Also require every catalog column that appears in the child tlist to
        // be decodable; callers that need additional projection columns (RLS
        // scope, SELECT list) fall back to SPI when the child omits them.
        Ok(Self {
            child,
            relation_owner,
            pk_columns,
            catalog_columns,
            slot_attnums,
            batch_size: NATIVE_HOT_FIRST_BATCH_ROWS,
            exhausted: false,
        })
    }

    /// Fetches up to one adaptive page of hot rows under the merge identity.
    pub(super) fn next_batch(&mut self) -> Result<Option<Vec<HotRow>>, String> {
        if self.exhausted {
            return Ok(None);
        }
        let child = self.child;
        let pk_columns = self.pk_columns.clone();
        let catalog_columns = self.catalog_columns.clone();
        let slot_attnums = self.slot_attnums.clone();
        let batch_size = self.batch_size;
        let rows =
            crate::catalog::owner::with_relation_owner_for_merge(self.relation_owner, || {
                with_hook_disabled(|| unsafe {
                    pull_hot_rows_from_child(
                        child,
                        &pk_columns,
                        &catalog_columns,
                        &slot_attnums,
                        batch_size,
                    )
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

    /// True when the child tlist includes relation `attnum`.
    #[must_use]
    pub(super) fn covers_attnum(&self, attnum: i16) -> bool {
        attnum > 0 && self.slot_attnums.contains(&attnum)
    }
}

fn pk_columns_covered(
    pk_columns: &[PkColumn],
    catalog_columns: &[CatalogColumn],
    slot_attnums: &[i16],
) -> bool {
    pk_columns.iter().all(|pk| {
        catalog_columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(pk.as_str()))
            .is_some_and(|column| slot_attnums.contains(&column.column_id.get()))
    })
}

unsafe fn pull_hot_rows_from_child(
    child: *mut pg_sys::PlanState,
    pk_columns: &[PkColumn],
    catalog_columns: &[CatalogColumn],
    slot_attnums: &[i16],
    batch_size: usize,
) -> Result<Vec<HotRow>, String> {
    let mut rows = Vec::with_capacity(batch_size);
    while rows.len() < batch_size {
        let slot = exec_proc_node(child);
        if tuple_slot_is_empty(slot) {
            break;
        }
        rows.push(hot_row_from_slot(
            slot,
            pk_columns,
            catalog_columns,
            slot_attnums,
        )?);
    }
    Ok(rows)
}

unsafe fn hot_row_from_slot(
    slot: *mut pg_sys::TupleTableSlot,
    pk_columns: &[PkColumn],
    catalog_columns: &[CatalogColumn],
    slot_attnums: &[i16],
) -> Result<HotRow, String> {
    if slot.is_null() {
        return Err("native hot cursor received a null tuple slot".to_string());
    }

    let need = i16::try_from(slot_attnums.len()).unwrap_or(i16::MAX);
    ensure_slot_attrs(slot, need);
    let nvalid = (*slot).tts_nvalid.max(0) as usize;

    let mut row_image = serde_json::Map::new();
    let mut pk_object = serde_json::Map::new();

    for column in catalog_columns {
        let attnum = column.column_id.get();
        if attnum <= 0 {
            continue;
        }
        let Some(index) = slot_attnums.iter().position(|mapped| *mapped == attnum) else {
            continue;
        };
        if index >= nvalid {
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
            "native hot cursor slot omitted primary-key column `{}` (slot attnums: {:?})",
            pk.as_str(),
            slot_attnums
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

/// Builds slot-index → relation-attnum from the child plan targetlist.
unsafe fn child_slot_attnums(
    child: *mut pg_sys::PlanState,
    catalog_columns: &[CatalogColumn],
) -> Result<Vec<i16>, String> {
    if child.is_null() || (*child).plan.is_null() {
        return Err("ordered hot cursor child plan is null".to_string());
    }
    let tlist = (*(*child).plan).targetlist;
    let len = list_len(tlist);
    let mut out = Vec::with_capacity(usize::try_from(len).unwrap_or(0));
    for index in 0..len {
        let entry = list_nth_ptr(tlist, index).cast::<pg_sys::TargetEntry>();
        if entry.is_null() {
            out.push(0);
            continue;
        }
        if let Some(attnum) = var_attnum((*entry).expr.cast()) {
            out.push(attnum);
            continue;
        }
        let resname = if (*entry).resname.is_null() {
            String::new()
        } else {
            std::ffi::CStr::from_ptr((*entry).resname)
                .to_string_lossy()
                .into_owned()
        };
        let attnum = catalog_columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(&resname))
            .map(|column| column.column_id.get())
            .unwrap_or(0);
        out.push(attnum);
    }
    Ok(out)
}

unsafe fn var_attnum(expr: *mut pg_sys::Expr) -> Option<i16> {
    let expr = unwrap_relabel(expr);
    if expr.is_null() || (*expr).type_ != pg_sys::NodeTag::T_Var {
        return None;
    }
    let var = expr.cast::<pg_sys::Var>();
    let attnum = (*var).varattno;
    (attnum > 0).then_some(attnum)
}

unsafe fn unwrap_relabel(expr: *mut pg_sys::Expr) -> *mut pg_sys::Expr {
    if expr.is_null() {
        return expr;
    }
    if (*expr).type_ == pg_sys::NodeTag::T_RelabelType {
        let relabel = expr.cast::<pg_sys::RelabelType>();
        (*relabel).arg.cast::<pg_sys::Expr>()
    } else {
        expr
    }
}

unsafe fn ensure_slot_attrs(slot: *mut pg_sys::TupleTableSlot, attnum: i16) {
    if slot.is_null() || attnum <= 0 {
        return;
    }
    if (*slot).tts_nvalid >= attnum {
        return;
    }
    if (*slot).tts_ops == std::ptr::addr_of!(pg_sys::TTSOpsVirtual) {
        return;
    }
    pg_sys::slot_getsomeattrs_int(slot, i32::from(attnum));
}
