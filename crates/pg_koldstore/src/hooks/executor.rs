//! DML hook and clean-schema mirror integration.

use koldstore_common::{scope, ScopeError, ScopeKey, TableKind};

pub use koldstore_merge::{
    extract_simple_pk_delete_predicate, plan_managed_delete_effect, plan_managed_insert_effect,
    plan_managed_update_effect, simple_pk_delete_supported, ManagedDmlEffect, SimplePkPredicate,
    HOT_DML_MANIFEST_SYNC_STATE,
};

/// DML operations observed by the managed hook shell.
#[must_use]
pub const fn managed_dml_hook_names() -> &'static [&'static str] {
    &["INSERT", "UPDATE", "DELETE", "MERGE", "COPY"]
}

/// Enforces user-scope checks before managed DML touches heap rows or cold metadata.
///
/// # Errors
///
/// Returns a scope error when user-scoped DML is missing an active session scope,
/// has no row scope, or targets a different scope.
pub fn enforce_dml_scope(
    table_kind: TableKind,
    session_user_id: Option<&str>,
    row_scope: Option<&ScopeKey>,
) -> Result<Option<ScopeKey>, ScopeError> {
    let active_scope = scope::active_scope_for_table(table_kind, session_user_id)?;
    if let Some(active_scope) = active_scope.as_ref() {
        scope::enforce_row_scope(active_scope, row_scope)?;
    }
    Ok(active_scope)
}

#[cfg(feature = "pg")]
mod live {
    use std::sync::atomic::{AtomicBool, Ordering};

    use pgrx::pg_sys;

    static REGISTERED: AtomicBool = AtomicBool::new(false);
    static mut PREVIOUS: pg_sys::ExecutorEnd_hook_type = None;

    pub(super) fn register() {
        if REGISTERED.swap(true, Ordering::AcqRel) {
            return;
        }
        unsafe {
            PREVIOUS = pg_sys::ExecutorEnd_hook;
            pg_sys::ExecutorEnd_hook = Some(executor_end);
        }
    }

    #[pgrx::pg_guard]
    unsafe extern "C-unwind" fn executor_end(query_desc: *mut pg_sys::QueryDesc) {
        unsafe {
            let changed_managed_relation = changed_managed_relation(query_desc);
            if let Some(previous) = PREVIOUS {
                previous(query_desc);
            } else {
                pg_sys::standard_ExecutorEnd(query_desc);
            }
            if changed_managed_relation {
                crate::database_worker::wake::mark_managed_dml_pending();
            }
            // Reclaim Rust heap after merge-scan / flush spikes even when the
            // next client command is a tiny keepalive (`SELECT 1`).
            crate::memory::release_process_heap_if_pending();
        }
    }

    unsafe fn changed_managed_relation(query_desc: *mut pg_sys::QueryDesc) -> bool {
        unsafe {
            if query_desc.is_null()
                || (*query_desc).plannedstmt.is_null()
                || (*query_desc).estate.is_null()
                || (*(*query_desc).estate).es_processed == 0
            {
                return false;
            }
            if !matches!(
                (*query_desc).operation,
                pg_sys::CmdType::CMD_INSERT
                    | pg_sys::CmdType::CMD_UPDATE
                    | pg_sys::CmdType::CMD_DELETE
                    | pg_sys::CmdType::CMD_MERGE
            ) {
                return false;
            }

            let planned = (*query_desc).plannedstmt;
            let result_relations = (*planned).resultRelations;
            let rtable = (*planned).rtable;
            if result_relations.is_null() || rtable.is_null() {
                return false;
            }
            for index in 0..(*result_relations).length as usize {
                let range_table_index = (*(*result_relations).elements.add(index)).int_value;
                if range_table_index <= 0 || range_table_index > (*rtable).length {
                    continue;
                }
                let rte = (*(*rtable).elements.add((range_table_index - 1) as usize))
                    .ptr_value
                    .cast::<pg_sys::RangeTblEntry>();
                if !rte.is_null()
                    && (*rte).rtekind == pg_sys::RTEKind::RTE_RELATION
                    && crate::catalog::cache::is_managed_relation((*rte).relid)
                {
                    return true;
                }
            }
            false
        }
    }
}

/// Registers the lightweight managed-DML completion hook.
#[cfg(feature = "pg")]
pub(crate) fn register_executor_end_hook() {
    live::register();
}
