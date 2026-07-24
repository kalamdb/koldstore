//! DROP TABLE ProcessUtility cleanup for managed KoldStore tables.
//!
//! Order matters to avoid deadlocks with an in-flight flush:
//! 1. Resolve OIDs with `NoLock` (do not hold relation locks across waits)
//! 2. Signal cooperative cancel (`table_cancel_requests`)
//! 3. Wait for the table-job advisory lock (flush holds it for the statement)
//! 4. Catalog + object-store cleanup, then allow PostgreSQL DROP
//! 5. Drop the change-log mirror after the heap is gone

use koldstore_common::QualifiedTableName;
use koldstore_manifest::table_object_prefix;
use koldstore_migrate::drop_table::{plan_drop_table_cleanup, DropTableCleanupPolicy};
use koldstore_storage::{open_client_from_catalog_fields, StorageClient};
use pgrx::datum::DatumWithOid;
use pgrx::pg_sys;

/// Cancels jobs and removes cold artifacts for managed tables in a DROP TABLE.
///
/// # Errors
///
/// Returns an error when catalog SPI or object-store GC fails.
pub(super) fn cleanup_managed_tables_before_drop(
    table_oids: &[pg_sys::Oid],
) -> Result<Vec<QualifiedTableName>, String> {
    let mut mirrors = Vec::new();
    for &table_oid in table_oids {
        if !crate::catalog::cache::is_managed_relation(table_oid) {
            continue;
        }
        if let Some(mirror) = cleanup_one_managed_table_before_drop(table_oid)? {
            mirrors.push(mirror);
        }
    }
    Ok(mirrors)
}

/// Drops change-log mirrors captured before DROP (best-effort after heap gone).
pub(super) fn drop_captured_mirrors(mirrors: &[QualifiedTableName]) {
    for mirror in mirrors {
        let sql = format!("DROP TABLE IF EXISTS {}", mirror.quoted());
        if let Err(error) = pgrx::Spi::run(&sql) {
            pgrx::warning!(
                "koldstore drop: failed to drop mirror {}: {error}",
                mirror.quoted()
            );
        }
    }
}

fn cleanup_one_managed_table_before_drop(
    table_oid: pg_sys::Oid,
) -> Result<Option<QualifiedTableName>, String> {
    // Cancel first so a concurrent flush can stop at its next wave check, then
    // wait for the same advisory lock flush holds for the whole statement. That
    // serializes DROP cleanup after flush releases relation locks — no deadlock
    // between DROP AccessExclusive and flush AccessShare on heap/mirror.
    let cancelled = crate::sql::flush::jobs::cancel_jobs_for_drop(table_oid)?;
    pgrx::log!(
        "koldstore drop: oid={} cancelled_or_signalled_jobs={}",
        table_oid.to_u32(),
        cancelled
    );
    crate::sql::job_lock_pg::lock_table_job(table_oid)?;

    let relation = crate::catalog::resolve::relation_context(table_oid)?;
    let storage = crate::catalog::resolve::active_flush_storage_context(table_oid)?;
    let mirror = crate::catalog::resolve::mirror_relation_by_table_oid(table_oid)?;
    let prefix = table_object_prefix(&relation.namespace, &relation.name);
    let table = QualifiedTableName::parse(&format!("{}.{}", relation.namespace, relation.name))
        .map_err(|error| error.to_string())?;

    let client = open_client_from_catalog_fields(
        &storage.storage_type,
        &storage.base_path,
        &storage.credentials,
        &storage.config,
    )
    .map_err(|error| error.to_string())?;

    let plan = plan_drop_table_cleanup(table, table_oid.to_u32(), DropTableCleanupPolicy::Delete)
        .map_err(|error| error.to_string())?;
    for statement in &plan.statements {
        crate::spi::update(statement, &[DatumWithOid::from(table_oid)])
            .map_err(|error| error.to_string())?;
    }

    let objects = client.list(&prefix).map_err(|error| error.to_string())?;
    for object in &objects {
        client
            .delete(&object.key)
            .map_err(|error| error.to_string())?;
    }
    pgrx::log!(
        "koldstore drop: table_oid={} deleted_objects={} prefix={}",
        table_oid.to_u32(),
        objects.len(),
        prefix
    );

    if let Some(audit) = &plan.audit_job {
        crate::spi::update(audit, &[DatumWithOid::from(table_oid)])
            .map_err(|error| error.to_string())?;
    }

    crate::catalog::cache::invalidate_table_globally(table_oid);
    crate::spi::invalidate_all_prepared_plans();
    Ok(mirror)
}

/// Resolves table OIDs named by a `DROP TABLE` statement (missing_ok aware).
///
/// Uses `NoLock` so this hook does not hold relation locks while waiting for a
/// concurrent flush to finish after cancel.
///
/// # Safety
///
/// `stmt` must point at a live `DropStmt`.
pub(super) unsafe fn drop_table_oids(stmt: *mut pg_sys::DropStmt) -> Vec<pg_sys::Oid> {
    unsafe {
        if stmt.is_null() || (*stmt).removeType != pg_sys::ObjectType::OBJECT_TABLE {
            return Vec::new();
        }
        let objects = (*stmt).objects;
        if objects.is_null() {
            return Vec::new();
        }
        let mut oids = Vec::new();
        let count = (*objects).length as usize;
        // RVROption is a C enum: MSVC bindgen often types it as signed `c_int`,
        // while `RangeVarGetRelidExtended` takes `uint32` flags on every PG major.
        let flags: u32 = if (*stmt).missing_ok {
            pg_sys::RVROption::RVR_MISSING_OK as u32
        } else {
            0
        };
        for index in 0..count {
            let names = (*(*objects).elements.add(index))
                .ptr_value
                .cast::<pg_sys::List>();
            if names.is_null() {
                continue;
            }
            let relation = pg_sys::makeRangeVarFromNameList(names);
            if relation.is_null() {
                continue;
            }
            let oid = pg_sys::RangeVarGetRelidExtended(
                relation,
                pg_sys::NoLock as pg_sys::LOCKMODE,
                flags,
                None,
                std::ptr::null_mut(),
            );
            if oid != pg_sys::InvalidOid {
                oids.push(oid);
            }
        }
        oids
    }
}
