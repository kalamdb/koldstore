//! Logical-slot and publication lifecycle for asynchronous mirror capture.
//!
//! A database owns one slot and one publication. Tables enter the publication
//! only after strict capture has initialized their mirror, which prevents a gap
//! while switching the write path from triggers to committed WAL.

use std::time::Duration;

use koldstore_common::{quote_ident, MirrorCaptureMode, QualifiedTableName};
use koldstore_worker::DatabaseOid;
use pgrx::datum::DatumWithOid;

const APPLY_LOCK_NAMESPACE: i32 = 1_263_354_732;
const WORKER_LOCK_NAMESPACE: i32 = 1_263_354_733;
const SLOT_PROVISION_LOCK_NAMESPACE: i32 = 1_263_354_734;
const LIFECYCLE_LOCK_NAMESPACE: i32 = 1_263_354_735;

/// Publication shared by async managed tables in one database.
pub const PUBLICATION_NAME: &str = "koldstore_async_mirror";

/// Returns the cluster-unique logical slot name for a database OID.
#[must_use]
pub(crate) fn slot_name(database_oid: u32) -> String {
    format!("koldstore_async_{database_oid}")
}

/// Prepares the database slot before `manage_table` performs transactional DDL.
///
/// PostgreSQL rejects logical-slot creation after a transaction has written.
/// This function therefore validates `wal_level` and reserves the deterministic
/// slot before the manage workflow takes locks or changes catalogs.
///
/// # Errors
///
/// Returns an error when logical decoding is disabled, installation did not
/// cannot provision the publication, the current transaction already has an
/// XID (slot creation would deadlock), or the deterministic slot name is
/// incompatible.
pub(crate) fn prepare_capture(mode: MirrorCaptureMode) -> Result<(), String> {
    if mode != MirrorCaptureMode::Async {
        return Ok(());
    }
    if unsafe { pgrx::pg_sys::wal_level }
        != pgrx::pg_sys::WalLevel::WAL_LEVEL_LOGICAL as std::os::raw::c_int
    {
        return Err(
            "mirror_capture_mode=async requires wal_level=logical (restart PostgreSQL after changing it)"
                .to_string(),
        );
    }

    let database_oid = unsafe { pgrx::pg_sys::MyDatabaseId }.to_u32();
    let slot = slot_name(database_oid);
    // Native advisory locking and slot discovery avoid SPI assigning the
    // parent transaction an XID before the autonomous worker finds its
    // consistent point.
    native_slot_provision_lock(database_oid, true);
    let prepared = prepare_slot_locked(database_oid, &slot);
    native_slot_provision_lock(database_oid, false);
    prepared?;
    // Held until manage_table commits. Cleanup takes the same lock, preventing
    // it from removing infrastructure between slot preparation and catalog
    // activation.
    lock_database(LIFECYCLE_LOCK_NAMESPACE, database_oid)?;
    Ok(())
}

fn prepare_slot_locked(database_oid: u32, slot: &str) -> Result<(), String> {
    // Do not call SPI before creating a missing logical slot: pgrx SPI assigns
    // the parent an XID, and the slot's consistent-point search would then
    // wait for the parent while the parent waits for the provisioner.
    let slot_ready = native_slot_exists(slot);
    let publication_ready = native_publication_exists();
    if !slot_ready || !publication_ready {
        require_no_assigned_xid_for_slot_provision()?;
        super::provision::provision_infrastructure(database_oid)?;
    }
    validate_slot(slot)?;
    if publication_exists()? {
        Ok(())
    } else {
        Err(format!(
            "async mirror provisioner did not create publication {PUBLICATION_NAME}"
        ))
    }
}

/// Slot init waits for concurrent XIDs. If this backend already wrote, the
/// provisioner blocks on us while we block on its shutdown → deadlock.
fn require_no_assigned_xid_for_slot_provision() -> Result<(), String> {
    let xid = unsafe { pgrx::pg_sys::GetCurrentTransactionIdIfAny() };
    if xid != pgrx::pg_sys::InvalidTransactionId {
        return Err(
            "async mirror slot provisioning cannot run after the current transaction has written; \
             commit preceding statements first, then call manage_table with mirror_capture_mode => 'async'"
                .to_string(),
        );
    }
    Ok(())
}

fn native_publication_exists() -> bool {
    let name = std::ffi::CString::new(PUBLICATION_NAME).expect("publication name contains no NUL");
    unsafe { pgrx::pg_sys::get_publication_oid(name.as_ptr(), true) != pgrx::pg_sys::InvalidOid }
}

pub(super) fn publication_exists() -> Result<bool, String> {
    pgrx::Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = $1)",
        &[DatumWithOid::from(PUBLICATION_NAME)],
    )
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "publication existence query returned no row".to_string())
}

pub(crate) fn native_slot_exists(slot: &str) -> bool {
    let slot = std::ffi::CString::new(slot).expect("deterministic slot name contains no NUL");
    native_slot_exists_cstr(&slot)
}

pub(crate) fn native_slot_exists_cstr(slot: &std::ffi::CStr) -> bool {
    !unsafe { pgrx::pg_sys::SearchNamedReplicationSlot(slot.as_ptr(), true) }.is_null()
}

unsafe extern "C-unwind" fn advisory_lock_int4(
    call_info: pgrx::pg_sys::FunctionCallInfo,
) -> pgrx::pg_sys::Datum {
    unsafe { pgrx::pg_sys::pg_advisory_lock_int4(call_info) }
}

unsafe extern "C-unwind" fn advisory_unlock_int4(
    call_info: pgrx::pg_sys::FunctionCallInfo,
) -> pgrx::pg_sys::Datum {
    unsafe { pgrx::pg_sys::pg_advisory_unlock_int4(call_info) }
}

fn native_slot_provision_lock(database_oid: u32, acquire: bool) {
    let function = if acquire {
        advisory_lock_int4
    } else {
        advisory_unlock_int4
    };
    unsafe {
        pgrx::pg_sys::DirectFunctionCall2Coll(
            Some(function),
            pgrx::pg_sys::InvalidOid,
            pgrx::pg_sys::Datum::from(SLOT_PROVISION_LOCK_NAMESPACE),
            pgrx::pg_sys::Datum::from(database_oid as i32),
        );
    }
}

/// Atomically switches one initialized table from triggers to WAL capture.
///
/// The publication membership and trigger removal share the migration
/// transaction. Earlier WAL was already mirrored by strict triggers; later WAL
/// is visible to the async applier.
///
/// # Errors
///
/// Returns an error when publication DDL or trigger removal fails.
pub(crate) fn activate_table(
    mode: MirrorCaptureMode,
    source: &QualifiedTableName,
    mirror: &QualifiedTableName,
    primary_key: &koldstore_common::PrimaryKeyShape,
) -> Result<(), String> {
    if mode != MirrorCaptureMode::Async {
        return Ok(());
    }

    let publication = quote_ident(PUBLICATION_NAME);

    let source_oid = pgrx::Spi::get_one_with_args::<pgrx::pg_sys::Oid>(
        "SELECT $1::regclass::oid",
        &[DatumWithOid::from(source.quoted().as_str())],
    )
    .map_err(|error| error.to_string())?
    .ok_or_else(|| format!("source table {} no longer exists", source.quoted()))?;
    let is_member = pgrx::Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (\
           SELECT 1 FROM pg_catalog.pg_publication_rel pr \
           JOIN pg_catalog.pg_publication p ON p.oid = pr.prpubid \
           WHERE p.pubname = $1 AND pr.prrelid = $2\
         )",
        &[
            DatumWithOid::from(PUBLICATION_NAME),
            DatumWithOid::from(source_oid),
        ],
    )
    .map_err(|error| error.to_string())?
    .unwrap_or(false);
    if !is_member {
        let published_columns = primary_key
            .columns()
            .iter()
            .map(|column| quote_ident(column.column().as_str()))
            .collect::<Vec<_>>()
            .join(", ");
        pgrx::Spi::run(&format!(
            "ALTER PUBLICATION {publication} ADD TABLE {} ({published_columns})",
            source.quoted(),
        ))
        .map_err(|error| error.to_string())?;
    }

    let drop = koldstore_mirror::plan_drop_mirror_dml_triggers(source, mirror)
        .map_err(|error| error.to_string())?;
    pgrx::Spi::run(&drop.sql).map_err(|error| error.to_string())?;
    let kick_names = koldstore_mirror::async_worker_kick_trigger_names(&mirror.name);
    let legacy_kicks = [
        koldstore_mirror::async_worker_kick_trigger_name(&mirror.name),
        // Truncated mid-migration names from the `_ins/_upd/_del` experiment.
        {
            let mut name = format!("{}_async_worker_kick_ins", mirror.name);
            name.truncate(63);
            while !name.is_char_boundary(name.len()) {
                name.pop();
            }
            name
        },
        {
            let mut name = format!("{}_async_worker_kick_upd", mirror.name);
            name.truncate(63);
            while !name.is_char_boundary(name.len()) {
                name.pop();
            }
            name
        },
        {
            let mut name = format!("{}_async_worker_kick_del", mirror.name);
            name.truncate(63);
            while !name.is_char_boundary(name.len()) {
                name.pop();
            }
            name
        },
    ];
    for kick_name in kick_names.iter().cloned().chain(legacy_kicks) {
        pgrx::Spi::run(&format!(
            "DROP TRIGGER IF EXISTS {} ON {}",
            quote_ident(&kick_name),
            source.quoted(),
        ))
        .map_err(|error| error.to_string())?;
    }
    // No per-DML kick: the WAL applier is started here, auto-restarted by
    // postmaster on crash, and re-ensured after postmaster restart by the
    // shared_preload launcher / first backend transaction.
    crate::database_worker::require_async_mirror_worker()?;
    Ok(())
}

fn validate_slot(slot: &str) -> Result<(), String> {
    let compatible = pgrx::Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (\
           SELECT 1 FROM pg_catalog.pg_replication_slots \
           WHERE slot_name = $1 AND slot_type = 'logical' \
             AND plugin = 'pgoutput' AND database = pg_catalog.current_database()\
         )",
        &[DatumWithOid::from(slot)],
    )
    .map_err(|error| error.to_string())?
    .unwrap_or(false);
    if compatible {
        Ok(())
    } else {
        Err(format!(
            "async mirror slot {slot} is missing or has an incompatible database, type, or output plugin"
        ))
    }
}

/// Serializes logical decoding and explicit cleanup for one database.
///
/// Uses a transaction-scoped advisory lock. Releasing during Parquet upload
/// requires ending the flush transaction first (session locks deadlock with
/// logical decoding waiting on the flush XID); that multi-txn redesign is
/// deferred. Phase-5.5 still drains WAL before the relation lock.
pub(crate) fn lock_apply(database_oid: u32) -> Result<(), String> {
    lock_database(APPLY_LOCK_NAMESPACE, database_oid)
}

/// Poll interval while waiting for another backend to release the logical slot.
const SLOT_INACTIVE_POLL: Duration = Duration::from_millis(10);
/// Abort-window wait only: locks are released before `ReplicationSlotRelease`.
const SLOT_INACTIVE_MAX_WAIT: Duration = Duration::from_secs(2);

/// Returns the PID holding `slot`, if any.
fn slot_active_pid(slot: &str) -> Result<Option<i32>, String> {
    // Scalar subquery always returns exactly one row (NULL when missing), so SPI
    // never hits "SpiTupleTable positioned before the start or after the end".
    pgrx::Spi::get_one_with_args::<i32>(
        "SELECT (SELECT active_pid FROM pg_catalog.pg_replication_slots WHERE slot_name = $1)",
        &[DatumWithOid::from(slot)],
    )
    .map_err(|error| error.to_string())
}

/// Waits until `slot` is missing or has no `active_pid`.
///
/// PostgreSQL's SQL slot APIs (`pg_logical_slot_peek_*`,
/// `pg_replication_slot_advance`, `pg_drop_replication_slot`) acquire with
/// `nowait=true` and ERROR immediately when another PID holds the slot. Callers
/// must already hold [`lock_apply`] so only abort/exit windows (locks released
/// before `ReplicationSlotRelease`) can still show an active PID.
///
/// # Errors
///
/// Returns an error when the slot stays active longer than
/// [`SLOT_INACTIVE_MAX_WAIT`].
pub(super) fn wait_until_slot_inactive(slot: &str) -> Result<(), String> {
    let deadline = std::time::Instant::now() + SLOT_INACTIVE_MAX_WAIT;
    loop {
        match slot_active_pid(slot)? {
            // Slot gone, or present with NULL active_pid.
            None => return Ok(()),
            Some(pid) if pid == unsafe { pgrx::pg_sys::MyProcPid } => {
                // We already own it (should not happen before peek/drop).
                return Ok(());
            }
            Some(_) => {
                if std::time::Instant::now() >= deadline {
                    return Err(format!(
                        "replication slot {slot} stayed active for longer than {}s",
                        SLOT_INACTIVE_MAX_WAIT.as_secs()
                    ));
                }
                std::thread::sleep(SLOT_INACTIVE_POLL);
                // Allow CANCEL / postmaster death between polls.
                pgrx::check_for_interrupts!();
            }
        }
    }
}

/// Stops the database WAL applier so disable cannot deadlock on [`lock_apply`].
///
/// The applier may hold the apply lock inside a peek that waits for concurrent
/// XIDs — including this backend's open transaction (common under `#[pg_test]`).
/// Terminate by `backend_type` (not only `active_pid`) so a worker blocked
/// before acquiring the slot is also cleared.
fn stop_async_mirror_applier(database_oid: u32, slot: &str) -> Result<(), String> {
    let worker_type = koldstore_worker::async_mirror_worker_type(DatabaseOid::new(database_oid));
    // Always return a row: an empty SELECT through Spi::run_with_args errors with
    // "SpiTupleTable positioned before the start or after the end" when no
    // applier is running (common under parallel E2E once the worker already exited).
    let _ = pgrx::Spi::get_one_with_args::<bool>(
        "SELECT COALESCE(\
           (SELECT bool_or(pg_catalog.pg_terminate_backend(pid)) \
            FROM pg_catalog.pg_stat_activity \
            WHERE backend_type = $1), \
           false)",
        &[DatumWithOid::from(worker_type.as_str())],
    )
    .map_err(|error| error.to_string())?;
    // Also clear a non-worker holder (manual fence) if it still owns the slot.
    if let Some(active_pid) = slot_active_pid(slot)? {
        let my_pid = unsafe { pgrx::pg_sys::MyProcPid };
        if active_pid != my_pid {
            let _ = pgrx::Spi::get_one_with_args::<bool>(
                "SELECT pg_catalog.pg_terminate_backend($1)",
                &[DatumWithOid::from(active_pid)],
            )
            .map_err(|error| error.to_string())?;
        }
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let my_pid = unsafe { pgrx::pg_sys::MyProcPid };
    loop {
        let slot_idle = match slot_active_pid(slot)? {
            None => true,
            Some(pid) => pid == my_pid,
        };
        let worker_gone = pgrx::Spi::get_one_with_args::<bool>(
            "SELECT NOT EXISTS (\
               SELECT 1 FROM pg_catalog.pg_stat_activity WHERE backend_type = $1\
             )",
            &[DatumWithOid::from(worker_type.as_str())],
        )
        .map_err(|error| error.to_string())?
        .unwrap_or(false);
        if slot_idle && worker_gone {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            return Err(format!(
                "async mirror applier for slot {slot} did not stop after terminate"
            ));
        }
        std::thread::sleep(SLOT_INACTIVE_POLL);
        pgrx::check_for_interrupts!();
    }
}

/// Serializes dynamic worker discovery and registration for one database.
pub(crate) fn lock_worker_registration(database_oid: u32) -> Result<(), String> {
    lock_database(WORKER_LOCK_NAMESPACE, database_oid)
}

/// Non-blocking variant for the cluster launcher (skip when a session holds the lock).
pub(crate) fn try_lock_worker_registration(database_oid: u32) -> Result<bool, String> {
    pgrx::Spi::get_one_with_args::<bool>(
        "SELECT pg_catalog.pg_try_advisory_xact_lock($1, $2)",
        &[
            DatumWithOid::from(WORKER_LOCK_NAMESPACE),
            DatumWithOid::from(database_oid as i32),
        ],
    )
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "worker registration try-lock returned no row".to_string())
}

fn lock_database(namespace: i32, database_oid: u32) -> Result<(), String> {
    pgrx::Spi::run_with_args(
        "SELECT pg_catalog.pg_advisory_xact_lock($1, $2)",
        &[
            DatumWithOid::from(namespace),
            DatumWithOid::from(database_oid as i32),
        ],
    )
    .map_err(|error| error.to_string())
}

/// Returns the current database's async slot name.
#[must_use]
pub(super) fn current_slot_name() -> String {
    slot_name(unsafe { pgrx::pg_sys::MyDatabaseId }.to_u32())
}

/// Returns the logical-slot name KoldStore creates for this database.
///
/// SQL contract: `koldstore.async_mirror_slot_name()` returns text and does not
/// mutate replication state.
#[pgrx::pg_extern(name = "async_mirror_slot_name", schema = "koldstore")]
pub fn async_mirror_slot_name() -> String {
    current_slot_name()
}

/// Removes automatic async-mirror infrastructure after async tables are gone.
///
/// SQL contract: `koldstore.disable_async_mirror()` is idempotent and refuses
/// cleanup while an active table still depends on async capture.
#[pgrx::pg_extern(name = "disable_async_mirror", schema = "koldstore", security_definer)]
pub fn disable_async_mirror() -> bool {
    disable_async_mirror_impl()
        .unwrap_or_else(|error| pgrx::error!("disable async mirror failed: {error}"))
}

fn disable_async_mirror_impl() -> Result<bool, String> {
    let database_oid = unsafe { pgrx::pg_sys::MyDatabaseId }.to_u32();
    lock_database(LIFECYCLE_LOCK_NAMESPACE, database_oid)?;
    let active = pgrx::Spi::get_one::<bool>(
        "SELECT EXISTS (\
           SELECT 1 FROM koldstore.schemas \
           WHERE active AND COALESCE(options->>'mirror_capture_mode', 'strict') = 'async'\
         )",
    )
    .map_err(|error| error.to_string())?
    .unwrap_or(false);
    if active {
        return Err(
            "unmanage every async table before disabling async mirror infrastructure".to_string(),
        );
    }
    let slot = slot_name(database_oid);
    // Stop the applier before lock_apply: a peek blocked on concurrent XIDs
    // (including this backend's open transaction) holds the apply lock and
    // would otherwise deadlock with disable.
    stop_async_mirror_applier(database_oid, &slot)
        .map_err(|error| format!("stop applier: {error}"))?;
    lock_apply(database_oid).map_err(|error| format!("lock apply: {error}"))?;
    let slot_exists = pgrx::Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = $1)",
        &[DatumWithOid::from(slot.as_str())],
    )
    .map_err(|error| error.to_string())?
    .unwrap_or(false);
    let publication_exists = publication_exists()?;
    if slot_exists {
        // Drop uses nowait acquire; wait out abort/exit windows first.
        wait_until_slot_inactive(&slot).map_err(|error| format!("wait slot inactive: {error}"))?;
        // Void-returning drop must still produce a readable SPI row.
        let _ = pgrx::Spi::get_one_with_args::<bool>(
            "SELECT (pg_catalog.pg_drop_replication_slot($1) IS NULL)",
            &[DatumWithOid::from(slot.as_str())],
        )
        .map_err(|error| format!("drop slot: {error}"))?;
    }
    if publication_exists {
        pgrx::Spi::run(&format!(
            "DROP PUBLICATION IF EXISTS {}",
            quote_ident(PUBLICATION_NAME)
        ))
        .map_err(|error| format!("drop publication: {error}"))?;
    }
    // DELETE may affect zero rows; SELECT-wrap keeps SPI positioning safe.
    let _ = pgrx::Spi::get_one_with_args::<i64>(
        "WITH deleted AS (\
           DELETE FROM koldstore.async_mirror_state WHERE database_oid = $1 RETURNING 1\
         ) SELECT count(*)::bigint FROM deleted",
        &[DatumWithOid::from(pgrx::pg_sys::Oid::from(database_oid))],
    )
    .map_err(|error| format!("clear async_mirror_state: {error}"))?;
    crate::database_worker::mark_worker_not_ensured();
    Ok(slot_exists || publication_exists)
}
