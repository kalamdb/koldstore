//! Logical-slot and publication lifecycle for asynchronous mirror capture.
//!
//! A database owns one slot and one publication. Tables enter the publication
//! only after strict capture has initialized their mirror, which prevents a gap
//! while switching the write path from triggers to committed WAL.

use koldstore_common::{quote_ident, MirrorCaptureMode, QualifiedTableName};
use pgrx::datum::DatumWithOid;

const APPLY_LOCK_NAMESPACE: i32 = 1_263_354_732;
const WORKER_LOCK_NAMESPACE: i32 = 1_263_354_733;
const SLOT_PROVISION_LOCK_NAMESPACE: i32 = 1_263_354_734;
const LIFECYCLE_LOCK_NAMESPACE: i32 = 1_263_354_735;

/// Publication shared by async managed tables in one database.
pub const PUBLICATION_NAME: &str = "koldstore_async_mirror";

/// Returns the cluster-unique logical slot name for a database OID.
#[must_use]
pub(super) fn slot_name(database_oid: u32) -> String {
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
/// cannot provision the publication, or the deterministic slot name is incompatible.
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
    let needs_provisioning = if native_slot_exists(slot) {
        !publication_exists()?
    } else {
        true
    };
    if needs_provisioning {
        super::worker::provision_infrastructure(database_oid)?;
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

fn publication_exists() -> Result<bool, String> {
    pgrx::Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = $1)",
        &[DatumWithOid::from(PUBLICATION_NAME)],
    )
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "publication existence query returned no row".to_string())
}

pub(super) fn native_slot_exists(slot: &str) -> bool {
    let slot = std::ffi::CString::new(slot).expect("deterministic slot name contains no NUL");
    native_slot_exists_cstr(&slot)
}

pub(super) fn native_slot_exists_cstr(slot: &std::ffi::CStr) -> bool {
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

    let drop = koldstore_migrate::capture::plan_drop_mirror_dml_triggers(source, mirror)
        .map_err(|error| error.to_string())?;
    pgrx::Spi::run(&drop.sql).map_err(|error| error.to_string())?;
    let kick_name = koldstore_migrate::capture::async_worker_kick_trigger_name(&mirror.name);
    pgrx::Spi::run(&format!(
        "DROP TRIGGER IF EXISTS {} ON {}",
        quote_ident(&kick_name),
        source.quoted(),
    ))
    .map_err(|error| error.to_string())?;
    pgrx::Spi::run(&format!(
        "CREATE TRIGGER {} AFTER INSERT OR UPDATE OR DELETE ON {} \
         FOR EACH STATEMENT EXECUTE FUNCTION koldstore.async_mirror_kick()",
        quote_ident(&kick_name),
        source.quoted(),
    ))
    .map_err(|error| error.to_string())?;
    super::worker::ensure_applier()?;
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
pub(super) fn lock_apply(database_oid: u32) -> Result<(), String> {
    lock_database(APPLY_LOCK_NAMESPACE, database_oid)
}

/// Serializes dynamic worker discovery and registration for one database.
pub(super) fn lock_worker_registration(database_oid: u32) -> Result<(), String> {
    lock_database(WORKER_LOCK_NAMESPACE, database_oid)
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
    lock_apply(database_oid)?;
    let slot = slot_name(database_oid);
    let slot_exists = pgrx::Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = $1)",
        &[DatumWithOid::from(slot.as_str())],
    )
    .map_err(|error| error.to_string())?
    .unwrap_or(false);
    let publication_exists = pgrx::Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = $1)",
        &[DatumWithOid::from(PUBLICATION_NAME)],
    )
    .map_err(|error| error.to_string())?
    .unwrap_or(false);
    if slot_exists {
        pgrx::Spi::run_with_args(
            "SELECT pg_catalog.pg_drop_replication_slot($1)",
            &[DatumWithOid::from(slot.as_str())],
        )
        .map_err(|error| error.to_string())?;
    }
    if publication_exists {
        pgrx::Spi::run(&format!(
            "DROP PUBLICATION IF EXISTS {}",
            quote_ident(PUBLICATION_NAME)
        ))
        .map_err(|error| error.to_string())?;
    }
    pgrx::Spi::run_with_args(
        "DELETE FROM koldstore.async_mirror_state WHERE database_oid = $1",
        &[DatumWithOid::from(pgrx::pg_sys::Oid::from(database_oid))],
    )
    .map_err(|error| error.to_string())?;
    super::worker::mark_applier_not_ensured();
    Ok(slot_exists || publication_exists)
}
