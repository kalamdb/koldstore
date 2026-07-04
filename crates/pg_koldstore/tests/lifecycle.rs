use pg_koldstore::{
    flush, guc, hooks, koldstore_version, memory, migrate, observability, security, spi, sql,
};

#[test]
fn extension_version_and_session_functions_are_non_empty_and_monotonic() {
    assert!(!koldstore_version().is_empty());

    let first = sql::session::snowflake_id();
    let second = sql::session::snowflake_id();
    assert!(second > first);
}

#[test]
fn guc_definitions_include_public_and_internal_settings() {
    let gucs = guc::definitions();

    assert!(gucs
        .iter()
        .any(|guc| guc.name == "koldstore.user_id" && !guc.internal));
    assert!(gucs
        .iter()
        .any(|guc| guc.name == "koldstore.internal_system_write" && guc.internal));
    assert!(gucs
        .iter()
        .any(|guc| guc.name == "koldstore.internal_flush_cleanup" && guc.internal));
}

#[test]
fn application_roles_cannot_set_internal_gucs() {
    assert!(security::privileges::can_set_guc(
        security::privileges::RoleClass::Application,
        "koldstore.user_id",
    ));
    assert!(!security::privileges::can_set_guc(
        security::privileges::RoleClass::Application,
        "koldstore.internal_system_write",
    ));
    assert!(security::privileges::can_set_guc(
        security::privileges::RoleClass::Superuser,
        "koldstore.internal_system_write",
    ));
}

#[test]
fn commit_sequence_allocator_is_monotonic_for_test_shell() {
    use koldstore_core::ScopeKey;

    let first = hooks::xact::allocate_commit_seq_for_tests().unwrap();
    let second = hooks::xact::allocate_commit_seq_for_tests().unwrap();
    assert!(second > first);

    let domain = hooks::xact::CommitSequenceDomain::for_table_scope(
        42,
        Some(ScopeKey::new("tenant-a").unwrap()),
    );
    assert_eq!(domain.name(), "table:42:scope:tenant-a");
    assert_eq!(
        domain.advisory_lock_key(),
        hooks::xact::CommitSequenceDomain::for_table_scope(
            42,
            Some(ScopeKey::new("tenant-a").unwrap())
        )
        .advisory_lock_key()
    );
    assert_ne!(
        domain.advisory_lock_key(),
        hooks::xact::CommitSequenceDomain::for_table_scope(
            42,
            Some(ScopeKey::new("tenant-b").unwrap())
        )
        .advisory_lock_key()
    );

    let allocator = hooks::xact::CommitSequenceAllocator::new_for_tests();
    let first_tx = allocator.allocate_for_domain(&domain).unwrap();
    let second_tx = allocator.allocate_for_domain(&domain).unwrap();
    assert!(second_tx.commit_seq > first_tx.commit_seq);
    assert_eq!(first_tx.lock_key, domain.advisory_lock_key());
    assert_eq!(allocator.domain(), domain.name());
}

#[test]
fn hook_shell_exposes_required_hook_names() {
    let hooks = hooks::registered_hook_names();
    assert!(hooks.contains(&"set_rel_pathlist"));
    assert!(hooks.contains(&"ExecutorStart"));
    assert!(hooks.contains(&"ProcessUtility"));
    assert!(hooks.contains(&"XactCallback"));
}

#[test]
fn managed_column_and_migration_helpers_match_contract() {
    assert!(hooks::executor::is_system_column("_seq"));
    assert!(hooks::executor::is_system_column("_commit_seq"));
    assert!(hooks::executor::is_system_column("_deleted"));
    assert!(!hooks::executor::is_system_column("title"));

    assert!(migrate::constraints::primary_key_shape_supported(&["id"]));
    assert!(!migrate::constraints::primary_key_shape_supported(&[]));
    assert_eq!(
        migrate::columns::REQUIRED_SYSTEM_COLUMNS,
        ["_seq", "_commit_seq", "_deleted"]
    );
}

#[test]
fn spi_and_memory_boundaries_expose_diagnostics() {
    assert_eq!(spi::KOLDSTORE_SQLSTATE, "XXKLD");
    assert_eq!(
        spi::map_spi_error("select", "permission denied").to_string(),
        "SPI select failed: permission denied"
    );

    let select =
        spi::SpiStatement::read("read active schema", "SELECT * FROM koldstore.schemas").unwrap();
    assert_eq!(select.access, spi::SpiAccess::ReadOnly);
    let insert = spi::SpiStatement::write(
        "insert row event",
        "INSERT INTO koldstore.row_events DEFAULT VALUES",
    )
    .unwrap();
    assert_eq!(insert.access, spi::SpiAccess::ReadWrite);
    assert!(spi::SpiStatement::read("blank", "  ").is_err());

    let executor = spi::RecordingSpiExecutor::default();
    let rows = spi::execute_catalog_write(&executor, insert).unwrap();
    assert_eq!(rows.rows_affected, 1);
    assert_eq!(executor.statements()[0].operation, "insert row event");

    let mut owner = memory::MemoryOwner::new("scan_state");
    owner.track_allocation(1024);
    owner.track_allocation(512);
    assert_eq!(owner.allocated_bytes(), 1536);
    owner.reset();
    assert_eq!(owner.allocated_bytes(), 0);

    assert!(memory::MEMORY_OWNER_LABELS.contains(&"scan_state"));
    assert!(memory::MEMORY_OWNER_LABELS.contains(&"object_store_handle"));
    assert!(observability::SPAN_NAMES.contains(&"koldstore.merge_execute"));

    let sql_span = observability::KoldstoreSpan::SqlApi {
        function: "koldstore.migrate_table",
    };
    assert_eq!(sql_span.name(), "koldstore.sql_api");
    assert!(sql_span
        .fields()
        .contains(&("function", "koldstore.migrate_table")));

    let counter = observability::ObjectStoreIoCounter::default();
    counter.record_read("manifest");
    counter.record_write("parquet");
    assert_eq!(counter.reads(), 1);
    assert_eq!(counter.writes(), 1);
}

#[test]
fn sql_migration_creates_required_catalog_tables() {
    let sql = include_str!("../sql/koldstore--0.1.0.sql");

    for needle in [
        "CREATE TABLE IF NOT EXISTS koldstore.storage",
        "CREATE TABLE IF NOT EXISTS koldstore.schemas",
        "CREATE TABLE IF NOT EXISTS koldstore.manifest",
        "CREATE TABLE IF NOT EXISTS koldstore.jobs",
        "CREATE TABLE IF NOT EXISTS koldstore.cold_segments",
        "CREATE TABLE IF NOT EXISTS koldstore.cold_pk_hints",
        "CREATE TABLE IF NOT EXISTS koldstore.row_events",
        "CREATE UNIQUE INDEX IF NOT EXISTS schemas_one_active_per_table_idx",
        "CREATE INDEX IF NOT EXISTS manifest_dirty_idx",
        "CREATE INDEX IF NOT EXISTS manifest_scope_lookup_idx",
        "CREATE INDEX IF NOT EXISTS jobs_pending_idx",
        "CREATE INDEX IF NOT EXISTS cold_segments_active_scope_seq_idx",
        "CREATE INDEX IF NOT EXISTS cold_segments_active_commit_idx",
    ] {
        assert!(sql.contains(needle), "missing SQL fragment: {needle}");
    }

    assert!(
        !sql.contains("CREATE SCHEMA IF NOT EXISTS system"),
        "extension catalog should use a single extension-owned schema"
    );
    assert!(
        !sql.contains("system."),
        "extension SQL should not create or reference the legacy system schema"
    );
    for duplicate_index in ["cold_pk_hints_lookup_idx", "row_events_commit_idx"] {
        assert!(
            !sql.contains(duplicate_index),
            "primary key prefix already covers lookup pattern: {duplicate_index}"
        );
    }
}

#[test]
fn sql_migration_keeps_behavior_in_rust_pgrx_modules() {
    let sql = include_str!("../sql/koldstore--0.1.0.sql");

    for forbidden in [
        "CREATE OR REPLACE FUNCTION",
        "LANGUAGE plpgsql",
        "LANGUAGE sql",
        "CREATE TRIGGER",
        "EXECUTE format",
        "TO PROGRAM",
    ] {
        assert!(
            !sql.contains(forbidden),
            "extension SQL should not contain executable behavior: {forbidden}"
        );
    }
}

#[test]
fn operation_boundaries_document_safe_defaults() {
    assert!(flush::worker::requires_shared_preload());
    assert!(flush::cleanup::cleanup_allowed(true));
    assert!(!flush::cleanup::cleanup_allowed(false));
    assert_eq!(sql::events::DEFAULT_CHANGE_LIMIT, 1000);
}
