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
    let first = hooks::xact::allocate_commit_seq_for_tests().unwrap();
    let second = hooks::xact::allocate_commit_seq_for_tests().unwrap();
    assert!(second > first);

    let allocator = hooks::xact::CommitSequenceAllocator::new_for_tests();
    let first_tx = allocator.allocate_for_domain("global").unwrap();
    let second_tx = allocator.allocate_for_domain("global").unwrap();
    assert!(second_tx > first_tx);
    assert_eq!(allocator.domain(), "global");
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

    let mut owner = memory::MemoryOwner::new("scan_state");
    owner.track_allocation(1024);
    owner.track_allocation(512);
    assert_eq!(owner.allocated_bytes(), 1536);
    owner.reset();
    assert_eq!(owner.allocated_bytes(), 0);

    assert!(memory::MEMORY_OWNER_LABELS.contains(&"scan_state"));
    assert!(memory::MEMORY_OWNER_LABELS.contains(&"object_store_handle"));
    assert!(observability::SPAN_NAMES.contains(&"koldstore.merge_execute"));
}

#[test]
fn sql_migration_creates_required_catalog_tables() {
    let sql = include_str!("../../../sql/koldstore--0.1.0.sql");

    for needle in [
        "CREATE SCHEMA IF NOT EXISTS koldstore",
        "CREATE TABLE IF NOT EXISTS koldstore.storage",
        "CREATE TABLE IF NOT EXISTS system.schemas",
        "CREATE TABLE IF NOT EXISTS koldstore.manifest",
        "CREATE TABLE IF NOT EXISTS system.jobs",
        "CREATE TABLE IF NOT EXISTS koldstore.cold_segments",
        "CREATE TABLE IF NOT EXISTS koldstore.cold_pk_hints",
        "CREATE TABLE IF NOT EXISTS koldstore.row_events",
    ] {
        assert!(sql.contains(needle), "missing SQL fragment: {needle}");
    }
}

#[test]
fn operation_boundaries_document_safe_defaults() {
    assert!(flush::worker::requires_shared_preload());
    assert!(flush::cleanup::cleanup_allowed(true));
    assert!(!flush::cleanup::cleanup_allowed(false));
    assert_eq!(sql::events::DEFAULT_CHANGE_LIMIT, 1000);
}
