use koldstore::{catalog, guc, hooks, koldstore_version, memory, observability, spi, sql};
use koldstore_common::{can_set_guc, RoleClass};

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
    assert!(gucs.iter().any(|guc| guc.name == "koldstore.cold_reads"
        && !guc.internal
        && guc.default_value == "auto"));
    assert!(gucs
        .iter()
        .any(|guc| guc.name == "koldstore.max_open_parquet_readers"
            && !guc.internal
            && guc.default_value == "32"));
    assert!(gucs.iter().any(|guc| guc.name == "koldstore.log_level"
        && !guc.internal
        && guc.default_value == "info"));
    assert!(gucs
        .iter()
        .any(|guc| guc.name == "koldstore.min_max_rows_per_file"
            && !guc.internal
            && guc.default_value == "1000"));
    assert!(gucs
        .iter()
        .any(|guc| guc.name == "koldstore.flush_check_interval_seconds"
            && !guc.internal
            && guc.default_value == "30"));
    assert!(gucs
        .iter()
        .any(|guc| guc.name == "koldstore.async_apply_poll_interval_ms"
            && !guc.internal
            && guc.default_value == "100"));
    assert!(gucs
        .iter()
        .any(|guc| guc.name == "koldstore.async_apply_max_rows_per_tick"
            && !guc.internal
            && guc.default_value == "0"));
    assert!(gucs
        .iter()
        .any(|guc| guc.name == "koldstore.async_apply_max_ms_per_tick"
            && !guc.internal
            && guc.default_value == "0"));
    assert!(gucs
        .iter()
        .any(|guc| guc.name == "koldstore.flush_prelock_max_passes"
            && !guc.internal
            && guc.default_value == "3"));
    assert!(gucs
        .iter()
        .any(|guc| guc.name == "koldstore.flush_prelock_max_ms"
            && !guc.internal
            && guc.default_value == "5000"));
    assert!(gucs.iter().any(
        |guc| guc.name == "koldstore.async_mirror_max_retained_bytes"
            && !guc.internal
            && guc.default_value == "1073741824"
    ));
    assert!(gucs
        .iter()
        .any(|guc| guc.name == "koldstore.internal_system_write" && guc.internal));
    assert!(gucs
        .iter()
        .any(|guc| guc.name == "koldstore.internal_flush_cleanup" && guc.internal));
    assert!(gucs.iter().any(|guc| guc.name == "koldstore.failpoint"
        && !guc.internal
        && guc.default_value.is_empty()));
}

#[test]
fn application_roles_cannot_set_internal_gucs() {
    assert!(can_set_guc(RoleClass::Application, "koldstore.user_id"));
    assert!(can_set_guc(
        RoleClass::Application,
        "koldstore.max_open_parquet_readers",
    ));
    assert!(can_set_guc(
        RoleClass::Application,
        "koldstore.min_max_rows_per_file",
    ));
    assert!(!can_set_guc(
        RoleClass::Application,
        "koldstore.internal_system_write",
    ));
    assert!(can_set_guc(
        RoleClass::Superuser,
        "koldstore.internal_system_write",
    ));
}

#[test]
fn commit_sequence_allocator_is_monotonic_for_test_shell() {
    use koldstore_common::ScopeKey;

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
    assert!(hooks.contains(&"ProcessUtility"));
    assert!(hooks.contains(&"XactCallback"));
    assert!(hooks.contains(&"RelcacheCallback"));
    assert!(!hooks.contains(&"ExecutorStart"));
}

#[test]
fn migration_helpers_match_contract() {
    assert!(koldstore_migrate::constraints::primary_key_shape_supported(
        &["id"]
    ));
    assert!(!koldstore_migrate::constraints::primary_key_shape_supported(&[]));
}

#[test]
fn spi_and_memory_boundaries_expose_diagnostics() {
    assert_eq!(spi::KOLDSTORE_SQLSTATE, "XXKLD");
    assert_eq!(
        spi::map_spi_error("select", "permission denied").to_string(),
        "SQL select failed: permission denied"
    );

    let select =
        spi::SpiStatement::read("read active schema", "SELECT * FROM koldstore.schemas").unwrap();
    assert_eq!(select.access, spi::SpiAccess::ReadOnly);
    assert!(select.param_types.is_empty());
    assert!(spi::require_read_only(&select).is_ok());
    assert!(spi::require_read_write(&select).is_err());
    let insert = spi::SpiStatement::write(
        "insert change-log mirror row",
        "INSERT INTO koldstore.items__cl DEFAULT VALUES",
    )
    .unwrap();
    assert_eq!(insert.access, spi::SpiAccess::ReadWrite);
    assert!(spi::require_read_write(&insert).is_ok());
    assert!(spi::require_read_only(&insert).is_err());
    assert!(spi::SpiStatement::read("blank", "  ").is_err());

    let read_with_param = spi::SpiStatement::read_with_params(
        "read one",
        "SELECT $1::bigint",
        [spi::SqlParamType::BigInt],
    )
    .unwrap();
    let same_read = spi::SpiStatement::read_with_params(
        "read again",
        "SELECT $1::bigint",
        [spi::SqlParamType::BigInt],
    )
    .unwrap();
    let write_with_param = spi::SpiStatement::write_with_params(
        "write one",
        "SELECT $1::bigint",
        [spi::SqlParamType::BigInt],
    )
    .unwrap();
    let different_param = spi::SpiStatement::read_with_params(
        "read text",
        "SELECT $1::bigint",
        [spi::SqlParamType::Text],
    )
    .unwrap();
    assert_eq!(
        spi::prepared_plan_key(&read_with_param),
        spi::prepared_plan_key(&same_read)
    );
    assert_ne!(
        spi::prepared_plan_key(&read_with_param),
        spi::prepared_plan_key(&write_with_param)
    );
    assert_ne!(
        spi::prepared_plan_key(&read_with_param),
        spi::prepared_plan_key(&different_param)
    );

    let executor = spi::RecordingSpiExecutor::default();
    let rows = spi::execute_catalog_write(&executor, insert).unwrap();
    assert_eq!(rows.rows_affected, 1);
    assert_eq!(
        executor.statements()[0].operation,
        "insert change-log mirror row"
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

    let sql_span = observability::KoldstoreSpan::SqlApi {
        function: "koldstore.manage_table",
    };
    assert_eq!(sql_span.name(), "koldstore.sql_api");
    assert!(sql_span
        .fields()
        .contains(&("function", "koldstore.manage_table")));

    let counter = observability::ObjectStoreIoCounter::default();
    counter.record_read("manifest");
    counter.record_write("parquet");
    assert_eq!(counter.reads(), 1);
    assert_eq!(counter.writes(), 1);
}

#[test]
fn catalog_helpers_build_queries_and_decode_contexts() {
    let mirror_lookup = catalog::queries::plan_mirror_relation_by_table_oid().unwrap();
    assert_eq!(mirror_lookup.access, spi::SpiAccess::ReadOnly);
    assert_eq!(mirror_lookup.param_types, vec![spi::SqlParamType::Oid]);
    assert!(mirror_lookup.sql.contains("koldstore.schemas"));
    assert!(mirror_lookup.sql.contains("s.mirror_relation"));

    let mirror_statement = koldstore_mirror::MirrorStatement::read_with_params(
        "mirror scan",
        "SELECT * FROM koldstore.items__cl WHERE seq > $1::bigint",
        [koldstore_mirror::SqlParamType::BigInt],
    );
    let spi_statement = koldstore_mirror::mirror_to_sql(mirror_statement).unwrap();
    assert_eq!(spi_statement.param_types, vec![spi::SqlParamType::BigInt]);

    let relation = catalog::decode::relation_context(&serde_json::json!({
        "namespace": "public",
        "name": "items"
    }))
    .unwrap();
    assert_eq!(relation.namespace, "public");
    assert_eq!(relation.name, "items");

    let storage = catalog::decode::flush_storage_context(&serde_json::json!({
        "base_path": "s3://bucket/prefix",
        "schema_version": 7,
        "compression": "zstd"
    }))
    .unwrap();
    assert_eq!(storage.base_path, "s3://bucket/prefix");
    assert_eq!(storage.schema_version, 7);
    assert_eq!(storage.compression, "zstd");

    let missing = catalog::decode::relation_context(&serde_json::json!({"namespace": "public"}));
    assert!(missing.unwrap_err().contains("missing string field `name`"));

    let snapshot = koldstore_catalog::decode_managed_table_snapshot(&serde_json::json!({
        "table_oid": 42,
        "schema_version": 3,
        "active": true,
        "initialization_state": "complete",
        "mirror_relation": "koldstore.items__cl",
        "primary_key": ["id"],
        "primary_key_shape": [{"column": "id", "type_oid": 20}],
        "scope_column": null
    }))
    .unwrap();
    assert_eq!(snapshot.table_oid, 42);
    assert_eq!(snapshot.schema_version, 3);
    assert!(snapshot.active);
    assert_eq!(
        snapshot.initialization_state,
        koldstore_schema::MirrorInitializationState::Complete
    );
    assert_eq!(snapshot.mirror_relation.relation(), "items__cl");
    assert_eq!(snapshot.primary_key_columns, vec!["id".to_string()]);
    assert!(snapshot.scope_column.is_none());
}

#[test]
fn operation_boundaries_document_safe_defaults() {
    assert!(koldstore_flush::worker::requires_shared_preload());
    assert!(koldstore_flush::cleanup::cleanup_allowed(true));
    assert!(!koldstore_flush::cleanup::cleanup_allowed(false));
    assert_eq!(koldstore_merge::events::DEFAULT_CHANGE_LIMIT, 1000);
}
