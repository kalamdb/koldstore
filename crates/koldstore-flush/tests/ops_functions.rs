#[test]
fn sql_exposes_operational_functions() {
    for status_field in [
        "hot_rows",
        "cold_segment_count",
        "manifest_state",
        "pending_jobs",
        "jobs",
        "storage_binding",
        "last_error",
    ] {
        assert!(
            koldstore_flush::ops::TABLE_STATUS_FIELDS.contains(&status_field),
            "missing {status_field}"
        );
    }

    let validation = koldstore_flush::ops::ValidationSummary {
        manifests_checked: 1,
        segments_checked: 2,
        catalog_consistent: true,
    };
    assert!(validation.catalog_consistent);

    for function in [
        "koldstore.enqueue_flush_job",
        "koldstore.flush_table",
        "koldstore.recover_segments",
        "koldstore.table_status",
        "koldstore.manage_table",
        "koldstore.unmanage_table",
    ] {
        assert!(
            koldstore_flush::ops::FLUSH_SQL_FUNCTIONS.contains(&function),
            "missing SQL function boundary {function}"
        );
    }
}

#[test]
fn operational_functions_build_parameterized_catalog_plans() {
    use koldstore_common::SqlAccess as SpiAccess;
    use koldstore_common::{QualifiedTableName, ScopeKey, TableName};

    let table = TableName::parse("app.items").unwrap();
    let qualified = QualifiedTableName::parse("app.items").unwrap();
    let mirror = QualifiedTableName::parse("koldstore.items__cl").unwrap();
    let status = koldstore_flush::ops::table_status_plan(&qualified, &mirror).unwrap();
    assert_eq!(status.table_name.as_str(), "app.items");
    assert!(status.statement.sql.contains("jsonb_build_object"));
    assert!(status.statement.sql.contains("'hot_rows'"));
    assert!(
        status.statement.sql.contains("NULLIF(m.hot_row_count, 0)"),
        "hot_rows should fall back to ONLY heap count when the counter is still 0"
    );
    assert!(status.statement.sql.contains("'mirror_rows'"));
    assert!(status.statement.sql.contains("'cold_row_count'"));
    assert!(
        status.statement.sql.contains("'duration_ms'"),
        "table_status jobs should expose duration_ms"
    );
    assert!(status.statement.sql.contains("\"app\".\"items\""));
    assert!(status.statement.sql.contains("\"koldstore\".\"items__cl\""));
    assert_eq!(status.statement.access, SpiAccess::ReadOnly);

    let backup = koldstore_flush::ops::backup_manifest_plan(
        Some(table.clone()),
        Some(ScopeKey::new("tenant-a").unwrap()),
    )
    .unwrap();
    assert!(backup.statement.sql.contains("SELECT etag"));
    assert_eq!(backup.scope_key.unwrap().as_str(), "tenant-a");

    let validation = koldstore_flush::ops::validate_cold_storage_plan(Some(table.clone())).unwrap();
    assert!(validation.statement.sql.contains("koldstore.cold_segments"));
    assert!(validation
        .statement
        .sql
        .contains("cs.scope_key = m.scope_key"));
    assert!(validation.statement.sql.contains("cs.status = 'active'"));
    assert!(!validation.statement.sql.contains("cs.column_stats"));
    assert!(validation.statement.sql.contains("cs.path"));
    assert!(!validation.statement.sql.contains("cold_pk_hints"));

    let recovery = koldstore_flush::ops::recover_segments_plan(Some(table), false).unwrap();
    assert!(!recovery.request.dry_run);
    assert!(recovery
        .request
        .table_name
        .as_ref()
        .is_some_and(|name| name.to_string() == "app.items"));
}

#[test]
fn sql_exposes_export_import_boundary() {
    use koldstore_common::TableName;

    let export = koldstore_flush::ops::plan_koldstore_exec("EXPORT TABLE app.items").unwrap();
    assert_eq!(
        export.command,
        koldstore_flush::ops::OpsCommand::ExportTable {
            table_name: TableName::parse("app.items").unwrap()
        }
    );
    assert!(export.statement.sql.contains("koldstore.manifest"));
    assert!(export.statement.sql.contains("cs.scope_key = m.scope_key"));
    assert!(export.statement.sql.contains("cs.status = 'active'"));
    assert!(export.archive_manifest_path.ends_with("manifest.json"));
    assert_eq!(export.archive_manifest_path, "app/items/manifest.json");

    assert_eq!(
        koldstore_flush::ops::classify_command("IMPORT TABLE app.items"),
        Some(koldstore_flush::ops::OpsCommand::ImportTable {
            table_name: TableName::parse("app.items").unwrap()
        })
    );
    assert_eq!(
        koldstore_flush::ops::plan_koldstore_exec("IMPORT TABLE app.items")
            .unwrap_err()
            .to_string(),
        "IMPORT TABLE is not supported in this MVP"
    );
    assert_eq!(
        koldstore_flush::ops::classify_command("DROP TABLE app.items"),
        None
    );
}

#[test]
fn flush_sql_requests_capture_table_scope_and_enqueue_metadata() {
    use koldstore_common::{ScopeKey, SeqId, TableName};

    let table_flush = koldstore_flush::ops::flush_table_request(
        TableName::parse("app.items").unwrap(),
        Some(ScopeKey::new("tenant-a").unwrap()),
        true,
    );

    assert_eq!(table_flush.scope_key.as_ref().unwrap().as_str(), "tenant-a");
    assert!(table_flush.force);

    let enqueue = koldstore_flush::ops::plan_enqueue_or_lookup_flush_job(
        table_flush,
        Some(SeqId::new(1_000).unwrap()),
    )
    .unwrap();
    assert_eq!(enqueue.seq_upper_bound.unwrap().get(), 1_000);
    assert!(enqueue.statement.sql.contains("flush_seq_upper_bound"));
    assert!(enqueue.statement.sql.contains("ON CONFLICT"));
    assert!(enqueue.statement.sql.contains("DO UPDATE SET"));
    assert!(enqueue.statement.sql.contains("RETURNING id"));
    assert!(enqueue
        .statement
        .sql
        .contains("WHERE job_type = 'flush' AND status IN ('pending', 'running')"));

    let lookup = koldstore_flush::ops::plan_enqueue_or_lookup_flush_job(
        koldstore_flush::ops::flush_table_request(
            TableName::parse("app.items").unwrap(),
            None,
            false,
        ),
        None,
    )
    .unwrap();
    assert_eq!(lookup.statement.operation, "enqueue or lookup flush job");

    let pending = koldstore_flush::ops::plan_select_pending_flush_candidates().unwrap();
    assert!(pending.sql.contains("status = 'pending'"));
    assert!(pending.sql.contains("'force'"));
    assert!(pending.sql.contains("available_at, updated_at, id"));
    assert!(pending.sql.contains("LIMIT $1"));

    let pending_after = koldstore_flush::ops::plan_select_pending_flush_candidates_after().unwrap();
    assert!(pending_after
        .sql
        .contains("(available_at, updated_at, id) > ($2, $3, $4)"));
    assert_eq!(
        pending_after.param_types,
        vec![
            koldstore_common::SqlParamType::BigInt,
            koldstore_common::SqlParamType::TimestampWithTimeZone,
            koldstore_common::SqlParamType::TimestampWithTimeZone,
            koldstore_common::SqlParamType::Uuid,
        ]
    );

    let next_due = koldstore_flush::ops::plan_next_pending_flush_due_epoch_ms().unwrap();
    assert!(next_due.sql.contains("min(available_at)"));

    let count = koldstore_flush::ops::plan_count_pending_flush_jobs().unwrap();
    assert!(count.sql.contains("count(*)::bigint"));
}
