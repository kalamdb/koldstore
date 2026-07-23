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
        "koldstore.describe_table",
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
    let status = koldstore_flush::ops::describe_table_plan(&qualified, &mirror).unwrap();
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
        "describe_table jobs should expose duration_ms"
    );
    assert!(status.statement.sql.contains("\"app\".\"items\""));
    assert!(status.statement.sql.contains("\"koldstore\".\"items__cl\""));
    assert_eq!(status.statement.access, SpiAccess::ReadOnly);

    let backup = koldstore_flush::ops::backup_manifest_plan(
        Some(table.clone()),
        Some(ScopeKey::new("tenant-a").unwrap()),
    )
    .unwrap();
    assert!(backup.statement.sql.contains("SELECT manifest_path"));
    assert_eq!(backup.scope_key.unwrap().as_str(), "tenant-a");

    let validation = koldstore_flush::ops::validate_cold_storage_plan(Some(table.clone())).unwrap();
    assert!(validation.statement.sql.contains("koldstore.cold_segments"));
    assert!(validation
        .statement
        .sql
        .contains("cs.scope_key = m.scope_key"));
    assert!(validation.statement.sql.contains("cs.status = 'active'"));
    assert!(validation.statement.sql.contains("cs.column_stats"));
    assert!(!validation.statement.sql.contains("cold_pk_hints"));

    let recovery = koldstore_flush::ops::recover_segments_plan(Some(table), false).unwrap();
    assert!(!recovery.request.dry_run);
    assert!(recovery.statement.sql.contains("koldstore.jobs"));
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

    let enqueue =
        koldstore_flush::ops::enqueue_flush_job_plan(table_flush, Some(SeqId::new(1_000).unwrap()))
            .unwrap();
    assert_eq!(enqueue.seq_upper_bound.unwrap().get(), 1_000);
    assert!(enqueue.statement.sql.contains("flush_seq_upper_bound"));
    assert!(enqueue.statement.sql.contains("ON CONFLICT"));
    assert!(enqueue
        .statement
        .sql
        .contains("WHERE job_type = 'flush' AND status IN ('pending', 'running')"));
}
