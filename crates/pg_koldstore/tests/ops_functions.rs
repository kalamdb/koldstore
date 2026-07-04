#[test]
fn sql_exposes_operational_functions() {
    for status_field in [
        "hot_rows",
        "cold_segment_count",
        "manifest_state",
        "pending_jobs",
        "storage_binding",
        "last_error",
    ] {
        assert!(
            pg_koldstore::sql::ops::TABLE_STATUS_FIELDS.contains(&status_field),
            "missing {status_field}"
        );
    }

    let validation = pg_koldstore::sql::ops::ValidationSummary {
        manifests_checked: 1,
        segments_checked: 2,
        catalog_consistent: true,
    };
    assert!(validation.catalog_consistent);

    for function in [
        "koldstore.set_flush_policy",
        "koldstore.flush_table",
        "koldstore.flush_pending",
    ] {
        assert!(
            pg_koldstore::sql::ops::FLUSH_SQL_FUNCTIONS.contains(&function),
            "missing SQL function boundary {function}"
        );
    }
}

#[test]
fn operational_functions_build_parameterized_catalog_plans() {
    use koldstore_core::{ScopeKey, TableName};
    use pg_koldstore::spi::SpiAccess;

    let table = TableName::parse("app.items").unwrap();
    let status = pg_koldstore::sql::ops::table_status_plan(table.clone(), None).unwrap();
    assert_eq!(status.table_name.as_str(), "app.items");
    assert!(status.statement.sql.contains("koldstore.manifest"));
    assert!(status.statement.sql.contains("j.scope_key = $2"));
    assert_eq!(status.statement.access, SpiAccess::ReadOnly);

    let backup = pg_koldstore::sql::ops::backup_manifest_plan(
        Some(table.clone()),
        Some(ScopeKey::new("tenant-a").unwrap()),
    )
    .unwrap();
    assert!(backup.statement.sql.contains("SELECT manifest_path"));
    assert_eq!(backup.scope_key.unwrap().as_str(), "tenant-a");

    let validation =
        pg_koldstore::sql::ops::validate_cold_storage_plan(Some(table.clone())).unwrap();
    assert!(validation.statement.sql.contains("koldstore.cold_segments"));
    assert!(validation
        .statement
        .sql
        .contains("cs.scope_key = m.scope_key"));
    assert!(validation.statement.sql.contains("cs.status = 'active'"));
    assert!(validation
        .statement
        .sql
        .contains("h.segment_id = cs.segment_id"));

    let recovery = pg_koldstore::sql::ops::recover_segments_plan(Some(table), false).unwrap();
    assert!(!recovery.request.dry_run);
    assert!(recovery.statement.sql.contains("koldstore.jobs"));
}

#[test]
fn flush_job_claim_plan_uses_skip_locked_leases_and_seq_watermark() {
    use pg_koldstore::flush::job::FlushLeaseSeconds;
    use pg_koldstore::spi::SpiAccess;

    let claim =
        pg_koldstore::sql::ops::claim_flush_jobs_plan(32, FlushLeaseSeconds::new(30).unwrap())
            .unwrap();

    assert_eq!(claim.limit, 32);
    assert_eq!(claim.lease_seconds.get(), 30);
    assert_eq!(claim.statement.access, SpiAccess::ReadWrite);
    assert!(claim.statement.sql.contains("FOR UPDATE SKIP LOCKED"));
    assert!(claim
        .statement
        .sql
        .contains("lease_epoch = j.lease_epoch + 1"));
    assert!(claim.statement.sql.contains("flush_seq_upper_bound"));
    assert!(claim.statement.sql.contains("status = 'running'"));
}

#[test]
fn flush_job_progress_and_finish_plans_are_guarded_by_live_lease() {
    use koldstore_core::{CommitSeq, SeqId};
    use pg_koldstore::flush::job::{FlushJobPhase, JobLeaseEpoch};
    use pg_koldstore::spi::SpiAccess;
    use uuid::Uuid;

    let job_id = Uuid::new_v4();
    let owner = Uuid::new_v4();
    let progress = pg_koldstore::sql::ops::flush_job_progress_plan(
        job_id,
        owner,
        JobLeaseEpoch::new(3).unwrap(),
        FlushJobPhase::CommitCatalog,
        SeqId::new(99).unwrap(),
        CommitSeq::new(199).unwrap(),
        2,
        500,
    )
    .unwrap();
    let finish = pg_koldstore::sql::ops::finish_flush_job_plan(
        job_id,
        owner,
        JobLeaseEpoch::new(3).unwrap(),
        true,
        None,
    )
    .unwrap();

    assert_eq!(progress.job_id, job_id);
    assert_eq!(progress.lease_owner, owner);
    assert_eq!(progress.lease_epoch.get(), 3);
    assert_eq!(progress.phase, FlushJobPhase::CommitCatalog);
    assert_eq!(progress.statement.access, SpiAccess::ReadWrite);
    assert!(progress.statement.sql.contains("lease_owner = $2::uuid"));
    assert!(progress.statement.sql.contains("lease_epoch = $3::bigint"));
    assert!(progress
        .statement
        .sql
        .contains("checkpoint_seq = $5::bigint"));
    assert!(progress.statement.sql.contains("last_heartbeat_at = now()"));

    assert_eq!(finish.job_id, job_id);
    assert!(finish.success);
    assert_eq!(finish.statement.access, SpiAccess::ReadWrite);
    assert!(finish
        .statement
        .sql
        .contains("status = CASE WHEN $4::boolean THEN 'completed' ELSE 'error' END"));
    assert!(finish.statement.sql.contains("lease_owner = $2::uuid"));
    assert!(finish.statement.sql.contains("lease_epoch = $3::bigint"));
    assert!(finish.statement.sql.contains("lease_owner = NULL"));
}

#[test]
fn sql_exposes_export_import_boundary() {
    use koldstore_core::TableName;

    let export = pg_koldstore::sql::ops::plan_koldstore_exec("EXPORT TABLE app.items").unwrap();
    assert_eq!(
        export.command,
        pg_koldstore::sql::ops::OpsCommand::ExportTable {
            table_name: TableName::parse("app.items").unwrap()
        }
    );
    assert!(export.statement.sql.contains("koldstore.manifest"));
    assert!(export.statement.sql.contains("cs.scope_key = m.scope_key"));
    assert!(export.statement.sql.contains("cs.status = 'active'"));
    assert!(export.archive_manifest_path.ends_with("manifest.json"));

    assert_eq!(
        pg_koldstore::sql::ops::classify_command("IMPORT TABLE app.items"),
        Some(pg_koldstore::sql::ops::OpsCommand::ImportTable {
            table_name: TableName::parse("app.items").unwrap()
        })
    );
    assert_eq!(
        pg_koldstore::sql::ops::plan_koldstore_exec("IMPORT TABLE app.items")
            .unwrap_err()
            .to_string(),
        "IMPORT TABLE is not supported in this MVP"
    );
    assert_eq!(
        pg_koldstore::sql::ops::classify_command("DROP TABLE app.items"),
        None
    );
}

#[test]
fn flush_sql_requests_capture_policy_table_scope_and_pending_limits() {
    use koldstore_core::{ScopeKey, SeqId, TableName};

    let policy = pg_koldstore::sql::ops::set_flush_policy_request(
        TableName::parse("app.items").unwrap(),
        Some("rows:1000,interval:60".to_string()),
    );
    let table_flush = pg_koldstore::sql::ops::flush_table_request(
        TableName::parse("app.items").unwrap(),
        Some(ScopeKey::new("tenant-a").unwrap()),
        true,
    );
    let pending_flush = pg_koldstore::sql::ops::flush_pending_request(25);

    assert_eq!(policy.table_name.as_str(), "app.items");
    assert_eq!(
        policy.flush_policy.as_deref(),
        Some("rows:1000,interval:60")
    );
    assert_eq!(table_flush.scope_key.as_ref().unwrap().as_str(), "tenant-a");
    assert!(table_flush.force);
    assert_eq!(pending_flush.limit, 25);

    let enqueue = pg_koldstore::sql::ops::enqueue_flush_job_plan(
        table_flush,
        Some(SeqId::new(1_000).unwrap()),
    )
    .unwrap();
    assert_eq!(enqueue.seq_upper_bound.unwrap().get(), 1_000);
    assert!(enqueue.statement.sql.contains("flush_seq_upper_bound"));
    assert!(enqueue.statement.sql.contains("ON CONFLICT"));
    assert!(enqueue
        .statement
        .sql
        .contains("WHERE job_type = 'flush' AND status IN ('pending', 'running')"));
}
