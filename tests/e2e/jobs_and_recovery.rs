#[path = "common/mod.rs"]
mod common;

use anyhow::Result;

#[test]
fn jobs_and_recovery_contract_covers_status_retries_and_idempotence() {
    let recovery = pg_koldstore::sql::ops::recover_segments_plan(
        Some(koldstore_core::TableName::parse("app.items").unwrap()),
        false,
    )
    .unwrap();

    assert!(recovery.statement.sql.contains("koldstore.jobs"));
    assert!(recovery.statement.sql.contains("recover_segments"));
    assert!(recovery.statement.sql.contains("attempts"));
    assert!(recovery.statement.sql.contains("dry_run"));
}

#[tokio::test]
async fn jobs_are_durable_idempotent_and_use_claim_indexes_on_pgrx() -> Result<()> {
    for target in common::local_pg_matrix() {
        let db = common::TestDb::start(target, "jobs_and_recovery").await?;
        let table = db.create_indexed_items_table("job_items", 16).await?;
        db.migrate_shared(&table.relation, "id").await?;

        let first_insert = db.insert_pending_flush_job(&table.relation, "").await?;
        let duplicate_insert = db.insert_pending_flush_job(&table.relation, "").await?;
        assert_eq!(first_insert, 1);
        assert_eq!(duplicate_insert, 0);
        assert_eq!(
            common::active_job_count(&db.client, &table.relation).await?,
            1
        );

        let pending_plan = common::explain_with_seqscan_disabled(
            &db.client,
            "SELECT id FROM koldstore.jobs WHERE table_oid = 'pg_catalog.pg_class'::regclass::oid AND scope_key = '' AND status IN ('pending', 'running') ORDER BY updated_at",
        )
        .await?;
        common::assertions::assert_catalog_index_plan(&pending_plan, "jobs_pending_idx")?;

        let claim_plan = common::explain_with_seqscan_disabled(
            &db.client,
            r#"
            SELECT id
            FROM koldstore.jobs
            WHERE job_type = 'flush'
              AND status IN ('pending', 'running')
              AND run_after <= now()
              AND (status = 'pending' OR lease_expires_at IS NULL OR lease_expires_at < now())
            ORDER BY priority DESC, updated_at, id
            LIMIT 1
            "#,
        )
        .await?;
        assert!(
            claim_plan.contains("jobs_claimable_by_type_idx")
                || claim_plan.contains("jobs_one_active_flush_per_scope_idx"),
            "expected an efficient flush-job index, got:\n{claim_plan}"
        );
        assert!(
            claim_plan.contains("Index Scan") || claim_plan.contains("Index Only Scan"),
            "expected an index-backed claim plan, got:\n{claim_plan}"
        );

        let recovered = db
            .client
            .query_one(
                "SELECT koldstore.recover_segments($1::text::regclass, true)",
                &[&table.relation],
            )
            .await?;
        assert_eq!(recovered.get::<_, i64>(0), 1);

        let recovery_state = db
            .client
            .query_one(
                r#"
                SELECT status, attempts, error_trace
                FROM koldstore.jobs
                WHERE table_oid = $1::text::regclass::oid
                  AND job_type = 'recover_segments'
                ORDER BY created_at DESC
                LIMIT 1
                "#,
                &[&table.relation],
            )
            .await?;
        assert_eq!(recovery_state.get::<_, String>(0), "dry_run");
        assert_eq!(recovery_state.get::<_, i32>(1), 0);
        assert_eq!(recovery_state.get::<_, Option<String>>(2), None);
    }

    Ok(())
}

#[tokio::test]
async fn extension_catalog_dml_is_blocked_but_storage_api_is_allowed_on_pgrx() -> Result<()> {
    for target in common::local_pg_matrix() {
        let db = common::TestDb::start(target, "catalog_dml_blocked").await?;
        let app_role = format!("{}_app", db.schema);
        db.client
            .batch_execute(&format!(
                "DROP ROLE IF EXISTS {app_role}; CREATE ROLE {app_role};"
            ))
            .await?;

        db.client
            .batch_execute(&format!("SET ROLE {app_role};"))
            .await?;
        let direct_insert = db
            .client
            .execute(
                r#"
                INSERT INTO koldstore.storage (
                  id, name, storage_type, base_path, credentials, config
                )
                VALUES (
                  gen_random_uuid(), 'blocked', 'filesystem', '/tmp/blocked',
                  '{}'::jsonb, '{}'::jsonb
                )
                "#,
                &[],
            )
            .await;
        assert!(direct_insert.is_err());

        let api_insert = db
            .client
            .query_one(
                r#"
                SELECT koldstore.register_storage(
                  'api_allowed',
                  'filesystem',
                  '/tmp/api-allowed',
                  '{}'::jsonb,
                  '{}'::jsonb
                )
                "#,
                &[],
            )
            .await;
        db.client.batch_execute("RESET ROLE").await?;
        assert!(api_insert.is_ok());
    }

    Ok(())
}
