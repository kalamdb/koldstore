use crate::common;

use anyhow::Result;

#[tokio::test]
async fn alter_table_add_nullable_column_refreshes_schema_and_reads_old_cold_rows() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "schema_evolution_add").await?;
        let table = db.create_indexed_items_table("evolve_items", 12).await?;
        db.manage_shared(&table.relation, "id").await?;
        assert_eq!(db.flush_table(&table.relation).await?, 12);

        // Stop the async applier before ALTER/INSERT so those commits stay in WAL
        // until flush's fence applies them in the same transaction. That is the
        // path where pending counter deltas must be visible to flush selection.
        if common::selected_mirror_capture_mode()?.is_async() {
            let dbname: String = db
                .client
                .query_one("SELECT current_database()", &[])
                .await?
                .get(0);
            db.client
                .batch_execute(&format!(
                    "ALTER DATABASE \"{dbname}\" SET koldstore.internal_async_mirror_worker = off; \
                     SET koldstore.internal_async_mirror_worker = off"
                ))
                .await?;
            let _ = common::terminate_async_worker(&db.client).await?;
        }

        db.client
            .batch_execute(&format!(
                r#"
                ALTER TABLE {} ADD COLUMN note text;
                INSERT INTO {} (id, account_id, title, qty, category, note)
                VALUES
                  (100, 1, 'new-100', 10, 'new', 'after-alter'),
                  (101, 1, 'new-101', 11, 'new', 'after-alter-2');
                "#,
                table.relation, table.relation
            ))
            .await?;

        let flushed = db.flush_table(&table.relation).await;
        if common::selected_mirror_capture_mode()?.is_async() {
            let dbname: String = db
                .client
                .query_one("SELECT current_database()", &[])
                .await?
                .get(0);
            db.client
                .batch_execute(&format!(
                    "ALTER DATABASE \"{dbname}\" RESET koldstore.internal_async_mirror_worker; \
                     RESET koldstore.internal_async_mirror_worker"
                ))
                .await?;
        }
        assert_eq!(flushed?, 2);

        let schema = db
            .client
            .query_one(
                r#"
                SELECT version, columns::text
                FROM koldstore.schemas
                WHERE table_oid = $1::text::regclass::oid
                  AND active
                "#,
                &[&table.relation],
            )
            .await?;
        let version: i32 = schema.get(0);
        let columns_text: String = schema.get(1);
        let columns: serde_json::Value = serde_json::from_str(&columns_text)?;
        assert_eq!(version, 2);
        assert!(columns
            .as_array()
            .is_some_and(|columns| columns.iter().any(|column| {
                column.get("name").and_then(serde_json::Value::as_str) == Some("note")
            })));

        let rows = db
            .client
            .query(
                &format!(
                    "SELECT id, note FROM {} WHERE id IN (1, 100) ORDER BY id",
                    table.relation
                ),
                &[],
            )
            .await?;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get::<_, i64>(0), 1);
        assert_eq!(rows[0].get::<_, Option<String>>(1), None);
        assert_eq!(rows[1].get::<_, i64>(0), 100);
        assert_eq!(
            rows[1].get::<_, Option<String>>(1).as_deref(),
            Some("after-alter")
        );
    }

    Ok(())
}

#[tokio::test]
async fn unsupported_alter_table_type_records_error_job_without_pruning_hot_rows() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "schema_evolution_reject").await?;
        let table = db.create_indexed_items_table("reject_items", 8).await?;
        db.manage_shared(&table.relation, "id").await?;

        db.client
            .batch_execute(&format!(
                "ALTER TABLE {} ADD COLUMN search tsvector",
                table.relation
            ))
            .await?;
        let flushed = db.flush_table(&table.relation).await?;
        assert_eq!(flushed, 0);

        let job = db
            .client
            .query_one(
                r#"
                SELECT status, phase, error_trace
                FROM koldstore.jobs
                WHERE table_oid = $1::text::regclass::oid
                  AND job_type = 'flush'
                ORDER BY updated_at DESC
                LIMIT 1
                "#,
                &[&table.relation],
            )
            .await?;
        assert_eq!(job.get::<_, String>(0), "error");
        assert_eq!(job.get::<_, String>(1), "failed");
        let error_trace = job.get::<_, Option<String>>(2).unwrap_or_default();
        assert!(
            error_trace.contains("unsupported PostgreSQL type: tsvector"),
            "unexpected error_trace: {error_trace}"
        );
        // Managed-table SELECT goes through merge scan, which re-introspects the
        // live catalog and cannot decode unsupported types. Prove hot rows were
        // not pruned via the change-log mirror and cold-segment catalog instead.
        let mirror = common::change_log_mirror_relation(&table.relation);
        assert_eq!(common::row_count(&db.client, &mirror).await?, 8);
        assert_eq!(
            common::cold_segment_count(&db.client, &table.relation).await?,
            0
        );
    }

    Ok(())
}
