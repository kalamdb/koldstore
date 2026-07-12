#[path = "common/mod.rs"]
mod common;

use anyhow::{Context, Result};

async fn active_schema_columns(
    db: &common::TestDb,
    relation: &str,
) -> Result<(i32, serde_json::Value, i64)> {
    let row = db
        .client
        .query_one(
            r#"
            SELECT version, columns::text, next_column_id
            FROM koldstore.schemas
            WHERE table_oid = $1::text::regclass::oid
              AND active
            ORDER BY version DESC
            LIMIT 1
            "#,
            &[&relation],
        )
        .await?;
    let columns = serde_json::from_str::<serde_json::Value>(&row.get::<_, String>(1))?;
    Ok((row.get(0), columns, row.get(2)))
}

fn column_id(columns: &serde_json::Value, name: &str) -> Result<u64> {
    columns
        .as_array()
        .and_then(|columns| {
            columns.iter().find(|column| {
                column.get("name").and_then(serde_json::Value::as_str) == Some(name)
                    && column
                        .get("active")
                        .and_then(serde_json::Value::as_bool)
                        .unwrap_or(true)
            })
        })
        .and_then(|column| column.get("column_id"))
        .and_then(serde_json::Value::as_u64)
        .with_context(|| format!("active schema column `{name}` has no column_id"))
}

#[tokio::test]
async fn alter_table_add_nullable_column_refreshes_schema_and_reads_old_cold_rows() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "schema_evolution_add").await?;
        let table = db.create_indexed_items_table("evolve_items", 12).await?;
        db.manage_shared(&table.relation, "id").await?;
        assert_eq!(db.flush_table(&table.relation).await?, 12);

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

        assert_eq!(db.flush_table(&table.relation).await?, 2);

        let (version, columns, _) = active_schema_columns(&db, &table.relation).await?;
        assert_eq!(version, 2);
        assert!(columns.as_array().is_some_and(|columns| {
            columns.iter().any(|column| {
                column.get("name").and_then(serde_json::Value::as_str) == Some("note")
            })
        }));

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
async fn rename_and_drop_preserve_column_id_and_never_reuse() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "schema_evolution_rename_drop").await?;
        let relation = db.relation("evolve_rename");
        db.client
            .batch_execute(&format!(
                "CREATE TABLE {relation} (id bigint PRIMARY KEY, payload text NOT NULL, qty int4 NOT NULL)"
            ))
            .await?;
        db.manage_shared(&relation, "id").await?;
        db.client
            .execute(
                &format!("INSERT INTO {relation} (id, payload, qty) VALUES (1, 'before', 7)"),
                &[],
            )
            .await?;
        assert_eq!(db.flush_table(&relation).await?, 1);
        let (_, initial_columns, initial_next) = active_schema_columns(&db, &relation).await?;
        let payload_id = column_id(&initial_columns, "payload")?;
        let qty_id = column_id(&initial_columns, "qty")?;

        db.client
            .batch_execute(&format!(
                r#"
                ALTER TABLE {relation} RENAME COLUMN payload TO body;
                ALTER TABLE {relation} ALTER COLUMN qty TYPE int8;
                INSERT INTO {relation} (id, body, qty) VALUES (2, 'after-rename', 8);
                "#
            ))
            .await?;
        assert_eq!(db.flush_table(&relation).await?, 1);
        let (version, renamed_columns, renamed_next) =
            active_schema_columns(&db, &relation).await?;
        assert!(version >= 2);
        assert_eq!(column_id(&renamed_columns, "body")?, payload_id);
        assert_eq!(column_id(&renamed_columns, "qty")?, qty_id);
        assert_eq!(renamed_next, initial_next);

        let cold = db
            .client
            .query_one(&format!("SELECT body FROM {relation} WHERE id = 1"), &[])
            .await?;
        assert_eq!(cold.get::<_, String>(0), "before");

        db.client
            .batch_execute(&format!(
                r#"
                ALTER TABLE {relation} DROP COLUMN body;
                ALTER TABLE {relation} ADD COLUMN replacement text;
                INSERT INTO {relation} (id, qty, replacement) VALUES (3, 9, 'replacement');
                "#
            ))
            .await?;
        assert_eq!(db.flush_table(&relation).await?, 1);
        let (_, replaced_columns, replaced_next) = active_schema_columns(&db, &relation).await?;
        let replacement_id = column_id(&replaced_columns, "replacement")?;
        assert_ne!(replacement_id, payload_id);
        assert!(u64::try_from(replaced_next)? > replacement_id);
        assert!(replaced_columns.as_array().is_some_and(|columns| {
            !columns.iter().any(|column| {
                column.get("name").and_then(serde_json::Value::as_str) == Some("body")
                    && column
                        .get("active")
                        .and_then(serde_json::Value::as_bool)
                        .unwrap_or(true)
            })
        }));
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
                "ALTER TABLE {} ADD COLUMN raw bytea",
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
            error_trace.contains("unsupported type `bytea`"),
            "unexpected error_trace: {error_trace}"
        );
        assert_eq!(common::row_count(&db.client, &table.relation).await?, 8);
        assert_eq!(
            common::cold_segment_count(&db.client, &table.relation).await?,
            0
        );
    }

    Ok(())
}
