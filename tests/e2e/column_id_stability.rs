#[path = "common/mod.rs"]
mod common;

use anyhow::{Context, Result};

async fn active_schema(db: &common::TestDb, relation: &str) -> Result<(serde_json::Value, i64)> {
    let row = db
        .client
        .query_one(
            r#"
            SELECT columns::text, next_column_id
            FROM koldstore.schemas
            WHERE table_oid = $1::text::regclass::oid
              AND active
            ORDER BY version DESC
            LIMIT 1
            "#,
            &[&relation],
        )
        .await?;
    let columns = serde_json::from_str::<serde_json::Value>(&row.get::<_, String>(0))?;
    Ok((columns, row.get(1)))
}

fn active_column_id(columns: &serde_json::Value, name: &str) -> Result<u64> {
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
async fn rename_drop_and_add_preserve_permanent_column_identity() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "column_id_stability").await?;
        let relation = db.relation("items");
        db.client
            .batch_execute(&format!(
                "CREATE TABLE {relation} (id bigint PRIMARY KEY, payload text NOT NULL)"
            ))
            .await?;
        db.manage_shared(&relation, "id").await?;

        db.client
            .execute(
                &format!("INSERT INTO {relation} (id, payload) VALUES (1, 'before-rename')"),
                &[],
            )
            .await?;
        assert_eq!(db.flush_table(&relation).await?, 1);
        let (initial_columns, initial_next) = active_schema(&db, &relation).await?;
        let payload_id = active_column_id(&initial_columns, "payload")?;

        db.client
            .batch_execute(&format!(
                r#"
                ALTER TABLE {relation} RENAME COLUMN payload TO body;
                INSERT INTO {relation} (id, body) VALUES (2, 'after-rename');
                "#
            ))
            .await?;
        assert_eq!(db.flush_table(&relation).await?, 1);
        let (renamed_columns, renamed_next) = active_schema(&db, &relation).await?;
        assert_eq!(active_column_id(&renamed_columns, "body")?, payload_id);
        assert_eq!(renamed_next, initial_next);

        let renamed_read = db
            .client
            .query_one(&format!("SELECT body FROM {relation} WHERE id = 1"), &[])
            .await?;
        assert_eq!(renamed_read.get::<_, String>(0), "before-rename");

        db.client
            .batch_execute(&format!(
                r#"
                ALTER TABLE {relation} DROP COLUMN body;
                ALTER TABLE {relation} ADD COLUMN replacement text;
                INSERT INTO {relation} (id, replacement) VALUES (3, 'replacement');
                "#
            ))
            .await?;
        assert_eq!(db.flush_table(&relation).await?, 1);
        let (replaced_columns, replaced_next) = active_schema(&db, &relation).await?;
        let replacement_id = active_column_id(&replaced_columns, "replacement")?;
        assert_ne!(replacement_id, payload_id);
        assert!(u64::try_from(replaced_next)? > replacement_id);

        let stats_keys = db
            .client
            .query_one(
                r#"
                SELECT stats.key
                FROM koldstore.segments segment
                CROSS JOIN LATERAL jsonb_object_keys(segment.column_stats) AS stats(key)
                WHERE segment.table_oid = $1::text::regclass::oid
                  AND segment.status = 'published'
                  AND segment.column_stats <> '{}'::jsonb
                ORDER BY segment.batch_number
                LIMIT 1
                "#,
                &[&relation],
            )
            .await?;
        let key = stats_keys.get::<_, String>(0);
        assert!(
            !key.is_empty() && key.chars().all(|character| character.is_ascii_digit()),
            "column_stats key must be a stringified ColumnId, got `{key}`"
        );
    }
    Ok(())
}
