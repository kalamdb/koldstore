#[path = "../common/mod.rs"]
mod common;

use anyhow::Result;

/// Verifies flush cleanup removes flushed rows from both the base table and `__cl`
/// mirror in the same operation, including delete-marker rows that have no base row.
#[tokio::test]
async fn flush_removes_base_and_mirror_rows_together() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "flush_hot_mirror_cleanup").await?;
        let table_name = "flush_mirror_cleanup_items";
        let relation = db.relation(table_name);
        let mirror = common::change_log_mirror_relation(&relation);

        db.client
            .batch_execute(&format!(
                r#"
                CREATE TABLE {relation} (
                  id bigint PRIMARY KEY,
                  body text NOT NULL
                );
                "#
            ))
            .await?;
        db.manage_shared(&relation, "id").await?;

        db.client
            .batch_execute(&format!(
                r#"
                INSERT INTO {relation} (id, body) VALUES
                  (1, 'one'),
                  (2, 'two'),
                  (3, 'three');
                UPDATE {relation} SET body = 'two-updated' WHERE id = 2;
                DELETE FROM {relation} WHERE id = 3;
                INSERT INTO {relation} (id, body) VALUES (4, 'four');
                "#
            ))
            .await?;

        assert_eq!(common::hot_row_count(&db.client, &relation).await?, 3);
        assert_eq!(common::row_count(&db.client, &mirror).await?, 4);

        let flushed = db.flush_table(&relation).await?;
        assert_eq!(flushed, 4);
        common::assert_flush_pruned_hot_storage(&db.client, &relation, 4).await?;
        common::assert_cold_metadata_present(&db.client, &relation).await?;
        common::assert_no_active_jobs(&db.client, &relation).await?;

        let merged = db
            .client
            .query(
                &format!("SELECT id, body FROM {relation} WHERE id IN (1, 2, 4) ORDER BY id"),
                &[],
            )
            .await?;
        assert_eq!(merged.len(), 3);
        assert_eq!(merged[0].get::<_, i64>(0), 1);
        assert_eq!(merged[0].get::<_, String>(1), "one");
        assert_eq!(merged[1].get::<_, i64>(0), 2);
        assert_eq!(merged[1].get::<_, String>(1), "two-updated");
        assert_eq!(merged[2].get::<_, i64>(0), 4);
        assert_eq!(merged[2].get::<_, String>(1), "four");

        db.client
            .execute(
                &format!("INSERT INTO {relation} (id, body) VALUES (5, 'five')"),
                &[],
            )
            .await?;
        assert_eq!(common::hot_row_count(&db.client, &relation).await?, 1);
        assert_eq!(common::row_count(&db.client, &mirror).await?, 1);

        let second_flush = db.flush_table(&relation).await?;
        assert_eq!(second_flush, 1);
        common::assert_flush_pruned_hot_storage(&db.client, &relation, 5).await?;

        let total = db
            .client
            .query_one(&format!("SELECT count(*) FROM {relation}"), &[])
            .await?
            .get::<_, i64>(0);
        assert_eq!(
            total, 4,
            "merged reads should return live rows only; deleted id=3 stays masked"
        );
    }

    Ok(())
}
