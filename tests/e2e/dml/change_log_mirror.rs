#[path = "../common/mod.rs"]
mod common;

use anyhow::Result;

#[tokio::test]
async fn mirror_tracks_insert_update_delete_reinsert_and_rollback() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "change_log_mirror").await?;
        let table_name = format!("{}_messages", db.schema);
        let relation = db.relation(&table_name);
        let mirror = format!("koldstore.{table_name}__cl");

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
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name     => $1::text::regclass,
                  storage        => $2,
                  hot_row_limit  => 1000,
                  min_flush_rows => 1,
                  migration_order_by => 'id'
                )
                "#,
                &[&relation, &db.storage_name],
            )
            .await?;

        common::assert_system_columns_absent(&db.client, &relation).await?;
        common::assert_change_log_mirror_exists(&db.client, &mirror).await?;
        common::assert_primary_key_columns_match(&db.client, &relation, &mirror).await?;

        db.client
            .execute(
                &format!("INSERT INTO {relation} (id, body) VALUES (1, 'one')"),
                &[],
            )
            .await?;
        let insert = mirror_state(&db.client, &mirror, 1).await?;
        assert_eq!(insert.op, 1);

        db.client
            .execute(
                &format!("UPDATE {relation} SET body = 'two' WHERE id = 1"),
                &[],
            )
            .await?;
        let update = mirror_state(&db.client, &mirror, 1).await?;
        assert_eq!(update.op, 2);
        assert!(update.seq > insert.seq);

        db.client
            .execute(&format!("DELETE FROM {relation} WHERE id = 1"), &[])
            .await?;
        let delete = mirror_state(&db.client, &mirror, 1).await?;
        assert_eq!(delete.op, 3);
        assert!(delete.seq > update.seq);

        db.client
            .execute(
                &format!("INSERT INTO {relation} (id, body) VALUES (1, 'again')"),
                &[],
            )
            .await?;
        let reinsert = mirror_state(&db.client, &mirror, 1).await?;
        assert_eq!(reinsert.op, 1);
        assert!(reinsert.seq > delete.seq);
        assert_eq!(mirror_row_count(&db.client, &mirror, 1).await?, 1);

        db.client
            .batch_execute(&format!(
                r#"
                BEGIN;
                UPDATE {relation} SET body = 'rolled-back' WHERE id = 1;
                ROLLBACK;
                "#
            ))
            .await?;
        let after_rollback = mirror_state(&db.client, &mirror, 1).await?;
        assert_eq!(after_rollback, reinsert);

        let pk_update = db
            .client
            .execute(&format!("UPDATE {relation} SET id = 2 WHERE id = 1"), &[])
            .await;
        assert!(
            pk_update.is_err(),
            "managed primary-key updates must be rejected to avoid stale mirror rows"
        );
        assert_eq!(mirror_row_count(&db.client, &mirror, 1).await?, 1);
        assert_eq!(mirror_row_count(&db.client, &mirror, 2).await?, 0);
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MirrorState {
    seq: i64,
    op: i16,
}

async fn mirror_state(
    client: &tokio_postgres::Client,
    mirror: &str,
    id: i64,
) -> Result<MirrorState> {
    let row = client
        .query_one(
            &format!("SELECT seq, op FROM {mirror} WHERE id = $1"),
            &[&id],
        )
        .await?;
    Ok(MirrorState {
        seq: row.get(0),
        op: row.get(1),
    })
}

async fn mirror_row_count(client: &tokio_postgres::Client, mirror: &str, id: i64) -> Result<i64> {
    let row = client
        .query_one(
            &format!("SELECT count(*) FROM {mirror} WHERE id = $1"),
            &[&id],
        )
        .await?;
    Ok(row.get(0))
}
