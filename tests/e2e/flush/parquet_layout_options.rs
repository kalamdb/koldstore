//! E2E coverage for per-table Parquet writer layout settings.

use anyhow::Result;

use crate::common;

#[tokio::test]
async fn manage_table_layout_options_control_future_parquet_flushes() -> Result<()> {
    common::require_pgrx_server().await?;
    let target = common::scenario_pg_matrix()
        .into_iter()
        .next()
        .expect("at least one pgrx PostgreSQL target");
    let db = common::TestDb::start(target, "parquet_layout_options").await?;
    let relation = db.relation("layout_items");
    db.client
        .batch_execute(&format!(
            "CREATE TABLE {relation} (id bigint PRIMARY KEY, payload text NOT NULL); \
             SET koldstore.min_max_rows_per_file = 1;"
        ))
        .await?;
    db.client
        .execute(
            r#"
            SELECT koldstore.manage_table(
              table_name => $1::text::regclass,
              storage => $2,
              hot_row_limit => 1,
              min_flush_rows => 1,
              max_rows_per_file => 16,
              migration_order_by => 'id',
              parquet_row_group_size => 4,
              parquet_data_page_row_count_limit => 2,
              parquet_bloom_filter_fpp => 0.02,
              auto_flush => false
            )
            "#,
            &[&relation, &db.storage_name],
        )
        .await?;
    db.client
        .batch_execute(&format!(
            "INSERT INTO {relation} (id, payload) \
             SELECT gs, repeat('payload-', 64) || gs::text FROM generate_series(1, 16) AS gs;"
        ))
        .await?;
    common::fence_async_mirror(&db.client).await?;
    assert_eq!(db.flush_table_with_force(&relation, true).await?, 16);

    let options = db
        .client
        .query_one(
            r#"
            SELECT options::text
            FROM koldstore.schemas
            WHERE table_oid = $1::text::regclass::oid AND active
            "#,
            &[&relation],
        )
        .await?
        .get::<_, String>(0)
        .parse::<serde_json::Value>()?;
    assert_eq!(options["parquet_row_group_size"], 4);
    assert_eq!(options["parquet_data_page_row_count_limit"], 2);
    assert_eq!(options["parquet_bloom_filter_fpp"], 0.02);
    assert_eq!(
        options["cold_metadata"]["stats_columns"][0]["name"], "id",
        "primary key must retain automatic pruning metadata"
    );
    assert_eq!(
        options["cold_metadata"]["bloom_filter_columns"][0]["name"], "id",
        "primary key must retain automatic Bloom metadata"
    );

    let first_row_group_count: i32 = db
        .client
        .query_one(
            "SELECT row_group_count FROM koldstore.cold_segments \
             WHERE table_oid = $1::text::regclass::oid AND status = 'active' \
             ORDER BY batch_number LIMIT 1",
            &[&relation],
        )
        .await?
        .get(0);
    assert_eq!(first_row_group_count, 4);

    db.client
        .batch_execute(&format!(
            "ALTER TABLE {relation} SET (\
              koldstore_parquet_row_group_size = 8, \
              koldstore_parquet_data_page_row_count_limit = 4, \
              koldstore_parquet_bloom_filter_fpp = 0.03\
             )"
        ))
        .await?;
    db.client
        .batch_execute(&format!(
            "INSERT INTO {relation} (id, payload) \
             SELECT gs, repeat('new-payload-', 64) || gs::text FROM generate_series(17, 24) AS gs;"
        ))
        .await?;
    common::fence_async_mirror(&db.client).await?;
    assert_eq!(db.flush_table_with_force(&relation, true).await?, 8);
    let row_group_count: i32 = db
        .client
        .query_one(
            "SELECT row_group_count FROM koldstore.cold_segments \
             WHERE table_oid = $1::text::regclass::oid AND status = 'active' \
             ORDER BY batch_number DESC LIMIT 1",
            &[&relation],
        )
        .await?
        .get(0);
    assert_eq!(row_group_count, 1, "ALTER must apply to future flushes");
    Ok(())
}
