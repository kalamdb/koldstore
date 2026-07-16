//! Smoke test: managed table stays equal to a plain-heap baseline under the same DML.
use crate::common;

use anyhow::Result;

#[tokio::test]
async fn baseline_equality_smoke_matches_after_shared_dml() -> Result<()> {
    common::require_pgrx_server().await?;
    let mode = common::selected_mirror_capture_mode()?.as_str();

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "eq_smoke").await?;
        let baseline = db.relation("eq_baseline");
        let managed = db.relation("eq_managed");

        for relation in [&baseline, &managed] {
            db.client
                .batch_execute(&format!(
                    r#"
                    CREATE TABLE {relation} (
                      id bigint PRIMARY KEY,
                      body text NOT NULL,
                      qty integer NOT NULL
                    );
                    "#
                ))
                .await?;
        }

        db.client
            .batch_execute("SET koldstore.min_max_rows_per_file = 1;")
            .await?;
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass,
                  storage => $2,
                  hot_row_limit => 4,
                  min_flush_rows => 1,
                  max_rows_per_file => 8,
                  migration_order_by => 'id',
                  mirror_capture_mode => $3
                )
                "#,
                &[&managed, &db.storage_name, &mode],
            )
            .await?;

        for relation in [&baseline, &managed] {
            db.client
                .batch_execute(&format!(
                    r#"
                    INSERT INTO {relation} (id, body, qty)
                    SELECT gs, 'row-' || gs::text, (gs % 5)::integer
                    FROM generate_series(1, 20) AS gs;
                    UPDATE {relation} SET body = body || '-u' WHERE id <= 5;
                    DELETE FROM {relation} WHERE id = 3;
                    INSERT INTO {relation} (id, body, qty) VALUES (3, 'reinsert', 9);
                    "#
                ))
                .await?;
        }

        db.flush_table(&managed).await?;

        common::assert_relations_equal(&db.client, &baseline, &managed).await?;
        common::assert_row_counts_equal(&db.client, &baseline, &managed).await?;
        common::assert_pk_unique(&db.client, &managed, &["id"]).await?;
        common::assert_pk_unique(&db.client, &baseline, &["id"]).await?;
    }

    Ok(())
}
