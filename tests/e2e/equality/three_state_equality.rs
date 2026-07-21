//! Three-state differential: managed table equals plain-heap baseline when
//! all-hot, mixed hot/cold, and mostly-cold.
//!
//! Curated queries stay small in-repo. Broader random coverage is external via
//! `scripts/differential/run-sqlsmith-compare.sh` (SQLsmith).
//!
//! Table names must be unique per fixture (not only schemas): change-log /
//! mirror relations are derived from the unqualified relname and would collide
//! across schemas if every fixture reused `eq3_managed`.

use crate::common;

use anyhow::{Context, Result};

const CURATED_QUERIES: &[&str] = &[
    "SELECT id, body, qty FROM {rel} WHERE id = 1",
    "SELECT id, body, qty FROM {rel} WHERE id BETWEEN 3 AND 8 ORDER BY id",
    "SELECT count(*)::bigint AS c, coalesce(sum(qty), 0)::bigint AS s FROM {rel}",
    "SELECT id, body, qty FROM {rel} ORDER BY id LIMIT 5",
    "SELECT id, body, qty FROM {rel} WHERE body LIKE 'row-%' ORDER BY id",
];

#[tokio::test]
async fn three_state_baseline_equality_with_curated_queries() -> Result<()> {
    common::require_pgrx_server().await?;
    let mode = common::selected_mirror_capture_mode()?.as_str();

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "eq_3state").await?;
        // Unique unqualified names avoid `__cl` mirror collisions across leftover
        // schemas on the pooled worker database.
        let baseline = db.relation(&format!("eq3_b_{}", db.schema));
        let managed = db.relation(&format!("eq3_m_{}", db.schema));

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
                  hot_row_limit => 6,
                  min_flush_rows => 1,
                  max_rows_per_file => 8,
                  migration_order_by => 'id',
                  auto_flush => false,
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
                    FROM generate_series(1, 24) AS gs;
                    "#
                ))
                .await?;
        }

        assert_state(&db, &baseline, &managed, "hot").await?;

        let flushed_mixed = db.flush_table(&managed).await?;
        assert!(
            flushed_mixed > 0,
            "mixed state requires some rows flushed, got {flushed_mixed}"
        );
        assert_state(&db, &baseline, &managed, "mixed").await?;

        // Post-flush inserts only (cold SQL UPDATE is not a heap twin in MVP).
        for relation in [&baseline, &managed] {
            db.client
                .batch_execute(&format!(
                    r#"
                    INSERT INTO {relation} (id, body, qty)
                    SELECT gs, 'late-' || gs::text, 1
                    FROM generate_series(100, 105) AS gs;
                    "#
                ))
                .await?;
        }
        let flushed_cold = db.flush_table(&managed).await?;
        assert!(
            flushed_cold >= 0,
            "cold-leaning flush should not error (rows_flushed={flushed_cold})"
        );
        assert_state(&db, &baseline, &managed, "cold").await?;
    }

    Ok(())
}

async fn assert_state(
    db: &common::TestDb,
    baseline: &str,
    managed: &str,
    label: &str,
) -> Result<()> {
    let left_only = db
        .client
        .query(
            &format!(
                r#"
                SELECT id, body, qty FROM {managed}
                EXCEPT ALL
                SELECT id, body, qty FROM {baseline}
                "#
            ),
            &[],
        )
        .await
        .with_context(|| format!("{label}: managed-only rows"))?;
    let right_only = db
        .client
        .query(
            &format!(
                r#"
                SELECT id, body, qty FROM {baseline}
                EXCEPT ALL
                SELECT id, body, qty FROM {managed}
                "#
            ),
            &[],
        )
        .await
        .with_context(|| format!("{label}: baseline-only rows"))?;
    if !left_only.is_empty() || !right_only.is_empty() {
        anyhow::bail!(
            "{label}: business-column mismatch managed-only={} baseline-only={}",
            left_only.len(),
            right_only.len()
        );
    }
    common::assert_row_counts_equal(&db.client, baseline, managed)
        .await
        .with_context(|| format!("{label}: row counts"))?;
    common::assert_pk_unique(&db.client, managed, &["id"])
        .await
        .with_context(|| format!("{label}: managed PK unique"))?;

    for template in CURATED_QUERIES {
        let left_sql = template.replace("{rel}", baseline);
        let right_sql = template.replace("{rel}", managed);
        let left_hash: String = db
            .client
            .query_one(
                &format!(
                    r#"
                    SELECT md5(
                      coalesce(
                        string_agg(row_to_json(t)::text, E'\n' ORDER BY row_to_json(t)::text),
                        ''
                      )
                    )
                    FROM ({left_sql}) AS t
                    "#
                ),
                &[],
            )
            .await
            .with_context(|| format!("{label}: hash baseline `{template}`"))?
            .get(0);
        let right_hash: String = db
            .client
            .query_one(
                &format!(
                    r#"
                    SELECT md5(
                      coalesce(
                        string_agg(row_to_json(t)::text, E'\n' ORDER BY row_to_json(t)::text),
                        ''
                      )
                    )
                    FROM ({right_sql}) AS t
                    "#
                ),
                &[],
            )
            .await
            .with_context(|| format!("{label}: hash managed `{template}`"))?
            .get(0);
        assert_eq!(
            left_hash, right_hash,
            "{label}: result hash mismatch for `{template}`"
        );
    }
    Ok(())
}
