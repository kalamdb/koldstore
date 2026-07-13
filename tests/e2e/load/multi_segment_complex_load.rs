//! E2E: multi-segment cold layout under moderate complex load.

#[path = "../common/mod.rs"]
mod common;

use anyhow::Result;

const SEED_ROWS: i64 = 2_000;
const MAX_ROWS_PER_FILE: i64 = 100;
const WAVE_ROWS: i64 = 400;
const DELETE_COUNT: i64 = 100;

#[tokio::test]
async fn multi_segment_complex_load_stays_correct() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "multi_segment_complex_load").await?;
        let relation = format!("{}.complex_load_items", db.schema);

        db.client
            .batch_execute(&format!(
                r#"
                SET koldstore.min_max_rows_per_file = {MAX_ROWS_PER_FILE};
                CREATE TABLE {relation} (
                  id bigint PRIMARY KEY,
                  account_id integer NOT NULL,
                  title text NOT NULL,
                  qty integer NOT NULL,
                  payload jsonb NOT NULL,
                  created_at timestamptz NOT NULL DEFAULT now()
                );
                CREATE INDEX complex_load_account_idx ON {relation} (account_id);
                INSERT INTO {relation} (id, account_id, title, qty, payload)
                SELECT
                  gs,
                  (gs % 17)::integer,
                  'title-' || gs::text,
                  (gs % 50)::integer,
                  jsonb_build_object('id', gs, 'bucket', gs % 7)
                FROM generate_series(1, {SEED_ROWS}) AS gs;
                "#
            ))
            .await?;

        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name         => $1::text::regclass,
                  storage            => $2,
                  hot_row_limit      => 250,
                  min_flush_rows     => 1,
                  max_rows_per_file  => $3,
                  compression        => 'zstd',
                  migration_order_by => 'id'
                )
                "#,
                &[&relation, &db.storage_name, &MAX_ROWS_PER_FILE],
            )
            .await?;

        // Hot-path mutations before the first multi-segment flush.
        db.client
            .batch_execute(&format!(
                r#"
                UPDATE {relation}
                SET qty = qty + 1, title = title || '-u'
                WHERE id BETWEEN 1 AND 120;

                DELETE FROM {relation}
                WHERE id BETWEEN 1800 AND 1899;
                "#
            ))
            .await?;

        let flushed1 = db.flush_table(&relation).await?;
        assert!(
            flushed1 >= SEED_ROWS - DELETE_COUNT - 250,
            "first flush should move most rows cold, got {flushed1}"
        );

        // Insert a second wave against an already-cold table, then flush again.
        db.client
            .batch_execute(&format!(
                r#"
                INSERT INTO {relation} (id, account_id, title, qty, payload)
                SELECT
                  gs,
                  (gs % 17)::integer,
                  'wave-' || gs::text,
                  1,
                  jsonb_build_object('wave', 2, 'id', gs)
                FROM generate_series({SEED_ROWS} + 1, {SEED_ROWS} + {WAVE_ROWS}) AS gs;
                "#
            ))
            .await?;

        let flushed2 = db.flush_table(&relation).await?;
        assert!(flushed2 > 0, "second flush must move new wave rows");

        let segments = common::cold_segment_count(&db.client, &relation).await?;
        assert!(
            segments >= 10,
            "expected many small Parquet segments with max_rows_per_file={MAX_ROWS_PER_FILE}, got {segments}"
        );

        let total: i64 = db
            .client
            .query_one(&format!("SELECT count(*)::bigint FROM {relation}"), &[])
            .await?
            .get(0);
        let expected = SEED_ROWS - DELETE_COUNT + WAVE_ROWS;
        assert_eq!(total, expected, "merged count after complex load");

        for id in [1_i64, 60, 120] {
            let title: String = db
                .client
                .query_one(
                    &format!("SELECT title FROM {relation} WHERE id = {id}"),
                    &[],
                )
                .await?
                .get(0);
            assert!(
                title.ends_with("-u"),
                "expected updated title suffix for id={id}, got {title}"
            );
        }

        for id in [1800_i64, 1850, 1899] {
            let found: bool = db
                .client
                .query_one(
                    &format!("SELECT EXISTS(SELECT 1 FROM {relation} WHERE id = {id})"),
                    &[],
                )
                .await?
                .get(0);
            assert!(!found, "deleted id={id} must not remain visible");
        }

        for id in [1_i64, 500, SEED_ROWS, SEED_ROWS + WAVE_ROWS] {
            if (1800..=1899).contains(&id) {
                continue;
            }
            let found: bool = db
                .client
                .query_one(
                    &format!("SELECT EXISTS(SELECT 1 FROM {relation} WHERE id = {id})"),
                    &[],
                )
                .await?
                .get(0);
            assert!(found, "expected id={id} to remain visible");
        }

        let plan = common::explain_analyze(
            &db.client,
            &format!("SELECT * FROM {relation} WHERE id = 42"),
        )
        .await?;
        common::assert_kold_merge_scan_executed_cold_reads(&plan, 1)?;

        let status = common::describe_table(&db.client, &relation).await?;
        assert!(status.cold_segment_count >= 10);
        assert!(status.cold_row_count > 0);
        common::assert_no_active_jobs(&db.client, &relation).await?;
    }

    Ok(())
}
