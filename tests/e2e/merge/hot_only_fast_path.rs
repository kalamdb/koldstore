//! Hot-only KoldMergeScan execution must stay on the native PostgreSQL child.

#[path = "../common/mod.rs"]
mod common;

use anyhow::Result;

#[tokio::test]
async fn warmed_preflush_point_lookup_skips_all_cold_setup() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "hot_only_fast_path").await?;
        let table = db
            .create_indexed_items_table("hot_only_items", 1_000)
            .await?;
        db.manage_shared(&table.relation, "id").await?;

        let sql = format!("SELECT id, title FROM {} WHERE id = 1000", table.relation);
        let _: i64 = db.client.query_one(&sql, &[]).await?.get(0);
        let analyzed = common::explain_analyze(&db.client, &sql).await?;

        common::assert_kold_merge_scan_explain(&analyzed)?;
        anyhow::ensure!(
            analyzed.contains("Hot Plan: Index Scan"),
            "expected native index child, got:\n{analyzed}"
        );
        anyhow::ensure!(
            analyzed.contains("Execution Mode: hot-child"),
            "expected hot-child mode, got:\n{analyzed}"
        );
        anyhow::ensure!(
            analyzed.contains("Cold Decision: cached-no-published-segments"),
            "expected cached no-cold decision, got:\n{analyzed}"
        );
        anyhow::ensure!(
            analyzed.contains("Catalog Probes: 0"),
            "expected no executor catalog probes after warmup, got:\n{analyzed}"
        );
        anyhow::ensure!(
            analyzed.contains("Parquet segment: none"),
            "expected no Parquet open, got:\n{analyzed}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn prepared_hot_only_plan_observes_a_flush_committed_by_another_backend() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target.clone(), "hot_only_invalidation").await?;
        let table = db
            .create_indexed_items_table("invalidation_items", 32)
            .await?;
        db.manage_shared(&table.relation, "id").await?;

        let statement = db
            .client
            .prepare(&format!(
                "SELECT id, title FROM {} WHERE id = $1",
                table.relation
            ))
            .await?;
        let warm = db.client.query_one(&statement, &[&1_i64]).await?;
        assert_eq!(warm.get::<_, i64>(0), 1);

        let publisher = common::connect(&target).await?;
        let job = publisher
            .query_one(
                "SELECT koldstore.flush_table($1::text::regclass, force => true)::text",
                &[&table.relation],
            )
            .await?;
        let job_id: String = job.get(0);
        let flushed = publisher
            .query_one(
                "SELECT rows_flushed FROM koldstore.jobs WHERE id = $1::text::uuid",
                &[&job_id],
            )
            .await?
            .get::<_, i64>(0);
        anyhow::ensure!(flushed > 0, "expected publisher backend to flush rows");

        let after_flush = db.client.query_one(&statement, &[&1_i64]).await?;
        assert_eq!(after_flush.get::<_, i64>(0), 1);
        assert_eq!(after_flush.get::<_, String>(1), "item-000001");
    }

    Ok(())
}
