//! Deep retained-growth gates across flush, DML, merge-scan, and MinIO reads.
//!
//! These are not contract stubs: each cycle exercises manage-table hot paths
//! and fails when PostgreSQL memory contexts or process RSS grow unboundedly
//! after warmup.

use crate::common;

use anyhow::{bail, Context, Result};
use koldstore_memory::{
    evaluate_growth, format_comparison_table, format_overhead_table, WorkloadReport,
};
use koldstore_storage::StorageClient;
use parquet::file::reader::{FileReader, SerializedFileReader};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_flush_dml_and_merge_scan_memory_stays_bounded() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target.clone(), "memory_fs").await?;
        run_lifecycle_memory_probe(&db, false).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_minio_flush_dml_merge_scan_and_parquet_read_memory_stays_bounded() -> Result<()> {
    if !common::minio_enabled() {
        eprintln!(
            "skipping MinIO memory leak E2E: set KOLDSTORE_MINIO=1 and start MinIO \
             (docker/run.sh --no-build or scripts/ci/start-minio.sh)"
        );
        return Ok(());
    }

    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start_minio(target.clone(), "memory_minio").await?;
        run_lifecycle_memory_probe(&db, true).await?;
    }
    Ok(())
}

async fn run_lifecycle_memory_probe(db: &common::TestDb, use_minio: bool) -> Result<()> {
    let warmup = common::memory::warmup_cycles();
    let measure = common::memory::measure_cycles();
    let batch_rows = common::memory::batch_rows();
    let scan_reps = common::memory::scan_reps();
    let budget = common::memory::growth_budget_from_env();
    let seed_rows = batch_rows * 4;

    let table = db
        .create_indexed_items_table("memory_items", seed_rows)
        .await?;
    db.manage_shared(&table.relation, "id").await?;

    // Seed cold data so every measured cycle hits merge-scan cold reads.
    let flushed = db.flush_table(&table.relation).await?;
    assert!(flushed > 0, "initial flush should move hot rows to cold");
    common::assert_cold_metadata_present(&db.client, &table.relation).await?;
    common::assert_no_active_jobs(&db.client, &table.relation).await?;

    let mut next_id = seed_rows + 1;
    let mut samples = Vec::with_capacity(measure);

    for cycle in 0..(warmup + measure) {
        let _step = common::log_step(format!(
            "memory leak cycle {cycle}/{} (warmup={warmup}, measure={measure}, minio={use_minio})",
            warmup + measure - 1
        ));

        run_hot_dml_batch(db, &table.relation, next_id, batch_rows, cycle).await?;
        next_id += batch_rows;

        common::fence_async_mirror_if_needed(&db.client).await?;
        let flushed = db.flush_table(&table.relation).await?;
        assert!(
            flushed > 0,
            "cycle {cycle}: expected flush to write newly inserted hot rows"
        );
        common::assert_no_active_jobs(&db.client, &table.relation).await?;

        // Repeated merge-scan SELECTs are the primary scan-leak surface.
        for _ in 0..scan_reps {
            exercise_merge_scan_reads(db, &table.relation).await?;
        }

        if use_minio {
            // Read every active cold segment once per cycle to stress object-store
            // handle / parquet footer caching on the MinIO path.
            exercise_minio_parquet_reads(db, &table.relation).await?;
        }

        // Drop transient SPI/plan caches between cycles so retained growth is
        // attributable to extension-owned state rather than one-shot setup.
        db.client.batch_execute("DISCARD PLANS;").await?;

        if cycle >= warmup {
            let snapshot = common::memory::capture_snapshot(&db.client, db.target.port).await?;
            common::log(format!(
                "memory sample cycle={cycle} pg_context_bytes={} rss_bytes={}",
                snapshot.pg_context_bytes, snapshot.rss_bytes
            ));
            samples.push(snapshot);
        }
    }

    let evaluation = evaluate_growth(&samples).map_err(anyhow::Error::msg)?;
    if !evaluation.within_budget(budget) {
        bail!(
            "retained memory growth exceeded budget on pg{} (minio={use_minio}): \
             context +{} bytes ({}/cycle), rss +{} bytes ({}/cycle); budget={budget:?}; \
             evaluation={evaluation:?}",
            db.target.version,
            evaluation.pg_context_growth_bytes,
            evaluation.pg_context_bytes_per_cycle,
            evaluation.rss_growth_bytes,
            evaluation.rss_bytes_per_cycle,
        );
    }

    common::log_always(format!(
        "memory leak gate passed pg{} minio={use_minio}: context +{} bytes, rss +{} bytes over {} cycles",
        db.target.version,
        evaluation.pg_context_growth_bytes,
        evaluation.rss_growth_bytes,
        samples.len().saturating_sub(1)
    ));
    Ok(())
}

async fn run_hot_dml_batch(
    db: &common::TestDb,
    relation: &str,
    start_id: i64,
    rows: i64,
    cycle: usize,
) -> Result<()> {
    let end_id = start_id + rows - 1;
    db.client
        .batch_execute(&format!(
            r#"
            INSERT INTO {relation} (id, account_id, title, qty, category)
            SELECT
              gs::bigint,
              (gs % 17)::bigint,
              'mem-' || lpad(gs::text, 6, '0'),
              (gs % 100)::integer,
              'cycle-{cycle}'
            FROM generate_series({start_id}, {end_id}) AS gs;

            UPDATE {relation}
            SET qty = qty + 1,
                title = title || '-u'
            WHERE id BETWEEN {start_id} AND {mid_id};

            DELETE FROM {relation}
            WHERE id BETWEEN {delete_start} AND {delete_end};

            ANALYZE {relation};
            "#,
            mid_id = start_id + rows / 2,
            delete_start = start_id,
            delete_end = start_id + (rows / 8).max(1) - 1,
        ))
        .await
        .with_context(|| format!("hot DML batch starting at {start_id}"))?;
    Ok(())
}

async fn exercise_merge_scan_reads(db: &common::TestDb, relation: &str) -> Result<()> {
    let plan = common::explain(
        &db.client,
        &format!("SELECT id, title FROM {relation} WHERE id = 1"),
    )
    .await?;
    common::assert_kold_merge_scan_explain(&plan)?;

    let cold = db
        .client
        .query_one(
            &format!("SELECT id, title FROM {relation} WHERE id = 1"),
            &[],
        )
        .await
        .context("cold point lookup via merge scan")?;
    assert_eq!(cold.get::<_, i64>(0), 1);

    let count: i64 = db
        .client
        .query_one(&format!("SELECT count(*) FROM {relation}"), &[])
        .await?
        .get(0);
    assert!(count > 0, "merge-scan count should remain positive");

    // Indexed title + PK range paths pull additional cold row groups / overlays.
    let by_title: i64 = db
        .client
        .query_one(
            &format!("SELECT count(*) FROM {relation} WHERE title = 'item-000001'"),
            &[],
        )
        .await?
        .get(0);
    assert_eq!(by_title, 1);

    let by_range: i64 = db
        .client
        .query_one(
            &format!("SELECT count(*) FROM {relation} WHERE id BETWEEN 1 AND 16"),
            &[],
        )
        .await?
        .get(0);
    assert!(by_range > 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_overhead_vs_plain_postgres_reports_spikes_and_deltas() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target.clone(), "memory_cmp").await?;
        run_overhead_comparison(&db).await?;
    }
    Ok(())
}

async fn run_overhead_comparison(db: &common::TestDb) -> Result<()> {
    let batch_rows = common::memory::batch_rows();
    let scan_reps = common::memory::scan_reps();
    let seed_rows = batch_rows * 4;
    let budget = common::memory::overhead_budget_from_env();
    let port = db.target.port;

    let plain = db
        .create_indexed_items_table("plain_items", seed_rows)
        .await?;
    let managed = db
        .create_indexed_items_table("kold_items", seed_rows)
        .await?;
    db.manage_shared(&managed.relation, "id").await?;
    common::fence_async_mirror_if_needed(&db.client).await?;
    db.client.batch_execute("DISCARD PLANS;").await?;

    let mut reports = Vec::new();

    // Idle / at-rest: extension loaded + managed catalog vs plain heap.
    reports.push(WorkloadReport {
        workload: "idle".into(),
        target: "plain".into(),
        allocation: common::memory::measure_phase(
            &db.client,
            port,
            || async {
                let _ = common::row_count(&db.client, &plain.relation).await?;
                Ok(())
            },
            || async { Ok(()) },
        )
        .await?,
    });
    reports.push(WorkloadReport {
        workload: "idle".into(),
        target: "koldstore".into(),
        allocation: common::memory::measure_phase(
            &db.client,
            port,
            || async {
                let _ = common::row_count(&db.client, &managed.relation).await?;
                Ok(())
            },
            || async { Ok(()) },
        )
        .await?,
    });

    // DML: identical INSERT/UPDATE/DELETE shape on both relations.
    let half = (batch_rows / 2).max(4);
    let plain_id_a = seed_rows + 1;
    let plain_id_b = plain_id_a + half;
    let kold_id_a = seed_rows + 1;
    let kold_id_b = kold_id_a + half;
    reports.push(WorkloadReport {
        workload: format!("DML ({batch_rows} rows × 2 halves)"),
        target: "plain".into(),
        allocation: common::memory::measure_phase(
            &db.client,
            port,
            || async { run_hot_dml_batch(db, &plain.relation, plain_id_a, half, 0).await },
            || async { run_hot_dml_batch(db, &plain.relation, plain_id_b, half, 1).await },
        )
        .await?,
    });
    reports.push(WorkloadReport {
        workload: format!("DML ({batch_rows} rows × 2 halves)"),
        target: "koldstore".into(),
        allocation: common::memory::measure_phase(
            &db.client,
            port,
            || async { run_hot_dml_batch(db, &managed.relation, kold_id_a, half, 0).await },
            || async {
                run_hot_dml_batch(db, &managed.relation, kold_id_b, half, 1).await?;
                common::fence_async_mirror_if_needed(&db.client).await?;
                Ok(())
            },
        )
        .await?,
    });
    db.client.batch_execute("DISCARD PLANS;").await?;

    // Hot-only queries: managed table still fully hot (no flush yet).
    reports.push(WorkloadReport {
        workload: format!("query hot-only ({scan_reps} reps)"),
        target: "plain".into(),
        allocation: common::memory::measure_phase(
            &db.client,
            port,
            || async {
                exercise_plain_reads(db, &plain.relation, scan_reps / 2).await?;
                Ok(())
            },
            || async {
                exercise_plain_reads(db, &plain.relation, scan_reps - scan_reps / 2).await?;
                Ok(())
            },
        )
        .await?,
    });
    reports.push(WorkloadReport {
        workload: format!("query hot-only ({scan_reps} reps)"),
        target: "koldstore".into(),
        allocation: common::memory::measure_phase(
            &db.client,
            port,
            || async {
                exercise_merge_scan_reads_n(db, &managed.relation, scan_reps / 2).await?;
                Ok(())
            },
            || async {
                exercise_merge_scan_reads_n(db, &managed.relation, scan_reps - scan_reps / 2)
                    .await?;
                Ok(())
            },
        )
        .await?,
    });
    db.client.batch_execute("DISCARD PLANS;").await?;

    // Flush + hot+cold queries (koldstore only; plain has no cold tier).
    reports.push(WorkloadReport {
        workload: "flush".into(),
        target: "koldstore".into(),
        allocation: common::memory::measure_phase(
            &db.client,
            port,
            || async {
                let flushed = db.flush_table(&managed.relation).await?;
                assert!(flushed > 0);
                Ok(())
            },
            || async {
                common::assert_no_active_jobs(&db.client, &managed.relation).await?;
                Ok(())
            },
        )
        .await?,
    });
    // Leave a few hot rows so merge-scan mixes hot + cold.
    let hot_tail_id = kold_id_b + half;
    run_hot_dml_batch(
        db,
        &managed.relation,
        hot_tail_id,
        (batch_rows / 4).max(8),
        99,
    )
    .await?;
    common::fence_async_mirror_if_needed(&db.client).await?;
    db.client.batch_execute("DISCARD PLANS;").await?;

    reports.push(WorkloadReport {
        workload: format!("query hot+cold ({scan_reps} reps)"),
        target: "koldstore".into(),
        allocation: common::memory::measure_phase(
            &db.client,
            port,
            || async {
                exercise_merge_scan_reads_n(db, &managed.relation, scan_reps / 2).await?;
                Ok(())
            },
            || async {
                exercise_merge_scan_reads_n(db, &managed.relation, scan_reps - scan_reps / 2)
                    .await?;
                Ok(())
            },
        )
        .await?,
    });

    let title = format!(
        "Memory overhead vs plain PostgreSQL (pg{})",
        db.target.version
    );
    let table = format_comparison_table(&title, &reports);
    let overhead = format_overhead_table(&reports);
    // Emit on stderr; `run_memory_checks.sh` runs this filter with --no-capture.
    common::log_always(format!("\n{table}\n{overhead}"));

    assert_overhead_within_budget(&reports, budget)?;
    Ok(())
}

fn assert_overhead_within_budget(
    reports: &[WorkloadReport],
    budget: common::memory::OverheadBudget,
) -> Result<()> {
    let dml_plain = find_report(reports, "DML", "plain")?;
    let dml_kold = find_report(reports, "DML", "koldstore")?;
    let hot_plain = find_report(reports, "query hot-only", "plain")?;
    let hot_kold = find_report(reports, "query hot-only", "koldstore")?;

    let dml_delta_overhead = (dml_kold.allocation.context_delta_bytes()
        - dml_plain.allocation.context_delta_bytes())
    .max(0) as u64;
    let dml_after_overhead = dml_kold
        .allocation
        .after
        .pg_context_bytes
        .saturating_sub(dml_plain.allocation.after.pg_context_bytes);
    let hot_delta_overhead = (hot_kold.allocation.context_delta_bytes()
        - hot_plain.allocation.context_delta_bytes())
    .max(0) as u64;
    let hot_after_overhead = hot_kold
        .allocation
        .after
        .pg_context_bytes
        .saturating_sub(hot_plain.allocation.after.pg_context_bytes);

    if dml_delta_overhead > budget.max_dml_context_delta_overhead_bytes
        || dml_after_overhead > budget.max_dml_context_after_overhead_bytes
        || hot_delta_overhead > budget.max_hot_query_context_delta_overhead_bytes
        || hot_after_overhead > budget.max_hot_query_context_after_overhead_bytes
    {
        bail!(
            "koldstore memory overhead vs plain Postgres exceeded budget: \
             DML contextΔ overhead={} (max {}), DML context-after overhead={} (max {}), \
             hot-query contextΔ overhead={} (max {}), hot-query context-after overhead={} (max {})",
            dml_delta_overhead,
            budget.max_dml_context_delta_overhead_bytes,
            dml_after_overhead,
            budget.max_dml_context_after_overhead_bytes,
            hot_delta_overhead,
            budget.max_hot_query_context_delta_overhead_bytes,
            hot_after_overhead,
            budget.max_hot_query_context_after_overhead_bytes,
        );
    }
    Ok(())
}

fn find_report<'a>(
    reports: &'a [WorkloadReport],
    workload_prefix: &str,
    target: &str,
) -> Result<&'a WorkloadReport> {
    reports
        .iter()
        .find(|row| row.target == target && row.workload.starts_with(workload_prefix))
        .with_context(|| format!("missing report row for {workload_prefix}/{target}"))
}

async fn exercise_plain_reads(db: &common::TestDb, relation: &str, reps: usize) -> Result<()> {
    for _ in 0..reps.max(1) {
        let row = db
            .client
            .query_one(
                &format!("SELECT id, title FROM {relation} WHERE id = 1"),
                &[],
            )
            .await?;
        assert_eq!(row.get::<_, i64>(0), 1);
        let _: i64 = db
            .client
            .query_one(&format!("SELECT count(*) FROM {relation}"), &[])
            .await?
            .get(0);
        let _: i64 = db
            .client
            .query_one(
                &format!("SELECT count(*) FROM {relation} WHERE title = 'item-000001'"),
                &[],
            )
            .await?
            .get(0);
        let _: i64 = db
            .client
            .query_one(
                &format!("SELECT count(*) FROM {relation} WHERE id BETWEEN 1 AND 16"),
                &[],
            )
            .await?
            .get(0);
    }
    Ok(())
}

async fn exercise_merge_scan_reads_n(
    db: &common::TestDb,
    relation: &str,
    reps: usize,
) -> Result<()> {
    for _ in 0..reps.max(1) {
        exercise_merge_scan_reads(db, relation).await?;
    }
    Ok(())
}

async fn exercise_minio_parquet_reads(db: &common::TestDb, relation: &str) -> Result<()> {
    let artifacts = db
        .client
        .query(
            r#"
            SELECT cs.object_path, cs.row_count
            FROM koldstore.cold_segments cs
            WHERE cs.table_oid = $1::text::regclass::oid
              AND cs.status = 'active'
            ORDER BY cs.batch_number
            "#,
            &[&relation],
        )
        .await
        .context("load active cold parquet paths")?;
    assert!(
        !artifacts.is_empty(),
        "expected at least one active cold parquet segment"
    );

    let client = db.minio_client()?;
    for artifact in artifacts {
        let object_path: String = artifact.get(0);
        let row_count: i64 = artifact.get(1);
        assert!(row_count > 0, "cold segment should contain rows");

        let parquet_bytes = client
            .get(&object_path)
            .with_context(|| format!("get MinIO parquet {object_path}"))?;
        let reader = SerializedFileReader::new(bytes::Bytes::from(parquet_bytes))
            .with_context(|| format!("parse MinIO parquet {object_path}"))?;
        assert_eq!(
            reader.metadata().file_metadata().num_rows(),
            row_count,
            "parquet row count should match catalog for {object_path}"
        );
    }
    Ok(())
}
