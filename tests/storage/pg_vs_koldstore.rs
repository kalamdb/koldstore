//! Compare plain PostgreSQL storage/speed with the same table under KoldStore.
//!
//! Schema: [`schema.sql`](schema.sql). Seeds a wide (~50 column) table, measures
//! DML, then **hot-only PK lookups before flush** (both heaps still hold all
//! rows — fair merge-scan overhead), flushes older rows to zstd Parquet, then
//! measures hot+cold PK lookups and compares PostgreSQL heap/index sizes.

#[path = "../e2e/common/mod.rs"]
mod common;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tokio_postgres::Client;

/// Local default keeps the harness usable; set `KOLDSTORE_STORAGE_ROWS=1000000`
/// for the README-scale demonstration.
const DEFAULT_ROWS: i64 = 100_000;
const DEFAULT_HOT_LIMIT: i64 = 10_000;
const DEFAULT_DML_SAMPLE: i64 = 1_000;
const QUERY_LOOPS: usize = 20;

#[derive(Debug, Clone, Copy)]
struct Timing {
    elapsed: Duration,
    ops: i64,
}

impl Timing {
    fn per_op_us(self) -> f64 {
        if self.ops <= 0 {
            return 0.0;
        }
        self.elapsed.as_secs_f64() * 1_000_000.0 / self.ops as f64
    }

    fn ops_per_sec(self) -> f64 {
        if self.elapsed.is_zero() {
            return 0.0;
        }
        self.ops as f64 / self.elapsed.as_secs_f64()
    }
}

#[derive(Debug, Clone, Copy)]
struct Sizes {
    table_bytes: i64,
    index_bytes: i64,
    cold_bytes: i64,
}

/// Heap live/dead tuple counters from `pg_stat_user_tables` after the DML workload.
#[derive(Debug, Clone, Copy)]
struct HeapStats {
    n_live_tup: i64,
    n_dead_tup: i64,
}

#[derive(Debug, Clone, Copy)]
struct SideMetrics {
    insert: Timing,
    update: Timing,
    delete: Timing,
    query_hot_only: Timing,
    query_hot_cold: Timing,
    /// Timed `VACUUM (FULL, ANALYZE)` after flush (smaller hot heap → less work).
    vacuum: Timing,
    /// Dead/live tuple pressure after DML, before flush.
    heap_after_workload: HeapStats,
    sizes: Sizes,
}

#[tokio::test]
async fn pg_vs_koldstore_storage_and_speed_comparison() -> Result<()> {
    common::require_pgrx_server().await?;
    let rows = env_i64("KOLDSTORE_STORAGE_ROWS", DEFAULT_ROWS);
    let hot_limit = env_i64("KOLDSTORE_STORAGE_HOT_LIMIT", DEFAULT_HOT_LIMIT)
        .clamp(1, rows.saturating_sub(1).max(1));
    let dml_sample = env_i64("KOLDSTORE_STORAGE_DML_SAMPLE", DEFAULT_DML_SAMPLE).clamp(1, rows);

    let target = common::local_pg_matrix()
        .into_iter()
        .next()
        .context("no local pg target configured")?;
    let db = common::TestDb::start(target, "storage_cmp").await?;

    // Warm the extension library so planner hooks/GUCs are registered.
    let _ = db
        .client
        .query_one("SELECT koldstore_version()", &[])
        .await?;

    // Mirror tables are `koldstore.<unqualified>__cl`, so table names must be
    // unique across schemas in the shared E2E database.
    let baseline_table = format!("{}_baseline", db.schema);
    let managed_table = format!("{}_managed", db.schema);
    let baseline = format!("{}.{}", db.schema, baseline_table);
    let managed = format!("{}.{}", db.schema, managed_table);

    apply_schema_sql(&db.client, &db.schema, &baseline_table).await?;
    apply_schema_sql(&db.client, &db.schema, &managed_table).await?;

    let max_rows_per_file = env_i64(
        "KOLDSTORE_STORAGE_MAX_ROWS_PER_FILE",
        (rows / 10).max(1_000),
    );

    // Manage before DML so insert/update/delete timings include mirror capture.
    {
        let _step = common::log_step_always("storage_cmp: manage_table");
        manage_with_hot_limit(
            &db.client,
            &db.storage_name,
            &managed,
            hot_limit,
            1,
            max_rows_per_file,
        )
        .await?;
    }

    common::log_always(format!(
        "storage_cmp: seeding {rows} rows (hot_row_limit={hot_limit}, max_rows_per_file={max_rows_per_file})"
    ));
    let baseline_insert = {
        let _step = common::log_step_always(format!("storage_cmp: insert baseline ({rows} rows)"));
        time_insert(&db.client, &baseline, 1, rows).await?
    };
    let managed_insert = {
        let _step = common::log_step_always(format!(
            "storage_cmp: insert managed ({rows} rows, with change-log mirror)"
        ));
        time_insert(&db.client, &managed, 1, rows).await?
    };

    let baseline_update = {
        let _step = common::log_step_always(format!(
            "storage_cmp: update baseline sample ({dml_sample} rows)"
        ));
        time_update(&db.client, &baseline, rows - dml_sample + 1, dml_sample).await?
    };
    let managed_update = {
        let _step = common::log_step_always(format!(
            "storage_cmp: update managed sample ({dml_sample} rows)"
        ));
        time_update(&db.client, &managed, rows - dml_sample + 1, dml_sample).await?
    };

    // Delete from the high end of the seeded range, then re-insert the same
    // keys before flush. Seeding a disjoint range left `dml_sample` tombstones
    // in the mirror; flush-by-oldest-seq then evacuated live hot rows while
    // retaining tombstones whenever `dml_sample` approached `rows`.
    let delete_start = rows - dml_sample + 1;
    let delete_end = rows;
    let baseline_delete = {
        let _step = common::log_step_always("storage_cmp: delete baseline sample");
        time_delete(&db.client, &baseline, delete_start, delete_end).await?
    };
    let managed_delete = {
        let _step = common::log_step_always("storage_cmp: delete managed sample");
        time_delete(&db.client, &managed, delete_start, delete_end).await?
    };
    {
        let _step = common::log_step_always(format!(
            "storage_cmp: restore delete sample ({dml_sample} rows each side)"
        ));
        time_insert(&db.client, &baseline, delete_start, dml_sample).await?;
        time_insert(&db.client, &managed, delete_start, dml_sample).await?;
    }

    // Snapshot bloat / autovacuum pressure after DML, before flush reclaims space.
    // Force the stats collector so n_dead_tup reflects this backend's DML.
    let _ = db
        .client
        .execute("SELECT pg_stat_force_next_flush()", &[])
        .await;
    let baseline_heap = heap_stats(&db.client, &baseline).await?;
    let managed_heap = heap_stats(&db.client, &managed).await?;

    // Fair hot-only comparison: measure PK lookups while every row is still in
    // the PostgreSQL heap on both sides (before any flush). Post-flush "hot"
    // would compare a ~hot_limit managed heap to a full baseline heap.
    let hot_id = rows;
    let cold_id = 1_i64;

    {
        let _step = common::log_step_always("storage_cmp: pre-flush hot-only PK lookups");
        let plan_pre_flush = common::explain(
            &db.client,
            &format!("SELECT id, account_id, event_type FROM {managed} WHERE id = {hot_id}"),
        )
        .await?;
        common::assert_kold_merge_scan_explain(&plan_pre_flush)?;
        anyhow::ensure!(
            plan_pre_flush.contains("Parquet segment: none"),
            "pre-flush PK lookup must not open Parquet (no cold yet), got:\n{plan_pre_flush}"
        );
        assert_point_row_matches(&db.client, &baseline, &managed, hot_id).await?;
        assert_point_row_matches(&db.client, &baseline, &managed, cold_id).await?;
    }

    let baseline_hot = {
        let _step = common::log_step_always("storage_cmp: time baseline hot PK lookups");
        time_point_queries(&db.client, &baseline, hot_id).await?
    };
    let managed_hot = {
        let _step = common::log_step_always("storage_cmp: time managed hot PK lookups");
        time_point_queries(&db.client, &managed, hot_id).await?
    };

    let flushed = {
        let _step = common::log_step_always(format!(
            "storage_cmp: flush_table (expect ~{} cold rows)",
            rows.saturating_sub(hot_limit)
        ));
        flush_table(&db.client, &managed).await?
    };
    anyhow::ensure!(
        flushed > 0,
        "expected KoldStore flush to move rows cold (hot_limit={hot_limit}, rows={rows})"
    );
    common::log_always(format!(
        "storage_cmp: flush_table returned rows_flushed={flushed}"
    ));

    let status = common::describe_table(&db.client, &managed).await?;
    anyhow::ensure!(
        status.hot_rows > 0,
        "expected hot rows to remain after policy flush, got {status:?}"
    );
    anyhow::ensure!(
        status.cold_row_count > 0,
        "expected cold rows after flush, got {status:?}"
    );
    common::log_always(format!(
        "storage_cmp: after flush hot={} cold={} mirror={}",
        status.hot_rows, status.cold_row_count, status.mirror_rows
    ));

    // After flush the managed heap is smaller; time VACUUM FULL as the
    // maintenance-cost comparison, then REINDEX so size numbers are clean.
    let baseline_vacuum = {
        let _step = common::log_step_always("storage_cmp: VACUUM FULL baseline (~full heap)");
        time_vacuum_full(&db.client, &baseline).await?
    };
    let managed_vacuum = {
        let _step = common::log_step_always("storage_cmp: VACUUM FULL managed (hot heap)");
        time_vacuum_full(&db.client, &managed).await?
    };
    {
        let _step = common::log_step_always("storage_cmp: REINDEX + vacuum mirror");
        reindex_relation(&db.client, &baseline).await?;
        reindex_relation(&db.client, &managed).await?;
        if let Some(mirror) = mirror_relation(&managed) {
            time_vacuum_full(&db.client, &mirror).await?;
            reindex_relation(&db.client, &mirror).await?;
        }
    }

    {
        let _step = common::log_step_always("storage_cmp: post-flush hot+cold PK lookups");
        let plan_cold = common::explain(
            &db.client,
            &format!("SELECT id, account_id, event_type FROM {managed} WHERE id = {cold_id}"),
        )
        .await?;
        common::assert_kold_merge_scan_explain(&plan_cold)?;
        common::assert_kold_merge_scan_planned_cold_reads(&plan_cold, "manifest.json", 1)?;
        anyhow::ensure!(
            !plan_cold.contains("Parquet segment: none"),
            "hot+cold PK lookup should open at least one Parquet segment, got:\n{plan_cold}"
        );

        // Correctness after flush: cold-flushed and still-hot PKs must match baseline.
        assert_point_row_matches(&db.client, &baseline, &managed, hot_id).await?;
        assert_point_row_matches(&db.client, &baseline, &managed, cold_id).await?;
        let mid_cold_id = (hot_limit / 2).max(1);
        assert_point_row_matches(&db.client, &baseline, &managed, mid_cold_id).await?;
    }

    let baseline_cold = {
        let _step = common::log_step_always("storage_cmp: time baseline cold-id PK lookups");
        time_point_queries(&db.client, &baseline, cold_id).await?
    };
    let managed_cold = {
        let _step = common::log_step_always("storage_cmp: time managed hot+cold PK lookups");
        time_point_queries(&db.client, &managed, cold_id).await?
    };

    let baseline_sizes = relation_sizes(&db.client, &baseline, None).await?;
    let managed_sizes = relation_sizes(&db.client, &managed, Some(&managed)).await?;

    let baseline_metrics = SideMetrics {
        insert: baseline_insert,
        update: baseline_update,
        delete: baseline_delete,
        query_hot_only: baseline_hot,
        query_hot_cold: baseline_cold,
        vacuum: baseline_vacuum,
        heap_after_workload: baseline_heap,
        sizes: baseline_sizes,
    };
    let managed_metrics = SideMetrics {
        insert: managed_insert,
        update: managed_update,
        delete: managed_delete,
        query_hot_only: managed_hot,
        query_hot_cold: managed_cold,
        vacuum: managed_vacuum,
        heap_after_workload: managed_heap,
        sizes: managed_sizes,
    };

    print_comparison_table(
        rows,
        hot_limit,
        dml_sample,
        max_rows_per_file,
        flushed,
        baseline_metrics,
        managed_metrics,
    );

    anyhow::ensure!(
        managed_sizes.table_bytes < baseline_sizes.table_bytes,
        "expected KoldStore PostgreSQL table footprint smaller after flush: managed={} baseline={}",
        managed_sizes.table_bytes,
        baseline_sizes.table_bytes
    );
    anyhow::ensure!(
        managed_sizes.index_bytes < baseline_sizes.index_bytes,
        "expected KoldStore PostgreSQL index footprint smaller after flush: managed={} baseline={}",
        managed_sizes.index_bytes,
        baseline_sizes.index_bytes
    );
    anyhow::ensure!(
        managed_sizes.cold_bytes > 0,
        "expected positive cold Parquet bytes after flush"
    );

    // Do NOT `SELECT count(*)` through KoldMergeScan here. At multi-million row
    // scale BeginCustomScan still materializes the full hot∪cold result set,
    // which OOMs / closes the session after an otherwise successful run.
    // Point lookups above already checked correctness; catalog counters cover
    // row accounting without opening every Parquet segment.
    let final_status = {
        let _step = common::log_step_always(
            "storage_cmp: verify hot+cold coverage via describe_table (skip full COUNT(*))",
        );
        common::describe_table(&db.client, &managed).await?
    };
    let covered = final_status.hot_rows + final_status.cold_row_count;
    anyhow::ensure!(
        covered >= rows,
        "expected hot+cold coverage of at least {rows} rows after flush, got hot={} cold={} (sum={covered})",
        final_status.hot_rows,
        final_status.cold_row_count
    );
    common::log_always(format!(
        "storage_cmp: coverage ok hot={} cold={} sum={covered} (seeded={rows})",
        final_status.hot_rows, final_status.cold_row_count
    ));

    Ok(())
}

fn env_i64(name: &str, default: i64) -> i64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn mirror_relation(managed: &str) -> Option<String> {
    managed
        .rsplit('.')
        .next()
        .map(|table| format!("koldstore.{table}__cl"))
}

fn schema_sql_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("schema.sql")
}

async fn apply_schema_sql(client: &Client, schema: &str, table: &str) -> Result<()> {
    let template = std::fs::read_to_string(schema_sql_path())
        .with_context(|| format!("read {}", schema_sql_path().display()))?;
    let sql = template
        .replace("{{schema}}", schema)
        .replace("{{table}}", table);
    client
        .batch_execute(&sql)
        .await
        .with_context(|| format!("apply schema.sql for {schema}.{table}"))?;
    Ok(())
}

async fn manage_with_hot_limit(
    client: &Client,
    storage: &str,
    relation: &str,
    hot_row_limit: i64,
    min_flush_rows: i64,
    max_rows_per_file: i64,
) -> Result<()> {
    client
        .execute(
            r#"
            SELECT koldstore.manage_table(
              table_name        => $1::text::regclass,
              storage           => $2,
              hot_row_limit     => $3,
              min_flush_rows    => $4,
              max_rows_per_file => $5,
              migration_order_by => 'id',
              compression       => 'zstd'
            )
            "#,
            &[
                &relation,
                &storage,
                &hot_row_limit,
                &min_flush_rows,
                &max_rows_per_file,
            ],
        )
        .await
        .with_context(|| format!("manage_table {relation}"))?;
    Ok(())
}

async fn flush_table(client: &Client, relation: &str) -> Result<i64> {
    let row = client
        .query_one(
            "SELECT koldstore.flush_table($1::text::regclass)::text",
            &[&relation],
        )
        .await?;
    let job_id: String = row.get(0);
    let progress = client
        .query_one(
            "SELECT rows_flushed FROM koldstore.jobs WHERE id = $1::text::uuid",
            &[&job_id],
        )
        .await?;
    Ok(progress.get(0))
}

async fn time_vacuum_full(client: &Client, relation: &str) -> Result<Timing> {
    let sql = format!("VACUUM (FULL, ANALYZE) {relation}");
    let started = Instant::now();
    client
        .execute(&sql, &[])
        .await
        .with_context(|| format!("VACUUM failed for {relation}"))?;
    Ok(Timing {
        elapsed: started.elapsed(),
        ops: 1,
    })
}

async fn reindex_relation(client: &Client, relation: &str) -> Result<()> {
    let sql = format!("REINDEX TABLE {relation}");
    client
        .execute(&sql, &[])
        .await
        .with_context(|| format!("REINDEX failed for {relation}"))?;
    Ok(())
}

/// Reads live/dead tuple counters for one user table.
///
/// Call after DML (and `pg_stat_force_next_flush`) so `pg_stat_user_tables`
/// reflects this backend's workload. Pre-flush, both sides should show similar
/// `n_dead_tup` for the same DML sample; the maintenance win shows up in
/// post-flush `VACUUM` time on the smaller managed heap.
async fn heap_stats(client: &Client, relation: &str) -> Result<HeapStats> {
    let row = client
        .query_one(
            r#"
            SELECT
              COALESCE(s.n_live_tup, 0)::bigint,
              COALESCE(s.n_dead_tup, 0)::bigint
            FROM pg_stat_user_tables s
            WHERE s.schemaname = split_part($1, '.', 1)
              AND s.relname = split_part($1, '.', 2)
            "#,
            &[&relation],
        )
        .await
        .with_context(|| format!("pg_stat_user_tables for {relation}"))?;
    Ok(HeapStats {
        n_live_tup: row.get(0),
        n_dead_tup: row.get(1),
    })
}

async fn relation_sizes(
    client: &Client,
    relation: &str,
    managed_relation: Option<&str>,
) -> Result<Sizes> {
    let row = client
        .query_one(
            r#"
            SELECT
              pg_table_size($1::text::regclass)::bigint,
              pg_indexes_size($1::text::regclass)::bigint
            "#,
            &[&relation],
        )
        .await?;
    let mut table_bytes: i64 = row.get(0);
    let mut index_bytes: i64 = row.get(1);
    let mut cold_bytes = 0_i64;

    if let Some(managed) = managed_relation {
        if let Some(mirror) = mirror_relation(managed) {
            let mirror_row = client
                .query_one(
                    r#"
                    SELECT
                      pg_table_size($1::text::regclass)::bigint,
                      pg_indexes_size($1::text::regclass)::bigint
                    "#,
                    &[&mirror],
                )
                .await
                .with_context(|| format!("size mirror {mirror}"))?;
            table_bytes += mirror_row.get::<_, i64>(0);
            index_bytes += mirror_row.get::<_, i64>(1);
        }

        let cold = client
            .query_one(
                r#"
                SELECT COALESCE(sum(byte_size), 0)::bigint
                FROM koldstore.cold_segments
                WHERE table_oid = $1::text::regclass::oid
                  AND status = 'active'
                "#,
                &[&managed],
            )
            .await?;
        cold_bytes = cold.get(0);
    }

    Ok(Sizes {
        table_bytes,
        index_bytes,
        cold_bytes,
    })
}

fn seed_select_sql(start_id: i64, count: i64) -> String {
    let end_id = start_id + count - 1;
    format!(
        r#"
        SELECT
          gs AS id,
          gs % 1024 AS account_id,
          'tenant-' || ((gs % 64)::text) AS tenant_id,
          CASE gs % 5
            WHEN 0 THEN 'click'
            WHEN 1 THEN 'view'
            WHEN 2 THEN 'purchase'
            WHEN 3 THEN 'signup'
            ELSE 'heartbeat'
          END AS event_type,
          CASE gs % 4
            WHEN 0 THEN 'open'
            WHEN 1 THEN 'closed'
            WHEN 2 THEN 'pending'
            ELSE 'archived'
          END AS status,
          (gs % 10)::integer AS priority,
          (gs % 1000)::float8 / 10.0 AS score,
          (gs % 100000)::bigint AS amount_cents,
          (gs % 50)::integer AS quantity,
          (gs % 2 = 0) AS is_active,
          false AS is_deleted,
          'region-' || ((gs % 8)::text) AS region,
          'country-' || ((gs % 32)::text) AS country,
          'city-' || ((gs % 128)::text) AS city,
          CASE gs % 3 WHEN 0 THEN 'web' WHEN 1 THEN 'mobile' ELSE 'api' END AS channel,
          'source-' || ((gs % 16)::text) AS source,
          'campaign-' || ((gs % 20)::text) AS campaign,
          'device-' || ((gs % 12)::text) AS device,
          CASE gs % 3 WHEN 0 THEN 'ios' WHEN 1 THEN 'android' ELSE 'linux' END AS os_name,
          '1.' || ((gs % 9)::text) || '.0' AS app_version,
          'sess-' || lpad((gs % 10000)::text, 8, '0') AS session_id,
          'req-' || lpad(gs::text, 12, '0') AS request_id,
          'trace-' || lpad((gs % 100000)::text, 10, '0') AS trace_id,
          'Mozilla/5.0 row-' || gs::text AS user_agent,
          '10.' || ((gs / 65536) % 256)::text || '.' || ((gs / 256) % 256)::text || '.' || (gs % 256)::text AS ip_address,
          'https://example.com/ref/' || (gs % 100)::text AS referrer,
          '/api/v1/events/' || (gs % 50)::text AS path,
          CASE gs % 4 WHEN 0 THEN 'GET' WHEN 1 THEN 'POST' WHEN 2 THEN 'PUT' ELSE 'DELETE' END AS method,
          (200 + (gs % 20))::integer AS response_code,
          (gs % 500)::integer AS latency_ms,
          (100 + (gs % 9000))::integer AS payload_bytes,
          CASE WHEN gs % 17 = 0 THEN 'E' || (gs % 10)::text ELSE NULL END AS error_code,
          CASE WHEN gs % 17 = 0 THEN 'synthetic error ' || gs::text ELSE NULL END AS error_message,
          'tag-a-' || ((gs % 7)::text) AS tag_a,
          'tag-b-' || ((gs % 11)::text) AS tag_b,
          'tag-c-' || ((gs % 13)::text) AS tag_c,
          'tag-d-' || ((gs % 17)::text) AS tag_d,
          'tag-e-' || ((gs % 19)::text) AS tag_e,
          (gs % 100)::float8 AS metric_1,
          (gs % 200)::float8 / 2.0 AS metric_2,
          (gs % 300)::float8 / 3.0 AS metric_3,
          (gs % 400)::float8 / 4.0 AS metric_4,
          (gs % 500)::float8 / 5.0 AS metric_5,
          (gs % 2 = 0) AS flag_1,
          (gs % 3 = 0) AS flag_2,
          (gs % 5 = 0) AS flag_3,
          (gs % 7 = 0) AS flag_4,
          (gs % 11 = 0) AS flag_5,
          'note-1-' || lpad((gs % 1000)::text, 4, '0') AS note_1,
          'note-2-' || lpad((gs % 2000)::text, 4, '0') AS note_2,
          'note-3-' || lpad((gs % 3000)::text, 4, '0') AS note_3,
          'note-4 payload for row ' || gs::text AS note_4,
          'note-5 longer text body for compression demo row ' || gs::text AS note_5,
          timestamptz '2024-01-01 00:00:00+00' + ((gs % 86400) * interval '1 second') AS created_at,
          timestamptz '2024-01-01 00:00:00+00' + ((gs % 86400) * interval '1 second') AS updated_at
        FROM generate_series({start_id}, {end_id}) AS gs
        "#
    )
}

async fn time_insert(client: &Client, relation: &str, start_id: i64, count: i64) -> Result<Timing> {
    let select = seed_select_sql(start_id, count);
    let started = Instant::now();
    client
        .execute(
            &format!(
                r#"
                INSERT INTO {relation} (
                  id, account_id, tenant_id, event_type, status, priority, score,
                  amount_cents, quantity, is_active, is_deleted, region, country, city,
                  channel, source, campaign, device, os_name, app_version, session_id,
                  request_id, trace_id, user_agent, ip_address, referrer, path, method,
                  response_code, latency_ms, payload_bytes, error_code, error_message,
                  tag_a, tag_b, tag_c, tag_d, tag_e,
                  metric_1, metric_2, metric_3, metric_4, metric_5,
                  flag_1, flag_2, flag_3, flag_4, flag_5,
                  note_1, note_2, note_3, note_4, note_5, created_at, updated_at
                )
                {select}
                "#
            ),
            &[],
        )
        .await
        .with_context(|| format!("insert into {relation}"))?;
    Ok(Timing {
        elapsed: started.elapsed(),
        ops: count,
    })
}

async fn time_update(client: &Client, relation: &str, start_id: i64, count: i64) -> Result<Timing> {
    let end_id = start_id + count - 1;
    let started = Instant::now();
    client
        .execute(
            &format!(
                r#"
                UPDATE {relation}
                SET note_5 = note_5 || ' updated',
                    updated_at = now()
                WHERE id BETWEEN {start_id} AND {end_id}
                "#
            ),
            &[],
        )
        .await
        .with_context(|| format!("update {relation}"))?;
    Ok(Timing {
        elapsed: started.elapsed(),
        ops: count,
    })
}

async fn time_delete(
    client: &Client,
    relation: &str,
    start_id: i64,
    end_id: i64,
) -> Result<Timing> {
    let ops = end_id - start_id + 1;
    let started = Instant::now();
    client
        .execute(
            &format!("DELETE FROM {relation} WHERE id BETWEEN {start_id} AND {end_id}"),
            &[],
        )
        .await
        .with_context(|| format!("delete from {relation}"))?;
    Ok(Timing {
        elapsed: started.elapsed(),
        ops,
    })
}

async fn assert_point_row_matches(
    client: &Client,
    baseline: &str,
    managed: &str,
    id: i64,
) -> Result<()> {
    let sql = |relation: &str| {
        format!(
            "SELECT id::text, account_id::text, event_type::text, note_5::text, \
                    amount_cents::text, is_active::text \
             FROM {relation} WHERE id = {id}"
        )
    };
    let baseline_row = client
        .query_one(&sql(baseline), &[])
        .await
        .with_context(|| format!("baseline point row {baseline} id={id}"))?;
    let managed_row = client
        .query_one(&sql(managed), &[])
        .await
        .with_context(|| format!("managed point row {managed} id={id}"))?;
    for (index, column) in [
        "id",
        "account_id",
        "event_type",
        "note_5",
        "amount_cents",
        "is_active",
    ]
    .into_iter()
    .enumerate()
    {
        let baseline_text: String = baseline_row.get(index);
        let managed_text: String = managed_row.get(index);
        anyhow::ensure!(
            baseline_text == managed_text,
            "point lookup mismatch for id={id} column={column}: baseline={baseline_text} managed={managed_text}"
        );
    }
    Ok(())
}

async fn time_point_queries(client: &Client, relation: &str, id: i64) -> Result<Timing> {
    // Use a SQL literal (not `$1`) so timings match EXPLAIN and exercise cold
    // prune + hot equality pushdown the same way applications with Const quals do.
    // Parameterized `$1` is also supported via ParamListInfo resolution.
    let sql = format!("SELECT id, account_id, event_type, note_5 FROM {relation} WHERE id = {id}");
    let started = Instant::now();
    for _ in 0..QUERY_LOOPS {
        let _ = client
            .query_one(&sql, &[])
            .await
            .with_context(|| format!("point query {relation} id={id}"))?;
    }
    Ok(Timing {
        elapsed: started.elapsed(),
        ops: QUERY_LOOPS as i64,
    })
}

fn print_comparison_table(
    rows: i64,
    hot_limit: i64,
    dml_sample: i64,
    max_rows_per_file: i64,
    flushed: i64,
    baseline: SideMetrics,
    managed: SideMetrics,
) {
    let table_win = storage_win_pct(baseline.sizes.table_bytes, managed.sizes.table_bytes);
    let index_win = storage_win_pct(baseline.sizes.index_bytes, managed.sizes.index_bytes);
    let total_baseline = baseline.sizes.table_bytes + baseline.sizes.index_bytes;
    let total_managed = managed.sizes.table_bytes + managed.sizes.index_bytes;
    let total_win = storage_win_pct(total_baseline, total_managed);

    println!();
    println!("## pg vs KoldStore comparison");
    println!();
    println!(
        "schema=tests/storage/schema.sql rows={rows} hot_row_limit={hot_limit} \
         dml_sample={dml_sample} max_rows_per_file={max_rows_per_file} flushed={flushed} \
         compression=zstd"
    );
    println!();
    println!("| Operation | PostgreSQL only | PostgreSQL + KoldStore | Storage win |");
    println!("| --- | --- | --- | --- |");
    println!(
        "| insert speed† | {} | {} | — |",
        format_speed(baseline.insert),
        format_speed(managed.insert)
    );
    println!(
        "| update speed† | {} | {} | — |",
        format_speed(baseline.update),
        format_speed(managed.update)
    );
    println!(
        "| delete speed† | {} | {} | — |",
        format_speed(baseline.delete),
        format_speed(managed.delete)
    );
    println!(
        "| query hot only (before flush) | {} | {} | — |",
        format_speed(baseline.query_hot_only),
        format_speed(managed.query_hot_only)
    );
    println!(
        "| query with hot+cold (after flush) | {} | {} | — |",
        format_speed(baseline.query_hot_cold),
        format_speed(managed.query_hot_cold)
    );
    println!(
        "| VACUUM time (after flush) | {} | {} | {} |",
        format_duration(baseline.vacuum.elapsed),
        format_duration(managed.vacuum.elapsed),
        duration_win_pct(baseline.vacuum.elapsed, managed.vacuum.elapsed)
    );
    println!(
        "| dead tuples after workload | {} (live={}) | {} (live={}) | — |",
        format_count(baseline.heap_after_workload.n_dead_tup),
        format_count(baseline.heap_after_workload.n_live_tup),
        format_count(managed.heap_after_workload.n_dead_tup),
        format_count(managed.heap_after_workload.n_live_tup),
    );
    println!(
        "| index storage | {} | {} | {index_win} |",
        format_bytes(baseline.sizes.index_bytes),
        format_bytes(managed.sizes.index_bytes)
    );
    println!(
        "| table storage | {} | {}{} | {table_win} |",
        format_bytes(baseline.sizes.table_bytes),
        format_bytes(managed.sizes.table_bytes),
        if managed.sizes.cold_bytes > 0 {
            format!(
                " (+ {} cold Parquet)",
                format_bytes(managed.sizes.cold_bytes)
            )
        } else {
            String::new()
        }
    );
    println!("| total PG backup size | TODO | TODO | — |");
    println!("| restore time | TODO | TODO | — |");
    println!();
    println!(
        "PostgreSQL heap+index after flush: baseline={} managed={} ({total_win} smaller)",
        format_bytes(total_baseline),
        format_bytes(total_managed),
    );
    println!();
    println!(
        "† DML is expected to be slower under KoldStore: each statement also updates the \
         change-log mirror (`koldstore.<table>__cl`). That is the cost of transparent flush \
         and change cursors; the win shows up in PostgreSQL heap/index size after flush. \
         Hot-only timings are taken before flush so both heaps still hold all {rows} rows \
         (fair merge-scan overhead). Hot+cold timings and VACUUM are after flush. \
         Dead tuples are snapshotted after DML via `pg_stat_user_tables` (pre-flush, so \
         both sides match for the same update/delete sample). Autovacuum counters are \
         omitted: this harness is too short for autovacuum to run, so they would read \
         as zeros on both sides."
    );
    println!();
}

fn storage_win_pct(baseline: i64, managed: i64) -> String {
    if baseline <= 0 {
        return "n/a".to_string();
    }
    let saved = baseline.saturating_sub(managed).max(0) as f64;
    format!("{:.0}%", 100.0 * saved / baseline as f64)
}

fn duration_win_pct(baseline: Duration, managed: Duration) -> String {
    let baseline_us = baseline.as_micros() as i64;
    let managed_us = managed.as_micros() as i64;
    storage_win_pct(baseline_us, managed_us)
}

fn format_speed(timing: Timing) -> String {
    format!(
        "{:.0} ops/s ({:.0} µs/op)",
        timing.ops_per_sec(),
        timing.per_op_us()
    )
}

fn format_duration(elapsed: Duration) -> String {
    let ms = elapsed.as_secs_f64() * 1000.0;
    if ms >= 1000.0 {
        format!("{:.2} s", elapsed.as_secs_f64())
    } else {
        format!("{ms:.1} ms")
    }
}

fn format_count(value: i64) -> String {
    format!("{value}")
}

fn format_bytes(bytes: i64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
    let value = bytes as f64;
    if value >= GIB {
        format!("{:.2} GiB", value / GIB)
    } else if value >= MIB {
        format!("{:.2} MiB", value / MIB)
    } else if value >= KIB {
        format!("{:.1} KiB", value / KIB)
    } else {
        format!("{bytes} B")
    }
}
