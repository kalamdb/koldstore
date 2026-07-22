use crate::common;

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use koldstore_manifest::Manifest;
use tokio_postgres::Client;

const INITIAL_ROWS: i64 = 2_000;
const SECOND_INSERT_ROWS: i64 = 2_000;
const TOTAL_ROWS: i64 = INITIAL_ROWS + SECOND_INSERT_ROWS;
const FLUSH_POLICY_ROW_LIMIT: i64 = 100;
const SOURCE_COLUMN_COUNT: usize = 50;
const MAX_FLUSH_SECONDS: f64 = 120.0;

#[tokio::test]
async fn full_lifecycle_wide_table_migrates_flushes_in_batches_and_queries_all_rows() -> Result<()>
{
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        common::log_always(format!(
            "full_lifecycle starting pg{} on port {}",
            target.version, target.port
        ));

        let server = common::PgrxServer::start(target).await?;
        let storage_root = storage_root_for(server.target.version)?;
        run_full_lifecycle(&server.client, server.target.version, &storage_root).await?;
    }

    Ok(())
}

fn storage_root_for(pg_version: u16) -> Result<PathBuf> {
    let root = std::env::temp_dir().join(format!(
        "pg-koldstore-full-lifecycle-{}-{pg_version}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).with_context(|| format!("create {}", root.display()))?;
    Ok(root)
}

async fn run_full_lifecycle(client: &Client, pg_version: u16, storage_root: &Path) -> Result<()> {
    let scenario_started = Instant::now();
    let relation = relation(pg_version);
    let mirror = mirror_relation(pg_version);

    {
        let _step = common::log_step_always(format!("pg{pg_version}: install extension"));
        install_extension(client).await?;
    }
    {
        let _step = common::log_step_always(format!(
            "pg{pg_version}: create wide source table ({SOURCE_COLUMN_COUNT} columns)"
        ));
        create_wide_source_table(client, pg_version).await?;
    }
    {
        let _step = common::log_step_always(format!(
            "pg{pg_version}: insert initial {INITIAL_ROWS} rows"
        ));
        insert_rows(client, pg_version, 1, INITIAL_ROWS).await?;
    }
    {
        let _step = common::log_step_always(format!("pg{pg_version}: verify initial row count"));
        assert_eq!(common::row_count(client, &relation).await?, INITIAL_ROWS);
    }
    {
        let _step = common::log_step_always(format!("pg{pg_version}: register filesystem storage"));
        register_storage(client, storage_root, pg_version).await?;
    }
    {
        let _step = common::log_step_always(format!(
            "pg{pg_version}: manage table with hot_row_limit {FLUSH_POLICY_ROW_LIMIT}"
        ));
        manage_table(client, pg_version).await?;
    }
    {
        let _step = common::log_step_always(format!("pg{pg_version}: wait for migration jobs"));
        wait_for_jobs_to_finish(client, pg_version).await?;
    }
    {
        let _step =
            common::log_step_always(format!("pg{pg_version}: verify change-log mirror shape"));
        assert_change_log_mirror(client, pg_version, INITIAL_ROWS).await?;
    }
    {
        let _step = common::log_step_always(format!("pg{pg_version}: verify flush policy stored"));
        assert_hot_row_limit_registered(client, pg_version).await?;
    }

    let first_flush_started = Instant::now();
    {
        let _step = common::log_step_always(format!("pg{pg_version}: enqueue first flush job"));
        enqueue_flush_job(client, pg_version).await?;
    }
    {
        let _step = common::log_step_always(format!("pg{pg_version}: run first flush"));
        let flushed = flush_table(client, pg_version).await?;
        assert_eq!(flushed, INITIAL_ROWS);
    }
    {
        let _step = common::log_step_always(format!("pg{pg_version}: wait for first flush jobs"));
        wait_for_jobs_to_finish(client, pg_version).await?;
    }
    let first_flush_elapsed = first_flush_started.elapsed();
    assert!(
        first_flush_elapsed.as_secs_f64() < MAX_FLUSH_SECONDS,
        "first flush took {:.3}s, expected < {MAX_FLUSH_SECONDS}s",
        first_flush_elapsed.as_secs_f64()
    );
    common::log_always(format!(
        "pg{pg_version}: first flush wall time {:.3}s",
        first_flush_elapsed.as_secs_f64()
    ));

    let first_batches = {
        let _step = common::log_step_always(format!("pg{pg_version}: verify first flush batches"));
        assert_flush_jobs_recorded(client, pg_version, 1).await?;
        let batches = fetch_cold_flush_batches(client, pg_version).await?;
        assert_flush_completed_in_batches(&batches, INITIAL_ROWS, 1, FLUSH_POLICY_ROW_LIMIT, true)?;
        common::assert_flush_pruned_hot_storage(client, &relation, INITIAL_ROWS).await?;
        assert_eq!(common::hot_row_count(client, &relation).await?, 0);
        assert_eq!(common::row_count(client, &mirror).await?, 0);
        batches
    };
    let first_manifest = {
        let _step =
            common::log_step_always(format!("pg{pg_version}: verify first flush manifest.json"));
        load_manifest(client, pg_version, storage_root).await?
    };
    assert!(
        !first_manifest.segments.is_empty(),
        "manifest should list at least one parquet segment after first flush"
    );
    assert_eq!(
        first_manifest
            .segments
            .iter()
            .map(|segment| segment.row_count)
            .sum::<u64>(),
        INITIAL_ROWS as u64
    );

    {
        let _step = common::log_step_always(format!(
            "pg{pg_version}: insert second batch of {SECOND_INSERT_ROWS} rows"
        ));
        insert_rows(client, pg_version, INITIAL_ROWS + 1, SECOND_INSERT_ROWS).await?;
        common::fence_async_mirror_if_needed(client).await?;
    }
    {
        let _step = common::log_step_always(format!(
            "pg{pg_version}: verify row count before second flush"
        ));
        let status = common::assert_cold_rows_at_least(client, &relation, INITIAL_ROWS).await?;
        assert_eq!(
            status.hot_rows, SECOND_INSERT_ROWS,
            "second insert batch should remain on hot heap until second flush"
        );
        assert_eq!(
            status.mirror_rows, SECOND_INSERT_ROWS,
            "mirror should track only rows inserted after first flush prune"
        );
        assert_eq!(
            common::hot_row_count(client, &relation).await?,
            SECOND_INSERT_ROWS
        );
        assert_eq!(
            common::row_count(client, &mirror).await?,
            SECOND_INSERT_ROWS
        );
    }

    let second_flush_started = Instant::now();
    {
        let _step = common::log_step_always(format!("pg{pg_version}: enqueue second flush job"));
        enqueue_flush_job(client, pg_version).await?;
    }
    {
        let _step = common::log_step_always(format!("pg{pg_version}: run second flush"));
        let flushed = flush_table(client, pg_version).await?;
        assert_eq!(flushed, SECOND_INSERT_ROWS);
    }
    {
        let _step = common::log_step_always(format!("pg{pg_version}: wait for second flush jobs"));
        wait_for_jobs_to_finish(client, pg_version).await?;
    }
    let second_flush_elapsed = second_flush_started.elapsed();
    assert!(
        second_flush_elapsed.as_secs_f64() < MAX_FLUSH_SECONDS,
        "second flush took {:.3}s, expected < {MAX_FLUSH_SECONDS}s",
        second_flush_elapsed.as_secs_f64()
    );
    common::log_always(format!(
        "pg{pg_version}: second flush wall time {:.3}s",
        second_flush_elapsed.as_secs_f64()
    ));

    let second_batches = {
        let _step = common::log_step_always(format!("pg{pg_version}: verify second flush batches"));
        assert_flush_jobs_recorded(client, pg_version, 2).await?;
        let batches = fetch_cold_flush_batches(client, pg_version).await?;
        assert_flush_completed_in_batches(&batches, TOTAL_ROWS, 2, FLUSH_POLICY_ROW_LIMIT, false)?;
        common::assert_flush_pruned_hot_storage(client, &relation, TOTAL_ROWS).await?;
        assert_eq!(common::hot_row_count(client, &relation).await?, 0);
        assert_eq!(common::row_count(client, &mirror).await?, 0);
        batches
    };
    assert!(
        second_batches.segment_count >= first_batches.segment_count,
        "second flush should retain or extend cold segment batches"
    );

    let second_manifest = {
        let _step =
            common::log_step_always(format!("pg{pg_version}: verify updated manifest.json"));
        let manifest = load_manifest(client, pg_version, storage_root).await?;
        assert!(
            manifest.segments.len() >= first_manifest.segments.len(),
            "manifest segment list should grow after second flush"
        );
        assert!(
            manifest.max_seq >= first_manifest.max_seq,
            "manifest max_seq should advance after second flush"
        );
        let manifest_rows = manifest
            .segments
            .iter()
            .map(|segment| segment.row_count)
            .sum::<u64>();
        assert!(
            manifest_rows >= TOTAL_ROWS as u64,
            "manifest should account for all flushed rows, got {manifest_rows}"
        );
        manifest
    };

    {
        let _step = common::log_step_always(format!(
            "pg{pg_version}: verify cold storage covers all {TOTAL_ROWS} flushed rows"
        ));
        let status = common::describe_table(client, &relation).await?;
        assert_eq!(status.hot_rows, 0);
        assert_eq!(status.mirror_rows, 0);
        assert!(
            status.cold_row_count >= TOTAL_ROWS,
            "cold segments should account for all flushed rows, got {:?}",
            status
        );
        assert_cold_parquet_sample_readable(client, pg_version, storage_root).await?;
    }
    {
        let _step = common::log_step_always(format!(
            "pg{pg_version}: verify merged SELECT returns all {TOTAL_ROWS} rows"
        ));
        assert_sample_rows_readable(client, pg_version).await?;
        let status = common::describe_table(client, &relation).await?;
        assert_eq!(status.hot_rows, 0, "merged read should see pruned hot heap");
        assert_eq!(
            status.mirror_rows, 0,
            "merged read should see pruned mirror"
        );
        assert!(
            status.cold_row_count >= TOTAL_ROWS,
            "describe_table cold rows should cover flushed data, got {:?}",
            status
        );
    }
    {
        let _step = common::log_step_always(format!(
            "pg{pg_version}: verify KoldMergeScan planned cold reads in EXPLAIN"
        ));
        let plan = common::explain(client, &format!("SELECT count(*) FROM {relation}")).await?;
        common::assert_kold_merge_scan_cold_reads(&plan, "manifest.json", 2)?;
    }
    {
        let _step = common::log_step_always(format!(
            "pg{pg_version}: verify KoldMergeScan executed cold reads in EXPLAIN ANALYZE"
        ));
        let plan =
            common::explain_analyze(client, &format!("SELECT count(*) FROM {relation}")).await?;
        common::assert_kold_merge_scan_executed_cold_reads(&plan, 2)?;
    }
    {
        let _step = common::log_step_always(format!("pg{pg_version}: verify no pending jobs"));
        common::assert_no_active_jobs(client, &relation).await?;
    }

    common::log_always(format!(
        "full_lifecycle completed pg{pg_version}: rows={TOTAL_ROWS}, \
         first_flush_segments={}, second_flush_segments={}, \
         mirror={}, storage={}, elapsed={:.3}s",
        first_batches.segment_count,
        second_batches.segment_count,
        mirror,
        storage_root.display(),
        scenario_started.elapsed().as_secs_f64()
    ));

    let _ = second_manifest;
    Ok(())
}

async fn install_extension(client: &Client) -> Result<()> {
    client
        .batch_execute("CREATE EXTENSION IF NOT EXISTS koldstore;")
        .await?;
    Ok(())
}

async fn register_storage(client: &Client, storage_root: &Path, pg_version: u16) -> Result<()> {
    let base_path = storage_root
        .to_str()
        .context("filesystem storage path must be valid utf-8")?;
    client
        .execute(
            r#"
            SELECT koldstore.register_storage(
              $1,
              'filesystem',
              $2,
              '{}'::jsonb,
              '{}'::jsonb
            )
            "#,
            &[&storage_name(pg_version), &base_path],
        )
        .await?;
    Ok(())
}

fn wide_table_ddl(pg_version: u16) -> String {
    let relation = relation(pg_version);
    let mut ddl = format!(
        r#"
        CREATE SCHEMA IF NOT EXISTS lifecycle;
        DROP TABLE IF EXISTS {relation};

        CREATE TABLE {relation} (
          tenant_id integer NOT NULL,
          id bigint GENERATED BY DEFAULT AS IDENTITY,
          c_bool boolean NOT NULL,
          c_int2 smallint NOT NULL,
          c_int4 integer NOT NULL,
          c_int8 bigint NOT NULL,
          c_float4 real NOT NULL,
          c_float8 double precision NOT NULL,
          c_text text NOT NULL,
          c_varchar varchar(64) NOT NULL,
          c_uuid uuid NOT NULL,
          c_jsonb jsonb NOT NULL,
          c_timestamptz timestamptz NOT NULL,
          c_bool_null boolean,
          c_int2_null smallint,
          c_int4_null integer,
          c_int8_null bigint,
          c_float4_null real,
          c_float8_null double precision,
          c_text_null text,
          c_varchar_null varchar(64),
          c_uuid_null uuid,
          c_jsonb_null jsonb,
          c_timestamptz_null timestamptz,
        "#
    );

    for index in 1..=26 {
        ddl.push_str(&format!("  pad_text_{index:02} text NOT NULL,\n"));
    }

    ddl.push_str(&format!(
        r#"
          PRIMARY KEY (tenant_id, id)
        );
        CREATE INDEX full_lifecycle_text_idx_pg{pg_version} ON {relation} (c_text);
        CREATE INDEX full_lifecycle_ts_idx_pg{pg_version} ON {relation} (c_timestamptz);
        "#
    ));
    ddl
}

async fn create_wide_source_table(client: &Client, pg_version: u16) -> Result<()> {
    client.batch_execute(&wide_table_ddl(pg_version)).await?;

    let columns = relation_columns(client, &relation(pg_version)).await?;
    assert_eq!(
        columns.len(),
        SOURCE_COLUMN_COUNT,
        "expected {SOURCE_COLUMN_COUNT} user columns, got {columns:?}"
    );

    Ok(())
}

async fn insert_rows(
    client: &Client,
    pg_version: u16,
    series_start: i64,
    row_count: i64,
) -> Result<()> {
    let relation = relation(pg_version);
    let series_end = series_start + row_count - 1;
    client
        .batch_execute(&format!(
            r#"
            INSERT INTO {relation} (
              tenant_id,
              c_bool,
              c_int2,
              c_int4,
              c_int8,
              c_float4,
              c_float8,
              c_text,
              c_varchar,
              c_uuid,
              c_jsonb,
              c_timestamptz,
              c_bool_null,
              c_int2_null,
              c_int4_null,
              c_int8_null,
              c_float4_null,
              c_float8_null,
              c_text_null,
              c_varchar_null,
              c_uuid_null,
              c_jsonb_null,
              c_timestamptz_null,
              pad_text_01, pad_text_02, pad_text_03, pad_text_04, pad_text_05,
              pad_text_06, pad_text_07, pad_text_08, pad_text_09, pad_text_10,
              pad_text_11, pad_text_12, pad_text_13, pad_text_14, pad_text_15,
              pad_text_16, pad_text_17, pad_text_18, pad_text_19, pad_text_20,
              pad_text_21, pad_text_22, pad_text_23, pad_text_24, pad_text_25,
              pad_text_26
            )
            SELECT
              ((gs - 1) % 10)::integer + 1 AS tenant_id,
              (gs % 2) = 0,
              (gs % 32000)::smallint,
              (gs % 1000000)::integer,
              gs::bigint,
              (gs % 10000)::real / 100.0,
              (gs % 10000)::double precision / 100.0,
              'text-' || gs::text,
              'varchar-' || lpad(gs::text, 6, '0'),
              md5(gs::text)::uuid,
              jsonb_build_object('row', gs, 'tenant', ((gs - 1) % 10) + 1),
              timestamptz '2020-01-01' + (gs || ' seconds')::interval,
              CASE WHEN gs % 5 = 0 THEN NULL ELSE (gs % 2) = 1 END,
              CASE WHEN gs % 5 = 0 THEN NULL ELSE (gs % 100)::smallint END,
              CASE WHEN gs % 5 = 0 THEN NULL ELSE (gs % 1000)::integer END,
              CASE WHEN gs % 5 = 0 THEN NULL ELSE gs::bigint END,
              CASE WHEN gs % 5 = 0 THEN NULL ELSE (gs % 1000)::real / 10.0 END,
              CASE WHEN gs % 5 = 0 THEN NULL ELSE (gs % 1000)::double precision / 10.0 END,
              CASE WHEN gs % 5 = 0 THEN NULL ELSE 'null-text-' || gs::text END,
              CASE WHEN gs % 5 = 0 THEN NULL ELSE 'null-varchar-' || gs::text END,
              CASE WHEN gs % 5 = 0 THEN NULL ELSE md5('null-' || gs::text)::uuid END,
              CASE WHEN gs % 5 = 0 THEN NULL ELSE jsonb_build_object('nullable', gs) END,
              CASE WHEN gs % 5 = 0 THEN NULL ELSE timestamptz '2021-01-01' + (gs || ' seconds')::interval END,
              'pad-01-' || gs::text,
              'pad-02-' || gs::text,
              'pad-03-' || gs::text,
              'pad-04-' || gs::text,
              'pad-05-' || gs::text,
              'pad-06-' || gs::text,
              'pad-07-' || gs::text,
              'pad-08-' || gs::text,
              'pad-09-' || gs::text,
              'pad-10-' || gs::text,
              'pad-11-' || gs::text,
              'pad-12-' || gs::text,
              'pad-13-' || gs::text,
              'pad-14-' || gs::text,
              'pad-15-' || gs::text,
              'pad-16-' || gs::text,
              'pad-17-' || gs::text,
              'pad-18-' || gs::text,
              'pad-19-' || gs::text,
              'pad-20-' || gs::text,
              'pad-21-' || gs::text,
              'pad-22-' || gs::text,
              'pad-23-' || gs::text,
              'pad-24-' || gs::text,
              'pad-25-' || gs::text,
              'pad-26-' || gs::text
            FROM generate_series({series_start}, {series_end}) AS gs;
            ANALYZE {relation};
            "#
        ))
        .await?;
    Ok(())
}

async fn manage_table(client: &Client, pg_version: u16) -> Result<()> {
    let mode = common::selected_mirror_capture_mode()?.as_str();
    client
        .execute(
            r#"
            SELECT koldstore.manage_table(
              table_name     => $1::text::regclass,
              storage        => $2,
              hot_row_limit  => $3,
              min_flush_rows => 1,
              migration_order_by => 'id',
              auto_flush => false,
              mirror_capture_mode => $4
            )
            "#,
            &[
                &relation(pg_version),
                &storage_name(pg_version),
                &FLUSH_POLICY_ROW_LIMIT,
                &mode,
            ],
        )
        .await?;
    Ok(())
}

async fn enqueue_flush_job(client: &Client, pg_version: u16) -> Result<()> {
    let inserted = client
        .query_one(
            "SELECT koldstore.enqueue_flush_job(table_name => $1::text::regclass, force => true)",
            &[&relation(pg_version)],
        )
        .await?
        .get::<_, i64>(0);
    assert_eq!(inserted, 1, "expected a new pending flush job");
    Ok(())
}

async fn flush_table(client: &Client, pg_version: u16) -> Result<i64> {
    let row = client
        .query_one(
            "SELECT koldstore.flush_table($1::text::regclass)::text",
            &[&relation(pg_version)],
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

async fn wait_for_jobs_to_finish(client: &Client, pg_version: u16) -> Result<()> {
    for attempt in 0..120 {
        let active = common::active_job_count(client, &relation(pg_version)).await?;
        if active == 0 {
            common::log(format!(
                "pg{pg_version}: jobs idle after {} poll(s)",
                attempt + 1
            ));
            return Ok(());
        }
        common::log(format!(
            "pg{pg_version}: waiting on {active} active job(s), poll {}/120",
            attempt + 1
        ));
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    anyhow::bail!("jobs did not finish for {}", relation(pg_version));
}

async fn assert_change_log_mirror(
    client: &Client,
    pg_version: u16,
    expected_rows: i64,
) -> Result<()> {
    let relation = relation(pg_version);
    let mirror = mirror_relation(pg_version);

    common::assert_system_columns_absent(client, &relation).await?;
    common::assert_change_log_mirror_exists(client, &mirror).await?;
    common::assert_primary_key_columns_match(client, &relation, &mirror).await?;

    let mirror_columns = relation_columns(client, &mirror).await?;
    let expected_mirror_columns = ["tenant_id", "id", "seq", "op"];
    for column in expected_mirror_columns {
        assert!(
            mirror_columns.iter().any(|name| name == column),
            "mirror {mirror} missing expected column {column}, got {mirror_columns:?}"
        );
    }

    let row = client
        .query_one(&format!("SELECT count(*) FROM {mirror}"), &[])
        .await?;
    assert_eq!(row.get::<_, i64>(0), expected_rows);

    let mirror_pk = common::primary_key_columns(client, &mirror).await?;
    assert_eq!(mirror_pk, vec!["tenant_id", "id"]);

    Ok(())
}

async fn assert_hot_row_limit_registered(client: &Client, pg_version: u16) -> Result<()> {
    let row = client
        .query_one(
            r#"
            SELECT options->'flush_policy'->>'type',
                   (options->'flush_policy'->>'hot_row_limit')::bigint
            FROM koldstore.schemas
            WHERE table_oid = $1::text::regclass::oid
              AND active
            ORDER BY version DESC
            LIMIT 1
            "#,
            &[&relation(pg_version)],
        )
        .await?;
    assert_eq!(
        row.get::<_, Option<String>>(0).as_deref(),
        Some("row_limit"),
        "manage_table must persist tagged flush_policy JSON"
    );
    assert_eq!(row.get::<_, Option<i64>>(1), Some(FLUSH_POLICY_ROW_LIMIT));
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ColdFlushBatches {
    segment_count: i64,
    total_rows: i64,
    batch_numbers: Vec<i32>,
    row_counts: Vec<i64>,
}

async fn fetch_cold_flush_batches(client: &Client, pg_version: u16) -> Result<ColdFlushBatches> {
    let rows = client
        .query(
            r#"
            SELECT batch_number, row_count
            FROM koldstore.cold_segments
            WHERE table_oid = $1::text::regclass::oid
              AND scope_key = ''
              AND status = 'active'
            ORDER BY batch_number
            "#,
            &[&relation(pg_version)],
        )
        .await?;

    let batch_numbers = rows
        .iter()
        .map(|row| row.get::<_, i32>(0))
        .collect::<Vec<_>>();
    let row_counts = rows
        .iter()
        .map(|row| row.get::<_, i64>(1))
        .collect::<Vec<_>>();
    Ok(ColdFlushBatches {
        segment_count: rows.len() as i64,
        total_rows: row_counts.iter().sum(),
        batch_numbers,
        row_counts,
    })
}

fn assert_flush_completed_in_batches(
    batches: &ColdFlushBatches,
    expected_total_rows: i64,
    expected_flush_runs: u32,
    policy_row_limit: i64,
    exact_row_total: bool,
) -> Result<()> {
    if exact_row_total {
        assert_eq!(
            batches.total_rows, expected_total_rows,
            "cold segment row totals should match flushed rows"
        );
    } else {
        assert!(
            batches.total_rows >= expected_total_rows,
            "cold segment row totals should cover at least {expected_total_rows} rows, got {}",
            batches.total_rows
        );
    }
    assert!(
        batches.segment_count >= i64::from(expected_flush_runs),
        "expected at least {expected_flush_runs} parquet batch segment(s), got {}",
        batches.segment_count
    );

    for (index, batch_number) in batches.batch_numbers.iter().enumerate() {
        let row_count = batches.row_counts[index];
        assert!(row_count > 0, "batch {batch_number} must contain rows");
        assert_eq!(
            *batch_number,
            (index as i32) + 1,
            "batch numbers should be contiguous starting at 1"
        );
    }

    if expected_total_rows > policy_row_limit {
        common::log_always(format!(
            "flush batches: {} segment(s), row_counts={:?}, policy rows:{policy_row_limit}",
            batches.segment_count, batches.row_counts
        ));
    }

    Ok(())
}

async fn assert_flush_jobs_recorded(
    client: &Client,
    pg_version: u16,
    min_completed_flush_jobs: i64,
) -> Result<()> {
    let row = client
        .query_one(
            r#"
            SELECT
              count(*) FILTER (WHERE job_type = 'flush' AND status = 'completed'),
              count(*) FILTER (WHERE job_type = 'flush' AND status IN ('pending', 'running')),
              count(*) FILTER (WHERE job_type = 'flush' AND status = 'error')
            FROM koldstore.jobs
            WHERE table_oid = $1::text::regclass::oid
            "#,
            &[&relation(pg_version)],
        )
        .await?;

    let completed = row.get::<_, i64>(0);
    let active = row.get::<_, i64>(1);
    let errored = row.get::<_, i64>(2);

    assert!(
        completed >= min_completed_flush_jobs,
        "expected at least {min_completed_flush_jobs} completed flush jobs, got {completed}"
    );
    assert_eq!(active, 0, "flush jobs should not remain active");
    assert_eq!(errored, 0, "flush jobs should not error");

    Ok(())
}

async fn load_manifest(client: &Client, pg_version: u16, storage_root: &Path) -> Result<Manifest> {
    let manifest_path = client
        .query_one(
            r#"
            SELECT manifest_path
            FROM koldstore.manifest
            WHERE table_oid = $1::text::regclass::oid
              AND sync_state = 'in_sync'
            ORDER BY generation DESC
            LIMIT 1
            "#,
            &[&relation(pg_version)],
        )
        .await?
        .get::<_, String>(0);

    let manifest_file = storage_root.join(&manifest_path);
    assert!(
        manifest_file.exists(),
        "missing manifest at {}",
        manifest_file.display()
    );

    let manifest: Manifest = serde_json::from_str(&std::fs::read_to_string(&manifest_file)?)
        .with_context(|| format!("parse manifest.json at {}", manifest_file.display()))?;

    for segment in &manifest.segments {
        let parquet_path = manifest_file
            .parent()
            .context("manifest directory missing")?
            .join(&segment.path);
        assert!(
            parquet_path.exists(),
            "missing parquet segment {}",
            parquet_path.display()
        );
    }

    Ok(manifest)
}

async fn assert_cold_parquet_sample_readable(
    client: &Client,
    pg_version: u16,
    storage_root: &Path,
) -> Result<()> {
    use parquet::file::reader::{FileReader, SerializedFileReader};

    let manifest = load_manifest(client, pg_version, storage_root).await?;
    let manifest_path = client
        .query_one(
            r#"
            SELECT manifest_path
            FROM koldstore.manifest
            WHERE table_oid = $1::text::regclass::oid
              AND sync_state = 'in_sync'
            ORDER BY generation DESC
            LIMIT 1
            "#,
            &[&relation(pg_version)],
        )
        .await?
        .get::<_, String>(0);
    let manifest_file = storage_root.join(&manifest_path);
    let segment_dir = manifest_file
        .parent()
        .context("manifest directory missing")?;

    let mut total_rows = 0_i64;
    for segment in &manifest.segments {
        let parquet_path = segment_dir.join(&segment.path);
        let file = std::fs::File::open(&parquet_path)
            .with_context(|| format!("open cold segment {}", parquet_path.display()))?;
        let reader = SerializedFileReader::new(file)?;
        total_rows += reader.metadata().file_metadata().num_rows();
    }

    assert!(
        total_rows >= TOTAL_ROWS,
        "parquet segments should contain at least {TOTAL_ROWS} rows, got {total_rows}"
    );
    assert!(
        manifest.segments.len() >= 2,
        "expected at least two committed parquet segments after two flushes"
    );
    Ok(())
}

async fn assert_sample_rows_readable(client: &Client, pg_version: u16) -> Result<()> {
    let relation = relation(pg_version);
    let summary = client
        .query_one(
            &format!(
                r#"
                SELECT
                  count(*),
                  count(DISTINCT (tenant_id, id)),
                  min(c_text),
                  max(c_int8)
                FROM {relation}
                "#
            ),
            &[],
        )
        .await?;

    assert_eq!(summary.get::<_, i64>(0), TOTAL_ROWS);
    assert_eq!(summary.get::<_, i64>(1), TOTAL_ROWS);
    assert_eq!(summary.get::<_, String>(2), "text-1");
    assert_eq!(summary.get::<_, i64>(3), TOTAL_ROWS);
    Ok(())
}

async fn relation_columns(client: &Client, relation: &str) -> Result<Vec<String>> {
    let rows = client
        .query(
            r#"
            SELECT attname
            FROM pg_attribute
            WHERE attrelid = $1::text::regclass
              AND attnum > 0
              AND NOT attisdropped
            ORDER BY attnum
            "#,
            &[&relation],
        )
        .await?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

fn relation(pg_version: u16) -> String {
    format!("lifecycle.full_lifecycle_wide_pg{pg_version}")
}

fn mirror_relation(pg_version: u16) -> String {
    format!("koldstore.full_lifecycle_wide_pg{pg_version}__cl")
}

fn storage_name(pg_version: u16) -> String {
    format!("full-lifecycle-local-pg{pg_version}")
}
