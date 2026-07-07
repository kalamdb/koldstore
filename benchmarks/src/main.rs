//! pg-koldstore benchmark runner.

use std::env;
use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use tokio_postgres::NoTls;

mod client;
mod cold_pruning;
mod hot_dml_vs_heap;
mod pgbench;
mod report;
mod suite;
mod verdict;

use pgbench::{PgbenchConfig, PgbenchMeasurement, PgbenchWorkload};
use report::{BenchmarkReport, BenchmarkResult, MachineMetadata};

#[derive(Debug, Clone, PartialEq, Eq)]
struct BenchmarkConfig {
    database_url: String,
    rows: u64,
    clients: usize,
    jobs: usize,
    seconds: u64,
    output_json: Option<PathBuf>,
    output_html: Option<PathBuf>,
}

struct WorkloadPair {
    scenario_name: &'static str,
    heap: PgbenchWorkload,
    koldstore: PgbenchWorkload,
    max_overhead_ratio: Option<f64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    run_real_pgbench_suite(BenchmarkConfig::from_env_args()?).await
}

async fn run_real_pgbench_suite(config: BenchmarkConfig) -> Result<()> {
    keep_contract_helpers_referenced();

    let postgres_version = setup_database(&config).await?;
    let tempdir = tempfile::tempdir().context("create pgbench workspace")?;
    let pgbench_config = PgbenchConfig {
        database_url: config.database_url.clone(),
        clients: config.clients,
        jobs: config.jobs,
        seconds: config.seconds,
    };

    let mut results = Vec::new();
    run_pair(
        &pgbench_config,
        tempdir.path(),
        &mut results,
        config.rows,
        WorkloadPair {
            scenario_name: "hot_update_vs_heap",
            heap: update_workload("hot_update_heap", "bench.heap_items", config.rows),
            koldstore: update_workload(
                "hot_update_koldstore",
                "bench.koldstore_items",
                config.rows,
            ),
            max_overhead_ratio: Some(verdict::HOT_DML_MAX_OVERHEAD_RATIO),
        },
    )
    .await?;
    run_pair(
        &pgbench_config,
        tempdir.path(),
        &mut results,
        config.rows,
        WorkloadPair {
            scenario_name: "pk_select_hot_only",
            heap: select_workload("pk_select_heap", "bench.heap_items", config.rows),
            koldstore: select_workload("pk_select_koldstore", "bench.koldstore_items", config.rows),
            max_overhead_ratio: None,
        },
    )
    .await?;
    run_pair(
        &pgbench_config,
        tempdir.path(),
        &mut results,
        config.rows,
        WorkloadPair {
            scenario_name: "hot_insert_vs_heap",
            heap: insert_workload("hot_insert_heap", "bench.heap_items", config.rows),
            koldstore: insert_workload(
                "hot_insert_koldstore",
                "bench.koldstore_items",
                config.rows,
            ),
            max_overhead_ratio: Some(verdict::HOT_DML_MAX_OVERHEAD_RATIO),
        },
    )
    .await?;

    let report = BenchmarkReport {
        suite: "pg-koldstore".to_string(),
        generated_at: chrono::Utc::now(),
        machine: MachineMetadata {
            postgres_version: Some(postgres_version),
            object_store_backend: Some("local-pgrx".to_string()),
            os: Some(env::consts::OS.to_string()),
            cpu: None,
            memory_bytes: None,
        },
        results,
    };
    let json = serde_json::to_string_pretty(&report)?;
    if let Some(path) = config.output_json {
        fs::write(&path, &json).with_context(|| format!("write {}", path.display()))?;
    }
    if let Some(path) = config.output_html {
        fs::write(&path, report::to_html_summary(&report))
            .with_context(|| format!("write {}", path.display()))?;
    }
    println!("{json}");
    let failed_results = report.failed_result_names();
    if !failed_results.is_empty() {
        anyhow::bail!("benchmark verdict failed for {}", failed_results.join(", "));
    }
    Ok(())
}

async fn setup_database(config: &BenchmarkConfig) -> Result<String> {
    let (client, connection) = tokio_postgres::connect(&config.database_url, NoTls)
        .await
        .context("connect benchmark database")?;
    tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .batch_execute(
            "CREATE EXTENSION IF NOT EXISTS koldstore; CREATE SCHEMA IF NOT EXISTS bench;",
        )
        .await?;
    client
        .batch_execute(
            "SELECT koldstore.register_storage(
                'bench-local',
                'filesystem',
                '/tmp/pg-koldstore-bench',
                '{}'::jsonb,
                '{}'::jsonb
            );",
        )
        .await?;
    client
        .batch_execute(
            "DROP TABLE IF EXISTS bench.heap_items;
             DROP TABLE IF EXISTS bench.koldstore_items;
             CREATE TABLE bench.heap_items (
                id bigint PRIMARY KEY,
                body text NOT NULL,
                value bigint NOT NULL
             );
             CREATE TABLE bench.koldstore_items (
                id bigint PRIMARY KEY,
                body text NOT NULL,
                value bigint NOT NULL
             );",
        )
        .await?;
    let seed_sql = format!(
        "INSERT INTO bench.heap_items
            SELECT g, 'payload-' || g::text, g FROM generate_series(1, {rows}) g;
         INSERT INTO bench.koldstore_items
            SELECT g, 'payload-' || g::text, g FROM generate_series(1, {rows}) g;
         SELECT koldstore.manage_table(table_name => 'bench.koldstore_items'::regclass, storage => 'bench-local', hot_row_limit => NULL, order_column => 'id');",
        rows = config.rows
    );
    client.batch_execute(&seed_sql).await?;

    let version = client
        .query_one("SHOW server_version", &[])
        .await?
        .get::<_, String>(0);
    Ok(version)
}

async fn run_pair(
    pgbench_config: &PgbenchConfig,
    workspace: &std::path::Path,
    results: &mut Vec<BenchmarkResult>,
    row_count: u64,
    pair: WorkloadPair,
) -> Result<()> {
    let heap_result = pgbench::run_pgbench(pgbench_config, &pair.heap, workspace).await?;
    let koldstore_result = pgbench::run_pgbench(pgbench_config, &pair.koldstore, workspace).await?;
    let koldstore_passed = pair.max_overhead_ratio.is_none_or(|max| {
        heap_result.p95_ms > 0.0 && koldstore_result.p95_ms / heap_result.p95_ms <= max
    });

    results.push(to_result(
        format!("{}_heap", pair.scenario_name),
        row_count,
        &heap_result,
        true,
    ));
    results.push(to_result(
        format!("{}_koldstore", pair.scenario_name),
        row_count,
        &koldstore_result,
        koldstore_passed,
    ));
    Ok(())
}

fn to_result(
    name: String,
    row_count: u64,
    measurement: &PgbenchMeasurement,
    passed: bool,
) -> BenchmarkResult {
    BenchmarkResult {
        name,
        row_count,
        throughput_ops_sec: measurement.throughput_ops_sec,
        p50_ms: measurement.p50_ms,
        p95_ms: measurement.p95_ms,
        p99_ms: measurement.p99_ms,
        peak_rss_bytes: None,
        allocated_bytes: None,
        passed,
    }
}

fn select_workload(name: &str, table: &str, rows: u64) -> PgbenchWorkload {
    PgbenchWorkload {
        name: name.to_string(),
        script: format!(
            "\\set id random(1, {rows})\nSELECT id, body, value FROM {table} WHERE id = :id;\n"
        ),
    }
}

fn update_workload(name: &str, table: &str, rows: u64) -> PgbenchWorkload {
    PgbenchWorkload {
        name: name.to_string(),
        script: format!(
            "\\set id random(1, {rows})\nUPDATE {table} SET value = value + 1 WHERE id = :id;\n"
        ),
    }
}

fn insert_workload(name: &str, table: &str, rows: u64) -> PgbenchWorkload {
    let max = rows.saturating_mul(100).max(rows + 1);
    PgbenchWorkload {
        name: name.to_string(),
        script: format!(
            "\\set id random({start}, {max})\nINSERT INTO {table} (id, body, value) VALUES (:id, 'insert-payload', :id) ON CONFLICT (id) DO NOTHING;\n",
            start = rows + 1
        ),
    }
}

fn keep_contract_helpers_referenced() {
    let hot_dml = hot_dml_vs_heap::HotDmlScenario::default_tables();
    let _setup_sql = [
        hot_dml.heap.create_table_sql(),
        hot_dml.managed.create_table_sql(),
    ];
    let _scenario_names = [
        hot_dml_vs_heap::NAME,
        cold_pruning::NAME,
        suite::SCENARIOS[0],
    ];
    let pruning = cold_pruning::ColdPruningResult {
        total_row_groups: 100,
        selected_row_groups: 10,
    };
    let _thresholds = (
        verdict::HOT_DML_MAX_OVERHEAD_RATIO,
        verdict::PK_LOOKUP_MIN_ROW_GROUP_SKIP_RATIO,
    );
    let _suite = suite::FULL_SUITE;
    let _verdicts = (
        verdict::hot_dml_within_threshold(1.0, 1.05),
        pruning.meets_pk_lookup_target()
            && verdict::pk_lookup_pruning_within_threshold(pruning.skipped_ratio()),
    );
    let _empty_report = report::BenchmarkReport::empty("pg-koldstore");
}

impl BenchmarkConfig {
    fn from_env_args() -> Result<Self> {
        let args = env::args().collect::<Vec<_>>();
        let default_database_url = format!(
            "host=127.0.0.1 port=28816 user={} dbname=postgres",
            env::var("USER").unwrap_or_else(|_| "postgres".to_string())
        );
        Ok(Self {
            database_url: value_arg(&args, "--database-url")
                .or_else(|| env::var("DATABASE_URL").ok())
                .unwrap_or(default_database_url),
            rows: parse_arg(&args, "--rows", 10_000)?,
            clients: parse_arg(&args, "--clients", 4)?,
            jobs: parse_arg(&args, "--jobs", 4)?,
            seconds: parse_arg(&args, "--seconds", 3)?,
            output_json: value_arg(&args, "--output-json").map(PathBuf::from),
            output_html: value_arg(&args, "--output-html").map(PathBuf::from),
        })
    }
}

fn value_arg(args: &[String], name: &str) -> Option<String> {
    args.windows(2)
        .find(|window| window[0] == name)
        .map(|window| window[1].clone())
}

fn parse_arg<T>(args: &[String], name: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    value_arg(args, name)
        .map(|value| value.parse().with_context(|| format!("parse {name}")))
        .transpose()
        .map(|value| value.unwrap_or(default))
}
