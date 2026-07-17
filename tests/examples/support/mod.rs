//! Shared helpers for deep multi-tenant, multi-flush, concurrent DML scenarios.

#[path = "../../e2e/common/mod.rs"]
pub mod e2e;

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use parquet::file::reader::{FileReader, SerializedFileReader};
use tokio_postgres::Client;

/// Insert progress is logged every N rows (e.g. `10000 rows written`).
pub const INSERT_PROGRESS_INTERVAL: i64 = 10_000;

/// Always-on progress line, matching `tests/e2e/full_lifecycle.rs` style.
pub fn log_always(message: impl AsRef<str>) {
    e2e::log_always(format!("[examples] {}", message.as_ref()));
}

/// Step guard that logs start + elapsed seconds on drop.
#[must_use]
pub fn log_step(step: impl Into<String>) -> e2e::StepGuard {
    e2e::log_step_always(format!("[examples] {}", step.into()))
}

/// Runs an async action and logs elapsed wall time on success or failure.
pub async fn timed_async<T, E: std::fmt::Display>(
    label: impl Into<String>,
    action: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    let label = label.into();
    let started = Instant::now();
    log_always(format!("{label}: start"));
    match action.await {
        Ok(value) => {
            log_always(format!(
                "{label}: ok in {:.3}s",
                started.elapsed().as_secs_f64()
            ));
            Ok(value)
        }
        Err(error) => {
            log_always(format!(
                "{label}: failed in {:.3}s ({error})",
                started.elapsed().as_secs_f64()
            ));
            Err(error)
        }
    }
}

/// Logs scenario sizing and filesystem layout at startup.
pub fn log_scenario_start(name: &str, relation: &str, storage_root: &Path, config: ExampleConfig) {
    log_always(format!(
        "{name}: rows={} scopes={} clients={} ({} rows/scope)",
        config.rows,
        config.scopes,
        config.clients,
        config.rows_per_scope()
    ));
    log_always(format!("{name}: relation={relation}"));
    log_always(format!(
        "{name}: cold storage root {}",
        storage_root.display()
    ));
}

/// Optional context for flush helpers to emit row counts and on-disk paths.
#[derive(Debug, Clone, Copy)]
pub struct FlushCtx<'a> {
    /// Human-readable flush label such as `wave-1` or `force-final`.
    pub label: &'a str,
    /// Registered filesystem storage root for the scenario database.
    pub storage_root: &'a Path,
}

/// Tracks cumulative insert volume across parallel clients.
#[derive(Debug, Clone)]
pub struct InsertProgress {
    phase: String,
    target: i64,
    written: Arc<AtomicI64>,
}

impl InsertProgress {
    /// Creates a progress tracker for a named insert phase.
    #[must_use]
    pub fn new(phase: impl Into<String>, target: i64) -> Self {
        Self {
            phase: phase.into(),
            target,
            written: Arc::new(AtomicI64::new(0)),
        }
    }

    /// Records newly written rows and logs every [`INSERT_PROGRESS_INTERVAL`] rows.
    pub fn record(&self, delta: i64) {
        if delta <= 0 {
            return;
        }
        let previous = self.written.fetch_add(delta, Ordering::Relaxed);
        let current = previous + delta;
        let previous_bucket = previous / INSERT_PROGRESS_INTERVAL;
        let current_bucket = current / INSERT_PROGRESS_INTERVAL;
        if current_bucket > previous_bucket {
            log_always(format!(
                "{}: {} / {} rows written",
                self.phase, current, self.target
            ));
        }
    }

    /// Logs the final insert total for a phase.
    pub fn finish(&self) {
        let total = self.written.load(Ordering::Relaxed);
        log_always(format!(
            "{}: finished with {} / {} rows written",
            self.phase, total, self.target
        ));
    }
}

/// How many cold segment paths to print at the head/tail of a snapshot.
const COLD_SNAPSHOT_SAMPLE: usize = 5;

/// Logs active cold segment and manifest paths under the storage root.
///
/// Large tables print a head/tail sample plus a summary so progress stays
/// readable while still exposing the flushed folder layout.
///
/// # Errors
///
/// Returns an error when catalog queries fail.
pub async fn log_cold_storage_snapshot(
    client: &Client,
    relation: &str,
    storage_root: &Path,
    label: &str,
) -> Result<()> {
    let segments = load_cold_segments(client, relation).await?;
    let manifests = load_manifests(client, relation).await?;
    log_always(format!(
        "{label}: cold storage root {} ({} segments, {} manifests)",
        storage_root.display(),
        segments.len(),
        manifests.len()
    ));

    let sample = |segment: &ColdSegmentInfo| {
        log_always(format!(
            "{label}:   segment batch={} rows={} bytes={} file={}",
            segment.batch_number,
            segment.row_count,
            segment.byte_size,
            storage_root.join(&segment.object_path).display()
        ));
    };

    if segments.len() <= COLD_SNAPSHOT_SAMPLE * 2 {
        for segment in &segments {
            sample(segment);
        }
    } else {
        for segment in &segments[..COLD_SNAPSHOT_SAMPLE] {
            sample(segment);
        }
        log_always(format!(
            "{label}:   ... {} more segments ...",
            segments.len() - COLD_SNAPSHOT_SAMPLE * 2
        ));
        for segment in &segments[segments.len() - COLD_SNAPSHOT_SAMPLE..] {
            sample(segment);
        }
    }

    for manifest in manifests {
        log_always(format!(
            "{label}:   manifest scope={} state={} generation={} file={}",
            manifest.scope_key,
            manifest.sync_state,
            manifest.generation,
            storage_root.join(&manifest.manifest_path).display()
        ));
    }
    Ok(())
}

/// Default per-scenario wall-clock budget (seconds).
///
/// Override with `KOLDSTORE_EXAMPLE_TIMEOUT_SECS`. Nextest also terminates the
/// `examples` package after 10 minutes via `.config/nextest.toml`.
pub const DEFAULT_EXAMPLE_TIMEOUT_SECS: u64 = 600;

/// Runs an example scenario body under a wall-clock timeout.
///
/// # Errors
///
/// Returns an error when the inner scenario fails or the timeout elapses.
pub async fn with_example_timeout<F>(name: &str, future: F) -> Result<()>
where
    F: Future<Output = Result<()>>,
{
    let secs = env_u64(
        "KOLDSTORE_EXAMPLE_TIMEOUT_SECS",
        DEFAULT_EXAMPLE_TIMEOUT_SECS,
    )
    .max(30);
    match tokio::time::timeout(Duration::from_secs(secs), future).await {
        Ok(result) => result,
        Err(_) => anyhow::bail!(
            "{name} exceeded KOLDSTORE_EXAMPLE_TIMEOUT_SECS={secs}s; \
             lower KOLDSTORE_EXAMPLE_ROWS/SCOPES/CLIENTS or raise the timeout"
        ),
    }
}

/// Runtime knobs for example workloads.
#[derive(Debug, Clone, Copy)]
pub struct ExampleConfig {
    /// Total rows to seed per scenario (split across scopes).
    pub rows: i64,
    /// Number of parallel PostgreSQL clients used for inserts/queries.
    pub clients: usize,
    /// Number of tenant/workspace/game scopes to spread rows across.
    pub scopes: usize,
}

impl ExampleConfig {
    /// Loads example sizing from the environment.
    ///
    /// `KOLDSTORE_EXAMPLE_ROWS` defaults to `50000`.
    /// `KOLDSTORE_EXAMPLE_CLIENTS` defaults to `8`.
    /// `KOLDSTORE_EXAMPLE_SCOPES` defaults to `50`.
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            rows: env_i64("KOLDSTORE_EXAMPLE_ROWS", 50_000),
            clients: env_usize("KOLDSTORE_EXAMPLE_CLIENTS", 8),
            scopes: env_usize("KOLDSTORE_EXAMPLE_SCOPES", 50),
        }
    }

    /// Rows seeded per scope when distributing evenly.
    #[must_use]
    pub fn rows_per_scope(self) -> i64 {
        let scopes = self.scopes.max(1) as i64;
        self.rows / scopes
    }

    /// Human-readable scope id like `tenant-0003`.
    #[must_use]
    pub fn scope_id(self, prefix: &str, idx: usize) -> String {
        format!("{prefix}-{idx:04}")
    }
}

/// Snapshot of cold segment catalog rows used by examples.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ColdSegmentInfo {
    pub scope_key: String,
    pub object_path: String,
    pub row_count: i64,
    pub byte_size: i64,
    pub batch_number: i32,
}

/// Snapshot of manifest catalog state.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ManifestInfo {
    pub scope_key: String,
    pub manifest_path: String,
    pub sync_state: String,
    pub generation: i64,
}

/// Registers a user-scoped managed table with structured flush settings.
///
/// # Errors
///
/// Returns an error when management or catalog assertions fail.
#[allow(clippy::too_many_arguments)]
pub async fn manage_user_scoped_with_policy(
    client: &Client,
    storage_name: &str,
    relation: &str,
    scope_column: &str,
    migration_order_by: &str,
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
              table_type        => 'user',
              scope_column      => $6,
              migration_order_by => $7
            )
            "#,
            &[
                &relation,
                &storage_name,
                &hot_row_limit,
                &min_flush_rows,
                &max_rows_per_file,
                &scope_column,
                &migration_order_by,
            ],
        )
        .await?;
    e2e::assert_system_columns_absent(client, relation).await?;
    e2e::assert_catalog_has_active_schema(client, relation).await?;
    Ok(())
}

/// Sets the active user scope for scoped reads and writes.
///
/// # Errors
///
/// Returns an error when the GUC cannot be set.
pub async fn set_scope(client: &Client, scope_id: &str) -> Result<()> {
    client
        .batch_execute(&format!(
            "SET koldstore.user_id = '{}'",
            scope_id.replace('\'', "''")
        ))
        .await?;
    Ok(())
}

/// Runs `flush_table` and returns flushed row count from the job record.
///
/// When `ctx` is set, logs flushed row counts and cold artifact paths.
///
/// # Errors
///
/// Returns an error when flush or job lookup fails.
pub async fn flush_table(
    client: &Client,
    relation: &str,
    ctx: Option<FlushCtx<'_>>,
) -> Result<i64> {
    let _step = ctx.map(|ctx| log_step(format!("{}: flush_table", ctx.label)));
    let label = ctx
        .map(|ctx| ctx.label.to_string())
        .unwrap_or_else(|| format!("flush_table {relation}"));
    log_flush_inputs(client, relation, &label).await?;
    let row = timed_async(
        ctx.map(|ctx| format!("{}: koldstore.flush_table", ctx.label))
            .unwrap_or_else(|| format!("flush_table {relation}")),
        client.query_one(
            "SELECT koldstore.flush_table($1::text::regclass)::text",
            &[&relation],
        ),
    )
    .await?;
    let job_id: String = row.get(0);
    let progress = timed_async(
        format!("flush job rows_flushed lookup ({job_id})"),
        client.query_one(
            "SELECT rows_flushed FROM koldstore.jobs WHERE id = $1::text::uuid",
            &[&job_id],
        ),
    )
    .await?;
    let flushed: i64 = progress.get(0);
    if let Some(ctx) = ctx {
        log_always(format!("{}: flushed {} rows", ctx.label, flushed));
        if flushed > 0 {
            timed_async(
                format!("{}: cold storage snapshot", ctx.label),
                log_cold_storage_snapshot(client, relation, ctx.storage_root, ctx.label),
            )
            .await?;
        }
    }
    Ok(flushed)
}

async fn log_flush_inputs(client: &Client, relation: &str, label: &str) -> Result<()> {
    // PERFORMANCE: read O(1) counters from koldstore.manifest instead of COUNT(*) scans.
    let row = timed_async(
        format!("{label}: flush input metadata"),
        client.query_one(
            r#"
            SELECT
              COALESCE(m.hot_row_count, 0)::bigint AS hot_rows,
              COALESCE(m.mirror_row_count, 0)::bigint AS mirror_rows,
              COALESCE(m.segment_count, 0)::bigint AS cold_segments,
              COALESCE(m.cold_row_count, 0)::bigint AS cold_rows
            FROM koldstore.manifest m
            WHERE m.table_oid = $1::text::regclass::oid
              AND m.scope_key = ''
            "#,
            &[&relation],
        ),
    )
    .await?;
    let hot_rows: i64 = row.get(0);
    let mirror_rows: i64 = row.get(1);
    let cold_segments: i64 = row.get(2);
    let cold_rows: i64 = row.get(3);
    log_always(format!(
        "{label}: flush input mirror_rows={mirror_rows} hot_rows={hot_rows} \
         cold_segments={cold_segments} cold_rows={cold_rows}"
    ));
    Ok(())
}

/// Enqueues a force flush and runs it, returning flushed row count.
///
/// Waits for any active jobs first so a stale non-force pending job cannot
/// suppress the `force => true` payload.
///
/// # Errors
///
/// Returns an error when enqueue or flush fails.
pub async fn force_flush_table(
    client: &Client,
    relation: &str,
    ctx: Option<FlushCtx<'_>>,
) -> Result<i64> {
    let _step = ctx.map(|ctx| log_step(format!("{}: force_flush_table", ctx.label)));
    timed_async(
        ctx.map(|ctx| format!("{}: wait_for_jobs", ctx.label))
            .unwrap_or_else(|| format!("wait_for_jobs {relation}")),
        wait_for_jobs(client, relation),
    )
    .await?;
    timed_async(
        ctx.map(|ctx| format!("{}: enqueue force flush", ctx.label))
            .unwrap_or_else(|| format!("enqueue force flush {relation}")),
        client.execute(
            "SELECT koldstore.enqueue_flush_job(table_name => $1::text::regclass, force => true)",
            &[&relation],
        ),
    )
    .await?;
    let flushed = flush_table(client, relation, None).await?;
    timed_async(
        ctx.map(|ctx| format!("{}: wait_for_jobs after flush", ctx.label))
            .unwrap_or_else(|| format!("wait_for_jobs after flush {relation}")),
        wait_for_jobs(client, relation),
    )
    .await?;
    if let Some(ctx) = ctx {
        log_always(format!("{}: force-flushed {} rows", ctx.label, flushed));
        if flushed > 0 {
            timed_async(
                format!("{}: cold storage snapshot", ctx.label),
                log_cold_storage_snapshot(client, relation, ctx.storage_root, ctx.label),
            )
            .await?;
        }
    }
    Ok(flushed)
}

/// Runs multiple flush waves and returns the flushed counts for each wave.
///
/// # Errors
///
/// Returns an error when any flush or job wait fails.
#[allow(dead_code)] // used by some example binaries, not all
pub async fn flush_waves(
    client: &Client,
    relation: &str,
    max_waves: usize,
    ctx: Option<FlushCtx<'_>>,
) -> Result<Vec<i64>> {
    let mut flushed = Vec::new();
    for wave in 0..max_waves {
        if let Some(base) = ctx {
            log_always(format!("{}: starting flush wave {}", base.label, wave + 1));
        }
        let count = flush_table(client, relation, None).await?;
        wait_for_jobs(client, relation).await?;
        if let Some(base) = ctx {
            log_always(format!(
                "{}: wave {} flushed {} rows",
                base.label,
                wave + 1,
                count
            ));
            if count > 0 {
                log_cold_storage_snapshot(
                    client,
                    relation,
                    base.storage_root,
                    &format!("{}-wave{}", base.label, wave + 1),
                )
                .await?;
            }
        }
        if count == 0 {
            break;
        }
        flushed.push(count);
    }
    Ok(flushed)
}

/// Spawns parallel clients that each run one async workload against the same target.
///
/// # Errors
///
/// Returns an error when any client task fails.
pub async fn run_parallel_clients<F, Fut>(
    target: &e2e::PgTarget,
    clients: usize,
    worker: F,
) -> Result<()>
where
    F: Fn(usize, Client) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    let worker = Arc::new(worker);
    let mut tasks = Vec::with_capacity(clients);
    for client_idx in 0..clients {
        let target = target.clone();
        let worker = Arc::clone(&worker);
        tasks.push(tokio::spawn(async move {
            let client = e2e::connect(&target).await?;
            worker(client_idx, client).await
        }));
    }

    for task in tasks {
        task.await??;
    }
    Ok(())
}

/// Waits until no active jobs remain for a managed table.
///
/// # Errors
///
/// Returns an error when job polling fails or the timeout elapses.
pub async fn wait_for_jobs(client: &Client, relation: &str) -> Result<()> {
    for _ in 0..120 {
        let active = e2e::active_job_count(client, relation).await?;
        if active == 0 {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    anyhow::bail!("timed out waiting for jobs on {relation}");
}

/// Asserts that the named indexes exist for a table.
///
/// # Errors
///
/// Returns an error when any expected index is missing.
pub async fn assert_indexes_exist(
    client: &Client,
    schema: &str,
    index_names: &[&str],
) -> Result<()> {
    for index_name in index_names {
        let exists = client
            .query_one(
                r#"
                SELECT EXISTS (
                  SELECT 1
                  FROM pg_class c
                  JOIN pg_namespace n ON n.oid = c.relnamespace
                  WHERE n.nspname = $1
                    AND c.relname = $2
                    AND c.relkind = 'i'
                )
                "#,
                &[&schema, index_name],
            )
            .await?
            .get::<_, bool>(0);
        anyhow::ensure!(exists, "expected index {schema}.{index_name} to exist");
    }
    Ok(())
}

/// Loads active cold segment metadata for a managed table.
///
/// # Errors
///
/// Returns an error when the catalog query fails.
pub async fn load_cold_segments(client: &Client, relation: &str) -> Result<Vec<ColdSegmentInfo>> {
    let rows = client
        .query(
            r#"
            SELECT scope_key, object_path, row_count, byte_size, batch_number
            FROM koldstore.cold_segments
            WHERE table_oid = $1::text::regclass::oid
              AND status = 'active'
            ORDER BY scope_key, batch_number
            "#,
            &[&relation],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| ColdSegmentInfo {
            scope_key: row.get(0),
            object_path: row.get(1),
            row_count: row.get(2),
            byte_size: row.get(3),
            batch_number: row.get(4),
        })
        .collect())
}

/// Loads manifest catalog rows for a managed table.
///
/// # Errors
///
/// Returns an error when the catalog query fails.
pub async fn load_manifests(client: &Client, relation: &str) -> Result<Vec<ManifestInfo>> {
    let rows = client
        .query(
            r#"
            SELECT scope_key, manifest_path, sync_state, generation
            FROM koldstore.manifest
            WHERE table_oid = $1::text::regclass::oid
            ORDER BY scope_key
            "#,
            &[&relation],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| ManifestInfo {
            scope_key: row.get(0),
            manifest_path: row.get(1),
            sync_state: row.get(2),
            generation: row.get(3),
        })
        .collect())
}

/// Asserts flush produced sized parquet files and a usable manifest.
///
/// Checks:
/// - at least `min_segments` active cold segments
/// - every segment row_count is within `1..=max_rows_per_file`
/// - parquet files exist on disk with matching row counts
/// - at least one in-sync / pending manifest row exists
/// - cold PK hints and byte sizes are present
///
/// # Errors
///
/// Returns an error when catalog, filesystem, or parquet checks fail.
pub async fn assert_parquet_and_manifest(
    client: &Client,
    relation: &str,
    storage_root: &Path,
    max_rows_per_file: i64,
    min_segments: i64,
) -> Result<()> {
    let _step = log_step(format!(
        "verify parquet + manifest on disk under {}",
        storage_root.display()
    ));
    e2e::assert_cold_metadata_present(client, relation).await?;

    let segments = load_cold_segments(client, relation).await?;
    anyhow::ensure!(
        segments.len() as i64 >= min_segments,
        "expected at least {min_segments} cold segments for {relation}, got {}",
        segments.len()
    );
    log_always(format!(
        "parquet verify: opening {} files under {}",
        segments.len(),
        storage_root.display()
    ));

    for (idx, segment) in segments.iter().enumerate() {
        anyhow::ensure!(
            segment.row_count > 0 && segment.row_count <= max_rows_per_file,
            "segment {} row_count {} exceeds max_rows_per_file {max_rows_per_file}",
            segment.object_path,
            segment.row_count
        );
        anyhow::ensure!(
            segment.byte_size > 0,
            "segment {} must report positive byte_size",
            segment.object_path
        );

        let parquet_path = storage_root.join(&segment.object_path);
        anyhow::ensure!(
            parquet_path.exists(),
            "missing parquet file {}",
            parquet_path.display()
        );
        let file = std::fs::File::open(&parquet_path)
            .with_context(|| format!("open {}", parquet_path.display()))?;
        let reader = SerializedFileReader::new(file)
            .with_context(|| format!("read parquet {}", parquet_path.display()))?;
        let file_rows = reader.metadata().file_metadata().num_rows();
        anyhow::ensure!(
            file_rows == segment.row_count,
            "parquet {} has {file_rows} rows but catalog says {}",
            parquet_path.display(),
            segment.row_count
        );

        let checked = idx + 1;
        if checked == segments.len() || checked % 100 == 0 || checked == 1 {
            log_always(format!(
                "parquet verify: checked {checked}/{} files",
                segments.len()
            ));
        }
    }

    let manifests = load_manifests(client, relation).await?;
    anyhow::ensure!(
        !manifests.is_empty(),
        "expected manifest rows for {relation}"
    );
    for manifest in &manifests {
        anyhow::ensure!(
            manifest.sync_state == "in_sync"
                || manifest.sync_state == "pending"
                || manifest.sync_state == "pending_write",
            "unexpected manifest sync_state {} for scope {}",
            manifest.sync_state,
            manifest.scope_key
        );
        let path = storage_root.join(&manifest.manifest_path);
        anyhow::ensure!(path.exists(), "missing manifest.json at {}", path.display());
        let contents =
            std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        anyhow::ensure!(
            contents.contains("segments") || contents.contains("schema_version"),
            "manifest {} does not look like a koldstore manifest",
            path.display()
        );
        log_always(format!("parquet verify: manifest ok at {}", path.display()));
    }

    Ok(())
}

/// Runs SQL that already returns a single `bigint` count column.
///
/// Prefer this over [`e2e::row_count_from_sql`] when the SQL itself is
/// `SELECT count(*) ...`, which would nest two aggregations and always return `1`.
///
/// # Errors
///
/// Returns an error when the query fails.
pub async fn scalar_count(client: &Client, sql: &str) -> Result<i64> {
    Ok(client.query_one(sql, &[]).await?.get(0))
}

/// Asserts multiple tenants remain visible through scoped merge reads.
///
/// Inline `flush_table` stores cold segments table-wide in v1 (`scope_key = ''`),
/// so multi-tenant isolation is verified at the application RLS / `koldstore.user_id`
/// boundary rather than by counting distinct segment scope keys.
///
/// Example scenarios intentionally use an explicit scope filter here because it
/// matches real application queries (`WHERE tenant_id = ...`, `WHERE game_id = ...`,
/// etc.) and avoids over-trusting bare `SELECT * FROM table` behavior during
/// engine bring-up.
///
/// # Errors
///
/// Returns an error when any tenant has zero visible rows.
pub async fn assert_multi_tenant_visibility(
    client: &Client,
    relation: &str,
    scope_column: &str,
    tenant_ids: &[&str],
) -> Result<()> {
    for tenant in tenant_ids {
        set_scope(client, tenant).await?;
        let count = scalar_count(
            client,
            &format!("SELECT count(*) FROM {relation} WHERE {scope_column} = '{tenant}'"),
        )
        .await?;
        anyhow::ensure!(
            count > 0,
            "expected visible rows for tenant {tenant} on {relation}, got {count}"
        );
    }
    Ok(())
}

/// Options for [`assert_cold_then_delete_overlay`].
#[derive(Debug, Clone, Copy, Default)]
pub struct ColdDeleteOverlayOpts {
    /// Insert fresh ids and `force => true` flush them into cold (slow: re-flushes the
    /// full mirror). Default is false: reuse ids that prior flush waves already moved
    /// to cold.
    pub prime_dedicated_cold_rows: bool,
    /// Run `EXPLAIN ANALYZE` on merge-scan visibility checks (doubles query cost).
    pub profile_merge_scan: bool,
}

/// Picks the lowest primary keys for a tenant after multi-wave flush (already in cold).
///
/// # Errors
///
/// Returns an error when the scoped lookup returns fewer rows than requested.
pub async fn scoped_overlay_ids_from_cold(
    client: &Client,
    relation: &str,
    scope_column: &str,
    scope_id: &str,
    count: usize,
) -> Result<Vec<i64>> {
    set_scope(client, scope_id).await?;
    let rows = client
        .query(
            &format!(
                "SELECT id FROM {relation} WHERE {scope_column} = $1 ORDER BY id ASC LIMIT $2"
            ),
            &[
                &scope_id,
                &(i64::try_from(count).context("overlay id count")?),
            ],
        )
        .await?;
    anyhow::ensure!(
        rows.len() == count,
        "expected {count} scoped rows for overlay on {relation}, got {}",
        rows.len()
    );
    Ok(rows.iter().map(|row| row.get(0)).collect())
}

async fn log_overlay_catalog_snapshot(client: &Client, relation: &str, label: &str) -> Result<()> {
    let row = client
        .query_one(
            r#"
            SELECT
              COALESCE(m.mirror_row_count, 0)::bigint,
              COALESCE(m.hot_row_count, 0)::bigint,
              COALESCE(m.cold_row_count, 0)::bigint,
              COALESCE(m.segment_count, 0)::bigint
            FROM koldstore.manifest m
            WHERE m.table_oid = $1::text::regclass::oid
              AND m.scope_key = ''
            "#,
            &[&relation],
        )
        .await?;
    let mirror_rows: i64 = row.get(0);
    let hot_rows: i64 = row.get(1);
    let cold_rows: i64 = row.get(2);
    let segments: i64 = row.get(3);
    log_always(format!(
        "cold-delete overlay [{label}]: mirror_rows={mirror_rows} hot_rows={hot_rows} \
         cold_rows={cold_rows} cold_segments={segments}"
    ));
    Ok(())
}

/// Counts visible rows for a primary key (merge scan view).
///
/// # Errors
///
/// Returns an error when the query fails.
pub async fn visible_pk_count(client: &Client, relation: &str, id: i64) -> Result<i64> {
    visible_pk_count_with_profile(client, relation, id, true).await
}

/// Like [`visible_pk_count`] but skips `EXPLAIN ANALYZE` when `profile` is false.
///
/// # Errors
///
/// Returns an error when the query fails.
pub async fn visible_pk_count_with_profile(
    client: &Client,
    relation: &str,
    id: i64,
    profile: bool,
) -> Result<i64> {
    let sql = format!("SELECT count(*) FROM {relation} WHERE id = {id}");
    if profile {
        let plan = timed_async(
            format!("merge scan visible_pk_count id={id}: EXPLAIN ANALYZE"),
            e2e::explain_analyze(client, &sql),
        )
        .await?;
        log_merge_scan_profile(id, &plan);
    }
    timed_async(
        format!("merge scan visible_pk_count id={id}"),
        scalar_count(client, &sql),
    )
    .await
}

/// Returns how many of `ids` are visible through merge scan (one query).
///
/// # Errors
///
/// Returns an error when the query fails.
pub async fn visible_pk_batch_count(client: &Client, relation: &str, ids: &[i64]) -> Result<i64> {
    if ids.is_empty() {
        return Ok(0);
    }
    timed_async(
        format!("merge scan visible_pk_batch_count ({} ids)", ids.len()),
        async {
            let row = client
                .query_one(
                    &format!("SELECT count(*)::bigint FROM {relation} WHERE id = ANY($1)"),
                    &[&ids],
                )
                .await?;
            Ok(row.get(0))
        },
    )
    .await
}

fn log_merge_scan_profile(id: i64, plan: &str) {
    let mut parquet_files = 0_usize;
    let mut parquet_rows = 0_usize;
    let mut parquet_ms = 0.0_f64;

    for line in plan.lines() {
        let Some((_, detail)) = line.split_once("Parquet segment:") else {
            continue;
        };
        let detail = detail.trim();
        if detail == "none" || detail.contains("(planned)") {
            continue;
        }
        parquet_files += 1;
        let parts = detail.split(", ").collect::<Vec<_>>();
        if let Some(rows_part) = parts.iter().find(|part| part.ends_with(" rows")) {
            if let Ok(rows) = rows_part.trim_end_matches(" rows").parse::<usize>() {
                parquet_rows += rows;
            }
        }
        if let Some(ms_part) = parts.iter().rev().find(|part| part.ends_with(" ms")) {
            if let Ok(ms) = ms_part.trim_end_matches(" ms").parse::<f64>() {
                parquet_ms += ms;
            }
        }
    }

    log_always(format!(
        "merge scan visible_pk_count id={id}: cold read parquet_files={parquet_files} \
         parquet_rows={parquet_rows} parquet_read_ms={parquet_ms:.3}"
    ));
}

/// Reads mirror op for a primary key, if present.
///
/// # Errors
///
/// Returns an error when the query fails.
pub async fn mirror_op(client: &Client, relation: &str, id: i64) -> Result<Option<i16>> {
    let mirror = e2e::change_log_mirror_relation(relation);
    timed_async(format!("mirror op lookup id={id}"), async {
        let row = client
            .query_opt(&format!("SELECT op FROM {mirror} WHERE id = $1"), &[&id])
            .await?;
        Ok(row.map(|row| row.get(0)))
    })
    .await
}

pub async fn assert_cold_then_delete_overlay(
    client: &Client,
    relation: &str,
    scope_id: &str,
    scope_column: &str,
    ids: &[i64],
    rematerialize_sql: &dyn Fn(i64) -> String,
    flush_ctx: Option<FlushCtx<'_>>,
) -> Result<()> {
    assert_cold_then_delete_overlay_with_opts(
        client,
        relation,
        scope_id,
        scope_column,
        ids,
        rematerialize_sql,
        flush_ctx,
        ColdDeleteOverlayOpts::default(),
    )
    .await
}

/// Proves the durable flush → rematerialize → delete → flush path.
///
/// Covers the user-facing scenario "row was flushed into cold, then deleted in
/// hot":
/// 1. Insert dedicated ids and force-flush them into Parquet
/// 2. Rematerialize each id as a hot overlay (`INSERT` / upsert SQL)
/// 3. `DELETE` while hot (mirror `op=3`)
/// 4. Force-flush so the delete marker lands beside prior cold history
/// 5. Assert merge scan hides the ids and cold segment count does not shrink
///
/// Immediately after step 3 (before the second flush), merge may briefly re-show
/// the older cold live row because KoldMergeScan resolves hot heap + cold files
/// and does not keep a separate hot tombstone row in the user table. Durability
/// requires the tombstone flush in step 4.
///
/// By default [`ColdDeleteOverlayOpts::prime_dedicated_cold_rows`] is false: pass
/// ids that prior flush waves already moved to cold (see
/// [`scoped_overlay_ids_from_cold`]) to avoid a ~60s force flush that re-writes
/// the entire mirror.
///
/// # Errors
///
/// Returns an error when insert, flush, delete, or visibility checks fail.
#[allow(clippy::too_many_arguments)]
pub async fn assert_cold_then_delete_overlay_with_opts(
    client: &Client,
    relation: &str,
    scope_id: &str,
    scope_column: &str,
    ids: &[i64],
    rematerialize_sql: &dyn Fn(i64) -> String,
    flush_ctx: Option<FlushCtx<'_>>,
    opts: ColdDeleteOverlayOpts,
) -> Result<()> {
    let overlay_started = Instant::now();
    let _step = log_step(format!(
        "cold-delete overlay on {relation} for scope {scope_id} ({} ids)",
        ids.len()
    ));
    set_scope(client, scope_id).await?;

    let before_segments = e2e::cold_segment_count(client, relation).await?;
    anyhow::ensure!(
        before_segments > 0,
        "expected prior cold history before cold-then-delete coverage"
    );
    log_always(format!(
        "cold-delete overlay: starting with {before_segments} active cold segments; \
         prime_dedicated_cold_rows={} profile_merge_scan={}",
        opts.prime_dedicated_cold_rows, opts.profile_merge_scan
    ));
    log_overlay_catalog_snapshot(client, relation, "start").await?;

    let profile = opts.profile_merge_scan;
    let mut phase_started = Instant::now();

    if opts.prime_dedicated_cold_rows {
        log_always(
            "cold-delete overlay: priming dedicated ids via force flush \
             (slow: force=true re-flushes the full mirror, not just overlay ids)",
        );
        for &id in ids {
            timed_async(
                format!("cold-delete overlay: insert overlay id {id}"),
                client.batch_execute(&rematerialize_sql(id)),
            )
            .await?;
            anyhow::ensure!(
                visible_pk_count_with_profile(client, relation, id, profile).await? == 1,
                "id {id} must be visible after insert before cold flush"
            );
        }
        log_overlay_catalog_snapshot(client, relation, "before cold prime flush").await?;
        let cold_flush = flush_ctx.map(|ctx| FlushCtx {
            label: "overlay-cold-flush",
            storage_root: ctx.storage_root,
        });
        let first_flush = force_flush_table(client, relation, cold_flush).await?;
        anyhow::ensure!(
            first_flush > 0,
            "expected cold flush of overlay ids, flushed {first_flush}"
        );
        log_always(format!(
            "cold-delete overlay: dedicated cold prime flushed {first_flush} rows in {:.3}s",
            phase_started.elapsed().as_secs_f64()
        ));
    } else {
        log_always(
            "cold-delete overlay: reusing ids already in cold (skipping dedicated insert + force prime)",
        );
        for &id in ids {
            anyhow::ensure!(
                visible_pk_count_with_profile(client, relation, id, profile).await? == 1,
                "id {id} must be visible from cold before overlay rematerialize"
            );
        }
        log_always(format!(
            "cold-delete overlay: confirmed {} ids visible from cold in {:.3}s",
            ids.len(),
            phase_started.elapsed().as_secs_f64()
        ));
    }

    phase_started = Instant::now();
    for &id in ids {
        timed_async(
            format!("cold-delete overlay: rematerialize id {id}"),
            client.batch_execute(&rematerialize_sql(id)),
        )
        .await?;
        anyhow::ensure!(
            visible_pk_count_with_profile(client, relation, id, profile).await? == 1,
            "id {id} must be visible after rematerialize from cold"
        );

        let deleted = timed_async(
            format!("cold-delete overlay: DELETE id {id}"),
            client.execute(&format!("DELETE FROM {relation} WHERE id = $1"), &[&id]),
        )
        .await?;
        anyhow::ensure!(deleted == 1, "DELETE id {id} affected {deleted} rows");
        anyhow::ensure!(
            mirror_op(client, relation, id).await? == Some(3),
            "expected mirror tombstone op=3 for id {id}"
        );
        log_always(format!(
            "cold-delete overlay: hot DELETE recorded for id {id}"
        ));
    }
    log_always(format!(
        "cold-delete overlay: rematerialize+delete phase finished in {:.3}s",
        phase_started.elapsed().as_secs_f64()
    ));

    phase_started = Instant::now();
    log_overlay_catalog_snapshot(client, relation, "before tombstone flush").await?;
    let tombstone_flush_ctx = flush_ctx.map(|ctx| FlushCtx {
        label: "overlay-tombstone-flush",
        storage_root: ctx.storage_root,
    });
    let tombstone_flush = force_flush_table(client, relation, tombstone_flush_ctx).await?;
    anyhow::ensure!(
        tombstone_flush > 0,
        "expected tombstone flush after cold-then-hot deletes, flushed {tombstone_flush}"
    );
    log_always(format!(
        "cold-delete overlay: tombstone flush wrote {tombstone_flush} rows in {:.3}s",
        phase_started.elapsed().as_secs_f64()
    ));

    phase_started = Instant::now();
    log_always(format!(
        "cold-delete overlay: verifying {} deleted ids stay hidden (one batched merge scan)",
        ids.len()
    ));
    let visible = visible_pk_batch_count(client, relation, ids).await?;
    anyhow::ensure!(
        visible == 0,
        "expected all overlay ids hidden after tombstone flush, still visible: {visible}"
    );
    for &id in ids {
        log_always(format!(
            "cold-delete overlay: id {id} hidden by merge scan (visible=0)"
        ));
    }
    log_always(format!(
        "cold-delete overlay: batched visibility check finished in {:.3}s",
        phase_started.elapsed().as_secs_f64()
    ));

    let after_segments = e2e::cold_segment_count(client, relation).await?;
    anyhow::ensure!(
        after_segments >= before_segments,
        "delete+flush must not remove prior cold parquet"
    );
    log_overlay_catalog_snapshot(client, relation, "finish").await?;
    log_always(format!(
        "cold-delete overlay: finished with {after_segments} active cold segments in {:.3}s total",
        overlay_started.elapsed().as_secs_f64()
    ));

    let _ = scope_column;
    Ok(())
}

/// Asserts a merge-scan EXPLAIN for a filtered query lists cold sources.
///
/// # Errors
///
/// Returns an error when planning fails or the plan lacks cold reads.
pub async fn assert_merge_scan_uses_cold(
    client: &Client,
    relation: &str,
    filter_sql: &str,
    min_parquet_segments: usize,
) -> Result<()> {
    let sql = format!("SELECT * FROM {relation} WHERE {filter_sql}");
    let plan = timed_async(
        format!("EXPLAIN merge scan cold reads on {relation}"),
        e2e::explain(client, &sql),
    )
    .await?;
    let planned_segments = plan
        .lines()
        .filter(|line| line.contains("Parquet segment:") && !line.contains("none"))
        .count();
    log_always(format!(
        "EXPLAIN merge scan cold reads on {relation}: planned_parquet_segments={planned_segments}"
    ));
    e2e::assert_kold_merge_scan_cold_reads(&plan, "manifest.json", min_parquet_segments)?;
    Ok(())
}

/// Absolute path under an example storage root.
#[must_use]
#[allow(dead_code)]
pub fn storage_file(storage_root: &Path, relative: &str) -> PathBuf {
    storage_root.join(relative)
}

fn env_i64(name: &str, default: i64) -> i64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
        .max(1)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
        .max(1)
}
