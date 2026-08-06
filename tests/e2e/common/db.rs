//! Test database fixture layer.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use koldstore_storage::{ObjectStoreClient, StorageClient};
use tokio_postgres::Client;

use super::catalog;
use super::cluster::{PgTarget, PgrxServer};
use super::db_pool::DatabaseLease;
use super::minio::MinioConfig;

static NEXT_FIXTURE_ID: AtomicU64 = AtomicU64::new(1);

/// Managed table fixture.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedTable {
    /// Schema-qualified relation name.
    pub relation: String,
    /// Unqualified table name.
    pub table_name: String,
    /// Secondary title index name.
    pub title_index: String,
}

/// Cold storage backend used by a [`TestDb`] fixture.
#[derive(Debug, Clone)]
pub enum FixtureStorage {
    /// Local filesystem root under `storage_root`.
    Filesystem,
    /// S3-compatible MinIO storage scoped to `object_prefix`.
    Minio {
        /// MinIO connection settings.
        config: MinioConfig,
        /// Per-fixture object-store prefix under the bucket.
        object_prefix: String,
    },
}

/// Isolated pgrx-backed test database fixture.
#[derive(Debug)]
pub struct TestDb {
    /// Active PostgreSQL target (includes the claimed worker database name).
    pub target: PgTarget,
    /// Connected PostgreSQL client.
    pub client: Client,
    /// Unique schema for this test fixture.
    pub schema: String,
    /// Registered storage name.
    pub storage_name: String,
    /// Filesystem storage root (empty for MinIO fixtures).
    pub storage_root: PathBuf,
    /// Cold storage backend for this fixture.
    pub storage: FixtureStorage,
    /// Keeps the pooled worker database reserved until this fixture drops.
    _db_lease: DatabaseLease,
}

impl TestDb {
    /// Starts/connects to the pgrx server and creates an isolated schema plus filesystem storage.
    ///
    /// # Errors
    ///
    /// Returns an error when PostgreSQL, extension creation, schema creation, or
    /// storage registration fails.
    pub async fn start(target: PgTarget, label: &str) -> Result<Self> {
        let server = PgrxServer::start(target).await?;
        let schema = unique_identifier(label);
        let storage_name = format!("{schema}_storage");
        let storage_root = filesystem_storage_root(&schema)?;
        if storage_root.exists() {
            std::fs::remove_dir_all(&storage_root)
                .with_context(|| format!("remove {}", storage_root.display()))?;
        }
        std::fs::create_dir_all(&storage_root)
            .with_context(|| format!("create {}", storage_root.display()))?;

        server
            .client
            .batch_execute(&format!("CREATE SCHEMA {schema};"))
            .await
            .with_context(|| format!("create schema {schema}"))?;
        // Clear any database-level GUCs left by a prior crashed async test.
        let dbname: String = server
            .client
            .query_one("SELECT current_database()::text", &[])
            .await
            .context("read current database")?
            .get(0);
        reset_fixture_gucs(&server.client, &dbname).await?;
        // Running workers keep prior ALTER DATABASE GUCs until restart; bounce any
        // leftover applier so the next test inherits the reset defaults.
        let _ = super::async_mirror::terminate_async_worker(&server.client).await;
        register_filesystem_storage(&server.client, &storage_name, &storage_root).await?;

        Ok(Self {
            target: server.target,
            client: server.client,
            schema,
            storage_name,
            storage_root,
            storage: FixtureStorage::Filesystem,
            _db_lease: server._lease,
        })
    }

    /// Starts a fixture that registers S3/MinIO storage for cold objects.
    ///
    /// Requires `KOLDSTORE_MINIO=1` (or `KOLDSTORE_MINIO_ENDPOINT`) and a reachable
    /// MinIO with the configured bucket already created.
    ///
    /// # Errors
    ///
    /// Returns an error when MinIO is disabled/unreachable, or when PostgreSQL setup
    /// / storage registration fails.
    pub async fn start_minio(target: PgTarget, label: &str) -> Result<Self> {
        let config = MinioConfig::require()?;
        let server = PgrxServer::start(target).await?;
        let schema = unique_identifier(label);
        let storage_name = format!("{schema}_storage");
        let object_prefix = schema.clone();
        config
            .probe(&object_prefix)
            .context("MinIO must be reachable before S3-backed E2E fixtures start")?;

        server
            .client
            .batch_execute(&format!("CREATE SCHEMA {schema};"))
            .await
            .with_context(|| format!("create schema {schema}"))?;
        let dbname: String = server
            .client
            .query_one("SELECT current_database()::text", &[])
            .await
            .context("read current database")?
            .get(0);
        reset_fixture_gucs(&server.client, &dbname).await?;
        register_minio_storage(&server.client, &storage_name, &object_prefix, &config).await?;

        Ok(Self {
            target: server.target,
            client: server.client,
            schema,
            storage_name,
            storage_root: PathBuf::new(),
            storage: FixtureStorage::Minio {
                config,
                object_prefix,
            },
            _db_lease: server._lease,
        })
    }

    /// Starts a fixture for the first active pgrx target.
    ///
    /// # Errors
    ///
    /// Returns an error when no target exists or setup fails.
    pub async fn start_default(label: &str) -> Result<Self> {
        let target = super::cluster::local_pg_matrix()
            .into_iter()
            .next()
            .context("no local pg target configured")?;
        Self::start(target, label).await
    }

    /// Builds a schema-qualified relation name in this fixture.
    #[must_use]
    pub fn relation(&self, table_name: &str) -> String {
        format!("{}.{}", self.schema, table_name)
    }

    /// Creates a disposable non-owner role named `{schema}_app`.
    ///
    /// Roles are cluster-global and survive pooled-DB resets, so this drops any
    /// leftover role from a prior PID/counter collision before creating.
    ///
    /// # Errors
    ///
    /// Returns an error when role DDL fails.
    pub async fn ensure_app_role(&self) -> Result<String> {
        let app_role = format!("{}_app", self.schema);
        self.client
            .batch_execute(&format!(
                "DROP ROLE IF EXISTS {app_role}; CREATE ROLE {app_role};"
            ))
            .await
            .with_context(|| format!("ensure app role {app_role}"))?;
        Ok(app_role)
    }

    /// Creates and populates an indexed fixture table.
    ///
    /// # Errors
    ///
    /// Returns an error when the DDL or seed SQL fails.
    pub async fn create_indexed_items_table(
        &self,
        table_name: &str,
        rows: i64,
    ) -> Result<ManagedTable> {
        let relation = self.relation(table_name);
        let title_index = format!("{}_title_idx", table_name);
        let qty_index = format!("{}_qty_idx", table_name);
        self.client
            .batch_execute(&format!(
                r#"
                CREATE TABLE {relation} (
                  id bigint PRIMARY KEY,
                  account_id bigint NOT NULL,
                  title text NOT NULL,
                  qty integer NOT NULL,
                  category text NOT NULL,
                  created_at timestamptz NOT NULL DEFAULT now(),
                  CHECK (qty >= 0)
                );
                CREATE INDEX {title_index} ON {relation} (title);
                CREATE INDEX {qty_index} ON {relation} (qty);
                INSERT INTO {relation} (id, account_id, title, qty, category)
                SELECT
                  gs::bigint,
                  (gs % 17)::bigint,
                  'item-' || lpad(gs::text, 6, '0'),
                  (gs % 100)::integer,
                  CASE WHEN gs % 2 = 0 THEN 'even' ELSE 'odd' END
                FROM generate_series(1, {rows}) AS gs;
                ANALYZE {relation};
                "#
            ))
            .await?;
        Ok(ManagedTable {
            relation,
            table_name: table_name.to_string(),
            title_index,
        })
    }

    /// Manages a table as a shared managed table using the fixture storage.
    ///
    /// # Errors
    ///
    /// Returns an error when management fails.
    pub async fn manage_shared(&self, relation: &str, migration_order_by: &str) -> Result<()> {
        self.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name     => $1::text::regclass,
                  storage        => $2,
                  hot_row_limit  => NULL,
                  migration_order_by => $3
                )
                "#,
                &[&relation, &self.storage_name, &migration_order_by],
            )
            .await?;
        catalog::assert_system_columns_absent(&self.client, relation).await?;
        catalog::assert_change_log_mirror_exists(
            &self.client,
            &format!(
                "koldstore.{}__cl",
                relation.rsplit('.').next().unwrap_or(relation)
            ),
        )
        .await?;
        catalog::assert_catalog_has_active_schema(&self.client, relation).await?;
        Ok(())
    }

    /// Manages a table as a user-scoped managed table.
    ///
    /// # Errors
    ///
    /// Returns an error when management fails.
    pub async fn manage_user_scoped(&self, relation: &str, scope_column: &str) -> Result<()> {
        self.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name     => $1::text::regclass,
                  storage        => $2,
                  hot_row_limit  => NULL,
                  table_type     => 'user',
                  scope_column   => $3,
                  migration_order_by => 'id'
                )
                "#,
                &[&relation, &self.storage_name, &scope_column],
            )
            .await?;
        catalog::assert_system_columns_absent(&self.client, relation).await?;
        catalog::assert_change_log_mirror_exists(
            &self.client,
            &format!(
                "koldstore.{}__cl",
                relation.rsplit('.').next().unwrap_or(relation)
            ),
        )
        .await?;
        catalog::assert_catalog_has_active_schema(&self.client, relation).await?;
        Ok(())
    }

    /// Flushes a managed table and returns the number of hot rows written.
    ///
    /// Retries when the shared async-mirror apply lock is briefly held (fail-fast
    /// product contract). Tests that assert lock contention must call
    /// `flush_table` SQL directly instead of this helper.
    ///
    /// # Errors
    ///
    /// Returns an error when `koldstore.flush_table` fails.
    pub async fn flush_table(&self, relation: &str) -> Result<i64> {
        self.flush_table_with_force(relation, false).await
    }

    /// Like [`Self::flush_table`], optionally forcing a flush.
    ///
    /// # Errors
    ///
    /// Returns an error when flush fails after apply-lock retries.
    pub async fn flush_table_with_force(&self, relation: &str, force: bool) -> Result<i64> {
        // Policy flush decides from mirror pending counts; catch up WAL apply so
        // recently committed DML is visible to the due check.
        super::async_mirror::fence_async_mirror(&self.client).await?;
        let job_id = flush_table_job_id(&self.client, relation, force).await?;
        wait_for_flush_job_terminal(&self.client, &job_id).await
    }

    /// Creates a user-scoped notes table and seeds rows for two tenants.
    ///
    /// # Errors
    ///
    /// Returns an error when setup fails.
    pub async fn create_user_notes_table(&self, table_name: &str) -> Result<ManagedTable> {
        let relation = self.relation(table_name);
        let title_index = format!("{}_tenant_title_idx", table_name);
        self.client
            .batch_execute(&format!(
                r#"
                CREATE TABLE {relation} (
                  id bigint PRIMARY KEY,
                  user_id text NOT NULL,
                  title text NOT NULL,
                  body text NOT NULL
                );
                CREATE INDEX {title_index} ON {relation} (user_id, title);
                INSERT INTO {relation} (id, user_id, title, body)
                VALUES
                  (1, 'user-a', 'alpha', 'a1'),
                  (2, 'user-a', 'beta', 'a2'),
                  (3, 'user-b', 'alpha', 'b1');
                ANALYZE {relation};
                "#
            ))
            .await?;
        Ok(ManagedTable {
            relation,
            table_name: table_name.to_string(),
            title_index,
        })
    }

    /// Enqueues a flush job (or returns the existing active job UUID as text).
    ///
    /// # Errors
    ///
    /// Returns an error when the enqueue fails.
    pub async fn insert_pending_flush_job(&self, relation: &str) -> Result<String> {
        let row = self
            .client
            .query_one(
                r#"
                SELECT koldstore.enqueue_flush_job(
                  table_name => $1::text::regclass,
                  force      => false
                )::text
                "#,
                &[&relation],
            )
            .await?;
        Ok(row.get(0))
    }

    /// Opens an object-store client for this fixture's MinIO prefix.
    ///
    /// # Errors
    ///
    /// Returns an error when the fixture is filesystem-backed or MinIO cannot open.
    pub fn minio_client(&self) -> Result<ObjectStoreClient> {
        match &self.storage {
            FixtureStorage::Minio {
                config,
                object_prefix,
            } => config.open_client(object_prefix),
            FixtureStorage::Filesystem => {
                anyhow::bail!("minio_client requires a MinIO-backed TestDb fixture")
            }
        }
    }

    /// Asserts catalog cold paths exist as objects in MinIO and returns their keys.
    ///
    /// # Errors
    ///
    /// Returns an error when catalog rows are missing or MinIO objects are absent.
    pub async fn assert_minio_cold_artifacts(
        &self,
        relation: &str,
        expected_rows: i64,
    ) -> Result<(String, String)> {
        let artifact = self
            .client
            .query_one(
                &format!(
                    r#"
                SELECT
                  {manifest},
                  {object},
                  cs.row_count,
                  cs.byte_size
                FROM koldstore.manifest m
                JOIN pg_class c ON c.oid = m.table_oid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN koldstore.cold_segments cs
                  ON cs.table_oid = m.table_oid
                 AND cs.scope_key = m.scope_key
                WHERE m.table_oid = $1::text::regclass::oid
                  AND m.generation > 0
                  AND m.sync_state = 'in_sync'
                  AND cs.status = 'active'
                ORDER BY cs.batch_number
                LIMIT 1
                "#,
                    manifest = super::SQL_DEFAULT_MANIFEST_OBJECT_KEY,
                    object = super::SQL_DEFAULT_COLD_OBJECT_KEY,
                ),
                &[&relation],
            )
            .await
            .with_context(|| format!("load cold catalog rows for {relation}"))?;
        let manifest_path = artifact.get::<_, String>(0);
        let object_path = artifact.get::<_, String>(1);
        assert_eq!(artifact.get::<_, i64>(2), expected_rows);
        assert!(artifact.get::<_, i64>(3) > 0);

        let client = self.minio_client()?;
        let listing = client
            .list("")
            .context("list MinIO objects for fixture prefix")?;
        let listing_text = listing
            .iter()
            .map(|object| object.key.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        super::assertions::assert_minio_listing_contains(&listing_text, &manifest_path)?;
        super::assertions::assert_minio_listing_contains(&listing_text, &object_path)?;

        let manifest_bytes = client
            .get(&manifest_path)
            .with_context(|| format!("get MinIO manifest {manifest_path}"))?;
        anyhow::ensure!(
            !manifest_bytes.is_empty(),
            "MinIO manifest {manifest_path} is empty"
        );
        let parquet_bytes = client
            .get(&object_path)
            .with_context(|| format!("get MinIO parquet {object_path}"))?;
        anyhow::ensure!(
            !parquet_bytes.is_empty(),
            "MinIO parquet {object_path} is empty"
        );

        Ok((manifest_path, object_path))
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        let keep = std::env::var("KOLDSTORE_E2E_KEEP_STORAGE")
            .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes"))
            .unwrap_or(false);
        if matches!(self.storage, FixtureStorage::Filesystem)
            && !self.storage_root.as_os_str().is_empty()
            && !keep
        {
            let _ = std::fs::remove_dir_all(&self.storage_root);
        }
        if let FixtureStorage::Minio {
            config,
            object_prefix,
        } = &self.storage
        {
            if let Ok(client) = config.open_client(object_prefix) {
                if let Ok(objects) = client.list("") {
                    for object in objects {
                        let _ = client.delete(&object.key);
                    }
                }
            }
        }
    }
}

/// Resolves the filesystem cold-storage root for a fixture.
///
/// When `KOLDSTORE_E2E_STORAGE_ROOT` is set, objects are written under
/// `{STORAGE_ROOT}/{schema}` so callers can point at a project-local directory.
/// Otherwise uses the process temp dir (`pg-koldstore-e2e-{schema}`).
fn filesystem_storage_root(schema: &str) -> Result<PathBuf> {
    if let Ok(root) = std::env::var("KOLDSTORE_E2E_STORAGE_ROOT") {
        let root = PathBuf::from(root);
        return Ok(root.join(schema));
    }
    Ok(std::env::temp_dir().join(format!("pg-koldstore-e2e-{schema}")))
}

async fn register_filesystem_storage(
    client: &Client,
    storage_name: &str,
    storage_root: &Path,
) -> Result<()> {
    let root = storage_root
        .to_str()
        .context("storage root must be valid utf-8")?;
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
            &[&storage_name, &root],
        )
        .await?;
    Ok(())
}

async fn register_minio_storage(
    client: &Client,
    storage_name: &str,
    object_prefix: &str,
    config: &MinioConfig,
) -> Result<()> {
    let base_path = config.base_path_for_prefix(object_prefix);
    let credentials = config.credentials_json().to_string();
    let storage_config = config.config_json().to_string();
    // tokio-postgres is not built with the `with-serde_json-1` feature, so pass
    // JSON as SQL literals (same pattern as other E2E storage registrations).
    let sql = format!(
        r#"
        SELECT koldstore.register_storage(
          $1,
          's3',
          $2,
          '{credentials}'::jsonb,
          '{storage_config}'::jsonb
        )
        "#,
        credentials = credentials.replace('\'', "''"),
        storage_config = storage_config.replace('\'', "''"),
    );
    client
        .execute(&sql, &[&storage_name, &base_path])
        .await
        .with_context(|| format!("register MinIO storage {storage_name} at {base_path}"))?;
    Ok(())
}

fn unique_identifier(label: &str) -> String {
    let id = NEXT_FIXTURE_ID.fetch_add(1, Ordering::SeqCst);
    let sanitized = label
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    format!("e2e_{}_{}_{}", sanitized, std::process::id(), id)
}

/// True when flush failed because a fail-fast entry lock is busy.
///
/// Covers the database slot lock and the per-table job lock. Both return
/// immediately from `flush_table` so callers can retry instead of hanging.
#[must_use]
pub fn is_flush_entry_lock_busy(error: &tokio_postgres::Error) -> bool {
    // `Display` for tokio_postgres is often just "db error"; inspect the DB
    // message / debug form so fail-fast lock text is visible.
    let mut text = format!("{error:?}");
    if let Some(db) = error.as_db_error() {
        text.push(' ');
        text.push_str(db.message());
        if let Some(detail) = db.detail() {
            text.push(' ');
            text.push_str(detail);
        }
    }
    text.contains("apply lock")
        || text.contains("retry shortly")
        || text.contains("flush unavailable")
}

/// Clears leftover per-database GUCs and forces synchronous flush for E2E.
///
/// Production default is `flush_execution=queue`. Session-armed failpoints and
/// most E2E assertions need the calling backend to run flush (`inline`), and
/// peer connections inherit the database-level setting.
async fn reset_fixture_gucs(client: &Client, dbname: &str) -> Result<()> {
    client
        .batch_execute(&format!(
            "ALTER DATABASE \"{dbname}\" RESET koldstore.failpoint; \
             ALTER DATABASE \"{dbname}\" RESET koldstore.internal_async_mirror_worker; \
             ALTER DATABASE \"{dbname}\" RESET koldstore.flush_check_interval_seconds; \
             ALTER DATABASE \"{dbname}\" RESET koldstore.async_apply_watchdog_interval_ms; \
             ALTER DATABASE \"{dbname}\" SET koldstore.flush_execution = 'inline'; \
             RESET koldstore.failpoint; \
             RESET koldstore.internal_async_mirror_worker; \
             RESET koldstore.flush_check_interval_seconds; \
             RESET koldstore.async_apply_watchdog_interval_ms; \
             SET koldstore.flush_execution = 'inline'; \
             UPDATE koldstore.schemas \
               SET active = false \
             WHERE active"
        ))
        .await
        .context("reset leftover GUC / schema state for fixture")?;
    Ok(())
}

/// Polls until a flush job reaches a terminal status; returns `rows_flushed`.
///
/// # Errors
///
/// Returns an error when the job ends in `error`/`cancelled`, or times out while
/// still `pending`/`running` (queue mode without a live executor).
pub async fn wait_for_flush_job_terminal(client: &Client, job_id: &str) -> Result<i64> {
    for attempt in 1..=200 {
        let row = client
            .query_one(
                r#"
                SELECT rows_flushed, status, coalesce(error_trace, '')
                FROM koldstore.jobs
                WHERE id = $1::text::uuid
                "#,
                &[&job_id],
            )
            .await
            .with_context(|| format!("lookup flush job {job_id}"))?;
        let rows_flushed: i64 = row.get(0);
        let status: String = row.get(1);
        let error_trace: String = row.get(2);
        match status.as_str() {
            "completed" => return Ok(rows_flushed),
            "error" | "cancelled" => {
                anyhow::bail!(
                    "flush job {job_id} finished status={status} rows_flushed={rows_flushed}: {error_trace}"
                );
            }
            "pending" | "running" => {
                tokio::time::sleep(std::time::Duration::from_millis(
                    25 * attempt.min(20) as u64,
                ))
                .await;
            }
            other => {
                anyhow::bail!("flush job {job_id} unexpected status={other}");
            }
        }
    }
    anyhow::bail!("flush job {job_id} still active after wait budget")
}

/// Runs `koldstore.flush_table` and returns the job id, retrying apply-lock busy.
///
/// # Errors
///
/// Returns an error when flush fails for a non-lock reason, or the lock stays
/// busy across the retry budget.
pub async fn flush_table_job_id(client: &Client, relation: &str, force: bool) -> Result<String> {
    for attempt in 1..=40 {
        let result = if force {
            client
                .query_one(
                    "SELECT koldstore.flush_table($1::text::regclass, true)::text",
                    &[&relation],
                )
                .await
        } else {
            client
                .query_one(
                    "SELECT koldstore.flush_table($1::text::regclass)::text",
                    &[&relation],
                )
                .await
        };
        match result {
            Ok(row) => {
                let job_id: Option<String> = row.get(0);
                match job_id.filter(|value| !value.is_empty() && value != "null") {
                    Some(job_id) => return Ok(job_id),
                    None => anyhow::bail!(
                        "flush_table returned NULL for {relation} (force={force}); \
                         no flush work due (excess below max_rows_per_file / min_flush_rows)"
                    ),
                }
            }
            Err(error) if is_flush_entry_lock_busy(&error) => {
                tokio::time::sleep(std::time::Duration::from_millis(50 * attempt as u64)).await;
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("flush_table {relation} (force={force})"));
            }
        }
    }
    anyhow::bail!("flush_table still blocked by apply lock after retries for {relation}")
}
