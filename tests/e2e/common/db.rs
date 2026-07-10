//! Test database fixture layer.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use koldstore_storage::{ObjectStoreClient, StorageClient};
use tokio_postgres::Client;

use super::catalog;
use super::cluster::{PgTarget, PgrxServer};
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
    /// Active PostgreSQL target.
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
        let storage_root = std::env::temp_dir().join(format!("pg-koldstore-e2e-{schema}"));
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
        register_filesystem_storage(&server.client, &storage_name, &storage_root).await?;

        Ok(Self {
            target: server.target,
            client: server.client,
            schema,
            storage_name,
            storage_root,
            storage: FixtureStorage::Filesystem,
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
    /// # Errors
    ///
    /// Returns an error when `koldstore.flush_table` fails.
    pub async fn flush_table(&self, relation: &str) -> Result<i64> {
        let row = self
            .client
            .query_one(
                "SELECT koldstore.flush_table($1::text::regclass)::text",
                &[&relation],
            )
            .await?;
        let job_id: String = row.get(0);
        let progress = self
            .client
            .query_one(
                "SELECT rows_flushed FROM koldstore.jobs WHERE id = $1::text::uuid",
                &[&job_id],
            )
            .await?;
        Ok(progress.get(0))
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

    /// Inserts one pending flush job for a relation.
    ///
    /// # Errors
    ///
    /// Returns an error when the insert fails.
    pub async fn insert_pending_flush_job(&self, relation: &str) -> Result<i64> {
        let row = self
            .client
            .query_one(
                r#"
                SELECT koldstore.enqueue_flush_job(
                  table_name => $1::text::regclass,
                  force      => false
                )
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
                r#"
                SELECT m.manifest_path, cs.object_path, cs.row_count, cs.byte_size
                FROM koldstore.manifest m
                JOIN koldstore.cold_segments cs
                  ON cs.table_oid = m.table_oid
                 AND cs.scope_key = m.scope_key
                WHERE m.table_oid = $1::text::regclass::oid
                  AND m.sync_state = 'in_sync'
                  AND cs.status = 'active'
                ORDER BY cs.batch_number
                LIMIT 1
                "#,
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
        if matches!(self.storage, FixtureStorage::Filesystem)
            && !self.storage_root.as_os_str().is_empty()
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
