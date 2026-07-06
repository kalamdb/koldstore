//! Test database fixture layer.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use tokio_postgres::Client;

use super::catalog;
use super::cluster::{PgTarget, PgrxServer};

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
    /// Filesystem storage root.
    pub storage_root: PathBuf,
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

    /// Migrates a table as a shared managed table using the fixture storage.
    ///
    /// # Errors
    ///
    /// Returns an error when migration fails.
    pub async fn migrate_shared(&self, relation: &str, order_column: &str) -> Result<()> {
        self.client
            .execute(
                r#"
                SELECT koldstore.migrate_table(
                  $1::text::regclass,
                  'shared',
                  $2,
                  NULL,
                  NULL,
                  $3
                )
                "#,
                &[&relation, &self.storage_name, &order_column],
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

    /// Migrates a table as a user-scoped managed table.
    ///
    /// # Errors
    ///
    /// Returns an error when migration fails.
    pub async fn migrate_user_scoped(&self, relation: &str, scope_column: &str) -> Result<()> {
        self.client
            .execute(
                r#"
                SELECT koldstore.migrate_table(
                  $1::text::regclass,
                  'user',
                  $2,
                  NULL,
                  $3,
                  'id'
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
                "SELECT koldstore.flush_table($1::text::regclass)",
                &[&relation],
            )
            .await?;
        Ok(row.get(0))
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

    /// Inserts one pending flush job for a relation/scope.
    ///
    /// # Errors
    ///
    /// Returns an error when the insert fails.
    pub async fn insert_pending_flush_job(&self, relation: &str, scope_key: &str) -> Result<i64> {
        let row = self
            .client
            .query_one(
                r#"
                SELECT koldstore.enqueue_flush_job(
                  $1::text::regclass,
                  $2,
                  false
                )
                "#,
                &[&relation, &scope_key],
            )
            .await?;
        Ok(row.get(0))
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.storage_root);
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
