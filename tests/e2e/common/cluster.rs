//! Local pgrx PostgreSQL target discovery and startup.

use std::process::Command;
use std::sync::OnceLock;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio_postgres::{Client, NoTls};

use super::db_pool::{
    claim_database, e2e_db_pool_enabled, ensure_pool_config, shared_database_name,
    worker_database_name, DatabaseLease,
};

const FAST_CONNECT_ATTEMPTS: usize = 3;
const FAST_CONNECT_DELAY: Duration = Duration::from_millis(200);
const STARTUP_CONNECT_ATTEMPTS: usize = 30;
const STARTUP_CONNECT_DELAY: Duration = Duration::from_secs(1);

/// PostgreSQL target for one matrix entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgTarget {
    /// PostgreSQL major version.
    pub version: u16,
    /// TCP port.
    pub port: u16,
    /// Database name (pooled worker DB or shared fallback).
    pub dbname: String,
}

impl PgTarget {
    /// Builds a matrix target that still needs a concrete database assignment.
    #[must_use]
    pub fn new(version: u16, port: u16) -> Self {
        Self {
            version,
            port,
            // Placeholder replaced when a fixture claims a pooled database.
            dbname: shared_database_name(),
        }
    }

    /// Builds a libpq connection string for this target's database.
    #[must_use]
    pub fn connection_string(&self) -> String {
        let host =
            std::env::var("KOLDSTORE_E2E_PGHOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        let user = std::env::var("KOLDSTORE_E2E_PGUSER")
            .unwrap_or_else(|_| std::env::var("USER").unwrap_or_else(|_| "postgres".to_string()));
        let password = std::env::var("KOLDSTORE_E2E_PGPASSWORD")
            .ok()
            .filter(|password| !password.is_empty())
            .map(|password| format!(" password={password}"))
            .unwrap_or_default();

        format!(
            "host={host} port={} user={user}{password} dbname={}",
            self.port, self.dbname
        )
    }
}

/// A started local pgrx PostgreSQL server plus a ready client connection.
#[derive(Debug)]
pub struct PgrxServer {
    /// Target this server represents.
    pub target: PgTarget,
    /// Connected client.
    pub client: Client,
    /// Pool lease keeping this fixture's database reserved.
    pub(crate) _lease: DatabaseLease,
}

impl PgrxServer {
    /// Starts the pgrx-managed server when requested and waits for connections.
    ///
    /// The normal E2E path starts and installs the extension in
    /// `scripts/run-pg-e2e.sh`. Setting `KOLDSTORE_E2E_START_PGRX=1` makes
    /// this helper run `cargo pgrx start pgN` before connecting, which is useful
    /// for a single test binary during local development.
    ///
    /// Claims one pooled worker database when `KOLDSTORE_E2E_DB_POOL=1`.
    ///
    /// # Errors
    ///
    /// Returns an error when `cargo pgrx start` fails, the pool is exhausted, or
    /// PostgreSQL cannot be reached.
    pub async fn start(target: PgTarget) -> Result<Self> {
        ensure_pool_config()?;
        maybe_start_pgrx(&target)?;
        let (dbname, lease) = claim_database()?;
        let target = PgTarget { dbname, ..target };
        let client = wait_for_postgres(&target).await?;
        ensure_koldstore_extension(&client).await?;
        Ok(Self {
            target,
            client,
            _lease: lease,
        })
    }
}

/// Known local PostgreSQL matrix.
#[must_use]
pub fn local_pg_matrix() -> Vec<PgTarget> {
    if let Ok(port) = std::env::var("KOLDSTORE_E2E_PGPORT") {
        let version = std::env::var("KOLDSTORE_E2E_PGVERSION")
            .ok()
            .and_then(|version| version.parse().ok())
            .unwrap_or(16);
        return vec![PgTarget::new(
            version,
            port.parse().expect("KOLDSTORE_E2E_PGPORT must be a u16"),
        )];
    }

    if let Ok(versions) = std::env::var("KOLDSTORE_E2E_PGVERSIONS") {
        let targets = versions
            .split(',')
            .map(str::trim)
            .filter(|version| !version.is_empty())
            .map(|version| {
                let version = version
                    .parse::<u16>()
                    .expect("KOLDSTORE_E2E_PGVERSIONS must contain PostgreSQL major versions");
                PgTarget::new(version, 28800 + version)
            })
            .collect::<Vec<_>>();
        assert!(
            !targets.is_empty(),
            "KOLDSTORE_E2E_PGVERSIONS must configure at least one PostgreSQL target"
        );
        return targets;
    }

    vec![PgTarget::new(16, 28816)]
}

/// PostgreSQL targets for scenario tests.
///
/// # Panics
///
/// Panics when no target is configured. This keeps `for target in ...` scenario
/// tests from silently passing without executing any PostgreSQL work.
#[must_use]
pub fn scenario_pg_matrix() -> Vec<PgTarget> {
    let targets = local_pg_matrix();
    assert!(
        !targets.is_empty(),
        "E2E scenario matrix must contain at least one PostgreSQL target"
    );
    targets
}

/// Expected PostgreSQL versions for the active test target mode.
#[must_use]
pub fn expected_pg_versions() -> Vec<u16> {
    local_pg_matrix()
        .into_iter()
        .map(|target| target.version)
        .collect()
}

/// Expected PostgreSQL ports for the active test target mode.
#[must_use]
pub fn expected_pg_ports() -> Vec<u16> {
    local_pg_matrix()
        .into_iter()
        .map(|target| target.port)
        .collect()
}

/// Verifies every configured E2E target is reachable and has `koldstore` installed.
///
/// # Errors
///
/// Returns an error when PostgreSQL is unreachable, the server version/port do not
/// match the configured target, or the extension is missing.
pub async fn require_pgrx_server() -> Result<()> {
    ensure_pool_config()?;
    let targets = local_pg_matrix();
    anyhow::ensure!(
        !targets.is_empty(),
        "no E2E PostgreSQL target configured; set KOLDSTORE_E2E_PGPORT or KOLDSTORE_E2E_PGVERSIONS"
    );

    for target in &targets {
        let probe = probe_target(target);
        let client =
            connect_with_retries(&probe, FAST_CONNECT_ATTEMPTS, FAST_CONNECT_DELAY).await?;
        verify_pgrx_target(&client, &probe).await?;
    }

    Ok(())
}

fn probe_target(target: &PgTarget) -> PgTarget {
    let dbname = if e2e_db_pool_enabled() {
        worker_database_name(0)
    } else {
        shared_database_name()
    };
    PgTarget {
        dbname,
        version: target.version,
        port: target.port,
    }
}

/// Synchronous wrapper used by non-async E2E tests.
///
/// # Errors
///
/// Returns an error when [`require_pgrx_server`] fails.
pub fn require_pgrx_server_sync() -> Result<()> {
    static GATE: OnceLock<Result<(), String>> = OnceLock::new();
    GATE.get_or_init(|| {
        match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime
                .block_on(require_pgrx_server())
                .map_err(|error| error.to_string()),
            Err(error) => Err(error.to_string()),
        }
    })
    .as_ref()
    .map_err(|error| anyhow::anyhow!("{error}"))
    .map(|_| ())
}

/// Connects to PostgreSQL with tokio-postgres.
///
/// # Errors
///
/// Returns an error when the target cannot be reached.
pub async fn connect(target: &PgTarget) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(&target.connection_string(), NoTls)
        .await
        .with_context(|| {
            format!(
                "connect PostgreSQL {} on port {} db={}",
                target.version, target.port, target.dbname
            )
        })?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

/// Waits until PostgreSQL accepts connections.
///
/// # Errors
///
/// Returns an error if the target is not ready before the retry budget is exhausted.
pub async fn wait_for_postgres(target: &PgTarget) -> Result<Client> {
    let (attempts, delay) = connect_retry_budget();
    let client = connect_with_retries(target, attempts, delay).await?;
    verify_pgrx_target(&client, target).await?;
    Ok(client)
}

async fn connect_with_retries(
    target: &PgTarget,
    attempts: usize,
    delay: Duration,
) -> Result<Client> {
    let mut last_error = None;
    for _ in 0..attempts {
        match connect(target).await {
            Ok(client) => return Ok(client),
            Err(error) => {
                last_error = Some(error);
                tokio::time::sleep(delay).await;
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        anyhow::anyhow!(
            "PostgreSQL {} on port {} db={} did not become ready",
            target.version,
            target.port,
            target.dbname
        )
    }))
}

async fn verify_pgrx_target(client: &Client, target: &PgTarget) -> Result<()> {
    let server_version: String = client
        .query_one("SHOW server_version", &[])
        .await
        .context("read PostgreSQL server_version")?
        .get(0);
    anyhow::ensure!(
        server_version.starts_with(&target.version.to_string()),
        "PostgreSQL server on port {} reported version `{server_version}`, expected major {}",
        target.port,
        target.version
    );

    let server_port: i32 = client
        .query_one(
            "SELECT setting::integer FROM pg_settings WHERE name = 'port'",
            &[],
        )
        .await
        .context("read PostgreSQL port setting")?
        .get(0);
    anyhow::ensure!(
        u16::try_from(server_port).ok() == Some(target.port),
        "PostgreSQL server on port {} reported server port {server_port}",
        target.port
    );

    client
        .batch_execute("CREATE EXTENSION IF NOT EXISTS koldstore;")
        .await
        .context("install koldstore extension for E2E database")?;
    sync_koldstore_extension_sql(client).await?;

    let extension_present = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'koldstore')",
            &[],
        )
        .await
        .context("check koldstore extension")?
        .get::<_, bool>(0);
    anyhow::ensure!(
        extension_present,
        "koldstore extension is not available on PostgreSQL {}:{} db={}",
        target.version,
        target.port,
        target.dbname
    );

    Ok(())
}

async fn ensure_koldstore_extension(client: &Client) -> Result<()> {
    client
        .batch_execute("CREATE EXTENSION IF NOT EXISTS koldstore;")
        .await
        .context("create koldstore extension")?;
    sync_koldstore_extension_sql(client).await
}

/// Ensures the installed extension SQL matches the currently built `cargo pgrx install` artifacts.
///
/// `ALTER EXTENSION ... UPDATE` only applies when the extension version changes, so local
/// iterative development can leave an older SQL catalog behind. When required entities such
/// as `koldstore.describe_table` are missing, reinstall the extension in-place.
async fn sync_koldstore_extension_sql(client: &Client) -> Result<()> {
    let required_sql_present = client
        .query_one(
            r#"
            SELECT EXISTS (
              SELECT 1
              FROM pg_proc status_proc
              JOIN pg_namespace status_ns ON status_ns.oid = status_proc.pronamespace
              WHERE status_ns.nspname = 'koldstore'
                AND status_proc.proname = 'describe_table'
            )
            AND EXISTS (
              SELECT 1
              FROM pg_proc migrate_proc
              JOIN pg_namespace migrate_ns ON migrate_ns.oid = migrate_proc.pronamespace
              JOIN pg_proc migrate_args ON migrate_args.oid = migrate_proc.oid
              WHERE migrate_ns.nspname = 'koldstore'
                AND migrate_proc.proname = 'manage_table'
                AND migrate_proc.prorettype = 'uuid'::regtype
                AND 'migration_order_by' = ANY (migrate_proc.proargnames)
                AND 'target_file_size_mb' = ANY (migrate_proc.proargnames)
            )
            AND EXISTS (
              SELECT 1
              FROM pg_proc flush_proc
              JOIN pg_namespace flush_ns ON flush_ns.oid = flush_proc.pronamespace
              WHERE flush_ns.nspname = 'koldstore'
                AND flush_proc.proname = 'flush_table'
                AND flush_proc.prorettype = 'uuid'::regtype
                AND 'force' = ANY (flush_proc.proargnames)
            )
            "#,
            &[],
        )
        .await
        .context("check koldstore extension SQL availability")?
        .get::<_, bool>(0);

    if required_sql_present {
        client
            .batch_execute("ALTER EXTENSION koldstore UPDATE;")
            .await
            .context("refresh koldstore extension SQL after install")?;
        return Ok(());
    }

    client
        .batch_execute(
            r#"
            DROP EXTENSION IF EXISTS koldstore CASCADE;
            CREATE EXTENSION koldstore;
            "#,
        )
        .await
        .context("reinstall koldstore extension to pick up new SQL entities")?;
    Ok(())
}

fn connect_retry_budget() -> (usize, Duration) {
    let wait_for_startup = std::env::var("KOLDSTORE_E2E_WAIT_FOR_STARTUP")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes"))
        .unwrap_or(false);
    if wait_for_startup {
        (STARTUP_CONNECT_ATTEMPTS, STARTUP_CONNECT_DELAY)
    } else {
        (FAST_CONNECT_ATTEMPTS, FAST_CONNECT_DELAY)
    }
}

fn maybe_start_pgrx(target: &PgTarget) -> Result<()> {
    let should_start = std::env::var("KOLDSTORE_E2E_START_PGRX")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes"))
        .unwrap_or(false);
    if !should_start {
        return Ok(());
    }

    let status = Command::new("cargo")
        .args(["pgrx", "start", &format!("pg{}", target.version)])
        .status()
        .context("run cargo pgrx start")?;
    anyhow::ensure!(status.success(), "cargo pgrx start failed with {status}");
    Ok(())
}
