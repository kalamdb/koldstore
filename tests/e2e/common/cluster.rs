//! Local pgrx PostgreSQL target discovery and startup.

use std::process::Command;
use std::sync::OnceLock;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio_postgres::{Client, NoTls};

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
}

impl PgTarget {
    /// Builds a libpq connection string.
    #[must_use]
    pub fn connection_string(&self) -> String {
        let host =
            std::env::var("KOLDSTORE_E2E_PGHOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        let user = std::env::var("KOLDSTORE_E2E_PGUSER")
            .unwrap_or_else(|_| std::env::var("USER").unwrap_or_else(|_| "postgres".to_string()));
        let dbname = std::env::var("KOLDSTORE_E2E_PGDATABASE")
            .unwrap_or_else(|_| "koldstore_pgrx_e2e".to_string());
        let password = std::env::var("KOLDSTORE_E2E_PGPASSWORD")
            .ok()
            .filter(|password| !password.is_empty())
            .map(|password| format!(" password={password}"))
            .unwrap_or_default();

        format!(
            "host={host} port={} user={user}{password} dbname={dbname}",
            self.port
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
}

impl PgrxServer {
    /// Starts the pgrx-managed server when requested and waits for connections.
    ///
    /// The normal E2E path starts and installs the extension in
    /// `scripts/run-pg-e2e.sh`. Setting `KOLDSTORE_E2E_START_PGRX=1` makes
    /// this helper run `cargo pgrx start pgN` before connecting, which is useful
    /// for a single test binary during local development.
    ///
    /// # Errors
    ///
    /// Returns an error when `cargo pgrx start` fails or PostgreSQL cannot be
    /// reached.
    pub async fn start(target: PgTarget) -> Result<Self> {
        maybe_start_pgrx(&target)?;
        let client = wait_for_postgres(&target).await?;
        client
            .batch_execute("CREATE EXTENSION IF NOT EXISTS koldstore;")
            .await
            .context("create koldstore extension")?;
        Ok(Self { target, client })
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
        return vec![PgTarget {
            version,
            port: port.parse().expect("KOLDSTORE_E2E_PGPORT must be a u16"),
        }];
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
                PgTarget {
                    version,
                    port: 28800 + version,
                }
            })
            .collect::<Vec<_>>();
        assert!(
            !targets.is_empty(),
            "KOLDSTORE_E2E_PGVERSIONS must configure at least one PostgreSQL target"
        );
        return targets;
    }

    vec![PgTarget {
        version: 16,
        port: 28816,
    }]
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
    let targets = local_pg_matrix();
    anyhow::ensure!(
        !targets.is_empty(),
        "no E2E PostgreSQL target configured; set KOLDSTORE_E2E_PGPORT or KOLDSTORE_E2E_PGVERSIONS"
    );

    for target in &targets {
        let client =
            connect_with_retries(target, FAST_CONNECT_ATTEMPTS, FAST_CONNECT_DELAY).await?;
        verify_pgrx_target(&client, target).await?;
    }

    Ok(())
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
                "connect PostgreSQL {} on port {}",
                target.version, target.port
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
            "PostgreSQL {} on port {} did not become ready",
            target.version,
            target.port
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
        "koldstore extension is not available on PostgreSQL {}:{}",
        target.version,
        target.port
    );

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
