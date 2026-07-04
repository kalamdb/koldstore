//! Local pgrx PostgreSQL target discovery and startup.

use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio_postgres::{Client, NoTls};

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
    /// `tests/e2e/run_pg_matrix.sh`. Setting `KOLDSTORE_E2E_START_PGRX=1` makes
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
        return versions
            .split(',')
            .filter_map(|version| version.trim().parse::<u16>().ok())
            .map(|version| PgTarget {
                version,
                port: 28800 + version,
            })
            .collect();
    }

    vec![PgTarget {
        version: 16,
        port: 28816,
    }]
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

/// Connects to PostgreSQL with tokio-postgres.
///
/// # Errors
///
/// Returns an error when the target cannot be reached.
pub async fn connect(target: &PgTarget) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(&target.connection_string(), NoTls)
        .await
        .with_context(|| format!("connect PostgreSQL {}", target.version))?;
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
    let mut last_error = None;
    for _ in 0..30 {
        match connect(target).await {
            Ok(client) => return Ok(client),
            Err(error) => {
                last_error = Some(error);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("postgres did not become ready")))
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
