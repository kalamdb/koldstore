//! Shared E2E helpers for PostgreSQL and MinIO-backed tests.

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
    #[allow(dead_code)]
    pub fn connection_string(&self) -> String {
        let host =
            std::env::var("KOLDSTORE_E2E_PGHOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        let user = std::env::var("KOLDSTORE_E2E_PGUSER").unwrap_or_else(|_| "postgres".to_string());
        let dbname =
            std::env::var("KOLDSTORE_E2E_PGDATABASE").unwrap_or_else(|_| "koldstore".to_string());
        let password = std::env::var("KOLDSTORE_E2E_PGPASSWORD")
            .map(|password| {
                if password.is_empty() {
                    String::new()
                } else {
                    format!(" password={password}")
                }
            })
            .unwrap_or_else(|_| " password=postgres".to_string());

        format!(
            "host={host} port={} user={user}{password} dbname={dbname}",
            self.port
        )
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

    vec![
        PgTarget {
            version: 15,
            port: 5515,
        },
        PgTarget {
            version: 16,
            port: 5516,
        },
        PgTarget {
            version: 17,
            port: 5517,
        },
    ]
}

/// Expected PostgreSQL versions for the active test target mode.
#[allow(dead_code)]
#[must_use]
pub fn expected_pg_versions() -> Vec<u16> {
    local_pg_matrix()
        .into_iter()
        .map(|target| target.version)
        .collect()
}

/// Expected PostgreSQL ports for the active test target mode.
#[allow(dead_code)]
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
#[allow(dead_code)]
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
#[allow(dead_code)]
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
