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
        format!(
            "host=127.0.0.1 port={} user=postgres password=postgres dbname=koldstore",
            self.port
        )
    }
}

/// Known local PostgreSQL matrix.
#[must_use]
pub fn local_pg_matrix() -> [PgTarget; 3] {
    [
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
