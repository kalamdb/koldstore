//! Process / Postgres resource sampling during soak.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use serde::Serialize;
use tokio_postgres::Client;

use crate::control::SoakControl;
use crate::e2e;

/// Peaks observed by the watchdog.
#[derive(Debug, Default, Serialize)]
pub struct WatchdogPeaks {
    pub peak_open_fds: u64,
    pub peak_connections: i64,
    pub samples: u64,
    pub fd_unsupported: bool,
}

/// Shared watchdog peaks (fatal abort lives on [`SoakControl`]).
#[derive(Debug, Default)]
pub struct Watchdog {
    pub peak_open_fds: AtomicU64,
    pub peak_connections: AtomicU64,
    pub samples: AtomicU64,
    pub fd_unsupported: std::sync::atomic::AtomicBool,
}

impl Watchdog {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn peaks(&self) -> WatchdogPeaks {
        WatchdogPeaks {
            peak_open_fds: self.peak_open_fds.load(Ordering::Relaxed),
            peak_connections: self.peak_connections.load(Ordering::Relaxed) as i64,
            samples: self.samples.load(Ordering::Relaxed),
            fd_unsupported: self.fd_unsupported.load(Ordering::Relaxed),
        }
    }
}

/// Spawns a background sampler until `control` requests stop / fatal.
pub fn spawn_watchdog(
    target: e2e::PgTarget,
    control: Arc<SoakControl>,
    watchdog: Arc<Watchdog>,
    max_open_fds: u64,
    max_connections: i64,
) -> tokio::task::JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let client = match e2e::connect(&target).await {
            Ok(c) => c,
            Err(err) => {
                control.note_db_error("watchdog connect", &err);
                return Err(err);
            }
        };
        while !control.should_stop() {
            if let Err(err) =
                sample_once(&client, &control, &watchdog, max_open_fds, max_connections).await
            {
                control.note_db_error("watchdog sample", &err);
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        Ok(())
    })
}

async fn sample_once(
    client: &Client,
    control: &SoakControl,
    watchdog: &Watchdog,
    max_open_fds: u64,
    max_connections: i64,
) -> Result<()> {
    watchdog.samples.fetch_add(1, Ordering::Relaxed);

    match count_open_fds() {
        Some(fds) => {
            let prev = watchdog.peak_open_fds.load(Ordering::Relaxed);
            if fds > prev {
                watchdog.peak_open_fds.store(fds, Ordering::Relaxed);
            }
            if fds > max_open_fds {
                control.trip_fatal(format!(
                    "open FD count {fds} exceeded max_open_fds={max_open_fds}"
                ));
            }
        }
        None => {
            watchdog.fd_unsupported.store(true, Ordering::Relaxed);
        }
    }

    let row = client
        .query_one("SELECT count(*)::bigint FROM pg_stat_activity", &[])
        .await?;
    let conns: i64 = row.get(0);
    let prev = watchdog.peak_connections.load(Ordering::Relaxed);
    if (conns as u64) > prev {
        watchdog
            .peak_connections
            .store(conns as u64, Ordering::Relaxed);
    }
    if conns > max_connections {
        control.trip_fatal(format!(
            "pg_stat_activity count {conns} exceeded max_connections={max_connections}"
        ));
    }

    let _ = client.query_one("SELECT 1", &[]).await?;
    Ok(())
}

fn count_open_fds() -> Option<u64> {
    let candidates = [
        format!("/proc/{}/fd", std::process::id()),
        "/dev/fd".to_string(),
    ];
    for path in candidates {
        let p = Path::new(&path);
        if !p.is_dir() {
            continue;
        }
        let mut n = 0u64;
        let Ok(entries) = std::fs::read_dir(p) else {
            continue;
        };
        for entry in entries.flatten() {
            let _ = entry;
            n += 1;
        }
        return Some(n);
    }
    None
}
