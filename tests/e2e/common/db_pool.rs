//! Per-nextest-worker PostgreSQL database leases for parallel E2E.
//!
//! Async mirror is database-scoped (one slot, one worker, one apply lock). Suite
//! parallelism therefore needs one database per concurrent test, not schema-only
//! isolation on a shared DB.
//!
//! Nextest runs process-per-test, so coordination uses filesystem locks under
//! the temp directory. When `NEXTEST_TEST_GLOBAL_SLOT` is set, that slot is tried
//! first (fast path); otherwise the free list is scanned.
//!
//! Postmaster-killing crash tests also take [`ClusterExclusiveGuard`]: an
//! exclusive flock on a shared cluster lock file. Ordinary fixtures hold a
//! shared lock on that same file, so SIGKILL / postmaster-restart coverage
//! cannot run while sibling tests still hold live connections.
//!
//! The same exclusive lock is used for async-mirror / flush failpoint tests that
//! stress logical decoding: on assert-enabled pgrx builds a `ReorderBuffer`
//! assert abort restarts the whole cluster and surfaces as cascading
//! `connection closed` failures under parallel nextest.

use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{bail, Context, Result};

/// Set while this process holds [`ClusterExclusiveGuard`] so nested
/// [`claim_database`] skips re-taking the shared cluster lock (would deadlock).
static CLUSTER_EXCLUSIVE_HELD: AtomicBool = AtomicBool::new(false);

/// Lease of one pooled E2E database index; the flock is released on drop.
#[derive(Debug)]
pub struct DatabaseLease {
    /// Pool index when the pool is enabled.
    index: Option<usize>,
    /// Shared cluster flock (blocks exclusive crash tests while this fixture lives).
    _cluster_shared: Option<File>,
    /// Held exclusive flock for the leased worker DB (cross-process).
    _lock: Option<File>,
}

impl DatabaseLease {
    /// Returns the leased pool index, if any.
    #[must_use]
    pub fn index(&self) -> Option<usize> {
        self.index
    }
}

/// Exclusive ownership of the shared pgrx cluster for postmaster-killing tests.
///
/// Held across setup → SIGKILL/restart → reconnect. Dropping releases the lock
/// so waiting fixtures can proceed.
#[derive(Debug)]
pub struct ClusterExclusiveGuard {
    _lock: File,
}

impl Drop for ClusterExclusiveGuard {
    fn drop(&mut self) {
        CLUSTER_EXCLUSIVE_HELD.store(false, Ordering::SeqCst);
    }
}

/// Returns whether the runner prepared a worker-database pool.
#[must_use]
pub fn e2e_db_pool_enabled() -> bool {
    std::env::var("KOLDSTORE_E2E_DB_POOL")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes"))
        .unwrap_or(false)
}

/// Number of pooled worker databases (and intended nextest threads).
#[must_use]
pub fn e2e_pool_size() -> usize {
    std::env::var("KOLDSTORE_E2E_THREADS")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|threads| *threads > 0)
        .unwrap_or(1)
}

/// Database name prefix from the runner (`koldstore_pgrx_e2e` by default).
#[must_use]
pub fn e2e_database_prefix() -> String {
    std::env::var("KOLDSTORE_E2E_PGDATABASE").unwrap_or_else(|_| "koldstore_pgrx_e2e".to_string())
}

/// Worker database name for pool index `i`.
#[must_use]
pub fn worker_database_name(index: usize) -> String {
    format!("{}_w{index}", e2e_database_prefix())
}

/// Database used when the pool is disabled (single shared DB).
#[must_use]
pub fn shared_database_name() -> String {
    e2e_database_prefix()
}

fn nextest_global_slot() -> Option<usize> {
    std::env::var("NEXTEST_TEST_GLOBAL_SLOT")
        .ok()
        .and_then(|value| value.parse().ok())
}

fn lock_path(index: usize) -> PathBuf {
    std::env::temp_dir().join(format!(
        "koldstore-e2e-{}-w{index}.lock",
        e2e_database_prefix()
    ))
}

fn cluster_lock_path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "koldstore-e2e-{}-cluster.lock",
        e2e_database_prefix()
    ))
}

fn open_lock_file(path: &PathBuf) -> Result<File> {
    File::options()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .with_context(|| format!("open E2E lock {}", path.display()))
}

fn try_lock_index(index: usize) -> Result<Option<File>> {
    let path = lock_path(index);
    let file = open_lock_file(&path)?;
    match file.try_lock() {
        Ok(()) => Ok(Some(file)),
        Err(std::fs::TryLockError::WouldBlock) => Ok(None),
        Err(error) => Err(error).with_context(|| format!("lock {}", path.display())),
    }
}

fn take_cluster_shared_lock() -> Result<Option<File>> {
    // Same-process exclusive holders skip shared (posix flock would deadlock).
    if CLUSTER_EXCLUSIVE_HELD.load(Ordering::SeqCst) {
        return Ok(None);
    }
    let path = cluster_lock_path();
    let file = open_lock_file(&path)?;
    file.lock_shared()
        .with_context(|| format!("shared-lock {}", path.display()))?;
    Ok(Some(file))
}

/// Blocks until no other E2E fixture holds the shared cluster lock, then takes
/// exclusive ownership for postmaster-killing crash coverage.
///
/// Call before [`crate::common::TestDb::start`] in SIGKILL / postmaster-restart
/// tests. Drop the guard only after reconnect + assertions finish.
///
/// # Errors
///
/// Returns an error when the lock file cannot be opened or locked.
pub fn acquire_cluster_exclusive() -> Result<ClusterExclusiveGuard> {
    let path = cluster_lock_path();
    let file = open_lock_file(&path)?;
    // Blocking exclusive: waits for every ordinary fixture's shared lock.
    file.lock()
        .with_context(|| format!("exclusive-lock {}", path.display()))?;
    CLUSTER_EXCLUSIVE_HELD.store(true, Ordering::SeqCst);
    Ok(ClusterExclusiveGuard { _lock: file })
}

/// Claims a worker database for the duration of one fixture.
///
/// Uses an exclusive flock so concurrent nextest processes cannot share a DB.
/// Prefers `NEXTEST_TEST_GLOBAL_SLOT % N` when set. Also holds a shared cluster
/// flock so postmaster-killing tests wait until this fixture ends.
///
/// # Errors
///
/// Returns an error when pool size is zero or no lock can be acquired in time.
pub fn claim_database() -> Result<(String, DatabaseLease)> {
    let cluster_shared = take_cluster_shared_lock()?;

    if !e2e_db_pool_enabled() {
        return Ok((
            shared_database_name(),
            DatabaseLease {
                index: None,
                _cluster_shared: cluster_shared,
                _lock: None,
            },
        ));
    }

    let size = e2e_pool_size();
    if size == 0 {
        bail!("KOLDSTORE_E2E_THREADS must be >= 1");
    }

    let preferred = nextest_global_slot().map(|slot| slot % size);
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while std::time::Instant::now() < deadline {
        let mut order: Vec<usize> = (0..size).collect();
        if let Some(preferred) = preferred {
            order.sort_by_key(|index| usize::from(*index != preferred));
        }
        for index in order {
            if let Some(lock) = try_lock_index(index)? {
                return Ok((
                    worker_database_name(index),
                    DatabaseLease {
                        index: Some(index),
                        _cluster_shared: cluster_shared,
                        _lock: Some(lock),
                    },
                ));
            }
        }
        std::thread::sleep(Duration::from_millis(25));
    }

    bail!(
        "E2E database pool exhausted (size={size}); lower KOLDSTORE_E2E_THREADS or ensure each test drops TestDb"
    )
}

/// Validates pool configuration before the suite starts.
///
/// # Errors
///
/// Returns an error when pool size is zero.
pub fn ensure_pool_config() -> Result<()> {
    if e2e_pool_size() == 0 {
        bail!("KOLDSTORE_E2E_THREADS must be >= 1");
    }
    Ok(())
}
