//! Env knobs for the chat penetration soak.

use std::time::Duration;

use anyhow::{bail, Result};

use crate::packs::PackSet;

const ENV_PREFIX: &str = "KOLDSTORE_STRESS_";

/// Runtime configuration loaded from `KOLDSTORE_STRESS_*` environment variables.
#[derive(Debug, Clone)]
pub struct StressConfig {
    pub packs: PackSet,
    pub mirror_mode: MirrorMode,
    pub soak: Duration,
    pub clients: usize,
    pub history_clients: usize,
    pub cold_dml_clients: usize,
    pub multi_table_clients: usize,
    pub join_clients: usize,
    pub tenants: usize,
    pub conversations_per_tenant: usize,
    pub payload_bytes: usize,
    pub bytea_bytes: usize,
    pub latency_multiplier: f64,
    pub baseline_samples: usize,
    pub seed_rows_per_conversation: i64,
    pub hot_row_limit: i64,
    pub min_flush_rows: i64,
    pub max_rows_per_file: i64,
    pub flush_interval: Duration,
    pub absolute_latency_floor_us: u64,
    pub max_open_fds: u64,
    pub max_connections: i64,
    /// How often to print soak progress (messages + live percentiles).
    pub progress_interval: Duration,
    /// Sleep between writer insert/update iterations (lower = faster).
    pub writer_delay: Duration,
}

/// Mirror capture mode for manage_table / fencing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MirrorMode {
    Strict,
    Async,
}

impl MirrorMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Strict => "strict",
            Self::Async => "async",
        }
    }
}

impl StressConfig {
    /// Loads configuration from the environment.
    ///
    /// # Errors
    ///
    /// Returns an error when packs or numeric knobs are invalid.
    pub fn from_env() -> Result<Self> {
        let packs = PackSet::from_env()?;
        let mut mirror_mode = match std::env::var(format!("{ENV_PREFIX}MIRROR_MODE"))
            .unwrap_or_else(|_| "strict".into())
            .to_ascii_lowercase()
            .as_str()
        {
            "strict" => MirrorMode::Strict,
            "async" => MirrorMode::Async,
            other => bail!("invalid {ENV_PREFIX}MIRROR_MODE={other:?}; expected strict or async"),
        };
        if packs.async_mirror() {
            mirror_mode = MirrorMode::Async;
        }

        // Apply mirror mode early so e2e helpers see it during manage_table.
        // SAFETY: single-threaded before workers spawn; intentional for test harness.
        unsafe {
            std::env::set_var("KOLDSTORE_E2E_MIRROR_CAPTURE_MODE", mirror_mode.as_str());
        }

        let soak = soak_duration()?;
        let clients = env_usize("CLIENTS", 24)?.max(1);
        let history_clients = env_usize("HISTORY_CLIENTS", 8)?.max(1);
        let cold_dml_clients = env_usize("COLD_DML_CLIENTS", 4)?.max(1);
        let multi_table_clients = env_usize("MULTI_TABLE_CLIENTS", 4)?.max(1);
        let join_clients = env_usize("JOIN_CLIENTS", 4)?.max(1);
        let tenants = env_usize("TENANTS", 16)?.max(2);
        let conversations_per_tenant = env_usize("CONVERSATIONS_PER_TENANT", 8)?.max(1);
        let payload_bytes = env_usize("PAYLOAD_BYTES", 2048)?.max(32);
        let bytea_bytes = env_usize("BYTEA_BYTES", 2048)?.max(16);
        let latency_multiplier = env_f64("LATENCY_MULTIPLIER", 4.0)?;
        if !(1.0..100.0).contains(&latency_multiplier) {
            bail!("{ENV_PREFIX}LATENCY_MULTIPLIER must be in [1, 100)");
        }
        let baseline_samples = env_usize("BASELINE_SAMPLES", 50)?.max(5);
        let seed_rows_per_conversation = env_i64("SEED_ROWS_PER_CONVERSATION", 40)?.max(5);
        let hot_row_limit = env_i64("HOT_ROW_LIMIT", 4_000)?.max(1_000);
        let min_flush_rows = env_i64("MIN_FLUSH_ROWS", 1_000)?.max(100);
        let max_rows_per_file = env_i64("MAX_ROWS_PER_FILE", 2_000)?.max(1_000);
        let flush_interval =
            Duration::from_millis(env_u64("FLUSH_INTERVAL_MS", 500)?.max(50));
        let absolute_latency_floor_us = env_u64("LATENCY_FLOOR_US", 50_000)?;
        let max_open_fds = env_u64("MAX_OPEN_FDS", 10_000)?;
        let max_connections = env_i64(
            "MAX_CONNECTIONS",
            (clients + history_clients + cold_dml_clients + multi_table_clients + join_clients)
                as i64
                * 3
                + 32,
        )?;
        let progress_interval =
            Duration::from_secs(env_u64("PROGRESS_INTERVAL_SECS", 5)?.max(1));
        // Default 1ms (was 2ms) ≈ 2× writer insert/update rate.
        let writer_delay = Duration::from_millis(env_u64("WRITER_DELAY_MS", 1)?);

        Ok(Self {
            packs,
            mirror_mode,
            soak,
            clients,
            history_clients,
            cold_dml_clients,
            multi_table_clients,
            join_clients,
            tenants,
            conversations_per_tenant,
            payload_bytes,
            bytea_bytes,
            latency_multiplier,
            baseline_samples,
            seed_rows_per_conversation,
            hot_row_limit,
            min_flush_rows,
            max_rows_per_file,
            flush_interval,
            absolute_latency_floor_us,
            max_open_fds,
            max_connections,
            progress_interval,
            writer_delay,
        })
    }

    #[must_use]
    pub fn tenant_id(&self, idx: usize) -> String {
        format!("tenant-{:04}", idx % self.tenants)
    }

    #[must_use]
    pub fn conversation_id(&self, tenant_idx: usize, conv_idx: usize) -> String {
        format!(
            "conv-{:04}-{:04}",
            tenant_idx % self.tenants,
            conv_idx % self.conversations_per_tenant
        )
    }

    #[must_use]
    pub fn seed_total_rows(&self) -> i64 {
        self.tenants as i64
            * self.conversations_per_tenant as i64
            * self.seed_rows_per_conversation
    }
}

fn soak_duration() -> Result<Duration> {
    if let Ok(secs) = std::env::var(format!("{ENV_PREFIX}SOAK_SECONDS")) {
        let secs: u64 = secs
            .parse()
            .map_err(|_| anyhow::anyhow!("{ENV_PREFIX}SOAK_SECONDS must be an integer"))?;
        if secs == 0 {
            bail!("{ENV_PREFIX}SOAK_SECONDS must be > 0");
        }
        return Ok(Duration::from_secs(secs));
    }
    let minutes = env_u64("MINUTES", 5)?;
    if minutes == 0 {
        bail!("{ENV_PREFIX}MINUTES must be > 0 (or set {ENV_PREFIX}SOAK_SECONDS)");
    }
    Ok(Duration::from_secs(minutes.saturating_mul(60)))
}

fn env_usize(suffix: &str, default: usize) -> Result<usize> {
    match std::env::var(format!("{ENV_PREFIX}{suffix}")) {
        Ok(raw) => raw
            .parse()
            .map_err(|_| anyhow::anyhow!("{ENV_PREFIX}{suffix} must be an unsigned integer")),
        Err(_) => Ok(default),
    }
}

fn env_u64(suffix: &str, default: u64) -> Result<u64> {
    match std::env::var(format!("{ENV_PREFIX}{suffix}")) {
        Ok(raw) => raw
            .parse()
            .map_err(|_| anyhow::anyhow!("{ENV_PREFIX}{suffix} must be a u64")),
        Err(_) => Ok(default),
    }
}

fn env_i64(suffix: &str, default: i64) -> Result<i64> {
    match std::env::var(format!("{ENV_PREFIX}{suffix}")) {
        Ok(raw) => raw
            .parse()
            .map_err(|_| anyhow::anyhow!("{ENV_PREFIX}{suffix} must be an i64")),
        Err(_) => Ok(default),
    }
}

fn env_f64(suffix: &str, default: f64) -> Result<f64> {
    match std::env::var(format!("{ENV_PREFIX}{suffix}")) {
        Ok(raw) => raw
            .parse()
            .map_err(|_| anyhow::anyhow!("{ENV_PREFIX}{suffix} must be a float")),
        Err(_) => Ok(default),
    }
}
