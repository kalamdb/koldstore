//! Job identity and status models shared across workflow crates.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Durable job identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JobId(pub Uuid);

impl JobId {
    /// Creates a new random job id.
    #[must_use]
    pub fn new_v4() -> Self {
        Self(Uuid::new_v4())
    }
}

/// Workflow kind stored in `koldstore.jobs.job_type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobType {
    /// Hot-to-cold flush for a managed table scope.
    Flush,
    /// Backfill mirror state for an existing managed table.
    MigrateBackfill,
}

/// Lifecycle status for a durable job row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Pending,
    Running,
    DryRun,
    Completed,
    Cancelled,
    Error,
}

impl JobStatus {
    /// Returns whether the job can be claimed by a worker.
    #[must_use]
    pub const fn is_claimable(self) -> bool {
        matches!(self, Self::Pending | Self::Running)
    }
}
