//! Generic job phase progression helpers.

use serde::{Deserialize, Serialize};

/// Named execution phase for durable jobs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobPhase(pub String);

impl JobPhase {
    /// Initial phase for newly created jobs.
    #[must_use]
    pub fn pending() -> Self {
        Self("pending".to_owned())
    }

    /// Returns the phase name for persistence.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Recorded phase transition for auditing or state-machine tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhaseTransition {
    pub from: JobPhase,
    pub to: JobPhase,
}

impl PhaseTransition {
    /// Creates a transition between two phases.
    #[must_use]
    pub fn new(from: impl Into<JobPhase>, to: impl Into<JobPhase>) -> Self {
        Self {
            from: from.into(),
            to: to.into(),
        }
    }
}

impl From<&str> for JobPhase {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl From<String> for JobPhase {
    fn from(value: String) -> Self {
        Self(value)
    }
}
