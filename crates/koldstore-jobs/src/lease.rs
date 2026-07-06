//! Lease claim and stale-lease recovery primitives for background jobs.

use std::num::NonZeroU32;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Positive lease duration in seconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseSeconds(NonZeroU32);

impl LeaseSeconds {
    /// Creates a positive lease duration.
    #[must_use]
    pub fn new(value: u32) -> Option<Self> {
        NonZeroU32::new(value).map(Self)
    }

    /// Returns the raw second count.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// Monotonic lease generation counter per job row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LeaseEpoch(i64);

impl LeaseEpoch {
    /// Creates a non-negative lease epoch.
    #[must_use]
    pub const fn new(value: i64) -> Option<Self> {
        if value < 0 {
            None
        } else {
            Some(Self(value))
        }
    }

    /// Returns the raw epoch value.
    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }
}

/// Active lease metadata for a running job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobLease {
    pub owner: Uuid,
    pub expires_at: DateTime<Utc>,
    pub epoch: LeaseEpoch,
}

/// Result of attempting to claim or renew a job lease.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseClaim {
    pub owner: Uuid,
    pub epoch: LeaseEpoch,
    pub expires_at: DateTime<Utc>,
}

/// Recovery action for a stale or orphaned lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StaleLeaseAction {
    /// Requeue the job for another worker attempt.
    Requeue,
    /// Mark the job failed after too many attempts.
    Fail,
    /// Leave running until heartbeat or expiry policy decides.
    Wait,
}

impl JobLease {
    /// Returns whether the lease has expired relative to `now`.
    #[must_use]
    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        now >= self.expires_at
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lease_seconds_requires_positive_duration() {
        assert_eq!(LeaseSeconds::new(30).unwrap().get(), 30);
        assert!(LeaseSeconds::new(0).is_none());
    }

    #[test]
    fn lease_epoch_requires_non_negative_value() {
        assert_eq!(LeaseEpoch::new(7).unwrap().get(), 7);
        assert!(LeaseEpoch::new(-1).is_none());
    }
}
