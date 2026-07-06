//! Snowflake-style sequence id generation.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;

/// Custom epoch: 2024-01-01 00:00:00 UTC.
pub const KOLDSTORE_EPOCH_MILLIS: u64 = 1_704_067_200_000;
const TIMESTAMP_SHIFT: u64 = 22;
const WORKER_ID_SHIFT: u64 = 12;
const MAX_WORKER_ID: u16 = 1023;
const MAX_SEQUENCE: u16 = 4095;

static LAST_TIMESTAMP_AND_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Snowflake generation error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SnowflakeError {
    /// System time is before the configured epoch.
    #[error("current timestamp occurs before koldstore snowflake epoch")]
    TimestampBeforeEpoch,
    /// System clock failed.
    #[error("failed to read system clock")]
    ClockUnavailable,
    /// Worker id exceeds the snowflake worker-id bit budget.
    #[error("worker id {0} exceeds max snowflake worker id 1023")]
    WorkerIdOutOfRange(u16),
    /// Generated id overflowed i64.
    #[error("generated snowflake id exceeds i64 range")]
    IdOverflow,
}

/// Generates the next Snowflake id for a validated worker id.
///
/// # Errors
///
/// Returns an error when the worker id or current clock value is outside the
/// representable Snowflake range.
pub fn next_id(worker_id: u16) -> Result<i64, SnowflakeError> {
    if worker_id > MAX_WORKER_ID {
        return Err(SnowflakeError::WorkerIdOutOfRange(worker_id));
    }

    let (timestamp, sequence) = next_timestamp_and_sequence()?;
    compose_id(timestamp, worker_id, sequence)
}

/// Returns the worker id encoded in a generated id.
#[must_use]
pub fn worker_id(id: i64) -> u16 {
    ((id as u64 >> WORKER_ID_SHIFT) & u64::from(MAX_WORKER_ID)) as u16
}

fn current_timestamp_millis() -> Result<u64, SnowflakeError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| SnowflakeError::ClockUnavailable)?
        .as_millis() as u64;
    millis
        .checked_sub(KOLDSTORE_EPOCH_MILLIS)
        .ok_or(SnowflakeError::TimestampBeforeEpoch)
}

fn pack_timestamp_and_sequence(timestamp: u64, sequence: u16) -> u64 {
    (timestamp << 12) | u64::from(sequence)
}

fn unpack_timestamp_and_sequence(value: u64) -> (u64, u16) {
    (value >> 12, (value & u64::from(MAX_SEQUENCE)) as u16)
}

fn next_timestamp_and_sequence() -> Result<(u64, u16), SnowflakeError> {
    loop {
        let now = current_timestamp_millis()?;
        let observed = LAST_TIMESTAMP_AND_SEQUENCE.load(Ordering::Acquire);
        let (last_timestamp, last_sequence) = unpack_timestamp_and_sequence(observed);

        let (next_timestamp, next_sequence) = if now > last_timestamp {
            (now, 0)
        } else if last_sequence < MAX_SEQUENCE {
            (last_timestamp, last_sequence + 1)
        } else {
            (last_timestamp + 1, 0)
        };
        let next_state = pack_timestamp_and_sequence(next_timestamp, next_sequence);

        if LAST_TIMESTAMP_AND_SEQUENCE
            .compare_exchange(observed, next_state, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return Ok((next_timestamp, next_sequence));
        }
    }
}

fn compose_id(timestamp: u64, worker_id: u16, sequence: u16) -> Result<i64, SnowflakeError> {
    let id = (timestamp << TIMESTAMP_SHIFT)
        | (u64::from(worker_id) << WORKER_ID_SHIFT)
        | u64::from(sequence);
    i64::try_from(id).map_err(|_| SnowflakeError::IdOverflow)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_ids_are_monotonic_for_a_worker() {
        let first = next_id(7).unwrap();
        let second = next_id(7).unwrap();
        let third = next_id(7).unwrap();

        assert!(first < second);
        assert!(second < third);
        assert_eq!(worker_id(first), 7);
    }

    #[test]
    fn rejects_worker_ids_outside_snowflake_budget() {
        assert_eq!(next_id(1024), Err(SnowflakeError::WorkerIdOutOfRange(1024)));
    }
}
