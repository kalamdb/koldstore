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

/// Returns the first possible Snowflake id at `unix_millis`.
///
/// A `None` result means the cutoff predates the KoldStore epoch, so no
/// generated mirror sequence can be older than it.
#[must_use]
pub fn minimum_id_at_unix_millis(unix_millis: i64) -> Option<i64> {
    let unix_millis = u64::try_from(unix_millis).ok()?;
    let elapsed = unix_millis.checked_sub(KOLDSTORE_EPOCH_MILLIS)?;
    i64::try_from(elapsed.checked_shl(TIMESTAMP_SHIFT as u32)?).ok()
}

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

/// Generates the next Snowflake id that is strictly greater than `floor`.
///
/// Used by async flush prune fences so applied mutations of the flushed table
/// receive `new_seq > max_seq` and survive `seq <= max_seq` cleanup.
///
/// # Errors
///
/// Returns an error when the worker id is out of range, the clock is unusable,
/// or no representable id above `floor` exists.
pub fn next_id_after(worker_id: u16, floor: i64) -> Result<i64, SnowflakeError> {
    if worker_id > MAX_WORKER_ID {
        return Err(SnowflakeError::WorkerIdOutOfRange(worker_id));
    }
    if floor == i64::MAX {
        return Err(SnowflakeError::IdOverflow);
    }

    // Fast path: normal allocation already clears the floor.
    let candidate = next_id(worker_id)?;
    if candidate > floor {
        return Ok(candidate);
    }

    // Clock/generator is at or behind the prune watermark. Advance the shared
    // timestamp past the floor's timestamp so subsequent ids sort above it.
    let floor_u = u64::try_from(floor).unwrap_or(0);
    let min_timestamp = (floor_u >> TIMESTAMP_SHIFT)
        .checked_add(1)
        .ok_or(SnowflakeError::IdOverflow)?;
    advance_generator_to_timestamp(min_timestamp)?;

    let raised = next_id(worker_id)?;
    if raised > floor {
        return Ok(raised);
    }
    Err(SnowflakeError::IdOverflow)
}

/// Raises the shared generator clock to at least `timestamp` (sequence 0).
fn advance_generator_to_timestamp(timestamp: u64) -> Result<(), SnowflakeError> {
    let want = pack_timestamp_and_sequence(timestamp, 0);
    loop {
        let observed = LAST_TIMESTAMP_AND_SEQUENCE.load(Ordering::Acquire);
        let (last_timestamp, last_sequence) = unpack_timestamp_and_sequence(observed);
        if last_timestamp > timestamp
            || (last_timestamp == timestamp && last_sequence == MAX_SEQUENCE)
        {
            // Already past the target; let next_id allocate normally.
            return Ok(());
        }
        let next_state = if timestamp > last_timestamp {
            want
        } else if last_sequence < MAX_SEQUENCE {
            pack_timestamp_and_sequence(last_timestamp, last_sequence + 1)
        } else {
            pack_timestamp_and_sequence(
                last_timestamp
                    .checked_add(1)
                    .ok_or(SnowflakeError::IdOverflow)?,
                0,
            )
        };
        if LAST_TIMESTAMP_AND_SEQUENCE
            .compare_exchange(observed, next_state, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return Ok(());
        }
    }
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

    #[test]
    fn next_id_after_is_strictly_above_floor() {
        let baseline = next_id(3).unwrap();
        let above = next_id_after(3, baseline).unwrap();
        assert!(above > baseline);
        assert_eq!(worker_id(above), 3);

        let again = next_id_after(3, above).unwrap();
        assert!(again > above);
    }

    #[test]
    fn next_id_after_rejects_max_floor() {
        assert_eq!(next_id_after(1, i64::MAX), Err(SnowflakeError::IdOverflow));
    }

    #[test]
    fn cutoff_id_excludes_every_id_in_cutoff_millisecond() {
        let cutoff_ms = KOLDSTORE_EPOCH_MILLIS as i64 + 42;
        let cutoff = minimum_id_at_unix_millis(cutoff_ms).unwrap();
        assert_eq!(cutoff, 42_i64 << TIMESTAMP_SHIFT);
        assert!(compose_id(41, MAX_WORKER_ID, MAX_SEQUENCE).unwrap() < cutoff);
        assert_eq!(compose_id(42, 0, 0).unwrap(), cutoff);
        assert_eq!(
            minimum_id_at_unix_millis(KOLDSTORE_EPOCH_MILLIS as i64 - 1),
            None
        );
    }
}
