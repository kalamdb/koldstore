//! O(1) per-table row counters stored on `koldstore.manifest`.
//!
//! These counters avoid repeated `COUNT(*)` scans over hot heaps and mirrors during
//! flush logging, `describe_table`, and operator diagnostics. DML capture triggers
//! bump hot counts; flush finalization applies mirror/hot prune and cold deltas.

use koldstore_common::SqlStatement;
use thiserror::Error;

/// Cheap per-table row accounting read from `koldstore.manifest`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TableRowCounters {
    /// Live rows in the managed user heap.
    pub hot_row_count: i64,
    /// Latest-state rows in the change-log mirror.
    pub mirror_row_count: i64,
    /// Rows referenced by active cold segments.
    pub cold_row_count: i64,
    /// Active cold segment count.
    pub cold_segment_count: i64,
}

/// Table counter planning error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TableCounterError {
    /// SQL statement metadata could not be prepared.
    #[error("{0}")]
    Sql(String),
}

/// PERFORMANCE: Absolute ceiling for mirror rows fetched per SPI round-trip.
///
/// Keep this modest so a misconfigured `max_rows_per_file` cannot decode tens of
/// thousands of typed rows into backend RSS. Policy flushes further cap via
/// [`flush_mirror_fetch_limit`].
pub const FLUSH_MIRROR_FETCH_BATCH_SIZE: i64 = 4_096;

/// Resolves the SPI page size for one mirror fetch during flush.
///
/// Caps by `max_rows_per_file` so decode + encode peak memory stays near one
/// Parquet segment, not an unbounded SPI page.
#[must_use]
pub fn flush_mirror_fetch_limit(max_rows_per_file: usize) -> i64 {
    if max_rows_per_file == 0 || max_rows_per_file == usize::MAX {
        return FLUSH_MIRROR_FETCH_BATCH_SIZE;
    }
    let per_file = i64::try_from(max_rows_per_file).unwrap_or(FLUSH_MIRROR_FETCH_BATCH_SIZE);
    per_file.clamp(1, FLUSH_MIRROR_FETCH_BATCH_SIZE)
}

/// Plans a read of cached row counters from `koldstore.manifest`.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_read_table_row_counters() -> Result<SqlStatement, TableCounterError> {
    SqlStatement::read_with_params(
        "read manifest row counters",
        r#"
SELECT jsonb_build_object(
  'hot_row_count', COALESCE(m.hot_row_count, 0)::bigint,
  'mirror_row_count', COALESCE(m.mirror_row_count, 0)::bigint,
  'cold_row_count', COALESCE(m.cold_row_count, 0)::bigint,
  'cold_segment_count', COALESCE(m.segment_count, 0)::bigint
)::text
FROM koldstore.manifest m
WHERE m.table_oid = $1::oid
  AND m.scope_key = ''
"#,
        [koldstore_common::SqlParamType::Oid],
    )
    .map_err(|error| TableCounterError::Sql(error.to_string()))
}

/// Plans DML-time counter bumps from mirror capture triggers.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_bump_table_row_counts() -> Result<SqlStatement, TableCounterError> {
    SqlStatement::write_with_params(
        "bump manifest row counters",
        r#"
SELECT koldstore.internal_bump_row_counts($1::oid, $2::bigint, $3::bigint)
"#,
        [
            koldstore_common::SqlParamType::Oid,
            koldstore_common::SqlParamType::BigInt,
            koldstore_common::SqlParamType::BigInt,
        ],
    )
    .map_err(|error| TableCounterError::Sql(error.to_string()))
}

/// Plans flush-time counter adjustments after mirror prune and cold writes.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_apply_flush_row_count_deltas() -> Result<SqlStatement, TableCounterError> {
    SqlStatement::write_with_params(
        "apply flush row counter deltas",
        r#"
SELECT koldstore.internal_apply_flush_row_counts(
  $1::oid,
  $2::bigint,
  $3::bigint,
  $4::bigint
)
"#,
        [
            koldstore_common::SqlParamType::Oid,
            koldstore_common::SqlParamType::BigInt,
            koldstore_common::SqlParamType::BigInt,
            koldstore_common::SqlParamType::BigInt,
        ],
    )
    .map_err(|error| TableCounterError::Sql(error.to_string()))
}

/// Plans a one-time counter refresh from live table counts during migration.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_refresh_table_row_counters(
    table: &koldstore_common::QualifiedTableName,
    mirror: &koldstore_common::QualifiedTableName,
) -> Result<SqlStatement, TableCounterError> {
    SqlStatement::write_with_params(
        "refresh manifest row counters",
        &format!(
            r#"
SELECT koldstore.internal_refresh_row_counts(
  $1::oid,
  (SELECT count(*)::bigint FROM ONLY {table}),
  (SELECT count(*)::bigint FROM {mirror})
)
"#,
            table = table.quoted(),
            mirror = mirror.quoted(),
        ),
        [koldstore_common::SqlParamType::Oid],
    )
    .map_err(|error| TableCounterError::Sql(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::{flush_mirror_fetch_limit, FLUSH_MIRROR_FETCH_BATCH_SIZE};

    #[test]
    fn fetch_limit_caps_to_max_rows_per_file() {
        assert_eq!(flush_mirror_fetch_limit(1_000), 1_000);
        assert_eq!(flush_mirror_fetch_limit(100), 100);
    }

    #[test]
    fn fetch_limit_uses_ceiling_when_file_cap_unbounded() {
        assert_eq!(
            flush_mirror_fetch_limit(usize::MAX),
            FLUSH_MIRROR_FETCH_BATCH_SIZE
        );
        assert_eq!(flush_mirror_fetch_limit(0), FLUSH_MIRROR_FETCH_BATCH_SIZE);
    }

    #[test]
    fn fetch_limit_never_exceeds_absolute_ceiling() {
        assert_eq!(
            flush_mirror_fetch_limit(100_000),
            FLUSH_MIRROR_FETCH_BATCH_SIZE
        );
    }
}
