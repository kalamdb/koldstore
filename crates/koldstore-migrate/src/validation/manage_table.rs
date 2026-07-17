//! Central validation for the PostgreSQL `manage_table` boundary.
//!
//! PostgreSQL callers gather catalog facts and raw operator values, then pass
//! them here before constructing migration plans. This module owns no SPI or
//! PostgreSQL types.

use koldstore_common::{ManageTableOptions, MirrorCaptureMode, ParquetCompression};

use super::constraints::{
    ConstraintResult, MigrationConstraintError, MigrationValidation, MigrationValidationInput,
};

/// Raw numeric policy values accepted by `manage_table`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManageTablePolicyInput {
    /// Maximum hot rows before automatic flush.
    pub hot_row_limit: Option<i64>,
    /// Minimum rows moved by an automatic flush.
    pub min_flush_rows: i64,
    /// Maximum rows in one cold file.
    pub max_rows_per_file: i64,
    /// Optional target cold-file size in MiB.
    pub target_file_size_mb: Option<i64>,
    /// Runtime floor for `max_rows_per_file`.
    pub min_max_rows_per_file: u64,
}

/// PostgreSQL-free context required to validate one `manage_table` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManageTableValidationContext<'a> {
    /// Catalog-derived migration shape and constraint policy.
    pub migration: MigrationValidationInput,
    /// Whether any schema registration already exists for the table.
    pub already_managed: bool,
    /// Optional explicit backfill ordering column.
    pub migration_order_by: Option<&'a str>,
    /// Optional operator-provided compression spelling.
    pub compression: Option<&'a str>,
    /// Optional mirror consistency/write-throughput mode.
    pub mirror_capture_mode: Option<&'a str>,
    /// Raw numeric flush policy.
    pub policy: ManageTablePolicyInput,
}

/// Canonical data produced by successful manage-table validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedManageTable {
    /// Typed options ready for migration planning and persistence.
    pub options: ManageTableOptions,
    /// Validated table metadata used by migration registration.
    pub migration: MigrationValidation,
}

/// Validates all catalog and operator inputs for `manage_table`.
///
/// This is the single validation entry point called after PostgreSQL catalog
/// probes and before empty- or populated-table migration planning.
///
/// # Errors
///
/// Returns [`MigrationConstraintError`] when the table is already managed,
/// storage is unresolved, policy values or compression are invalid, required
/// columns are absent, or the table shape violates migration constraints.
pub fn validate_manage_table(
    mut context: ManageTableValidationContext<'_>,
) -> ConstraintResult<ValidatedManageTable> {
    if context.already_managed {
        return Err(MigrationConstraintError::AlreadyManaged);
    }
    if !context.migration.storage_exists {
        return Err(MigrationConstraintError::MissingStorage);
    }

    let mut options = ManageTableOptions::default();
    let compression = parse_compression(context.compression)?;
    options = options.with_compression(compression);
    let mirror_capture_mode = parse_mirror_capture_mode(context.mirror_capture_mode)?;
    options = options.with_mirror_capture_mode(mirror_capture_mode);

    if let Some(migration_order_by) = context
        .migration_order_by
        .map(str::trim)
        .filter(|column| !column.is_empty())
    {
        if !context
            .migration
            .columns
            .iter()
            .any(|column| column.name == migration_order_by)
        {
            return Err(MigrationConstraintError::MissingOrderColumn(
                migration_order_by.to_string(),
            ));
        }
        options = options.with_migration_order_by(migration_order_by);
    }

    if let Some(hot_row_limit) = context.policy.hot_row_limit {
        let hot_row_limit = positive_value(hot_row_limit, "hot_row_limit")?;
        let min_flush_rows = positive_value(context.policy.min_flush_rows, "min_flush_rows")?;
        let max_rows_per_file =
            positive_value(context.policy.max_rows_per_file, "max_rows_per_file")?;
        if max_rows_per_file < context.policy.min_max_rows_per_file {
            return Err(MigrationConstraintError::MaxRowsPerFileBelowFloor {
                value: max_rows_per_file,
                minimum: context.policy.min_max_rows_per_file,
            });
        }
        options = options.with_flush(hot_row_limit, min_flush_rows, max_rows_per_file);
    }

    if let Some(target_file_size_mb) = context.policy.target_file_size_mb {
        options = options
            .with_target_file_size_mb(positive_value(target_file_size_mb, "target_file_size_mb")?);
    }
    options.allow_fk_hot_only = Some(context.migration.allow_fk_hot_only);
    context.migration.flush_enabled = options.flush_enabled();
    let migration = context.migration.validate()?;

    Ok(ValidatedManageTable { options, migration })
}

fn parse_compression(compression: Option<&str>) -> ConstraintResult<ParquetCompression> {
    let Some(compression) = compression
        .map(str::trim)
        .filter(|compression| !compression.is_empty())
    else {
        return Ok(ParquetCompression::Zstd);
    };
    ParquetCompression::parse(compression)
        .ok_or_else(|| MigrationConstraintError::UnsupportedCompression(compression.to_string()))
}

fn parse_mirror_capture_mode(value: Option<&str>) -> ConstraintResult<MirrorCaptureMode> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(MirrorCaptureMode::Strict);
    };
    MirrorCaptureMode::parse(value)
        .ok_or_else(|| MigrationConstraintError::UnsupportedMirrorCaptureMode(value.to_string()))
}

fn positive_value(value: i64, field: &'static str) -> ConstraintResult<u64> {
    u64::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or(MigrationConstraintError::InvalidPolicyValue { field, value })
}
