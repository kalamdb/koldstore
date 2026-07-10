//! Managed-table configuration models persisted in `koldstore.schemas.options`.
//!
//! These types are the canonical representation for operator-facing manage-table
//! settings and flush policy. JSON conversion is limited to database boundaries.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Default minimum allowed `max_rows_per_file` unless lowered by GUC.
pub const DEFAULT_MIN_MAX_ROWS_PER_FILE: u64 = 1_000;

/// Validates `max_rows_per_file` against a configured floor.
///
/// # Errors
///
/// Returns an error when `value` is below `min_floor`.
pub fn validate_max_rows_per_file(
    value: u64,
    min_floor: u64,
    floor_override_hint: Option<&str>,
) -> Result<(), String> {
    if value >= min_floor {
        return Ok(());
    }

    let mut message = format!("max_rows_per_file must be at least {min_floor} (got {value})");
    if let Some(hint) = floor_override_hint {
        message.push_str(&format!("; {hint}"));
    }
    Err(message)
}

/// Migration lifecycle marker written by the extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationStatus {
    /// Schema is active and serving traffic.
    Active,
    /// Mirror initialization is still in progress.
    MirrorInitializing,
}

impl MigrationStatus {
    /// Returns the persisted JSON string for this status.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::MirrorInitializing => "mirror_initializing",
        }
    }
}

/// Parquet compression codec configured for cold segments.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParquetCompression {
    /// Snappy compression.
    Snappy,
    /// Zstandard compression (default for cold segments).
    #[default]
    Zstd,
    /// No compression.
    Uncompressed,
}

impl ParquetCompression {
    /// Returns the persisted JSON string for this codec.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Snappy => "snappy",
            Self::Zstd => "zstd",
            Self::Uncompressed => "uncompressed",
        }
    }

    /// Parses operator-provided compression names.
    #[must_use]
    pub fn parse(codec: &str) -> Option<Self> {
        match codec.trim().to_ascii_lowercase().as_str() {
            "" | "snappy" => Some(Self::Snappy),
            "zstd" => Some(Self::Zstd),
            "uncompressed" | "none" => Some(Self::Uncompressed),
            _ => None,
        }
    }
}

/// Row-limit flush policy stored in schema options.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlushPolicy {
    /// Maximum pending hot mirror rows to keep before flushing oldest rows.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hot_row_limit: Option<u64>,
    /// Minimum excess rows required before a non-forced flush runs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_flush_rows: Option<u64>,
    /// Maximum rows written into one cold Parquet segment per flush batch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_rows_per_file: Option<u64>,
    /// Preferred compressed Parquet segment size in megabytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_file_size_mb: Option<u64>,
}

impl FlushPolicy {
    /// Builds a flush policy with all structured fields set.
    #[must_use]
    pub const fn new(hot_row_limit: u64, min_flush_rows: u64, max_rows_per_file: u64) -> Self {
        Self {
            hot_row_limit: Some(hot_row_limit),
            min_flush_rows: Some(min_flush_rows),
            max_rows_per_file: Some(max_rows_per_file),
            target_file_size_mb: None,
        }
    }

    /// Returns true when automatic flush is configured.
    #[must_use]
    pub fn enabled(&self) -> bool {
        self.hot_row_limit.is_some_and(|limit| limit > 0)
    }

    /// Loads a flush policy from persisted schema options JSON.
    #[must_use]
    pub fn from_value(value: &Value) -> Option<Self> {
        ManageTableOptions::from_value(value).flush_policy()
    }
}

/// Operator and system options stored in `koldstore.schemas.options`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ManageTableOptions {
    /// Maximum pending hot mirror rows to keep before flushing oldest rows.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hot_row_limit: Option<u64>,
    /// Minimum excess rows required before a non-forced flush runs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_flush_rows: Option<u64>,
    /// Maximum rows written into one cold Parquet segment per flush batch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_rows_per_file: Option<u64>,
    /// Preferred Parquet segment size in megabytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_file_size_mb: Option<u64>,
    /// Explicit oldest-to-newest ordering column for populated-table backfill.
    #[serde(alias = "order_column", skip_serializing_if = "Option::is_none")]
    pub migration_order_by: Option<String>,
    /// Parquet compression codec.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression: Option<ParquetCompression>,
    /// Backfill batch size for populated-table migration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backfill_batch_size: Option<u64>,
    /// Operator accepted hot-only foreign-key semantics when flush is enabled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_fk_hot_only: Option<bool>,
    /// Migration lifecycle marker written by the extension.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub migration_status: Option<MigrationStatus>,
}

impl ManageTableOptions {
    /// Decodes schema options from JSON, defaulting missing fields.
    #[must_use]
    pub fn from_value(value: &Value) -> Self {
        if value.is_null() {
            return Self::default();
        }
        serde_json::from_value(value.clone()).unwrap_or_default()
    }

    /// Encodes schema options to JSON for catalog persistence.
    ///
    /// Derived fields such as `cold_metadata` are merged separately at registration time.
    #[must_use]
    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or_else(|_| Value::Object(Default::default()))
    }

    /// Returns true when automatic flush is configured.
    #[must_use]
    pub fn flush_enabled(&self) -> bool {
        self.hot_row_limit().is_some()
    }

    /// Returns the configured hot-row limit when flush is enabled.
    #[must_use]
    pub fn hot_row_limit(&self) -> Option<u64> {
        self.hot_row_limit.filter(|limit| *limit > 0)
    }

    /// Returns the structured flush policy when flush is enabled.
    #[must_use]
    pub fn flush_policy(&self) -> Option<FlushPolicy> {
        let hot_row_limit = self.hot_row_limit()?;
        Some(FlushPolicy {
            hot_row_limit: Some(hot_row_limit),
            min_flush_rows: self.min_flush_rows.filter(|value| *value > 0),
            max_rows_per_file: self.max_rows_per_file.filter(|value| *value > 0),
            target_file_size_mb: self.target_file_size_mb.filter(|value| *value > 0),
        })
    }

    /// Sets structured flush settings.
    #[must_use]
    pub fn with_flush(
        mut self,
        hot_row_limit: u64,
        min_flush_rows: u64,
        max_rows_per_file: u64,
    ) -> Self {
        self.hot_row_limit = Some(hot_row_limit);
        self.min_flush_rows = Some(min_flush_rows);
        self.max_rows_per_file = Some(max_rows_per_file);
        self
    }

    /// Sets the preferred Parquet segment size in megabytes.
    #[must_use]
    pub fn with_target_file_size_mb(mut self, target_file_size_mb: u64) -> Self {
        self.target_file_size_mb = Some(target_file_size_mb);
        self
    }

    /// Sets the explicit migration ordering column.
    #[must_use]
    pub fn with_migration_order_by(mut self, column: impl Into<String>) -> Self {
        self.migration_order_by = Some(column.into());
        self
    }

    /// Sets the Parquet compression codec.
    #[must_use]
    pub fn with_compression(mut self, codec: ParquetCompression) -> Self {
        self.compression = Some(codec);
        self
    }

    /// Sets the migration lifecycle marker.
    #[must_use]
    pub fn with_migration_status(mut self, status: MigrationStatus) -> Self {
        self.migration_status = Some(status);
        self
    }

    /// Returns a trimmed explicit migration ordering column when configured.
    #[must_use]
    pub fn explicit_migration_order_by(&self) -> Option<&str> {
        self.migration_order_by
            .as_deref()
            .map(str::trim)
            .filter(|column| !column.is_empty())
    }

    /// Returns whether the operator accepted hot-only FK semantics.
    #[must_use]
    pub fn allow_fk_hot_only(&self) -> bool {
        self.allow_fk_hot_only.unwrap_or(false)
    }
}

/// Returns whether schema options configure automatic flush.
#[must_use]
pub fn flush_enabled_from_options(options: &Value) -> bool {
    ManageTableOptions::from_value(options).flush_enabled()
}

/// Returns the configured hot-row limit from schema options.
#[must_use]
pub fn hot_row_limit_from_options(options: &Value) -> Option<u64> {
    ManageTableOptions::from_value(options).hot_row_limit()
}

#[cfg(test)]
mod tests {
    use super::{
        validate_max_rows_per_file, FlushPolicy, ManageTableOptions, MigrationStatus,
        ParquetCompression, DEFAULT_MIN_MAX_ROWS_PER_FILE,
    };

    #[test]
    fn validate_max_rows_per_file_accepts_values_at_or_above_floor() {
        assert!(validate_max_rows_per_file(1_000, 1_000, None).is_ok());
        assert!(validate_max_rows_per_file(5_000, 1_000, None).is_ok());
    }

    #[test]
    fn validate_max_rows_per_file_rejects_values_below_floor() {
        let error = validate_max_rows_per_file(1, DEFAULT_MIN_MAX_ROWS_PER_FILE, None).unwrap_err();
        assert!(error.contains("must be at least 1000"));
        assert!(error.contains("(got 1)"));
    }

    #[test]
    fn validate_max_rows_per_file_includes_override_hint_when_provided() {
        let error = validate_max_rows_per_file(
            500,
            1_000,
            Some("lower the floor with SET koldstore.min_max_rows_per_file = 100"),
        )
        .unwrap_err();

        assert!(error.contains("SET koldstore.min_max_rows_per_file = 100"));
    }

    #[test]
    fn manage_table_options_round_trip_flush_fields() {
        let options = ManageTableOptions::default().with_flush(10_000, 1_000, 500);
        let value = options.to_value();

        assert_eq!(
            value,
            serde_json::json!({
                "hot_row_limit": 10_000,
                "min_flush_rows": 1_000,
                "max_rows_per_file": 500,
            })
        );

        let decoded = ManageTableOptions::from_value(&value);
        assert_eq!(decoded.hot_row_limit(), Some(10_000));
        assert_eq!(
            decoded.flush_policy(),
            Some(FlushPolicy::new(10_000, 1_000, 500))
        );
    }

    #[test]
    fn manage_table_options_persist_new_migration_and_file_size_names() {
        let options = ManageTableOptions::default()
            .with_migration_order_by("created_at")
            .with_target_file_size_mb(256);

        assert_eq!(
            options.to_value(),
            serde_json::json!({
                "migration_order_by": "created_at",
                "target_file_size_mb": 256,
            })
        );
        assert_eq!(options.explicit_migration_order_by(), Some("created_at"));
    }

    #[test]
    fn manage_table_options_decode_legacy_order_column() {
        let options = ManageTableOptions::from_value(&serde_json::json!({
            "order_column": "created_at"
        }));

        assert_eq!(options.explicit_migration_order_by(), Some("created_at"));
        assert_eq!(
            options.to_value(),
            serde_json::json!({
                "migration_order_by": "created_at",
            })
        );
    }

    #[test]
    fn flush_policy_from_value_ignores_unrelated_fields() {
        let policy = FlushPolicy::from_value(&serde_json::json!({
            "migration_order_by": "created_at",
            "hot_row_limit": 500,
        }))
        .unwrap();

        assert_eq!(policy.hot_row_limit, Some(500));
    }

    #[test]
    fn flush_policy_preserves_optional_file_size_target() {
        let policy = FlushPolicy::from_value(&serde_json::json!({
            "hot_row_limit": 500,
            "target_file_size_mb": 64,
        }))
        .unwrap();

        assert_eq!(policy.target_file_size_mb, Some(64));
    }

    #[test]
    fn migration_options_round_trip_status_and_compression() {
        let options = ManageTableOptions::default()
            .with_compression(ParquetCompression::Zstd)
            .with_migration_status(MigrationStatus::MirrorInitializing);
        let value = options.to_value();

        assert_eq!(
            value,
            serde_json::json!({
                "compression": "zstd",
                "migration_status": "mirror_initializing",
            })
        );

        let decoded = ManageTableOptions::from_value(&value);
        assert_eq!(decoded.compression, Some(ParquetCompression::Zstd));
        assert_eq!(
            decoded.migration_status,
            Some(MigrationStatus::MirrorInitializing)
        );
    }
}
