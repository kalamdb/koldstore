//! Existing-row backfill helpers.

/// Backfill batch sizing default.
pub const DEFAULT_BACKFILL_BATCH_ROWS: usize = 10_000;

/// Existing-row backfill SQL expressions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackfillPlan {
    /// Relation name.
    pub table_name: String,
}

impl BackfillPlan {
    /// Creates a backfill plan.
    #[must_use]
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
        }
    }

    /// SQL statement that fills missing system columns.
    #[must_use]
    pub fn sql(&self) -> String {
        format!(
            "UPDATE {} SET _seq = COALESCE(_seq, SNOWFLAKE_ID()), _commit_seq = COALESCE(_commit_seq, nextval('koldstore.global_commit_seq'::regclass)), _deleted = COALESCE(_deleted, false) WHERE _seq IS NULL OR _commit_seq IS NULL OR _deleted IS NULL",
            self.table_name
        )
    }
}
