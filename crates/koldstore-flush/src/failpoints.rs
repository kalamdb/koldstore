//! Typed flush / mirror failpoint names and GUC action prefixes (pg-free).
//!
//! [`FlushFailpoint`] is the registry of armable phase names. [`FailpointAction`]
//! parses `error:` / `wait:` / `panic:` / `sleep:` prefixes from the GUC value.
//! SPI/GUC arming and barrier waits stay in `pg_koldstore::failpoints`.

/// Typed flush / mirror failpoints that map to GUC arming names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FlushFailpoint {
    /// After durable job claim commit.
    AfterClaim,
    /// After mirror row selection for a pass.
    AfterSelectRows,
    /// After pending segment catalog insert.
    AfterPendingSegment,
    /// During Parquet encode / write.
    DuringParquetWrite,
    /// After temporary object upload.
    AfterTempObject,
    /// After checksum / object metadata is known.
    AfterChecksumMetadata,
    /// Before manifest object publish.
    BeforeManifestPublish,
    /// Before cold segment activation.
    BeforeActivate,
    /// After manifest publish succeeds.
    AfterManifestPublish,
    /// Before hot/mirror prune.
    BeforeHotCleanup,
    /// During hot/mirror prune.
    DuringHotCleanup,
    /// After prune, before terminal job completion.
    AfterCleanupBeforeJobComplete,
    /// After job completion, before temp object cleanup.
    AfterJobCompleteBeforeTempCleanup,
    /// After a pass checkpoint / progress update.
    AfterPassProgress,
    /// Before acquiring the logical-slot / apply advisory lock.
    BeforeSlotLock,
    /// After acquiring the logical-slot / apply advisory lock.
    AfterSlotLock,
    /// Before acquiring the source-table fence lock.
    BeforeSourceLock,
    /// After acquiring the source-table fence lock.
    AfterSourceLock,
    /// Inside the async mirror apply loop (per change).
    AsyncMirrorApply,
    /// After an async mirror apply batch commits.
    AsyncMirrorApplyAfterBatch,
}

impl FlushFailpoint {
    /// GUC / harness name for this failpoint.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AfterClaim => "after_claim",
            Self::AfterSelectRows => "after_select_rows",
            Self::AfterPendingSegment => "after_pending_segment",
            Self::DuringParquetWrite => "during_parquet_write",
            Self::AfterTempObject => "after_temp_object",
            Self::AfterChecksumMetadata => "after_checksum_metadata",
            Self::BeforeManifestPublish => "before_manifest_publish",
            Self::BeforeActivate => "before_activate",
            Self::AfterManifestPublish => "after_manifest_publish",
            Self::BeforeHotCleanup => "before_hot_cleanup",
            Self::DuringHotCleanup => "during_hot_cleanup",
            Self::AfterCleanupBeforeJobComplete => "after_cleanup_before_job_complete",
            Self::AfterJobCompleteBeforeTempCleanup => "after_job_complete_before_temp_cleanup",
            Self::AfterPassProgress => "after_pass_progress",
            Self::BeforeSlotLock => "before_slot_lock",
            Self::AfterSlotLock => "after_slot_lock",
            Self::BeforeSourceLock => "before_source_lock",
            Self::AfterSourceLock => "after_source_lock",
            Self::AsyncMirrorApply => "async_mirror_apply",
            Self::AsyncMirrorApplyAfterBatch => "async_mirror_apply_after_batch",
        }
    }

    /// Parses a GUC target name into a typed failpoint.
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "after_claim" => Self::AfterClaim,
            "after_select_rows" => Self::AfterSelectRows,
            "after_pending_segment" => Self::AfterPendingSegment,
            "during_parquet_write" => Self::DuringParquetWrite,
            "after_temp_object" => Self::AfterTempObject,
            "after_checksum_metadata" => Self::AfterChecksumMetadata,
            "before_manifest_publish" => Self::BeforeManifestPublish,
            "before_activate" => Self::BeforeActivate,
            "after_manifest_publish" => Self::AfterManifestPublish,
            "before_hot_cleanup" => Self::BeforeHotCleanup,
            "during_hot_cleanup" => Self::DuringHotCleanup,
            "after_cleanup_before_job_complete" => Self::AfterCleanupBeforeJobComplete,
            "after_job_complete_before_temp_cleanup" => Self::AfterJobCompleteBeforeTempCleanup,
            "after_pass_progress" | "after_wave_progress" => Self::AfterPassProgress,
            "before_slot_lock" => Self::BeforeSlotLock,
            "after_slot_lock" => Self::AfterSlotLock,
            "before_source_lock" => Self::BeforeSourceLock,
            "after_source_lock" => Self::AfterSourceLock,
            "async_mirror_apply" => Self::AsyncMirrorApply,
            "async_mirror_apply_after_batch" => Self::AsyncMirrorApplyAfterBatch,
            _ => return None,
        })
    }

    /// All registered failpoints (stable order for docs / tests).
    #[must_use]
    pub const fn all() -> &'static [Self] {
        &FLUSH_FAILPOINTS
    }
}

const FLUSH_FAILPOINTS: &[FlushFailpoint] = &[
    FlushFailpoint::AfterClaim,
    FlushFailpoint::AfterSelectRows,
    FlushFailpoint::AfterPendingSegment,
    FlushFailpoint::DuringParquetWrite,
    FlushFailpoint::AfterTempObject,
    FlushFailpoint::AfterChecksumMetadata,
    FlushFailpoint::BeforeManifestPublish,
    FlushFailpoint::BeforeActivate,
    FlushFailpoint::AfterManifestPublish,
    FlushFailpoint::BeforeHotCleanup,
    FlushFailpoint::DuringHotCleanup,
    FlushFailpoint::AfterCleanupBeforeJobComplete,
    FlushFailpoint::AfterJobCompleteBeforeTempCleanup,
    FlushFailpoint::AfterPassProgress,
    FlushFailpoint::BeforeSlotLock,
    FlushFailpoint::AfterSlotLock,
    FlushFailpoint::BeforeSourceLock,
    FlushFailpoint::AfterSourceLock,
    FlushFailpoint::AsyncMirrorApply,
    FlushFailpoint::AsyncMirrorApplyAfterBatch,
];

/// Canonical failpoint names (flush crash points + async apply).
pub const FAILPOINT_NAMES: &[&str] = &[
    "after_claim",
    "after_select_rows",
    "after_pending_segment",
    "during_parquet_write",
    "after_temp_object",
    "after_checksum_metadata",
    "before_manifest_publish",
    "before_activate",
    "after_manifest_publish",
    "before_hot_cleanup",
    "during_hot_cleanup",
    "after_cleanup_before_job_complete",
    "after_job_complete_before_temp_cleanup",
    "after_pass_progress",
    "before_slot_lock",
    "after_slot_lock",
    "before_source_lock",
    "after_source_lock",
    "async_mirror_apply",
    "async_mirror_apply_after_batch",
];

/// Action taken when an armed failpoint is hit.
///
/// Destructive variants ([`FailpointAction::Panic`], [`FailpointAction::Sleep`])
/// are always defined so parse stays pg-free; production `hit` adapters only
/// wire [`FailpointAction::Error`] / [`FailpointAction::Wait`] unless test
/// features enable the rest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailpointAction {
    /// Return a controlled error from the flush / apply path.
    Error,
    /// Block on the advisory barrier until another session releases it.
    Wait,
    /// Panic / SIGKILL the backend (test harness; not selected by production hit).
    Panic,
    /// Sleep without blocking peers (test harness; not selected by production hit).
    Sleep,
}

impl FailpointAction {
    /// Parses the action prefix of an armed GUC value (`error:`, `wait:`, …).
    ///
    /// Bare names default to [`FailpointAction::Error`].
    #[must_use]
    pub fn parse_prefix(armed: &str) -> (Self, &str) {
        if let Some(rest) = armed.strip_prefix("wait:") {
            (Self::Wait, rest)
        } else if let Some(rest) = armed.strip_prefix("error:") {
            (Self::Error, rest)
        } else if let Some(rest) = armed.strip_prefix("panic:") {
            (Self::Panic, rest)
        } else if let Some(rest) = armed.strip_prefix("sleep:") {
            (Self::Sleep, rest)
        } else {
            (Self::Error, armed)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{FailpointAction, FlushFailpoint, FAILPOINT_NAMES};

    #[test]
    fn flush_failpoint_as_str_round_trips_parse() {
        for point in FlushFailpoint::all() {
            let name = point.as_str();
            assert_eq!(FlushFailpoint::parse(name), Some(*point));
            assert!(
                FAILPOINT_NAMES.contains(&name),
                "missing FAILPOINT_NAMES entry for {name}"
            );
        }
        assert_eq!(FlushFailpoint::all().len(), FAILPOINT_NAMES.len());
    }

    #[test]
    fn failpoint_action_parse_prefix_defaults_to_error() {
        assert_eq!(
            FailpointAction::parse_prefix("after_claim"),
            (FailpointAction::Error, "after_claim")
        );
        assert_eq!(
            FailpointAction::parse_prefix("error:before_activate"),
            (FailpointAction::Error, "before_activate")
        );
        assert_eq!(
            FailpointAction::parse_prefix("wait:before_slot_lock"),
            (FailpointAction::Wait, "before_slot_lock")
        );
        assert_eq!(
            FailpointAction::parse_prefix("panic:after_claim"),
            (FailpointAction::Panic, "after_claim")
        );
        assert_eq!(
            FailpointAction::parse_prefix("sleep:during_parquet_write"),
            (FailpointAction::Sleep, "during_parquet_write")
        );
    }

    #[test]
    fn unknown_failpoint_name_does_not_parse() {
        assert_eq!(FlushFailpoint::parse("not_a_real_point"), None);
    }

    #[test]
    fn lock_failpoints_use_stable_guc_names() {
        assert_eq!(FlushFailpoint::BeforeSlotLock.as_str(), "before_slot_lock");
        assert_eq!(FlushFailpoint::AfterSlotLock.as_str(), "after_slot_lock");
        assert_eq!(
            FlushFailpoint::BeforeSourceLock.as_str(),
            "before_source_lock"
        );
        assert_eq!(FlushFailpoint::AfterSourceLock.as_str(), "after_source_lock");
    }
}
