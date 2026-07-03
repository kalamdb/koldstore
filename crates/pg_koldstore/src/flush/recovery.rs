//! Orphan object recovery.

/// Recovery actions for orphan objects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryAction {
    /// Delete an unreferenced temporary object.
    DeleteTemp,
    /// Quarantine an unmanifested final object.
    QuarantineFinal,
}

/// Classifies an orphan object for recovery.
#[must_use]
pub fn classify_orphan_object(path: &str, manifest_referenced: bool) -> Option<RecoveryAction> {
    if manifest_referenced {
        None
    } else if path.ends_with(".tmp") {
        Some(RecoveryAction::DeleteTemp)
    } else {
        Some(RecoveryAction::QuarantineFinal)
    }
}
