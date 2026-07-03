//! Orphan object recovery.

use koldstore_core::{KoldstoreError, Result};

/// Validated object-store path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectPath(String);

impl ObjectPath {
    /// Parses an object-store path relative to the configured storage prefix.
    ///
    /// # Errors
    ///
    /// Returns an error when the path is blank, absolute, or attempts parent traversal.
    pub fn parse(path: impl AsRef<str>) -> Result<Self> {
        let path = path.as_ref().trim();
        let invalid = path.is_empty()
            || path.starts_with('/')
            || path
                .split('/')
                .any(|component| component.is_empty() || component == "." || component == "..");

        if invalid {
            Err(KoldstoreError::InvalidIdentifier {
                kind: "object path",
                value: path.to_string(),
            })
        } else {
            Ok(Self(path.to_string()))
        }
    }

    /// Returns the object-store path.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Recovery actions for orphan objects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryAction {
    /// Delete an unreferenced temporary object.
    DeleteTemp,
    /// Quarantine an unmanifested final object.
    QuarantineFinal,
}

/// Object found during orphan recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrphanObject {
    /// Object path.
    pub path: ObjectPath,
    /// Whether the object is referenced by the committed manifest.
    pub manifest_referenced: bool,
}

impl OrphanObject {
    /// Creates an orphan recovery candidate.
    #[must_use]
    pub const fn new(path: ObjectPath, manifest_referenced: bool) -> Self {
        Self {
            path,
            manifest_referenced,
        }
    }
}

/// Planned recovery action for one object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryStep {
    /// Object path.
    pub path: ObjectPath,
    /// Whether the object is referenced by the committed manifest.
    pub manifest_referenced: bool,
    /// Recovery action.
    pub action: RecoveryAction,
}

/// Planned idempotent recovery work.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecoveryPlan {
    /// Actions that should be applied.
    pub actions: Vec<RecoveryStep>,
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

/// Builds idempotent recovery actions for unreferenced temp/final objects.
#[must_use]
pub fn plan_recovery_actions(objects: impl IntoIterator<Item = OrphanObject>) -> RecoveryPlan {
    let actions = objects
        .into_iter()
        .filter_map(|object| {
            classify_orphan_object(object.path.as_str(), object.manifest_referenced).map(|action| {
                RecoveryStep {
                    path: object.path,
                    manifest_referenced: object.manifest_referenced,
                    action,
                }
            })
        })
        .collect();

    RecoveryPlan { actions }
}
