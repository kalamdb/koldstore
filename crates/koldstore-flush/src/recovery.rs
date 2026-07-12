//! Orphan object recovery.

use std::collections::HashSet;

use koldstore_common::{KoldstoreError, Result};
use koldstore_storage::{ObjectStoreClient, PutPrecondition, StorageClient};

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
///
/// Unreferenced `segment-*.parquet` finals and temp objects are recovered the
/// same way; callers should also mark matching catalog rows `orphaned` when a
/// catalog segment id is known (see [`plan_mark_segments_orphaned`]).
#[must_use]
pub fn classify_orphan_object(path: &str, manifest_referenced: bool) -> Option<RecoveryAction> {
    if manifest_referenced {
        None
    } else if path.contains("/.tmp/") || path.ends_with(".tmp") {
        Some(RecoveryAction::DeleteTemp)
    } else {
        Some(RecoveryAction::QuarantineFinal)
    }
}

/// Returns whether a path looks like a cold segment object (not temp/quarantine).
#[must_use]
pub fn is_cold_segment_object_path(path: &str) -> bool {
    let name = path.rsplit('/').next().unwrap_or(path);
    name.starts_with("segment-") && name.ends_with(".parquet") && !name.contains(".quarantine.")
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

/// Discovers objects below `prefix` and marks manifest/catalog references.
///
/// # Errors
///
/// Returns an error when listing fails or a backend returns an invalid key.
pub fn discover_orphan_objects(
    client: &ObjectStoreClient,
    prefix: &str,
    referenced: &HashSet<String>,
) -> std::result::Result<Vec<OrphanObject>, String> {
    client
        .list(prefix)
        .map_err(|error| error.to_string())?
        .into_iter()
        .filter(|object| !object.key.contains(".quarantine."))
        .map(|object| {
            let is_referenced = referenced.contains(&object.key);
            Ok(OrphanObject::new(
                ObjectPath::parse(object.key).map_err(|error| error.to_string())?,
                is_referenced,
            ))
        })
        .collect()
}

/// Applies a recovery plan using idempotent object-store operations.
///
/// Final orphans are copied to a unique quarantine key before deletion.
///
/// # Errors
///
/// Returns an error when object I/O fails.
pub fn apply_recovery_plan(
    client: &ObjectStoreClient,
    plan: &RecoveryPlan,
) -> std::result::Result<(), String> {
    for step in &plan.actions {
        match step.action {
            RecoveryAction::DeleteTemp => client
                .delete(step.path.as_str())
                .map_err(|error| error.to_string())?,
            RecoveryAction::QuarantineFinal => {
                let bytes = client
                    .get(step.path.as_str())
                    .map_err(|error| error.to_string())?;
                let quarantine =
                    format!("{}.quarantine.{}", step.path.as_str(), uuid::Uuid::new_v4());
                client
                    .put(&quarantine, &bytes, PutPrecondition::CreateIfAbsent)
                    .map_err(|error| error.to_string())?;
                client
                    .delete(step.path.as_str())
                    .map_err(|error| error.to_string())?;
            }
        }
    }
    Ok(())
}
