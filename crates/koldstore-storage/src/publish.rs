//! Backend-safe publish operation descriptions.

/// Conditional put precondition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConditionalPut {
    IfAbsent,
    IfMatch,
}

impl ConditionalPut {
    /// Describes the precondition without claiming atomic rename semantics.
    #[must_use]
    pub const fn description(self) -> &'static str {
        match self {
            Self::IfAbsent => "put only when target is absent",
            Self::IfMatch => "put only when identity matches",
        }
    }
}

/// Publish action kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishAction {
    PutTemp(String),
    CopyTempToFinal { temp: String, final_path: String },
    ValidateFinal(String),
    DeleteTemp(String),
    PutManifest(String),
    Rename { from: String, to: String },
}

/// Returns a backend-safe publish sequence without assuming atomic rename.
#[must_use]
pub fn backend_safe_publish_actions(
    temp_path: &str,
    final_path: &str,
    manifest_path: &str,
) -> Vec<PublishAction> {
    vec![
        PublishAction::PutTemp(temp_path.to_string()),
        PublishAction::CopyTempToFinal {
            temp: temp_path.to_string(),
            final_path: final_path.to_string(),
        },
        PublishAction::ValidateFinal(final_path.to_string()),
        PublishAction::DeleteTemp(temp_path.to_string()),
        PublishAction::PutManifest(manifest_path.to_string()),
    ]
}
