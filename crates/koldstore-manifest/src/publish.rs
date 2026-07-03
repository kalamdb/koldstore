//! Manifest publish plan placeholders.

/// Object written by a publish operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedObject {
    pub temp_path: String,
    pub final_path: String,
    pub etag: Option<String>,
}

/// Planned publish sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestPublishPlan {
    pub temp_path: String,
    pub final_path: String,
    pub manifest_path: String,
}

impl ManifestPublishPlan {
    /// Builds a backend-safe segment publish plan.
    #[must_use]
    pub fn for_segment(
        prefix: &str,
        file_name: &str,
        writer_id: &str,
        manifest_name: &str,
    ) -> Self {
        let prefix = prefix.trim_matches('/');
        Self {
            temp_path: format!("{prefix}/.tmp/{writer_id}/{file_name}.tmp"),
            final_path: format!("{prefix}/{file_name}"),
            manifest_path: format!("{prefix}/{manifest_name}"),
        }
    }

    /// Returns ordered publish actions. Manifest write is the visibility boundary.
    #[must_use]
    pub fn actions(&self) -> Vec<koldstore_storage::PublishAction> {
        koldstore_storage::backend_safe_publish_actions(
            &self.temp_path,
            &self.final_path,
            &self.manifest_path,
        )
    }
}
