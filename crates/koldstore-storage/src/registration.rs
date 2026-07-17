//! Storage registration and credential rotation SQL planning.
//!
//! Owns catalog mutation plans for `koldstore.storage`. PostgreSQL wrappers
//! stay in `pg_koldstore`.

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::PathTemplate;
use koldstore_common::SqlStatement;

/// Default shared table object path template.
pub const DEFAULT_SHARED_PATH_TEMPLATE: &str = "{namespace}/{tableName}/";

/// Default user-scoped table object path template.
pub const DEFAULT_USER_PATH_TEMPLATE: &str = "{namespace}/{tableName}/{scopeId}/";

/// Supported storage backends.
pub const SUPPORTED_STORAGE_TYPES: &[&str] = &["filesystem", "s3", "gcs", "azure"];

const REGISTER_STORAGE_SQL: &str = r#"
INSERT INTO koldstore.storage AS s (
    id,
    name,
    storage_type,
    base_path,
    credentials,
    config,
    shared_path_template,
    user_path_template
)
VALUES (
    $1,
    $2,
    $3,
    $4,
    jsonb_strip_nulls($5::jsonb),
    COALESCE($6::jsonb, '{}'::jsonb),
    $7,
    $8
)
ON CONFLICT (name) DO UPDATE
SET storage_type = EXCLUDED.storage_type,
    base_path = EXCLUDED.base_path,
    credentials = EXCLUDED.credentials,
    config = EXCLUDED.config,
    shared_path_template = EXCLUDED.shared_path_template,
    user_path_template = EXCLUDED.user_path_template,
    updated_at = now()
RETURNING s.id
"#;

const ALTER_STORAGE_CREDENTIALS_SQL: &str = r#"
UPDATE koldstore.storage
SET credentials = $2,
    updated_at = now()
WHERE name = $1
"#;

const ALTER_STORAGE_LOCATION_SQL: &str = r#"
UPDATE koldstore.storage
SET base_path = $2,
    config = COALESCE($3::jsonb, config),
    updated_at = now()
WHERE name = $1
RETURNING id
"#;

/// DDL SQL function planning result.
pub type DdlResult<T> = Result<T, DdlError>;

/// Storage DDL validation or planning error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DdlError {
    /// Storage name is blank.
    #[error("storage name cannot be blank")]
    BlankName,
    /// Storage base path is blank.
    #[error("storage base_path cannot be blank")]
    BlankBasePath,
    /// Storage backend is not supported.
    #[error("unsupported storage_type `{0}`")]
    UnsupportedStorageType(String),
    /// Shared table path template is invalid.
    #[error("invalid shared_path_template: {0}")]
    InvalidSharedPathTemplate(String),
    /// User table path template is invalid.
    #[error("invalid user_path_template: {0}")]
    InvalidUserPathTemplate(String),
    /// SQL statement metadata could not be prepared.
    #[error("{0}")]
    Sql(String),
}

/// Storage registration request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StorageRegistration {
    /// Unique storage name.
    pub name: String,
    /// Backend type.
    pub storage_type: String,
    /// Base object-store path.
    pub base_path: String,
    /// Redacted or raw credentials depending on caller privileges.
    pub credentials: serde_json::Value,
    /// Backend config.
    pub config: serde_json::Value,
    /// Shared table path template.
    pub shared_path_template: String,
    /// User table path template.
    pub user_path_template: String,
}

/// Planned storage registration catalog mutation.
#[derive(Debug, Clone, PartialEq)]
pub struct StorageRegistrationPlan {
    /// Storage id to bind as parameter `$1`.
    pub storage_id: Uuid,
    /// Validated registration values to bind as parameters `$2` through `$8`.
    pub registration: StorageRegistration,
    /// Parameterized catalog mutation statement.
    pub statement: SqlStatement,
}

/// Planned storage credential rotation catalog mutation.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterStorageCredentialsPlan {
    /// Storage name to bind as parameter `$1`.
    pub storage_name: String,
    /// New credentials to bind as parameter `$2`.
    pub credentials: serde_json::Value,
    /// Parameterized catalog mutation statement.
    pub statement: SqlStatement,
}

/// Planned storage location/configuration mutation.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterStorageLocationPlan {
    /// Storage name to bind as parameter `$1`.
    pub storage_name: String,
    /// New base path to bind as parameter `$2`.
    pub base_path: String,
    /// Optional config replacement to bind as parameter `$3`.
    pub config: serde_json::Value,
    /// Parameterized catalog mutation statement.
    pub statement: SqlStatement,
}

impl StorageRegistration {
    /// Returns a credential-redacted copy for application-role diagnostics.
    #[must_use]
    pub fn redacted(&self) -> Self {
        let mut copy = self.clone();
        copy.credentials = serde_json::json!({"redacted": true});
        copy
    }

    /// Renders the shared object prefix for a managed table.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured shared template is invalid.
    pub fn render_shared_prefix(
        &self,
        namespace: &str,
        table_name: &str,
    ) -> Result<String, String> {
        PathTemplate::new(&self.shared_path_template).render(namespace, table_name, None)
    }

    /// Renders the user-scoped object prefix for a managed table.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured user template is invalid or scope is blank.
    pub fn render_user_prefix(
        &self,
        namespace: &str,
        table_name: &str,
        scope_id: &str,
    ) -> Result<String, String> {
        let scope_id = scope_id.trim();
        if scope_id.is_empty() {
            return Err("scopeId is required by path template".to_string());
        }
        PathTemplate::new(&self.user_path_template).render(namespace, table_name, Some(scope_id))
    }

    /// Validates storage registration inputs before catalog mutation planning.
    ///
    /// # Errors
    ///
    /// Returns an error when required fields are blank, the backend is unsupported,
    /// or either path template cannot be rendered with the required placeholders.
    pub fn validate(&self) -> DdlResult<()> {
        if self.name.trim().is_empty() {
            return Err(DdlError::BlankName);
        }
        if self.base_path.trim().is_empty() {
            return Err(DdlError::BlankBasePath);
        }
        if !SUPPORTED_STORAGE_TYPES.contains(&self.storage_type.as_str()) {
            return Err(DdlError::UnsupportedStorageType(self.storage_type.clone()));
        }

        self.render_shared_prefix("namespace", "table")
            .map_err(DdlError::InvalidSharedPathTemplate)?;

        if !self.user_path_template.contains("{scopeId}") {
            return Err(DdlError::InvalidUserPathTemplate(
                "user path template must include {scopeId}".to_string(),
            ));
        }
        self.render_user_prefix("namespace", "table", "scope")
            .map_err(DdlError::InvalidUserPathTemplate)?;

        Ok(())
    }

    /// Builds a storage registration catalog mutation plan with a generated id.
    ///
    /// # Errors
    ///
    /// Returns an error when the registration is invalid or the SQL statement
    /// metadata cannot be prepared.
    pub fn register_plan(&self) -> DdlResult<StorageRegistrationPlan> {
        self.register_plan_with_id(Uuid::new_v4())
    }

    /// Builds a storage registration catalog mutation plan with a caller-provided id.
    ///
    /// # Errors
    ///
    /// Returns an error when the registration is invalid or the SQL statement
    /// metadata cannot be prepared.
    pub fn register_plan_with_id(&self, storage_id: Uuid) -> DdlResult<StorageRegistrationPlan> {
        self.validate()?;
        let statement = SqlStatement::write("register storage", REGISTER_STORAGE_SQL)
            .map_err(|error| DdlError::Sql(error.to_string()))?;

        Ok(StorageRegistrationPlan {
            storage_id,
            registration: self.clone(),
            statement,
        })
    }
}

/// Builds a storage credential rotation catalog mutation plan.
///
/// # Errors
///
/// Returns an error when the storage name is blank or statement metadata cannot
/// be prepared.
pub fn alter_storage_credentials_plan(
    name: &str,
    credentials: serde_json::Value,
) -> DdlResult<AlterStorageCredentialsPlan> {
    let storage_name = name.trim();
    if storage_name.is_empty() {
        return Err(DdlError::BlankName);
    }

    let statement = SqlStatement::write("alter storage credentials", ALTER_STORAGE_CREDENTIALS_SQL)
        .map_err(|error| DdlError::Sql(error.to_string()))?;

    Ok(AlterStorageCredentialsPlan {
        storage_name: storage_name.to_string(),
        credentials,
        statement,
    })
}

/// Builds a storage location/configuration mutation plan.
///
/// # Errors
///
/// Returns an error when the storage name or base path is blank, or statement
/// metadata cannot be prepared.
pub fn alter_storage_location_plan(
    name: &str,
    base_path: &str,
    config: serde_json::Value,
) -> DdlResult<AlterStorageLocationPlan> {
    let storage_name = name.trim();
    if storage_name.is_empty() {
        return Err(DdlError::BlankName);
    }
    let base_path = base_path.trim();
    if base_path.is_empty() {
        return Err(DdlError::BlankBasePath);
    }

    let statement = SqlStatement::write("alter storage location", ALTER_STORAGE_LOCATION_SQL)
        .map_err(|error| DdlError::Sql(error.to_string()))?;

    Ok(AlterStorageLocationPlan {
        storage_name: storage_name.to_string(),
        base_path: base_path.to_string(),
        config,
        statement,
    })
}
