//! Storage registration and credential rotation SQL planning.
//!
//! Owns catalog mutation plans for `koldstore.storage`. PostgreSQL wrappers
//! stay in `pg_koldstore`.

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::PathTemplate;
use koldstore_common::SqlStatement;

/// Default regular (unscoped) table object path template.
pub const DEFAULT_REGULAR_PATH_TMPL: &str = "{namespace}/{tableName}/";

/// Default scoped table object path template.
pub const DEFAULT_SCOPED_PATH_TMPL: &str = "{namespace}/{tableName}/{scopeId}/";

/// Supported storage backends.
///
/// `s3` is only listed when this crate is built with the `s3` feature (default
/// for the storage package itself; opt-in via `pg_koldstore`'s `s3` feature).
#[cfg(feature = "s3")]
pub const SUPPORTED_STORAGE_TYPES: &[&str] = &["filesystem", "s3", "gcs", "azure"];
#[cfg(not(feature = "s3"))]
pub const SUPPORTED_STORAGE_TYPES: &[&str] = &["filesystem", "gcs", "azure"];

// Scalar subquery always returns exactly one row (NULL on name conflict) so
// `Spi::get_one*` never hits "SpiTupleTable positioned before the start or
// after the end" when ON CONFLICT DO NOTHING inserts nothing.
const REGISTER_STORAGE_SQL: &str = r#"
WITH inserted AS (
    INSERT INTO koldstore.storage AS s (
        id,
        name,
        storage_type,
        base_path,
        credentials,
        config,
        regular_path_tmpl,
        scoped_path_tmpl
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
    ON CONFLICT (name) DO NOTHING
    RETURNING s.id
)
SELECT (SELECT id FROM inserted)
"#;

const ALTER_STORAGE_CREDENTIALS_SQL: &str = r#"
UPDATE koldstore.storage
SET credentials = $2,
    updated_at = now()
WHERE name = $1
"#;

// Scalar subquery always returns exactly one row (NULL when no matching name).
const ALTER_STORAGE_LOCATION_SQL: &str = r#"
WITH updated AS (
    UPDATE koldstore.storage
    SET base_path = $2,
        config = COALESCE($3::jsonb, config),
        updated_at = now()
    WHERE name = $1
    RETURNING id
)
SELECT (SELECT id FROM updated)
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
    /// A storage row with this name already exists.
    #[error("storage `{0}` already exists")]
    StorageAlreadyExists(String),
    /// Regular table path template is invalid.
    #[error("invalid regular_path_tmpl: {0}")]
    InvalidRegularPathTmpl(String),
    /// Scoped table path template is invalid.
    #[error("invalid scoped_path_tmpl: {0}")]
    InvalidScopedPathTmpl(String),
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
    /// Regular (unscoped) table path template.
    pub regular_path_tmpl: String,
    /// Scoped table path template.
    pub scoped_path_tmpl: String,
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

    /// Renders the regular (unscoped) object prefix for a managed table.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured regular template is invalid.
    pub fn render_regular_prefix(
        &self,
        namespace: &str,
        table_name: &str,
    ) -> Result<String, String> {
        PathTemplate::new(&self.regular_path_tmpl).render(namespace, table_name, None)
    }

    /// Renders the scoped object prefix for a managed table.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured scoped template is invalid or scope is blank.
    pub fn render_scoped_prefix(
        &self,
        namespace: &str,
        table_name: &str,
        scope_id: &str,
    ) -> Result<String, String> {
        let scope_id = scope_id.trim();
        if scope_id.is_empty() {
            return Err("scopeId is required by path template".to_string());
        }
        PathTemplate::new(&self.scoped_path_tmpl).render(namespace, table_name, Some(scope_id))
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

        self.render_regular_prefix("namespace", "table")
            .map_err(DdlError::InvalidRegularPathTmpl)?;

        if !self.scoped_path_tmpl.contains("{scopeId}") {
            return Err(DdlError::InvalidScopedPathTmpl(
                "scoped path template must include {scopeId}".to_string(),
            ));
        }
        self.render_scoped_prefix("namespace", "table", "scope")
            .map_err(DdlError::InvalidScopedPathTmpl)?;

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
