//! Public DDL SQL function boundaries.

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::migrate::rehydrate::DemigrateOptions;
use crate::spi::SpiStatement;
use koldstore_storage::PathTemplate;

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

/// Registers storage and returns its id.
#[must_use]
pub fn register_storage_name_only(_name: &str) -> Uuid {
    Uuid::new_v4()
}

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
    /// SPI statement metadata could not be prepared.
    #[error("{0}")]
    Spi(String),
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
    pub statement: SpiStatement,
}

/// Planned storage credential rotation catalog mutation.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterStorageCredentialsPlan {
    /// Storage name to bind as parameter `$1`.
    pub storage_name: String,
    /// New credentials to bind as parameter `$2`.
    pub credentials: serde_json::Value,
    /// Parameterized catalog mutation statement.
    pub statement: SpiStatement,
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
    /// Returns an error when the registration is invalid or the SPI statement
    /// metadata cannot be prepared.
    pub fn register_plan(&self) -> DdlResult<StorageRegistrationPlan> {
        self.register_plan_with_id(Uuid::new_v4())
    }

    /// Builds a storage registration catalog mutation plan with a caller-provided id.
    ///
    /// # Errors
    ///
    /// Returns an error when the registration is invalid or the SPI statement
    /// metadata cannot be prepared.
    pub fn register_plan_with_id(&self, storage_id: Uuid) -> DdlResult<StorageRegistrationPlan> {
        self.validate()?;
        let statement = SpiStatement::write("register storage", REGISTER_STORAGE_SQL)
            .map_err(|error| DdlError::Spi(error.to_string()))?;

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

    let statement = SpiStatement::write("alter storage credentials", ALTER_STORAGE_CREDENTIALS_SQL)
        .map_err(|error| DdlError::Spi(error.to_string()))?;

    Ok(AlterStorageCredentialsPlan {
        storage_name: storage_name.to_string(),
        credentials,
        statement,
    })
}

/// Registers or updates a storage backend from SQL.
#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
#[pgrx::pg_extern(name = "register_storage", schema = "koldstore")]
pub fn register_storage_pg(
    name: &str,
    storage_type: &str,
    base_path: &str,
    credentials: pgrx::JsonB,
    config: pgrx::JsonB,
    shared_path_template: &str,
    user_path_template: &str,
) -> pgrx::Uuid {
    register_storage_pg_impl(
        name,
        storage_type,
        base_path,
        credentials,
        config,
        shared_path_template,
        user_path_template,
    )
}

/// Registers or updates a storage backend from SQL using default path templates.
#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
#[pgrx::pg_extern(name = "register_storage", schema = "koldstore")]
pub fn register_storage_pg_with_default_templates(
    name: &str,
    storage_type: &str,
    base_path: &str,
    credentials: pgrx::JsonB,
    config: pgrx::JsonB,
) -> pgrx::Uuid {
    register_storage_pg_impl(
        name,
        storage_type,
        base_path,
        credentials,
        config,
        DEFAULT_SHARED_PATH_TEMPLATE,
        DEFAULT_USER_PATH_TEMPLATE,
    )
}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
fn register_storage_pg_impl(
    name: &str,
    storage_type: &str,
    base_path: &str,
    credentials: pgrx::JsonB,
    config: pgrx::JsonB,
    shared_path_template: &str,
    user_path_template: &str,
) -> pgrx::Uuid {
    use pgrx::datum::DatumWithOid;

    let registration = StorageRegistration {
        name: name.to_string(),
        storage_type: storage_type.to_string(),
        base_path: base_path.to_string(),
        credentials: credentials.0,
        config: config.0,
        shared_path_template: shared_path_template.to_string(),
        user_path_template: user_path_template.to_string(),
    };
    let plan = registration
        .register_plan()
        .unwrap_or_else(|error| pgrx::error!("{error}"));
    let storage_id = pgrx::Uuid::from_bytes(*plan.storage_id.as_bytes());

    let args = [
        DatumWithOid::from(storage_id),
        DatumWithOid::from(plan.registration.name.as_str()),
        DatumWithOid::from(plan.registration.storage_type.as_str()),
        DatumWithOid::from(plan.registration.base_path.as_str()),
        DatumWithOid::from(pgrx::JsonB(plan.registration.credentials)),
        DatumWithOid::from(pgrx::JsonB(plan.registration.config)),
        DatumWithOid::from(plan.registration.shared_path_template.as_str()),
        DatumWithOid::from(plan.registration.user_path_template.as_str()),
    ];

    let returned = pgrx::Spi::get_one_with_args::<pgrx::Uuid>(&plan.statement.sql, &args)
        .unwrap_or_else(|error| pgrx::error!("register storage failed: {error}"))
        .unwrap_or_else(|| pgrx::error!("register storage did not return an id"));

    returned
}

/// Rotates storage credentials from SQL without changing backend paths.
#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
#[pgrx::pg_extern(name = "alter_storage_credentials", schema = "koldstore")]
pub fn alter_storage_credentials_pg(name: &str, credentials: pgrx::JsonB) {
    use pgrx::datum::DatumWithOid;

    let plan = alter_storage_credentials_plan(name, credentials.0)
        .unwrap_or_else(|error| pgrx::error!("{error}"));
    let args = [
        DatumWithOid::from(plan.storage_name.as_str()),
        DatumWithOid::from(pgrx::JsonB(plan.credentials)),
    ];

    pgrx::Spi::run_with_args(&plan.statement.sql, &args)
        .unwrap_or_else(|error| pgrx::error!("alter storage credentials failed: {error}"));
}

/// Migration request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MigrateTableRequest {
    /// PostgreSQL relation name.
    pub table_name: String,
    /// `shared` or `user`.
    pub table_type: String,
    /// Storage registration name.
    pub storage_name: String,
    /// Optional flush policy.
    pub flush_policy: Option<String>,
    /// Optional app scope column.
    pub scope_column: Option<String>,
    /// Additional options.
    pub options: serde_json::Value,
}

/// Demigration request from `koldstore.demigrate_table`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DemigrateTableRequest {
    /// PostgreSQL relation name.
    pub table_name: String,
    /// Optional SQL argument; defaults to rehydrate.
    pub rehydrate: Option<bool>,
    /// Optional SQL argument; defaults to retaining cold artifacts.
    pub drop_cold: Option<bool>,
    /// Optional SQL argument; defaults to keeping system columns as ordinary columns.
    pub drop_system_columns: Option<bool>,
}

impl DemigrateTableRequest {
    /// Converts SQL optional arguments into demigration options.
    #[must_use]
    pub fn options(&self) -> DemigrateOptions {
        DemigrateOptions {
            rehydrate: self.rehydrate.unwrap_or(true),
            drop_cold: self.drop_cold.unwrap_or(false),
            drop_system_columns: self.drop_system_columns.unwrap_or(false),
        }
    }
}

impl MigrateTableRequest {
    /// Returns the effective user scope column.
    #[must_use]
    pub fn effective_scope_column(&self) -> Option<&str> {
        if self.table_type == "user" {
            Some(self.scope_column.as_deref().unwrap_or("_user_id"))
        } else {
            None
        }
    }

    /// Returns whether the request targets a supported greenfield table type.
    #[must_use]
    pub fn has_supported_table_type(&self) -> bool {
        matches!(self.table_type.as_str(), "shared" | "user")
    }

    /// Returns whether user-scope arguments are sufficient for migration.
    #[must_use]
    pub fn has_valid_scope_arguments(&self) -> bool {
        self.table_type != "user"
            || self
                .effective_scope_column()
                .map(str::trim)
                .filter(|scope| !scope.is_empty())
                .is_some()
    }
}
