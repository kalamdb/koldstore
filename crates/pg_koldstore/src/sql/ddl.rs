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

/// Migrates a heap table into pg-koldstore management from SQL.
#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
#[pgrx::pg_extern(name = "migrate_table", schema = "koldstore")]
pub fn migrate_table_pg(
    table_name: pgrx::pg_sys::Oid,
    table_type: &str,
    storage_name: &str,
    flush_policy: Option<&str>,
    scope_column: Option<&str>,
) -> pgrx::composite_type!('static, "koldstore.managed_table_info") {
    migrate_table_pg_impl(
        table_name,
        table_type,
        storage_name,
        flush_policy,
        scope_column,
        None,
    )
}

/// Migrates a heap table and supplies an explicit oldest-to-newest order column.
#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
#[pgrx::pg_extern(name = "migrate_table", schema = "koldstore")]
pub fn migrate_table_pg_with_order(
    table_name: pgrx::pg_sys::Oid,
    table_type: &str,
    storage_name: &str,
    flush_policy: Option<&str>,
    scope_column: Option<&str>,
    order_column: Option<&str>,
) -> pgrx::composite_type!('static, "koldstore.managed_table_info") {
    migrate_table_pg_impl(
        table_name,
        table_type,
        storage_name,
        flush_policy,
        scope_column,
        order_column,
    )
}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
#[allow(clippy::too_many_arguments)]
fn migrate_table_pg_impl(
    table_oid: pgrx::pg_sys::Oid,
    table_type: &str,
    storage_name: &str,
    flush_policy: Option<&str>,
    scope_column: Option<&str>,
    order_column: Option<&str>,
) -> pgrx::composite_type!('static, "koldstore.managed_table_info") {
    let table_oid_u32 = table_oid.to_u32();
    let relation = qualified_relation_name(table_oid_u32)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    let storage_id = storage_id_by_name(storage_name)
        .unwrap_or_else(|| pgrx::error!("storage `{storage_name}` is not registered"));
    let catalog = migration_catalog(table_oid_u32)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    let registry_catalog = catalog.clone();
    let mut options = serde_json::json!({});
    if let Some(order_column) = order_column
        .map(str::trim)
        .filter(|column| !column.is_empty())
    {
        options["order_column"] = serde_json::Value::String(order_column.to_string());
    }
    let request = MigrateTableRequest {
        table_name: relation,
        table_type: table_type.to_string(),
        storage_name: storage_name.to_string(),
        flush_policy: flush_policy.map(ToString::to_string),
        scope_column: scope_column.map(ToString::to_string),
        options,
    };
    let empty_plan = crate::migrate::plan_empty_table_migration(
        &request,
        crate::migrate::MigrationTableContext {
            table_oid: table_oid_u32,
            storage_id,
        },
    )
    .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));

    let has_existing_rows = table_has_rows(&empty_plan.table)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    if !has_existing_rows {
        let system_columns = crate::migrate::columns::plan_system_column_adds(
            &empty_plan.table,
            table_type == "user" && scope_column.is_none(),
        )
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
        pgrx::Spi::run(&system_columns.statement.sql)
            .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
        register_schema_version(SchemaRegistrationInput {
            table_oid: table_oid_u32,
            table_type,
            storage_id,
            scope_column: empty_plan.effective_scope_column.as_deref(),
            flush_policy,
            primary_key: &registry_catalog.primary_key.columns,
            columns: &registry_catalog.columns,
            indexed_columns: &registry_catalog.indexed_columns,
            active: true,
            migration_status: "active",
        })
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
        return managed_table_info_tuple(
            table_oid_u32,
            table_type,
            storage_id,
            empty_plan.effective_scope_column.as_deref(),
        );
    }

    let plan = crate::migrate::plan_existing_table_migration(
        &request,
        crate::migrate::MigrationTableContext {
            table_oid: table_oid_u32,
            storage_id,
        },
        catalog,
        Uuid::new_v4(),
    )
    .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));

    for statement in &plan.system_column_prepare.statements {
        pgrx::Spi::run(&statement.sql)
            .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    }
    register_schema_version(SchemaRegistrationInput {
        table_oid: table_oid_u32,
        table_type,
        storage_id,
        scope_column: plan.effective_scope_column.as_deref(),
        flush_policy,
        primary_key: &registry_catalog.primary_key.columns,
        columns: &registry_catalog.columns,
        indexed_columns: &registry_catalog.indexed_columns,
        active: false,
        migration_status: "backfill_pending",
    })
    .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    enqueue_backfill_job(&plan.backfill_job)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));

    managed_table_info_tuple(
        table_oid_u32,
        table_type,
        storage_id,
        plan.effective_scope_column.as_deref(),
    )
}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
fn table_has_rows(table: &crate::migrate::QualifiedTableName) -> Result<bool, pgrx::spi::Error> {
    pgrx::Spi::get_one::<bool>(&format!(
        "SELECT EXISTS (SELECT 1 FROM ONLY {} LIMIT 1)",
        table.quoted()
    ))
    .map(|value| value.unwrap_or(false))
}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
fn managed_table_info_tuple(
    table_oid_u32: u32,
    table_type: &str,
    storage_id: Uuid,
    scope_column: Option<&str>,
) -> pgrx::composite_type!('static, "koldstore.managed_table_info") {
    let mut tuple =
        pgrx::heap_tuple::PgHeapTuple::new_composite_type("koldstore.managed_table_info")
            .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    tuple
        .set_by_name("table_oid", pgrx::pg_sys::Oid::from(table_oid_u32))
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    tuple
        .set_by_name("table_type", table_type)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    tuple
        .set_by_name("storage_id", pgrx::Uuid::from_bytes(*storage_id.as_bytes()))
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    tuple
        .set_by_name("schema_version", 1_i32)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    tuple
        .set_by_name("scope_column", scope_column)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    tuple
}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
fn qualified_relation_name(table_oid: u32) -> Result<String, pgrx::spi::Error> {
    pgrx::Spi::get_one_with_args::<String>(
        "SELECT format('%I.%I', n.nspname, c.relname) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = $1::oid",
        &[pgrx::datum::DatumWithOid::from(pgrx::pg_sys::Oid::from(
            table_oid,
        ))],
    )
    .map(|value| value.unwrap_or_else(|| pgrx::error!("table oid {table_oid} does not exist")))
}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
fn storage_id_by_name(name: &str) -> Option<Uuid> {
    let id = pgrx::Spi::get_one_with_args::<pgrx::Uuid>(
        "SELECT id FROM koldstore.storage WHERE name = $1",
        &[pgrx::datum::DatumWithOid::from(name)],
    )
    .unwrap_or_else(|error| pgrx::error!("storage lookup failed: {error}"))?;
    Some(Uuid::from_bytes(*id.as_bytes()))
}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
fn migration_catalog(table_oid: u32) -> Result<crate::migrate::ExistingTableCatalog, String> {
    use pgrx::datum::DatumWithOid;

    let oid = pgrx::pg_sys::Oid::from(table_oid);
    let primary_key_json = pgrx::Spi::get_one_with_args::<String>(
        r#"
SELECT COALESCE(jsonb_agg(a.attname ORDER BY key_position.ordinality)::text, '[]')
FROM pg_index i
JOIN unnest(i.indkey) WITH ORDINALITY AS key_position(attnum, ordinality) ON true
JOIN pg_attribute a
  ON a.attrelid = i.indrelid
 AND a.attnum = key_position.attnum
WHERE i.indrelid = $1::oid
  AND i.indisprimary
"#,
        &[DatumWithOid::from(oid)],
    )
    .map_err(|error| error.to_string())?
    .unwrap_or_else(|| "[]".to_string());
    let columns_json = pgrx::Spi::get_one_with_args::<String>(
        r#"
WITH pk AS (
    SELECT a.attname
    FROM pg_index i
    JOIN unnest(i.indkey) WITH ORDINALITY AS key_position(attnum, ordinality) ON true
    JOIN pg_attribute a
      ON a.attrelid = i.indrelid
     AND a.attnum = key_position.attnum
    WHERE i.indrelid = $1::oid
      AND i.indisprimary
)
SELECT COALESCE(
    jsonb_agg(
        jsonb_build_object(
            'name', a.attname,
            'type_name', format_type(a.atttypid, a.atttypmod),
            'is_primary_key', pk.attname IS NOT NULL,
            'identity', a.attidentity <> '',
            'default_expr', pg_get_expr(d.adbin, d.adrelid)
        )
        ORDER BY a.attnum
    )::text,
    '[]'
)
FROM pg_attribute a
LEFT JOIN pg_attrdef d
  ON d.adrelid = a.attrelid
 AND d.adnum = a.attnum
LEFT JOIN pk
  ON pk.attname = a.attname
WHERE a.attrelid = $1::oid
  AND a.attnum > 0
  AND NOT a.attisdropped
"#,
        &[DatumWithOid::from(oid)],
    )
    .map_err(|error| error.to_string())?
    .unwrap_or_else(|| "[]".to_string());
    let indexed_columns_json = pgrx::Spi::get_one_with_args::<String>(
        r#"
WITH pk AS (
    SELECT a.attname
    FROM pg_index i
    JOIN unnest(i.indkey) WITH ORDINALITY AS key_position(attnum, ordinality) ON true
    JOIN pg_attribute a
      ON a.attrelid = i.indrelid
     AND a.attnum = key_position.attnum
    WHERE i.indrelid = $1::oid
      AND i.indisprimary
),
candidate AS (
    SELECT a.attname, i.indexrelid::bigint AS source_oid, key_position.ordinality
    FROM pg_index i
    JOIN unnest(i.indkey) WITH ORDINALITY AS key_position(attnum, ordinality) ON true
    JOIN pg_attribute a
      ON a.attrelid = i.indrelid
     AND a.attnum = key_position.attnum
    WHERE i.indrelid = $1::oid
      AND NOT i.indisprimary
      AND i.indexprs IS NULL
    UNION ALL
    SELECT a.attname, c.oid::bigint AS source_oid, key_position.ordinality
    FROM pg_constraint c
    JOIN unnest(c.conkey) WITH ORDINALITY AS key_position(attnum, ordinality) ON true
    JOIN pg_attribute a
      ON a.attrelid = c.conrelid
     AND a.attnum = key_position.attnum
    WHERE c.conrelid = $1::oid
      AND c.contype = 'f'
),
ranked AS (
    SELECT DISTINCT ON (candidate.attname)
        candidate.attname,
        candidate.source_oid,
        candidate.ordinality
    FROM candidate
    LEFT JOIN pk ON pk.attname = candidate.attname
    WHERE pk.attname IS NULL
    ORDER BY candidate.attname, candidate.source_oid, candidate.ordinality
)
SELECT COALESCE(jsonb_agg(attname ORDER BY source_oid, ordinality, attname)::text, '[]')
FROM ranked
"#,
        &[DatumWithOid::from(oid)],
    )
    .map_err(|error| error.to_string())?
    .unwrap_or_else(|| "[]".to_string());

    let primary_key = serde_json::from_str::<Vec<String>>(&primary_key_json)
        .map_err(|error| format!("primary key catalog decode failed: {error}"))?;
    let columns = serde_json::from_str::<Vec<crate::migrate::order::CatalogColumn>>(&columns_json)
        .map_err(|error| format!("column catalog decode failed: {error}"))?;
    let indexed_columns = serde_json::from_str::<Vec<String>>(&indexed_columns_json)
        .map_err(|error| format!("indexed column catalog decode failed: {error}"))?;

    Ok(crate::migrate::ExistingTableCatalog {
        primary_key: crate::migrate::order::CatalogPrimaryKey {
            columns: primary_key,
        },
        columns,
        indexed_columns,
    })
}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
struct SchemaRegistrationInput<'a> {
    table_oid: u32,
    table_type: &'a str,
    storage_id: Uuid,
    scope_column: Option<&'a str>,
    flush_policy: Option<&'a str>,
    primary_key: &'a [String],
    columns: &'a [crate::migrate::order::CatalogColumn],
    indexed_columns: &'a [String],
    active: bool,
    migration_status: &'a str,
}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
fn register_schema_version(input: SchemaRegistrationInput<'_>) -> Result<(), pgrx::spi::Error> {
    use pgrx::datum::DatumWithOid;

    let mut schema_columns = input
        .columns
        .iter()
        .map(|column| {
            serde_json::json!({
                "name": column.name,
                "type_name": column.type_name,
                "nullable": true,
                "system": false,
            })
        })
        .collect::<Vec<_>>();
    schema_columns.extend([
        serde_json::json!({"name": "_seq", "type_name": "bigint", "nullable": false, "system": true}),
        serde_json::json!({"name": "_commit_seq", "type_name": "bigint", "nullable": false, "system": true}),
        serde_json::json!({"name": "_deleted", "type_name": "boolean", "nullable": false, "system": true}),
    ]);
    if input.table_type == "user" && input.scope_column == Some(crate::migrate::SYSTEM_SCOPE_COLUMN)
    {
        schema_columns.push(serde_json::json!({
            "name": crate::migrate::SYSTEM_SCOPE_COLUMN,
            "type_name": "text",
            "nullable": true,
            "system": true,
        }));
    }

    let mut options = serde_json::json!({ "migration_status": input.migration_status });
    if let Some(policy) = input
        .flush_policy
        .map(str::trim)
        .filter(|policy| !policy.is_empty())
    {
        options["flush_policy"] = serde_json::Value::String(policy.to_string());
    }
    let cold_metadata =
        crate::migrate::register::cold_metadata_config(input.primary_key, input.indexed_columns);
    if !cold_metadata.is_empty() {
        options["cold_metadata"] = serde_json::to_value(cold_metadata)
            .unwrap_or_else(|_| serde_json::Value::Object(serde_json::Map::new()));
    }

    pgrx::Spi::run_with_args(
        "INSERT INTO koldstore.schemas (
            id, table_oid, version, active, table_type, columns, primary_key,
            scope_column, indexed_columns, type_matrix, options, storage_id
         )
         VALUES (
            gen_random_uuid(), $1, 1, $2, $3, $4::jsonb, $5::jsonb,
            $6, $7::jsonb, '{}'::jsonb, $8::jsonb, $9
         )
         ON CONFLICT (table_oid, version) DO UPDATE
         SET active = EXCLUDED.active,
             table_type = EXCLUDED.table_type,
             columns = EXCLUDED.columns,
             primary_key = EXCLUDED.primary_key,
             scope_column = EXCLUDED.scope_column,
            indexed_columns = EXCLUDED.indexed_columns,
             options = EXCLUDED.options,
             storage_id = EXCLUDED.storage_id,
             updated_at = now()",
        &[
            DatumWithOid::from(pgrx::pg_sys::Oid::from(input.table_oid)),
            DatumWithOid::from(input.active),
            DatumWithOid::from(input.table_type),
            DatumWithOid::from(pgrx::JsonB(serde_json::Value::Array(schema_columns))),
            DatumWithOid::from(pgrx::JsonB(serde_json::json!(input.primary_key))),
            DatumWithOid::from(input.scope_column),
            DatumWithOid::from(pgrx::JsonB(serde_json::json!(input.indexed_columns))),
            DatumWithOid::from(pgrx::JsonB(options)),
            DatumWithOid::from(pgrx::Uuid::from_bytes(*input.storage_id.as_bytes())),
        ],
    )
}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
fn enqueue_backfill_job(
    plan: &crate::migrate::jobs::MigrationJobEnqueuePlan,
) -> Result<(), pgrx::spi::Error> {
    use pgrx::datum::DatumWithOid;

    pgrx::Spi::run_with_args(
        &plan.statement.sql,
        &[
            DatumWithOid::from(pgrx::Uuid::from_bytes(*plan.job_id.as_bytes())),
            DatumWithOid::from(pgrx::pg_sys::Oid::from(plan.table_oid)),
            DatumWithOid::from(pgrx::JsonB(plan.payload.clone())),
        ],
    )
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
