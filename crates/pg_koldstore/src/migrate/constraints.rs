//! Migration validation.

use koldstore_core::PrimaryKeyShape;
use koldstore_parquet::PgType;
use thiserror::Error;

/// Returns true when the table has a primary-key shape pg-koldstore can manage.
#[must_use]
pub fn primary_key_shape_supported(columns: &[&str]) -> bool {
    !columns.is_empty() && columns.iter().all(|column| !column.trim().is_empty())
}

/// Returns true when an exact clean-schema primary-key shape can be mirrored.
#[must_use]
pub fn exact_primary_key_shape_supported(shape: &PrimaryKeyShape) -> bool {
    !shape.columns().is_empty()
        && shape
            .columns()
            .iter()
            .all(|column| column.not_null() && !column.column().as_str().trim().is_empty())
}

/// Migration validation result.
pub type ConstraintResult<T> = Result<T, MigrationConstraintError>;

/// Migration validation error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MigrationConstraintError {
    /// Primary key is missing or malformed.
    #[error("managed tables require a primary key")]
    MissingPrimaryKey,
    /// A primary-key column is not present in the table definition.
    #[error("primary key column not present in table: {0}")]
    MissingPrimaryKeyColumn(String),
    /// Expression primary keys are unsupported.
    #[error("expression primary keys are not supported")]
    ExpressionPrimaryKey,
    /// Column type is not supported by the MVP type matrix.
    #[error("unsupported column type `{type_name}` for column `{column}`")]
    UnsupportedColumnType {
        /// Column name.
        column: String,
        /// PostgreSQL type name.
        type_name: String,
    },
    /// Generated columns are unsupported.
    #[error("generated column `{0}` is not supported")]
    GeneratedColumn(String),
    /// Expression indexes are unsupported for migration metadata.
    #[error("expression index `{0}` is not supported")]
    ExpressionIndex(String),
    /// User-scoped migration is missing a scope column.
    #[error("user-scoped migration requires scope_column")]
    MissingScopeColumn,
    /// Storage registration lookup failed.
    #[error("storage registration must exist before migration")]
    MissingStorage,
    /// Flush policy is blank.
    #[error("flush_policy cannot be blank")]
    BlankFlushPolicy,
    /// Foreign keys need explicit hot-only acceptance when flush is enabled.
    #[error("foreign keys require options.allow_fk_hot_only = true when flush is enabled")]
    ForeignKeyRequiresHotOnlyOverride,
}

/// PostgreSQL column shape needed for migration validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDefinition {
    /// Column name.
    pub name: String,
    /// PostgreSQL type name.
    pub type_name: String,
    /// Whether the column is nullable.
    pub nullable: bool,
    /// Whether the column is generated.
    pub generated: bool,
}

impl ColumnDefinition {
    /// Creates a plain column definition.
    #[must_use]
    pub fn new(name: impl Into<String>, type_name: impl Into<String>, nullable: bool) -> Self {
        Self {
            name: name.into(),
            type_name: type_name.into(),
            nullable,
            generated: false,
        }
    }
}

/// Index shape needed for migration validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDefinition {
    /// Index name.
    pub name: String,
    /// Indexed columns.
    pub columns: Vec<String>,
    /// Whether the index expression is not a simple column list.
    pub expression: bool,
}

impl IndexDefinition {
    /// Creates a btree-like column index definition.
    #[must_use]
    pub fn btree(name: impl Into<String>, columns: Vec<String>) -> Self {
        Self {
            name: name.into(),
            columns,
            expression: false,
        }
    }

    /// Creates an expression index definition.
    #[must_use]
    pub fn expression(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            columns: Vec::new(),
            expression: true,
        }
    }
}

/// Direction of a foreign key relative to the migrating table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FkDirection {
    /// Another table references the migrating table.
    Inbound,
    /// The migrating table references another table.
    Outbound,
}

/// Foreign-key shape relevant to hot-only migration policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyShape {
    /// Constraint name.
    pub name: String,
    /// FK direction.
    pub direction: FkDirection,
}

/// Effective FK policy recorded by validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FkPolicy {
    /// No FKs are present.
    None,
    /// Native FK behavior remains acceptable because flush is disabled.
    Native,
    /// Operator explicitly accepted hot-only FK semantics.
    AllowHotOnly,
}

/// Migration validation input captured from PostgreSQL catalogs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationValidationInput {
    /// Managed table type.
    pub table_type: String,
    /// Optional user scope column.
    pub scope_column: Option<String>,
    /// Whether the storage registration exists.
    pub storage_exists: bool,
    /// Optional flush policy.
    pub flush_policy: Option<String>,
    /// Operator accepted hot-only FK semantics.
    pub allow_fk_hot_only: bool,
    /// Table columns.
    pub columns: Vec<ColumnDefinition>,
    /// Preserved application primary-key columns.
    pub primary_key: Vec<String>,
    /// Whether the primary key uses an expression.
    pub expression_primary_key: bool,
    /// Secondary index definitions.
    pub indexes: Vec<IndexDefinition>,
    /// Check constraints to preserve.
    pub check_constraints: Vec<String>,
    /// Not-null columns to preserve.
    pub not_null_columns: Vec<String>,
    /// Foreign keys involving this table.
    pub foreign_keys: Vec<ForeignKeyShape>,
}

impl MigrationValidationInput {
    /// Creates a minimal valid shared-table migration input.
    #[must_use]
    pub fn minimal_shared() -> Self {
        Self {
            table_type: "shared".to_string(),
            scope_column: None,
            storage_exists: true,
            flush_policy: None,
            allow_fk_hot_only: false,
            columns: vec![ColumnDefinition::new("id", "bigint", false)],
            primary_key: vec!["id".to_string()],
            expression_primary_key: false,
            indexes: Vec::new(),
            check_constraints: Vec::new(),
            not_null_columns: vec!["id".to_string()],
            foreign_keys: Vec::new(),
        }
    }

    /// Validates a migration input and returns the metadata pg-koldstore should preserve.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the table shape is unsafe or outside the MVP
    /// migration contract.
    pub fn validate(&self) -> ConstraintResult<MigrationValidation> {
        if self.primary_key.is_empty()
            || self
                .primary_key
                .iter()
                .any(|column| column.trim().is_empty())
        {
            return Err(MigrationConstraintError::MissingPrimaryKey);
        }
        if self.expression_primary_key {
            return Err(MigrationConstraintError::ExpressionPrimaryKey);
        }
        for pk_column in &self.primary_key {
            if !self.columns.iter().any(|column| column.name == *pk_column) {
                return Err(MigrationConstraintError::MissingPrimaryKeyColumn(
                    pk_column.clone(),
                ));
            }
        }

        for column in &self.columns {
            if column.generated {
                return Err(MigrationConstraintError::GeneratedColumn(
                    column.name.clone(),
                ));
            }
            if !type_supported(&column.type_name) {
                return Err(MigrationConstraintError::UnsupportedColumnType {
                    column: column.name.clone(),
                    type_name: column.type_name.clone(),
                });
            }
        }

        for index in &self.indexes {
            if index.expression {
                return Err(MigrationConstraintError::ExpressionIndex(
                    index.name.clone(),
                ));
            }
        }

        if self.table_type == "user"
            && self
                .scope_column
                .as_deref()
                .map(str::trim)
                .filter(|scope| !scope.is_empty())
                .is_none()
        {
            return Err(MigrationConstraintError::MissingScopeColumn);
        }
        if !self.storage_exists {
            return Err(MigrationConstraintError::MissingStorage);
        }
        if self
            .flush_policy
            .as_deref()
            .is_some_and(|policy| policy.trim().is_empty())
        {
            return Err(MigrationConstraintError::BlankFlushPolicy);
        }

        let flush_enabled = self.flush_policy.is_some();
        if !fk_policy_allowed(
            !self.foreign_keys.is_empty(),
            flush_enabled,
            self.allow_fk_hot_only,
        ) {
            return Err(MigrationConstraintError::ForeignKeyRequiresHotOnlyOverride);
        }

        let fk_policy = if self.foreign_keys.is_empty() {
            FkPolicy::None
        } else if flush_enabled {
            FkPolicy::AllowHotOnly
        } else {
            FkPolicy::Native
        };
        let indexed_columns = self
            .indexes
            .iter()
            .flat_map(|index| index.columns.iter().cloned())
            .filter(|column| !self.primary_key.iter().any(|pk| pk == column))
            .collect::<Vec<_>>();
        let preserved_indexes = self
            .indexes
            .iter()
            .map(|index| index.name.clone())
            .collect::<Vec<_>>();

        Ok(MigrationValidation {
            primary_key: self.primary_key.clone(),
            allow_fk_hot_only: self.allow_fk_hot_only,
            indexed_columns,
            preserved_indexes,
            preserved_check_constraints: self.check_constraints.clone(),
            preserved_not_null_columns: self.not_null_columns.clone(),
            fk_policy,
        })
    }
}

/// Migration validation outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationValidation {
    /// Supported primary-key columns.
    pub primary_key: Vec<String>,
    /// Whether FK semantics are accepted as hot-only.
    pub allow_fk_hot_only: bool,
    /// Indexed columns considered for cold stats.
    pub indexed_columns: Vec<String>,
    /// Existing hot indexes preserved by migration.
    pub preserved_indexes: Vec<String>,
    /// Existing CHECK constraints preserved by migration.
    pub preserved_check_constraints: Vec<String>,
    /// Existing NOT NULL columns preserved by migration.
    pub preserved_not_null_columns: Vec<String>,
    /// Effective FK policy.
    pub fk_policy: FkPolicy,
}

/// Returns whether FK configuration can be migrated.
#[must_use]
pub const fn fk_policy_allowed(has_fk: bool, flush_enabled: bool, allow_fk_hot_only: bool) -> bool {
    !has_fk || !flush_enabled || allow_fk_hot_only
}

/// Returns whether a column type is supported by the MVP type matrix.
#[must_use]
pub fn type_supported(type_name: &str) -> bool {
    PgType::from_postgres_name(type_name).is_ok()
}
