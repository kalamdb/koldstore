//! Migration validation.

use koldstore_common::PrimaryKeyShape;
use koldstore_schema::PgType;
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
    /// The table already has a managed schema registration.
    #[error("table is already managed")]
    AlreadyManaged,
    /// A numeric manage-table policy value is not positive.
    #[error("{field} must be greater than zero (got {value})")]
    InvalidPolicyValue {
        /// Operator-facing policy field.
        field: &'static str,
        /// Invalid value.
        value: i64,
    },
    /// The requested Parquet compression codec is unsupported.
    #[error("unsupported compression codec `{0}`")]
    UnsupportedCompression(String),
    /// The requested mirror capture consistency mode is unsupported.
    #[error("unsupported mirror capture mode `{0}`; expected `strict` or `async`")]
    UnsupportedMirrorCaptureMode(String),
    /// The requested table ownership model is unsupported.
    #[error("unsupported table type `{0}`")]
    UnsupportedTableType(String),
    /// The configured file row limit is below the runtime floor.
    #[error(
        "max_rows_per_file must be at least {minimum} (got {value}); \
omit max_rows_per_file to use the default ({minimum}), or temporarily \
SET koldstore.min_max_rows_per_file lower for demos"
    )]
    MaxRowsPerFileBelowFloor {
        /// Invalid row limit.
        value: u64,
        /// Runtime-configured minimum.
        minimum: u64,
    },
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
    /// User-scoped migration names a column absent from the table.
    #[error("scope column `{0}` does not exist")]
    ScopeColumnNotFound(String),
    /// Explicit migration ordering names a column absent from the table.
    #[error("migration order column `{0}` does not exist")]
    MissingOrderColumn(String),
    /// Storage registration lookup failed.
    #[error("storage registration must exist before migration")]
    MissingStorage,
    /// Non-primary-key unique constraints are unsafe once flush moves rows cold.
    #[error(
        "flush-enabled managed tables cannot preserve global uniqueness; koldstore enforces unique constraints on hot rows only: {constraints}"
    )]
    UnsupportedUniqueConstraints {
        /// Human-readable unique constraint listing.
        constraints: String,
    },
    /// Foreign keys need explicit hot-only acceptance when flush is enabled.
    #[error(
        "flush-enabled managed tables cannot preserve global referential integrity; koldstore enforces foreign keys on hot rows only. Foreign keys: {foreign_keys}. Drop or relocate them, or set options.allow_fk_hot_only = true"
    )]
    ForeignKeyRequiresHotOnlyOverride {
        /// Human-readable foreign-key listing.
        foreign_keys: String,
    },
}

/// PostgreSQL column shape needed for migration validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDefinition {
    /// Column name.
    pub name: String,
    /// PostgreSQL type parsed from catalog metadata when it is recognized.
    pub pg_type: Option<PgType>,
    /// Original catalog type spelling preserved for diagnostics.
    pub catalog_type_name: String,
    /// Whether the column is nullable.
    pub nullable: bool,
    /// Whether the column is generated.
    pub generated: bool,
}

impl ColumnDefinition {
    /// Creates a plain column definition from a PostgreSQL catalog type name.
    #[must_use]
    pub fn new(name: impl Into<String>, type_name: impl Into<String>, nullable: bool) -> Self {
        let catalog_type_name = type_name.into();
        let pg_type = PgType::from_postgres_name(&catalog_type_name).ok();
        Self {
            name: name.into(),
            pg_type,
            catalog_type_name,
            nullable,
            generated: false,
        }
    }

    /// Creates a column definition from a supported PostgreSQL type.
    #[must_use]
    pub fn typed(
        name: impl Into<String>,
        pg_type: PgType,
        catalog_type_name: impl Into<String>,
        nullable: bool,
        generated: bool,
    ) -> Self {
        Self {
            name: name.into(),
            pg_type: Some(pg_type),
            catalog_type_name: catalog_type_name.into(),
            nullable,
            generated,
        }
    }

    /// Returns the original catalog type spelling.
    #[must_use]
    pub fn catalog_type_name(&self) -> &str {
        &self.catalog_type_name
    }

    /// Marks the column as generated.
    #[must_use]
    pub fn generated(mut self) -> Self {
        self.generated = true;
        self
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

/// Non-primary-key unique constraint shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniqueConstraintShape {
    /// Constraint or unique-index name.
    pub name: String,
    /// Constrained columns in key order.
    pub columns: Vec<String>,
}

impl UniqueConstraintShape {
    fn display_label(&self) -> String {
        format!("{} (columns: {})", self.name, self.columns.join(", "))
    }
}

/// Foreign-key shape relevant to hot-only migration policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyShape {
    /// Constraint name.
    pub name: String,
    /// FK direction.
    pub direction: FkDirection,
    /// Local FK columns in key order.
    pub columns: Vec<String>,
    /// Referenced table for outbound FKs or referencing table for inbound FKs.
    pub related_relation: Option<String>,
}

impl ForeignKeyShape {
    fn display_label(&self) -> String {
        let columns = self.columns.join(", ");
        match (self.direction, self.related_relation.as_deref()) {
            (FkDirection::Outbound, Some(related)) => {
                format!("{} outbound (columns: {}) -> {related}", self.name, columns)
            }
            (FkDirection::Inbound, Some(related)) => {
                format!("{} inbound from {related} (columns: {columns})", self.name)
            }
            (FkDirection::Outbound, None) => {
                format!("{} outbound (columns: {columns})", self.name)
            }
            (FkDirection::Inbound, None) => {
                format!("{} inbound (columns: {columns})", self.name)
            }
        }
    }
}

/// Unique and foreign-key metadata probed before `manage_table`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ManageTableConstraintsCatalog {
    /// Non-primary-key unique constraints on the managed table.
    pub unique_constraints: Vec<UniqueConstraintShape>,
    /// Foreign keys involving the managed table.
    pub foreign_keys: Vec<ForeignKeyShape>,
}

impl ManageTableConstraintsCatalog {
    /// Validates hot/cold constraint policy for a managed table.
    ///
    /// # Errors
    ///
    /// Returns an error when flush is enabled and unique or foreign-key constraints
    /// cannot be preserved globally across hot and cold storage.
    pub fn validate_hot_cold_policy(
        &self,
        flush_enabled: bool,
        allow_fk_hot_only: bool,
    ) -> ConstraintResult<()> {
        if !flush_enabled {
            return Ok(());
        }
        self.validate_unique_constraints()?;
        self.validate_foreign_keys(allow_fk_hot_only)?;
        Ok(())
    }

    fn validate_unique_constraints(&self) -> ConstraintResult<()> {
        if self.unique_constraints.is_empty() {
            return Ok(());
        }
        let constraints = self
            .unique_constraints
            .iter()
            .map(UniqueConstraintShape::display_label)
            .collect::<Vec<_>>()
            .join("; ");
        Err(MigrationConstraintError::UnsupportedUniqueConstraints { constraints })
    }

    fn validate_foreign_keys(&self, allow_fk_hot_only: bool) -> ConstraintResult<()> {
        if self.foreign_keys.is_empty() || allow_fk_hot_only {
            return Ok(());
        }
        let foreign_keys = self
            .foreign_keys
            .iter()
            .map(ForeignKeyShape::display_label)
            .collect::<Vec<_>>()
            .join("; ");
        Err(MigrationConstraintError::ForeignKeyRequiresHotOnlyOverride { foreign_keys })
    }
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
    /// Whether automatic flush is enabled for this table.
    pub flush_enabled: bool,
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
    /// Non-primary-key unique constraints on this table.
    pub unique_constraints: Vec<UniqueConstraintShape>,
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
            flush_enabled: false,
            allow_fk_hot_only: false,
            columns: vec![ColumnDefinition::new("id", "bigint", false)],
            primary_key: vec!["id".to_string()],
            expression_primary_key: false,
            indexes: Vec::new(),
            check_constraints: Vec::new(),
            not_null_columns: vec!["id".to_string()],
            unique_constraints: Vec::new(),
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
        self.validate_primary_key()?;
        self.validate_columns()?;
        self.validate_indexes()?;
        self.validate_scope_and_storage()?;
        ManageTableConstraintsCatalog {
            unique_constraints: self.unique_constraints.clone(),
            foreign_keys: self.foreign_keys.clone(),
        }
        .validate_hot_cold_policy(self.flush_enabled, self.allow_fk_hot_only)?;
        let fk_policy = self.validate_fk_policy()?;

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

    fn validate_primary_key(&self) -> ConstraintResult<()> {
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
        Ok(())
    }

    fn validate_columns(&self) -> ConstraintResult<()> {
        for column in &self.columns {
            if column.generated {
                return Err(MigrationConstraintError::GeneratedColumn(
                    column.name.clone(),
                ));
            }
            let Some(pg_type) = column.pg_type else {
                return Err(MigrationConstraintError::UnsupportedColumnType {
                    column: column.name.clone(),
                    type_name: column.catalog_type_name.clone(),
                });
            };
            if !pg_type.is_mvp_supported() {
                return Err(MigrationConstraintError::UnsupportedColumnType {
                    column: column.name.clone(),
                    type_name: column.catalog_type_name.clone(),
                });
            }
        }
        Ok(())
    }

    fn validate_indexes(&self) -> ConstraintResult<()> {
        for index in &self.indexes {
            if index.expression {
                return Err(MigrationConstraintError::ExpressionIndex(
                    index.name.clone(),
                ));
            }
        }
        Ok(())
    }

    fn validate_scope_and_storage(&self) -> ConstraintResult<()> {
        if !matches!(self.table_type.as_str(), "shared" | "user") {
            return Err(MigrationConstraintError::UnsupportedTableType(
                self.table_type.clone(),
            ));
        }
        if self.table_type == "user" {
            let scope_column = self
                .scope_column
                .as_deref()
                .map(str::trim)
                .filter(|scope| !scope.is_empty())
                .ok_or(MigrationConstraintError::MissingScopeColumn)?;
            if !self
                .columns
                .iter()
                .any(|column| column.name == scope_column)
            {
                return Err(MigrationConstraintError::ScopeColumnNotFound(
                    scope_column.to_string(),
                ));
            }
        }
        if !self.storage_exists {
            return Err(MigrationConstraintError::MissingStorage);
        }
        Ok(())
    }

    fn validate_fk_policy(&self) -> ConstraintResult<FkPolicy> {
        Ok(if self.foreign_keys.is_empty() {
            FkPolicy::None
        } else if self.flush_enabled {
            FkPolicy::AllowHotOnly
        } else {
            FkPolicy::Native
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
    koldstore_schema::PgType::from_postgres_name(type_name)
        .map(|pg_type| pg_type.is_mvp_supported())
        .unwrap_or(false)
}
