//! Safe SPI helper boundary.

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::Mutex,
};

use thiserror::Error;

/// SQLSTATE used for pg-koldstore errors.
pub const KOLDSTORE_SQLSTATE: &str = "XXKLD";

/// SPI helper result.
pub type SpiResult<T> = Result<T, SpiError>;

/// Mapped SPI error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("SPI {operation} failed: {message}")]
pub struct SpiError {
    /// SPI operation name.
    pub operation: String,
    /// Error message.
    pub message: String,
}

/// Maps a SPI failure into a typed error.
#[must_use]
pub fn map_spi_error(operation: &str, message: &str) -> SpiError {
    SpiError {
        operation: operation.to_string(),
        message: message.to_string(),
    }
}

/// SPI access mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SpiAccess {
    /// Read-only catalog access.
    ReadOnly,
    /// Read/write catalog access.
    ReadWrite,
}

/// Pg-free SQL parameter type metadata used by prepared-plan signatures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SqlParamType {
    /// PostgreSQL `bigint` / `int8`.
    BigInt,
    /// PostgreSQL `integer` / `int4`.
    Integer,
    /// PostgreSQL `text`.
    Text,
    /// PostgreSQL `jsonb`.
    Jsonb,
    /// PostgreSQL `oid`.
    Oid,
    /// PostgreSQL `uuid`.
    Uuid,
    /// PostgreSQL `boolean`.
    Boolean,
}

impl From<koldstore_mirror::SqlParamType> for SqlParamType {
    fn from(value: koldstore_mirror::SqlParamType) -> Self {
        match value {
            koldstore_mirror::SqlParamType::BigInt => Self::BigInt,
            koldstore_mirror::SqlParamType::Integer => Self::Integer,
            koldstore_mirror::SqlParamType::Text => Self::Text,
            koldstore_mirror::SqlParamType::Jsonb => Self::Jsonb,
            koldstore_mirror::SqlParamType::Oid => Self::Oid,
            koldstore_mirror::SqlParamType::Uuid => Self::Uuid,
            koldstore_mirror::SqlParamType::Boolean => Self::Boolean,
        }
    }
}

/// Maps pg-free parameter metadata to pgrx PostgreSQL OIDs.
#[cfg(feature = "pg")]
#[must_use]
pub fn pg_param_oids(param_types: &[SqlParamType]) -> Vec<pgrx::pg_sys::PgOid> {
    param_types
        .iter()
        .copied()
        .map(SqlParamType::pg_oid)
        .collect()
}

#[cfg(feature = "pg")]
impl SqlParamType {
    fn pg_oid(self) -> pgrx::pg_sys::PgOid {
        let oid = match self {
            Self::BigInt => pgrx::pg_sys::INT8OID,
            Self::Integer => pgrx::pg_sys::INT4OID,
            Self::Text => pgrx::pg_sys::TEXTOID,
            Self::Jsonb => pgrx::pg_sys::JSONBOID,
            Self::Oid => pgrx::pg_sys::OIDOID,
            Self::Uuid => pgrx::pg_sys::UUIDOID,
            Self::Boolean => pgrx::pg_sys::BOOLOID,
        };
        pgrx::pg_sys::PgOid::from(oid)
    }
}

/// Cache key for a backend-local prepared SPI plan.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PreparedPlanKey {
    /// Required access mode.
    pub access: SpiAccess,
    /// Hash of the canonical SQL text.
    pub sql_hash: u64,
    /// Bind parameter signature.
    pub param_types: Vec<SqlParamType>,
}

/// Builds the prepared-plan key for a statement.
#[must_use]
pub fn prepared_plan_key(statement: &SpiStatement) -> PreparedPlanKey {
    let mut hasher = DefaultHasher::new();
    statement.sql.hash(&mut hasher);
    PreparedPlanKey {
        access: statement.access,
        sql_hash: hasher.finish(),
        param_types: statement.param_types.clone(),
    }
}

/// Validated SPI statement metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpiStatement {
    /// Human-readable operation name for diagnostics.
    pub operation: String,
    /// SQL text.
    pub sql: String,
    /// Required SPI access mode.
    pub access: SpiAccess,
    /// Bind parameter types by one-based placeholder position.
    pub param_types: Vec<SqlParamType>,
}

impl SpiStatement {
    /// Creates a read-only catalog statement.
    ///
    /// # Errors
    ///
    /// Returns an error when operation or SQL text is blank.
    pub fn read(operation: &str, sql: &str) -> SpiResult<Self> {
        Self::read_with_params(operation, sql, [])
    }

    /// Creates a read-only catalog statement with parameter metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when operation or SQL text is blank.
    pub fn read_with_params(
        operation: &str,
        sql: &str,
        param_types: impl Into<Vec<SqlParamType>>,
    ) -> SpiResult<Self> {
        Self::new(operation, sql, SpiAccess::ReadOnly, param_types)
    }

    /// Creates a read/write catalog statement.
    ///
    /// # Errors
    ///
    /// Returns an error when operation or SQL text is blank.
    pub fn write(operation: &str, sql: &str) -> SpiResult<Self> {
        Self::write_with_params(operation, sql, [])
    }

    /// Creates a read/write catalog statement with parameter metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when operation or SQL text is blank.
    pub fn write_with_params(
        operation: &str,
        sql: &str,
        param_types: impl Into<Vec<SqlParamType>>,
    ) -> SpiResult<Self> {
        Self::new(operation, sql, SpiAccess::ReadWrite, param_types)
    }

    fn new(
        operation: &str,
        sql: &str,
        access: SpiAccess,
        param_types: impl Into<Vec<SqlParamType>>,
    ) -> SpiResult<Self> {
        let operation = operation.trim();
        let sql = sql.trim();
        if operation.is_empty() {
            return Err(map_spi_error(
                "validate statement",
                "operation cannot be empty",
            ));
        }
        if sql.is_empty() {
            return Err(map_spi_error(operation, "sql cannot be empty"));
        }

        Ok(Self {
            operation: operation.to_string(),
            sql: sql.to_string(),
            access,
            param_types: param_types.into(),
        })
    }
}

impl TryFrom<koldstore_mirror::MirrorStatement> for SpiStatement {
    type Error = SpiError;

    fn try_from(statement: koldstore_mirror::MirrorStatement) -> SpiResult<Self> {
        let access = match statement.access {
            koldstore_mirror::MirrorAccess::ReadOnly => SpiAccess::ReadOnly,
            koldstore_mirror::MirrorAccess::ReadWrite => SpiAccess::ReadWrite,
        };
        Self::new(
            statement.label,
            &statement.sql,
            access,
            statement
                .param_types
                .into_iter()
                .map(SqlParamType::from)
                .collect::<Vec<_>>(),
        )
    }
}

/// Converts a mirror storage statement into SPI metadata.
///
/// # Errors
///
/// Returns an error when statement metadata is blank or invalid.
pub fn mirror_to_spi(statement: koldstore_mirror::MirrorStatement) -> SpiResult<SpiStatement> {
    statement.try_into()
}

/// SPI execution result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpiRows {
    /// Number of affected rows when known.
    pub rows_affected: u64,
}

/// Testable SPI executor boundary.
pub trait SpiExecutor {
    /// Executes a validated statement.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying SPI call fails.
    fn execute(&self, statement: SpiStatement) -> SpiResult<SpiRows>;
}

/// Executes a read-only catalog statement.
///
/// # Errors
///
/// Returns an error if the statement is not read-only or execution fails.
pub fn execute_catalog_read(
    executor: &impl SpiExecutor,
    statement: SpiStatement,
) -> SpiResult<SpiRows> {
    if statement.access != SpiAccess::ReadOnly {
        return Err(map_spi_error(
            &statement.operation,
            "read helper requires read-only statement",
        ));
    }
    executor.execute(statement)
}

/// Executes a read/write catalog statement.
///
/// # Errors
///
/// Returns an error if the statement is not read/write or execution fails.
pub fn execute_catalog_write(
    executor: &impl SpiExecutor,
    statement: SpiStatement,
) -> SpiResult<SpiRows> {
    if statement.access != SpiAccess::ReadWrite {
        return Err(map_spi_error(
            &statement.operation,
            "write helper requires read/write statement",
        ));
    }
    executor.execute(statement)
}

/// Ensures a statement is read-only before using a read SPI path.
///
/// # Errors
///
/// Returns an error when the statement requires read/write access.
pub fn require_read_only(statement: &SpiStatement) -> SpiResult<()> {
    if statement.access != SpiAccess::ReadOnly {
        return Err(map_spi_error(
            &statement.operation,
            "read helper requires read-only statement",
        ));
    }
    Ok(())
}

/// Ensures a statement is read/write before using a mutating SPI path.
///
/// # Errors
///
/// Returns an error when the statement is read-only.
pub fn require_read_write(statement: &SpiStatement) -> SpiResult<()> {
    if statement.access != SpiAccess::ReadWrite {
        return Err(map_spi_error(
            &statement.operation,
            "write helper requires read/write statement",
        ));
    }
    Ok(())
}

/// Recording executor used by pure Rust tests.
#[derive(Debug, Default)]
pub struct RecordingSpiExecutor {
    statements: Mutex<Vec<SpiStatement>>,
}

impl RecordingSpiExecutor {
    /// Returns the recorded statement sequence.
    #[must_use]
    pub fn statements(&self) -> Vec<SpiStatement> {
        self.statements
            .lock()
            .map(|statements| statements.clone())
            .unwrap_or_default()
    }
}

impl SpiExecutor for RecordingSpiExecutor {
    fn execute(&self, statement: SpiStatement) -> SpiResult<SpiRows> {
        self.statements
            .lock()
            .map_err(|_| map_spi_error(&statement.operation, "recording executor lock poisoned"))?
            .push(statement);
        Ok(SpiRows { rows_affected: 1 })
    }
}

/// PostgreSQL SPI executor.
#[cfg(feature = "pg")]
#[derive(Debug, Default)]
pub struct PgSpiExecutor;

#[cfg(feature = "pg")]
impl SpiExecutor for PgSpiExecutor {
    fn execute(&self, statement: SpiStatement) -> SpiResult<SpiRows> {
        let rows = match statement.access {
            SpiAccess::ReadOnly => pgrx::Spi::connect(|client| {
                client
                    .select(&statement.sql, None, &[])
                    .map(|tuples| SpiRows {
                        rows_affected: tuples.len() as u64,
                    })
            }),
            SpiAccess::ReadWrite => pgrx::Spi::connect_mut(|client| {
                client
                    .update(&statement.sql, None, &[])
                    .map(|tuples| SpiRows {
                        rows_affected: tuples.len() as u64,
                    })
            }),
        }
        .map_err(|error| map_spi_error(&statement.operation, &error.to_string()))?;
        Ok(rows)
    }
}

#[cfg(feature = "pg")]
thread_local! {
    static PREPARED_PLAN_CACHE: std::cell::RefCell<
        std::collections::HashMap<PreparedPlanKey, pgrx::spi::OwnedPreparedStatement>
    > = std::cell::RefCell::new(std::collections::HashMap::new());
}

/// Invalidates one cached prepared plan in the current backend.
#[cfg(feature = "pg")]
pub fn invalidate_prepared_plan(key: &PreparedPlanKey) {
    PREPARED_PLAN_CACHE.with(|cache| {
        cache.borrow_mut().remove(key);
    });
}

/// Invalidates all cached prepared plans in the current backend.
#[cfg(feature = "pg")]
pub fn invalidate_all_prepared_plans() {
    PREPARED_PLAN_CACHE.with(|cache| {
        cache.borrow_mut().clear();
    });
}

/// Executes a statement through a backend-local prepared plan cache.
///
/// # Errors
///
/// Returns an error when preparing, executing, or decoding the statement fails.
#[cfg(feature = "pg")]
pub fn execute_prepared<R>(
    statement: &SpiStatement,
    args: &[pgrx::datum::DatumWithOid],
    decode: impl for<'conn> Fn(pgrx::spi::SpiTupleTable<'conn>) -> pgrx::spi::Result<R>,
) -> SpiResult<R> {
    let key = prepared_plan_key(statement);
    match execute_prepared_once(statement, args, &decode, &key) {
        Ok(value) => Ok(value),
        Err(_) => {
            invalidate_prepared_plan(&key);
            execute_prepared_once(statement, args, &decode, &key)
                .map_err(|error| map_spi_error(&statement.operation, &error.to_string()))
        }
    }
}

#[cfg(feature = "pg")]
fn execute_prepared_once<R>(
    statement: &SpiStatement,
    args: &[pgrx::datum::DatumWithOid],
    decode: &impl for<'conn> Fn(pgrx::spi::SpiTupleTable<'conn>) -> pgrx::spi::Result<R>,
    key: &PreparedPlanKey,
) -> pgrx::spi::Result<R> {
    match statement.access {
        SpiAccess::ReadOnly => pgrx::Spi::connect(|client| {
            PREPARED_PLAN_CACHE.with(|cache| {
                let mut cache = cache.borrow_mut();
                if !cache.contains_key(key) {
                    let arg_types = pg_param_oids(&statement.param_types);
                    let prepared = client.prepare(&statement.sql, &arg_types)?.keep();
                    cache.insert((*key).clone(), prepared);
                }
                let plan = cache
                    .get(key)
                    .expect("prepared plan inserted before execution");
                let tuples = client.select(plan, None, args)?;
                decode(tuples)
            })
        }),
        SpiAccess::ReadWrite => pgrx::Spi::connect_mut(|client| {
            PREPARED_PLAN_CACHE.with(|cache| {
                let mut cache = cache.borrow_mut();
                if !cache.contains_key(key) {
                    let arg_types = pg_param_oids(&statement.param_types);
                    let prepared = client.prepare_mut(&statement.sql, &arg_types)?.keep();
                    cache.insert((*key).clone(), prepared);
                }
                let plan = cache
                    .get(key)
                    .expect("prepared plan inserted before execution");
                let tuples = client.update(plan, None, args)?;
                decode(tuples)
            })
        }),
    }
}

/// Executes a read-only parameterized statement and decodes the first column of
/// the first row while the SPI connection is still open.
///
/// # Errors
///
/// Returns an error if the statement is not read-only, SPI execution fails, or
/// the returned datum cannot be decoded as `T`.
#[cfg(feature = "pg")]
pub fn select_one<T>(
    statement: &SpiStatement,
    args: &[pgrx::datum::DatumWithOid],
) -> SpiResult<Option<T>>
where
    T: pgrx::datum::FromDatum + pgrx::datum::IntoDatum,
{
    require_read_only(statement)?;
    pgrx::Spi::connect(|client| client.select(&statement.sql, Some(1), args)?.get_one())
        .map_err(|error| map_spi_error(&statement.operation, &error.to_string()))
}

/// Executes a read-only statement returning a JSON string in the first column.
///
/// # Errors
///
/// Returns an error if the statement is not read-only, SPI execution fails, the
/// query returns no row, or the returned text is not valid JSON.
#[cfg(feature = "pg")]
pub fn select_json_one(
    statement: &SpiStatement,
    args: &[pgrx::datum::DatumWithOid],
) -> SpiResult<serde_json::Value> {
    let json = select_one::<String>(statement, args)?
        .ok_or_else(|| map_spi_error(&statement.operation, "query returned no rows"))?;
    serde_json::from_str(&json)
        .map_err(|error| map_spi_error(&statement.operation, &error.to_string()))
}

/// Executes a read/write parameterized statement and returns processed rows.
///
/// # Errors
///
/// Returns an error if the statement is not read/write or SPI execution fails.
#[cfg(feature = "pg")]
pub fn update(statement: &SpiStatement, args: &[pgrx::datum::DatumWithOid]) -> SpiResult<SpiRows> {
    require_read_write(statement)?;
    let rows = pgrx::Spi::connect_mut(|client| {
        client
            .update(&statement.sql, None, args)
            .map(|tuples| SpiRows {
                rows_affected: tuples.len() as u64,
            })
    })
    .map_err(|error| map_spi_error(&statement.operation, &error.to_string()))?;
    Ok(rows)
}

/// Executes a read/write parameterized statement and decodes the first column
/// of the first returned row while the SPI connection is still open.
///
/// # Errors
///
/// Returns an error if the statement is not read/write, SPI execution fails, or
/// the returned datum cannot be decoded as `T`.
#[cfg(feature = "pg")]
pub fn update_one<T>(
    statement: &SpiStatement,
    args: &[pgrx::datum::DatumWithOid],
) -> SpiResult<Option<T>>
where
    T: pgrx::datum::FromDatum + pgrx::datum::IntoDatum,
{
    require_read_write(statement)?;
    pgrx::Spi::connect_mut(|client| client.update(&statement.sql, Some(1), args)?.get_one())
        .map_err(|error| map_spi_error(&statement.operation, &error.to_string()))
}
