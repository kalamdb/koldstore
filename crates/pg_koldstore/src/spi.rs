//! Safe SPI helper boundary.
//!
//! Re-exports pg-free SQL plans from `koldstore-common` and adds PostgreSQL SPI
//! execution helpers behind the `pg` feature.

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::Mutex,
};

pub use koldstore_common::sql::{
    map_sql_error as map_spi_error, SqlAccess as SpiAccess, SqlError as SpiError, SqlParamType,
    SqlResult as SpiResult, SqlStatement as SpiStatement,
};

/// SQLSTATE used for pg-koldstore errors.
pub const KOLDSTORE_SQLSTATE: &str = "XXKLD";

/// Maps one pg-free parameter type to a PostgreSQL OID.
#[cfg(feature = "pg")]
#[must_use]
fn sql_param_pg_oid(param: SqlParamType) -> pgrx::pg_sys::PgOid {
    let oid = match param {
        SqlParamType::BigInt => pgrx::pg_sys::INT8OID,
        SqlParamType::Integer => pgrx::pg_sys::INT4OID,
        SqlParamType::Text => pgrx::pg_sys::TEXTOID,
        SqlParamType::Jsonb => pgrx::pg_sys::JSONBOID,
        SqlParamType::Oid => pgrx::pg_sys::OIDOID,
        SqlParamType::Uuid => pgrx::pg_sys::UUIDOID,
        SqlParamType::Boolean => pgrx::pg_sys::BOOLOID,
    };
    pgrx::pg_sys::PgOid::from(oid)
}

/// Maps pg-free parameter metadata to pgrx PostgreSQL OIDs.
#[cfg(feature = "pg")]
#[must_use]
pub fn pg_param_oids(param_types: &[SqlParamType]) -> Vec<pgrx::pg_sys::PgOid> {
    param_types.iter().copied().map(sql_param_pg_oid).collect()
}

/// Converts a `uuid::Uuid` into a pgrx SPI UUID datum type.
#[cfg(feature = "pg")]
#[must_use]
pub fn uuid_to_pgrx(id: uuid::Uuid) -> pgrx::Uuid {
    pgrx::Uuid::from_bytes(*id.as_bytes())
}

/// Converts a pgrx SPI UUID into `uuid::Uuid`.
#[cfg(feature = "pg")]
#[must_use]
pub fn uuid_from_pgrx(id: pgrx::Uuid) -> uuid::Uuid {
    uuid::Uuid::from_bytes(*id.as_bytes())
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

#[cfg(feature = "pg")]
const PREPARED_PLAN_CACHE_LIMIT: usize = 64;

#[cfg(feature = "pg")]
thread_local! {
    static PREPARED_PLAN_CACHE: std::cell::RefCell<
        std::collections::HashMap<PreparedPlanKey, pgrx::spi::OwnedPreparedStatement>
    > = std::cell::RefCell::new(std::collections::HashMap::new());
}

/// Inserts a prepared plan, evicting an arbitrary entry when the cache is full.
#[cfg(feature = "pg")]
fn insert_prepared_plan(
    cache: &mut std::collections::HashMap<PreparedPlanKey, pgrx::spi::OwnedPreparedStatement>,
    key: PreparedPlanKey,
    plan: pgrx::spi::OwnedPreparedStatement,
) {
    if cache.len() >= PREPARED_PLAN_CACHE_LIMIT && !cache.contains_key(&key) {
        if let Some(evicted) = cache.keys().next().cloned() {
            cache.remove(&evicted);
        }
    }
    cache.insert(key, plan);
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
                    insert_prepared_plan(&mut cache, (*key).clone(), prepared);
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
                    insert_prepared_plan(&mut cache, (*key).clone(), prepared);
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

/// Decodes the first column of the first SPI row when present.
#[cfg(feature = "pg")]
pub(crate) fn first_row<T>(tuples: pgrx::spi::SpiTupleTable<'_>) -> pgrx::spi::Result<Option<T>>
where
    T: pgrx::datum::FromDatum + pgrx::datum::IntoDatum,
{
    if tuples.is_empty() {
        Ok(None)
    } else {
        tuples.first().get_one()
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
    pgrx::Spi::connect(|client| first_row(client.select(&statement.sql, Some(1), args)?))
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

/// Executes a read/write statement from a transaction callback where no executor
/// portal or snapshot is active.
///
/// `RegisterXactCallback` handlers must push a transaction snapshot and connect
/// with `SPI_OPT_NONATOMIC` before calling SPI.
///
/// # Errors
///
/// Returns an error if the statement is not read/write or SPI execution fails.
#[cfg(feature = "pg")]
pub fn update_in_xact_callback(
    statement: &SpiStatement,
    args: &[pgrx::datum::DatumWithOid],
) -> SpiResult<SpiRows> {
    use std::ffi::CString;
    use std::os::raw::c_char;

    require_read_write(statement)?;

    let sql = CString::new(statement.sql.trim())
        .map_err(|error| map_spi_error(&statement.operation, &error.to_string()))?;

    let mut argtypes = Vec::with_capacity(args.len());
    let mut datums = Vec::with_capacity(args.len());
    let mut nulls = Vec::with_capacity(args.len());
    for arg in args {
        argtypes.push(arg.oid());
        match arg.datum() {
            Some(datum) => {
                datums.push(datum.sans_lifetime());
                nulls.push(' ' as c_char);
            }
            None => {
                datums.push(pgrx::pg_sys::Datum::from(0usize));
                nulls.push('n' as c_char);
            }
        }
    }

    struct XactCallbackSpiGuard;
    impl Drop for XactCallbackSpiGuard {
        fn drop(&mut self) {
            unsafe {
                pgrx::pg_sys::PopActiveSnapshot();
                let _ = pgrx::pg_sys::SPI_finish();
            }
        }
    }

    unsafe {
        pgrx::Spi::check_status(pgrx::pg_sys::SPI_connect_ext(
            pgrx::pg_sys::SPI_OPT_NONATOMIC as i32,
        ))
        .map_err(|error| map_spi_error(&statement.operation, &error.to_string()))?;

        pgrx::pg_sys::PushActiveSnapshot(pgrx::pg_sys::GetTransactionSnapshot());
        let _guard = XactCallbackSpiGuard;

        pgrx::pg_sys::SPI_tuptable = std::ptr::null_mut();
        let status_code = pgrx::pg_sys::SPI_execute_with_args(
            sql.as_ptr(),
            args.len() as i32,
            argtypes.as_mut_ptr(),
            datums.as_mut_ptr(),
            nulls.as_ptr(),
            false,
            0,
        );
        pgrx::Spi::check_status(status_code)
            .map_err(|error| map_spi_error(&statement.operation, &error.to_string()))?;

        Ok(SpiRows {
            rows_affected: pgrx::pg_sys::SPI_processed,
        })
    }
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
    pgrx::Spi::connect_mut(|client| first_row(client.update(&statement.sql, Some(1), args)?))
        .map_err(|error| map_spi_error(&statement.operation, &error.to_string()))
}
