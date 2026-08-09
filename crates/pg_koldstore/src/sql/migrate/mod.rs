//! PostgreSQL table migration SQL entrypoints and module boundaries.
//!
//! This module owns the SQL wrappers and delegates SPI orchestration to the
//! feature-specific implementation modules below.

#[cfg(feature = "pg")]
use koldstore_migrate::rehydrate::DemigrateOptions;

#[cfg(feature = "pg")]
mod introspection_spi;
#[cfg(feature = "pg")]
mod manage;
#[cfg(feature = "pg")]
mod migration_jobs;
#[cfg(feature = "pg")]
mod schema_registry;
#[cfg(feature = "pg")]
mod unmanage;

#[cfg(feature = "pg")]
pub(crate) use introspection_spi::{load_migration_catalog, migration_catalog};
#[cfg(feature = "pg")]
pub(crate) use manage::manage_table_pg_impl;
#[cfg(feature = "pg")]
use manage::set_table_auto_flush_pg_impl;
#[cfg(feature = "pg")]
pub(crate) use schema_registry::{
    refresh_active_schema_if_changed, sync_active_mirror_relation_names_in_schema,
};

/// A SQL `regclass` argument decoded without opening or locking the relation.
///
/// `PgRelation` eagerly opens its relation during argument conversion. Table
/// management must provision logical capture before opening the source table,
/// so retaining only the resolved OID preserves that ordering.
#[cfg(feature = "pg")]
pub struct RegClassOid(pgrx::pg_sys::Oid);

#[cfg(feature = "pg")]
unsafe impl<'fcx> pgrx::callconv::ArgAbi<'fcx> for RegClassOid {
    unsafe fn unbox_arg_unchecked(arg: pgrx::callconv::Arg<'_, 'fcx>) -> Self {
        Self(unsafe { <pgrx::pg_sys::Oid as pgrx::callconv::ArgAbi>::unbox_arg_unchecked(arg) })
    }
}

#[cfg(feature = "pg")]
impl pgrx::FromDatum for RegClassOid {
    unsafe fn from_polymorphic_datum(
        datum: pgrx::pg_sys::Datum,
        is_null: bool,
        typoid: pgrx::pg_sys::Oid,
    ) -> Option<Self> {
        unsafe {
            <pgrx::pg_sys::Oid as pgrx::FromDatum>::from_polymorphic_datum(datum, is_null, typoid)
        }
        .map(Self)
    }
}

#[cfg(feature = "pg")]
pgrx::impl_sql_translatable!(RegClassOid, arg_only = "regclass");

/// Manages a heap table with structured hot/cold flush settings.
///
/// SQL contract:
/// `koldstore.manage_table(table_name regclass, storage, hot_row_limit, min_flush_rows default 1000, max_rows_per_file default 1000, table_type default 'shared', scope_column default null, migration_order_by default null, compression default null, target_file_size_mb default null, auto_flush default true, segment_order_column default null, pruning_columns default null, bloom_filter_columns default null, parquet_row_group_size default null, parquet_data_page_row_count_limit default null, parquet_bloom_filter_fpp default null)`.
/// When `segment_order_column` is omitted, `migration_order_by` is used for
/// both migration and cold-segment ordering.
///
/// `table_name` is PostgreSQL `regclass`, so relation names like `'app.messages'`
/// cast correctly (plain `oid` would reject that string).
/// Capture is always committed-WAL / async apply. `wal_level=logical` is required.
#[cfg(feature = "pg")]
#[allow(clippy::too_many_arguments)]
#[pgrx::pg_extern(name = "manage_table", schema = "koldstore", security_definer)]
pub fn manage_table_pg(
    table_name: RegClassOid,
    storage: &str,
    hot_row_limit: Option<i64>,
    min_flush_rows: pgrx::default!(i64, 1000),
    max_rows_per_file: pgrx::default!(i64, 1000),
    table_type: pgrx::default!(&str, "'shared'"),
    scope_column: pgrx::default!(Option<&str>, "NULL"),
    migration_order_by: pgrx::default!(Option<&str>, "NULL"),
    compression: pgrx::default!(Option<&str>, "NULL"),
    target_file_size_mb: pgrx::default!(Option<i64>, "NULL"),
    auto_flush: pgrx::default!(bool, true),
    segment_order_column: pgrx::default!(Option<&str>, "NULL"),
    pruning_columns: pgrx::default!(Option<Vec<String>>, "NULL"),
    bloom_filter_columns: pgrx::default!(Option<Vec<String>>, "NULL"),
    parquet_row_group_size: pgrx::default!(Option<i64>, "NULL"),
    parquet_data_page_row_count_limit: pgrx::default!(Option<i64>, "NULL"),
    parquet_bloom_filter_fpp: pgrx::default!(Option<f64>, "NULL"),
) -> pgrx::Uuid {
    manage::manage_table_pg_impl(
        table_name.0,
        table_type,
        storage,
        scope_column,
        migration_order_by,
        compression,
        target_file_size_mb,
        hot_row_limit,
        min_flush_rows,
        max_rows_per_file,
        auto_flush,
        segment_order_column,
        pruning_columns,
        bloom_filter_columns,
        parquet_row_group_size,
        parquet_data_page_row_count_limit,
        parquet_bloom_filter_fpp,
    )
}

/// Sets whether the built-in flush scheduler may auto-flush a managed table.
///
/// SQL contract: `koldstore.set_table_auto_flush(table_name regclass, enabled boolean)`.
/// Manual `flush_table` / `enqueue_flush_job` / cron ignore this flag.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "set_table_auto_flush", schema = "koldstore", security_definer)]
pub fn set_table_auto_flush_pg(table_name: pgrx::PgRelation, enabled: bool) -> bool {
    set_table_auto_flush_pg_impl(table_name.oid(), enabled)
        .unwrap_or_else(|error| pgrx::error!("set_table_auto_flush failed: {error}"))
}

/// Unmanages a managed table through the SQL API.
///
/// SQL contract:
/// `koldstore.unmanage_table(table_name regclass, rehydrate boolean default null, drop_cold boolean default null)`.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "unmanage_table", schema = "koldstore", security_definer)]
pub fn unmanage_table_pg(
    table_name: pgrx::PgRelation,
    rehydrate: pgrx::default!(Option<bool>, "NULL"),
    drop_cold: pgrx::default!(Option<bool>, "NULL"),
) -> i64 {
    let table_oid = table_name.oid();
    drop(table_name);
    let options = DemigrateOptions {
        rehydrate: rehydrate.unwrap_or(true),
        drop_cold: drop_cold.unwrap_or(false),
    };
    unmanage::unmanage_table_pg_impl(table_oid, options)
        .unwrap_or_else(|error| pgrx::error!("unmanage table failed: {error}"))
}
