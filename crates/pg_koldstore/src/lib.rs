//! Thin PostgreSQL integration layer for pg-koldstore.

pub mod catalog;
/// Adapter-layer SPI / PostgreSQL error type.
#[cfg(feature = "pg")]
pub(crate) mod error;
/// Test-only flush failpoints (GUC-armed; inert when unset).
pub mod failpoints;
pub mod guc;
pub mod hooks;
pub mod memory;
pub mod merge_scan;
/// WAL-backed latest-state mirror capture (slot, apply, provision).
pub mod mirror;
pub mod object_store;
pub mod observability;
#[cfg(feature = "pg")]
pub mod preload;
pub mod row_counter_cache;
pub mod settings;
pub mod spi;
pub mod sql;
/// Cluster-supervised PostgreSQL background work adapter over `koldstore-worker`.
#[cfg(feature = "pg")]
pub mod worker;

#[cfg(feature = "pg_test")]
mod pg_tests;

#[cfg(feature = "pg_bench")]
mod pg_benches;

/// Required by `cargo pgrx test` invocations. Must remain at the crate root.
#[cfg(feature = "pg_test")]
pub mod pg_test {
    /// One-off initialization when the pgrx test framework starts.
    pub fn setup(_options: Vec<&str>) {}

    /// Extra `postgresql.conf` settings required for in-server tests.
    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![
            "wal_level=logical",
            // Merge-scan hooks + supervisor must exist in every backend.
            "shared_preload_libraries=koldstore",
            // Supervisor + provisioner + ephemeral workers need headroom.
            "max_worker_processes=16",
        ]
    }
}

/// Extension version exposed by SQL.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(feature = "pg")]
::pgrx::pg_module_magic!();

/// Extension-owned SQL schema for pgrx-generated functions.
#[cfg(feature = "pg")]
#[pgrx::pg_schema]
mod koldstore {}

#[cfg(feature = "pg")]
pgrx::extension_sql_file!(
    "../sql/koldstore--0.1.0.sql",
    name = "koldstore_catalog",
    bootstrap
);

#[cfg(feature = "pg")]
pgrx::extension_sql_file!(
    "../sql/koldstore-performance-indexes.sql",
    name = "koldstore_performance_indexes",
    requires = ["koldstore_catalog"]
);

/// Returns the extension version.
#[must_use]
#[cfg_attr(feature = "pg", pgrx::pg_extern(name = "koldstore_version"))]
pub fn koldstore_version() -> &'static str {
    VERSION
}

/// Initializes extension hooks when loaded by PostgreSQL.
///
/// Must run under `shared_preload_libraries`. Loading via `CREATE EXTENSION` /
/// `LOAD` / `session_preload_libraries` alone is rejected so managed-table
/// SELECTs cannot silently fall back to heap-only scans in fresh backends.
#[cfg(feature = "pg")]
#[no_mangle]
pub extern "C" fn _PG_init() {
    let preloading = unsafe { pgrx::pg_sys::process_shared_preload_libraries_in_progress };
    if !preloading {
        pgrx::error!("{}", preload::preload_required_message());
    }
    preload::mark_loaded_via_shared_preload();

    #[cfg(any(feature = "s3", feature = "gcs", feature = "azure"))]
    koldstore_storage::ensure_rustls_ring_provider();
    observability::init_tracing();
    guc::define_gucs();
    object_store::install_interrupt_hook();
    worker::wake::initialize();
    catalog::cache::register_invalidation_callback();
    hooks::register_hooks();
    row_counter_cache::register_xact_callbacks();
    sql::flush::spi::register_flush_origin_xact_callback();
    worker::register_supervisor_if_shared_preload();
}
