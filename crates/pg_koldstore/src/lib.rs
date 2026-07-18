//! Thin PostgreSQL integration layer for pg-koldstore.

/// Asynchronous WAL-backed latest-state mirror capture.
pub mod async_mirror;
pub mod catalog;
/// Database-scoped background worker adapter over `koldstore-worker`.
#[cfg(feature = "pg")]
pub mod database_worker;
/// Test-only flush failpoints (GUC-armed; inert when unset).
pub mod failpoints;
pub mod guc;
pub mod hooks;
pub mod memory;
pub mod merge_scan;
pub mod observability;
pub mod row_counter_cache;
pub mod settings;
pub mod spi;
pub mod sql;

#[cfg(feature = "pg_test")]
mod pg_tests;

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
            // Launcher + provisioner + applier need headroom beyond defaults.
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

/// Returns the extension version.
#[must_use]
#[cfg_attr(feature = "pg", pgrx::pg_extern(name = "koldstore_version"))]
pub fn koldstore_version() -> &'static str {
    VERSION
}

/// Initializes extension hooks when loaded by PostgreSQL.
#[cfg(feature = "pg")]
#[no_mangle]
pub extern "C" fn _PG_init() {
    #[cfg(feature = "s3")]
    koldstore_storage::ensure_rustls_ring_provider();
    observability::init_tracing();
    guc::define_gucs();
    catalog::cache::register_invalidation_callback();
    hooks::register_hooks();
    row_counter_cache::register_xact_callbacks();
    database_worker::register_launcher_if_shared_preload();
}
