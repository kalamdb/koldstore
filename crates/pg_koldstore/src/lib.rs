//! Thin PostgreSQL integration layer for pg-koldstore.

pub mod flush;
pub mod guc;
pub mod hooks;
pub mod memory;
pub mod merge_scan;
pub mod migrate;
pub mod observability;
pub mod security;
pub mod spi;
pub mod sql;

/// Extension version exposed by SQL.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
::pgrx::pg_module_magic!();

/// Extension-owned SQL schema for pgrx-generated functions.
#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
#[pgrx::pg_schema]
mod koldstore {}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
pgrx::extension_sql_file!(
    "../sql/koldstore--0.1.0.sql",
    name = "koldstore_catalog",
    bootstrap
);

/// Returns the extension version.
#[must_use]
#[cfg_attr(
    any(feature = "pg15", feature = "pg16", feature = "pg17"),
    pgrx::pg_extern(name = "koldstore_version")
)]
pub fn koldstore_version() -> &'static str {
    VERSION
}

/// Initializes extension hooks when loaded by PostgreSQL.
#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
#[no_mangle]
pub extern "C" fn _PG_init() {
    observability::init_tracing();
    guc::define_gucs();
    hooks::register_hooks();
}
