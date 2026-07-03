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

#[cfg(feature = "pg16")]
::pgrx::pg_module_magic!();

/// Returns the extension version.
#[must_use]
#[cfg_attr(feature = "pg16", pgrx::pg_extern(name = "koldstore_version"))]
pub fn koldstore_version() -> &'static str {
    VERSION
}

/// Initializes extension hooks when loaded by PostgreSQL.
#[cfg(feature = "pg16")]
#[no_mangle]
pub extern "C" fn _PG_init() {
    observability::init_tracing();
    guc::define_gucs();
    hooks::register_hooks();
}
