//! PostgreSQL hook integration.

pub mod ddl;
#[cfg(feature = "pg")]
pub(crate) mod drop_cleanup;
pub mod executor;
pub mod planner;
pub mod xact;

/// Registers PostgreSQL hooks.
pub fn register_hooks() {
    #[cfg(feature = "pg")]
    {
        crate::merge_scan::pg::register_custom_scan_hooks();
        ddl::register_process_utility_hook();
    }
}

/// Hook names installed by the extension shell at `_PG_init`.
#[must_use]
pub const fn registered_hook_names() -> &'static [&'static str] {
    &[
        "set_rel_pathlist",
        "ProcessUtility",
        "XactCallback",
        "RelcacheCallback",
    ]
}
