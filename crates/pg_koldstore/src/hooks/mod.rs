//! PostgreSQL hook integration.

pub mod ddl;
pub mod executor;
pub mod planner;
pub mod xact;

/// Registers PostgreSQL hooks.
pub fn register_hooks() {}

/// Hook names installed by the extension shell.
#[must_use]
pub const fn registered_hook_names() -> &'static [&'static str] {
    &[
        "set_rel_pathlist",
        "ExecutorStart",
        "ProcessUtility",
        "XactCallback",
    ]
}
