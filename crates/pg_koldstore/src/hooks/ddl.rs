//! DDL event-trigger integration.
//!
//! DROP TABLE cleanup planning lives in `koldstore-migrate`; this module
//! re-exports those plans for the extension shell. Live ProcessUtility hook
//! wiring is not installed yet.

pub use koldstore_migrate::drop_table::{
    plan_drop_table_cleanup, DropTableCleanupError, DropTableCleanupOutcome, DropTableCleanupPlan,
    DropTableCleanupPolicy,
};
