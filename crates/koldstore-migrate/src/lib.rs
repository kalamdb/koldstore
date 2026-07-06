//! Migrate and demigrate workflow planning.
//!
//! Owns constraint validation, backfill ordering, registry plans, rollback, and
//! durable migration job models. Must not depend on `pgrx`. Trigger creation and
//! SPI execution stay in `pg_koldstore`.

pub mod backfill;
pub mod capture;
pub mod constraints;
pub mod introspection;
pub mod jobs;
pub mod lock;
pub mod mirror;
pub mod order;
pub mod plan;
pub mod register;
pub mod rehydrate;
pub mod request;
pub mod rollback;
pub mod scope;

pub use capture::{
    plan_mirror_capture, plan_mirror_capture_teardown, MirrorCaptureError, MirrorCapturePlan,
    MirrorCaptureResult,
};
pub use koldstore_common::QualifiedTableName;
pub use mirror::{
    mirror_relation_for_source, plan_change_log_mirror, plan_change_log_mirror_from_columns,
    ChangeLogMirrorPlan, MirrorError, MirrorResult,
};
pub use plan::{
    plan_empty_table_migration, plan_existing_table_migration, EmptyTableMigrationPlan,
    ExistingTableCatalog, ExistingTableMigrationPlan, MigrationTableContext,
};
pub use request::{DemigrateTableRequest, MigrateTableRequest, MigrationError, MigrationResult};
