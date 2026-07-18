//! Migrate and demigrate workflow planning.
//!
//! Owns constraint validation, backfill ordering, registry plans, rollback, and
//! durable migration job models. Must not depend on `pgrx`. Trigger creation and
//! SPI execution stay in `pg_koldstore`.

#[path = "workflow/backfill.rs"]
pub mod backfill;
#[path = "sql/capture.rs"]
pub mod capture;
#[path = "workflow/drop_table.rs"]
pub mod drop_table;
#[path = "catalog/introspection.rs"]
pub mod introspection;
pub mod jobs;
#[path = "workflow/lock.rs"]
pub mod lock;
#[path = "sql/mirror.rs"]
pub mod mirror;
#[path = "workflow/plan.rs"]
pub mod plan;
#[path = "catalog/refresh.rs"]
pub mod refresh;
#[path = "catalog/register.rs"]
pub mod register;
#[path = "workflow/rehydrate.rs"]
pub mod rehydrate;
#[path = "models/request.rs"]
pub mod request;
#[path = "workflow/rollback.rs"]
pub mod rollback;
#[path = "security/scope.rs"]
pub mod scope;
pub mod validation;

pub use validation::{constraints, manage_table, order};

pub use capture::{
    async_worker_kick_trigger_name, async_worker_kick_trigger_names, plan_drop_mirror_dml_triggers,
    plan_mirror_capture, plan_mirror_capture_teardown, MirrorCaptureError, MirrorCapturePlan,
    MirrorCaptureResult,
};
pub use drop_table::{
    plan_drop_table_cleanup, DropTableCleanupError, DropTableCleanupOutcome, DropTableCleanupPlan,
    DropTableCleanupPolicy,
};
pub use koldstore_common::{FlushPolicy, ManageTableOptions, QualifiedTableName};
pub use mirror::{
    mirror_relation_for_source, plan_change_log_mirror, plan_change_log_mirror_from_columns,
    ChangeLogMirrorPlan, MirrorError, MirrorResult,
};
pub use plan::{
    plan_empty_table_migration, plan_existing_table_migration, EmptyTableMigrationPlan,
    ExistingTableCatalog, ExistingTableMigrationPlan, MigrationTableContext,
};
pub use refresh::{
    plan_active_schema_refresh_context_json, plan_schema_refresh,
    registration_metadata_for_refresh, ActiveSchemaRefreshContext, SchemaRefreshPlan,
};
pub use request::{DemigrateTableRequest, MigrateTableRequest, MigrationError, MigrationResult};
