//! WAL capture stack: persistent applier lifecycle plus clean-schema mirror contracts.
//!
//! ```text
//! koldstore-wal-mirror
//! ├── wal     — applier registry, worker identity, apply batch capacity
//! └── mirror  — __cl SQL planners, pgoutput decode, PK guards
//! ```
//!
//! This crate is PostgreSQL-free. SPI, latches, shared memory, and background
//! worker entry points remain in `pg_koldstore`. Flush/migrate/merge depend on
//! [`mirror`] for SQL contracts; the supervisor depends on [`wal`] for the
//! persistent service lifecycle.

pub mod mirror;
pub mod wal;

pub use mirror::{
    apply_row, batch, columns, error, guard, pgoutput, read, relation, row_json, schema, shared,
    statement, write,
};
pub use mirror::{
    decode_message, mirror_relation_for_source, must_flush_before_push, parse_pk_bool,
    parse_pk_ints, pg_value_json, pg_value_text, pk_identity,
    plan_async_mirror_batch_delete_existing, plan_async_mirror_batch_update,
    plan_async_mirror_batch_upsert, plan_drop_mirror_table, plan_mirror_force_flush_stats,
    plan_mirror_oldest_rows_max_seq, plan_mirror_op_stats, plan_mirror_pk_column_renames,
    plan_mirror_pk_guard, plan_mirror_schema, plan_mirror_schema_with_order_key,
    plan_mirror_source_teardown, plan_mirror_stats, plan_select_mirror_last_rows,
    plan_select_mirror_last_rows_with_params, plan_select_mirror_rows_after_seq,
    plan_select_mirror_rows_after_seq_with_params, plan_upsert_mirror_row, primary_key_json,
    published_column_list, quoted_pk_columns, BatchFlushReason, MirrorColumn, MirrorError,
    MirrorGuardError, MirrorGuardResult, MirrorPkGuardPlan, MirrorRelation, MirrorResult,
    MirrorSchemaPlan, MirrorSeqStats, PgOutputColumn, PgOutputDecodeError, PgOutputMessage,
    PgOutputRelation, PgOutputTuple, PgOutputValue, SqlAccess, SqlParamType, SqlStatement,
    APPLY_BATCH_ROWS, CHANGE_LOG_MIRROR_SUFFIX, KOLDSTORE_SCHEMA,
};
pub use wal::apply_contract::{
    budget_hit, resolve_row_budget, resolve_time_budget, BoundedApplyOutcome, BoundedApplyRequest,
    PruneSeqFloor,
};
pub use wal::naming::{
    flush_replication_origin_name, is_flush_replication_origin, slot_name, PUBLICATION_NAME,
};
pub use wal::status::{
    build_async_mirror_status, ApplyMetricsSnapshot, AsyncMirrorStatusInput,
    StatusSupervisorSnapshot, StatusWalApplierSnapshot,
};
pub use wal::{
    wal_applier_worker_type, WalApplierRegistry, WalApplierSnapshot, WAL_APPLIER_REGISTRY_CAPACITY,
    WAL_APPLY_BATCH_ROWS,
};
