//! Shared E2E helpers for local pgrx-backed PostgreSQL tests.
#![allow(dead_code, unused_imports)]

pub mod assertions;
mod async_mirror;
mod catalog;
mod cluster;
mod db;
mod db_pool;
pub mod equality;
mod flush_executor;
mod log;
pub mod memory;
mod minio;
mod oracle;
mod peer;
mod sql;
mod table_status;

pub use assertions::{
    assert_kold_merge_scan_cold_reads, assert_kold_merge_scan_executed_cold_reads,
    assert_kold_merge_scan_explain, assert_kold_merge_scan_explain_json_tracing,
    assert_kold_merge_scan_hot_planned_access, assert_managed_read_plan,
    assert_minio_listing_contains,
};

pub use async_mirror::{
    async_mirror_progress, async_worker_running, current_wal_lsn, fence_async_mirror,
    force_stop_async_worker, mirror_op_count, release_async_worker_stop_lock,
    terminate_async_worker, wait_for_async_mirror, wait_for_async_worker,
    wait_for_async_worker_auto_restart, wait_for_confirmed_flush_at_least,
    wait_for_confirmed_flush_past, wait_for_mirror_op_count, wal_lsn_diff_bytes,
    AsyncMirrorProgress,
};

pub use catalog::{
    active_job_count, assert_catalog_has_active_schema, assert_change_log_mirror_exists,
    assert_cold_metadata_present, assert_no_active_jobs, assert_primary_key_columns_match,
    assert_system_columns_absent, change_log_mirror_relation, cold_segment_count, manifest_count,
    primary_key_columns, published_manifest_count,
};
pub use cluster::{
    connect, error_chain_contains, expected_pg_ports, expected_pg_versions, local_pg_matrix,
    require_pgrx_server, require_pgrx_server_sync, scenario_pg_matrix, wait_for_postgres, PgTarget,
    PgrxServer,
};
pub use db::{
    flush_table_job_id, is_flush_entry_lock_busy, is_flush_slot_lock_contention,
    wait_for_flush_job_terminal, FixtureStorage, ManagedTable, TestDb,
};
pub use db_pool::{acquire_cluster_exclusive, ClusterExclusiveGuard};
pub use equality::{
    assert_pk_unique, assert_relations_equal, assert_row_counts_equal, relation_row_count,
};
pub use flush_executor::{
    flush_executor_backend_type, flush_executor_pids, sigkill_flush_executors, sigkill_pid,
    wait_for_flush_executor_pids, wait_until_no_flush_executors, FLUSH_EXECUTOR_BACKEND_PREFIX,
};
pub use log::{log, log_always, log_step, log_step_always, timed_sync, verbose_enabled, StepGuard};
pub use minio::{minio_enabled, MinioConfig};
pub use oracle::{
    apply_dml_to_both, assert_managed_matches_reference, assert_managed_matches_reference_ordered,
    clone_reference_sql, create_reference_clone,
};
pub use peer::{
    barrier_lock, barrier_unlock, connect_flush_peer, connect_peer, BARRIER_LOCK_NAMESPACE,
};
pub use sql::{
    assert_index_scan, explain, explain_analyze, explain_analyze_json,
    explain_with_seqscan_disabled, hot_row_count, relation_size, row_count, row_count_from_sql,
    RelationSize, SQL_DEFAULT_COLD_OBJECT_KEY, SQL_DEFAULT_MANIFEST_OBJECT_KEY,
};
pub use table_status::{
    assert_cold_rows_at_least, assert_flush_pruned_hot_storage, table_status, TableStorageStatus,
};
