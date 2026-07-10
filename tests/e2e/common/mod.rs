//! Shared E2E helpers for local pgrx-backed PostgreSQL tests.
#![allow(dead_code, unused_imports)]

pub mod assertions;
mod catalog;
mod cluster;
mod db;
mod describe_table;
mod log;
mod minio;
mod sql;

pub use assertions::{
    assert_kold_merge_scan_cold_reads, assert_kold_merge_scan_executed_cold_reads,
    assert_kold_merge_scan_explain, assert_kold_merge_scan_planned_cold_reads,
    assert_merge_scan_explain, assert_minio_listing_contains,
};

pub use catalog::{
    active_job_count, assert_catalog_has_active_schema, assert_change_log_mirror_exists,
    assert_cold_metadata_present, assert_no_active_jobs, assert_primary_key_columns_match,
    assert_system_columns_absent, change_log_mirror_relation, cold_segment_count, manifest_count,
    primary_key_columns, published_manifest_count,
};
pub use cluster::{
    connect, expected_pg_ports, expected_pg_versions, local_pg_matrix, require_pgrx_server,
    require_pgrx_server_sync, scenario_pg_matrix, wait_for_postgres, PgTarget, PgrxServer,
};
pub use db::{FixtureStorage, ManagedTable, TestDb};
pub use describe_table::{
    assert_cold_rows_at_least, assert_flush_pruned_hot_storage, describe_table, TableStorageStatus,
};
pub use log::{log, log_always, log_step, log_step_always, timed_sync, verbose_enabled, StepGuard};
pub use minio::{minio_enabled, MinioConfig};
pub use sql::{
    assert_index_scan, explain, explain_analyze, explain_with_seqscan_disabled, hot_row_count,
    relation_size, row_count, row_count_from_sql, RelationSize,
};
