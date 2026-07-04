//! Shared E2E helpers for local pgrx-backed PostgreSQL tests.
#![allow(dead_code, unused_imports)]

pub mod assertions;
mod catalog;
mod cluster;
mod db;
mod sql;

pub use catalog::{
    active_job_count, assert_catalog_has_active_schema, assert_cold_metadata_present,
    assert_no_active_jobs, assert_system_columns_present, cold_segment_count, manifest_count,
};
pub use cluster::{
    connect, expected_pg_ports, expected_pg_versions, local_pg_matrix, require_pgrx_server,
    require_pgrx_server_sync, wait_for_postgres, PgTarget, PgrxServer,
};
pub use db::{ManagedTable, TestDb};
pub use sql::{
    assert_index_scan, explain, explain_with_seqscan_disabled, relation_size, row_count,
    RelationSize,
};
