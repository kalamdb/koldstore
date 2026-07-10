#[path = "../common/mod.rs"]
mod common;

use koldstore_flush::policy::{policy_flush_row_count, FlushPolicy};

#[test]
fn flush_policy_e2e_contract_selects_row_limit_candidates() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    let flush_count = policy_flush_row_count(
        3,
        &FlushPolicy {
            hot_row_limit: Some(1),
            min_flush_rows: None,
            max_rows_per_file: None,
            target_file_size_mb: None,
        },
    );

    assert_eq!(flush_count, 2);
}
