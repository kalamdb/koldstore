use crate::common;

use koldstore_flush::policy::{policy_flush_row_count, FlushPolicy};

#[test]
fn flush_policy_e2e_contract_selects_row_limit_candidates() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    let flush_count = policy_flush_row_count(
        3,
        &FlushPolicy::RowLimit {
            hot_row_limit: 1,
            min_flush_rows: 1,
            max_rows_per_file: 1_000,
            max_rows_per_flush: 10_000,
        },
    );

    assert_eq!(flush_count, 2);
}
