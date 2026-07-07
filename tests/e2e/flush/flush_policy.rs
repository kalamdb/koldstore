#[path = "../common/mod.rs"]
mod common;

use koldstore_common::SeqId;
use koldstore_flush::policy::{select_mirror_flush_candidates, FlushPolicy, MirrorPolicyRow};

#[test]
fn flush_policy_e2e_contract_selects_row_limit_candidates() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    let rows = vec![
        MirrorPolicyRow {
            pk_json: serde_json::json!({"id": 1}),
            seq: SeqId::new(1).unwrap(),
        },
        MirrorPolicyRow {
            pk_json: serde_json::json!({"id": 2}),
            seq: SeqId::new(2).unwrap(),
        },
        MirrorPolicyRow {
            pk_json: serde_json::json!({"id": 3}),
            seq: SeqId::new(3).unwrap(),
        },
    ];

    let selected = select_mirror_flush_candidates(
        &FlushPolicy {
            hot_row_limit: Some(1),
            min_flush_rows: None,
            max_rows_per_file: None,
        },
        &rows,
    );

    assert_eq!(
        selected.iter().map(|row| row.seq.get()).collect::<Vec<_>>(),
        vec![1, 2]
    );
}
