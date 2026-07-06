#[path = "../common/mod.rs"]
mod common;

use std::time::{Duration, SystemTime};

use koldstore_common::SeqId;
use koldstore_flush::policy::{select_mirror_flush_candidates, FlushPolicy, MirrorPolicyRow};

#[test]
fn flush_policy_e2e_contract_selects_row_limit_and_duration_candidates() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100_000);
    let rows = vec![
        MirrorPolicyRow {
            pk_json: serde_json::json!({"id": 1}),
            seq: SeqId::new(1).unwrap(),
            changed_at: now - Duration::from_secs(90_000),
        },
        MirrorPolicyRow {
            pk_json: serde_json::json!({"id": 2}),
            seq: SeqId::new(2).unwrap(),
            changed_at: now - Duration::from_secs(90_000),
        },
        MirrorPolicyRow {
            pk_json: serde_json::json!({"id": 3}),
            seq: SeqId::new(3).unwrap(),
            changed_at: now - Duration::from_secs(60),
        },
    ];

    let selected = select_mirror_flush_candidates(
        &FlushPolicy::parse("rows:1,duration:1d").unwrap(),
        &rows,
        now,
    );

    assert_eq!(
        selected.iter().map(|row| row.seq.get()).collect::<Vec<_>>(),
        vec![1, 2]
    );
}
