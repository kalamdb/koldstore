use koldstore_common::SeqId;
use koldstore_flush::policy::{
    flush_rows_for_excess, select_mirror_flush_candidates, FlushPolicy, MirrorPolicyRow,
};

#[test]
fn structured_schema_options_load_hot_row_limit_policy() {
    let policy = FlushPolicy::from_value(&serde_json::json!({
        "hot_row_limit": 10_000,
        "min_flush_rows": 1_000,
        "max_rows_per_file": 500,
    }))
    .unwrap();

    assert_eq!(policy.hot_row_limit, Some(10_000));
    assert_eq!(policy.min_flush_rows, Some(1_000));
    assert_eq!(policy.max_rows_per_file, Some(500));
}

#[test]
fn flush_rows_for_excess_honors_min_flush_rows_threshold() {
    assert_eq!(flush_rows_for_excess(505, 1_000), 0);
    assert_eq!(flush_rows_for_excess(1_000, 1_000), 1_000);
    assert_eq!(flush_rows_for_excess(1_250, 1_000), 1_000);
    assert_eq!(flush_rows_for_excess(1_500, 1_000), 1_500);
}

#[test]
fn row_limit_policy_selects_oldest_excess_mirror_rows_by_seq() {
    let rows = (1..=5)
        .rev()
        .map(|seq| MirrorPolicyRow {
            pk_json: serde_json::json!({ "id": seq }),
            seq: SeqId::new(seq).unwrap(),
        })
        .collect::<Vec<_>>();

    let selected = select_mirror_flush_candidates(
        &FlushPolicy {
            hot_row_limit: Some(3),
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

#[test]
fn row_limit_policy_with_min_flush_rows_flushes_oldest_excess_in_chunks() {
    let rows = (1..=11_250)
        .map(|seq| MirrorPolicyRow {
            pk_json: serde_json::json!({ "id": seq }),
            seq: SeqId::new(seq).unwrap(),
        })
        .collect::<Vec<_>>();

    let selected = select_mirror_flush_candidates(
        &FlushPolicy {
            hot_row_limit: Some(10_000),
            min_flush_rows: Some(1_000),
            max_rows_per_file: Some(500),
        },
        &rows,
    );

    assert_eq!(selected.len(), 1_000);
    assert_eq!(selected.first().unwrap().seq.get(), 1);
    assert_eq!(selected.last().unwrap().seq.get(), 1_000);
}
