use chrono::Utc;
use koldstore_core::{CommitSeq, LogicalPk, PkColumn, RowEvent, RowOperation, SeqId, StablePkHash};
use koldstore_merge::{changes_since, ChangeCursor};
use serde_json::json;

#[test]
fn changelog_orders_by_commit_seq_and_reports_retention_gap() {
    let columns = vec![PkColumn::new("id").unwrap()];
    let pk = LogicalPk::from_json_object(&json!({"id": 1}), &columns).unwrap();
    let event = |commit_seq| RowEvent {
        table_oid: 1,
        scope_key: None,
        pk_hash: StablePkHash::compute(&pk),
        pk_json: pk.to_canonical_json(),
        op: RowOperation::Update,
        seq: SeqId::new(commit_seq).unwrap(),
        commit_seq: CommitSeq::new(commit_seq).unwrap(),
        deleted: false,
        row_image_json: None,
        created_at: Utc::now(),
    };

    let events = vec![event(3), event(2)];
    let result = changes_since(
        &events,
        ChangeCursor {
            since_commit_seq: 1,
            limit: 10,
        },
        Some(CommitSeq::new(2).unwrap()),
    )
    .unwrap();
    assert_eq!(
        result
            .iter()
            .map(|event| event.commit_seq.get())
            .collect::<Vec<_>>(),
        vec![2, 3]
    );

    assert!(changes_since(
        &events,
        ChangeCursor {
            since_commit_seq: 0,
            limit: 10
        },
        Some(CommitSeq::new(2).unwrap())
    )
    .is_err());
}

#[test]
fn changelog_orders_same_commit_events_by_seq_for_stable_pagination() {
    let columns = vec![PkColumn::new("id").unwrap()];
    let pk = LogicalPk::from_json_object(&json!({"id": 1}), &columns).unwrap();
    let event = |seq| RowEvent {
        table_oid: 1,
        scope_key: None,
        pk_hash: StablePkHash::compute(&pk),
        pk_json: pk.to_canonical_json(),
        op: RowOperation::Update,
        seq: SeqId::new(seq).unwrap(),
        commit_seq: CommitSeq::new(10).unwrap(),
        deleted: false,
        row_image_json: None,
        created_at: Utc::now(),
    };

    let events = vec![event(3), event(1), event(2)];
    let result = changes_since(
        &events,
        ChangeCursor {
            since_commit_seq: 0,
            limit: 2,
        },
        None,
    )
    .unwrap();

    assert_eq!(
        result
            .iter()
            .map(|event| event.seq.get())
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
}
