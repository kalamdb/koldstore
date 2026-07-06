use chrono::{TimeZone, Utc};
use koldstore_core::{ChangeSource, LogicalPk, MirrorChange, MirrorOperation, PkColumn, SeqId};
use koldstore_merge::{changes_since, ChangeCursor};
use serde_json::json;

#[test]
fn changelog_orders_by_mirror_seq_and_reports_retention_gap() {
    let columns = vec![PkColumn::new("id").unwrap()];
    let change = |seq| MirrorChange {
        table_oid: 1,
        scope_key: None,
        pk_json: LogicalPk::from_json_object(&json!({"id": seq}), &columns)
            .unwrap()
            .to_canonical_json(),
        operation: MirrorOperation::Update,
        seq: SeqId::new(seq).unwrap(),
        changed_at: Utc.timestamp_opt(seq, 0).unwrap(),
        deleted: false,
        row_image_json: None,
        source: ChangeSource::HotMirror,
    };

    let changes = vec![change(3), change(2)];
    let result = changes_since(
        &changes,
        ChangeCursor {
            since_seq: 1,
            limit: 10,
        },
        Some(SeqId::new(2).unwrap()),
    )
    .unwrap();
    assert_eq!(
        result
            .iter()
            .map(|change| change.seq.get())
            .collect::<Vec<_>>(),
        vec![2, 3]
    );

    assert!(changes_since(
        &changes,
        ChangeCursor {
            since_seq: 0,
            limit: 10
        },
        Some(SeqId::new(2).unwrap())
    )
    .is_err());
}

#[test]
fn changelog_returns_latest_state_per_primary_key() {
    let columns = vec![PkColumn::new("id").unwrap()];
    let pk = LogicalPk::from_json_object(&json!({"id": 1}), &columns).unwrap();
    let change = |seq, operation, source| MirrorChange {
        table_oid: 1,
        scope_key: None,
        pk_json: pk.to_canonical_json(),
        operation,
        seq: SeqId::new(seq).unwrap(),
        changed_at: Utc.timestamp_opt(seq, 0).unwrap(),
        deleted: operation.is_delete(),
        row_image_json: Some(json!({"id": 1, "version": seq})),
        source,
    };

    let changes = vec![
        change(3, MirrorOperation::Update, ChangeSource::ColdRecord),
        change(1, MirrorOperation::Insert, ChangeSource::ColdRecord),
        change(5, MirrorOperation::Delete, ChangeSource::HotMirror),
        change(4, MirrorOperation::Update, ChangeSource::ColdRecord),
    ];
    let result = changes_since(
        &changes,
        ChangeCursor {
            since_seq: 0,
            limit: 2,
        },
        None,
    )
    .unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].seq.get(), 5);
    assert_eq!(result[0].operation, MirrorOperation::Delete);
    assert!(result[0].deleted);
}
