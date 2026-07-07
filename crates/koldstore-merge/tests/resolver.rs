use koldstore_common::{
    ChangeSource, ColdRow, CommitSeq, HotRow, LogicalPk, MirrorChange, MirrorOperation, PkColumn,
    ScopeKey, SeqId,
};
use koldstore_merge::{
    changes_since, resolve_rows, tombstone_required, ChangeCursor, TombstoneDecision,
};
use serde_json::json;

fn pk(id: i64) -> LogicalPk {
    let columns = vec![PkColumn::new("id").unwrap()];
    LogicalPk::from_json_object(&json!({"id": id}), &columns).unwrap()
}

fn hot(id: i64, seq: i64, commit_seq: i64, deleted: bool, body: &str) -> HotRow {
    HotRow {
        pk: pk(id),
        scope_key: None,
        seq: SeqId::new(seq).unwrap(),
        commit_seq: CommitSeq::new(commit_seq).unwrap(),
        deleted,
        row_image: json!({"id": id, "body": body}),
    }
}

fn cold(id: i64, seq: i64, commit_seq: i64, deleted: bool, body: &str) -> ColdRow {
    ColdRow {
        pk: pk(id),
        scope_key: None,
        seq: SeqId::new(seq).unwrap(),
        commit_seq: CommitSeq::new(commit_seq).unwrap(),
        deleted,
        schema_version: 1,
        row_image: json!({"id": id, "body": body}),
    }
}

#[test]
fn resolver_selects_newest_row_per_pk_and_hot_wins_exact_tie() {
    let rows = resolve_rows(
        &[hot(1, 10, 10, false, "hot"), hot(2, 5, 5, false, "hot-2")],
        &[cold(1, 9, 9, false, "old"), cold(2, 5, 5, false, "cold-2")],
    );

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].source, koldstore_merge::RowSource::Hot);
    assert_eq!(rows[0].seq.get(), 10);
    assert_eq!(rows[0].commit_seq.get(), 10);
    assert_eq!(rows[0].row_image, json!({"id": 1, "body": "hot"}));
    assert_eq!(rows[1].source, koldstore_merge::RowSource::Hot);
    assert_eq!(rows[1].seq.get(), 5);
    assert_eq!(rows[1].row_image, json!({"id": 2, "body": "hot-2"}));
}

#[test]
fn resolver_emits_at_most_one_visible_winner_per_pk() {
    let rows = resolve_rows(
        &[hot(1, 12, 12, false, "hot"), hot(2, 1, 1, false, "hot-2")],
        &[
            cold(1, 10, 10, false, "old-1"),
            cold(1, 11, 11, false, "newer-cold-1"),
            cold(2, 2, 2, false, "cold-2"),
        ],
    );

    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows.iter()
            .map(|row| row.pk_json.clone())
            .collect::<Vec<_>>(),
        vec![json!({"id": 1}), json!({"id": 2})]
    );
    assert_eq!(rows[0].row_image, json!({"id": 1, "body": "hot"}));
    assert_eq!(rows[1].row_image, json!({"id": 2, "body": "cold-2"}));
}

#[test]
fn resolver_masks_deleted_winners() {
    let rows = resolve_rows(
        &[hot(1, 11, 11, true, "deleted")],
        &[cold(1, 10, 10, false, "old")],
    );

    assert!(rows.is_empty());
}

#[test]
fn cold_delete_marker_masks_older_live_rows_and_newer_hot_reinsert_wins() {
    let deleted = ColdRow {
        pk: pk(1),
        scope_key: None,
        seq: SeqId::new(20).unwrap(),
        commit_seq: CommitSeq::new(20).unwrap(),
        deleted: true,
        schema_version: 1,
        row_image: json!({"id": 1}),
    };
    let old_live = cold(1, 10, 10, false, "old-cold");

    assert!(resolve_rows(&[], &[old_live.clone(), deleted.clone()]).is_empty());

    let rows = resolve_rows(&[hot(1, 30, 30, false, "reinserted")], &[old_live, deleted]);

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].source, koldstore_merge::RowSource::Hot);
    assert_eq!(rows[0].seq.get(), 30);
    assert_eq!(rows[0].row_image, json!({"id": 1, "body": "reinserted"}));
}

#[test]
fn tombstone_required_only_when_cold_may_contain_pk() {
    assert_eq!(tombstone_required(true), TombstoneDecision::KeepTombstone);
    assert_eq!(tombstone_required(false), TombstoneDecision::PhysicalDelete);
}

#[test]
fn changes_since_orders_by_seq_and_detects_retention_gap() {
    let change = |seq| MirrorChange {
        table_oid: 1,
        scope_key: Some(ScopeKey::new("a").unwrap()),
        pk_json: serde_json::json!({"id": seq}),
        operation: MirrorOperation::Update,
        seq: SeqId::new(seq).unwrap(),
        deleted: false,
        row_image_json: None,
        source: ChangeSource::HotMirror,
    };

    let changes = vec![change(5), change(3), change(4)];
    let selected = changes_since(
        &changes,
        ChangeCursor {
            since_seq: 3,
            limit: 10,
        },
        Some(SeqId::new(3).unwrap()),
    )
    .unwrap();

    assert_eq!(
        selected
            .iter()
            .map(|change| change.seq.get())
            .collect::<Vec<_>>(),
        vec![4, 5]
    );
    assert!(changes_since(
        &changes,
        ChangeCursor {
            since_seq: 1,
            limit: 10,
        },
        Some(SeqId::new(4).unwrap()),
    )
    .is_err());
}
