use chrono::Utc;
use koldstore_core::{
    ColdRow, CommitSeq, HotRow, LogicalPk, PkColumn, RowEvent, RowOperation, ScopeKey, SeqId,
    StablePkHash,
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
fn tombstone_required_only_when_cold_may_contain_pk() {
    assert_eq!(tombstone_required(true), TombstoneDecision::KeepTombstone);
    assert_eq!(tombstone_required(false), TombstoneDecision::PhysicalDelete);
}

#[test]
fn changes_since_orders_by_commit_seq_and_detects_retention_gap() {
    let pk = pk(1);
    let event = |commit_seq| RowEvent {
        table_oid: 1,
        scope_key: Some(ScopeKey::new("a").unwrap()),
        pk_hash: StablePkHash::compute(&pk),
        pk_json: pk.to_canonical_json(),
        op: RowOperation::Update,
        seq: SeqId::new(commit_seq + 10).unwrap(),
        commit_seq: CommitSeq::new(commit_seq).unwrap(),
        deleted: false,
        row_image_json: None,
        created_at: Utc::now(),
    };

    let events = vec![event(5), event(3), event(4)];
    let selected = changes_since(
        &events,
        ChangeCursor {
            since_commit_seq: 3,
            limit: 10,
        },
        Some(CommitSeq::new(3).unwrap()),
    )
    .unwrap();

    assert_eq!(
        selected
            .iter()
            .map(|event| event.commit_seq.get())
            .collect::<Vec<_>>(),
        vec![4, 5]
    );
    assert!(changes_since(
        &events,
        ChangeCursor {
            since_commit_seq: 1,
            limit: 10,
        },
        Some(CommitSeq::new(4).unwrap()),
    )
    .is_err());
}
