use koldstore_common::{
    ChangeSource, ColdRow, HotRow, LogicalPk, MirrorChange, MirrorOperation, PkColumn, RowImage,
    ScopeKey, SeqId,
};
use koldstore_merge::{
    changes_since, resolve_rows, tombstone_required, ChangeCursor, NewestFirstWinnerResolver,
    TombstoneDecision,
};
use serde_json::json;

fn pk(id: i64) -> LogicalPk {
    let columns = vec![PkColumn::new("id").unwrap()];
    LogicalPk::from_json_object(&json!({"id": id}), &columns).unwrap()
}

fn hot(id: i64, seq: i64, deleted: bool, body: &str) -> HotRow {
    HotRow {
        pk: pk(id),
        scope_key: None,
        seq: SeqId::new(seq).unwrap(),
        deleted,
        row_image: RowImage::from_json_value(json!({"id": id, "body": body})),
    }
}

fn cold(id: i64, seq: i64, deleted: bool, body: &str) -> ColdRow {
    ColdRow {
        pk: pk(id),
        scope_key: None,
        seq: SeqId::new(seq).unwrap(),
        deleted,
        schema_version: 1,
        row_image: RowImage::from_json_value(json!({"id": id, "body": body})),
    }
}

#[test]
fn streaming_resolver_preserves_hot_batch_encounter_order() {
    let winners = NewestFirstWinnerResolver::default()
        .resolve_hot_batch(vec![
            hot(9, 30, false, "hot-9"),
            hot(8, 30, false, "hot-8"),
            hot(7, 30, false, "hot-7"),
            hot(1, 30, false, "hot-1"),
        ])
        .expect("hot batch");
    let bodies = winners
        .iter()
        .map(|row| row.row_image["body"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(bodies, vec!["hot-9", "hot-8", "hot-7", "hot-1"]);
}

#[test]
fn resolver_selects_newest_row_per_pk_and_hot_wins_exact_tie() {
    let rows = resolve_rows(
        &[hot(1, 10, false, "hot"), hot(2, 5, false, "hot-2")],
        &[cold(1, 9, false, "old"), cold(2, 5, false, "cold-2")],
    );

    assert_eq!(rows.len(), 2);
    let by_id: std::collections::BTreeMap<_, _> = rows
        .into_iter()
        .map(|row| (row.pk_json["id"].as_i64().unwrap(), row))
        .collect();

    let row1 = by_id.get(&1).expect("pk 1");
    assert_eq!(row1.source, koldstore_merge::RowSource::Hot);
    assert_eq!(row1.seq.get(), 10);
    assert_eq!(row1.row_image.to_json(), json!({"id": 1, "body": "hot"}));

    let row2 = by_id.get(&2).expect("pk 2");
    assert_eq!(row2.source, koldstore_merge::RowSource::Hot);
    assert_eq!(row2.seq.get(), 5);
    assert_eq!(row2.row_image.to_json(), json!({"id": 2, "body": "hot-2"}));
}

#[test]
fn resolver_emits_at_most_one_visible_winner_per_pk() {
    let rows = resolve_rows(
        &[hot(1, 12, false, "hot"), hot(2, 1, false, "hot-2")],
        &[
            cold(1, 10, false, "old-1"),
            cold(1, 11, false, "newer-cold-1"),
            cold(2, 2, false, "cold-2"),
        ],
    );

    assert_eq!(rows.len(), 2);
    let by_id: std::collections::BTreeMap<_, _> = rows
        .into_iter()
        .map(|row| (row.pk_json["id"].as_i64().unwrap(), row))
        .collect();
    assert_eq!(by_id.keys().copied().collect::<Vec<_>>(), vec![1_i64, 2]);
    assert_eq!(
        by_id.get(&1).unwrap().row_image.to_json(),
        json!({"id": 1, "body": "hot"})
    );
    assert_eq!(
        by_id.get(&2).unwrap().row_image.to_json(),
        json!({"id": 2, "body": "cold-2"})
    );
}

#[test]
fn resolver_masks_deleted_winners() {
    let rows = resolve_rows(&[hot(1, 11, true, "deleted")], &[cold(1, 10, false, "old")]);

    assert!(rows.is_empty());
}

#[test]
fn streaming_resolver_keeps_newest_winner_across_payload_batches() {
    let mut resolver = NewestFirstWinnerResolver::new([pk(4)]);

    let hot_rows = resolver
        .resolve_hot_batch(vec![hot(1, 30, false, "hot")])
        .unwrap();
    assert_eq!(hot_rows.len(), 1);
    assert_eq!(hot_rows[0].row_image["body"].as_str(), Some("hot"));

    let newer_cold = resolver
        .resolve_cold_batch(vec![
            cold(1, 20, false, "shadowed-by-hot"),
            cold(2, 20, false, "newer-cold"),
            cold(3, 19, true, "deleted"),
            cold(4, 18, false, "masked-by-mirror"),
        ])
        .unwrap();
    assert_eq!(newer_cold.len(), 1);
    assert_eq!(newer_cold[0].row_image["body"].as_str(), Some("newer-cold"));

    let older_cold = resolver
        .resolve_cold_batch(vec![
            cold(2, 10, false, "older-duplicate"),
            cold(3, 9, false, "older-before-delete"),
            cold(5, 8, false, "old-only"),
        ])
        .unwrap();
    assert_eq!(older_cold.len(), 1);
    assert_eq!(older_cold[0].row_image["body"].as_str(), Some("old-only"));
    assert_eq!(resolver.seen_key_count(), 5);
}

#[test]
fn streaming_resolver_resolves_duplicates_inside_one_overlapping_batch() {
    let mut resolver = NewestFirstWinnerResolver::default();

    let rows = resolver
        .resolve_cold_batch(vec![
            cold(1, 10, false, "old"),
            cold(1, 20, false, "new"),
            cold(2, 30, false, "live"),
            cold(2, 31, true, "deleted"),
        ])
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_image["body"].as_str(), Some("new"));
    assert_eq!(resolver.seen_key_count(), 2);
}

#[test]
fn streaming_resolver_mirror_mask_applies_after_live_hot_winners() {
    let mut resolver = NewestFirstWinnerResolver::default();

    let hot_rows = resolver
        .resolve_hot_batch(vec![hot(4, 30, false, "live-hot")])
        .unwrap();
    resolver.mask_older_pks([pk(4)]).unwrap();
    let cold_rows = resolver
        .resolve_cold_batch(vec![cold(4, 20, false, "stale-cold")])
        .unwrap();

    assert_eq!(hot_rows.len(), 1);
    assert_eq!(hot_rows[0].row_image["body"].as_str(), Some("live-hot"));
    assert!(cold_rows.is_empty());
}

#[test]
fn streaming_resolver_fails_closed_when_seen_key_limit_is_exceeded() {
    let mut resolver = NewestFirstWinnerResolver::default().with_max_seen_keys(Some(2));

    let first = resolver
        .resolve_cold_batch(vec![cold(1, 10, false, "one"), cold(2, 10, false, "two")])
        .unwrap();
    assert_eq!(first.len(), 2);

    let exceeded = resolver
        .resolve_cold_batch(vec![cold(3, 9, false, "three")])
        .expect_err("third distinct key must fail closed");
    assert_eq!(exceeded.limit, 2);
    assert_eq!(exceeded.seen, 2);
    assert_eq!(resolver.seen_key_count(), 2);
}

#[test]
fn cold_delete_marker_masks_older_live_rows_and_newer_hot_reinsert_wins() {
    let deleted = ColdRow {
        pk: pk(1),
        scope_key: None,
        seq: SeqId::new(20).unwrap(),
        deleted: true,
        schema_version: 1,
        row_image: RowImage::from_json_value(json!({"id": 1})),
    };
    let old_live = cold(1, 10, false, "old-cold");

    assert!(resolve_rows(&[], &[old_live.clone(), deleted.clone()]).is_empty());

    let rows = resolve_rows(&[hot(1, 30, false, "reinserted")], &[old_live, deleted]);

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].source, koldstore_merge::RowSource::Hot);
    assert_eq!(rows[0].seq.get(), 30);
    assert_eq!(rows[0].row_image.to_json(), json!({"id": 1, "body": "reinserted"}));
}

#[test]
fn tombstone_required_only_when_cold_may_contain_pk() {
    assert_eq!(tombstone_required(true), TombstoneDecision::KeepTombstone);
    assert_eq!(tombstone_required(false), TombstoneDecision::PhysicalDelete);
}

#[test]
fn changes_since_orders_by_seq_and_detects_retention_gap() {
    let change = |seq| MirrorChange {
        table_oid: koldstore_common::TableOid::from_raw(1),
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
