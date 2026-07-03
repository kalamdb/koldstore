use koldstore_core::{ColdRow, CommitSeq, HotRow, LogicalPk, PkColumn, SeqId};
use pg_koldstore::merge_scan::exec::execute_merge_scan;
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

fn cold(id: i64, seq: i64, commit_seq: i64, body: &str) -> ColdRow {
    ColdRow {
        pk: pk(id),
        scope_key: None,
        seq: SeqId::new(seq).unwrap(),
        commit_seq: CommitSeq::new(commit_seq).unwrap(),
        deleted: false,
        schema_version: 1,
        row_image: json!({"id": id, "body": body}),
    }
}

#[test]
fn merge_scan_results_resolve_hot_winner_and_tombstone_masking() {
    let result = execute_merge_scan(
        vec![
            hot(1, 20, 20, false, "hot-winner"),
            hot(2, 21, 21, true, "deleted"),
        ],
        vec![
            cold(1, 10, 10, "older-cold"),
            cold(2, 10, 10, "masked-cold"),
        ],
    )
    .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0].row_image,
        json!({"id": 1, "body": "hot-winner"})
    );
    assert_eq!(result.hot_rows_seen, 2);
    assert_eq!(result.cold_rows_seen, 2);
    assert_eq!(result.tombstones_masked, 1);
}
