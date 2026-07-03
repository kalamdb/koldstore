use koldstore_core::{CommitSeq, LogicalPk, PkColumn, RowOperation, ScopeKey, SeqId};
use pg_koldstore::sql::events;
use serde_json::json;

fn pk(id: i64) -> LogicalPk {
    LogicalPk::from_json_object(&json!({"id": id}), &[PkColumn::new("id").unwrap()]).unwrap()
}

fn event(
    table_oid: u32,
    scope: Option<&str>,
    id: i64,
    seq: i64,
    commit_seq: i64,
) -> koldstore_core::RowEvent {
    events::append_row_event(
        table_oid,
        scope.map(|scope| ScopeKey::new(scope).unwrap()),
        &pk(id),
        RowOperation::Update,
        SeqId::new(seq).unwrap(),
        CommitSeq::new(commit_seq).unwrap(),
        None,
    )
}

#[test]
fn changes_since_filters_table_scope_and_orders_by_commit_seq() {
    let later = pg_koldstore::sql::events::append_row_event(
        42,
        Some(ScopeKey::new("user-a").unwrap()),
        &pk(1),
        RowOperation::Update,
        SeqId::new(2).unwrap(),
        CommitSeq::new(20).unwrap(),
        None,
    );
    let earlier = pg_koldstore::sql::events::append_row_event(
        42,
        Some(ScopeKey::new("user-a").unwrap()),
        &pk(1),
        RowOperation::Insert,
        SeqId::new(100).unwrap(),
        CommitSeq::new(10).unwrap(),
        None,
    );
    let other_scope = event(42, Some("user-b"), 1, 3, 15);
    let other_table = event(99, Some("user-a"), 1, 4, 17);

    let changes = events::changes_since(
        &[later, earlier, other_scope, other_table],
        42,
        Some(&ScopeKey::new("user-a").unwrap()),
        0,
        Some(10),
        None,
    )
    .unwrap();

    assert_eq!(changes[0].commit_seq.get(), 10);
    assert_eq!(changes[1].commit_seq.get(), 20);
    assert_eq!(changes.len(), 2);
}

#[test]
fn changes_since_applies_default_limit_validates_limit_and_reports_gap() {
    let events = [event(42, None, 1, 1, 10)];

    let defaulted =
        pg_koldstore::sql::events::changes_since(&events, 42, None, 0, None, None).unwrap();
    assert_eq!(defaulted.len(), 1);

    let invalid =
        pg_koldstore::sql::events::changes_since(&events, 42, None, 0, Some(0), None).unwrap_err();
    assert_eq!(invalid.to_string(), "limit_rows must be positive");

    let gap = pg_koldstore::sql::events::changes_since(
        &events,
        42,
        None,
        5,
        Some(10),
        Some(CommitSeq::new(10).unwrap()),
    )
    .unwrap_err();
    assert_eq!(
        gap.to_string(),
        "change events before commit sequence 10 are no longer retained"
    );
}
