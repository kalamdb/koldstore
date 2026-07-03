use pg_koldstore::sql::events;

#[test]
fn row_event_sql_contract_contains_insert_update_delete_revive_ops() {
    use koldstore_core::{CommitSeq, LogicalPk, PkColumn, RowOperation, SeqId};

    let sql = include_str!("../../../sql/koldstore--0.1.0.sql");
    for op in ["insert", "update", "delete", "revive"] {
        assert!(sql.contains(op));
    }
    assert!(sql.contains("CREATE TABLE IF NOT EXISTS koldstore.row_events"));
    assert_eq!(events::DEFAULT_CHANGE_LIMIT, 1000);

    let pk = LogicalPk::from_json_object(
        &serde_json::json!({"id": 42}),
        &[PkColumn::new("id").unwrap()],
    )
    .unwrap();
    let event = events::append_cold_only_tombstone_event(
        42,
        None,
        &pk,
        SeqId::new(10).unwrap(),
        CommitSeq::new(20).unwrap(),
    );

    assert_eq!(event.op, RowOperation::Delete);
    assert!(event.deleted);
    assert_eq!(event.pk_json, serde_json::json!({"id": 42}));
    assert!(event.row_image_json.is_none());
}

#[test]
fn row_events_preserve_scope_payload_and_operation_flags() {
    use koldstore_core::{CommitSeq, LogicalPk, PkColumn, RowOperation, ScopeKey, SeqId};

    let pk = LogicalPk::from_json_object(
        &serde_json::json!({"id": 42}),
        &[PkColumn::new("id").unwrap()],
    )
    .unwrap();
    let payload = serde_json::json!({"id": 42, "body": "hello"});
    let event = events::append_row_event(
        42,
        Some(ScopeKey::new("user-a").unwrap()),
        &pk,
        RowOperation::Revive,
        SeqId::new(11).unwrap(),
        CommitSeq::new(21).unwrap(),
        Some(payload.clone()),
    );

    assert_eq!(event.scope_key.unwrap().as_str(), "user-a");
    assert_eq!(event.op, RowOperation::Revive);
    assert!(!event.deleted);
    assert_eq!(event.row_image_json, Some(payload));
}

#[test]
fn row_event_retention_purges_old_events_and_tracks_oldest_remaining_commit_seq() {
    use koldstore_core::{CommitSeq, LogicalPk, PkColumn, RowOperation, SeqId};

    let pk = LogicalPk::from_json_object(
        &serde_json::json!({"id": 42}),
        &[PkColumn::new("id").unwrap()],
    )
    .unwrap();
    let event = |commit_seq| {
        events::append_row_event(
            42,
            None,
            &pk,
            RowOperation::Update,
            SeqId::new(commit_seq).unwrap(),
            CommitSeq::new(commit_seq).unwrap(),
            None,
        )
    };

    let retained = events::purge_retained_events(&[event(10), event(11), event(12)], 42, None, 2);

    assert_eq!(
        retained
            .iter()
            .map(|event| event.commit_seq.get())
            .collect::<Vec<_>>(),
        vec![11, 12]
    );
    assert_eq!(
        events::oldest_retained_commit_seq(&retained).unwrap().get(),
        11
    );
}
