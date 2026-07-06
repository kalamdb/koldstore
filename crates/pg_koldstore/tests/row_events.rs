use pg_koldstore::sql::events;

#[test]
fn row_events_catalog_is_not_required_by_clean_schema_default() {
    use koldstore_core::{
        PgTypeName, PgTypeOid, PgTypmod, PkColumn, PkOrdinal, PrimaryKeyColumnShape,
    };
    use pg_koldstore::migrate::{mirror::plan_change_log_mirror_from_columns, QualifiedTableName};

    let sql = include_str!("../sql/koldstore--0.1.0.sql");
    assert!(!sql.contains("CREATE TABLE IF NOT EXISTS koldstore.row_events"));
    assert_eq!(events::DEFAULT_CHANGE_LIMIT, 1000);

    let source = QualifiedTableName::parse("public.messages").unwrap();
    let pk = PrimaryKeyColumnShape::new(
        PkColumn::new("id").unwrap(),
        PkOrdinal::new(1).unwrap(),
        PgTypeOid::new(20).unwrap(),
        PgTypeName::new("bigint").unwrap(),
        PgTypmod::new(-1),
        None,
        None,
        true,
    );
    let mirror = plan_change_log_mirror_from_columns(&source, &[pk]).unwrap();

    assert!(mirror
        .create_table
        .sql
        .contains("CREATE TABLE IF NOT EXISTS \"koldstore\".\"messages__cl\""));
    assert!(mirror.create_table.sql.contains("\"op\" smallint NOT NULL"));
    assert!(mirror.create_table.sql.contains("PRIMARY KEY (\"id\")"));
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
