use koldstore_core::{
    ColdRow, ColumnClass, CommitSeq, HotRow, LogicalPk, PkColumn, PkValue, Predicate,
    PredicateClass, PredicateValue, RowOperation, SeqId, StablePkHash, TableKind, TableName,
};
use serde_json::json;

fn pk_from_json(value: serde_json::Value) -> LogicalPk {
    let columns = vec![
        PkColumn::new("tenant_id").unwrap(),
        PkColumn::new("id").unwrap(),
    ];
    LogicalPk::from_json_object(&value, &columns).unwrap()
}

#[test]
fn sequence_newtypes_reject_zero_and_negative_values() {
    assert!(SeqId::new(0).is_err());
    assert!(SeqId::new(-1).is_err());
    assert_eq!(SeqId::new(42).unwrap().get(), 42);

    assert!(CommitSeq::new(0).is_err());
    assert!(CommitSeq::new(-5).is_err());
    assert_eq!(CommitSeq::new(100).unwrap().get(), 100);
}

#[test]
fn table_kind_parses_contract_names() {
    assert_eq!("shared".parse::<TableKind>().unwrap(), TableKind::Shared);
    assert_eq!("user".parse::<TableKind>().unwrap(), TableKind::User);
    assert!("table_am".parse::<TableKind>().is_err());
}

#[test]
fn table_name_newtype_normalizes_and_rejects_unsafe_names() {
    let table = TableName::parse(" app.items ").unwrap();
    assert_eq!(table.as_str(), "app.items");
    assert_eq!(table.schema(), Some("app"));
    assert_eq!(table.relation(), "items");
    assert_eq!(table.to_string(), "app.items");

    assert!(TableName::parse("").is_err());
    assert!(TableName::parse("app.items.extra").is_err());
    assert!(TableName::parse("app.items;drop table app.items").is_err());
    assert!(TableName::parse("not safe").is_err());
}

#[test]
fn logical_pk_hash_is_stable_for_ordered_columns() {
    let left = pk_from_json(json!({"id": 7, "tenant_id": "a"}));
    let right = pk_from_json(json!({"tenant_id": "a", "id": 7}));

    assert_eq!(left.to_canonical_json(), json!({"tenant_id": "a", "id": 7}));
    assert_eq!(StablePkHash::compute(&left), StablePkHash::compute(&right));
}

#[test]
fn logical_pk_rejects_missing_null_and_duplicate_columns() {
    let columns = vec![PkColumn::new("id").unwrap()];
    assert!(LogicalPk::from_json_object(&json!({}), &columns).is_err());
    assert!(LogicalPk::from_json_object(&json!({"id": null}), &columns).is_err());
    assert!(LogicalPk::new(vec![
        (
            PkColumn::new("id").unwrap(),
            PkValue::new(json!(1)).unwrap()
        ),
        (
            PkColumn::new("id").unwrap(),
            PkValue::new(json!(2)).unwrap()
        ),
    ])
    .is_err());
}

#[test]
fn predicate_classification_keeps_mutable_columns_residual() {
    let pk_predicate = Predicate {
        column: "id".to_string(),
        class: ColumnClass::PrimaryKey,
        value: PredicateValue::Eq(json!(1)),
    };
    let mutable_predicate = Predicate {
        column: "status".to_string(),
        class: ColumnClass::Mutable,
        value: PredicateValue::Eq(json!("open")),
    };
    let security_predicate = Predicate {
        column: "tenant_id".to_string(),
        class: ColumnClass::Security,
        value: PredicateValue::Eq(json!("tenant-a")),
    };

    assert_eq!(pk_predicate.classify().unwrap(), PredicateClass::SafePrune);
    assert_eq!(
        mutable_predicate.classify().unwrap(),
        PredicateClass::Residual
    );
    assert_eq!(
        security_predicate.classify().unwrap(),
        PredicateClass::Security
    );
}

#[test]
fn hot_tombstone_preserves_pk_scope_and_versions() {
    let pk = pk_from_json(json!({"tenant_id": "a", "id": 1}));
    let hot = HotRow {
        pk: pk.clone(),
        scope_key: Some("a".parse().unwrap()),
        seq: SeqId::new(11).unwrap(),
        commit_seq: CommitSeq::new(22).unwrap(),
        deleted: true,
        row_image: json!({"tenant_id": "a", "id": 1}),
    };

    let tombstone = hot.into_tombstone();

    assert_eq!(tombstone.pk, pk);
    assert_eq!(tombstone.scope_key.unwrap().as_str(), "a");
    assert_eq!(tombstone.seq.get(), 11);
    assert_eq!(tombstone.commit_seq.get(), 22);
}

#[test]
fn cold_row_model_preserves_schema_version_and_delete_marker() {
    let cold = ColdRow {
        pk: pk_from_json(json!({"tenant_id": "a", "id": 1})),
        scope_key: Some("a".parse().unwrap()),
        seq: SeqId::new(1).unwrap(),
        commit_seq: CommitSeq::new(1).unwrap(),
        deleted: false,
        schema_version: 3,
        row_image: json!({"body": "cold"}),
    };

    assert_eq!(cold.schema_version, 3);
    assert!(!cold.deleted);
}

#[test]
fn row_operation_serializes_contract_names() {
    assert_eq!(
        serde_json::to_value(RowOperation::Insert).unwrap(),
        json!("insert")
    );
    assert_eq!(
        serde_json::to_value(RowOperation::Revive).unwrap(),
        json!("revive")
    );
}
