use koldstore_common::{
    ColdRow, ColumnClass, CommitSeq, HotRow, LogicalPk, MirrorOperation, MirrorState, PgCollation,
    PgTypeName, PgTypeOid, PgTypmod, PkColumn, PkOrdinal, PkValue, Predicate, PredicateClass,
    PredicateValue, PrimaryKeyColumnShape, PrimaryKeyShape, QualifiedTableName, SeqId,
    StablePkHash, TableKind, TableName,
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
    assert_eq!(table.quoted(), "\"app\".\"items\"");
    assert_eq!(TableName::parse("items").unwrap().quoted(), "\"items\"");
    assert_eq!(table.to_string(), "app.items");

    assert!(TableName::parse("").is_err());
    assert!(TableName::parse("app.items.extra").is_err());
    assert!(TableName::parse("app.items;drop table app.items").is_err());
    assert!(TableName::parse("not safe").is_err());
}

#[test]
fn qualified_table_name_exposes_parts_and_safe_quoting() {
    let table = QualifiedTableName::parse(" app.items ").unwrap();
    assert_eq!(table.schema.as_deref(), Some("app"));
    assert_eq!(table.name, "items");
    assert_eq!(table.as_table_name().unwrap().as_str(), "app.items");
    assert_eq!(table.quoted(), "\"app\".\"items\"");

    let unqualified = QualifiedTableName::parse("items").unwrap();
    assert_eq!(unqualified.schema, None);
    assert_eq!(unqualified.name, "items");
    assert_eq!(unqualified.quoted(), "\"items\"");

    assert!(QualifiedTableName::parse("app.items.extra").is_err());
    assert!(QualifiedTableName::parse("not safe").is_err());
}

#[test]
fn logical_pk_hash_is_stable_for_ordered_columns() {
    let left = pk_from_json(json!({"id": 7, "tenant_id": "a"}));
    let right = pk_from_json(json!({"tenant_id": "a", "id": 7}));

    assert_eq!(left.to_canonical_json(), json!({"tenant_id": "a", "id": 7}));
    assert_eq!(StablePkHash::compute(&left), StablePkHash::compute(&right));
}

#[test]
fn logical_pk_is_hashable_map_key_without_json_stringify() {
    use std::collections::HashMap;

    let columns = vec![PkColumn::new("id").unwrap()];
    let left =
        LogicalPk::from_json_object(&json!({"id": 7}), &columns).unwrap();
    let right =
        LogicalPk::from_json_object(&json!({"id": 7}), &columns).unwrap();
    let other =
        LogicalPk::from_json_object(&json!({"id": 8}), &columns).unwrap();

    let mut map = HashMap::new();
    map.insert(left, "seven");
    assert_eq!(map.get(&right), Some(&"seven"));
    assert!(map.get(&other).is_none());
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
fn mirror_operation_maps_to_smallint_contract_values() {
    assert_eq!(MirrorOperation::Insert.code(), 1);
    assert_eq!(MirrorOperation::Update.code(), 2);
    assert_eq!(MirrorOperation::Delete.code(), 3);

    assert_eq!(
        MirrorOperation::from_code(1).unwrap(),
        MirrorOperation::Insert
    );
    assert_eq!(
        MirrorOperation::from_code(2).unwrap(),
        MirrorOperation::Update
    );
    assert_eq!(
        MirrorOperation::from_code(3).unwrap(),
        MirrorOperation::Delete
    );
    assert!(MirrorOperation::from_code(4).is_err());
}

#[test]
fn mirror_operation_capture_metadata_is_derived_from_enum_variants() {
    assert_eq!(MirrorOperation::ALL.len(), 3);
    assert_eq!(
        MirrorOperation::Insert.capture_trigger_name("messages__cl"),
        "messages__cl_insert_capture"
    );
    assert_eq!(
        MirrorOperation::Update.capture_trigger_name("messages__cl"),
        "messages__cl_update_capture"
    );
    assert_eq!(
        MirrorOperation::Delete.capture_trigger_name("messages__cl"),
        "messages__cl_delete_capture"
    );
    assert_eq!(MirrorOperation::Insert.sql_trigger_event(), "INSERT");
    assert_eq!(MirrorOperation::Update.sql_trigger_event(), "UPDATE");
    assert_eq!(MirrorOperation::Delete.sql_trigger_event(), "DELETE");
    assert_eq!(MirrorOperation::Insert.capture_row_ref(), "NEW");
    assert_eq!(MirrorOperation::Delete.capture_row_ref(), "OLD");
}

#[test]
fn mirror_state_transitions_cover_insert_update_delete_and_reinsert() {
    let inserted = MirrorState::missing().apply(MirrorOperation::Insert);
    assert_eq!(inserted.operation(), Some(MirrorOperation::Insert));
    assert!(!inserted.is_tombstone());

    let updated = inserted.apply(MirrorOperation::Update);
    assert_eq!(updated.operation(), Some(MirrorOperation::Update));
    assert!(!updated.is_tombstone());

    let deleted = updated.apply(MirrorOperation::Delete);
    assert_eq!(deleted.operation(), Some(MirrorOperation::Delete));
    assert!(deleted.is_tombstone());

    let reinserted = deleted.apply(MirrorOperation::Insert);
    assert_eq!(reinserted.operation(), Some(MirrorOperation::Insert));
    assert!(!reinserted.is_tombstone());
}

#[test]
fn primary_key_shape_preserves_exact_column_metadata() {
    let shape = PrimaryKeyShape::new(vec![
        PrimaryKeyColumnShape::new(
            PkColumn::new("tenant_id").unwrap(),
            PkOrdinal::new(1).unwrap(),
            PgTypeOid::new(2950).unwrap(),
            PgTypeName::new("uuid").unwrap(),
            PgTypmod::new(-1),
            None,
            None,
            true,
        ),
        PrimaryKeyColumnShape::new(
            PkColumn::new("slug").unwrap(),
            PkOrdinal::new(2).unwrap(),
            PgTypeOid::new(1043).unwrap(),
            PgTypeName::new("varchar").unwrap(),
            PgTypmod::new(68),
            Some(PgCollation::new("en_US").unwrap()),
            Some(PgTypeName::new("app.slug_domain").unwrap()),
            true,
        ),
    ])
    .unwrap();

    assert_eq!(shape.columns()[0].column().as_str(), "tenant_id");
    assert_eq!(shape.columns()[1].column().as_str(), "slug");
    assert_eq!(shape.columns()[1].ordinal().get(), 2);
    assert_eq!(shape.columns()[1].type_oid().get(), 1043);
    assert_eq!(shape.columns()[1].type_name().as_str(), "varchar");
    assert_eq!(shape.columns()[1].typmod().get(), 68);
    assert_eq!(shape.columns()[1].collation().unwrap().as_str(), "en_US");
    assert_eq!(
        shape.columns()[1].domain_identity().unwrap().as_str(),
        "app.slug_domain"
    );
    assert!(shape.columns()[1].not_null());
}

#[test]
fn primary_key_shape_rejects_empty_duplicate_or_unordered_ordinals() {
    assert!(PrimaryKeyShape::new(vec![]).is_err());

    let id = PrimaryKeyColumnShape::new(
        PkColumn::new("id").unwrap(),
        PkOrdinal::new(1).unwrap(),
        PgTypeOid::new(20).unwrap(),
        PgTypeName::new("int8").unwrap(),
        PgTypmod::new(-1),
        None,
        None,
        true,
    );
    assert!(PrimaryKeyShape::new(vec![id.clone(), id]).is_err());

    let first = PrimaryKeyColumnShape::new(
        PkColumn::new("tenant_id").unwrap(),
        PkOrdinal::new(2).unwrap(),
        PgTypeOid::new(2950).unwrap(),
        PgTypeName::new("uuid").unwrap(),
        PgTypmod::new(-1),
        None,
        None,
        true,
    );
    let second = PrimaryKeyColumnShape::new(
        PkColumn::new("id").unwrap(),
        PkOrdinal::new(1).unwrap(),
        PgTypeOid::new(20).unwrap(),
        PgTypeName::new("int8").unwrap(),
        PgTypmod::new(-1),
        None,
        None,
        true,
    );
    assert!(PrimaryKeyShape::new(vec![first, second]).is_err());
}
