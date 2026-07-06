use koldstore_catalog::{
    ColdPkHint, FkPolicyDecision, HintKind, ManagedTableMeta, PkLookup, SegmentVisibility,
};
use koldstore_common::{
    PgTypeName, PgTypeOid, PgTypmod, PkColumn, PkOrdinal, PrimaryKeyColumnShape, PrimaryKeyShape,
    TableKind,
};
use koldstore_schema::MirrorInitializationState;
use uuid::Uuid;

fn pk_shape() -> PrimaryKeyShape {
    PrimaryKeyShape::new(vec![PrimaryKeyColumnShape::new(
        PkColumn::new("id").unwrap(),
        PkOrdinal::new(1).unwrap(),
        PgTypeOid::new(20).unwrap(),
        PgTypeName::new("int8").unwrap(),
        PgTypmod::new(-1),
        None,
        None,
        true,
    )])
    .unwrap()
}

#[test]
fn managed_table_meta_enforces_user_scope_column() {
    let shared = ManagedTableMeta {
        table_oid: 1,
        table_kind: TableKind::Shared,
        scope_column: None,
        mirror_relation: Some("koldstore.items__cl".to_string()),
        primary_key_shape: Some(pk_shape()),
        initialization_state: MirrorInitializationState::Complete,
        flush_policy: None,
        schema_version: 1,
    };
    shared.validate().unwrap();

    let user_missing_scope = ManagedTableMeta {
        table_oid: 2,
        table_kind: TableKind::User,
        scope_column: None,
        mirror_relation: Some("koldstore.notes__cl".to_string()),
        primary_key_shape: Some(pk_shape()),
        initialization_state: MirrorInitializationState::Complete,
        flush_policy: None,
        schema_version: 1,
    };
    assert!(user_missing_scope.validate().is_err());

    let complete_without_pk_shape = ManagedTableMeta {
        table_oid: 3,
        table_kind: TableKind::Shared,
        scope_column: None,
        mirror_relation: Some("koldstore.no_shape__cl".to_string()),
        primary_key_shape: None,
        initialization_state: MirrorInitializationState::Complete,
        flush_policy: None,
        schema_version: 1,
    };
    assert!(complete_without_pk_shape.validate().is_err());
}

#[test]
fn fk_policy_rejects_risky_flush_tables_unless_operator_accepts_hot_only_semantics() {
    assert_eq!(
        FkPolicyDecision::classify(true, false, true, false),
        FkPolicyDecision::Reject
    );
    assert_eq!(
        FkPolicyDecision::classify(false, true, true, true),
        FkPolicyDecision::AllowHotOnly
    );
    assert_eq!(
        FkPolicyDecision::classify(true, true, false, false),
        FkPolicyDecision::Allow
    );
}

#[test]
fn pk_lookup_prefers_exact_hints_over_may_contain_hints() {
    let exact = ColdPkHint {
        table_oid: 1,
        scope_key: None,
        pk_hash: "abc".to_string(),
        segment_id: Uuid::new_v4(),
        hint_kind: HintKind::Exact,
        latest_seq: 10,
        latest_commit_seq: 20,
    };
    let bloom = ColdPkHint {
        hint_kind: HintKind::Bloom,
        latest_seq: 8,
        latest_commit_seq: 18,
        ..exact.clone()
    };

    assert_eq!(
        ColdPkHint::lookup("abc", &[bloom.clone(), exact.clone()]),
        PkLookup::Exact(exact)
    );
    let may_contain = ColdPkHint::lookup("abc", std::slice::from_ref(&bloom));
    assert!(may_contain.can_write_idempotent_tombstone(true));
    assert!(!may_contain.can_preserve_exact_rowcount());
    assert_eq!(ColdPkHint::lookup("missing", &[bloom]), PkLookup::Absent);
}

#[test]
fn active_segment_visibility_only_includes_active_segments() {
    assert!(SegmentVisibility::Active.is_query_visible());
    assert!(!SegmentVisibility::Pending.is_query_visible());
    assert!(!SegmentVisibility::Deleted.is_query_visible());
}
