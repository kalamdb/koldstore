use koldstore_catalog::{
    ColdPkHint, FkPolicyDecision, HintKind, ManagedTableMeta, PkLookup, SchemaColumn,
    SchemaRegistryEntry, SegmentVisibility, TypeMatrix,
};
use koldstore_core::TableKind;
use uuid::Uuid;

#[test]
fn type_matrix_reports_supported_and_unsupported_types() {
    let matrix = TypeMatrix::postgres_15_default();

    assert!(matrix.support_for("int8").supported);
    assert!(matrix.support_for("text").supported);
    let unsupported = matrix.support_for("tsvector");
    assert!(!unsupported.supported);
    assert!(unsupported
        .diagnostic
        .unwrap()
        .contains("unsupported PostgreSQL type"));
}

#[test]
fn schema_registry_validation_requires_pk_and_system_columns() {
    let entry = SchemaRegistryEntry {
        id: Uuid::new_v4(),
        table_oid: 42,
        version: 1,
        columns: vec![
            SchemaColumn::app("id", "int8", false),
            SchemaColumn::system("_seq", "int8"),
            SchemaColumn::system("_commit_seq", "int8"),
            SchemaColumn::system("_deleted", "bool"),
        ],
    };

    entry.validate(&["id"]).unwrap();
    assert!(entry.validate(&[]).is_err());
    assert!(entry.validate(&["missing"]).is_err());
}

#[test]
fn managed_table_meta_enforces_user_scope_column() {
    let shared = ManagedTableMeta {
        table_oid: 1,
        table_kind: TableKind::Shared,
        scope_column: None,
        flush_policy: None,
    };
    shared.validate().unwrap();

    let user_missing_scope = ManagedTableMeta {
        table_oid: 2,
        table_kind: TableKind::User,
        scope_column: None,
        flush_policy: None,
    };
    assert!(user_missing_scope.validate().is_err());
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
