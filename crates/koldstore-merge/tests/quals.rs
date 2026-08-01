use koldstore_common::{ColumnClass, ColumnId, Predicate, PredicateClass, PredicateValue};
use koldstore_merge::{build_pruning_plan, classify_predicates};
use serde_json::json;

#[test]
fn classify_predicates_splits_safe_residual_and_security() {
    let predicates = vec![
        Predicate {
            column_id: ColumnId::from_attnum(1),
            column: "id".to_string(),
            class: ColumnClass::PrimaryKey,
            value: PredicateValue::Eq(json!(1)),
        },
        Predicate {
            column_id: ColumnId::from_attnum(2),
            column: "status".to_string(),
            class: ColumnClass::Mutable,
            value: PredicateValue::Eq(json!("open")),
        },
        Predicate {
            column_id: ColumnId::from_attnum(3),
            column: "tenant_id".to_string(),
            class: ColumnClass::Security,
            value: PredicateValue::Eq(json!("tenant-a")),
        },
    ];

    let classified = classify_predicates(&predicates).unwrap();

    assert_eq!(
        classified.safe[0].classify().unwrap(),
        PredicateClass::SafePrune
    );
    assert_eq!(
        classified.residual[0].classify().unwrap(),
        PredicateClass::Residual
    );
    assert_eq!(
        classified.security[0].classify().unwrap(),
        PredicateClass::Security
    );
    assert!(classified.requires_post_merge_filtering());
    assert_eq!(
        classified.safe_pruning_column_ids(),
        vec![ColumnId::from_attnum(1)]
    );
}

#[test]
fn mutable_app_column_filters_remain_residual_after_winner_resolution() {
    let predicates = vec![
        Predicate {
            column_id: ColumnId::from_attnum(2),
            column: "status".to_string(),
            class: ColumnClass::Mutable,
            value: PredicateValue::Eq(json!("archived")),
        },
        Predicate {
            column_id: ColumnId::from_attnum(4),
            column: "seq".to_string(),
            class: ColumnClass::Seq,
            value: PredicateValue::Range { min: 10, max: 20 },
        },
    ];

    let classified = classify_predicates(&predicates).unwrap();

    assert_eq!(classified.safe.len(), 1);
    assert_eq!(classified.safe[0].column, "seq");
    assert_eq!(classified.residual.len(), 1);
    assert_eq!(classified.residual[0].column, "status");
    assert!(classified.requires_post_merge_filtering());
}

#[test]
fn malformed_ranges_are_rejected_before_pruning() {
    let predicates = vec![Predicate {
        column_id: ColumnId::from_attnum(3),
        column: "seq".to_string(),
        class: ColumnClass::Seq,
        value: PredicateValue::Range { min: 20, max: 10 },
    }];

    assert!(classify_predicates(&predicates).is_err());
}

#[test]
fn pruning_plan_extracts_pk_scope_sequence_and_immutable_columns() {
    let predicates = vec![
        Predicate {
            column_id: ColumnId::from_attnum(1),
            column: "id".to_string(),
            class: ColumnClass::PrimaryKey,
            value: PredicateValue::Eq(json!(42)),
        },
        Predicate {
            column_id: ColumnId::from_attnum(2),
            column: "tenant_id".to_string(),
            class: ColumnClass::Scope,
            value: PredicateValue::Eq(json!("tenant-a")),
        },
        Predicate {
            column_id: ColumnId::from_attnum(3),
            column: "seq".to_string(),
            class: ColumnClass::Seq,
            value: PredicateValue::Range { min: 10, max: 20 },
        },
        Predicate {
            column_id: ColumnId::from_attnum(5),
            column: "created_at".to_string(),
            class: ColumnClass::Immutable,
            value: PredicateValue::Range { min: 100, max: 200 },
        },
    ];

    let plan = build_pruning_plan(&predicates).unwrap();

    assert_eq!(plan.pk_columns, vec![ColumnId::from_attnum(1)]);
    assert_eq!(plan.scope_columns, vec![ColumnId::from_attnum(2)]);
    assert_eq!(plan.seq_range.unwrap().column, "seq");
    assert_eq!(plan.immutable_stat_columns, vec![ColumnId::from_attnum(5)]);
    assert!(plan.residual_columns.is_empty());
}
