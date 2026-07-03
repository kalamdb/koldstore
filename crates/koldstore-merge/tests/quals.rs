use koldstore_core::{ColumnClass, Predicate, PredicateClass, PredicateValue};
use koldstore_merge::classify_predicates;
use serde_json::json;

#[test]
fn classify_predicates_splits_safe_residual_and_security() {
    let predicates = vec![
        Predicate {
            column: "id".to_string(),
            class: ColumnClass::PrimaryKey,
            value: PredicateValue::Eq(json!(1)),
        },
        Predicate {
            column: "status".to_string(),
            class: ColumnClass::Mutable,
            value: PredicateValue::Eq(json!("open")),
        },
        Predicate {
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
}
