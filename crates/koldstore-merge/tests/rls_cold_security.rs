use koldstore_common::{ColumnClass, Predicate, PredicateValue};
use koldstore_merge::rls;
use serde_json::json;

#[test]
fn unsupported_cold_rls_fails_closed() {
    assert!(rls::enforce_or_fail_closed(false).is_err());
    assert!(rls::enforce_or_fail_closed(true).is_ok());
}

#[test]
fn security_quals_add_required_columns_or_fail_closed() {
    let qual = Predicate {
        column: "tenant_id".to_string(),
        class: ColumnClass::Security,
        value: PredicateValue::Eq(json!("user-a")),
    };

    let plan = rls::plan_security_quals(&[qual], &["id".to_string()]).unwrap();
    assert!(plan.can_enforce);
    assert_eq!(
        plan.required_projection,
        vec!["id".to_string(), "tenant_id".to_string()]
    );

    let unsupported = Predicate {
        column: "tenant_id".to_string(),
        class: ColumnClass::Security,
        value: PredicateValue::Expression("current_user = owner".to_string()),
    };
    let err = rls::plan_security_quals(&[unsupported], &["id".to_string()]).unwrap_err();
    assert_eq!(err, rls::unsupported_rls_error());
}
