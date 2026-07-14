use koldstore::hooks::executor;

#[test]
fn simple_pk_delete_detection_requires_exact_metadata() {
    assert!(executor::simple_pk_delete_supported(true, true));
    assert!(!executor::simple_pk_delete_supported(true, false));
    assert!(!executor::simple_pk_delete_supported(false, true));

    let predicate = executor::SimplePkPredicate::new("id", serde_json::json!(42));
    let extracted = executor::extract_simple_pk_delete_predicate(
        std::slice::from_ref(&predicate),
        &["id".to_string()],
        true,
    );

    assert_eq!(extracted, Some(predicate.clone()));
    assert_eq!(
        executor::extract_simple_pk_delete_predicate(&[predicate], &["id".to_string()], false),
        None
    );
}
