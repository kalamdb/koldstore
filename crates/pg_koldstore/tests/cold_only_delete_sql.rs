use pg_koldstore::hooks::executor;

#[test]
fn simple_pk_delete_detection_requires_exact_metadata() {
    assert!(executor::simple_pk_delete_supported(true, true));
    assert!(!executor::simple_pk_delete_supported(true, false));
    assert!(!executor::simple_pk_delete_supported(false, true));
}
