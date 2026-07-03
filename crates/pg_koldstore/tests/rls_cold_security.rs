use pg_koldstore::security::rls;

#[test]
fn unsupported_cold_rls_fails_closed() {
    assert!(rls::enforce_or_fail_closed(false).is_err());
    assert!(rls::enforce_or_fail_closed(true).is_ok());
}
