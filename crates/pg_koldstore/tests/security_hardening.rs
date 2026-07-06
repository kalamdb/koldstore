#[test]
fn security_hardening_contract_covers_credentials_gucs_rls_and_sql_safety() {
    let sql = include_str!("../sql/koldstore--0.1.0.sql");
    let privileges = include_str!("../src/privileges.rs");
    let rls = include_str!("../../koldstore-merge/src/rls.rs");

    assert!(sql.contains("REVOKE ALL ON"));
    assert!(sql.contains("koldstore.storage"));
    assert!(sql.contains("FROM PUBLIC"));
    assert!(privileges.contains("internal_system_write"));
    assert!(rls.contains("fail closed") || rls.contains("fail-closed"));
    assert!(!sql.contains("EXECUTE format"));
    assert!(!sql.contains("TO PROGRAM"));
}
