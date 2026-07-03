#[test]
fn ddl_hook_documents_drop_table_cleanup_policy() {
    let ddl = include_str!("../src/hooks/ddl.rs");

    for policy in ["retain", "delete", "failed"] {
        assert!(ddl.contains(policy), "missing DROP TABLE policy {policy}");
    }

    assert!(ddl.contains("DROP TABLE"));
    assert!(ddl.contains("object artifact"));
}
