#[test]
fn copy_and_dump_contract_documents_supported_paths() {
    let docs = include_str!("../../../docs/backup-and-operations.md");

    assert!(docs.contains("COPY FROM"));
    assert!(docs.contains("COPY (SELECT ...) TO"));
    assert!(docs.contains("pg_dump"));
    assert!(docs.contains("object-store"));
}
