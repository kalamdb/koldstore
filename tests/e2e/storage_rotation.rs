#[test]
fn storage_rotation_contract_keeps_existing_object_paths_stable() {
    let sql = include_str!("../../sql/koldstore--0.1.0.sql");

    assert!(sql.contains("koldstore.alter_storage_credentials"));
    assert!(sql.contains("without rewriting existing cold object paths"));
}
