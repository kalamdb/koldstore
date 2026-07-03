#[test]
fn sql_exposes_operational_functions() {
    let sql = include_str!("../../../sql/koldstore--0.1.0.sql");

    for function_name in [
        "CREATE OR REPLACE FUNCTION koldstore.table_status",
        "CREATE OR REPLACE FUNCTION koldstore.backup_manifest",
        "CREATE OR REPLACE FUNCTION koldstore.validate_cold_storage",
        "CREATE OR REPLACE FUNCTION koldstore.recover_segments",
    ] {
        assert!(sql.contains(function_name), "missing {function_name}");
    }

    for status_field in [
        "hot_rows",
        "cold_segment_count",
        "manifest_state",
        "pending_jobs",
        "storage_binding",
        "last_error",
    ] {
        assert!(sql.contains(status_field), "missing {status_field}");
    }
}

#[test]
fn sql_exposes_export_import_boundary() {
    let sql = include_str!("../../../sql/koldstore--0.1.0.sql");
    assert!(sql.contains("CREATE OR REPLACE FUNCTION koldstore_exec"));
    assert!(sql.contains("EXPORT TABLE"));
    assert!(sql.contains("IMPORT TABLE"));
}
