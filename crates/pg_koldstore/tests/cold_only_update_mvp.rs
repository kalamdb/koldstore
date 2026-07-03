#[test]
fn standard_sql_cold_only_update_is_documented_as_out_of_mvp() {
    use koldstore_core::TableName;
    use pg_koldstore::sql::dml::{plan_standard_sql_cold_only_update, ColdUpdateOutcome};

    let spec = include_str!("../../../specs/001-pg-kalam-hot-cold-storage/spec.md");
    let request = pg_koldstore::sql::dml::UpdateRowRequest {
        table_name: TableName::parse("app.items").unwrap(),
        pk_json: serde_json::json!({"id": 1}),
        patch_json: serde_json::json!({"title": "new"}),
        lookup_cold: false,
    };

    assert!(spec.contains("Standard SQL cold-only UPDATE: affects 0 rows in MVP"));
    assert!(!request.cold_lookup_allowed());
    assert_eq!(
        plan_standard_sql_cold_only_update(&request),
        ColdUpdateOutcome::NoOpColdLookupDisabled
    );
}
