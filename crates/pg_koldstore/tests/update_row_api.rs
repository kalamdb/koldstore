#[test]
fn sql_exposes_update_row_lookup_cold_api() {
    use koldstore_common::TableName;
    use koldstore_merge::dml::{plan_update_row, ColdUpdateOutcome};

    let request = koldstore_merge::dml::UpdateRowRequest {
        table_name: TableName::parse("app.items").unwrap(),
        pk_json: serde_json::json!({"id": 1}),
        patch_json: serde_json::json!({"title": "new"}),
        lookup_cold: true,
    };

    assert!(koldstore_merge::dml::COLD_DML_FUNCTIONS.contains(&"koldstore.update_row"));
    assert!(request.cold_lookup_allowed());

    let without_lookup = koldstore_merge::dml::UpdateRowRequest {
        lookup_cold: false,
        ..request.clone()
    };
    assert_eq!(
        plan_update_row(&without_lookup, false, true),
        ColdUpdateOutcome::NoOpColdLookupDisabled
    );
    assert_eq!(
        plan_update_row(&request, false, true),
        ColdUpdateOutcome::ColdLookupAndUpdate
    );
}
