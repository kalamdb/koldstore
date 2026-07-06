#[test]
fn sql_exposes_hydrate_pk_api() {
    use koldstore_common::TableName;
    use koldstore_merge::dml::{plan_hydrate_pk, HydratePkRequest};

    let contract =
        include_str!("../../../specs/001-pg-kalam-hot-cold-storage/contracts/sql-api.md");
    let request = HydratePkRequest {
        table_name: TableName::parse("app.items").unwrap(),
        pk_json: serde_json::json!({"id": 1}),
    };

    assert!(koldstore_merge::dml::COLD_DML_FUNCTIONS.contains(&"koldstore.hydrate_pk"));
    assert!(contract.contains("hydrate_pk"));
    assert!(contract.contains("reads cold only for the requested PK"));

    let result = plan_hydrate_pk(&request, true);
    assert_eq!(result.affected_rows, 1);
    assert!(result.cold_lookup_performed);
    assert!(!result.tombstone_written);
}
