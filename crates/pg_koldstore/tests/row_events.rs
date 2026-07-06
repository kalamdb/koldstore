#[test]
fn row_events_catalog_is_not_required_by_clean_schema_default() {
    let sql = include_str!("../sql/koldstore--0.1.0.sql");

    assert!(!sql.contains("CREATE TABLE IF NOT EXISTS koldstore.row_events"));
    assert!(!sql.contains("INSERT INTO koldstore.row_events"));
    assert!(!sql.contains("FROM koldstore.row_events"));
}

#[test]
fn clean_schema_change_feed_contract_mentions_mirrors_not_row_events() {
    let sql_api = include_str!("../../../specs/002-clean-schema-change-log/contracts/sql-api.md");

    assert!(sql_api
        .contains("Read unflushed latest-state changes from the table-specific change-log mirror"));
    assert!(sql_api.contains("Do not require or read `koldstore.row_events`"));
}
