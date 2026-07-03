use pg_koldstore::sql::events;

#[test]
fn row_event_sql_contract_contains_insert_update_delete_revive_ops() {
    let sql = include_str!("../../../sql/koldstore--0.1.0.sql");
    for op in ["insert", "update", "delete", "revive"] {
        assert!(sql.contains(op));
    }
    assert_eq!(events::DEFAULT_CHANGE_LIMIT, 1000);
}
