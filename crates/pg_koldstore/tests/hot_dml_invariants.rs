use pg_koldstore::{hooks::executor, sql::dml};

#[test]
fn dml_helpers_keep_one_hot_row_per_pk_by_using_upsert_revival() {
    let insert = dml::ManagedDmlOperation::Insert;
    let revive = dml::ManagedDmlOperation::Revive;

    assert!(insert.keeps_one_hot_row_per_pk());
    assert!(revive.keeps_one_hot_row_per_pk());
    assert_eq!(
        dml::revive_tombstone_sql("app.items"),
        "UPDATE app.items SET _deleted = false WHERE _deleted = true"
    );
    assert!(executor::managed_dml_hook_names().contains(&"INSERT"));
    assert!(executor::managed_dml_hook_names().contains(&"UPDATE"));
    assert!(executor::managed_dml_hook_names().contains(&"DELETE"));
}
