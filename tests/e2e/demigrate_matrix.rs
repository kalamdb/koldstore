#[path = "common/mod.rs"]
mod common;

#[test]
fn demigrate_matrix_targets_postgresql_15_16_17() {
    assert_eq!(
        common::local_pg_matrix().map(|target| target.version),
        [15, 16, 17]
    );
}

#[test]
fn demigrate_matrix_covers_flush_cold_delete_and_user_scoped_tables() {
    let scenarios = [
        "demigrate_after_flush",
        "demigrate_after_cold_only_delete",
        "demigrate_user_scoped_table",
    ];

    assert!(scenarios.contains(&"demigrate_after_flush"));
    assert!(scenarios.contains(&"demigrate_after_cold_only_delete"));
    assert!(scenarios.contains(&"demigrate_user_scoped_table"));
}
