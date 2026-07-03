#[path = "common/mod.rs"]
mod common;

#[test]
fn migrate_existing_matrix_targets_postgresql_15_16_17() {
    let ports: Vec<u16> = common::local_pg_matrix()
        .into_iter()
        .map(|target| target.port)
        .collect();

    assert_eq!(ports, vec![5515, 5516, 5517]);
}

