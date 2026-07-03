#[path = "common/mod.rs"]
mod common;

#[test]
fn greenfield_matrix_targets_postgresql_15_16_17() {
    let versions: Vec<u16> = common::local_pg_matrix()
        .into_iter()
        .map(|target| target.version)
        .collect();

    assert_eq!(versions, vec![15, 16, 17]);
}

