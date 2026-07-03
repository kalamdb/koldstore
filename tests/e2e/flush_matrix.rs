#[path = "common/mod.rs"]
mod common;

#[test]
fn flush_matrix_targets_postgresql_15_16_17() {
    assert_eq!(common::local_pg_matrix().len(), 3);
}

