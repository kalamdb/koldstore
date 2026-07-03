#[path = "common/mod.rs"]
mod common;

#[test]
fn user_scope_matrix_targets_postgresql_15_16_17() {
    assert_eq!(common::local_pg_matrix().map(|target| target.version), [15, 16, 17]);
}

