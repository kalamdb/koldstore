#[path = "common/mod.rs"]
mod common;

#[test]
fn quickstart_matrix_covers_all_documented_scenarios() {
    let quickstart = include_str!("../../specs/001-pg-kalam-hot-cold-storage/quickstart.md");
    let scenario_count = quickstart.matches("## Scenario ").count();

    assert!(scenario_count >= 10);
    assert_eq!(
        common::local_pg_matrix()
            .into_iter()
            .map(|target| target.version)
            .collect::<Vec<_>>(),
        common::expected_pg_versions()
    );
}
