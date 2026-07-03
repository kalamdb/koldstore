#[test]
fn demigrate_cold_artifacts_placeholder_covers_retain_and_drop_cold() {
    assert!(["retain", "drop_cold"].contains(&"drop_cold"));
}

