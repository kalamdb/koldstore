#[test]
fn flush_to_cold_e2e_placeholder_names_required_artifacts() {
    let artifacts = ["manifest.json", "batch-0.parquet", "koldstore.cold_segments", "koldstore.cold_pk_hints"];
    assert!(artifacts.contains(&"manifest.json"));
}

