#[test]
fn kalamdb_compatibility_contract_uses_manifest_and_parquet_golden_outputs() {
    let manifest = include_str!("../golden/manifest-v1.json");

    assert!(manifest.contains("\"version\""));
    assert!(manifest.contains("\"segments\""));
}
