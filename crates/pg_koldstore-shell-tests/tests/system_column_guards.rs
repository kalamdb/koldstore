#[test]
fn clean_schema_records_metadata_in_mirror_not_user_table() {
    use koldstore_parquet::ColdMetadataColumn;

    assert_eq!(
        koldstore::hooks::executor::managed_dml_hook_names(),
        ["INSERT", "UPDATE", "DELETE", "COPY"]
    );
    assert_eq!(ColdMetadataColumn::Seq.name(), "seq");
    assert_eq!(ColdMetadataColumn::Deleted.name(), "deleted");
    for legacy in ["_seq", "_commit_seq", "_deleted", "_user_id"] {
        assert_ne!(ColdMetadataColumn::Seq.name(), legacy);
    }
}
