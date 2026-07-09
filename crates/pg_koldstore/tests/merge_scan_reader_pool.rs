#[test]
fn parquet_reader_pool_uses_bounded_global_slots() {
    use pg_koldstore::merge_scan::reader_pool::{
        parquet_reader_lock_key, validated_max_open_parquet_readers, READER_LOCK_NAMESPACE,
    };

    assert_eq!(validated_max_open_parquet_readers(0), 1);
    assert_eq!(validated_max_open_parquet_readers(32), 32);
    assert_eq!(validated_max_open_parquet_readers(10_000), 1024);
    assert_eq!(parquet_reader_lock_key(0).0, READER_LOCK_NAMESPACE);
    assert_ne!(parquet_reader_lock_key(0), parquet_reader_lock_key(1));
}
