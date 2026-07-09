#[test]
fn parquet_reader_pool_uses_per_backend_fail_fast_limit() {
    use pg_koldstore::merge_scan::reader_pool::{
        open_reader_count, try_acquire_parquet_reader_permit, validated_max_open_parquet_readers,
    };

    assert_eq!(validated_max_open_parquet_readers(0), 1);
    assert_eq!(validated_max_open_parquet_readers(32), 32);
    assert_eq!(validated_max_open_parquet_readers(10_000), 1024);

    assert_eq!(open_reader_count(), 0);
    let first = try_acquire_parquet_reader_permit(1).expect("first permit");
    assert_eq!(open_reader_count(), 1);
    assert!(try_acquire_parquet_reader_permit(1).is_err());
    drop(first);
    assert_eq!(open_reader_count(), 0);
    let _second = try_acquire_parquet_reader_permit(1).expect("permit after release");
}
