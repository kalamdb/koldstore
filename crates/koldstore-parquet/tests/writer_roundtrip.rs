use koldstore_parquet::{ParquetSegmentWriter, WriterOptions};

#[test]
fn writer_plan_records_kalamdb_compatible_layout_metadata() {
    let writer = ParquetSegmentWriter::new(WriterOptions::default());
    let plan = writer.plan_segment("app/items", 7, 1, 10, 11, 20);

    assert_eq!(plan.object_path, "app/items/batch-7.parquet");
    assert_eq!(plan.min_seq, 1);
    assert_eq!(plan.max_seq, 10);
    assert_eq!(plan.min_commit_seq, 11);
    assert_eq!(plan.max_commit_seq, 20);
    assert_eq!(plan.compression, "snappy");
}
