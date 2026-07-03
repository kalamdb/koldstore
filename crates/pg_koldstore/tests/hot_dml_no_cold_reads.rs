use pg_koldstore::observability::ObjectStoreReadCounter;

#[test]
fn hot_dml_counter_proves_no_object_store_reads() {
    let counter = ObjectStoreReadCounter::default();
    counter.record_hot_dml_operation();
    counter.record_hot_dml_operation();

    assert_eq!(counter.reads(), 0);

    counter.record_object_store_read();
    assert_eq!(counter.reads(), 1);
}
