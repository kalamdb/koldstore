#[pg_test]
fn async_manage_starts_database_worker() {
    // #[pg_test] runs the whole body inside one SQL-function transaction.
    // Logical slot creation waits for concurrent XIDs, so provision before any
    // SPI write (CREATE TABLE) or the provisioner deadlocks with this backend.
    //
    // The WAL applier finishes database connect only after this transaction
    // commits, so we cannot assert pg_stat_activity visibility here. E2E tests
    // cover post-commit visibility; this test covers registration + cleanup.
    let database_oid = unsafe { pgrx::pg_sys::MyDatabaseId }.to_u32();
    crate::async_mirror::provision::provision_infrastructure(database_oid)
        .expect("pre-provision async slot/publication");

    let suffix = unique_suffix("async_worker");
    let schema = format!("pgtest_{suffix}");
    let table = "events";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    Spi::run(&format!(
        r#"
        SELECT koldstore.manage_table(
          table_name => '{relation}'::regclass,
          storage => '{storage}',
          hot_row_limit => 1000,
          mirror_capture_mode => 'async'
        )
        "#
    ))
    .expect("manage_table async");

    // manage_table required the applier (wait_for_startup). A second ensure in
    // this same open transaction must not try to register another worker.
    let ensured_again = Spi::get_one::<bool>("SELECT koldstore.internal_ensure_async_mirror_worker()")
        .expect("second ensure")
        .expect("non-null");
    assert!(
        !ensured_again,
        "second ensure must be an idempotent no-op after manage registered the applier"
    );

    Spi::run(&format!(
        "SELECT koldstore.unmanage_table('{relation}'::regclass, true, true)"
    ))
    .expect("unmanage");
    assert!(
        Spi::get_one::<bool>("SELECT koldstore.disable_async_mirror()")
            .expect("disable")
            .unwrap_or(false),
        "disable must clean up async infrastructure"
    );
}

#[pg_test]
fn async_worker_guc_off_skips_registration() {
    Spi::run("SET koldstore.internal_async_mirror_worker = off").expect("set guc");
    let registered = Spi::get_one::<bool>("SELECT koldstore.internal_ensure_async_mirror_worker()")
        .expect("ensure with guc off")
        .expect("non-null");
    assert!(
        !registered,
        "ensure must be a no-op when the worker GUC is off"
    );
    Spi::run("RESET koldstore.internal_async_mirror_worker").expect("reset guc");
}

#[pg_test]
fn async_retained_wal_health_status_is_exposed() {
    let status = Spi::get_one::<pgrx::JsonB>("SELECT koldstore.async_mirror_status()")
        .expect("async_mirror_status spi")
        .expect("non-null status");
    assert!(
        status.0.get("retention").and_then(|v| v.get("ok")).is_some(),
        "status must expose retention.ok; got {}",
        status.0
    );
    assert!(
        status.0.get("admission").and_then(|v| v.get("ok")).is_some(),
        "status must preserve the admission.ok compatibility alias; got {}",
        status.0
    );
}
