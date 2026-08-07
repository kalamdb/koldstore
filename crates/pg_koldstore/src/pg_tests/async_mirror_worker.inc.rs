#[pg_test]
fn async_manage_schedules_database_maintenance() {
    // #[pg_test] runs the body inside one SQL-function transaction. Provision
    // the logical slot before CREATE TABLE assigns the parent transaction an XID.
    preprovision_async_mirror();

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
          hot_row_limit => 1000
        )
        "#
    ))
    .expect("manage_table async");

    // The new architecture has no permanent per-database worker to "ensure".
    // This compatibility call only publishes a coalesced maintenance request;
    // it must never block on or register a dynamic worker from this backend.
    let requested = Spi::get_one::<bool>("SELECT koldstore.internal_ensure_async_mirror_worker()")
        .expect("request maintenance")
        .expect("non-null");
    assert!(requested, "maintenance request should be accepted while enabled");

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
fn async_worker_guc_off_skips_maintenance_request() {
    Spi::run("SET koldstore.internal_async_mirror_worker = off").expect("set guc");
    let requested = Spi::get_one::<bool>("SELECT koldstore.internal_ensure_async_mirror_worker()")
        .expect("request with guc off")
        .expect("non-null");
    assert!(
        !requested,
        "maintenance request must be a no-op when the worker GUC is off"
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
}
