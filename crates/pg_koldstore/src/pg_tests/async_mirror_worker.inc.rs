#[pg_test]
fn async_manage_starts_database_worker() {
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

    assert!(
        Spi::get_one::<bool>("SELECT koldstore.internal_ensure_async_mirror_worker()")
            .expect("ensure")
            .unwrap_or(false)
            || spi_get_i64(
                "SELECT count(*)::bigint FROM pg_catalog.pg_stat_activity \
                 WHERE backend_type = 'koldstore async mirror ' \
                   || (SELECT oid::text FROM pg_catalog.pg_database \
                       WHERE datname = current_database())"
            ) >= 1,
        "async manage_table must leave a database worker running"
    );

    let worker_count = spi_get_i64(
        "SELECT count(*)::bigint FROM pg_catalog.pg_stat_activity \
         WHERE backend_type = 'koldstore async mirror ' \
           || (SELECT oid::text FROM pg_catalog.pg_database \
               WHERE datname = current_database())",
    );
    assert!(worker_count >= 1, "worker must be visible in pg_stat_activity");

    let ensured_again = Spi::get_one::<bool>("SELECT koldstore.internal_ensure_async_mirror_worker()")
        .expect("second ensure")
        .expect("non-null");
    assert!(
        !ensured_again,
        "second ensure must be an idempotent no-op while the worker is running"
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
