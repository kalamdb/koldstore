#[pg_test]
fn flush_scheduler_tick_enqueues_and_flushes_when_over_hot_limit() {
    let suffix = unique_suffix("flush_sched");
    let schema = format!("pgtest_{suffix}");
    let table = "msgs";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    Spi::run(&format!(
        r#"
        SELECT koldstore.manage_table(
          table_name => '{relation}'::regclass,
          storage => '{storage}',
          hot_row_limit => 5,
          min_flush_rows => 1,
          max_rows_per_file => 1000,
          auto_flush => true
        )
        "#
    ))
    .expect("manage_table");

    for i in 1..=10 {
        Spi::run(&format!(
            "INSERT INTO {relation} (id, body) VALUES ({i}, 'b{i}')"
        ))
        .expect("insert");
    }

    let ran = Spi::get_one::<bool>("SELECT koldstore.internal_run_flush_scheduler_tick()")
        .expect("scheduler tick")
        .expect("non-null");
    assert!(ran, "scheduler should flush when over hot_row_limit");

    let completed = spi_get_i64(&format!(
        r#"
        SELECT count(*)::bigint
        FROM koldstore.jobs
        WHERE table_oid = '{relation}'::regclass::oid
          AND job_type = 'flush'
          AND status = 'completed'
        "#
    ));
    assert!(
        completed >= 1,
        "expected at least one completed flush job, got {completed}"
    );
}

#[pg_test]
fn flush_scheduler_skips_auto_flush_disabled_tables() {
    let suffix = unique_suffix("flush_optout");
    let schema = format!("pgtest_{suffix}");
    let table = "msgs";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    Spi::run(&format!(
        r#"
        SELECT koldstore.manage_table(
          table_name => '{relation}'::regclass,
          storage => '{storage}',
          hot_row_limit => 5,
          min_flush_rows => 1,
          max_rows_per_file => 1000,
          auto_flush => false
        )
        "#
    ))
    .expect("manage_table");

    for i in 1..=10 {
        Spi::run(&format!(
            "INSERT INTO {relation} (id, body) VALUES ({i}, 'b{i}')"
        ))
        .expect("insert");
    }

    let ran = Spi::get_one::<bool>("SELECT koldstore.internal_run_flush_scheduler_tick()")
        .expect("scheduler tick")
        .expect("non-null");
    assert!(!ran, "scheduler must skip auto_flush=false tables");

    // Manual flush still works.
    let _ = flush_table_rows(&relation, false);
    let completed = spi_get_i64(&format!(
        r#"
        SELECT count(*)::bigint
        FROM koldstore.jobs
        WHERE table_oid = '{relation}'::regclass::oid
          AND job_type = 'flush'
          AND status = 'completed'
        "#
    ));
    assert!(completed >= 1);

    Spi::run(&format!(
        "SELECT koldstore.set_table_auto_flush('{relation}'::regclass, true)"
    ))
    .expect("set_table_auto_flush true");
    let enabled = Spi::get_one::<bool>(&format!(
        r#"
        SELECT COALESCE((options->>'auto_flush')::boolean, true)
        FROM koldstore.schemas
        WHERE table_oid = '{relation}'::regclass::oid AND active
        "#
    ))
    .expect("read auto_flush")
    .expect("non-null");
    assert!(enabled, "set_table_auto_flush(true) clears the opt-out");

    Spi::run(&format!(
        "SELECT koldstore.set_table_auto_flush('{relation}'::regclass, false)"
    ))
    .expect("set_table_auto_flush false");
    let disabled = Spi::get_one::<bool>(&format!(
        r#"
        SELECT COALESCE((options->>'auto_flush')::boolean, true)
        FROM koldstore.schemas
        WHERE table_oid = '{relation}'::regclass::oid AND active
        "#
    ))
    .expect("read auto_flush false")
    .expect("non-null");
    assert!(!disabled, "set_table_auto_flush(false) must persist");
}

#[pg_test]
fn flush_scheduler_skips_table_with_recent_error_job() {
    let suffix = unique_suffix("flush_cooldown");
    let schema = format!("pgtest_{suffix}");
    let table = "msgs";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    Spi::run(&format!(
        r#"
        SELECT koldstore.manage_table(
          table_name => '{relation}'::regclass,
          storage => '{storage}',
          hot_row_limit => 5,
          min_flush_rows => 1,
          max_rows_per_file => 1000,
          auto_flush => true
        )
        "#
    ))
    .expect("manage_table");

    for i in 1..=10 {
        Spi::run(&format!(
            "INSERT INTO {relation} (id, body) VALUES ({i}, 'b{i}')"
        ))
        .expect("insert");
    }

    Spi::run(&format!(
        r#"
        INSERT INTO koldstore.jobs (
          id, table_oid, scope_key, job_type, status, phase, updated_at
        ) VALUES (
          gen_random_uuid(),
          '{relation}'::regclass::oid,
          '',
          'flush',
          'error',
          'failed',
          now()
        )
        "#
    ))
    .expect("seed recent error job");

    let ran = Spi::get_one::<bool>("SELECT koldstore.internal_run_flush_scheduler_tick()")
        .expect("scheduler tick")
        .expect("non-null");
    assert!(!ran, "recent error job must cool down auto-flush for 60s");

    let completed = spi_get_i64(&format!(
        r#"
        SELECT count(*)::bigint
        FROM koldstore.jobs
        WHERE table_oid = '{relation}'::regclass::oid
          AND job_type = 'flush'
          AND status = 'completed'
        "#
    ));
    assert_eq!(completed, 0, "cooldown must not complete a flush");
}

#[pg_test]
fn flush_scheduler_retries_after_error_cooldown() {
    let suffix = unique_suffix("flush_cd_ok");
    let schema = format!("pgtest_{suffix}");
    let table = "msgs";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    Spi::run(&format!(
        r#"
        SELECT koldstore.manage_table(
          table_name => '{relation}'::regclass,
          storage => '{storage}',
          hot_row_limit => 5,
          min_flush_rows => 1,
          max_rows_per_file => 1000,
          auto_flush => true
        )
        "#
    ))
    .expect("manage_table");

    for i in 1..=10 {
        Spi::run(&format!(
            "INSERT INTO {relation} (id, body) VALUES ({i}, 'b{i}')"
        ))
        .expect("insert");
    }

    Spi::run(&format!(
        r#"
        INSERT INTO koldstore.jobs (
          id, table_oid, scope_key, job_type, status, phase, updated_at
        ) VALUES (
          gen_random_uuid(),
          '{relation}'::regclass::oid,
          '',
          'flush',
          'error',
          'failed',
          now() - interval '61 seconds'
        )
        "#
    ))
    .expect("seed cooled-down error job");

    let ran = Spi::get_one::<bool>("SELECT koldstore.internal_run_flush_scheduler_tick()")
        .expect("scheduler tick")
        .expect("non-null");
    assert!(ran, "cooled-down error must allow another flush");

    let completed = spi_get_i64(&format!(
        r#"
        SELECT count(*)::bigint
        FROM koldstore.jobs
        WHERE table_oid = '{relation}'::regclass::oid
          AND job_type = 'flush'
          AND status = 'completed'
        "#
    ));
    assert!(completed >= 1);
}

#[pg_test]
fn flush_scheduler_tick_processes_only_one_due_table() {
    let suffix = unique_suffix("flush_one");
    let schema = format!("pgtest_{suffix}");
    let storage = register_temp_storage(&suffix);
    let a = format!("{schema}.a");
    let b = format!("{schema}.b");

    create_messages_table(&schema, "a");
    create_messages_table(&schema, "b");

    for relation in [&a, &b] {
        Spi::run(&format!(
            r#"
            SELECT koldstore.manage_table(
              table_name => '{relation}'::regclass,
              storage => '{storage}',
              hot_row_limit => 5,
              min_flush_rows => 1,
              max_rows_per_file => 1000,
              auto_flush => true
            )
            "#
        ))
        .expect("manage_table");
        for i in 1..=10 {
            Spi::run(&format!(
                "INSERT INTO {relation} (id, body) VALUES ({i}, 'b{i}')"
            ))
            .expect("insert");
        }
    }

    let completed_both = || {
        spi_get_i64(&format!(
            r#"
            SELECT count(*)::bigint FROM koldstore.jobs
            WHERE table_oid IN ('{a}'::regclass::oid, '{b}'::regclass::oid)
              AND job_type = 'flush'
              AND status = 'completed'
            "#
        ))
    };

    let ran = Spi::get_one::<bool>("SELECT koldstore.internal_run_flush_scheduler_tick()")
        .expect("first tick")
        .expect("non-null");
    assert!(ran, "first tick should flush one table");
    assert_eq!(completed_both(), 1, "exactly one completed flush after first tick");

    let ran = Spi::get_one::<bool>("SELECT koldstore.internal_run_flush_scheduler_tick()")
        .expect("second tick")
        .expect("non-null");
    assert!(ran, "second tick should flush the remaining due table");
    assert_eq!(completed_both(), 2, "second tick completes exactly one more flush");
}
