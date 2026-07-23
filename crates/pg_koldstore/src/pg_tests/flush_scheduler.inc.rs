#[pg_test]
fn flush_job_records_segment_batches_completed() {
    let suffix = unique_suffix("flush_batches");
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
          hot_row_limit => 1,
          min_flush_rows => 1,
          max_rows_per_file => 1000,
          auto_flush => false
        )
        "#
    ))
    .expect("manage_table");

    // 2501 rows with hot_row_limit=1 => flush 2500 rows; max_rows_per_file=1000 => 3 segments.
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) SELECT gs, 'b' || gs FROM generate_series(1, 2501) AS gs"
    ))
    .expect("insert");

    let job_id = spi_get_text(&format!(
        "SELECT koldstore.flush_table('{relation}'::regclass, force => false)::text"
    ));
    let batches = spi_get_i64(&format!(
        "SELECT batches_completed::bigint FROM koldstore.jobs WHERE id = '{job_id}'::uuid"
    ));
    let rows = spi_get_i64(&format!(
        "SELECT rows_flushed FROM koldstore.jobs WHERE id = '{job_id}'::uuid"
    ));
    let segments = spi_get_i64(&format!(
        r#"
        SELECT count(*)::bigint
        FROM koldstore.cold_segments
        WHERE table_oid = '{relation}'::regclass::oid
          AND status IN ('pending', 'active')
        "#
    ));

    assert!(
        rows >= 2500,
        "expected at least 2500 rows flushed, got {rows}"
    );
    assert!(
        batches >= 3,
        "expected batches_completed to match Parquet segments (>=3), got {batches}"
    );
    assert_eq!(
        batches, segments,
        "batches_completed ({batches}) must equal catalog segments ({segments})"
    );
}

#[pg_test]
fn flush_table_drains_multiple_policy_waves_in_one_job() {
    // Policy waves are capped at max_rows_per_flush (default 10k). A single
    // flush_table call must keep draining until under hot_row_limit so catch-up
    // does not wait for many scheduler ticks / job rows.
    let suffix = unique_suffix("flush_multiwave");
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
          hot_row_limit => 1,
          min_flush_rows => 1,
          max_rows_per_file => 1000,
          auto_flush => false
        )
        "#
    ))
    .expect("manage_table");

    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) SELECT gs, 'b' || gs FROM generate_series(1, 15001) AS gs"
    ))
    .expect("insert");

    let job_id = spi_get_text(&format!(
        "SELECT koldstore.flush_table('{relation}'::regclass, force => false)::text"
    ));
    let jobs = spi_get_i64(&format!(
        r#"
        SELECT count(*)::bigint
        FROM koldstore.jobs
        WHERE table_oid = '{relation}'::regclass::oid
          AND job_type = 'flush'
          AND status = 'completed'
        "#
    ));
    let rows = spi_get_i64(&format!(
        "SELECT rows_flushed FROM koldstore.jobs WHERE id = '{job_id}'::uuid"
    ));
    let batches = spi_get_i64(&format!(
        "SELECT batches_completed::bigint FROM koldstore.jobs WHERE id = '{job_id}'::uuid"
    ));
    let hot = spi_get_i64(&format!(
        "SELECT (koldstore.describe_table(table_name => '{relation}'::regclass)->>'hot_rows')::bigint"
    ));

    assert_eq!(jobs, 1, "expected one completed catch-up job, got {jobs}");
    assert!(
        rows >= 15000,
        "expected one job to drain past the 10k wave cap, got rows_flushed={rows}"
    );
    assert!(
        batches >= 15,
        "expected many segment batches from multi-wave catch-up, got {batches}"
    );
    assert!(
        hot <= 1,
        "expected hot rows at hot_row_limit after catch-up, got {hot}"
    );
    // Post-flush cache free: footer metadata must not survive finalize.
    assert!(
        koldstore_parquet::parquet_footer_cache::is_empty(),
        "flush must clear parquet footer cache when the job finishes"
    );
}

#[pg_test]
fn flush_scheduler_skips_table_with_running_flush_job() {
    // A durable `running` flush means another backend owns the work. The next
    // tick must ignore that table instead of waiting or starting a second job.
    let suffix = unique_suffix("flush_skip_running");
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
          id, table_oid, scope_key, job_type, status, phase, payload
        ) VALUES (
          gen_random_uuid(),
          '{relation}'::regclass::oid,
          '',
          'flush',
          'running',
          'writing',
          '{{"force":false}}'::jsonb
        )
        "#
    ))
    .expect("insert running flush job");

    let ran = Spi::get_one::<bool>("SELECT koldstore.internal_run_flush_scheduler_tick()")
        .expect("scheduler tick")
        .expect("non-null");
    assert!(
        !ran,
        "scheduler must skip while a flush job is already running"
    );

    let completed = spi_get_i64(&format!(
        r#"
        SELECT count(*)::bigint
        FROM koldstore.jobs
        WHERE table_oid = '{relation}'::regclass::oid
          AND job_type = 'flush'
          AND status = 'completed'
        "#
    ));
    assert_eq!(
        completed, 0,
        "skipped tick must not create a completed flush job"
    );
}

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
