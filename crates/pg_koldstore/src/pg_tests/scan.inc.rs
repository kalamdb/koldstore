#[pg_test]
fn explain_shows_kold_merge_scan_for_managed_table() {
    let suffix = unique_suffix("explain");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    manage_shared(&relation, &storage);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (1, 'hot')"
    ))
    .expect("insert");

    let plan = spi_get_explain(&format!("EXPLAIN SELECT * FROM {relation}"));
    assert!(
        plan.contains("KoldMergeScan") || plan.contains("Custom Scan"),
        "expected custom merge scan in EXPLAIN: {plan}"
    );
    assert!(
        plan.contains("Candidate segments")
            || plan.contains("Segments pruned by min/max")
            || plan.contains("Parquet segments opened"),
        "expected Timescale-style prune properties in EXPLAIN: {plan}"
    );
}

#[pg_test]
fn explain_analyze_shows_prune_summary_after_flush() {
    let suffix = unique_suffix("explain_prune");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    manage_shared(&relation, &storage);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (1, 'a'), (2, 'b'), (3, 'c')"
    ))
    .expect("insert");
    let flushed = flush_table_rows(&relation, true);
    assert!(flushed >= 1, "expected flush to publish cold rows");

    let plan = spi_get_explain(&format!(
        "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) SELECT body FROM {relation} WHERE id = 2"
    ));
    assert!(
        plan.contains("KoldMergeScan") || plan.contains("Custom Scan"),
        "expected custom merge scan: {plan}"
    );
    for needle in [
        "Emit path",
        "Hot rows",
        "Candidate segments",
        "Segments pruned by scope",
        "Segments pruned by min/max",
        "Parquet segments opened",
        "Bytes fetched",
    ] {
        assert!(
            plan.contains(needle),
            "EXPLAIN ANALYZE missing `{needle}`: {plan}"
        );
    }
}

#[pg_test]
fn hot_only_and_mixed_hot_cold_results_match_expected_values() {
    let suffix = unique_suffix("scan");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    manage_shared(&relation, &storage);

    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (1, 'hot-a'), (2, 'hot-b')"
    ))
    .expect("insert hot");

    let hot_only = spi_get_text(&format!(
        "SELECT string_agg(body, ',' ORDER BY id) FROM {relation}"
    ));
    assert_eq!(hot_only, "hot-a,hot-b");

    let flushed = flush_table_rows(&relation, true);
    assert!(flushed >= 2, "expected at least two rows flushed, got {flushed}");

    // After flush with hot_row_limit high, rows may remain hot or move cold depending
    // on policy; either way the logical result must stay identical.
    let after_flush = spi_get_text(&format!(
        "SELECT string_agg(body, ',' ORDER BY id) FROM {relation}"
    ));
    assert_eq!(hot_only, after_flush);

    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (3, 'hot-c')"
    ))
    .expect("insert post-flush hot");

    let mixed = spi_get_text(&format!(
        "SELECT string_agg(body, ',' ORDER BY id) FROM {relation}"
    ));
    assert_eq!(mixed, "hot-a,hot-b,hot-c");
    assert_eq!(
        spi_get_i64(&format!("SELECT count(*)::bigint FROM {relation}")),
        3
    );
}

#[pg_test]
fn prepared_statement_repeated_execution_returns_stable_values() {
    let suffix = unique_suffix("prep");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    manage_shared(&relation, &storage);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (1, 'one'), (2, 'two')"
    ))
    .expect("insert");

    Spi::run(&format!(
        "PREPARE ks_prep_{suffix} AS SELECT body FROM {relation} WHERE id = $1"
    ))
    .expect("prepare");

    let first = spi_get_text(&format!("EXECUTE ks_prep_{suffix}(1)"));
    let second = spi_get_text(&format!("EXECUTE ks_prep_{suffix}(1)"));
    let third = spi_get_text(&format!("EXECUTE ks_prep_{suffix}(2)"));
    assert_eq!(first, "one");
    assert_eq!(second, "one");
    assert_eq!(third, "two");
}

#[pg_test]
fn merge_scan_spi_connection_is_closed_after_decode_error() {
    let error = unsafe {
        crate::merge_scan::pg::execute_mirror_overlay_query_for_test(
            "SELECT 'not-json'::text AS pk_json, 3::smallint AS op",
        )
    }
    .expect_err("invalid mirror JSON must fail decoding");
    assert!(error.contains("mirror overlay pk JSON"), "{error}");

    let finish = unsafe { pgrx::pg_sys::SPI_finish() };
    assert_eq!(finish, pgrx::pg_sys::SPI_ERROR_UNCONNECTED);
}

#[pg_test]
fn merge_scan_hook_state_is_restored_after_postgres_error() {
    pgrx::PgTryBuilder::new(|| {
        crate::merge_scan::pg::with_hook_disabled(|| {
            pgrx::error!("forced merge-scan hook cleanup test error");
        });
    })
    .catch_others(|_| ())
    .execute();

    assert!(!crate::merge_scan::pg::hook_is_disabled_for_test());
}
