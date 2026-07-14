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

    let plan = spi_get_text(&format!("EXPLAIN SELECT * FROM {relation}"));
    assert!(
        plan.contains("KoldMergeScan") || plan.contains("Custom Scan"),
        "expected custom merge scan in EXPLAIN: {plan}"
    );
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
