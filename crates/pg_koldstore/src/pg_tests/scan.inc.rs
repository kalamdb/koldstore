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
        plan.contains("Candidate Segments")
            || plan.contains("Segments Pruned by Min/Max")
            || plan.contains("Parquet Segments Opened")
            || plan.contains("Parquet Segments Planned"),
        "expected Timescale-style prune properties in EXPLAIN: {plan}"
    );
}

#[pg_test]
fn explain_analyze_uses_native_hot_child_counters() {
    let suffix = unique_suffix("explain_hot_child");
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

    let plan = spi_get_explain(&format!(
        "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) \
         SELECT body FROM {relation} ORDER BY id"
    ));
    for expected in [
        "Emit Path: hot_child",
        "Access Method: PostgreSQL child plan",
        "Hot Rows: 3",
        "Rows Scanned: 3",
        "Input Rows: 3",
        "Output Rows: 3",
    ] {
        assert!(
            plan.contains(expected),
            "EXPLAIN ANALYZE hot-child flow missing exact counter `{expected}`: {plan}"
        );
    }
}

#[pg_test]
fn explain_json_nests_parquet_segment_groups() {
    // Structured formats must use ExplainOpenGroup so graph clients can nest
    // cold-segment timing under the Custom Scan node. YAML keeps a text result
    // type while still exercising the same grouping APIs as JSON.
    let suffix = unique_suffix("explain_json");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    manage_for_cold_flush(&relation, &storage);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (1, 'a'), (2, 'b'), (3, 'c')"
    ))
    .expect("insert");
    let flushed = flush_table_rows(&relation, true);
    assert!(flushed >= 1, "expected flush to publish cold rows");

    let plan = spi_get_explain(&format!(
        "EXPLAIN (ANALYZE, FORMAT YAML, COSTS OFF, SUMMARY OFF) \
         SELECT body FROM {relation} WHERE id = 2"
    ));
    assert!(
        plan.contains("Emit Path"),
        "expected typed emit-path property in structured explain: {plan}"
    );
    assert!(
        plan.contains("Parquet Segments"),
        "expected nested Parquet Segments group for graph clients: {plan}"
    );
    assert!(
        plan.contains("Scan Sources") && plan.contains("Cold Scan"),
        "expected nested scan-source flow for graph clients: {plan}"
    );
    assert!(
        plan.contains("Merge"),
        "expected nested merge stage for graph clients: {plan}"
    );
    assert!(
        plan.contains("Timing"),
        "expected Timing group for graph clients: {plan}"
    );
    assert!(
        plan.contains("Cold Read Time"),
        "expected cold read timing in structured explain: {plan}"
    );
    assert!(
        plan.contains("Read Time"),
        "expected per-segment Read Time in structured explain: {plan}"
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
        "Emit Path",
        "Hot Rows",
        "Candidate Segments",
        "Segments Pruned by Scope",
        "Segments Pruned by Min/Max",
        "Parquet Segments Opened",
        "Bytes Fetched",
        "Segment Catalog Source",
    ] {
        assert!(
            plan.contains(needle),
            "EXPLAIN ANALYZE missing `{needle}`: {plan}"
        );
    }
    assert!(
        !plan.contains("Timing:") && !plan.contains("Cold Read Time"),
        "TIMING OFF must suppress custom phase timing like native PostgreSQL nodes: {plan}"
    );
}

#[pg_test]
fn explain_analyze_shows_scan_merge_flow_and_phase_timing() {
    let suffix = unique_suffix("explain_flow");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    manage_for_cold_flush(&relation, &storage);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES \
         (1, 'cold-a'), (2, 'cold-b'), (3, 'cold-c')"
    ))
    .expect("insert cold candidates");
    let flushed = flush_table_rows(&relation, true);
    assert!(flushed >= 1, "expected flush to publish cold rows");
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (4, 'hot-d')"
    ))
    .expect("insert hot row");

    let plan = spi_get_explain(&format!(
        "EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF) \
         SELECT body FROM {relation} WHERE id IN (1, 4) ORDER BY id"
    ));
    for needle in [
        "Scan Sources",
        "Hot Scan",
        "Cold Scan",
        "Mirror Scan",
        "Rows Scanned",
        "Rows Removed by Overlay",
        "Merge",
        "Strategy",
        "Input Rows",
        "Output Rows",
        "Rows Removed by Merge",
        "Timing",
        "Initialization Time",
        "Metadata Time",
        "Hot Scan Time",
        "Cold Read Time",
        "Mirror Scan Time",
        "Merge Time",
        "Materialization Time",
    ] {
        assert!(
            plan.contains(needle),
            "EXPLAIN ANALYZE flow missing `{needle}`: {plan}"
        );
    }
    for expected in [
        "Emit Path: merge_buffer",
        "Hot Rows: 1",
        "Rows Scanned: 3",
        "Input Rows: 4",
        "Output Rows: 4",
        "Rows Removed by Merge: 0",
        "Rows Removed by Filter: 2",
    ] {
        assert!(
            plan.contains(expected),
            "EXPLAIN ANALYZE flow missing exact counter `{expected}`: {plan}"
        );
    }
}

#[pg_test]
fn plain_explain_never_reuses_prior_analyze_counters() {
    let suffix = unique_suffix("explain_lifecycle");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    manage_for_cold_flush(&relation, &storage);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (1, 'a'), (2, 'b')"
    ))
    .expect("insert");
    let flushed = flush_table_rows(&relation, true);
    assert!(flushed >= 1, "expected flush to publish cold rows");

    let _analyzed = spi_get_explain(&format!(
        "EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF) SELECT body FROM {relation}"
    ));
    let planned = spi_get_explain(&format!(
        "EXPLAIN (COSTS OFF) SELECT body FROM {relation}"
    ));
    assert!(
        planned.contains("Status: planned"),
        "plain EXPLAIN must report planned source state: {planned}"
    );
    assert!(
        planned.contains("Parquet Segments Planned"),
        "plain EXPLAIN must label cold segments as planned: {planned}"
    );
    assert!(
        !planned.contains("Emit Path:")
            && !planned.contains("Rows Scanned:")
            && !planned.contains("Mirror Tombstones:")
            && !planned.contains("Parquet Segments Opened")
            && !planned.contains("Status: executed"),
        "plain EXPLAIN must not reuse prior execution counters: {planned}"
    );
}

#[pg_test]
fn explain_analyze_counts_mirror_overlay_rows() {
    let suffix = unique_suffix("explain_overlay");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    manage_for_cold_flush(&relation, &storage);
    let mirror = spi_get_text(&format!(
        "SELECT mirror_relation::text \
         FROM koldstore.schemas \
         WHERE table_oid = '{relation}'::regclass AND active"
    ));
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (1, 'a'), (2, 'b'), (3, 'c')"
    ))
    .expect("insert");
    let flushed = flush_table_rows(&relation, true);
    assert!(flushed >= 1, "expected flush to publish cold rows");

    // pg_test wraps the fixture in one transaction, so a row pruned earlier in
    // this same transaction still conflicts in the heap's unique index. Seed
    // the post-flush mirror state directly to isolate EXPLAIN's overlay metrics.
    Spi::run(&format!(
        "INSERT INTO {mirror} (id, seq, op) \
         SELECT 2, last_flush_seq + 1, 3 \
         FROM koldstore.schemas \
         WHERE table_oid = '{relation}'::regclass AND active"
    ))
    .expect("seed unflushed tombstone");
    assert_eq!(
        spi_get_i64(&format!(
            "SELECT COALESCE(max(op), -1)::bigint FROM {mirror} WHERE id = 2"
        )),
        3,
        "expected strict mirror tombstone before EXPLAIN"
    );

    let plan = spi_get_explain(&format!(
        "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) \
         SELECT body FROM {relation} ORDER BY id"
    ));
    for expected in [
        "Mirror Tombstones: 1",
        "Mirror Scan:",
        "Rows Scanned: 1",
        "Rows Removed by Overlay: 1",
        "Input Rows: 2",
        "Output Rows: 2",
    ] {
        assert!(
            plan.contains(expected),
            "EXPLAIN ANALYZE overlay missing exact counter `{expected}`: {plan}"
        );
    }
}

#[pg_test]
fn untyped_int_literal_on_bigint_pk_uses_cold_native_emit_path() {
    // Untyped `2` is an int4 Const against bigint `id`. Hot pushdown must accept
    // that promotion (same as `2::bigint`) so cold PK lookups do not fall through
    // to merge_buffer and materialize the entire hot heap.
    let suffix = unique_suffix("int4_pk");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    manage_for_cold_flush(&relation, &storage);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (1, 'a'), (2, 'b'), (3, 'c')"
    ))
    .expect("insert");
    let flushed = flush_table_rows(&relation, true);
    assert!(flushed >= 1, "expected flush to publish cold rows");

    let plan = spi_get_explain(&format!(
        "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) \
         SELECT body FROM {relation} WHERE id = 2"
    ));
    assert!(
        plan.contains("Emit Path: cold_native"),
        "expected cold_native for untyped int4 literal on bigint PK, got: {plan}"
    );
    assert!(
        plan.contains("Hot Rows: 0"),
        "expected hot PK miss (0 rows), got: {plan}"
    );
    assert!(
        !plan.contains("Emit Path: merge_buffer"),
        "untyped bigint PK lookup must not merge-buffer the hot heap: {plan}"
    );
    assert_eq!(
        spi_get_text(&format!("SELECT body FROM {relation} WHERE id = 2")),
        "b"
    );
}

#[pg_test]
fn hot_pk_hit_skips_parquet_open_when_cold_segment_stats_overlap() {
    // After flush, hot rows are pruned. Re-inserting the same PK leaves the old
    // version in cold while the live row is hot. Catalog min/max still keeps the
    // segment (PK is in range). Hot-first must return the hot row without opening
    // Parquet.
    let suffix = unique_suffix("hot_first");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    manage_for_cold_flush(&relation, &storage);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (1, 'a'), (2, 'cold'), (3, 'c')"
    ))
    .expect("insert");
    let flushed = flush_table_rows(&relation, true);
    assert!(flushed >= 1, "expected flush to publish cold rows");

    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (2, 'hot')"
    ))
    .expect("re-insert PK so live row is hot while cold still overlaps");

    let plan = spi_get_explain(&format!(
        "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) \
         SELECT body FROM {relation} WHERE id = 2"
    ));
    assert!(
        plan.contains("Emit Path: hot_native"),
        "expected hot_native for live PK that still overlaps cold stats, got: {plan}"
    );
    assert!(
        plan.contains("Parquet Segments Opened: 0"),
        "hot PK hit must not open overlapping cold Parquet, got: {plan}"
    );
    assert_eq!(
        spi_get_text(&format!("SELECT body FROM {relation} WHERE id = 2")),
        "hot"
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

#[pg_test]
fn unmanaged_select_second_plan_does_not_spi_managed_lookup() {
    let suffix = unique_suffix("neg_cache");
    let schema = format!("pgtest_{suffix}");
    let table = "plain_heap";
    let relation = format!("{schema}.{table}");

    Spi::run(&format!("CREATE SCHEMA {schema}")).expect("schema");
    Spi::run(&format!(
        "CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL)"
    ))
    .expect("create");
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (1, 'plain')"
    ))
    .expect("insert");

    // Warm the absence cache, then prove later plans do not reload via SPI.
    let _ = spi_get_explain(&format!("EXPLAIN SELECT count(*) FROM {relation}"));
    crate::catalog::cache::reset_managed_table_spi_load_count();
    let after_reset = crate::catalog::cache::managed_table_spi_load_count();
    assert_eq!(after_reset, 0);

    let plan = spi_get_explain(&format!("EXPLAIN SELECT count(*) FROM {relation}"));
    assert!(
        !plan.contains("KoldMergeScan") && !plan.contains("Custom Scan"),
        "unmanaged table must stay on heap paths: {plan}"
    );
    let loads = crate::catalog::cache::managed_table_spi_load_count();
    assert_eq!(
        loads, 0,
        "second unmanaged plan must use cached absence, not SPI"
    );
}
