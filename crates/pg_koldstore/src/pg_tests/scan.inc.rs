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
        "Emit Path: merge_stream",
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
    let planned = spi_get_explain(&format!("EXPLAIN (COSTS OFF) SELECT body FROM {relation}"));
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
fn plain_explain_applies_catalog_prune_without_opening_parquet() {
    // 2501 rows + hot_row_limit=1 + max_rows_per_file=1000 => three cold
    // segments with disjoint PK ranges (same shape as flush_scheduler tests).
    // Point-lookup EXPLAIN must prune non-overlapping segments and report
    // planned opens == survivors, without opening Parquet (BeginCustomScan
    // skips under EXEC_FLAG_EXPLAIN_ONLY).
    let suffix = unique_suffix("explain_prune");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    Spi::run(&format!(
        r#"
        SELECT koldstore.manage_table(
          table_name     => '{relation}'::regclass,
          storage        => '{storage}',
          hot_row_limit  => 1,
          min_flush_rows => 1,
          max_rows_per_file => 1000,
          auto_flush => false,
          migration_order_by => 'id'
        )
        "#
    ))
    .expect("manage_table");
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body)
         SELECT gs, 'row-' || gs::text FROM generate_series(1, 2501) AS gs"
    ))
    .expect("insert");
    let flushed = flush_table_rows(&relation, true);
    assert!(
        flushed >= 2500,
        "expected multi-segment flush, rows_flushed={flushed}"
    );

    // Catalog must publish PK min/max or EXPLAIN prune cannot work.
    let id_stats_present = spi_get_i64(&format!(
        "SELECT count(*)::bigint
         FROM koldstore.cold_segments
         WHERE table_oid = '{relation}'::regclass
           AND status = 'active'
           AND column_stats ? 'id'
           AND column_stats->'id' ? 'min'
           AND column_stats->'id' ? 'max'"
    ));
    assert_eq!(
        id_stats_present, 3,
        "every active segment must carry id min/max in column_stats"
    );

    let planned = spi_get_explain(&format!(
        "EXPLAIN (COSTS OFF) SELECT body FROM {relation} WHERE id = 1"
    ));
    assert!(
        planned.contains("Status: planned"),
        "plain EXPLAIN must stay planned: {planned}"
    );
    assert!(
        planned.contains("Candidate Segments: 3"),
        "expected three candidate segments after 2501-row flush: {planned}"
    );
    let pruned = scan_explain_counter(&planned, "Segments Pruned by Min/Max");
    let planned_opens = scan_explain_counter(&planned, "Parquet Segments Planned");
    assert!(
        pruned >= 1,
        "PK point EXPLAIN must prune at least one disjoint segment: pruned={pruned} planned={planned_opens}\n{planned}"
    );
    assert_eq!(
        planned_opens, 1,
        "planned opens must equal prune survivors for a single PK: {planned}"
    );
    assert!(
        planned.contains("PK Probe Column: id") && planned.contains("PK Probe Values: 1"),
        "plain EXPLAIN must advertise the PK probe used for cold prune: {planned}"
    );
    assert!(
        !planned.contains("Parquet Segments Opened")
            && !planned.contains("Footer First")
            && !planned.contains("Status: executed"),
        "plain EXPLAIN must not open Parquet or report execution: {planned}"
    );
}

#[pg_test]
fn pk_filters_prune_cold_segments_before_parquet_open() {
    // Shared fixture: 2501 rows → three cold segments with disjoint PK ranges
    // (~1..1000, ~1001..2000, ~2001..2500) plus one hot PK (2501). Each case
    // asserts EXPLAIN ANALYZE opens only the Parquet survivors needed.
    let suffix = unique_suffix("pk_prune_cases");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    Spi::run(&format!(
        r#"
        SELECT koldstore.manage_table(
          table_name     => '{relation}'::regclass,
          storage        => '{storage}',
          hot_row_limit  => 1,
          min_flush_rows => 1,
          max_rows_per_file => 1000,
          auto_flush => false,
          migration_order_by => 'id'
        )
        "#
    ))
    .expect("manage_table");
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body)
         SELECT gs, 'row-' || gs::text FROM generate_series(1, 2501) AS gs"
    ))
    .expect("insert");
    let flushed = flush_table_rows(&relation, true);
    assert!(
        flushed >= 2500,
        "expected multi-segment flush, rows_flushed={flushed}"
    );

    // 1) Mid-range PK equality (int4 literal on bigint) → one segment.
    assert_cold_parquet_opens(
        &relation,
        "id = 1500",
        ColdOpenExpect {
            candidates: 3,
            pruned_min_max: 2,
            opened: 1,
        },
        "mid-range PK equality",
    );
    assert_eq!(
        spi_get_i64(&format!("SELECT count(*) FROM {relation} WHERE id = 1500")),
        1
    );

    // 2) PK miss outside every segment range → open nothing.
    assert_cold_parquet_opens(
        &relation,
        "id = 999999",
        ColdOpenExpect {
            candidates: 3,
            pruned_min_max: 3,
            opened: 0,
        },
        "out-of-range PK miss",
    );

    // 3) Closed range wholly inside one segment → one segment.
    assert_cold_parquet_opens(
        &relation,
        "id BETWEEN 1100 AND 1200",
        ColdOpenExpect {
            candidates: 3,
            pruned_min_max: 2,
            opened: 1,
        },
        "intra-segment PK range",
    );

    // 4) Range crossing a segment boundary → exactly the two overlapping segments.
    assert_cold_parquet_opens(
        &relation,
        "id BETWEEN 990 AND 1010",
        ColdOpenExpect {
            candidates: 3,
            pruned_min_max: 1,
            opened: 2,
        },
        "cross-boundary PK range",
    );

    // 5) One-sided bound that drops older segments → last cold segment only.
    assert_cold_parquet_opens(
        &relation,
        "id >= 2001",
        ColdOpenExpect {
            candidates: 3,
            pruned_min_max: 2,
            opened: 1,
        },
        "lower-bound PK prune",
    );

    // 6) Hot-only PK inserted after flush → cold is skipped (hot_native) or
    // every catalog segment is pruned before any Parquet open.
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (3000, 'hot-only')"
    ))
    .expect("insert hot-only row");
    let hot_only = spi_get_explain(&format!(
        "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) \
         SELECT count(*) FROM {relation} WHERE id = 3000"
    ));
    assert_eq!(
        scan_explain_counter(&hot_only, "Parquet Segments Opened"),
        0,
        "hot-only PK must not open Parquet: {hot_only}"
    );
    assert!(
        hot_only.contains("Emit Path: hot_native")
            || scan_explain_counter(&hot_only, "Segments Pruned by Min/Max") == 3,
        "hot-only PK must use hot_native or prune all cold segments: {hot_only}"
    );
    assert_eq!(
        spi_get_i64(&format!("SELECT count(*) FROM {relation} WHERE id = 3000")),
        1
    );
}

#[derive(Clone, Copy)]
struct ColdOpenExpect {
    candidates: usize,
    pruned_min_max: usize,
    opened: usize,
}

/// Asserts ANALYZE cold counters for one WHERE clause against a shared fixture.
fn assert_cold_parquet_opens(
    relation: &str,
    where_sql: &str,
    expect: ColdOpenExpect,
    label: &str,
) {
    let analyzed = spi_get_explain(&format!(
        "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) \
         SELECT count(*) FROM {relation} WHERE {where_sql}"
    ));
    assert_eq!(
        scan_explain_counter(&analyzed, "Candidate Segments"),
        expect.candidates,
        "{label}: candidate segments: {analyzed}"
    );
    assert_eq!(
        scan_explain_counter(&analyzed, "Segments Pruned by Min/Max"),
        expect.pruned_min_max,
        "{label}: min/max prune: {analyzed}"
    );
    assert_eq!(
        scan_explain_counter(&analyzed, "Parquet Segments Opened"),
        expect.opened,
        "{label}: parquet opens: {analyzed}"
    );
}

#[pg_test]
fn full_cold_count_streams_one_disjoint_segment_payload_at_a_time() {
    let suffix = unique_suffix("streaming_count");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    manage_for_cold_flush(&relation, &storage);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body)
         SELECT gs, repeat('payload-', 8) || gs::text
         FROM generate_series(1, 2501) AS gs"
    ))
    .expect("insert");
    let flushed = flush_table_rows(&relation, true);
    assert!(
        flushed >= 2500,
        "expected three cold segments, rows_flushed={flushed}"
    );

    let analyzed = spi_get_explain(&format!(
        "EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF) SELECT count(*) FROM {relation}"
    ));
    assert!(
        analyzed.contains("Emit Path: merge_stream"),
        "full cold count must use streaming merge emission: {analyzed}"
    );
    assert_eq!(
        scan_explain_counter(&analyzed, "Parquet Segments Opened"),
        3,
        "count must consume all three segments: {analyzed}"
    );
    assert!(
        scan_explain_counter(&analyzed, "Peak Cold Batch Rows") <= 1000,
        "stream must retain at most one disjoint segment payload: {analyzed}"
    );

    let limited = spi_get_explain(&format!(
        "EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF) SELECT id FROM {relation} LIMIT 1"
    ));
    assert_eq!(
        scan_explain_counter(&limited, "Parquet Segments Opened"),
        1,
        "parent LIMIT must stop the stream before older segments open: {limited}"
    );
}

#[pg_test]
fn mixed_scan_pages_hot_json_one_batch_at_a_time() {
    let suffix = unique_suffix("hot_batch");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    Spi::run(&format!(
        r#"
        SELECT koldstore.manage_table(
          table_name     => '{relation}'::regclass,
          storage        => '{storage}',
          hot_row_limit  => 5000,
          min_flush_rows => 1,
          max_rows_per_file => 1000,
          migration_order_by => 'id'
        )
        "#
    ))
    .expect("manage_table");
    // Force-flush seeds cold segments, then leave a large hot heap so MergeStream
    // must page hot JSON instead of loading it all at BeginCustomScan.
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body)
         SELECT gs, repeat('payload-', 8) || gs::text
         FROM generate_series(1, 2500) AS gs"
    ))
    .expect("insert cold seed");
    let flushed = flush_table_rows(&relation, true);
    assert!(
        flushed >= 2500,
        "expected cold segments for mixed scan, rows_flushed={flushed}"
    );
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body)
         SELECT gs, repeat('hot-', 8) || gs::text
         FROM generate_series(2501, 5000) AS gs"
    ))
    .expect("insert hot pages");

    let total = Spi::get_one::<i64>(&format!("SELECT count(*)::bigint FROM {relation}"))
        .expect("count")
        .expect("count value");
    assert_eq!(total, 5000, "mixed scan must return every hot and cold row");

    let analyzed = spi_get_explain(&format!(
        "EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF) SELECT count(*) FROM {relation}"
    ));
    assert!(
        analyzed.contains("Emit Path: merge_stream"),
        "mixed count must use streaming merge emission: {analyzed}"
    );
    let peak_hot = scan_explain_counter(&analyzed, "Peak Hot Batch Rows");
    assert!(
        peak_hot > 0 && peak_hot <= 1024,
        "hot JSON must page at most HOT_MERGE_BATCH_ROWS: peak={peak_hot}, plan={analyzed}"
    );
    assert!(
        scan_explain_counter(&analyzed, "Peak Cold Batch Rows") <= 1000,
        "cold payload must stay segment-bounded: {analyzed}"
    );
    assert!(
        scan_explain_counter(&analyzed, "Hot Rows") >= 2500,
        "hot pages must cover the retained hot heap: {analyzed}"
    );
    assert!(
        scan_explain_counter(&analyzed, "Seen Keys") >= 5000,
        "exact winner identities must cover every distinct PK: {analyzed}"
    );
}

#[pg_test]
fn merge_scan_fails_closed_when_seen_key_limit_is_exceeded() {
    let suffix = unique_suffix("seen_limit");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    manage_for_cold_flush(&relation, &storage);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body)
         SELECT gs, 'body-' || gs::text
         FROM generate_series(1, 250) AS gs"
    ))
    .expect("insert");
    let flushed = flush_table_rows(&relation, true);
    assert!(flushed >= 200, "expected cold rows, rows_flushed={flushed}");

    Spi::run("SET koldstore.max_merge_seen_keys = 100").expect("set seen-key limit");
    // Catch the PostgreSQL ERROR in a subtransaction so the pg_test txn stays usable.
    Spi::run(&format!(
        r#"
        DO $do$
        BEGIN
          PERFORM count(*) FROM {relation};
          RAISE EXCEPTION 'expected KoldMergeScan seen-key limit to fail the scan';
        EXCEPTION
          WHEN OTHERS THEN
            IF position('max_merge_seen_keys' in SQLERRM) = 0
               AND position('exact primary-key identities' in SQLERRM) = 0 THEN
              RAISE;
            END IF;
        END
        $do$;
        "#
    ))
    .expect("seen-key limit must fail closed with a clear error");
    Spi::run("RESET koldstore.max_merge_seen_keys").expect("reset seen-key limit");
}

fn scan_explain_counter(plan: &str, label: &str) -> usize {
    let prefix = format!("{label}:");
    plan.lines()
        .find_map(|line| {
            let trimmed = line.trim_start();
            trimmed
                .strip_prefix(&prefix)
                .and_then(|value| value.trim().parse::<usize>().ok())
        })
        .unwrap_or_else(|| panic!("missing EXPLAIN counter `{label}` in:\n{plan}"))
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
    // that promotion (same as `2::bigint`) so cold PK lookups stay on the direct
    // cold path instead of loading the hot heap for winner resolution.
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
        !plan.contains("Emit Path: merge_stream"),
        "untyped bigint PK lookup must not load the hot heap for merging: {plan}"
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
    assert!(
        flushed >= 2,
        "expected at least two rows flushed, got {flushed}"
    );

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
