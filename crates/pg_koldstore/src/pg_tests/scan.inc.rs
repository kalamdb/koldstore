#[pg_test]
fn managed_table_without_published_cold_segments_keeps_native_postgresql_plan() {
    let suffix = unique_suffix("native_no_cold");
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

    let plan = spi_get_explain(&format!(
        "EXPLAIN (COSTS OFF) SELECT body FROM {relation} WHERE id BETWEEN 1 AND 10"
    ));
    assert!(
        !plan.contains("KoldMergeScan") && !plan.contains("Custom Scan"),
        "zero published cold segments must retain PostgreSQL's native plan: {plan}"
    );
    assert!(
        plan.contains("Index Scan")
            || plan.contains("Index Only Scan")
            || plan.contains("Bitmap Heap Scan")
            || plan.contains("Seq Scan"),
        "expected a native PostgreSQL scan node: {plan}"
    );
    assert_eq!(
        spi_get_text(&format!(
            "SELECT string_agg(body, ',' ORDER BY id) FROM {relation} WHERE id BETWEEN 1 AND 10"
        )),
        "hot"
    );
}

#[pg_test]
fn prepared_native_plan_is_invalidated_when_first_cold_segment_is_published() {
    let suffix = unique_suffix("native_plan_invalidation");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let statement = format!("ks_native_invalidation_{suffix}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (1, 'a'), (2, 'b'), (3, 'c')"
    ))
    .expect("insert");
    manage_for_cold_flush(&relation, &storage);
    Spi::run(&format!(
        "PREPARE {statement} AS SELECT count(*)::bigint FROM {relation}"
    ))
    .expect("prepare");

    let before_plan = spi_get_explain(&format!("EXPLAIN (COSTS OFF) EXECUTE {statement}"));
    assert!(
        !before_plan.contains("KoldMergeScan") && !before_plan.contains("Custom Scan"),
        "prepared pre-flush query must begin with a native PostgreSQL plan: {before_plan}"
    );
    assert_eq!(spi_get_i64(&format!("EXECUTE {statement}")), 3);

    let flushed = flush_table_rows(&relation, true);
    assert!(flushed >= 1, "expected flush to publish cold rows");

    let after_plan = spi_get_explain(&format!("EXPLAIN (COSTS OFF) EXECUTE {statement}"));
    assert!(
        after_plan.contains("KoldMergeScan") || after_plan.contains("Custom Scan"),
        "relcache invalidation must rebuild the prepared plan with cold visibility: {after_plan}"
    );
    assert_eq!(
        spi_get_i64(&format!("EXECUTE {statement}")),
        3,
        "replanned prepared statement must retain all flushed rows"
    );
    Spi::run(&format!("DEALLOCATE {statement}")).expect("deallocate");
}

#[pg_test]
fn hot_primary_key_range_above_cold_max_keeps_native_postgresql_plan() {
    let suffix = unique_suffix("native_above_cold");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (1, 'cold-a'), (2, 'cold-b'), (3, 'cold-c')"
    ))
    .expect("insert cold candidates");
    manage_for_cold_flush(&relation, &storage);
    assert!(flush_table_rows(&relation, true) >= 1);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (100, 'hot-a'), (101, 'hot-b'), (102, 'hot-c')"
    ))
    .expect("insert hot range");

    let plan = spi_get_explain(&format!(
        "EXPLAIN (COSTS OFF) \
         SELECT body FROM {relation} WHERE id BETWEEN 100 AND 102"
    ));
    assert!(
        !plan.contains("KoldMergeScan") && !plan.contains("Custom Scan"),
        "PK bounds proving zero cold matches must retain PostgreSQL's native plan: {plan}"
    );
    assert_eq!(
        spi_get_text(&format!(
            "SELECT string_agg(body, ',' ORDER BY id) FROM {relation} \
             WHERE id BETWEEN 100 AND 102"
        )),
        "hot-a,hot-b,hot-c"
    );
}

#[pg_test]
fn prepared_native_range_plan_is_invalidated_when_catalog_bounds_expand() {
    let suffix = unique_suffix("native_bound_invalidation");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let statement = format!("ks_native_bound_invalidation_{suffix}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (1, 'cold-a'), (2, 'cold-b')"
    ))
    .expect("insert initial cold candidates");
    manage_for_cold_flush(&relation, &storage);
    assert!(flush_table_rows(&relation, true) >= 1);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (100, 'newer-hot')"
    ))
    .expect("insert newer hot row");
    Spi::run(&format!(
        "PREPARE {statement} AS SELECT body FROM {relation} WHERE id = 100"
    ))
    .expect("prepare");

    let before_plan = spi_get_explain(&format!("EXPLAIN (COSTS OFF) EXECUTE {statement}"));
    assert!(
        !before_plan.contains("KoldMergeScan") && !before_plan.contains("Custom Scan"),
        "aggregate bounds should initially prove a native-only lookup: {before_plan}"
    );
    assert_eq!(spi_get_text(&format!("EXECUTE {statement}")), "newer-hot");

    Spi::run(&format!(
        "UPDATE koldstore.cold_segment_index
         SET max_value = koldstore.internal_encode_sort_key(100::bigint)
         WHERE table_oid = '{relation}'::regclass
           AND scope_key = ''
           AND column_id = 1"
    ))
    .expect("expand indexed cold bound");
    let table_oid = Spi::get_one::<pg_sys::Oid>(&format!(
        "SELECT '{relation}'::regclass::oid"
    ))
    .expect("read table oid")
    .expect("table oid");
    crate::catalog::cache::invalidate_table_globally(table_oid);

    let after_plan = spi_get_explain(&format!("EXPLAIN (COSTS OFF) EXECUTE {statement}"));
    assert!(
        after_plan.contains("KoldMergeScan") || after_plan.contains("Custom Scan"),
        "flush publication must invalidate the native plan after cold bounds expand: {after_plan}"
    );
    assert_eq!(
        spi_get_text(&format!("EXECUTE {statement}")),
        "newer-hot",
        "replanned lookup must read the newly cold row"
    );
    Spi::run(&format!("DEALLOCATE {statement}")).expect("deallocate");
}

#[pg_test]
fn explain_analyze_uses_native_hot_child_counters() {
    let suffix = unique_suffix("explain_hot_child");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (1, 'cold-a'), (2, 'cold-b'), (3, 'cold-c')"
    ))
    .expect("insert cold candidates");
    manage_for_cold_flush(&relation, &storage);
    assert!(flush_table_rows(&relation, true) >= 1);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (10, 'a'), (11, 'b'), (12, 'c')"
    ))
    .expect("insert hot rows");
    let statement = format!("ks_hot_child_counters_{suffix}");
    Spi::run("SET LOCAL plan_cache_mode = force_generic_plan").expect("force generic plan");
    Spi::run(&format!(
        "PREPARE {statement}(bigint) AS \
         SELECT body FROM {relation} WHERE id >= $1 ORDER BY id"
    ))
    .expect("prepare parameterized hot range");

    let plan = spi_get_explain(&format!(
        "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) \
         EXECUTE {statement}(10)"
    ));
    for expected in [
        "Emit Path: hot_child",
        "Actual Access: Native PostgreSQL Child",
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
    Spi::run(&format!("DEALLOCATE {statement}")).expect("deallocate");
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
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (1, 'a'), (2, 'b'), (3, 'c')"
    ))
    .expect("insert");
    manage_for_cold_flush(&relation, &storage);
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
fn explain_json_exposes_koldstore_read_pipeline_as_plan_nodes() {
    let suffix = unique_suffix("explain_json_plan_nodes");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (1, 'a'), (2, 'b'), (3, 'c')"
    ))
    .expect("insert");
    manage_for_cold_flush(&relation, &storage);
    assert!(flush_table_rows(&relation, true) >= 1);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (10, 'hot')"
    ))
    .expect("insert hot row");

    let plan = Spi::connect(|client| {
        let mut table = client
            .select(
                &format!(
                    "EXPLAIN (ANALYZE, FORMAT JSON, COSTS OFF, SUMMARY OFF) \
                     SELECT body FROM {relation} WHERE id >= 2 ORDER BY id"
                ),
                None,
                &[],
            )
            .expect("explain select");
        table
            .next()
            .expect("explain row")
            .get::<pgrx::Json>(1)
            .expect("explain json column")
            .expect("explain json")
            .0
            .to_string()
    });
    // Native hot child still owns `Plans`; Internal nodes live under the distinct
    // `KoldStore Pipeline` key so JSON parsers keep both. `pgrx::Json` →
    // `serde_json::Value::to_string()` is compact (no spaces after `:`).
    for expected in [
        "\"Custom Plan Provider\":\"KoldMergeScan\"",
        "\"KoldStore Pipeline\"",
        "\"Scan Sources\"",
        "\"Hot Scan\"",
        "\"Planned Access\"",
        "\"Actual Access\":\"Native PostgreSQL Child\"",
        "\"Cold Scan\"",
        "\"Runtime Manifest Read\":false",
        "\"Cold Segments Query\"",
        "\"Parquet Segments\"",
        "\"Mirror Scan\"",
        "\"Actual Rows\":1",
        "\"Node Type\":\"KoldStore Hot Scan\"",
        "\"Node Type\":\"KoldStore Cold Storage Scan\"",
        "\"Node Type\":\"KoldStore Segment Catalog Scan\"",
        "\"Node Type\":\"KoldStore Catalog Query\"",
        "\"Node Type\":\"KoldStore Parquet Scan\"",
        "\"Node Type\":\"KoldStore Parquet Footer\"",
        "\"Node Type\":\"KoldStore Parquet Row Group Prune\"",
        "\"Node Type\":\"KoldStore Parquet Column Fetch\"",
        "\"KoldStore Internal\":true",
    ] {
        assert!(
            plan.contains(expected),
            "expected KoldStore explain contract `{expected}`: {plan}"
        );
    }
    assert!(
        !plan.contains("\"Hot SPI Query\""),
        "native hot child must not advertise SPI keyset text: {plan}"
    );
    assert!(
        !plan.contains("\"Node Type\":\"KoldStore Mirror Overlay\""),
        "Mirror Overlay must not appear as a visual plan node: {plan}"
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
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (1, 'a'), (2, 'b'), (3, 'c')"
    ))
    .expect("insert");
    manage_shared(&relation, &storage);
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
        "Segments Pruned by Catalog Index",
        "Parquet Segments Opened",
        "Bytes Fetched",
        "Runtime Catalog Source",
        "Published Manifest Path",
        "Cold Segments Query",
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
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES \
         (1, 'cold-a'), (2, 'cold-b'), (3, 'cold-c')"
    ))
    .expect("insert cold candidates");
    manage_for_cold_flush(&relation, &storage);
    let flushed = flush_table_rows(&relation, true);
    assert!(flushed >= 1, "expected flush to publish cold rows");
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (4, 'hot-d')"
    ))
    .expect("insert hot row");

    let plan = spi_get_explain(&format!(
        "EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF) \
         SELECT body FROM {relation} ORDER BY id DESC LIMIT 5"
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
        "Emit Path: ordered_merge_native",
        "Actual Access: Native PostgreSQL Child",
        "Strategy: Ordered Progressive",
        "Seen Keys: 4",
        "Hot Rows: 1",
        "Rows Scanned: 3",
        "Input Rows: 4",
        "Output Rows: 4",
        "Rows Removed by Merge: 0",
    ] {
        assert!(
            plan.contains(expected),
            "EXPLAIN ANALYZE flow missing exact counter `{expected}`: {plan}"
        );
    }
    assert!(
        !plan.contains("SPI JSON Keyset Scan") && !plan.contains("to_jsonb(proj)"),
        "Ordered Progressive must not use SPI JSON keyset hot paging: {plan}"
    );
}

#[pg_test]
fn primary_key_range_pushes_hot_candidates_into_merge_stream() {
    let suffix = unique_suffix("hot_range_pushdown");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    // Cold max must sit strictly below the first DESC hot page (ids 9..2) so
    // ordered progressive stays hot-dominant and parent LIMIT can stop after
    // the adaptive first page. Higher cold ids (e.g. 1..12) correctly force a
    // sorted-buffer drain — covered by ordered_limit_cold_wins_returns_cold_first.
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) \
         SELECT id, 'cold-' || id::text FROM generate_series(1, 1) AS id"
    ))
    .expect("insert cold candidates");
    manage_for_cold_flush(&relation, &storage);
    assert!(flush_table_rows(&relation, true) >= 1);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) \
         SELECT id, 'hot-' || id::text FROM generate_series(1, 1000) AS id"
    ))
    .expect("insert hot rows");

    let plan = spi_get_explain(&format!(
        "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) \
         SELECT body FROM {relation} WHERE id < 10 ORDER BY id DESC LIMIT 5"
    ));
    assert!(
        plan.contains("Emit Path: ordered_merge_native")
            || plan.contains("Emit Path: merge_stream"),
        "overlapping cold candidates must use a merge emit path: {plan}"
    );
    assert!(
        plan.contains("Hot Rows: 8") && plan.contains("Peak Hot Batch Rows: 8"),
        "PK range + LIMIT must use index pushdown and stop after the first adaptive hot page: {plan}"
    );
    assert_eq!(
        spi_get_text(&format!(
            "SELECT string_agg(body, ',' ORDER BY id DESC) FROM (\
             SELECT id, body FROM {relation} WHERE id < 10 ORDER BY id DESC LIMIT 5\
             ) candidates"
        )),
        "hot-9,hot-8,hot-7,hot-6,hot-5",
        "newer hot candidates must win over their cold versions"
    );
}

#[pg_test]
fn plain_explain_never_reuses_prior_analyze_counters() {
    let suffix = unique_suffix("explain_lifecycle");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (1, 'a'), (2, 'b')"
    ))
    .expect("insert");
    manage_for_cold_flush(&relation, &storage);
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
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (1, 'a'), (2, 'b'), (3, 'c')"
    ))
    .expect("insert");
    manage_for_cold_flush(&relation, &storage);
    let mirror = spi_get_text(&format!(
        "SELECT mirror_relation::text \
         FROM koldstore.schemas \
         WHERE table_oid = '{relation}'::regclass AND active"
    ));
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
        "expected mirror tombstone before EXPLAIN"
    );

    let plan = spi_get_explain(&format!(
        "EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF) \
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
    // to merge_stream and materialize the entire hot heap.
    let suffix = unique_suffix("int4_pk");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (1, 'a'), (2, 'b'), (3, 'c')"
    ))
    .expect("insert");
    manage_for_cold_flush(&relation, &storage);
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
        "untyped bigint PK lookup must not merge-buffer the hot heap: {plan}"
    );
    assert_eq!(
        spi_get_text(&format!("SELECT body FROM {relation} WHERE id = 2")),
        "b"
    );
}

#[pg_test]
fn hot_pk_hit_skips_parquet_open_when_cold_segment_index_overlaps() {
    // After flush, hot rows are pruned. Re-inserting the same PK leaves the old
    // version in cold while the live row is hot. Catalog index still keeps the
    // segment (PK is in range). Hot-first must return the hot row without opening
    // Parquet.
    let suffix = unique_suffix("hot_first");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (1, 'a'), (2, 'cold'), (3, 'c')"
    ))
    .expect("insert");
    manage_for_cold_flush(&relation, &storage);
    let flushed = flush_table_rows(&relation, true);
    assert!(flushed >= 1, "expected flush to publish cold rows");

    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (2, 'hot')"
    ))
    .expect("re-insert PK so live row is hot while cold still overlaps");

    let plan = spi_get_explain(&format!(
        "EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF) \
         SELECT body FROM {relation} WHERE id = 2"
    ));
    assert!(
        plan.contains("Emit Path: hot_child"),
        "expected native PostgreSQL hot child for live PK that still overlaps cold stats, got: {plan}"
    );
    assert!(
        plan.contains("Actual Access: Native PostgreSQL Child"),
        "exact hot PK hit must use the already-planned PostgreSQL child: {plan}"
    );
    assert!(
        !plan.contains("Metadata Time:"),
        "visible hot PK hit must not initialize KoldStore executor metadata: {plan}"
    );
    assert!(
        plan.contains("Parquet Segments Opened: 0"),
        "hot PK hit must not open overlapping cold Parquet, got: {plan}"
    );
    assert_eq!(
        spi_get_text(&format!("SELECT body FROM {relation} WHERE id = 2")),
        "hot"
    );

    // The native child applies every query qual. If a mutable-column qual
    // rejects the hot row, KoldStore must still discover that PK in the hot
    // heap so the older matching cold image cannot be resurrected.
    let residual_miss = spi_get_explain(&format!(
        "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) \
         SELECT body FROM {relation} WHERE id = 2 AND body = 'cold'"
    ));
    assert!(
        residual_miss.contains("Emit Path: hot_native"),
        "child miss must fall back to owner-visible hot PK resolution: {residual_miss}"
    );
    assert!(
        residual_miss.contains("Parquet Segments Opened: 0"),
        "newer hot PK must mask the older cold image without opening Parquet: {residual_miss}"
    );
    assert_eq!(
        spi_get_i64(&format!(
            "SELECT count(*)::bigint FROM {relation} \
             WHERE id = 2 AND body = 'cold'"
        )),
        0,
        "a rejected current hot version must not expose an older cold version"
    );
}

#[pg_test]
fn exact_hot_pk_hit_avoids_merge_runtime_bookkeeping() {
    let suffix = unique_suffix("hot_runtime_work");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (1, 'cold'), (2, 'pad')"
    ))
    .expect("insert cold candidates");
    manage_for_cold_flush(&relation, &storage);
    assert!(flush_table_rows(&relation, true) >= 1);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (1, 'hot')"
    ))
    .expect("insert newer hot row");

    crate::merge_scan::pg::reset_fast_path_test_counters();
    assert_eq!(
        spi_get_text(&format!("SELECT body FROM {relation} WHERE id = 1")),
        "hot"
    );
    assert_eq!(
        crate::merge_scan::pg::fast_path_test_counters(),
        crate::merge_scan::pg::FastPathTestCounters {
            global_state_inserts: 0,
            tuple_copies: 0,
            fallback_initializations: 0,
        },
        "uninstrumented exact hot-PK hits must delegate without merge bookkeeping"
    );
}

#[pg_test]
fn parameterized_hot_range_above_cold_max_skips_merge_fallback() {
    let suffix = unique_suffix("parameter_hot_delegate");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let statement = format!("ks_parameter_hot_delegate_{suffix}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (1, 'cold-a'), (2, 'cold-b'), (3, 'cold-c')"
    ))
    .expect("insert cold candidates");
    manage_for_cold_flush(&relation, &storage);
    assert!(flush_table_rows(&relation, true) >= 1);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (100, 'hot-a'), (101, 'hot-b'), (102, 'hot-c')"
    ))
    .expect("insert hot range");
    Spi::run("SET LOCAL plan_cache_mode = force_generic_plan").expect("force generic plan");
    Spi::run(&format!(
        "PREPARE {statement}(bigint, bigint) AS
         SELECT count(*)::bigint FROM {relation} WHERE id BETWEEN $1 AND $2"
    ))
    .expect("prepare parameterized range");
    let plan = spi_get_explain(&format!(
        "EXPLAIN (COSTS OFF) EXECUTE {statement}(100, 102)"
    ));
    assert!(
        plan.contains("KoldMergeScan") || plan.contains("Custom Scan"),
        "generic parameter plan must retain a runtime-capable KoldMergeScan: {plan}"
    );

    crate::merge_scan::pg::reset_fast_path_test_counters();
    assert_eq!(spi_get_i64(&format!("EXECUTE {statement}(100, 102)")), 3);
    assert_eq!(
        crate::merge_scan::pg::fast_path_test_counters()
            .fallback_initializations,
        0,
        "runtime aggregate bounds should delegate directly to the native hot child"
    );
    Spi::run(&format!("DEALLOCATE {statement}")).expect("deallocate");
}

#[pg_test]
fn parameterized_primary_key_miss_above_cold_max_skips_merge_fallback() {
    let suffix = unique_suffix("parameter_pk_miss");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let statement = format!("ks_parameter_pk_miss_{suffix}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (1, 'cold-a'), (2, 'cold-b')"
    ))
    .expect("insert cold candidates");
    manage_for_cold_flush(&relation, &storage);
    assert!(flush_table_rows(&relation, true) >= 1);
    Spi::run("SET LOCAL plan_cache_mode = force_generic_plan").expect("force generic plan");
    Spi::run(&format!(
        "PREPARE {statement}(bigint) AS
         SELECT count(*)::bigint FROM {relation} WHERE id = $1"
    ))
    .expect("prepare parameterized PK lookup");

    crate::merge_scan::pg::reset_fast_path_test_counters();
    assert_eq!(spi_get_i64(&format!("EXECUTE {statement}(100)")), 0);
    assert_eq!(
        crate::merge_scan::pg::fast_path_test_counters()
            .fallback_initializations,
        0,
        "a PK miss beyond complete cold bounds must not initialize the merge"
    );
    Spi::run(&format!("DEALLOCATE {statement}")).expect("deallocate");
}

#[pg_test]
fn packed_row_group_arrays_skip_parquet_when_scalar_segment_bounds_overlap() {
    let suffix = unique_suffix("packed_rg");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    Spi::run(&format!(
        r#"
        INSERT INTO {relation} (id, body)
        SELECT id, 'row-' || id::text
        FROM (
          SELECT generate_series(1, 1024)::bigint AS id
          UNION ALL
          SELECT generate_series(2000, 3023)::bigint AS id
        ) rows
        "#
    ))
    .expect("insert disjoint row-group ranges");
    Spi::run(&format!(
        r#"
        SELECT koldstore.manage_table(
          table_name     => '{relation}'::regclass,
          storage        => '{storage}',
          hot_row_limit  => 1,
          min_flush_rows => 1,
          max_rows_per_file => 3000,
          migration_order_by => 'id'
        )
        "#
    ))
    .expect("manage table with two row groups in one segment");
    assert_eq!(flush_table_rows(&relation, true), 2048);

    let arrays_aligned = Spi::get_one::<bool>(&format!(
        r#"
        SELECT count(*) = 1
           AND bool_and(cs.row_group_count = 2)
           AND bool_and(cardinality(cs.row_group_row_counts) = cs.row_group_count)
           AND bool_and(cardinality(cs.row_group_min_seqs) = cs.row_group_count)
           AND bool_and(cardinality(cs.row_group_max_seqs) = cs.row_group_count)
           AND bool_and(cardinality(csi.row_group_min_values) = cs.row_group_count)
           AND bool_and(cardinality(csi.row_group_max_values) = cs.row_group_count)
           AND bool_and(cardinality(csi.row_group_null_counts) = cs.row_group_count)
           AND bool_and(csi.min_value IS NOT NULL AND csi.max_value IS NOT NULL)
           AND bool_and(csoi.sort_order_id = csi.column_id)
           AND bool_and(cardinality(csoi.row_group_min_composite_keys) = cs.row_group_count)
           AND bool_and(cardinality(csoi.row_group_max_composite_keys) = cs.row_group_count)
           AND bool_and(csoi.min_composite_key IS NOT NULL AND csoi.max_composite_key IS NOT NULL)
           AND bool_and(csoi.bounds_exact)
        FROM koldstore.cold_segments cs
        JOIN koldstore.cold_segment_index csi USING (segment_id)
        JOIN koldstore.cold_segment_order_index csoi
          ON csoi.segment_id = cs.segment_id
         AND csoi.sort_order_id = csi.column_id
        WHERE cs.table_oid = '{relation}'::regclass
          AND csi.column_id = (
            SELECT attnum
            FROM pg_attribute
            WHERE attrelid = '{relation}'::regclass
              AND attname = 'id'
              AND NOT attisdropped
          )
        "#
    ))
    .expect("inspect packed catalog arrays")
    .expect("packed catalog array assertion");
    assert!(
        arrays_aligned,
        "expected aligned PK row-group metadata and order-index bounds"
    );

    // 1500 is inside the segment-level [1, 3023] range, but falls in the gap
    // between row-group ranges [1, 1024] and [2000, 3023].
    let plan = spi_get_explain(&format!(
        "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) \
         SELECT body FROM {relation} WHERE id = 1500"
    ));
    assert!(
        plan.contains("Parquet Segments Opened: 0"),
        "packed row-group pruning must skip the object before footer access: {plan}"
    );
    assert_eq!(
        spi_get_i64(&format!(
            "SELECT count(*)::bigint FROM {relation} WHERE id = 1500"
        )),
        0
    );

    crate::catalog::cache::invalidate_all();
    crate::catalog::cache::reset_packed_row_group_spi_load_count();
    assert_eq!(
        spi_get_i64(&format!(
            "SELECT count(*)::bigint FROM {relation} WHERE id = 1500"
        )),
        0
    );
    assert_eq!(
        spi_get_i64(&format!(
            "SELECT count(*)::bigint FROM {relation} WHERE id = 1500"
        )),
        0
    );
    assert_eq!(
        crate::catalog::cache::packed_row_group_spi_load_count(),
        0,
        "one-column lookup must not execute a secondary packed-metadata query"
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
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (1, 'hot-a'), (2, 'hot-b')"
    ))
    .expect("insert hot");
    manage_shared(&relation, &storage);

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

    let plan = spi_get_explain(&format!(
        "EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF) EXECUTE ks_prep_{suffix}(1)"
    ));
    assert!(
        !plan.contains("KoldMergeScan") && !plan.contains("Custom Scan"),
        "pre-flush prepared parameters must retain PostgreSQL's native plan: {plan}"
    );

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

#[pg_test]
fn merge_scan_fails_closed_when_seen_key_limit_is_exceeded() {
    let suffix = unique_suffix("seen_limit");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body)
         SELECT gs, 'body-' || gs::text
         FROM generate_series(1, 250) AS gs"
    ))
    .expect("insert");
    manage_for_cold_flush(&relation, &storage);
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

#[pg_test]
fn ordered_pk_limit_uses_kold_merge_scan_without_external_sort() {
    let suffix = unique_suffix("ordered_pk_pathkeys");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES
         (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (6, 'f')"
    ))
    .expect("insert");
    manage_for_cold_flush(&relation, &storage);
    let flushed = flush_table_rows(&relation, true);
    assert!(flushed >= 1, "expected flush to publish cold rows");
    // Keep a few hot rows newer than the flush cutoff so the query is mixed.
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (100, 'hot-100'), (101, 'hot-101')"
    ))
    .expect("insert hot");

    let plan = spi_get_explain(&format!(
        "EXPLAIN (COSTS OFF) \
         SELECT body FROM {relation} ORDER BY id DESC LIMIT 5"
    ));
    assert!(
        plan.contains("KoldMergeScan") || plan.contains("Custom Scan"),
        "cold-capable ordered limit must use KoldMergeScan: {plan}"
    );
    assert!(
        plan.contains("Strategy") && plan.contains("Ordered Progressive"),
        "ordered portfolio path should advertise Ordered Progressive strategy: {plan}"
    );
    assert!(
        !plan.contains("Sort"),
        "ordered KoldMergeScan pathkeys should avoid an external Sort: {plan}"
    );

    // Locked empty-cold native plan still applies when no cold is published.
    let native_schema = format!("pgtest_{suffix}_native");
    let native_relation = format!("{native_schema}.{table}");
    create_messages_table(&native_schema, table);
    manage_shared(&native_relation, &storage);
    Spi::run(&format!(
        "INSERT INTO {native_relation} (id, body) VALUES (1, 'only-hot')"
    ))
    .expect("insert native");
    let native_plan = spi_get_explain(&format!(
        "EXPLAIN (COSTS OFF) SELECT body FROM {native_relation} WHERE id = 1"
    ));
    assert!(
        !native_plan.contains("KoldMergeScan") && !native_plan.contains("Custom Scan"),
        "empty cold must keep native plans: {native_plan}"
    );
}

#[pg_test]
fn ordered_limit_does_not_drain_full_hot_heap() {
    let suffix = unique_suffix("ordered_limit_lazy");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) \
         SELECT id, 'cold-' || id::text FROM generate_series(1, 20) AS id"
    ))
    .expect("insert cold seed");
    manage_for_cold_flush(&relation, &storage);
    assert!(flush_table_rows(&relation, true) >= 20);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) \
         SELECT id, 'hot-' || id::text FROM generate_series(1, 500) AS id"
    ))
    .expect("insert large hot heap");

    let plan = spi_get_explain(&format!(
        "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) \
         SELECT body FROM {relation} ORDER BY id DESC LIMIT 5"
    ));
    assert!(
        plan.contains("Emit Path: ordered_merge_native")
            && plan.contains("Strategy: Ordered Progressive"),
        "ordered LIMIT must use native ordered progressive merge: {plan}"
    );
    assert!(
        plan.contains("Hot Rows: 8") && plan.contains("Peak Hot Batch Rows: 8"),
        "parent LIMIT must stop after the first adaptive hot page, not drain 500 hot rows: {plan}"
    );
    assert!(
        plan.contains("Parquet Segments Opened: 0"),
        "hot-dominant ordered LIMIT must not open Parquet: {plan}"
    );
    assert!(
        plan.contains("Cold Compete Opens: 0") && plan.contains("Cold Body Opens: 0"),
        "hot-dominant ordered LIMIT must not compete or hydrate body: {plan}"
    );
    assert!(
        plan.contains("Cold Skip Reason:")
            || plan.contains("hot satisfied parent Limit without cold expansion"),
        "EXPLAIN should report why cold was skipped: {plan}"
    );
    assert!(
        !plan.contains("Mirror Tombstones:") || plan.contains("Mirror Tombstones: 0") || plan.contains("Rows Scanned: 0"),
        "deferred mirror must stay unread when cold never expands: {plan}"
    );
    assert_eq!(
        spi_get_text(&format!(
            "SELECT string_agg(body, ',' ORDER BY id DESC) FROM (\
             SELECT id, body FROM {relation} ORDER BY id DESC LIMIT 5\
             ) topn"
        )),
        "hot-500,hot-499,hot-498,hot-497,hot-496",
        "ordered LIMIT must still return the newest hot winners"
    );
}

#[pg_test]
fn ordered_limit_cold_wins_returns_cold_first() {
    let suffix = unique_suffix("ordered_cold_wins");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) \
         SELECT id, 'cold-' || id::text FROM generate_series(1000, 1010) AS id"
    ))
    .expect("insert high-id cold seed");
    manage_for_cold_flush(&relation, &storage);
    assert!(flush_table_rows(&relation, true) >= 11);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) \
         SELECT id, 'hot-' || id::text FROM generate_series(1, 10) AS id"
    ))
    .expect("insert low-id hot rows");

    let plan = spi_get_explain(&format!(
        "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) \
         SELECT id, body FROM {relation} ORDER BY id DESC LIMIT 5"
    ));
    assert!(
        plan.contains("Emit Path: ordered_merge_native")
            && plan.contains("Strategy: Ordered Progressive"),
        "cold-wins ordered LIMIT must use ordered progressive merge: {plan}"
    );
    assert!(
        plan.contains("Parquet Segments Opened:")
            && !plan.contains("Parquet Segments Opened: 0"),
        "cold-wins ordered LIMIT must open competitive Parquet: {plan}"
    );
    assert!(
        plan.contains("Cold Compete Opens:")
            && !plan.contains("Cold Compete Opens: 0")
            && plan.contains("Cold Body Opens:")
            && !plan.contains("Cold Body Opens: 0"),
        "cold emit must compete then hydrate body columns: {plan}"
    );
    assert!(
        plan.contains("Cold Body Columns:") && plan.contains("body"),
        "late materialization must list body as a body column: {plan}"
    );
    assert_eq!(
        spi_get_text(&format!(
            "SELECT string_agg(body, ',' ORDER BY id DESC) FROM (\
             SELECT id, body FROM {relation} ORDER BY id DESC LIMIT 5\
             ) topn"
        )),
        "cold-1010,cold-1009,cold-1008,cold-1007,cold-1006",
        "ordered LIMIT must emit cold winners when they outrank hot"
    );
}

#[pg_test]
fn ordered_limit_asc_cold_wins_after_hot_prune() {
    let suffix = unique_suffix("ordered_asc_cold_wins");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    // Low ids go cold first; later high-id hot inserts must not win ASC top-N.
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) \
         SELECT id, 'cold-' || id::text FROM generate_series(1, 11) AS id"
    ))
    .expect("insert low-id cold seed");
    manage_for_cold_flush(&relation, &storage);
    assert!(flush_table_rows(&relation, true) >= 11);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) \
         SELECT id, 'hot-' || id::text FROM generate_series(1000, 1010) AS id"
    ))
    .expect("insert high-id hot rows");

    let plan = spi_get_explain(&format!(
        "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) \
         SELECT id, body FROM {relation} ORDER BY id ASC LIMIT 5"
    ));
    assert!(
        plan.contains("Emit Path: ordered_merge_native")
            && plan.contains("Strategy: Ordered Progressive")
            && plan.contains("Order Direction: ASC"),
        "ASC cold-wins must advertise ascending ordered progressive: {plan}"
    );
    assert!(
        plan.contains("Parquet Segments Opened:")
            && !plan.contains("Parquet Segments Opened: 0"),
        "ASC LIMIT after hot prune must open cold for lower ids: {plan}"
    );
    assert!(
        plan.contains("Cold Compete Opens:")
            && !plan.contains("Cold Compete Opens: 0")
            && plan.contains("Cold Body Opens:")
            && !plan.contains("Cold Body Opens: 0"),
        "ASC cold emit must use compete-then-body late materialization: {plan}"
    );
    assert_eq!(
        spi_get_text(&format!(
            "SELECT string_agg(body, ',' ORDER BY id) FROM (\
             SELECT id, body FROM {relation} ORDER BY id ASC LIMIT 5\
             ) topn"
        )),
        "cold-1,cold-2,cold-3,cold-4,cold-5",
        "ORDER BY id ASC LIMIT 5 must emit cold winners when they outrank hot"
    );
}

#[pg_test]
fn ordered_limit_late_mat_skips_body_when_limit_is_hot_only() {
    let suffix = unique_suffix("ordered_late_mat_hot_only");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) \
         SELECT id, 'cold-' || id::text FROM generate_series(1, 30) AS id"
    ))
    .expect("insert cold seed");
    manage_for_cold_flush(&relation, &storage);
    assert!(flush_table_rows(&relation, true) >= 30);
    // Flush prune frees the heap PK slots; re-insert newer hot versions of the
    // lowest ids so ASC LIMIT can stop on hot after compete expands cold.
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) \
         SELECT id, 'hot-' || id::text FROM generate_series(1, 8) AS id"
    ))
    .expect("re-insert hot overrides for low ids");

    let plan = spi_get_explain(&format!(
        "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) \
         SELECT id, body FROM {relation} ORDER BY id ASC LIMIT 3"
    ));
    assert!(
        plan.contains("Emit Path: ordered_merge_native")
            && plan.contains("Strategy: Ordered Progressive"),
        "late-mat hot-only LIMIT must stay on ordered progressive: {plan}"
    );
    assert!(
        plan.contains("Cold Compete Opens:")
            && !plan.contains("Cold Compete Opens: 0"),
        "hot/cold overlap must compete so sort can prefer hot: {plan}"
    );
    assert!(
        plan.contains("Cold Body Opens: 0"),
        "parent LIMIT satisfied from hot after compete must skip body hydrate: {plan}"
    );
    assert!(
        plan.contains("Cold Body Columns:") && plan.contains("body"),
        "wide projection must arm late materialization body columns: {plan}"
    );
    assert_eq!(
        spi_get_text(&format!(
            "SELECT string_agg(body, ',' ORDER BY id) FROM (\
             SELECT id, body FROM {relation} ORDER BY id ASC LIMIT 3\
             ) topn"
        )),
        "hot-1,hot-2,hot-3",
        "ASC LIMIT after hot updates must return hot bodies without body opens"
    );
}

#[pg_test]
fn ordered_limit_narrow_select_fail_opens_to_full() {
    let suffix = unique_suffix("ordered_late_mat_narrow");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) \
         SELECT id, 'cold-' || id::text FROM generate_series(1000, 1010) AS id"
    ))
    .expect("insert high-id cold seed");
    manage_for_cold_flush(&relation, &storage);
    assert!(flush_table_rows(&relation, true) >= 11);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) \
         SELECT id, 'hot-' || id::text FROM generate_series(1, 10) AS id"
    ))
    .expect("insert low-id hot rows");

    let plan = spi_get_explain(&format!(
        "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) \
         SELECT id FROM {relation} ORDER BY id DESC LIMIT 5"
    ));
    assert!(
        plan.contains("Emit Path: ordered_merge_native")
            && plan.contains("Strategy: Ordered Progressive"),
        "narrow ordered LIMIT must stay on ordered progressive: {plan}"
    );
    assert!(
        plan.contains("Parquet Segments Opened:")
            && !plan.contains("Parquet Segments Opened: 0"),
        "narrow cold-wins LIMIT must still open Parquet: {plan}"
    );
    assert!(
        plan.contains("Cold Compete Opens: 0") && plan.contains("Cold Body Opens: 0"),
        "narrow SELECT id must fail-open to a single Full open (no compete/body split): {plan}"
    );
    assert!(
        !plan.contains("Cold Body Columns:"),
        "fail-open narrow projection must not advertise body columns: {plan}"
    );
    assert_eq!(
        spi_get_text(&format!(
            "SELECT string_agg(id::text, ',' ORDER BY id DESC) FROM (\
             SELECT id FROM {relation} ORDER BY id DESC LIMIT 5\
             ) topn"
        )),
        "1010,1009,1008,1007,1006",
        "narrow ordered LIMIT must still return cold-winning ids"
    );
}

#[pg_test]
fn unordered_limit_uses_hot_first_and_defers_cold() {
    let suffix = unique_suffix("unordered_limit");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) \
         SELECT id, 'cold-' || id::text FROM generate_series(1, 20) AS id"
    ))
    .expect("insert cold seed");
    manage_for_cold_flush(&relation, &storage);
    assert!(flush_table_rows(&relation, true) >= 20);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) \
         SELECT id, 'hot-' || id::text FROM generate_series(100, 200) AS id"
    ))
    .expect("insert hot rows");

    let plan = spi_get_explain(&format!(
        "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) \
         SELECT body FROM {relation} LIMIT 5"
    ));
    assert!(
        plan.contains("Strategy: Unordered Hot First")
            && plan.contains("Emit Path: unordered_hot_first"),
        "LIMIT without ORDER BY must use Unordered Hot First: {plan}"
    );
    assert!(
        plan.contains("Actual Access: Native PostgreSQL Child"),
        "unordered hot-first must use the native hot child, not SPI JSON: {plan}"
    );
    assert!(
        !plan.contains("SPI JSON Keyset Scan") && !plan.contains("to_jsonb(proj)"),
        "unordered hot-first must not use SPI JSON keyset paging: {plan}"
    );
    assert!(
        !plan.contains("Sort"),
        "unordered LIMIT must not invent pathkeys/Sort: {plan}"
    );
    assert!(
        plan.contains("Parquet Segments Opened: 0"),
        "LIMIT satisfied from hot must defer cold Parquet: {plan}"
    );
    assert!(
        plan.contains("Cold Compete Opens: 0") && plan.contains("Cold Body Opens: 0"),
        "unordered hot-first must not use ordered late materialization: {plan}"
    );
    let bodies = spi_get_text(&format!(
        "SELECT string_agg(body, ',') FROM (SELECT body FROM {relation} LIMIT 5) t"
    ));
    assert_eq!(bodies.split(',').count(), 5, "LIMIT 5 must return 5 rows: {bodies}");
    assert!(
        bodies.split(',').all(|b| b.starts_with("hot-")),
        "hot-first LIMIT should return hot bodies before opening cold: {bodies}"
    );
}

