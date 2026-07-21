// Cold-path multi-type query + join coverage (~15 cases).
// Cold filters must use indexed columns (PK + secondary indexes created before manage).
// Prefer `id IN (1..7)` over inequalities for cold-only row sets.

#[pg_test]
fn cold_query_01_count_cold_rows() {
    let fx = setup_cold_typed_join_fixture("cq01");
    assert_finishes_under(COLD_QUERY_BUDGET, || {
        let cold = spi_get_i64(&format!(
            "SELECT COUNT(*)::bigint FROM {facts} WHERE id IN ({COLD_FACT_IDS})",
            facts = fx.facts
        ));
        assert_eq!(cold, 7);
        let all = spi_get_i64(&format!(
            "SELECT COUNT(*)::bigint FROM {facts}",
            facts = fx.facts
        ));
        assert_eq!(all, fx.row_count);
    });
}

#[pg_test]
fn cold_query_02_pk_lookup_multi_type_roundtrip() {
    let fx = setup_cold_typed_join_fixture("cq02");
    let payload = jsonb_obj("payload");
    assert_finishes_under(COLD_QUERY_BUDGET, || {
        let row = spi_get_text(&format!(
            r#"
            SELECT concat_ws('|',
              id::text,
              flag::text,
              qty::text,
              amount::text,
              note,
              tag::text,
              {payload}->>'k',
              category
            )
            FROM {facts}
            WHERE id = 3
            "#,
            facts = fx.facts
        ));
        assert_eq!(
            row,
            "3|true|30|30.25|charlie|33333333-3333-3333-3333-333333333333|v3|odd"
        );
    });
}

#[pg_test]
fn cold_query_03_in_list_order_limit() {
    let fx = setup_cold_typed_join_fixture("cq03");
    assert_finishes_under(COLD_QUERY_BUDGET, || {
        let notes = spi_get_text(&format!(
            r#"
            SELECT string_agg(note, ',' ORDER BY id)
            FROM (
              SELECT id, note FROM {facts}
              WHERE id IN (2,3,4,5)
              ORDER BY id DESC
              LIMIT 3
            ) s
            "#,
            facts = fx.facts
        ));
        assert_eq!(notes, "charlie,delta,echo");
    });
}

#[pg_test]
fn cold_query_04_group_by_aggregate() {
    let fx = setup_cold_typed_join_fixture("cq04");
    assert_finishes_under(COLD_QUERY_BUDGET, || {
        let summary = spi_get_text(&format!(
            r#"
            SELECT string_agg(category || ':' || cnt::text, ',' ORDER BY category)
            FROM (
              SELECT category, COUNT(*)::bigint AS cnt
              FROM {facts}
              WHERE id IN ({COLD_FACT_IDS})
              GROUP BY category
            ) g
            "#,
            facts = fx.facts
        ));
        assert_eq!(summary, "even:3,odd:4");
    });
}

#[pg_test]
fn cold_query_05_distinct_and_in_list() {
    let fx = setup_cold_typed_join_fixture("cq05");
    assert_finishes_under(COLD_QUERY_BUDGET, || {
        let cats = spi_get_text(&format!(
            "SELECT string_agg(category, ',' ORDER BY category) FROM (
               SELECT DISTINCT category FROM {facts} WHERE id IN (1,2,6)
             ) d",
            facts = fx.facts
        ));
        assert_eq!(cats, "even,odd");
    });
}

#[pg_test]
fn cold_query_06_jsonb_project_and_uuid_filter() {
    let fx = setup_cold_typed_join_fixture("cq06");
    let payload = jsonb_obj("payload");
    assert_finishes_under(COLD_QUERY_BUDGET, || {
        // Filter on indexed uuid `tag`; project jsonb (unwrap string-scalar cold encoding).
        let k = spi_get_text(&format!(
            r#"
            SELECT {payload}->>'k'
            FROM {facts}
            WHERE tag = '11111111-1111-1111-1111-111111111111'::uuid
            "#,
            facts = fx.facts
        ));
        assert_eq!(k, "v1");
        let labels = spi_get_text(&format!(
            r#"
            SELECT {payload}->'labels'->>0
            FROM {facts}
            WHERE id = 5
            "#,
            facts = fx.facts
        ));
        assert_eq!(labels, "a");
    });
}

#[pg_test]
fn cold_query_07_null_safe_filters_and_coalesce() {
    let fx = setup_cold_typed_join_fixture("cq07");
    assert_finishes_under(COLD_QUERY_BUDGET, || {
        let note = spi_get_text(&format!(
            r#"
            SELECT coalesce(note, 'missing')
            FROM {facts}
            WHERE id = 7 AND flag IS NULL AND amount IS NULL
            "#,
            facts = fx.facts
        ));
        assert_eq!(note, "missing");
    });
}

#[pg_test]
fn cold_query_08_note_and_timestamptz_predicates() {
    let fx = setup_cold_typed_join_fixture("cq08");
    assert_finishes_under(COLD_QUERY_BUDGET, || {
        // `note` and `ts` are indexed so cold pushdown is allowed.
        let ids = spi_get_text(&format!(
            r#"
            SELECT string_agg(id::text, ',' ORDER BY id)
            FROM {facts}
            WHERE note IN ('alpha','bravo','charlie','delta')
              AND ts >= '2024-01-01 00:00:00+00'::timestamptz
              AND ts <  '2024-01-05 00:00:00+00'::timestamptz
            "#,
            facts = fx.facts
        ));
        assert_eq!(ids, "1,2,3,4");
    });
}

#[pg_test]
fn cold_query_09_prepared_statement_pk_lookup() {
    let fx = setup_cold_typed_join_fixture("cq09");
    let suffix = unique_suffix("prep");
    assert_finishes_under(COLD_QUERY_BUDGET, || {
        Spi::run(&format!(
            "PREPARE cold_prep_{suffix} AS SELECT note FROM {facts} WHERE id = $1",
            facts = fx.facts
        ))
        .expect("prepare");
        assert_eq!(
            spi_get_text(&format!("EXECUTE cold_prep_{suffix}(5)")),
            "echo"
        );
        assert_eq!(
            spi_get_text(&format!("EXECUTE cold_prep_{suffix}(2)")),
            "bravo"
        );
    });
}

#[pg_test]
fn cold_query_10_inner_join_plain_dim() {
    let fx = setup_cold_typed_join_fixture("cq10");
    assert_finishes_under(COLD_QUERY_BUDGET, || {
        let accounts = spi_get_text(&format!(
            "SELECT string_agg(id::text || ':' || account_id::text, ',' ORDER BY id)
             FROM {facts} WHERE id IN ({COLD_FACT_IDS})",
            facts = fx.facts
        ));
        assert_eq!(accounts, "1:1,2:1,3:2,4:2,5:3,6:3,7:3");

        let pairs = spi_get_text(&format!(
            r#"
            WITH f AS MATERIALIZED (
              SELECT id, account_id FROM {facts} WHERE id IN ({COLD_FACT_IDS})
            )
            SELECT string_agg(f.id::text || ':' || a.account_name, ',' ORDER BY f.id, a.account_name)
            FROM f
            INNER JOIN {accounts} a ON f.account_id = a.account_id
            "#,
            facts = fx.facts,
            accounts = fx.accounts
        ));
        assert_eq!(
            pairs,
            "1:acct-one,2:acct-one,3:acct-two,4:acct-two,5:acct-three,6:acct-three,7:acct-three"
        );
    });
}

#[pg_test]
fn cold_query_11_left_join_plain_dim() {
    let fx = setup_cold_typed_join_fixture("cq11");
    assert_finishes_under(COLD_QUERY_BUDGET, || {
        let orphan = spi_get_text(&format!(
            r#"
            SELECT coalesce(a.account_name, 'none')
            FROM {facts} f
            LEFT JOIN {accounts} a ON f.account_id = a.account_id
            WHERE f.id = 8
            "#,
            facts = fx.facts,
            accounts = fx.accounts
        ));
        assert_eq!(orphan, "none");
        let cold_matched = spi_get_i64(&format!(
            r#"
            SELECT COUNT(*)::bigint
            FROM {facts} f
            LEFT JOIN {accounts} a ON f.account_id = a.account_id
            WHERE f.id IN ({COLD_FACT_IDS})
            "#,
            facts = fx.facts,
            accounts = fx.accounts
        ));
        assert_eq!(cold_matched, 7);
    });
}

#[pg_test]
fn cold_query_12_right_join_plain_dim() {
    let fx = setup_cold_typed_join_fixture("cq12");
    assert_finishes_under(COLD_QUERY_BUDGET, || {
        let orphan_dim = spi_get_text(&format!(
            r#"
            WITH f AS MATERIALIZED (
              SELECT id, account_id FROM {facts} WHERE id IN ({COLD_FACT_IDS})
            )
            SELECT a.account_name
            FROM {accounts} a
            LEFT JOIN f ON f.account_id = a.account_id
            WHERE a.account_id = 50
            "#,
            facts = fx.facts,
            accounts = fx.accounts
        ));
        assert_eq!(orphan_dim, "acct-orphan-dim");
        let rows = spi_get_i64(&format!(
            r#"
            WITH f AS MATERIALIZED (
              SELECT id, account_id FROM {facts} WHERE id IN ({COLD_FACT_IDS})
            )
            SELECT COUNT(*)::bigint
            FROM {accounts} a
            LEFT JOIN f ON f.account_id = a.account_id
            "#,
            facts = fx.facts,
            accounts = fx.accounts
        ));
        assert_eq!(rows, 8);
    });
}

#[pg_test]
fn cold_query_13_full_join_plain_dim() {
    let fx = setup_cold_typed_join_fixture("cq13");
    assert_finishes_under(COLD_QUERY_BUDGET, || {
        let rows = spi_get_i64(&format!(
            r#"
            SELECT COUNT(*)::bigint
            FROM {facts} f
            FULL JOIN {accounts} a ON f.account_id = a.account_id
            WHERE f.id IN ({COLD_FACT_IDS}) OR f.id IS NULL
            "#,
            facts = fx.facts,
            accounts = fx.accounts
        ));
        // cold facts 1..7 + orphan dim 50
        assert_eq!(rows, 8);
        let dim_orphan = spi_get_i64(&format!(
            r#"
            SELECT COUNT(*)::bigint
            FROM {facts} f
            FULL JOIN {accounts} a ON f.account_id = a.account_id
            WHERE f.id IS NULL AND a.account_id = 50
            "#,
            facts = fx.facts,
            accounts = fx.accounts
        ));
        assert_eq!(dim_orphan, 1);
    });
}

#[pg_test]
fn cold_query_14_cross_join_limited() {
    let fx = setup_cold_typed_join_fixture("cq14");
    assert_finishes_under(COLD_QUERY_BUDGET, || {
        let rows = spi_get_i64(&format!(
            r#"
            SELECT COUNT(*)::bigint
            FROM {facts} f
            CROSS JOIN {accounts} a
            WHERE f.id IN (1,2) AND a.account_id IN (1,2)
            "#,
            facts = fx.facts,
            accounts = fx.accounts
        ));
        assert_eq!(rows, 4);
    });
}

#[pg_test]
fn cold_query_15_exists_semi_join_and_union() {
    let fx = setup_cold_typed_join_fixture("cq15");
    assert_finishes_under(COLD_QUERY_BUDGET, || {
        let exists_ids = spi_get_text(&format!(
            r#"
            WITH f AS MATERIALIZED (
              SELECT id, account_id FROM {facts} WHERE id IN ({COLD_FACT_IDS})
            )
            SELECT string_agg(id::text, ',' ORDER BY id)
            FROM f
            WHERE f.account_id IN (SELECT account_id FROM {accounts})
            "#,
            facts = fx.facts,
            accounts = fx.accounts
        ));
        assert_eq!(exists_ids, "1,2,3,4,5,6,7");

        let unioned = spi_get_i64(&format!(
            r#"
            SELECT COUNT(*)::bigint FROM (
              SELECT id FROM {facts}
              WHERE id IN ({COLD_FACT_IDS}) AND category = 'odd'
              UNION ALL
              SELECT id FROM {facts}
              WHERE id IN ({COLD_FACT_IDS}) AND category = 'even'
            ) u
            "#,
            facts = fx.facts
        ));
        assert_eq!(unioned, 7);
    });
}
