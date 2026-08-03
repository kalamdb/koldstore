// Change-feed benchmarks cover both the hot mirror and the streamed cold path.

const CHANGE_ROWS: i64 = 2_000;
const CHANGE_PAGE_SIZE: i64 = 25;

fn changes_since_count(relation: &str, since_seq: i64) -> i64 {
    spi_get_i64(&format!(
        "SELECT COUNT(*)::bigint FROM koldstore.changes_since(\
            table_name => '{relation}'::regclass, \
            since_seq => {since_seq}, \
            limit_rows => {CHANGE_PAGE_SIZE})"
    ))
}

fn assert_changes_since_page(relation: &str, since_seq: i64) {
    let count = changes_since_count(relation, since_seq);
    assert_eq!(
        count, CHANGE_PAGE_SIZE,
        "changes_since returned an incomplete page for {relation}, since_seq={since_seq}"
    );
}

fn prepare_changes_since_hot() {
    let relation = prepare_seeded_managed_messages("changes_since_hot", CHANGE_ROWS, false);
    assert_changes_since_page(&relation, 0);
}

fn prepare_changes_since_cold() {
    let relation = prepare_seeded_managed_messages("changes_since_cold", CHANGE_ROWS, true);
    let flushed = flush_table_rows(&relation);
    assert!(
        flushed > 0,
        "changes_since cold fixture did not flush any rows for {relation}"
    );
    // Sequence values are Snowflake IDs, not dense row numbers. Use the
    // retained cold segment boundary instead of inventing a numeric cursor.
    let cold_since = spi_get_i64(&format!(
        "SELECT COALESCE(MIN(min_seq), 0)::bigint \
         FROM koldstore.cold_segments \
         WHERE table_oid = '{relation}'::regclass"
    ));
    assert!(cold_since > 0, "changes_since cold fixture has no segment boundary");
    stash("cold_since", &(cold_since - 1).to_string());
    assert_changes_since_page(&relation, cold_since - 1);
    assert_changes_since_page(&relation, cold_since);
}

#[pg_bench(
    setup = prepare_changes_since_hot,
    sample_size = 30,
    measurement_time_ms = 2_000,
    warm_up_time_ms = 500
)]
fn changes_since_hot_mirror(b: &mut Bencher) {
    let relation = ctx("relation");
    b.iter(move || {
        let count = changes_since_count(&relation, 0);
        black_box(count);
    });
}

#[pg_bench(
    setup = prepare_changes_since_cold,
    sample_size = 30,
    measurement_time_ms = 2_000,
    warm_up_time_ms = 500
)]
fn changes_since_cold_first_page(b: &mut Bencher) {
    let relation = ctx("relation");
    let since_seq = ctx("cold_since").parse::<i64>().expect("cold cursor");
    b.iter(move || {
        let count = changes_since_count(&relation, since_seq);
        black_box(count);
    });
}

#[pg_bench(
    setup = prepare_changes_since_cold,
    sample_size = 30,
    measurement_time_ms = 2_000,
    warm_up_time_ms = 500
)]
fn changes_since_cold_cursor_page(b: &mut Bencher) {
    let relation = ctx("relation");
    let since_seq = ctx("cold_since").parse::<i64>().expect("cold cursor") + 1;
    b.iter(move || {
        let count = changes_since_count(&relation, since_seq);
        black_box(count);
    });
}
