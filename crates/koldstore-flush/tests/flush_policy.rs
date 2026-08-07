use koldstore_flush::policy::{flush_rows_for_excess, policy_flush_row_count, FlushPolicy};

#[test]
fn structured_schema_options_load_hot_row_limit_policy() {
    let policy = FlushPolicy::from_value(&serde_json::json!({
        "flush_policy": {
            "type": "row_limit",
            "hot_row_limit": 10_000,
            "min_flush_rows": 1_000,
            "max_rows_per_file": 500,
            "max_rows_per_flush": 10_000
        }
    }))
    .unwrap();

    assert!(matches!(
        policy,
        FlushPolicy::RowLimit {
            hot_row_limit: 10_000,
            min_flush_rows: 1_000,
            max_rows_per_file: 500,
            ..
        }
    ));
}

#[test]
fn flush_rows_for_excess_honors_min_flush_rows_threshold() {
    assert_eq!(flush_rows_for_excess(505, 1_000), 0);
    assert_eq!(flush_rows_for_excess(1_000, 1_000), 1_000);
    assert_eq!(flush_rows_for_excess(1_250, 1_000), 1_000);
    assert_eq!(flush_rows_for_excess(1_500, 1_000), 1_500);
}

#[test]
fn policy_flush_row_count_honors_hot_row_limit_and_min_flush_rows() {
    let policy = FlushPolicy::RowLimit {
        hot_row_limit: 25_000,
        min_flush_rows: 300,
        max_rows_per_file: 1_000,
        max_rows_per_flush: 30_000,
    };
    assert_eq!(policy_flush_row_count(50_000, &policy), 24_900);
    assert_eq!(policy_flush_row_count(25_000, &policy), 0);
}

#[test]
fn policy_flush_row_count_chunks_large_excess_like_row_selection_did() {
    let policy = FlushPolicy::RowLimit {
        hot_row_limit: 10_000,
        min_flush_rows: 1_000,
        max_rows_per_file: 500,
        max_rows_per_flush: 10_000,
    };
    assert_eq!(policy_flush_row_count(11_250, &policy), 1_000);
}

#[test]
fn policy_flush_row_count_skips_undersized_segment_below_max_rows_per_file() {
    // Docker demo shape: min_flush_rows=1 allows any excess, but a 450-row
    // selection must not enqueue when max_rows_per_file is 1000.
    let policy = FlushPolicy::RowLimit {
        hot_row_limit: 1_000,
        min_flush_rows: 1,
        max_rows_per_file: 1_000,
        max_rows_per_flush: 10_000,
    };
    assert_eq!(policy_flush_row_count(1_450, &policy), 0);
    assert_eq!(policy_flush_row_count(2_000, &policy), 1_000);
    assert_eq!(policy_flush_row_count(2_500, &policy), 1_500);
}

#[test]
fn policy_flush_row_count_recovers_half_chunk_undershoot_to_full_file() {
    // CI ai_memory shape: excess=1000, min_flush_rows=300 → half-chunk drops the
    // 100-row remainder (900), which is below max_rows_per_file=1000. Still flush
    // one full file because raw excess meets the floor.
    let policy = FlushPolicy::RowLimit {
        hot_row_limit: 1_000,
        min_flush_rows: 300,
        max_rows_per_file: 1_000,
        max_rows_per_flush: 10_000,
    };
    assert_eq!(policy_flush_row_count(2_000, &policy), 1_000);
}
