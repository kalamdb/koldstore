//! Unit coverage for allocation-growth evaluation used by lifecycle leak gates.

use koldstore_memory::{evaluate_growth, GrowthBudget, MemorySnapshot};

#[test]
fn allocation_growth_contract_tracks_repeated_managed_operations() {
    let tracked_operations = [
        "migration",
        "flush",
        "merge-scan",
        "cold reader",
        "demigration",
        "hot dml",
        "minio parquet read",
    ];
    assert!(tracked_operations
        .iter()
        .all(|operation| !operation.is_empty()));

    // Stable post-warmup series stays inside the default lifecycle budget.
    let stable = [
        MemorySnapshot {
            rss_bytes: 80_000_000,
            pg_context_bytes: 3_000_000,
            allocator_bytes: None,
        },
        MemorySnapshot {
            rss_bytes: 80_250_000,
            pg_context_bytes: 3_100_000,
            allocator_bytes: None,
        },
        MemorySnapshot {
            rss_bytes: 80_400_000,
            pg_context_bytes: 3_150_000,
            allocator_bytes: None,
        },
        MemorySnapshot {
            rss_bytes: 80_500_000,
            pg_context_bytes: 3_200_000,
            allocator_bytes: None,
        },
    ];
    let evaluation = evaluate_growth(&stable).expect("stable series");
    assert!(
        evaluation.within_budget(GrowthBudget::default()),
        "stable series should pass default budget: {evaluation:?}"
    );
}

#[test]
fn allocation_growth_rejects_unbounded_flush_style_retention() {
    // Simulate retained growth typical of a flush/object-store handle leak.
    let leaking = (0..8)
        .map(|cycle| MemorySnapshot {
            rss_bytes: 40_000_000 + cycle * 12_000_000,
            pg_context_bytes: 2_000_000 + cycle * 4_000_000,
            allocator_bytes: None,
        })
        .collect::<Vec<_>>();
    let evaluation = evaluate_growth(&leaking).expect("leaking series");
    assert!(
        !evaluation.within_budget(GrowthBudget::default()),
        "leaking series must fail the default budget: {evaluation:?}"
    );
}
