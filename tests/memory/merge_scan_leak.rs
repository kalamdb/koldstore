//! Unit coverage for merge-scan retained-growth arithmetic.

use koldstore_memory::{
    evaluate_growth, repeated_scan_releases_resources, GrowthBudget, MemorySnapshot, PeakAllocation,
};

#[test]
fn merge_scan_leak_probe_detects_retained_growth() {
    let before = MemorySnapshot {
        rss_bytes: 10_000_000,
        pg_context_bytes: 1_000_000,
        allocator_bytes: Some(256_000),
    };
    let peak = MemorySnapshot {
        rss_bytes: 14_000_000,
        pg_context_bytes: 3_000_000,
        allocator_bytes: Some(512_000),
    };
    let after = before;
    let released = PeakAllocation {
        before,
        peak,
        after,
    };
    assert_eq!(released.retained_growth_bytes(), 0);
    assert!(repeated_scan_releases_resources([released]));

    let retained = PeakAllocation {
        before,
        peak,
        after: MemorySnapshot {
            rss_bytes: 12_000_000,
            pg_context_bytes: 2_500_000,
            allocator_bytes: Some(400_000),
        },
    };
    assert!(retained.retained_growth_bytes() > 0);
    assert!(!repeated_scan_releases_resources([retained]));
}

#[test]
fn merge_scan_leak_probe_flags_monotone_context_growth() {
    let samples = (0..6)
        .map(|cycle| MemorySnapshot {
            rss_bytes: 20_000_000,
            pg_context_bytes: 500_000 + cycle * 3_000_000,
            allocator_bytes: None,
        })
        .collect::<Vec<_>>();
    let evaluation = evaluate_growth(&samples).expect("samples");
    assert!(
        !evaluation.within_budget(GrowthBudget {
            max_pg_context_growth_bytes: 2 * 1024 * 1024,
            max_rss_growth_bytes: 64 * 1024 * 1024,
            max_pg_context_bytes_per_cycle: 512 * 1024,
            max_rss_bytes_per_cycle: 8 * 1024 * 1024,
        }),
        "monotone merge-scan context growth must fail: {evaluation:?}"
    );
}
