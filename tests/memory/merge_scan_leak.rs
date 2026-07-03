#[path = "memory_probe.rs"]
mod memory_probe;

#[test]
fn merge_scan_leak_probe_detects_retained_growth() {
    let before = memory_probe::MemorySnapshot::empty();
    let peak = memory_probe::MemorySnapshot {
        rss_bytes: 1024,
        pg_context_bytes: 512,
        allocator_bytes: Some(128),
    };
    let after = memory_probe::MemorySnapshot::empty();
    let allocation = memory_probe::PeakAllocation { before, peak, after };

    assert_eq!(allocation.retained_growth_bytes(), 0);
}

