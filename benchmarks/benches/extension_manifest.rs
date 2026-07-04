use std::collections::BTreeMap;
use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use koldstore_manifest::{Manifest, ManifestBloomFilter, ManifestColumnStats, ManifestSegment};
use serde_json::json;

fn bench_manifest_segment_pruning(c: &mut Criterion) {
    let mut group = c.benchmark_group("manifest_segment_pruning");
    for size in [1_000usize, 10_000, 100_000] {
        let segments = build_segments(size);
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            &segments,
            |b, segments| b.iter(|| select_segments_by_created_day(black_box(segments), 90, 180)),
        );
    }
    group.finish();
}

fn bench_cold_lookup_and_miss(c: &mut Criterion) {
    let segments = build_segments(100_000);
    let mut group = c.benchmark_group("cold_segment_lookup");
    group.bench_function("lookup_by_user_and_time_range", |b| {
        b.iter(|| lookup_by_user_and_time(black_box(&segments), "user-42", 90, 180))
    });
    group.bench_function("cold_miss_by_user_and_time_range", |b| {
        b.iter(|| lookup_by_user_and_time(black_box(&segments), "user-missing", 900, 1200))
    });
    group.finish();
}

fn bench_manifest_metadata(c: &mut Criterion) {
    let segments = build_segments(10_000);
    let mut manifest = Manifest::new_shared("bench", "bench_events", 1);
    let _ = manifest.append_segment_batch(segments.clone());

    c.bench_function("manifest_metadata_serialization", |b| {
        b.iter(|| serde_json::to_vec(black_box(&manifest)).expect("manifest serializes"))
    });

    let serialized = serde_json::to_vec(&manifest).expect("manifest serializes");
    c.bench_function("manifest_metadata_deserialization", |b| {
        b.iter(|| {
            serde_json::from_slice::<Manifest>(black_box(&serialized)).expect("manifest parses")
        })
    });

    c.bench_function("bloom_filter_metadata_check", |b| {
        b.iter(|| count_segments_with_user_bloom(black_box(&segments)))
    });

    c.bench_function("manifest_minmax_overlap", |b| {
        b.iter(|| select_segments_by_created_day(black_box(&segments), 120, 121))
    });
}

fn build_segments(count: usize) -> Vec<ManifestSegment> {
    (0..count)
        .map(|idx| {
            let min_seq = (idx as i64 * 1_000) + 1;
            let max_seq = min_seq + 999;
            let mut segment = ManifestSegment::committed(
                idx as u32,
                format!("bench/bench_events/batch-{idx}.parquet"),
                min_seq..=max_seq,
                min_seq..=max_seq,
                1_000,
                128 * 1024,
                1,
            );
            segment.column_stats = column_stats(idx);
            segment.bloom_filters.push(ManifestBloomFilter::bloom(
                vec!["user_id".to_string()],
                Some(0.01),
            ));
            segment
        })
        .collect()
}

fn column_stats(idx: usize) -> BTreeMap<String, ManifestColumnStats> {
    let day = (idx % 365) as i64;
    let user = format!("user-{}", idx % 2_500);
    BTreeMap::from([
        (
            "created_day".to_string(),
            ManifestColumnStats::new(json!(day), json!(day + 1)),
        ),
        (
            "user_id".to_string(),
            ManifestColumnStats::new(json!(user), json!(user)),
        ),
    ])
}

fn select_segments_by_created_day(
    segments: &[ManifestSegment],
    min_day: i64,
    max_day: i64,
) -> usize {
    segments
        .iter()
        .filter(|segment| {
            let Some(stats) = segment.column_stats.get("created_day") else {
                return true;
            };
            let Some(segment_min) = stats.min.as_i64() else {
                return true;
            };
            let Some(segment_max) = stats.max.as_i64() else {
                return true;
            };
            segment_max >= min_day && segment_min <= max_day
        })
        .count()
}

fn lookup_by_user_and_time(
    segments: &[ManifestSegment],
    user_id: &str,
    min_day: i64,
    max_day: i64,
) -> usize {
    segments
        .iter()
        .filter(|segment| {
            let user_matches = segment
                .column_stats
                .get("user_id")
                .and_then(|stats| stats.min.as_str())
                .is_none_or(|segment_user| segment_user == user_id);
            user_matches
        })
        .filter(|segment| {
            let Some(stats) = segment.column_stats.get("created_day") else {
                return true;
            };
            let segment_min = stats.min.as_i64().unwrap_or(i64::MIN);
            let segment_max = stats.max.as_i64().unwrap_or(i64::MAX);
            segment_max >= min_day && segment_min <= max_day
        })
        .count()
}

fn count_segments_with_user_bloom(segments: &[ManifestSegment]) -> usize {
    segments
        .iter()
        .filter(|segment| {
            segment
                .bloom_filters
                .iter()
                .any(|filter| filter.columns.iter().any(|column| column == "user_id"))
        })
        .count()
}

criterion_group!(
    benches,
    bench_manifest_segment_pruning,
    bench_cold_lookup_and_miss,
    bench_manifest_metadata
);
criterion_main!(benches);
