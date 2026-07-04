use std::collections::BTreeMap;
use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};
use koldstore_core::{CommitSeq, SeqId};
use koldstore_parquet::{
    ColumnStats, FooterSummary, RowGroupPruner, RowGroupStats, SegmentFooterMetadata,
};
use serde_json::json;

fn bench_row_group_pruning(c: &mut Criterion) {
    let footer = footer_with_row_groups(100_000);
    let pruner = RowGroupPruner;

    c.bench_function("row_group_seq_range_pruning", |b| {
        b.iter(|| {
            pruner.prune_seq_range(
                black_box(&footer),
                SeqId::new(10_000).expect("valid seq"),
                SeqId::new(30_000).expect("valid seq"),
            )
        })
    });

    c.bench_function("row_group_commit_seq_pruning", |b| {
        b.iter(|| {
            pruner.prune_commit_seq_range(
                black_box(&footer),
                CommitSeq::new(10_000).expect("valid commit seq"),
                CommitSeq::new(30_000).expect("valid commit seq"),
            )
        })
    });
}

fn bench_pk_and_minmax_pruning(c: &mut Criterion) {
    let footer = footer_with_row_groups(100_000);
    let bloom_values = bloom_values_for_groups(100_000);
    let pruner = RowGroupPruner;

    c.bench_function("pk_bloom_lookup_hit", |b| {
        b.iter(|| pruner.prune_pk_values(black_box(&footer), black_box(&bloom_values), ["user-42"]))
    });

    c.bench_function("pk_bloom_lookup_miss", |b| {
        b.iter(|| {
            pruner.prune_pk_values(
                black_box(&footer),
                black_box(&bloom_values),
                ["missing-user"],
            )
        })
    });

    let column_stats = BTreeMap::from([(
        "created_day".to_string(),
        ColumnStats {
            min: json!(90),
            max: json!(180),
        },
    )]);
    c.bench_function("segment_minmax_overlap_check", |b| {
        b.iter(|| {
            pruner.segment_column_may_overlap(
                black_box(&column_stats),
                "created_day",
                &json!(120),
                &json!(121),
            )
        })
    });
}

fn bench_footer_metadata_parsing(c: &mut Criterion) {
    let footer = footer_with_row_groups(10_000);
    let column_stats = vec![(
        "created_day".to_string(),
        ColumnStats {
            min: json!(0),
            max: json!(365),
        },
    )];

    c.bench_function("parquet_footer_metadata_parsing", |b| {
        b.iter(|| {
            SegmentFooterMetadata::from_footer(
                black_box(&footer),
                10_000_000,
                512 * 1024 * 1024,
                1,
                black_box(column_stats.clone()),
            )
        })
    });
}

fn footer_with_row_groups(count: usize) -> FooterSummary {
    FooterSummary {
        row_groups: (0..count)
            .map(|idx| {
                let min = (idx as i64 * 1_000) + 1;
                RowGroupStats {
                    row_group: idx,
                    min_seq: Some(min),
                    max_seq: Some(min + 999),
                    min_commit_seq: Some(min),
                    max_commit_seq: Some(min + 999),
                }
            })
            .collect(),
    }
}

fn bloom_values_for_groups(count: usize) -> BTreeMap<usize, Vec<String>> {
    (0..count)
        .map(|idx| (idx, vec![format!("user-{}", idx % 2_500)]))
        .collect()
}

criterion_group!(
    benches,
    bench_row_group_pruning,
    bench_pk_and_minmax_pruning,
    bench_footer_metadata_parsing
);
criterion_main!(benches);
