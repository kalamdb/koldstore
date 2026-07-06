use std::collections::BTreeMap;
use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use koldstore_core::{CommitSeq, SeqId, StablePkHash, TableName};
use koldstore_mirror::{mirror_relation_for_source, plan_mirror_stats};
use pg_koldstore::{
    flush::job::{
        conditional_cleanup_allowed, plan_cold_pk_hint_updates, FlushBatchBuilder, FlushBatchInput,
        FlushExecutionConfig, FlushWatermark, HotRowCandidate,
    },
    migrate::QualifiedTableName,
    spi::{mirror_to_spi, prepared_plan_key},
    sql::events::plan_mirror_changes_since,
};
use serde_json::json;

fn bench_flush_candidate_selection(c: &mut Criterion) {
    let mut group = c.benchmark_group("flush_candidate_selection");
    for rows in [1_000usize, 10_000, 100_000] {
        let input = flush_input(rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &input, |b, input| {
            b.iter(|| black_box(input.clone()).plan())
        });
    }
    group.finish();
}

fn bench_flush_batch_builder(c: &mut Criterion) {
    let config =
        FlushExecutionConfig::new(10_000, 128 * 1024 * 1024, 8, 60).expect("valid flush config");
    let rows = hot_candidates(10_000);

    c.bench_function("bounded_flush_batch_builder", |b| {
        b.iter(|| {
            let mut builder = FlushBatchBuilder::new(config);
            for row in black_box(&rows) {
                let _ = builder.push(row.clone(), 512);
            }
            builder.finish()
        })
    });
}

fn bench_flush_metadata(c: &mut Criterion) {
    let plan = flush_input(10_000).plan();
    c.bench_function("flush_footer_summary", |b| {
        b.iter(|| black_box(&plan).footer_summary())
    });
    c.bench_function("flush_column_stats", |b| {
        b.iter(|| black_box(&plan).column_stats(["created_day", "priority"]))
    });
    c.bench_function("cold_pk_hint_update_generation", |b| {
        b.iter(|| plan_cold_pk_hint_updates(42, None, black_box(&plan), "exact"))
    });
}

fn bench_cleanup_policy(c: &mut Criterion) {
    let candidate = candidate(42, false);
    let watermark = FlushWatermark::new(SeqId::new(100).expect("valid seq"));
    c.bench_function("conditional_hot_cleanup_allowed", |b| {
        b.iter(|| {
            conditional_cleanup_allowed(
                black_box(&candidate),
                SeqId::new(42).expect("valid seq"),
                CommitSeq::new(42).expect("valid commit seq"),
                watermark,
            )
        })
    });
}

fn bench_spi_plan_cache_shapes(c: &mut Criterion) {
    let source = TableName::parse("app.items").expect("valid table name");
    let mirror = mirror_relation_for_source(&source).expect("valid mirror relation");
    let cached_flush_stats = mirror_to_spi(plan_mirror_stats(&mirror)).expect("valid statement");
    let cached_flush_key = prepared_plan_key(&cached_flush_stats);

    c.bench_function("one_shot_flush_stats_statement_key", |b| {
        b.iter(|| {
            let statement =
                mirror_to_spi(plan_mirror_stats(black_box(&mirror))).expect("valid statement");
            prepared_plan_key(black_box(&statement))
        })
    });
    c.bench_function("cached_flush_stats_statement_key", |b| {
        b.iter(|| black_box(&cached_flush_key))
    });

    let table = QualifiedTableName::parse("app.items").expect("valid table");
    let mirror_table = QualifiedTableName::parse("koldstore.items__cl").expect("valid mirror");
    let primary_key = vec!["tenant_id".to_string(), "id".to_string()];
    let cached_changes_since =
        plan_mirror_changes_since(&table, &mirror_table, &primary_key, Some("tenant_id"))
            .expect("valid changes_since plan");
    let cached_changes_key = prepared_plan_key(&cached_changes_since.statement);

    c.bench_function("one_shot_changes_since_statement_key", |b| {
        b.iter(|| {
            let plan = plan_mirror_changes_since(
                black_box(&table),
                black_box(&mirror_table),
                black_box(&primary_key),
                Some("tenant_id"),
            )
            .expect("valid changes_since plan");
            prepared_plan_key(black_box(&plan.statement))
        })
    });
    c.bench_function("cached_changes_since_statement_key", |b| {
        b.iter(|| black_box(&cached_changes_key))
    });
}

fn flush_input(rows: usize) -> FlushBatchInput {
    FlushBatchInput {
        batch_size: rows,
        rows: hot_candidates(rows),
    }
}

fn hot_candidates(rows: usize) -> Vec<HotRowCandidate> {
    (1..=rows)
        .map(|idx| candidate(idx as i64, idx % 20 == 0))
        .collect()
}

fn candidate(seq: i64, deleted: bool) -> HotRowCandidate {
    let pk_hash = StablePkHash::from_hex(format!("{seq:064x}")).expect("valid hash");
    let seq_id = SeqId::new(seq).expect("valid seq");
    let commit_seq = CommitSeq::new(seq).expect("valid commit seq");
    let base = if deleted {
        HotRowCandidate::tombstone(pk_hash, seq_id, commit_seq)
    } else {
        HotRowCandidate::live(pk_hash, seq_id, commit_seq)
    };
    base.with_column_values(BTreeMap::from([
        ("created_day", json!(seq % 365)),
        ("priority", json!(seq % 10)),
    ]))
}

criterion_group!(
    benches,
    bench_flush_candidate_selection,
    bench_flush_batch_builder,
    bench_flush_metadata,
    bench_cleanup_policy,
    bench_spi_plan_cache_shapes
);
criterion_main!(benches);
