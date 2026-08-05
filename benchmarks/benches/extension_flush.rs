use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};
use koldstore::spi::prepared_plan_key;
use koldstore_common::TableName;
use koldstore_merge::events::plan_mirror_changes_since;
use koldstore_migrate::QualifiedTableName;
use koldstore_mirror::{mirror_relation_for_source, plan_mirror_stats};

fn bench_spi_plan_cache_shapes(c: &mut Criterion) {
    let source = TableName::parse("app.items").expect("valid table name");
    let mirror = mirror_relation_for_source(&source).expect("valid mirror relation");
    let cached_flush_stats = plan_mirror_stats(&mirror).expect("valid statement");
    let cached_flush_key = prepared_plan_key(&cached_flush_stats);

    c.bench_function("one_shot_flush_stats_statement_key", |b| {
        b.iter(|| {
            let statement = plan_mirror_stats(black_box(&mirror)).expect("valid statement");
            prepared_plan_key(black_box(&statement))
        })
    });
    c.bench_function("cached_flush_stats_statement_key", |b| {
        b.iter(|| black_box(&cached_flush_key))
    });

    let mirror_table = QualifiedTableName::parse("koldstore.items__cl").expect("valid mirror");
    let primary_key = vec!["tenant_id".to_string(), "id".to_string()];
    let cached_changes_since =
        plan_mirror_changes_since(&mirror_table, &primary_key, Some("tenant_id"))
            .expect("valid changes_since plan");
    let cached_changes_key = prepared_plan_key(&cached_changes_since.statement);

    c.bench_function("one_shot_changes_since_statement_key", |b| {
        b.iter(|| {
            let plan = plan_mirror_changes_since(
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

criterion_group!(benches, bench_spi_plan_cache_shapes);
criterion_main!(benches);
