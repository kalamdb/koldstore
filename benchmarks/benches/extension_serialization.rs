use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};
use koldstore::merge_scan::plan::{MergeScanPlan, SegmentHint};
use koldstore_catalog::FlushPolicy;
use koldstore_common::{ColdRow, CommitSeq, HotRow, LogicalPk, PkColumn, PkValue, ScopeKey, SeqId};
use koldstore_merge::resolve_rows;
use koldstore_storage::PathTemplate;
use serde_json::json;

fn bench_merge_plan_serialization(c: &mut Criterion) {
    let plan = merge_scan_plan(1_000);
    c.bench_function("merge_scan_plan_serialize", |b| {
        b.iter(|| black_box(&plan).serialize().expect("plan serializes"))
    });

    let serialized = plan.serialize().expect("plan serializes");
    c.bench_function("merge_scan_plan_deserialize", |b| {
        b.iter(|| MergeScanPlan::deserialize(black_box(&serialized)).expect("plan parses"))
    });
}

fn bench_hot_cold_deduplication(c: &mut Criterion) {
    let hot = hot_rows(10_000);
    let cold = cold_rows(10_000);
    c.bench_function("deduplicate_hot_and_cold_by_primary_key", |b| {
        b.iter(|| resolve_rows(black_box(&hot), black_box(&cold)))
    });
}

fn bench_path_and_policy(c: &mut Criterion) {
    let template = PathTemplate::new("{namespace}/{tableName}/{scopeId}/");
    c.bench_function("cold_object_path_generation", |b| {
        b.iter(|| {
            render_cold_path(
                black_box(&template),
                "bench",
                "bench_events",
                Some("user-42"),
                7,
            )
        })
    });

    let policy = FlushPolicy::new(10_000, 1, 1_000);
    c.bench_function("policy_evaluation_for_hot_retention", |b| {
        b.iter(|| should_flush_by_policy(black_box(&policy), 12_000))
    });
}

fn bench_query_mode_decision(c: &mut Criterion) {
    let request = QueryRequest {
        has_cold_segments: true,
        overlaps_hot_range: true,
        overlaps_cold_range: true,
        cold_api_enabled: true,
    };
    c.bench_function("query_mode_hot_only", |b| {
        let mut request = request;
        request.overlaps_cold_range = false;
        b.iter(|| decide_query_mode(black_box(request)))
    });
    c.bench_function("query_mode_hot_cold", |b| {
        b.iter(|| decide_query_mode(black_box(request)))
    });
    c.bench_function("query_mode_cold_only", |b| {
        let mut request = request;
        request.overlaps_hot_range = false;
        b.iter(|| decide_query_mode(black_box(request)))
    });
    c.bench_function("query_mode_skip_cold", |b| {
        let mut request = request;
        request.cold_api_enabled = false;
        request.overlaps_hot_range = false;
        b.iter(|| decide_query_mode(black_box(request)))
    });
}

fn merge_scan_plan(segment_count: usize) -> MergeScanPlan {
    let mut plan = MergeScanPlan::new(42, vec!["id".to_string()]);
    plan.scanrelid = 1;
    plan.scope_key = Some(ScopeKey::new("user-42").expect("valid scope"));
    plan.projection = vec![
        "id".to_string(),
        "user_id".to_string(),
        "created_at".to_string(),
        "payload".to_string(),
    ];
    plan.segment_hints = (0..segment_count)
        .map(|idx| SegmentHint {
            segment_id: format!("segment-{idx}"),
            scope_key: Some(ScopeKey::new("user-42").expect("valid scope")),
            object_path: format!("bench/bench_events/user-42/batch-{idx}.parquet"),
            selected_row_groups: vec![idx % 64],
            min_seq: SeqId::new((idx as i64 * 1_000) + 1).expect("valid seq"),
            max_seq: SeqId::new((idx as i64 * 1_000) + 1_000).expect("valid seq"),
        })
        .collect();
    plan
}

fn hot_rows(count: usize) -> Vec<HotRow> {
    (0..count)
        .map(|idx| HotRow {
            pk: pk(idx),
            scope_key: Some(ScopeKey::new("user-42").expect("valid scope")),
            seq: SeqId::new((idx + 20_000) as i64).expect("valid seq"),
            commit_seq: CommitSeq::new((idx + 20_000) as i64).expect("valid commit seq"),
            deleted: idx % 23 == 0,
            row_image: json!({ "id": idx, "source": "hot" }),
        })
        .collect()
}

fn cold_rows(count: usize) -> Vec<ColdRow> {
    (0..count)
        .map(|idx| ColdRow {
            pk: pk(idx),
            scope_key: Some(ScopeKey::new("user-42").expect("valid scope")),
            seq: SeqId::new((idx + 1) as i64).expect("valid seq"),
            commit_seq: CommitSeq::new((idx + 1) as i64).expect("valid commit seq"),
            deleted: false,
            schema_version: 1,
            row_image: json!({ "id": idx, "source": "cold" }),
        })
        .collect()
}

fn pk(value: usize) -> LogicalPk {
    LogicalPk::new(vec![(
        PkColumn::new("id").expect("valid pk column"),
        PkValue::new(json!(value)).expect("valid pk value"),
    )])
    .expect("valid logical pk")
}

fn render_cold_path(
    template: &PathTemplate,
    namespace: &str,
    table_name: &str,
    scope_id: Option<&str>,
    batch: u32,
) -> String {
    let prefix = template
        .render(namespace, table_name, scope_id)
        .expect("template renders");
    format!("{prefix}batch-{batch}.parquet")
}

fn should_flush_by_policy(policy: &FlushPolicy, pending_rows: u64) -> bool {
    matches!(policy, FlushPolicy::RowLimit { hot_row_limit, .. } if pending_rows >= *hot_row_limit)
}

#[derive(Debug, Clone, Copy)]
struct QueryRequest {
    has_cold_segments: bool,
    overlaps_hot_range: bool,
    overlaps_cold_range: bool,
    cold_api_enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueryMode {
    HotOnly,
    HotCold,
    ColdOnly,
    SkipCold,
}

fn decide_query_mode(request: QueryRequest) -> QueryMode {
    if !request.cold_api_enabled || !request.has_cold_segments {
        return if request.overlaps_hot_range {
            QueryMode::HotOnly
        } else {
            QueryMode::SkipCold
        };
    }
    match (request.overlaps_hot_range, request.overlaps_cold_range) {
        (true, true) => QueryMode::HotCold,
        (true, false) => QueryMode::HotOnly,
        (false, true) => QueryMode::ColdOnly,
        (false, false) => QueryMode::SkipCold,
    }
}

criterion_group!(
    benches,
    bench_merge_plan_serialization,
    bench_hot_cold_deduplication,
    bench_path_and_policy,
    bench_query_mode_decision
);
criterion_main!(benches);
