//! pg-koldstore benchmark runner.

use anyhow::Result;

mod client;
mod cold_pruning;
mod hot_dml_vs_heap;
mod report;
mod suite;
mod verdict;

#[tokio::main]
async fn main() -> Result<()> {
    let hot_dml = hot_dml_vs_heap::HotDmlScenario::default_tables();
    let _setup_sql = [
        hot_dml.heap.create_table_sql(),
        hot_dml.managed.create_table_sql(),
    ];
    let _scenario_names = [
        hot_dml_vs_heap::NAME,
        cold_pruning::NAME,
        suite::SCENARIOS[0],
    ];
    let pruning = cold_pruning::ColdPruningResult {
        total_row_groups: 100,
        selected_row_groups: 10,
    };
    let _thresholds = (
        verdict::HOT_DML_MAX_OVERHEAD_RATIO,
        verdict::PK_LOOKUP_MIN_ROW_GROUP_SKIP_RATIO,
    );
    let report = report::BenchmarkReport::empty("pg-koldstore");
    let _html = report::to_html_summary(&report);
    let _suite = suite::FULL_SUITE;
    let _verdicts = (
        verdict::hot_dml_within_threshold(1.0, 1.05),
        pruning.meets_pk_lookup_target()
            && verdict::pk_lookup_pruning_within_threshold(pruning.skipped_ratio()),
    );
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
