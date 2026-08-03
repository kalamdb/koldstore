//! Cold-read profiling and EXPLAIN rendering for KoldMergeScan.
//!
//! Uses PostgreSQL's public Explain APIs (`ExplainPropertyText/Integer/UInteger/
//! Float/Bool/List`, `ExplainOpenGroup` / `ExplainCloseGroup`) so TEXT / JSON /
//! YAML / XML stay consistent with native plan nodes. TEXT section headers
//! mirror JIT (`Label:\n` + indent) because `ExplainOpenGroup` is a no-op for
//! TEXT. Graph clients that parse structured formats get nested Scan Sources,
//! Merge, Timing, and Parquet Segments groups. JSON additionally exposes a
//! clearly marked diagnostic `Plans` subtree when the executor owns the
//! hot/cold pipeline, so plan visualizers can render its read stages.

use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::CString;
use std::time::Instant;

use koldstore_parquet::{BloomPruneMode, ParquetReadProfile};
use pgrx::pg_sys;

/// EXPLAIN data PostgreSQL asked this plan node to collect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ProfileCollectionMode {
    /// Ordinary execution: no EXPLAIN-specific work or allocation.
    Disabled,
    /// Row counters only, as used by `EXPLAIN (ANALYZE, TIMING OFF)`.
    Counts,
    /// Row counters and phase clocks for timed `EXPLAIN ANALYZE`.
    CountsAndTiming,
}

impl ProfileCollectionMode {
    pub(super) const fn from_instrumentation(instrumented: bool, need_timer: bool) -> Self {
        match (instrumented, need_timer) {
            (false, _) => Self::Disabled,
            (true, false) => Self::Counts,
            (true, true) => Self::CountsAndTiming,
        }
    }

    pub(super) const fn collects_counts(self) -> bool {
        !matches!(self, Self::Disabled)
    }

    pub(super) const fn collects_timing(self) -> bool {
        matches!(self, Self::CountsAndTiming)
    }
}

/// Collects execution counters and clocks only when PostgreSQL requests them.
pub(super) struct ScanProfiler {
    collection: ProfileCollectionMode,
    execution: Option<Box<ScanExecutionProfile>>,
}

/// Profiling operations used by source execution.
///
/// The uninstrumented implementation is a zero-sized no-op, allowing LLVM to
/// remove every profiling call from ordinary query execution.
pub(super) trait ScanProfileSink {
    fn start_timer(&self) -> Option<Instant>;
    fn record_hot_scan(&mut self, started: Option<Instant>);
    fn record_hot_buffer(&mut self, row_count: usize);
    fn record_hot_spi_query(&mut self, sql: String);
    fn record_mirror_scan(&mut self, row_count: usize, started: Option<Instant>);
}

/// Zero-cost profiling sink used outside `EXPLAIN ANALYZE`.
pub(super) struct DisabledScanProfiler;

impl ScanProfileSink for DisabledScanProfiler {
    #[inline(always)]
    fn start_timer(&self) -> Option<Instant> {
        None
    }

    #[inline(always)]
    fn record_hot_scan(&mut self, _started: Option<Instant>) {}

    #[inline(always)]
    fn record_hot_buffer(&mut self, _row_count: usize) {}

    #[inline(always)]
    fn record_hot_spi_query(&mut self, _sql: String) {}

    #[inline(always)]
    fn record_mirror_scan(&mut self, _row_count: usize, _started: Option<Instant>) {}
}

impl ScanProfiler {
    /// Creates a profiler from PostgreSQL's native executor instrumentation flags.
    pub(super) fn from_instrumentation(instrumentation: i32) -> Self {
        let collection = ProfileCollectionMode::from_instrumentation(
            instrumentation != 0,
            instrumentation & pg_sys::InstrumentOption::INSTRUMENT_TIMER as i32 != 0,
        );
        Self {
            collection,
            execution: collection
                .collects_counts()
                .then(|| Box::new(ScanExecutionProfile::default())),
        }
    }

    /// Returns true when PostgreSQL requested execution counters.
    #[inline]
    pub(super) fn is_enabled(&self) -> bool {
        self.execution.is_some()
    }

    /// Records managed-table metadata preparation.
    pub(super) fn record_metadata(&mut self, started: Option<Instant>) {
        if let Some(execution) = self.execution.as_mut() {
            execution.metadata_ms = started.map(elapsed_ms);
        }
    }

    /// Completes the profile and returns it for executor-state storage.
    pub(super) fn finish(
        mut self,
        hot_rows: usize,
        initialization_started: Option<Instant>,
    ) -> Option<Box<ScanExecutionProfile>> {
        if let Some(execution) = self.execution.as_mut() {
            execution.hot_rows = hot_rows;
            execution.initialization_ms = initialization_started.map(elapsed_ms);
        }
        self.execution
    }
}

impl ScanProfileSink for ScanProfiler {
    #[inline]
    fn start_timer(&self) -> Option<Instant> {
        self.collection.collects_timing().then(Instant::now)
    }

    fn record_hot_scan(&mut self, started: Option<Instant>) {
        if let Some(execution) = self.execution.as_mut() {
            execution.hot_scan_ms = started.map(elapsed_ms);
        }
    }

    fn record_hot_buffer(&mut self, row_count: usize) {
        if let Some(execution) = self.execution.as_mut() {
            execution.merge_input_rows = row_count;
            execution.merge_output_rows = row_count;
        }
    }

    fn record_hot_spi_query(&mut self, sql: String) {
        if let Some(execution) = self.execution.as_mut() {
            execution.hot_spi_query = Some(sql);
        }
    }

    fn record_mirror_scan(&mut self, row_count: usize, started: Option<Instant>) {
        if let Some(execution) = self.execution.as_mut() {
            execution.mirror_rows = row_count;
            execution.mirror_scan_ms = started.map(elapsed_ms);
        }
    }
}

/// How BeginCustomScan chose to emit rows (surfaced in EXPLAIN).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(super) enum EmitPath {
    /// Native hot child plan streamed via ExecProcNode.
    #[default]
    HotChild,
    /// Hot-only SPI native Datums (no JSON), buffered.
    HotNative,
    /// Cold-only after PK probe: no hot JSON merge.
    ColdNative,
    /// Hot+cold winners emitted from newest-first cold segment groups.
    MergeStream,
}

impl EmitPath {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::HotChild => "hot_child",
            Self::HotNative => "hot_native",
            Self::ColdNative => "cold_native",
            Self::MergeStream => "merge_stream",
        }
    }
}

/// Execution counters and phase timings for one KoldMergeScan invocation.
#[derive(Debug, Clone, Default)]
pub(super) struct ScanExecutionProfile {
    /// Rows read from the hot heap, including a zero-row point probe.
    pub(super) hot_rows: usize,
    /// Maximum hot JSON rows retained in one MergeStream SPI page.
    pub(super) peak_hot_batch_rows: usize,
    /// First-page SPI SQL for merge_stream hot JSON paging (diagnostic).
    pub(super) hot_spi_query: Option<String>,
    /// Exact PK identities retained by newest-first resolution.
    pub(super) seen_key_count: usize,
    /// Rows decoded from selected Parquet row groups.
    pub(super) cold_rows: usize,
    /// Peak decoded cold rows retained for one newest-first segment group.
    pub(super) peak_cold_batch_rows: usize,
    /// Mirror tombstones inspected for the immediate delete overlay.
    pub(super) mirror_rows: usize,
    /// Cold rows masked by matching mirror tombstones.
    pub(super) overlay_rows_removed: usize,
    /// Candidate rows handed to winner resolution after overlay masking.
    pub(super) merge_input_rows: usize,
    /// Visible rows produced by winner resolution before PostgreSQL filters.
    pub(super) merge_output_rows: usize,
    /// Duplicate or deleted candidates removed by winner resolution.
    pub(super) merge_rows_removed: usize,
    /// Whether hot/cold winner resolution ran for this path.
    pub(super) merge_executed: bool,
    /// Total `BeginCustomScan` work, which PostgreSQL node timing excludes.
    pub(super) initialization_ms: Option<f64>,
    /// Managed-table metadata lookup and scan-shape construction.
    pub(super) metadata_ms: Option<f64>,
    /// Hot SPI scan or point-probe time. Native child timing remains on its plan node.
    pub(super) hot_scan_ms: Option<f64>,
    /// Mirror tombstone SPI scan time.
    pub(super) mirror_scan_ms: Option<f64>,
    /// Cold overlay filtering time.
    pub(super) overlay_ms: Option<f64>,
    /// Hot/cold winner-resolution time.
    pub(super) merge_ms: Option<f64>,
    /// Winner row-image to PostgreSQL Datum materialization time.
    pub(super) materialization_ms: Option<f64>,
}

/// Execution metadata retained until PostgreSQL invokes the EXPLAIN callback.
#[derive(Debug)]
pub(super) struct CompletedExplainState {
    pub(super) cold_profile: ColdReadProfile,
    pub(super) hot_plan_label: String,
    pub(super) emit_path: EmitPath,
    pub(super) execution: Box<ScanExecutionProfile>,
}

thread_local! {
    static COMPLETED_EXPLAIN_STATES: RefCell<HashMap<usize, CompletedExplainState>> =
        RefCell::new(HashMap::new());
}

/// Removes stale retained metadata when PostgreSQL reuses a plan-state address.
pub(super) fn clear_completed_explain_state(node_key: usize) {
    COMPLETED_EXPLAIN_STATES.with(|states| {
        states.borrow_mut().remove(&node_key);
    });
}

/// Retains instrumented execution metadata across `EndCustomScan`.
pub(super) fn remember_completed_explain_state(node_key: usize, state: CompletedExplainState) {
    COMPLETED_EXPLAIN_STATES.with(|states| {
        states.borrow_mut().insert(node_key, state);
    });
}

/// Takes retained execution metadata for PostgreSQL's EXPLAIN callback.
pub(super) fn take_completed_explain_state(node_key: usize) -> Option<CompletedExplainState> {
    COMPLETED_EXPLAIN_STATES.with(|states| states.borrow_mut().remove(&node_key))
}

#[derive(Debug, Clone)]
pub(super) struct SegmentReadProfile {
    pub(super) object_path: String,
    pub(super) row_count: usize,
    pub(super) read_ms: Option<f64>,
    /// Catalog object size when known.
    pub(super) byte_size: Option<u64>,
    /// ObjectStore Parquet read diagnostics (footer / bloom / range I/O).
    pub(super) parquet: Option<ParquetReadProfile>,
}

#[derive(Debug, Clone)]
pub(super) struct ColdReadProfile {
    /// Published object-store manifest path for the table (diagnostic metadata).
    ///
    /// Runtime cold selection reads `koldstore.cold_segments` /
    /// `koldstore.cold_segment_index`; this path is not opened by the scan.
    pub(super) manifest_path: String,
    pub(super) storage_type: String,
    pub(super) base_path: String,
    /// Catalog SPI time for `koldstore.cold_segments` listing (not object-store JSON).
    pub(super) manifest_read_ms: Option<f64>,
    /// Segments considered before any prune (catalog candidates).
    pub(super) segments_considered: usize,
    /// Segments rejected by `cold_segment_index` candidate lookup.
    pub(super) segments_pruned_catalog_index: usize,
    /// Segments opened after catalog prune.
    pub(super) segments_opened: usize,
    /// Stable column ID used for cold_segment_index lookup, when any.
    pub(super) segment_index_order_column_id: Option<i16>,
    /// Current name of the order column (diagnostic only).
    pub(super) segment_index_order_column: Option<String>,
    /// Bound shape used for cold_segment_index candidate SQL, when planned.
    pub(super) segment_index_lookup_shape: Option<koldstore_catalog::SegmentIndexLookupShape>,
    /// Preferred index access for the bound shape (not forced by SQL).
    pub(super) segment_index_plan: Option<String>,
    /// Wall time spent on the segment-index SPI lookup, when executed.
    pub(super) segment_index_lookup_ms: Option<f64>,
    /// Candidate segments returned by cold_segment_index before Parquet open.
    pub(super) segment_index_candidate_segments: Option<usize>,
    /// SPI SQL text that listed active cold segments (when catalog was consulted).
    pub(super) cold_segments_query: Option<String>,
    /// SPI SQL text used for cold_segment_index candidate prune (when executed).
    pub(super) segment_index_query: Option<String>,
    /// PK equality probe pushed into Parquet row-group prune, when present.
    pub(super) pk_probe: Option<(String, Vec<String>)>,
    pub(super) projected_columns: Vec<String>,
    pub(super) segments: Vec<SegmentReadProfile>,
}

impl ColdReadProfile {
    pub(super) fn empty(manifest_path: impl Into<String>) -> Self {
        Self {
            manifest_path: manifest_path.into(),
            storage_type: String::new(),
            base_path: String::new(),
            manifest_read_ms: None,
            segments_considered: 0,
            segments_pruned_catalog_index: 0,
            segments_opened: 0,
            segment_index_order_column_id: None,
            segment_index_order_column: None,
            segment_index_lookup_shape: None,
            segment_index_plan: None,
            segment_index_lookup_ms: None,
            segment_index_candidate_segments: None,
            cold_segments_query: None,
            segment_index_query: None,
            pk_probe: None,
            projected_columns: Vec::new(),
            segments: vec![],
        }
    }

    /// Sum of per-segment Parquet open+decode times when available.
    pub(super) fn cold_read_ms(&self) -> Option<f64> {
        if self.segments.is_empty() {
            return None;
        }
        let mut total = 0.0;
        let mut any = false;
        for segment in &self.segments {
            if let Some(ms) = segment.read_ms {
                total += ms;
                any = true;
            }
        }
        any.then_some(total)
    }
}

pub(super) fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

/// Formats a byte count for EXPLAIN (for example `1.8 MB`).
#[cfg(test)]
#[must_use]
fn format_bytes_human(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    let bytes_f = bytes as f64;
    if bytes_f >= GB {
        format!("{:.1} GB", bytes_f / GB)
    } else if bytes_f >= MB {
        format!("{:.1} MB", bytes_f / MB)
    } else if bytes_f >= KB {
        format!("{:.1} kB", bytes_f / KB)
    } else {
        format!("{bytes} bytes")
    }
}

/// Renders the KoldMergeScan source-to-merge flow using native Explain APIs.
pub(super) fn explain_scan_profile(
    es: *mut pg_sys::ExplainState,
    profile: &ColdReadProfile,
    hot_plan_label: &str,
    emit_path: EmitPath,
    execution: Option<&ScanExecutionProfile>,
) {
    let show_timing = explain_wants_timing(es);
    if let Some(execution) = execution {
        // Retain the concise summary properties used by existing text tooling.
        explain_text(es, "Emit Path", emit_path.as_str());
        explain_integer(es, "Hot Rows", None, execution.hot_rows as i64);
    }

    explain_open_group(es, "Scan Sources", Some("Scan Sources"), true);
    explain_hot_scan(es, hot_plan_label, emit_path, execution);
    explain_cold_scan(es, profile, execution, show_timing);
    explain_mirror_scan(es, execution);
    explain_close_group(es, "Scan Sources", Some("Scan Sources"), true);

    explain_merge(es, emit_path, execution);
    explain_timing_group(es, show_timing, profile, execution);
}

/// Renders KoldStore-owned read stages as plan-shaped JSON objects.
///
/// PostgreSQL renders only executor plan nodes beneath `Plans`. On merge paths,
/// the cold reader and catalog lookup execute inside one `KoldMergeScan`, so
/// they otherwise have no visual representation. These nodes are explicitly
/// marked as internal diagnostics and are only emitted when there is no native
/// child plan for PostgreSQL to render itself.
pub(super) fn explain_visual_pipeline(
    es: *mut pg_sys::ExplainState,
    profile: &ColdReadProfile,
    hot_plan_label: &str,
    emit_path: EmitPath,
    execution: Option<&ScanExecutionProfile>,
) {
    if !explain_is_json(es) {
        return;
    }

    let analyze = explain_is_analyze(es);
    let show_timing = explain_wants_timing(es);
    explain_open_group(es, "Plans", Some("Plans"), false);

    explain_visual_node_open(es, "KoldStore Hot Scan", "Hot Source");
    if !hot_plan_label.is_empty() {
        explain_text(es, "Hot Planned Access", hot_plan_label);
    }
    match execution {
        Some(execution) => {
            explain_text(es, "Hot Actual Access", hot_actual_access(emit_path));
            if let Some(sql) = execution.hot_spi_query.as_deref() {
                explain_text(es, "Hot SPI Query", &compact_sql_for_explain(sql));
            }
            explain_integer(es, "Rows Scanned", None, execution.hot_rows as i64);
            if show_timing {
                if let Some(ms) = execution.hot_scan_ms {
                    explain_float(es, "Read Time", "ms", ms, 3);
                }
            }
        }
        None => explain_text(es, "Status", "planned"),
    }
    explain_open_group(es, "Plans", Some("Plans"), false);
    explain_visual_postgres_hot_access(es, hot_plan_label, emit_path, execution, show_timing);
    explain_close_group(es, "Plans", Some("Plans"), false);
    explain_visual_node_close(es, "KoldStore Hot Scan");

    explain_visual_node_open(es, "KoldStore Cold Storage Scan", "Cold Source");
    let cold_accessed = profile.manifest_read_ms.is_some();
    explain_text(
        es,
        "Status",
        if analyze {
            if cold_accessed {
                "executed"
            } else {
                "not executed"
            }
        } else {
            "planned"
        },
    );
    if let Some(execution) = execution {
        explain_integer(es, "Rows Scanned", None, execution.cold_rows as i64);
    }
    explain_integer(
        es,
        "Candidate Segments",
        None,
        profile.segments_considered as i64,
    );
    explain_integer(
        es,
        "Parquet Segments Opened",
        None,
        profile.segments_opened as i64,
    );
    // Catalog lookup determines which Parquet segments open — nest Parquet
    // under the catalog node so visualizers show the real dependency.
    explain_open_group(es, "Plans", Some("Plans"), false);
    explain_visual_catalog_node(es, profile, analyze, show_timing);
    explain_close_group(es, "Plans", Some("Plans"), false);
    explain_visual_node_close(es, "KoldStore Cold Storage Scan");

    explain_close_group(es, "Plans", Some("Plans"), false);
}

fn explain_visual_catalog_node(
    es: *mut pg_sys::ExplainState,
    profile: &ColdReadProfile,
    analyze: bool,
    show_timing: bool,
) {
    explain_visual_node_open(es, "KoldStore Segment Catalog Scan", "Catalog Lookup");
    explain_text(
        es,
        "Status",
        if analyze {
            if profile.manifest_read_ms.is_some() {
                "executed"
            } else {
                "not executed"
            }
        } else {
            "planned"
        },
    );
    explain_text(
        es,
        "Runtime Catalog Source",
        "postgres (koldstore.cold_segments)",
    );
    explain_bool(es, "Runtime Manifest Read", false);
    if profile.manifest_path != "(none)" {
        explain_text(es, "Published Manifest Path", &profile.manifest_path);
    }
    explain_integer(
        es,
        "Candidate Segments",
        None,
        profile.segments_considered as i64,
    );
    explain_integer(
        es,
        "Segments Pruned by Catalog Index",
        None,
        profile.segments_pruned_catalog_index as i64,
    );
    if let Some(candidates) = profile.segment_index_candidate_segments {
        explain_integer(
            es,
            "Segments Returned by Segment Index",
            None,
            candidates as i64,
        );
    }
    if let Some(query) = profile.cold_segments_query.as_deref() {
        explain_text(es, "Cold Segments Query", &compact_sql_for_explain(query));
    }
    if let Some(query) = profile.segment_index_query.as_deref() {
        explain_text(es, "Segment Index Query", &compact_sql_for_explain(query));
    }
    if show_timing {
        if let Some(ms) = profile.manifest_read_ms {
            explain_float(es, "Segment Catalog Lookup Time", "ms", ms, 3);
        }
        if let Some(ms) = profile.segment_index_lookup_ms {
            explain_float(es, "Segment Index Lookup Time", "ms", ms, 3);
        }
    }
    // Parquet opens are children of catalog selection.
    explain_open_group(es, "Plans", Some("Plans"), false);
    for segment in &profile.segments {
        explain_visual_node_open(es, "KoldStore Parquet Scan", "Parquet Segment");
        explain_segment(es, segment, show_timing, analyze);
        explain_visual_node_close(es, "KoldStore Parquet Scan");
    }
    explain_close_group(es, "Plans", Some("Plans"), false);
    explain_visual_node_close(es, "KoldStore Segment Catalog Scan");
}

fn explain_visual_postgres_hot_access(
    es: *mut pg_sys::ExplainState,
    hot_plan_label: &str,
    emit_path: EmitPath,
    execution: Option<&ScanExecutionProfile>,
    show_timing: bool,
) {
    explain_visual_node_open(
        es,
        "KoldStore PostgreSQL Hot Access",
        "PostgreSQL Hot Access",
    );
    explain_text(es, "KoldStore Role", "PostgreSQL Hot Access");
    if !hot_plan_label.is_empty() {
        explain_text(es, "Hot Planned Access", hot_plan_label);
    }
    explain_text(es, "Hot Actual Access", hot_actual_access(emit_path));
    match execution {
        Some(execution) => {
            explain_integer(es, "Actual Rows", None, execution.hot_rows as i64);
            explain_integer(es, "Actual Loops", None, 1);
            if show_timing {
                if let Some(ms) = execution.hot_scan_ms {
                    explain_float(es, "Actual Total Time", "ms", ms, 3);
                }
            }
        }
        None => explain_text(es, "Status", "planned"),
    }
    explain_visual_node_close(es, "KoldStore PostgreSQL Hot Access");
}

fn explain_visual_node_open(
    es: *mut pg_sys::ExplainState,
    node_type: &str,
    parent_relationship: &str,
) {
    explain_open_group(es, node_type, None, true);
    explain_text(es, "Node Type", node_type);
    explain_text(es, "Parent Relationship", parent_relationship);
    explain_bool(es, "KoldStore Internal", true);
}

fn explain_visual_node_close(es: *mut pg_sys::ExplainState, node_type: &str) {
    explain_close_group(es, node_type, None, true);
}

fn explain_hot_scan(
    es: *mut pg_sys::ExplainState,
    hot_plan_label: &str,
    emit_path: EmitPath,
    execution: Option<&ScanExecutionProfile>,
) {
    explain_open_group(es, "Hot Scan", Some("Hot Scan"), true);
    if !hot_plan_label.is_empty() {
        explain_text(es, "Planned Access", hot_plan_label);
    }
    match execution {
        Some(execution) => {
            explain_text(es, "Actual Access", hot_actual_access(emit_path));
            if let Some(sql) = execution.hot_spi_query.as_deref() {
                explain_text(es, "Hot SPI Query", &compact_sql_for_explain(sql));
            }
            explain_integer(es, "Rows Scanned", None, execution.hot_rows as i64);
            if matches!(emit_path, EmitPath::MergeStream) {
                explain_integer(
                    es,
                    "Peak Hot Batch Rows",
                    None,
                    execution.peak_hot_batch_rows as i64,
                );
            }
        }
        None => explain_text(es, "Status", "planned"),
    }
    explain_close_group(es, "Hot Scan", Some("Hot Scan"), true);
}

/// Honest runtime hot access label — distinct from the planner's child shape.
fn hot_actual_access(emit_path: EmitPath) -> &'static str {
    match emit_path {
        EmitPath::HotChild => "Native PostgreSQL Child",
        EmitPath::HotNative => "SPI Native Tuple Scan",
        EmitPath::ColdNative => "SPI Native Point Probe",
        EmitPath::MergeStream => "SPI JSON Keyset Scan",
    }
}

/// Collapses SQL whitespace so EXPLAIN TEXT stays on one readable property line.
fn compact_sql_for_explain(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn explain_cold_scan(
    es: *mut pg_sys::ExplainState,
    profile: &ColdReadProfile,
    execution: Option<&ScanExecutionProfile>,
    show_timing: bool,
) {
    // Plan-only EXPLAIN must not look "executed" just because a planned profile
    // carries placeholder timing (e.g. manifest_read_ms: Some(0.0)).
    let analyze = explain_is_analyze(es);
    explain_open_group(es, "Cold Scan", Some("Cold Scan"), true);
    let cold_accessed = profile.manifest_read_ms.is_some();
    let status = if analyze {
        if cold_accessed {
            "executed"
        } else {
            "not executed"
        }
    } else {
        "planned"
    };
    explain_text(es, "Status", status);
    if let Some(execution) = execution {
        explain_integer(es, "Rows Scanned", None, execution.cold_rows as i64);
    }
    explain_integer(
        es,
        "Candidate Segments",
        None,
        profile.segments_considered as i64,
    );
    if let Some(column_id) = profile.segment_index_order_column_id {
        explain_text(
            es,
            "Segment Index Source",
            "postgres (koldstore.cold_segment_index)",
        );
        explain_integer(es, "Order Column ID", None, i64::from(column_id));
        if let Some(name) = profile.segment_index_order_column.as_deref() {
            explain_text(es, "Order Column", name);
        }
        if let Some(shape) = profile.segment_index_lookup_shape {
            explain_text(es, "Segment Index Lookup Shape", shape.as_str());
        }
        if let Some(plan) = profile.segment_index_plan.as_deref() {
            explain_text(es, "Segment Index Preferred Access", plan);
        }
        if let Some(candidates) = profile.segment_index_candidate_segments {
            explain_integer(
                es,
                "Segments Returned by Segment Index",
                None,
                candidates as i64,
            );
        }
        if analyze {
            if let Some(ms) = profile.segment_index_lookup_ms {
                explain_float(es, "Segment Index Lookup Time", "ms", ms, 3);
            }
        }
    }
    explain_integer(
        es,
        "Segments Pruned by Catalog Index",
        None,
        profile.segments_pruned_catalog_index as i64,
    );
    if analyze {
        explain_integer(
            es,
            "Parquet Segments Opened",
            None,
            profile.segments_opened as i64,
        );
    } else {
        explain_integer(
            es,
            "Parquet Segments Planned",
            None,
            profile.segments_opened as i64,
        );
    }
    explain_integer(
        es,
        "Segments Pruned by Bloom",
        None,
        profile.segments_pruned_by_bloom() as i64,
    );

    let (row_groups_total, row_groups_selected, row_groups_skipped, bloom_filters_fetched) =
        profile.row_group_totals();
    let bytes_fetched = profile.bytes_fetched();
    let footer_cache_hits = profile.footer_cache_hits();

    if analyze && (cold_accessed || row_groups_total > 0 || bytes_fetched > 0) {
        explain_integer(es, "Row Groups Read", None, row_groups_selected as i64);
        if row_groups_total > 0 {
            explain_integer(es, "Row Groups Total", None, row_groups_total as i64);
            explain_integer(es, "Row Groups Skipped", None, row_groups_skipped as i64);
            explain_integer(
                es,
                "Row Groups Pruned by Column Stats",
                None,
                profile.row_groups_pruned_by_stats() as i64,
            );
            explain_integer(
                es,
                "Row Groups Pruned by Bloom",
                None,
                profile.row_groups_pruned_by_bloom_filters() as i64,
            );
        }
        // Unit on ExplainPropertyUInteger renders as "N bytes" — same as native PG.
        explain_uinteger(es, "Bytes Fetched", Some("bytes"), bytes_fetched);
        if footer_cache_hits > 0 {
            explain_integer(es, "Footer Cache Hits", None, footer_cache_hits as i64);
        }
        if bloom_filters_fetched > 0 {
            explain_integer(
                es,
                "Bloom Filters Fetched",
                None,
                bloom_filters_fetched as i64,
            );
        }
    }

    if profile.manifest_path != "(none)" {
        // Catalog listing — never opens object-store manifest.json.
        explain_text(es, "Published Manifest Path", &profile.manifest_path);
        explain_text(
            es,
            "Runtime Catalog Source",
            "postgres (koldstore.cold_segments)",
        );
        explain_bool(es, "Runtime Manifest Read", false);
    }
    if let Some(query) = profile.cold_segments_query.as_deref() {
        explain_text(es, "Cold Segments Query", &compact_sql_for_explain(query));
    }
    if let Some(query) = profile.segment_index_query.as_deref() {
        explain_text(es, "Segment Index Query", &compact_sql_for_explain(query));
    }

    if !profile.storage_type.is_empty() {
        explain_text(es, "Cold Storage Type", &profile.storage_type);
    }
    if !profile.base_path.is_empty() {
        explain_text(es, "Cold Storage Base", &profile.base_path);
    }

    if let Some((column, values)) = &profile.pk_probe {
        explain_text(es, "PK Probe Column", column);
        explain_list(es, "PK Probe Values", values);
    }

    if !profile.projected_columns.is_empty() {
        explain_list(es, "Cold Projection", &profile.projected_columns);
    }

    // Nested group for JSON/YAML/XML graph clients; TEXT uses a native "Label:\n"
    // section header. Empty arrays still emit so structured clients see a stable key.
    explain_open_group(es, "Parquet Segments", Some("Parquet Segments"), false);
    for segment in &profile.segments {
        explain_open_group(es, "Parquet Segment", None, true);
        explain_segment(es, segment, show_timing, analyze);
        explain_close_group(es, "Parquet Segment", None, true);
    }
    explain_close_group(es, "Parquet Segments", Some("Parquet Segments"), false);
    explain_close_group(es, "Cold Scan", Some("Cold Scan"), true);
}

fn explain_mirror_scan(es: *mut pg_sys::ExplainState, execution: Option<&ScanExecutionProfile>) {
    explain_open_group(es, "Mirror Scan", Some("Mirror Scan"), true);
    match execution {
        Some(execution) => {
            explain_text(
                es,
                "Status",
                if execution.cold_rows > 0 {
                    "executed"
                } else {
                    "not executed"
                },
            );
            explain_integer(es, "Rows Scanned", None, execution.mirror_rows as i64);
            explain_integer(
                es,
                "Rows Removed by Overlay",
                None,
                execution.overlay_rows_removed as i64,
            );
        }
        None => explain_text(es, "Status", "planned"),
    }
    explain_close_group(es, "Mirror Scan", Some("Mirror Scan"), true);
}

fn explain_merge(
    es: *mut pg_sys::ExplainState,
    emit_path: EmitPath,
    execution: Option<&ScanExecutionProfile>,
) {
    explain_open_group(es, "Merge", Some("Merge"), true);
    match execution {
        Some(execution) => {
            explain_text(
                es,
                "Strategy",
                if execution.merge_executed {
                    "Primary Key Winner Resolution"
                } else {
                    "Not Required"
                },
            );
            explain_integer(es, "Input Rows", None, execution.merge_input_rows as i64);
            explain_integer(es, "Output Rows", None, execution.merge_output_rows as i64);
            explain_integer(
                es,
                "Rows Removed by Merge",
                None,
                execution.merge_rows_removed as i64,
            );
            explain_integer(
                es,
                "Rows Removed by Overlay",
                None,
                execution.overlay_rows_removed as i64,
            );
            if matches!(emit_path, EmitPath::MergeStream | EmitPath::ColdNative) {
                explain_integer(es, "Seen Keys", None, execution.seen_key_count as i64);
                explain_integer(
                    es,
                    "Peak Cold Batch Rows",
                    None,
                    execution.peak_cold_batch_rows as i64,
                );
            }
            explain_text(es, "Tuple Path", emit_path.as_str());
            explain_text(es, "Post-Merge Filter", "PostgreSQL ExecScan");
        }
        None => explain_text(es, "Status", "runtime"),
    }
    explain_close_group(es, "Merge", Some("Merge"), true);
}

/// Timing subgroup matching native JIT / serialization grouping.
fn explain_timing_group(
    es: *mut pg_sys::ExplainState,
    show_timing: bool,
    profile: &ColdReadProfile,
    execution: Option<&ScanExecutionProfile>,
) {
    let Some(execution) = execution else {
        return;
    };
    if !show_timing {
        return;
    }
    let catalog_ms = profile.manifest_read_ms;
    let cold_ms = profile.cold_read_ms();

    explain_open_group(es, "Timing", Some("Timing"), true);
    if let Some(ms) = execution.initialization_ms {
        explain_float(es, "Initialization Time", "ms", ms, 3);
    }
    if let Some(ms) = execution.metadata_ms {
        explain_float(es, "Metadata Time", "ms", ms, 3);
    }
    if let Some(ms) = catalog_ms {
        explain_float(es, "Segment Catalog Time", "ms", ms, 3);
    }
    if let Some(ms) = execution.hot_scan_ms {
        explain_float(es, "Hot Scan Time", "ms", ms, 3);
    }
    if let Some(ms) = cold_ms {
        explain_float(es, "Cold Read Time", "ms", ms, 3);
    }
    if let Some(ms) = execution.mirror_scan_ms {
        explain_float(es, "Mirror Scan Time", "ms", ms, 3);
    }
    if let Some(ms) = execution.overlay_ms {
        explain_float(es, "Overlay Time", "ms", ms, 3);
    }
    if let Some(ms) = execution.merge_ms {
        explain_float(es, "Merge Time", "ms", ms, 3);
    }
    if let Some(ms) = execution.materialization_ms {
        explain_float(es, "Materialization Time", "ms", ms, 3);
    }
    explain_close_group(es, "Timing", Some("Timing"), true);
}

fn explain_segment(
    es: *mut pg_sys::ExplainState,
    segment: &SegmentReadProfile,
    show_timing: bool,
    executed: bool,
) {
    explain_text(es, "Object", &segment.object_path);
    if let Some(size) = segment
        .byte_size
        .or_else(|| segment.parquet.as_ref().and_then(|p| p.file_size))
    {
        explain_uinteger(es, "Bytes", Some("bytes"), size);
    }
    if executed {
        explain_integer(es, "Rows", None, segment.row_count as i64);
        if show_timing {
            if let Some(ms) = segment.read_ms {
                explain_float(es, "Read Time", "ms", ms, 3);
            }
        }
    } else {
        explain_text(es, "Status", "planned");
    }

    let Some(parquet) = &segment.parquet else {
        return;
    };
    explain_bool(es, "Footer First", parquet.footer_first);
    explain_bool(es, "Footer Cache Hit", parquet.footer_cache_hit);
    explain_uinteger(es, "Range Gets", None, parquet.range_calls);
    explain_uinteger(es, "Bytes Read", Some("bytes"), parquet.bytes_read);
    explain_integer(
        es,
        "Row Groups Total",
        None,
        parquet.row_groups_total as i64,
    );
    explain_integer(
        es,
        "Row Groups Selected",
        None,
        parquet.row_groups_selected.len() as i64,
    );
    explain_integer(
        es,
        "Row Groups Skipped",
        None,
        parquet.row_groups_skipped as i64,
    );
    if !parquet.row_groups_selected.is_empty() {
        let selected = parquet
            .row_groups_selected
            .iter()
            .map(|idx| idx.to_string())
            .collect::<Vec<_>>();
        explain_list(es, "Selected Row Groups", &selected);
    }
    explain_bool(es, "Stats Pruned", parquet.stats_pruned);
    explain_text(es, "Bloom", parquet.bloom.as_str());
    if parquet.bloom_filters_fetched > 0 {
        explain_integer(
            es,
            "Bloom Filters Fetched",
            None,
            parquet.bloom_filters_fetched as i64,
        );
    }
}

impl ColdReadProfile {
    fn segments_pruned_by_bloom(&self) -> usize {
        self.segments
            .iter()
            .filter_map(|segment| segment.parquet.as_ref())
            .filter(|parquet| {
                parquet.bloom == BloomPruneMode::Applied && parquet.row_groups_selected.is_empty()
            })
            .count()
    }

    fn row_groups_pruned_by_stats(&self) -> usize {
        self.segments
            .iter()
            .filter_map(|segment| segment.parquet.as_ref())
            .filter(|parquet| parquet.stats_pruned)
            .map(|parquet| parquet.row_groups_skipped)
            .sum()
    }

    fn row_groups_pruned_by_bloom_filters(&self) -> usize {
        self.segments
            .iter()
            .filter_map(|segment| segment.parquet.as_ref())
            .filter(|parquet| parquet.bloom == BloomPruneMode::Applied && !parquet.stats_pruned)
            .map(|parquet| parquet.row_groups_skipped)
            .sum()
    }

    fn row_group_totals(&self) -> (usize, usize, usize, usize) {
        self.segments
            .iter()
            .filter_map(|segment| segment.parquet.as_ref())
            .fold((0, 0, 0, 0), |totals, parquet| {
                (
                    totals.0 + parquet.row_groups_total,
                    totals.1 + parquet.row_groups_selected.len(),
                    totals.2 + parquet.row_groups_skipped,
                    totals.3 + parquet.bloom_filters_fetched,
                )
            })
    }

    fn bytes_fetched(&self) -> u64 {
        self.segments
            .iter()
            .filter_map(|segment| segment.parquet.as_ref())
            .map(|parquet| parquet.bytes_read)
            .sum()
    }

    fn footer_cache_hits(&self) -> usize {
        self.segments
            .iter()
            .filter_map(|segment| segment.parquet.as_ref())
            .filter(|parquet| parquet.footer_cache_hit)
            .count()
    }
}

fn explain_is_analyze(es: *mut pg_sys::ExplainState) -> bool {
    if es.is_null() {
        return false;
    }
    unsafe { (*es).analyze }
}

fn explain_is_json(es: *mut pg_sys::ExplainState) -> bool {
    if es.is_null() {
        return false;
    }
    unsafe { (*es).format == pg_sys::ExplainFormat::EXPLAIN_FORMAT_JSON }
}

fn explain_wants_timing(es: *mut pg_sys::ExplainState) -> bool {
    if es.is_null() {
        return false;
    }
    unsafe { (*es).timing }
}

pub(super) fn explain_integer(
    es: *mut pg_sys::ExplainState,
    label: &str,
    unit: Option<&str>,
    value: i64,
) {
    let label = CString::new(label).unwrap_or_default();
    let unit = unit.map(|u| CString::new(u).unwrap_or_default());
    unsafe {
        pg_sys::ExplainPropertyInteger(
            label.as_ptr(),
            unit.as_ref().map_or(std::ptr::null(), |u| u.as_ptr()),
            value,
            es,
        );
    }
}

pub(super) fn explain_text(es: *mut pg_sys::ExplainState, label: &str, value: &str) {
    let label = CString::new(label).unwrap_or_default();
    let value = CString::new(value).unwrap_or_default();
    unsafe {
        pg_sys::ExplainPropertyText(label.as_ptr(), value.as_ptr(), es);
    }
}

fn explain_uinteger(es: *mut pg_sys::ExplainState, label: &str, unit: Option<&str>, value: u64) {
    let label = CString::new(label).unwrap_or_default();
    let unit = unit.map(|u| CString::new(u).unwrap_or_default());
    unsafe {
        pg_sys::ExplainPropertyUInteger(
            label.as_ptr(),
            unit.as_ref().map_or(std::ptr::null(), |u| u.as_ptr()),
            value,
            es,
        );
    }
}

fn explain_float(es: *mut pg_sys::ExplainState, label: &str, unit: &str, value: f64, ndigits: i32) {
    let label = CString::new(label).unwrap_or_default();
    let unit = CString::new(unit).unwrap_or_default();
    unsafe {
        pg_sys::ExplainPropertyFloat(label.as_ptr(), unit.as_ptr(), value, ndigits, es);
    }
}

fn explain_bool(es: *mut pg_sys::ExplainState, label: &str, value: bool) {
    let label = CString::new(label).unwrap_or_default();
    unsafe {
        pg_sys::ExplainPropertyBool(label.as_ptr(), value, es);
    }
}

/// Renders a list via `ExplainPropertyList` (TEXT comma-separated; JSON/YAML arrays).
fn explain_list(es: *mut pg_sys::ExplainState, label: &str, items: &[impl AsRef<str>]) {
    if items.is_empty() {
        return;
    }
    let label = CString::new(label).unwrap_or_default();
    unsafe {
        let mut list: *mut pg_sys::List = std::ptr::null_mut();
        for item in items {
            let c = CString::new(item.as_ref()).unwrap_or_default();
            // ExplainPropertyList expects a List of C strings (not String nodes).
            let pg_str = pg_sys::pstrdup(c.as_ptr());
            list = pg_sys::lappend(list, pg_str.cast());
        }
        pg_sys::ExplainPropertyList(label.as_ptr(), list, es);
    }
}

fn explain_open_group(
    es: *mut pg_sys::ExplainState,
    objtype: &str,
    labelname: Option<&str>,
    labeled: bool,
) {
    let objtype_c = CString::new(objtype).unwrap_or_default();
    let label_c = labelname.map(|l| CString::new(l).unwrap_or_default());
    unsafe {
        // ExplainOpenGroup is a no-op for TEXT; mirror JIT / Gather by writing
        // "Label:\n" and bumping indent ourselves.
        if (*es).format == pg_sys::ExplainFormat::EXPLAIN_FORMAT_TEXT {
            if let Some(label) = labelname {
                explain_text_section_header(es, label);
                (*es).indent = (*es).indent.saturating_add(1);
            }
        }
        pg_sys::ExplainOpenGroup(
            objtype_c.as_ptr(),
            label_c
                .as_ref()
                .map_or(std::ptr::null(), |value| value.as_ptr()),
            labeled,
            es,
        );
    }
}

fn explain_close_group(
    es: *mut pg_sys::ExplainState,
    objtype: &str,
    labelname: Option<&str>,
    labeled: bool,
) {
    let objtype_c = CString::new(objtype).unwrap_or_default();
    let label_c = labelname.map(|l| CString::new(l).unwrap_or_default());
    unsafe {
        pg_sys::ExplainCloseGroup(
            objtype_c.as_ptr(),
            label_c
                .as_ref()
                .map_or(std::ptr::null(), |value| value.as_ptr()),
            labeled,
            es,
        );
        if (*es).format == pg_sys::ExplainFormat::EXPLAIN_FORMAT_TEXT && labelname.is_some() {
            (*es).indent = (*es).indent.saturating_sub(1);
        }
    }
}

/// Replicates static `ExplainIndentText` + `"Label:\n"` used by JIT / Gather.
fn explain_text_section_header(es: *mut pg_sys::ExplainState, label: &str) {
    unsafe {
        explain_indent_text(es);
        let line = CString::new(format!("{label}:\n")).unwrap_or_default();
        pg_sys::appendStringInfoString((*es).str_, line.as_ptr());
    }
}

/// Port of PostgreSQL's static `ExplainIndentText` (not exported from explain.c).
unsafe fn explain_indent_text(es: *mut pg_sys::ExplainState) {
    if (*es).format != pg_sys::ExplainFormat::EXPLAIN_FORMAT_TEXT {
        return;
    }
    let str_info = (*es).str_;
    if str_info.is_null() {
        return;
    }
    let len = (*str_info).len;
    if len > 0 {
        let last = *((*str_info).data).offset((len - 1) as isize);
        if last == b'\n' as std::os::raw::c_char {
            pg_sys::appendStringInfoSpaces(str_info, (*es).indent * 2);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{format_bytes_human, ProfileCollectionMode};

    #[test]
    fn format_bytes_human_uses_readable_units() {
        assert_eq!(format_bytes_human(512), "512 bytes");
        assert_eq!(format_bytes_human(2048), "2.0 kB");
        assert_eq!(format_bytes_human(1_889_000), "1.8 MB");
        assert_eq!(format_bytes_human(3_221_225_472), "3.0 GB");
    }

    #[test]
    fn profile_collection_requires_postgresql_instrumentation() {
        assert_eq!(
            ProfileCollectionMode::from_instrumentation(false, false),
            ProfileCollectionMode::Disabled
        );
        assert_eq!(
            ProfileCollectionMode::from_instrumentation(true, false),
            ProfileCollectionMode::Counts
        );
        assert_eq!(
            ProfileCollectionMode::from_instrumentation(true, true),
            ProfileCollectionMode::CountsAndTiming
        );
    }
}
