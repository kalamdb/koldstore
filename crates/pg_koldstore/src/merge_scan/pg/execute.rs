//! Runtime source selection and row materialization for KoldMergeScan.
//!
//! This module owns the PostgreSQL-facing hot/cold/mirror execution flow.
//! Pure winner resolution remains in `koldstore-merge`; SPI, plan-state, and
//! PostgreSQL memory-context work must remain in the extension crate.

use std::collections::VecDeque;
use std::time::Instant;

use koldstore_merge::{NewestFirstWinnerResolver, ResolvedRow, RowSource};
use koldstore_migrate::{order::CatalogColumn, ExistingTableCatalog};
use pgrx::pg_sys;

use super::cold::{prepare_cold_row_stream, ColdRowStream};
use super::emit::materialize_scan_row_from_image;
use super::hot::{load_hot_rows_native, HotEqualityFilter, HotMergeBatchReader, HotRangeFilter};
use super::mirror::{filter_cold_rows_with_overlay, load_mirror_tombstone_overlay, MirrorOverlay};
use super::profile::{
    elapsed_ms, ColdReadProfile, DisabledScanProfiler, EmitPath, ScanExecutionProfile,
    ScanProfileSink, ScanProfiler,
};
use super::qual::ScanProjection;
use super::tuple::{MaterializedRow, ScanMemory};
use super::{hot_child_planstate, initialize_custom_plan_children, ScanEmitMode, CUSTOM_PATH_NAME};

/// Inputs prepared by `BeginCustomScan` for source execution.
pub(super) struct ScanSourceInputs<'a> {
    pub(super) node: *mut pg_sys::CustomScanState,
    pub(super) estate: *mut pg_sys::EState,
    pub(super) eflags: i32,
    pub(super) table_oid: pg_sys::Oid,
    pub(super) scanrelid: pg_sys::Index,
    pub(super) relation_owner: pg_sys::Oid,
    pub(super) relation: &'a str,
    pub(super) snapshot: &'a koldstore_catalog::ManagedTableSnapshot,
    pub(super) catalog: &'a ExistingTableCatalog,
    pub(super) qual: *mut pg_sys::List,
    pub(super) params: pg_sys::ParamListInfo,
    pub(super) projection: &'a ScanProjection,
    pub(super) image_columns: &'a [&'a CatalogColumn],
    pub(super) pk_equality: &'a [HotEqualityFilter],
    pub(super) pk_range: &'a [HotRangeFilter],
    pub(super) pk_point_lookup: bool,
}

/// Source execution result stored in the Custom Scan's backend-local state.
pub(super) struct ScanSourceExecution {
    pub(super) mode: ScanEmitMode,
    pub(super) cold_profile: ColdReadProfile,
    pub(super) emit_path: EmitPath,
    pub(super) hot_rows: usize,
    pub(super) memory: ScanMemory,
}

/// Payload-bounded hot/cold winner stream owned by one CustomScan.
///
/// Hot JSON pages and cold segment groups are loaded lazily. Peak retained row
/// images are one hot SPI batch plus one cold segment group. Exact PK identities
/// remain in the resolver for the full scan.
#[derive(Debug)]
pub(super) struct MergeRowStream {
    hot: HotMergeBatchReader,
    cold: ColdRowStream,
    overlay: MirrorOverlay,
    resolver: NewestFirstWinnerResolver,
    hot_winners: VecDeque<ResolvedRow>,
    cold_winners: VecDeque<ResolvedRow>,
    /// True after every hot page has been folded into `seen` (or replayed) and
    /// the mirror overlay has been checkpointed on the first pass.
    hot_phase_done: bool,
    /// Rescan reloads hot payloads for emit without re-inserting identities.
    replay_hot: bool,
}

impl MergeRowStream {
    fn new(hot: HotMergeBatchReader, cold: ColdRowStream, overlay: MirrorOverlay) -> Self {
        let max_seen = crate::guc::max_merge_seen_keys() as usize;
        Self {
            hot,
            cold,
            overlay,
            resolver: NewestFirstWinnerResolver::default().with_max_seen_keys(Some(max_seen)),
            hot_winners: VecDeque::new(),
            cold_winners: VecDeque::new(),
            hot_phase_done: false,
            replay_hot: false,
        }
    }

    /// Materializes the next winner while retaining at most one hot SPI page and
    /// one cold segment-group of decoded payloads.
    pub(super) unsafe fn next_materialized(
        &mut self,
        projection: &ScanProjection,
        memory: &mut ScanMemory,
        cold_profile: &mut ColdReadProfile,
        mut execution: Option<&mut ScanExecutionProfile>,
    ) -> Result<Option<MaterializedRow>, String> {
        loop {
            if let Some(row) = self.hot_winners.pop_front() {
                return materialize_owned_row(row, projection, memory, execution.as_deref_mut())
                    .map(Some);
            }
            if !self.hot_phase_done {
                self.load_next_hot_page(execution.as_deref_mut())?;
                if !self.hot_winners.is_empty() {
                    continue;
                }
                // Hot exhausted for this pass.
                if !self.replay_hot {
                    self.resolver
                        .mask_older_pks(self.overlay.masked_pks.iter().cloned())
                        .map_err(seen_key_limit_error)?;
                    self.resolver.checkpoint();
                }
                if let Some(execution) = execution.as_deref_mut() {
                    execution.seen_key_count = self.resolver.seen_key_count();
                }
                self.hot_phase_done = true;
                continue;
            }

            if let Some(row) = self.cold_winners.pop_front() {
                return materialize_owned_row(row, projection, memory, execution.as_deref_mut())
                    .map(Some);
            }

            let collect_profile = execution.is_some();
            let Some((cold_rows, segment_profiles)) = self.cold.next_batch(collect_profile)? else {
                return Ok(None);
            };
            let decoded_rows = cold_rows.len();
            if let Some(execution) = execution.as_deref_mut() {
                cold_profile.segments_opened += segment_profiles.len();
                cold_profile.segments.extend(segment_profiles);
                execution.cold_rows += decoded_rows;
                execution.peak_cold_batch_rows = execution.peak_cold_batch_rows.max(decoded_rows);
            }

            let overlay_input = cold_rows.len();
            let overlay_started = execution.as_ref().map(|_| Instant::now());
            let cold_rows = filter_cold_rows_with_overlay(cold_rows, &self.overlay);
            let overlay_removed = overlay_input.saturating_sub(cold_rows.len());
            let merge_input = cold_rows.len();
            let merge_started = execution.as_ref().map(|_| Instant::now());
            let winners = self
                .resolver
                .resolve_cold_batch(cold_rows)
                .map_err(seen_key_limit_error)?;
            if let Some(execution) = execution.as_deref_mut() {
                execution.overlay_rows_removed += overlay_removed;
                accumulate_ms(&mut execution.overlay_ms, overlay_started);
                execution.merge_executed = true;
                execution.merge_input_rows += merge_input;
                execution.merge_output_rows += winners.len();
                execution.merge_rows_removed = execution
                    .merge_input_rows
                    .saturating_sub(execution.merge_output_rows);
                accumulate_ms(&mut execution.merge_ms, merge_started);
                execution.seen_key_count = self.resolver.seen_key_count();
            }
            self.cold_winners = VecDeque::from(winners);
        }
    }

    fn load_next_hot_page(
        &mut self,
        execution: Option<&mut ScanExecutionProfile>,
    ) -> Result<(), String> {
        let started = execution.as_ref().map(|_| Instant::now());
        let Some(rows) = self.hot.next_batch()? else {
            return Ok(());
        };
        let fetched = rows.len();
        let merge_started = execution.as_ref().map(|_| Instant::now());
        let winners = if self.replay_hot {
            rows.into_iter().map(hot_row_as_resolved).collect()
        } else {
            self.resolver
                .resolve_hot_batch(rows)
                .map_err(seen_key_limit_error)?
        };
        if let Some(execution) = execution {
            accumulate_ms(&mut execution.hot_scan_ms, started);
            execution.hot_rows += fetched;
            execution.peak_hot_batch_rows = execution.peak_hot_batch_rows.max(fetched);
            execution.merge_executed = true;
            execution.merge_input_rows += fetched;
            execution.merge_output_rows += winners.len();
            execution.merge_rows_removed = execution
                .merge_input_rows
                .saturating_sub(execution.merge_output_rows);
            accumulate_ms(&mut execution.merge_ms, merge_started);
            execution.seen_key_count = self.resolver.seen_key_count();
        }
        self.hot_winners = VecDeque::from(winners);
        Ok(())
    }

    pub(super) fn reset(&mut self) {
        self.cold.reset();
        self.resolver.reset();
        self.hot.reset();
        self.hot_winners.clear();
        self.cold_winners.clear();
        self.hot_phase_done = false;
        // Checkpoint already holds first-pass hot + tombstone identities, so
        // rescan reloads hot payloads for emit only.
        self.replay_hot = true;
    }
}

fn seen_key_limit_error(error: koldstore_merge::SeenKeyLimitExceeded) -> String {
    format!(
        "{CUSTOM_PATH_NAME} retained too many exact primary-key identities \
         (seen={}, limit={}). Raise koldstore.max_merge_seen_keys for large \
         intentional scans, set it to 0 to disable the cap, or add filters to \
         reduce distinct keys.",
        error.seen, error.limit
    )
}

fn hot_row_as_resolved(row: koldstore_common::HotRow) -> ResolvedRow {
    ResolvedRow {
        pk_json: row.pk.to_canonical_json(),
        source: RowSource::Hot,
        seq: row.seq,
        row_image: row.row_image,
        deleted: row.deleted,
    }
}

unsafe fn materialize_owned_row(
    row: ResolvedRow,
    projection: &ScanProjection,
    memory: &mut ScanMemory,
    execution: Option<&mut ScanExecutionProfile>,
) -> Result<MaterializedRow, String> {
    let started = execution.as_ref().map(|_| Instant::now());
    let materialized =
        memory.switch(|| materialize_scan_row_from_image(&row.row_image, projection));
    if let Some(execution) = execution {
        accumulate_ms(&mut execution.materialization_ms, started);
    }
    // `row` drops here, releasing the JSON payload after Datum materialization.
    drop(row);
    materialized
}

fn accumulate_ms(total: &mut Option<f64>, started: Option<Instant>) {
    let Some(started) = started else {
        return;
    };
    *total = Some(total.unwrap_or(0.0) + elapsed_ms(started));
}

/// Selects and executes the hot, cold, mirror, and winner-resolution paths.
///
/// PostgreSQL errors abort the active backend invocation; successful execution
/// returns scan-owned rows and memory ready for `ExecCustomScan`.
pub(super) unsafe fn execute_scan_sources(
    inputs: ScanSourceInputs<'_>,
    profiler: &mut ScanProfiler,
) -> ScanSourceExecution {
    execute_scan_sources_with_profile(inputs, profiler)
}

/// Executes sources without counters, clocks, allocation, or profiling branches.
#[inline(always)]
pub(super) unsafe fn execute_scan_sources_unprofiled(
    inputs: ScanSourceInputs<'_>,
) -> ScanSourceExecution {
    execute_scan_sources_with_profile(inputs, &mut DisabledScanProfiler)
}

#[inline(always)]
unsafe fn execute_scan_sources_with_profile<P: ScanProfileSink>(
    inputs: ScanSourceInputs<'_>,
    profiler: &mut P,
) -> ScanSourceExecution {
    let mut memory = ScanMemory::create("KoldMergeScan");

    // Full-PK probes run before Parquet opens. A hot winner makes every older
    // cold version irrelevant and keeps the common point-hit path hot-only.
    if let Some(rows) = probe_hot_point_hit(&inputs, &mut memory, profiler) {
        return hot_buffer_execution(
            rows,
            ColdReadProfile::empty("(none)"),
            &inputs,
            memory,
            profiler,
        );
    }

    let (cold_profile, cold_stream) = prepare_cold_stream(&inputs);
    let has_no_cold_source = cold_stream.is_none();
    if has_no_cold_source {
        initialize_custom_plan_children(inputs.node, inputs.estate, inputs.eflags);
    }

    let (mode, emit_path, hot_rows) = match cold_stream {
        None if hot_child_planstate(inputs.node).is_some() => (
            ScanEmitMode::HotChild { prefetched: None },
            EmitPath::HotChild,
            0,
        ),
        None => {
            let started = profiler.start_timer();
            let rows = load_native_hot_rows(&inputs, &mut memory, "hot-only read");
            profiler.record_hot_scan(started);
            let hot_rows = rows.len();
            profiler.record_hot_buffer(hot_rows);
            (
                ScanEmitMode::buffer(rows, inputs.projection),
                EmitPath::HotNative,
                hot_rows,
            )
        }
        Some(cold_stream) if inputs.pk_point_lookup => {
            prepare_cold_point_stream(cold_stream, &inputs, profiler)
        }
        Some(cold_stream) => prepare_merged_stream(cold_stream, &inputs, profiler),
    };

    ScanSourceExecution {
        mode,
        cold_profile,
        emit_path,
        hot_rows,
        memory,
    }
}

#[inline(always)]
fn probe_hot_point_hit<P: ScanProfileSink>(
    inputs: &ScanSourceInputs<'_>,
    memory: &mut ScanMemory,
    profiler: &mut P,
) -> Option<Vec<MaterializedRow>> {
    if !inputs.pk_point_lookup {
        return None;
    }
    let started = profiler.start_timer();
    let rows = load_native_hot_rows(inputs, memory, "hot probe");
    profiler.record_hot_scan(started);
    (!rows.is_empty()).then_some(rows)
}

#[inline(always)]
fn load_native_hot_rows(
    inputs: &ScanSourceInputs<'_>,
    memory: &mut ScanMemory,
    operation: &str,
) -> Vec<MaterializedRow> {
    match crate::catalog::owner::with_relation_owner_for_merge(inputs.relation_owner, || {
        load_hot_rows_native(
            inputs.relation,
            inputs.pk_equality,
            inputs.image_columns,
            inputs.projection,
            memory,
        )
    }) {
        Ok(rows) => rows,
        Err(error) => pgrx::error!("{CUSTOM_PATH_NAME} {operation} failed: {error}"),
    }
}

#[inline(always)]
fn hot_buffer_execution<P: ScanProfileSink>(
    rows: Vec<MaterializedRow>,
    cold_profile: ColdReadProfile,
    inputs: &ScanSourceInputs<'_>,
    memory: ScanMemory,
    profiler: &mut P,
) -> ScanSourceExecution {
    let hot_rows = rows.len();
    profiler.record_hot_buffer(hot_rows);
    ScanSourceExecution {
        mode: ScanEmitMode::buffer(rows, inputs.projection),
        cold_profile,
        emit_path: EmitPath::HotNative,
        hot_rows,
        memory,
    }
}

#[inline(always)]
fn prepare_cold_stream(inputs: &ScanSourceInputs<'_>) -> (ColdReadProfile, Option<ColdRowStream>) {
    prepare_cold_row_stream(
        inputs.table_oid,
        inputs.scanrelid,
        inputs.snapshot,
        inputs.catalog,
        inputs.qual,
        inputs.image_columns,
        inputs.params,
    )
    .unwrap_or_else(|error| pgrx::error!("{CUSTOM_PATH_NAME} cold stream setup failed: {error}"))
}

fn prepare_cold_point_stream<P: ScanProfileSink>(
    cold_stream: ColdRowStream,
    inputs: &ScanSourceInputs<'_>,
    profiler: &mut P,
) -> (ScanEmitMode, EmitPath, usize) {
    let overlay = load_overlay(inputs, profiler);
    profiler.record_hot_buffer(0);
    let stream = MergeRowStream::new(
        HotMergeBatchReader::empty(inputs.relation_owner),
        cold_stream,
        overlay,
    );
    (
        ScanEmitMode::stream(stream, inputs.projection.clone()),
        EmitPath::ColdNative,
        0,
    )
}

fn prepare_merged_stream<P: ScanProfileSink>(
    cold_stream: ColdRowStream,
    inputs: &ScanSourceInputs<'_>,
    profiler: &mut P,
) -> (ScanEmitMode, EmitPath, usize) {
    let overlay = load_overlay(inputs, profiler);

    let hot = HotMergeBatchReader::open(
        inputs.relation,
        inputs.snapshot,
        inputs.pk_equality,
        inputs.pk_range,
        inputs.image_columns,
        inputs.relation_owner,
    )
    .unwrap_or_else(|error| pgrx::error!("{CUSTOM_PATH_NAME} hot reader setup failed: {error}"));
    if let Some(sql) = hot.first_page_sql() {
        profiler.record_hot_spi_query(sql);
    }
    // Hot pages load during ExecCustomScan; EXPLAIN counters accumulate there.
    profiler.record_hot_buffer(0);
    let stream = MergeRowStream::new(hot, cold_stream, overlay);
    (
        ScanEmitMode::stream(stream, inputs.projection.clone()),
        EmitPath::MergeStream,
        0,
    )
}

fn load_overlay<P: ScanProfileSink>(
    inputs: &ScanSourceInputs<'_>,
    profiler: &mut P,
) -> MirrorOverlay {
    let started = profiler.start_timer();
    let overlay = load_mirror_tombstone_overlay(
        &inputs.snapshot.mirror_relation,
        &inputs.snapshot.primary_key_columns,
        inputs.pk_equality,
    )
    .unwrap_or_else(|error| pgrx::error!("{CUSTOM_PATH_NAME} mirror overlay failed: {error}"));
    profiler.record_mirror_scan(overlay.tombstones, started);
    overlay
}
