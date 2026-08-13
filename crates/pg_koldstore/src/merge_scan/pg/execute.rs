//! Runtime source selection and row materialization for KoldMergeScan.
//!
//! This module owns the PostgreSQL-facing hot/cold/mirror execution flow.
//! Pure winner resolution remains in `koldstore-merge`; SPI, plan-state, and
//! PostgreSQL memory-context work must remain in the extension crate.

use std::collections::VecDeque;
use std::time::Instant;

use koldstore_merge::scan::{hot_keys_dominate_bound, OrderDirection};
use koldstore_merge::{MirrorOverlay, NewestFirstWinnerResolver, ResolvedRow, RowSource};
use koldstore_migrate::{order::CatalogColumn, ExistingTableCatalog};
use pgrx::pg_sys;

use super::cold::{prepare_cold_row_stream, ColdReadPhase, ColdRowStream, ColdStreamPlanRequest};
use super::cold_frontier;
use super::emit::materialize_scan_row_from_image;
use super::hot::{load_hot_rows_native, HotEqualityFilter, HotMergeBatchReader, HotRangeFilter};
use super::hot_cursor::{HotMergeSource, NativeHotCursor};
use super::mirror::{load_mirror_tombstone_overlay, load_mirror_tombstones_for_pks};
use super::path_strategy::{STRATEGY_TAG_ORDERED_PROGRESSIVE, STRATEGY_TAG_UNORDERED_HOT_FIRST};
use super::profile::{
    elapsed_ms, ColdReadProfile, DisabledScanProfiler, EmitPath, ProfileCollectionMode,
    ScanExecutionProfile, ScanProfileSink, ScanProfiler,
};
use super::qual::ScanProjection;
use super::tuple::{MaterializedRow, ScanMemory};
use super::{
    custom_private_strategy_tag, hot_child_planstate, initialize_custom_plan_children,
    ScanEmitMode, CUSTOM_PATH_NAME,
};

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
    pub(super) catalog: std::sync::Arc<ExistingTableCatalog>,
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
    hot: HotMergeSource,
    cold: ColdRowStream,
    overlay: MirrorOverlay,
    /// When set, tombstones load on first cold batch (ordered progressive).
    deferred_overlay: Option<DeferredOverlayLoad>,
    resolver: NewestFirstWinnerResolver,
    hot_winners: VecDeque<ResolvedRow>,
    cold_winners: VecDeque<ResolvedRow>,
    /// True after every hot page has been folded into `seen` (or replayed) and
    /// the mirror overlay has been checkpointed on the first pass.
    hot_phase_done: bool,
    /// Rescan reloads hot payloads for emit without re-inserting identities.
    replay_hot: bool,
    /// Bound-gated ordered progressive control; `None` for unordered merge.
    ordered: Option<OrderedProgressiveCtrl>,
}

#[derive(Debug)]
struct DeferredOverlayLoad {
    mirror_relation: koldstore_common::TableName,
    primary_key_columns: Vec<koldstore_common::ColumnRef>,
}

/// Runtime control for `OrderedProgressive` emit ordering.
#[derive(Debug)]
struct OrderedProgressiveCtrl {
    direction: OrderDirection,
    cold_bound: Option<Vec<u8>>,
    leading_column: String,
    leading_type_oid: u32,
    table_oid: pg_sys::Oid,
    scope_key: String,
    sort_order_id: i32,
    mode: OrderedEmitMode,
    /// When true, first cold emit from [`OrderedEmitMode::SortedBuffer`] hydrates body.
    body_hydrate_pending: bool,
}

#[derive(Debug)]
enum OrderedEmitMode {
    /// Still deciding per hot page whether cold can win/tie.
    Streaming,
    /// Drain remaining hot+cold, resolve, sort by leading Sort Key, emit.
    SortedBuffer(VecDeque<ResolvedRow>),
}

impl MergeRowStream {
    fn new(hot: HotMergeSource, cold: ColdRowStream, overlay: MirrorOverlay) -> Self {
        Self::new_inner(hot, cold, overlay, None, None)
    }

    fn new_ordered_deferred_overlay(
        hot: HotMergeSource,
        cold: ColdRowStream,
        mirror_relation: koldstore_common::TableName,
        primary_key_columns: Vec<koldstore_common::ColumnRef>,
    ) -> Self {
        Self::new_inner(
            hot,
            cold,
            MirrorOverlay::default(),
            Some(DeferredOverlayLoad {
                mirror_relation,
                primary_key_columns,
            }),
            None,
        )
    }

    fn new_ordered_progressive(
        hot: HotMergeSource,
        cold: ColdRowStream,
        mirror_relation: koldstore_common::TableName,
        primary_key_columns: Vec<koldstore_common::ColumnRef>,
        ordered: OrderedProgressiveCtrl,
    ) -> Self {
        Self::new_inner(
            hot,
            cold,
            MirrorOverlay::default(),
            Some(DeferredOverlayLoad {
                mirror_relation,
                primary_key_columns,
            }),
            Some(ordered),
        )
    }

    fn new_inner(
        hot: HotMergeSource,
        cold: ColdRowStream,
        overlay: MirrorOverlay,
        deferred_overlay: Option<DeferredOverlayLoad>,
        ordered: Option<OrderedProgressiveCtrl>,
    ) -> Self {
        let max_seen = crate::guc::max_merge_seen_keys() as usize;
        Self {
            hot,
            cold,
            overlay,
            deferred_overlay,
            resolver: NewestFirstWinnerResolver::default().with_max_seen_keys(Some(max_seen)),
            hot_winners: VecDeque::new(),
            cold_winners: VecDeque::new(),
            hot_phase_done: false,
            replay_hot: false,
            ordered,
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
            // Lazy body hydrate: open body columns only when a cold winner is
            // about to emit. Parent LIMIT that stops on hot-only keeps body opens at 0.
            if self.ordered_needs_body_hydrate() {
                self.hydrate_ordered_cold_bodies(cold_profile, execution.as_deref_mut())?;
            }

            if let Some(ctrl) = self.ordered.as_mut() {
                if let OrderedEmitMode::SortedBuffer(queue) = &mut ctrl.mode {
                    return match queue.pop_front() {
                        Some(row) => {
                            materialize_owned_row(row, projection, memory, execution.as_deref_mut())
                                .map(Some)
                        }
                        None => Ok(None),
                    };
                }
            }

            if let Some(row) = self.hot_winners.pop_front() {
                return materialize_owned_row(row, projection, memory, execution.as_deref_mut())
                    .map(Some);
            }
            if !self.hot_phase_done {
                self.load_next_hot_page(execution.as_deref_mut())?;
                if !self.hot_winners.is_empty() {
                    if self.ordered.is_some() {
                        self.maybe_enter_ordered_buffer(execution.as_deref_mut(), cold_profile)?;
                    }
                    continue;
                }
                // Hot exhausted for this pass.
                if self.ordered.is_some() && !self.replay_hot {
                    // Empty or exhausted hot with a cold frontier: sort remaining cold.
                    self.maybe_enter_ordered_buffer(execution.as_deref_mut(), cold_profile)?;
                    if matches!(
                        self.ordered.as_ref().map(|o| &o.mode),
                        Some(OrderedEmitMode::SortedBuffer(_))
                    ) {
                        continue;
                    }
                }
                if !self.replay_hot {
                    self.resolver
                        .mask_older_pks(self.overlay.iter().cloned())
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

            let profile_mode = execution_profile_mode(execution.as_deref());
            let Some((cold_rows, segment_profiles)) =
                self.cold.next_batch(ColdReadPhase::Full, profile_mode)?
            else {
                return Ok(None);
            };
            let decoded_rows = cold_rows.len();
            if let Some(execution) = execution.as_deref_mut() {
                cold_profile.segments_opened += segment_profiles.len();
                cold_profile.segments.extend(segment_profiles);
                execution.cold_rows += decoded_rows;
                execution.peak_cold_batch_rows = execution.peak_cold_batch_rows.max(decoded_rows);
            }

            // Batched tombstone probe for this cold page only (deferred overlay).
            if self.deferred_overlay.is_some() {
                self.probe_overlay_for_cold_batch(&cold_rows, execution.as_deref_mut())?;
            }

            let overlay_started = execution_timer(execution.as_deref());
            let mut cold_rows = cold_rows;
            let overlay_removed = self.overlay.retain_unmasked(&mut cold_rows);
            let merge_input = cold_rows.len();
            let merge_started = execution_timer(execution.as_deref());
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

    /// True when the next SortedBuffer emit is a cold row still missing body columns.
    fn ordered_needs_body_hydrate(&self) -> bool {
        let Some(ctrl) = self.ordered.as_ref() else {
            return false;
        };
        if !ctrl.body_hydrate_pending {
            return false;
        }
        match &ctrl.mode {
            OrderedEmitMode::SortedBuffer(queue) => queue
                .front()
                .is_some_and(|row| row.source == RowSource::Cold),
            OrderedEmitMode::Streaming => false,
        }
    }

    /// Re-opens competitive segments for body columns and merges into cold winners.
    fn hydrate_ordered_cold_bodies(
        &mut self,
        cold_profile: &mut ColdReadProfile,
        mut execution: Option<&mut ScanExecutionProfile>,
    ) -> Result<(), String> {
        if !self.cold.late_materialization_enabled() {
            if let Some(ctrl) = self.ordered.as_mut() {
                ctrl.body_hydrate_pending = false;
            }
            return Ok(());
        }

        self.cold.reset();
        let mut body_by_pk_seq: std::collections::HashMap<
            (serde_json::Value, i64),
            koldstore_common::RowImage,
        > = std::collections::HashMap::new();
        loop {
            let profile_mode = execution_profile_mode(execution.as_deref());
            let Some((cold_rows, segment_profiles)) = self.cold.next_body_batch(profile_mode)?
            else {
                break;
            };
            let decoded_rows = cold_rows.len();
            if let Some(execution) = execution.as_deref_mut() {
                let opened = segment_profiles.len();
                cold_profile.body_opens += opened;
                cold_profile.segments_opened += opened;
                cold_profile.segments.extend(segment_profiles);
                execution.cold_rows += decoded_rows;
                execution.peak_cold_batch_rows = execution.peak_cold_batch_rows.max(decoded_rows);
            }
            for row in cold_rows {
                let pk_json = row.pk.to_canonical_json();
                body_by_pk_seq.insert((pk_json, row.seq.get()), row.row_image);
            }
        }

        if let Some(ctrl) = self.ordered.as_mut() {
            if let OrderedEmitMode::SortedBuffer(queue) = &mut ctrl.mode {
                for row in queue.iter_mut() {
                    if row.source != RowSource::Cold {
                        continue;
                    }
                    let key = (row.pk_json.clone(), row.seq.get());
                    if let Some(body) = body_by_pk_seq.get(&key) {
                        for (name, value) in body.iter() {
                            row.row_image.insert(name.clone(), value.clone());
                        }
                    }
                }
            }
            ctrl.body_hydrate_pending = false;
        }
        Ok(())
    }

    /// When the loaded hot page cannot strictly dominate cold, drain + sort.
    fn maybe_enter_ordered_buffer(
        &mut self,
        mut execution: Option<&mut ScanExecutionProfile>,
        cold_profile: &mut ColdReadProfile,
    ) -> Result<(), String> {
        let Some(ctrl) = self.ordered.as_ref() else {
            return Ok(());
        };
        if matches!(ctrl.mode, OrderedEmitMode::SortedBuffer(_)) {
            return Ok(());
        }
        let keys = self
            .hot_winners
            .iter()
            .map(|row| encode_leading_key(row, &ctrl.leading_column, ctrl.leading_type_oid))
            .collect::<Vec<_>>();
        // Missing catalog bound must not imply "skip cold" while segments remain:
        // that regresses ORDER BY ASC LIMIT after hot prune (lower keys live in
        // Parquet). Only dominate when the frontier is empty or every hot key
        // strictly outranks the bound.
        let dominates = if ctrl.cold_bound.is_none() {
            !self.cold.has_pending_segments()
        } else if keys.is_empty() {
            false
        } else {
            hot_keys_dominate_bound(ctrl.direction, &keys, ctrl.cold_bound.as_deref())
        };
        if dominates {
            return Ok(());
        }

        // Competitive RG prune before opening Parquet. Full buffer sort needs
        // every group that can appear in the remaining result; pass no hot tip.
        let table_oid = ctrl.table_oid;
        let scope_key = ctrl.scope_key.clone();
        let sort_order_id = ctrl.sort_order_id;
        let direction = ctrl.direction;
        self.cold.apply_competitive_row_groups(
            table_oid,
            &scope_key,
            sort_order_id,
            direction,
            None,
        )?;

        let mut buffered = Vec::new();
        buffered.extend(self.hot_winners.drain(..));
        while !self.hot_phase_done {
            self.load_next_hot_page(execution.as_deref_mut())?;
            if self.hot_winners.is_empty() {
                if !self.replay_hot {
                    self.resolver.checkpoint();
                }
                self.hot_phase_done = true;
                break;
            }
            buffered.extend(self.hot_winners.drain(..));
        }

        let compete_phase = self.cold.late_materialization_enabled();
        let cold_phase = if compete_phase {
            ColdReadPhase::Compete
        } else {
            ColdReadPhase::Full
        };
        loop {
            let profile_mode = execution_profile_mode(execution.as_deref());
            let Some((cold_rows, segment_profiles)) =
                self.cold.next_batch(cold_phase, profile_mode)?
            else {
                break;
            };
            let decoded_rows = cold_rows.len();
            if let Some(execution) = execution.as_deref_mut() {
                let opened = segment_profiles.len();
                cold_profile.segments_opened += opened;
                if compete_phase {
                    cold_profile.compete_opens += opened;
                }
                cold_profile.segments.extend(segment_profiles);
                execution.cold_rows += decoded_rows;
                execution.peak_cold_batch_rows = execution.peak_cold_batch_rows.max(decoded_rows);
            }
            if self.deferred_overlay.is_some() {
                self.probe_overlay_for_cold_batch(&cold_rows, execution.as_deref_mut())?;
            }
            let overlay_input = cold_rows.len();
            let mut cold_rows = cold_rows;
            let overlay_removed = self.overlay.retain_unmasked(&mut cold_rows);
            let winners = self
                .resolver
                .resolve_cold_batch(cold_rows)
                .map_err(seen_key_limit_error)?;
            if let Some(execution) = execution.as_deref_mut() {
                execution.overlay_rows_removed += overlay_removed;
                execution.merge_executed = true;
                execution.merge_input_rows += overlay_input.saturating_sub(overlay_removed);
                execution.merge_output_rows += winners.len();
                execution.seen_key_count = self.resolver.seen_key_count();
            }
            buffered.extend(winners);
        }

        let Some(ctrl) = self.ordered.as_ref() else {
            return Err(
                "ordered progressive control missing when entering sort buffer".to_string(),
            );
        };
        let direction = ctrl.direction;
        let leading_column = ctrl.leading_column.clone();
        let leading_type_oid = ctrl.leading_type_oid;
        buffered.sort_by(|left, right| {
            let left_key = encode_leading_key(left, &leading_column, leading_type_oid);
            let right_key = encode_leading_key(right, &leading_column, leading_type_oid);
            match direction {
                OrderDirection::Asc => left_key.cmp(&right_key),
                OrderDirection::Desc => right_key.cmp(&left_key),
            }
        });
        let needs_body_hydrate = compete_phase
            && buffered
                .iter()
                .any(|row| row.source == RowSource::Cold && !row.deleted);
        if let Some(ctrl) = self.ordered.as_mut() {
            ctrl.mode = OrderedEmitMode::SortedBuffer(VecDeque::from(buffered));
            ctrl.body_hydrate_pending = needs_body_hydrate;
        }
        self.cold_winners.clear();
        Ok(())
    }

    fn probe_overlay_for_cold_batch(
        &mut self,
        cold_rows: &[koldstore_common::ColdRow],
        execution: Option<&mut ScanExecutionProfile>,
    ) -> Result<(), String> {
        let Some(deferred) = self.deferred_overlay.as_ref() else {
            return Ok(());
        };
        let mut unseen = Vec::new();
        for row in cold_rows {
            if !self.overlay.contains(&row.pk) {
                unseen.push(row.pk.clone());
            }
        }
        if unseen.is_empty() {
            return Ok(());
        }
        let started = execution_timer(execution.as_deref());
        let batch = load_mirror_tombstones_for_pks(
            &deferred.mirror_relation,
            &deferred.primary_key_columns,
            &unseen,
        )?;
        if let Some(execution) = execution {
            execution.mirror_rows = execution.mirror_rows.saturating_add(batch.len());
            // Deferred PK probes are the ordered/unordered mirror scan phase.
            accumulate_ms(&mut execution.mirror_scan_ms, started);
        }
        for pk in batch.into_masked_pks() {
            self.overlay.insert(pk);
        }
        Ok(())
    }

    fn load_next_hot_page(
        &mut self,
        execution: Option<&mut ScanExecutionProfile>,
    ) -> Result<(), String> {
        let started = execution_timer(execution.as_deref());
        let Some(rows) = self.hot.next_batch()? else {
            return Ok(());
        };
        let fetched = rows.len();
        let merge_started = execution_timer(execution.as_deref());
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
        if let Some(ctrl) = self.ordered.as_mut() {
            ctrl.mode = OrderedEmitMode::Streaming;
        }
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
    let started = execution_timer(execution.as_deref());
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

#[inline]
fn execution_profile_mode(execution: Option<&ScanExecutionProfile>) -> ProfileCollectionMode {
    execution.map_or(ProfileCollectionMode::Disabled, |profile| {
        profile.collection_mode()
    })
}

#[inline]
fn execution_timer(execution: Option<&ScanExecutionProfile>) -> Option<Instant> {
    execution_profile_mode(execution)
        .collects_timing()
        .then(Instant::now)
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
        let cold_profile = if profiler.collection_mode().collects_counts() {
            ColdReadProfile::empty("(none)")
        } else {
            ColdReadProfile::disabled()
        };
        return hot_buffer_execution(rows, cold_profile, &inputs, memory, profiler);
    }

    let (mut cold_profile, cold_stream) = prepare_cold_stream(&inputs, profiler.collection_mode());
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
        Some(cold_stream) => {
            prepare_merged_stream(cold_stream, &mut cold_profile, &inputs, profiler)
        }
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
    // Always SPI-probe as relation owner (`SECURITY_NOFORCE_RLS`). The native
    // Index/Seq child applies session RLS, so a child miss can hide a newer hot
    // PK winner; skipping this probe would let a superseded cold row reappear
    // (user-scope / FORCE RLS point lookups).
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
fn prepare_cold_stream(
    inputs: &ScanSourceInputs<'_>,
    profile_mode: ProfileCollectionMode,
) -> (ColdReadProfile, Option<ColdRowStream>) {
    prepare_cold_row_stream(ColdStreamPlanRequest {
        table_oid: inputs.table_oid,
        scanrelid: inputs.scanrelid,
        snapshot: inputs.snapshot,
        catalog: &inputs.catalog,
        qual: inputs.qual,
        projected_columns: inputs.image_columns,
        params: inputs.params,
        profile_mode,
    })
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
        HotMergeSource::empty(inputs.relation_owner),
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
    cold_profile: &mut ColdReadProfile,
    inputs: &ScanSourceInputs<'_>,
    profiler: &mut P,
) -> (ScanEmitMode, EmitPath, usize) {
    let plan = unsafe { (*inputs.node).ss.ps.plan };
    let strategy_tag = unsafe { custom_private_strategy_tag(plan) };
    if strategy_tag == STRATEGY_TAG_ORDERED_PROGRESSIVE {
        return prepare_ordered_merged_stream(cold_stream, cold_profile, inputs, profiler);
    }
    if strategy_tag == STRATEGY_TAG_UNORDERED_HOT_FIRST {
        return prepare_unordered_hot_first_stream(cold_stream, inputs, profiler);
    }

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
    let hot = HotMergeSource::SpiJson(hot);
    if profiler.collection_mode().collects_counts() {
        if let Some(sql) = hot.first_page_sql() {
            profiler.record_hot_spi_query(sql);
        }
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

fn prepare_unordered_hot_first_stream<P: ScanProfileSink>(
    cold_stream: ColdRowStream,
    inputs: &ScanSourceInputs<'_>,
    profiler: &mut P,
) -> (ScanEmitMode, EmitPath, usize) {
    unsafe {
        initialize_custom_plan_children(inputs.node, inputs.estate, inputs.eflags);
    }
    let hot = match open_native_hot_cursor(inputs) {
        Ok(cursor) => HotMergeSource::NativeChild(cursor),
        Err(_) => {
            // Empty/`count(*)` child tlists omit PK Vars; SPI keyset still works.
            let reader = HotMergeBatchReader::open(
                inputs.relation,
                inputs.snapshot,
                inputs.pk_equality,
                inputs.pk_range,
                inputs.image_columns,
                inputs.relation_owner,
            )
            .unwrap_or_else(|error| {
                pgrx::error!("{CUSTOM_PATH_NAME} unordered hot-first SPI fallback failed: {error}")
            });
            if profiler.collection_mode().collects_counts() {
                if let Some(sql) = reader.first_page_sql() {
                    profiler.record_hot_spi_query(sql);
                }
            }
            HotMergeSource::SpiJson(reader)
        }
    };
    profiler.record_hot_buffer(0);
    // Defer mirror/cold until hot is exhausted under parent LIMIT.
    let stream = MergeRowStream::new_ordered_deferred_overlay(
        hot,
        cold_stream,
        inputs.snapshot.mirror_relation.clone(),
        inputs.snapshot.primary_key_columns.clone(),
    );
    (
        ScanEmitMode::stream(stream, inputs.projection.clone()),
        EmitPath::UnorderedHotFirst,
        0,
    )
}

fn prepare_ordered_merged_stream<P: ScanProfileSink>(
    mut cold_stream: ColdRowStream,
    cold_profile: &mut ColdReadProfile,
    inputs: &ScanSourceInputs<'_>,
    profiler: &mut P,
) -> (ScanEmitMode, EmitPath, usize) {
    unsafe {
        initialize_custom_plan_children(inputs.node, inputs.estate, inputs.eflags);
    }
    let hot = match open_native_hot_cursor(inputs) {
        Ok(cursor) => HotMergeSource::NativeChild(cursor),
        Err(_) => {
            let reader = HotMergeBatchReader::open(
                inputs.relation,
                inputs.snapshot,
                inputs.pk_equality,
                inputs.pk_range,
                inputs.image_columns,
                inputs.relation_owner,
            )
            .unwrap_or_else(|error| {
                pgrx::error!("{CUSTOM_PATH_NAME} ordered SPI fallback failed: {error}")
            });
            if profiler.collection_mode().collects_counts() {
                if let Some(sql) = reader.first_page_sql() {
                    profiler.record_hot_spi_query(sql);
                }
            }
            HotMergeSource::SpiJson(reader)
        }
    };

    let plan = unsafe { (*inputs.node).ss.ps.plan };
    let mut sort_order_id = unsafe { super::custom_private_sort_order_id(plan) };
    let leading_column_id = unsafe { super::custom_private_leading_column_id(plan) };
    // sort_order_id mirrors the leading order column_id in the catalog; recover
    // when private data omitted the sort-order field but still carried leading.
    if sort_order_id == 0 && leading_column_id > 0 {
        sort_order_id = i32::from(leading_column_id);
    }
    let scope_key = unsafe { super::custom_private_scope_key(plan) };
    let direction = if unsafe { super::custom_private_order_descending(plan) } {
        OrderDirection::Desc
    } else {
        OrderDirection::Asc
    };
    let (leading_column, leading_type_oid) = inputs
        .catalog
        .column_by_attnum(leading_column_id)
        .map(|column| (column.name.clone(), column.pg_type.type_oid()))
        .unwrap_or_else(|| {
            pgrx::error!(
                "{CUSTOM_PATH_NAME} ordered merge missing leading column_id {leading_column_id} in catalog"
            )
        });
    if let Some(leading) = inputs.catalog.column_by_attnum(leading_column_id) {
        let leading_ref = koldstore_common::ColumnRef::new(leading.column_id, leading.name.clone());
        if cold_stream.enable_late_materialization(&leading_ref)
            && profiler.collection_mode().collects_counts()
        {
            cold_profile.compete_columns = cold_stream.compete_projection_names();
            cold_profile.body_columns = cold_stream.body_projection_names();
        }
    }
    let cold_bound =
        cold_frontier::load_cold_best_bound(inputs.table_oid, &scope_key, sort_order_id, direction)
            .unwrap_or_else(|error| {
                pgrx::error!("{CUSTOM_PATH_NAME} cold frontier failed: {error}")
            });
    profiler.record_hot_buffer(0);
    let stream = MergeRowStream::new_ordered_progressive(
        hot,
        cold_stream,
        inputs.snapshot.mirror_relation.clone(),
        inputs.snapshot.primary_key_columns.clone(),
        OrderedProgressiveCtrl {
            direction,
            cold_bound,
            leading_column,
            leading_type_oid,
            table_oid: inputs.table_oid,
            scope_key,
            sort_order_id,
            mode: OrderedEmitMode::Streaming,
            body_hydrate_pending: false,
        },
    );
    (
        ScanEmitMode::stream(stream, inputs.projection.clone()),
        EmitPath::OrderedMergeNative,
        0,
    )
}

fn open_native_hot_cursor(inputs: &ScanSourceInputs<'_>) -> Result<NativeHotCursor, String> {
    let child = unsafe { hot_child_planstate(inputs.node) }.ok_or_else(|| {
        format!("{CUSTOM_PATH_NAME} native hot path missing initialized hot child")
    })?;
    let pk_columns = inputs
        .snapshot
        .primary_key_columns
        .iter()
        .map(|column| koldstore_common::PkColumn::new(&column.name))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    let catalog_columns = inputs.catalog.columns.clone();
    let cursor = unsafe {
        NativeHotCursor::open(
            child,
            inputs.relation_owner,
            inputs.table_oid,
            pk_columns,
            catalog_columns,
        )
    }?;
    for projected in &inputs.projection.columns {
        let attnum = projected.catalog.column_id.get();
        if !cursor.covers_attnum(attnum) {
            return Err(format!(
                "native hot cursor child targetlist omits projected column `{}`",
                projected.catalog.name
            ));
        }
    }
    Ok(cursor)
}

fn encode_leading_key(
    row: &ResolvedRow,
    leading_column: &str,
    leading_type_oid: u32,
) -> Option<Vec<u8>> {
    let sort_type = koldstore_sortkey::SortKeyType::from_type_oid(leading_type_oid)?;
    let value = row.row_image.get(leading_column)?;
    koldstore_sortkey::encode_sort_key_json(sort_type, &value.to_json()).ok()
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
    profiler.record_mirror_scan(overlay.len(), started);
    overlay
}
