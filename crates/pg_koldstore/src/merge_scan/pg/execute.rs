//! Runtime source selection and row materialization for KoldMergeScan.
//!
//! This module owns the PostgreSQL-facing hot/cold/mirror execution flow.
//! Pure winner resolution remains in `koldstore-merge`; SPI, plan-state, and
//! PostgreSQL memory-context work must remain in the extension crate.

use koldstore_common::ColdRow;
use koldstore_migrate::{order::CatalogColumn, ExistingTableCatalog};
use pgrx::pg_sys;

use super::cold::load_cold_rows_for_merge;
use super::emit::materialize_merged_rows;
use super::hot::{load_hot_rows_for_merge, load_hot_rows_native, HotEqualityFilter};
use super::mirror::{filter_cold_rows_with_overlay, load_mirror_tombstone_overlay, MirrorOverlay};
use super::profile::{
    ColdReadProfile, DisabledScanProfiler, EmitPath, ScanProfileSink, ScanProfiler,
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
    pub(super) projection: &'a ScanProjection<'a>,
    pub(super) image_columns: &'a [&'a CatalogColumn],
    pub(super) pk_equality: &'a [HotEqualityFilter],
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

    let (mut cold_profile, cold_rows) = load_cold_rows(&inputs, profiler);
    let has_no_cold_source = cold_rows.is_empty() && cold_profile.segments.is_empty();
    if has_no_cold_source {
        initialize_custom_plan_children(inputs.node, inputs.estate, inputs.eflags);
    }

    let (mode, emit_path, hot_rows) =
        if has_no_cold_source && hot_child_planstate(inputs.node).is_some() {
            (ScanEmitMode::HotChild, EmitPath::HotChild, 0)
        } else if has_no_cold_source {
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
        } else if inputs.pk_point_lookup {
            emit_cold_point_result(cold_rows, &inputs, &mut memory, profiler)
        } else {
            emit_merged_result(cold_rows, &inputs, &mut memory, profiler)
        };

    cold_profile.segments_opened = cold_profile.segments.len();
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
fn load_cold_rows<P: ScanProfileSink>(
    inputs: &ScanSourceInputs<'_>,
    profiler: &mut P,
) -> (ColdReadProfile, Vec<ColdRow>) {
    let (profile, cold_rows) = load_cold_rows_for_merge(
        inputs.table_oid,
        inputs.scanrelid,
        inputs.snapshot,
        inputs.catalog,
        inputs.qual,
        inputs.image_columns,
        inputs.params,
    )
    .unwrap_or_else(|error| pgrx::error!("{CUSTOM_PATH_NAME} cold read failed: {error}"));
    profiler.record_cold_rows(cold_rows.len());

    let overlay = if cold_rows.is_empty() {
        profiler.record_mirror_scan(0, None);
        MirrorOverlay::default()
    } else {
        let started = profiler.start_timer();
        let overlay = load_mirror_tombstone_overlay(
            &inputs.snapshot.mirror_relation,
            &inputs.snapshot.primary_key_columns,
            inputs.pk_equality,
        )
        .unwrap_or_else(|error| pgrx::error!("{CUSTOM_PATH_NAME} mirror overlay failed: {error}"));
        profiler.record_mirror_scan(overlay.tombstones, started);
        overlay
    };

    let started = profiler.start_timer();
    let input_rows = cold_rows.len();
    let cold_rows = filter_cold_rows_with_overlay(cold_rows, &overlay);
    profiler.record_overlay(input_rows, cold_rows.len(), started);
    (profile, cold_rows)
}

#[inline(always)]
fn emit_cold_point_result<P: ScanProfileSink>(
    cold_rows: Vec<ColdRow>,
    inputs: &ScanSourceInputs<'_>,
    memory: &mut ScanMemory,
    profiler: &mut P,
) -> (ScanEmitMode, EmitPath, usize) {
    let started = profiler.start_timer();
    let merged =
        koldstore_merge::scan::execute_merge_scan(Vec::new(), cold_rows).unwrap_or_else(|error| {
            pgrx::error!("{CUSTOM_PATH_NAME} cold-native merge failed: {error}")
        });
    profiler.record_merge(&merged, started);

    let started = profiler.start_timer();
    let rows = unsafe { materialize_merged_rows(&merged, inputs.projection, memory) }
        .unwrap_or_else(|error| {
            pgrx::error!("{CUSTOM_PATH_NAME} cold-native emit failed: {error}")
        });
    profiler.record_materialization(started);
    (
        ScanEmitMode::buffer(rows, inputs.projection),
        EmitPath::ColdNative,
        0,
    )
}

#[inline(always)]
fn emit_merged_result<P: ScanProfileSink>(
    cold_rows: Vec<ColdRow>,
    inputs: &ScanSourceInputs<'_>,
    memory: &mut ScanMemory,
    profiler: &mut P,
) -> (ScanEmitMode, EmitPath, usize) {
    let started = profiler.start_timer();
    let hot_rows =
        crate::catalog::owner::with_relation_owner_for_merge(inputs.relation_owner, || {
            load_hot_rows_for_merge(
                inputs.relation,
                inputs.snapshot,
                inputs.pk_equality,
                inputs.image_columns,
            )
        })
        .unwrap_or_else(|error| pgrx::error!("{CUSTOM_PATH_NAME} hot read failed: {error}"));
    profiler.record_hot_scan(started);
    let hot_row_count = hot_rows.len();

    let started = profiler.start_timer();
    let merged = koldstore_merge::scan::execute_merge_scan(hot_rows, cold_rows)
        .unwrap_or_else(|error| pgrx::error!("{CUSTOM_PATH_NAME} merge failed: {error}"));
    profiler.record_merge(&merged, started);

    let started = profiler.start_timer();
    let rows = unsafe { materialize_merged_rows(&merged, inputs.projection, memory) }
        .unwrap_or_else(|error| pgrx::error!("{CUSTOM_PATH_NAME} emit failed: {error}"));
    profiler.record_materialization(started);
    (
        ScanEmitMode::buffer(rows, inputs.projection),
        EmitPath::MergeBuffer,
        hot_row_count,
    )
}
