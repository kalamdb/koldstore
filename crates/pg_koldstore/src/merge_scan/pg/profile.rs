//! Cold-read profiling and EXPLAIN rendering for KoldMergeScan.
//!
//! Uses PostgreSQL's public Explain APIs (`ExplainPropertyText/Integer/UInteger/
//! Float/Bool/List`, `ExplainOpenGroup` / `ExplainCloseGroup`) so TEXT / JSON /
//! YAML / XML stay consistent with native plan nodes. TEXT section headers
//! mirror JIT (`Label:\n` + indent) because `ExplainOpenGroup` is a no-op for
//! TEXT. Graph clients that parse structured formats get nested Timing and
//! Parquet Segments groups.

use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::CString;
use std::time::Instant;

use koldstore_parquet::{BloomPruneMode, ParquetReadProfile};
use pgrx::pg_sys;

const EXPLAIN_PROFILE_LIMIT: usize = 64;

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
    /// Hot+cold overlap via JSON merge buffer.
    MergeBuffer,
}

impl EmitPath {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::HotChild => "hot_child",
            Self::HotNative => "hot_native",
            Self::ColdNative => "cold_native",
            Self::MergeBuffer => "merge_buffer",
        }
    }
}

/// Scan metadata retained across EndCustomScan for EXPLAIN ANALYZE rendering.
#[derive(Debug, Clone)]
pub(super) struct ExplainScanMeta {
    pub(super) cold_profile: ColdReadProfile,
    pub(super) hot_plan_label: String,
    pub(super) mirror_tombstones: usize,
    pub(super) mirror_live_overrides: usize,
    pub(super) emit_path: EmitPath,
    pub(super) hot_rows: usize,
    pub(super) result_rows: usize,
    pub(super) hot_probe_ms: Option<f64>,
}

thread_local! {
    static EXPLAIN_PROFILES: RefCell<HashMap<usize, ExplainScanMeta>> = RefCell::new(HashMap::new());
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
    pub(super) manifest_path: String,
    pub(super) storage_type: String,
    pub(super) base_path: String,
    /// Catalog SPI time for `koldstore.cold_segments` listing (not object-store JSON).
    pub(super) manifest_read_ms: Option<f64>,
    /// Segments considered before any prune (catalog candidates).
    pub(super) segments_considered: usize,
    /// Segments rejected because they do not match the scan scope.
    pub(super) segments_pruned_scope: usize,
    /// Segments rejected by normalized catalog min/max statistics.
    pub(super) segments_pruned_min_max: usize,
    /// Segments opened after catalog prune.
    pub(super) segments_opened: usize,
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
            segments_pruned_scope: 0,
            segments_pruned_min_max: 0,
            segments_opened: 0,
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

pub(super) fn remember_explain_profile(node_key: usize, meta: ExplainScanMeta) {
    EXPLAIN_PROFILES.with(|profiles| {
        let mut profiles = profiles.borrow_mut();
        profiles.insert(node_key, meta);
        if profiles.len() <= EXPLAIN_PROFILE_LIMIT {
            return;
        }
        if let Some(evicted) = profiles.keys().copied().find(|key| *key != node_key) {
            profiles.remove(&evicted);
        }
    });
}

pub(super) fn saved_explain_profile(node_key: usize) -> Option<ExplainScanMeta> {
    EXPLAIN_PROFILES.with(|profiles| profiles.borrow().get(&node_key).cloned())
}

pub(super) fn forget_explain_profile(node_key: usize) {
    EXPLAIN_PROFILES.with(|profiles| {
        profiles.borrow_mut().remove(&node_key);
    });
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

/// Renders KoldMergeScan cold-path diagnostics using native Explain property APIs.
pub(super) fn explain_cold_read_profile(
    es: *mut pg_sys::ExplainState,
    profile: &ColdReadProfile,
    emit_path: EmitPath,
    hot_rows: usize,
    result_rows: usize,
    hot_probe_ms: Option<f64>,
) {
    let show_timing = explain_wants_timing(es);
    let executed = profile.manifest_read_ms.is_some();

    explain_text(es, "Emit Path", emit_path.as_str());
    explain_integer(es, "Hot Rows", None, hot_rows as i64);
    if executed {
        explain_integer(es, "Result Rows", None, result_rows as i64);
    }

    explain_integer(
        es,
        "Candidate Segments",
        None,
        profile.segments_considered as i64,
    );
    explain_integer(
        es,
        "Segments Pruned by Scope",
        None,
        profile.segments_pruned_scope as i64,
    );
    explain_integer(
        es,
        "Segments Pruned by Min/Max",
        None,
        profile.segments_pruned_min_max as i64,
    );
    explain_integer(
        es,
        "Parquet Segments Opened",
        None,
        profile.segments_opened as i64,
    );
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

    if executed || row_groups_total > 0 || bytes_fetched > 0 {
        explain_integer(es, "Row Groups Read", None, row_groups_selected as i64);
        if row_groups_total > 0 {
            explain_integer(es, "Row Groups Total", None, row_groups_total as i64);
            explain_integer(es, "Row Groups Skipped", None, row_groups_skipped as i64);
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

    // Catalog listing — never opens object-store manifest.json.
    explain_text(es, "Segment Catalog Path", &profile.manifest_path);
    explain_text(
        es,
        "Segment Catalog Source",
        "postgres (koldstore.cold_segments)",
    );

    explain_timing_group(es, show_timing, profile, hot_probe_ms);

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
        explain_segment(es, segment, show_timing, executed);
        explain_close_group(es, "Parquet Segment", None, true);
    }
    explain_close_group(es, "Parquet Segments", Some("Parquet Segments"), false);
}

/// Timing subgroup matching native JIT / Gather style (`Timing: { … }`).
fn explain_timing_group(
    es: *mut pg_sys::ExplainState,
    show_timing: bool,
    profile: &ColdReadProfile,
    hot_probe_ms: Option<f64>,
) {
    if !show_timing {
        return;
    }
    let catalog_ms = profile.manifest_read_ms;
    let cold_ms = profile.cold_read_ms();
    if catalog_ms.is_none() && hot_probe_ms.is_none() && cold_ms.is_none() {
        return;
    }

    explain_open_group(es, "Timing", Some("Timing"), true);
    if let Some(ms) = catalog_ms {
        explain_float(es, "Segment Catalog Time", "ms", ms, 3);
    }
    if let Some(ms) = hot_probe_ms {
        explain_float(es, "Hot Probe Time", "ms", ms, 3);
    }
    if let Some(ms) = cold_ms {
        explain_float(es, "Cold Read Time", "ms", ms, 3);
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

fn explain_wants_timing(es: *mut pg_sys::ExplainState) -> bool {
    if es.is_null() {
        return false;
    }
    unsafe { (*es).analyze || (*es).timing }
}

pub(super) fn explain_property(es: *mut pg_sys::ExplainState, label: &str, value: &str) {
    explain_text(es, label, value);
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

fn explain_text(es: *mut pg_sys::ExplainState, label: &str, value: &str) {
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
    use super::format_bytes_human;

    #[test]
    fn format_bytes_human_uses_readable_units() {
        assert_eq!(format_bytes_human(512), "512 bytes");
        assert_eq!(format_bytes_human(2048), "2.0 kB");
        assert_eq!(format_bytes_human(1_889_000), "1.8 MB");
        assert_eq!(format_bytes_human(3_221_225_472), "3.0 GB");
    }
}
