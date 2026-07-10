//! Cold-read profiling and EXPLAIN rendering for KoldMergeScan.

use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::CString;
use std::time::Instant;

use koldstore_parquet::{BloomPruneMode, ParquetReadProfile};
use pgrx::pg_sys;

const EXPLAIN_PROFILE_LIMIT: usize = 64;

/// Scan metadata retained across EndCustomScan for EXPLAIN ANALYZE rendering.
#[derive(Debug, Clone)]
pub(super) struct ExplainScanMeta {
    pub(super) cold_profile: ColdReadProfile,
    pub(super) hot_plan_label: String,
    pub(super) mirror_tombstones: usize,
    pub(super) mirror_live_overrides: usize,
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
    /// Always catalog (`koldstore.manifest` + `cold_segments`), never object-store JSON.
    pub(super) manifest_source: &'static str,
    pub(super) storage_type: String,
    pub(super) base_path: String,
    pub(super) manifest_read_ms: Option<f64>,
    /// Segments considered before catalog min/max prune.
    pub(super) segments_considered: usize,
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
            manifest_source: "catalog",
            storage_type: String::new(),
            base_path: String::new(),
            manifest_read_ms: None,
            segments_considered: 0,
            segments_pruned_min_max: 0,
            segments_opened: 0,
            pk_probe: None,
            projected_columns: Vec::new(),
            segments: vec![],
        }
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

pub(super) fn explain_cold_read_profile(es: *mut pg_sys::ExplainState, profile: &ColdReadProfile) {
    let executed = profile.manifest_read_ms.is_some()
        && profile
            .segments
            .iter()
            .all(|segment| segment.read_ms.is_some());

    let manifest_value = if executed {
        format!(
            "{}, source={}, {:.3} ms",
            profile.manifest_path,
            profile.manifest_source,
            profile.manifest_read_ms.unwrap_or(0.0)
        )
    } else {
        format!(
            "{}, source={} (planned)",
            profile.manifest_path, profile.manifest_source
        )
    };
    explain_property(es, "Manifest", &manifest_value);

    if !profile.storage_type.is_empty() || !profile.base_path.is_empty() {
        explain_property(
            es,
            "Cold storage",
            &format!(
                "type={}, base={}",
                if profile.storage_type.is_empty() {
                    "unknown"
                } else {
                    &profile.storage_type
                },
                if profile.base_path.is_empty() {
                    "(none)"
                } else {
                    &profile.base_path
                }
            ),
        );
    }

    explain_property(
        es,
        "Cold segments",
        &format!(
            "considered={}, pruned_min_max={}, pruned_bloom={}, opened={}",
            profile.segments_considered,
            profile.segments_pruned_min_max,
            profile.segments_pruned_by_bloom(),
            profile.segments_opened
        ),
    );

    let (row_groups_total, row_groups_selected, row_groups_skipped, bloom_filters_fetched) =
        profile.row_group_totals();
    if row_groups_total > 0 || bloom_filters_fetched > 0 {
        explain_property(
            es,
            "Cold row groups",
            &format!(
                "total={row_groups_total}, selected={row_groups_selected}, skipped={row_groups_skipped}, bloom_filters_fetched={bloom_filters_fetched}"
            ),
        );
    }

    if let Some((column, values)) = &profile.pk_probe {
        explain_property(
            es,
            "PK probe",
            &format!("{column} IN ({})", values.join(", ")),
        );
    }

    if !profile.projected_columns.is_empty() {
        explain_property(es, "Cold projection", &profile.projected_columns.join(", "));
    }

    if profile.segments.is_empty() {
        explain_property(es, "Parquet segment", "none");
        return;
    }

    for segment in &profile.segments {
        explain_property(
            es,
            "Parquet segment",
            &format_segment_line(segment, executed),
        );
        if let Some(parquet) = &segment.parquet {
            explain_property(es, "  Parquet I/O", &format_parquet_io(parquet));
            explain_property(es, "  Row groups", &format_row_groups(parquet));
            explain_property(es, "  Bloom", &format_bloom(parquet));
        }
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
}

fn format_segment_line(segment: &SegmentReadProfile, executed: bool) -> String {
    let mut parts = vec![segment.object_path.clone()];
    if let Some(size) = segment
        .byte_size
        .or_else(|| segment.parquet.as_ref().and_then(|p| p.file_size))
    {
        parts.push(format!("{size} bytes"));
    }
    if executed {
        if let Some(read_ms) = segment.read_ms {
            parts.push(format!("{} rows", segment.row_count));
            parts.push(format!("{read_ms:.3} ms"));
        }
    } else {
        parts.push("(planned)".to_string());
    }
    parts.join(", ")
}

fn format_parquet_io(parquet: &ParquetReadProfile) -> String {
    parquet.format_io_summary()
}

fn format_row_groups(parquet: &ParquetReadProfile) -> String {
    parquet.format_row_groups_summary()
}

fn format_bloom(parquet: &ParquetReadProfile) -> String {
    parquet.format_bloom_summary()
}

pub(super) fn explain_property(es: *mut pg_sys::ExplainState, label: &str, value: &str) {
    let label = CString::new(label).unwrap_or_default();
    let value = CString::new(value).unwrap_or_default();
    unsafe {
        pg_sys::ExplainPropertyText(label.as_ptr(), value.as_ptr(), es);
    }
}
