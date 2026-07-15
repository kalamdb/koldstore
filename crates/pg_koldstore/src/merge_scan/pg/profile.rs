//! Cold-read profiling and EXPLAIN rendering for KoldMergeScan.

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
            manifest_source: "catalog",
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
#[must_use]
pub(super) fn format_bytes_human(bytes: u64) -> String {
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

pub(super) fn explain_cold_read_profile(
    es: *mut pg_sys::ExplainState,
    profile: &ColdReadProfile,
    emit_path: EmitPath,
    hot_rows: usize,
    result_rows: usize,
) {
    let executed = profile.manifest_read_ms.is_some()
        && profile
            .segments
            .iter()
            .all(|segment| segment.read_ms.is_some());

    // Timescale-style prune summary first — easy to scan while tuning.
    explain_property(es, "Emit path", emit_path.as_str());
    explain_property(es, "Hot rows", &hot_rows.to_string());
    if executed {
        explain_property(es, "Result rows", &result_rows.to_string());
    }
    explain_property(
        es,
        "Candidate segments",
        &profile.segments_considered.to_string(),
    );
    explain_property(
        es,
        "Segments pruned by scope",
        &profile.segments_pruned_scope.to_string(),
    );
    explain_property(
        es,
        "Segments pruned by min/max",
        &profile.segments_pruned_min_max.to_string(),
    );
    explain_property(
        es,
        "Parquet segments opened",
        &profile.segments_opened.to_string(),
    );

    let (row_groups_total, row_groups_selected, row_groups_skipped, bloom_filters_fetched) =
        profile.row_group_totals();
    let bytes_fetched = profile.bytes_fetched();
    let footer_cache_hits = profile.footer_cache_hits();

    if executed || row_groups_total > 0 || bytes_fetched > 0 {
        explain_property(
            es,
            "Row groups read",
            &row_groups_selected.to_string(),
        );
        if row_groups_total > 0 {
            explain_property(
                es,
                "Row groups skipped",
                &format!("{row_groups_skipped} of {row_groups_total}"),
            );
        }
        explain_property(es, "Bytes fetched", &format_bytes_human(bytes_fetched));
        if footer_cache_hits > 0 {
            explain_property(es, "Footer cache hits", &footer_cache_hits.to_string());
        }
    }

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
            "considered={}, pruned_scope={}, pruned_min_max={}, pruned_bloom={}, opened={}",
            profile.segments_considered,
            profile.segments_pruned_scope,
            profile.segments_pruned_min_max,
            profile.segments_pruned_by_bloom(),
            profile.segments_opened
        ),
    );

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
