//! Cold-read profiling and EXPLAIN rendering for KoldMergeScan.

use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::CString;
use std::time::Instant;

use pgrx::pg_sys;

const EXPLAIN_PROFILE_LIMIT: usize = 64;

thread_local! {
    static EXPLAIN_PROFILES: RefCell<HashMap<usize, ColdReadProfile>> = RefCell::new(HashMap::new());
}

#[derive(Debug, Clone)]
pub(super) struct SegmentReadProfile {
    pub(super) object_path: String,
    pub(super) row_count: usize,
    pub(super) read_ms: Option<f64>,
}

#[derive(Debug, Clone)]
pub(super) struct ColdReadProfile {
    pub(super) manifest_path: String,
    pub(super) manifest_read_ms: Option<f64>,
    pub(super) segments: Vec<SegmentReadProfile>,
}

pub(super) fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

pub(super) fn remember_explain_profile(node_key: usize, profile: ColdReadProfile) {
    EXPLAIN_PROFILES.with(|profiles| {
        let mut profiles = profiles.borrow_mut();
        profiles.insert(node_key, profile);
        if profiles.len() <= EXPLAIN_PROFILE_LIMIT {
            return;
        }
        if let Some(evicted) = profiles.keys().copied().find(|key| *key != node_key) {
            profiles.remove(&evicted);
        }
    });
}

pub(super) fn saved_explain_profile(node_key: usize) -> Option<ColdReadProfile> {
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
            "{}, {:.3} ms",
            profile.manifest_path,
            profile.manifest_read_ms.unwrap_or(0.0)
        )
    } else {
        format!("{} (planned)", profile.manifest_path)
    };
    explain_property(es, "Manifest", &manifest_value);

    if profile.segments.is_empty() {
        explain_property(es, "Parquet segment", "none");
        return;
    }

    for segment in &profile.segments {
        let value = if let Some(read_ms) = segment.read_ms {
            format!(
                "{}, {} rows, {:.3} ms",
                segment.object_path, segment.row_count, read_ms
            )
        } else {
            format!("{} (planned)", segment.object_path)
        };
        explain_property(es, "Parquet segment", &value);
    }
}

pub(super) fn explain_property(es: *mut pg_sys::ExplainState, label: &str, value: &str) {
    let label = CString::new(label).unwrap_or_default();
    let value = CString::new(value).unwrap_or_default();
    unsafe {
        pg_sys::ExplainPropertyText(label.as_ptr(), value.as_ptr(), es);
    }
}
