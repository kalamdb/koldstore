//! Strict mirror DML capture planning.
//!
//! Implementation lives in [`koldstore_mirror::strict`]. This module re-exports
//! that API so existing `koldstore_migrate::capture` callers keep compiling.

pub use koldstore_mirror::strict::{
    async_worker_kick_trigger_name, async_worker_kick_trigger_names, plan_drop_mirror_dml_triggers,
    plan_mirror_capture, plan_mirror_capture_teardown, MirrorCaptureError, MirrorCapturePlan,
    MirrorCaptureResult,
};
