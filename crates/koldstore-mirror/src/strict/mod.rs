//! Strict mirror capture (statement triggers + transition tables).
//!
//! Plans the `FOR EACH STATEMENT` capture function and triggers that keep the
//! `__cl` mirror up to date under `mirror_capture_mode = strict`. Async capture
//! does not use these triggers.

pub mod capture;

pub use capture::{
    async_worker_kick_trigger_name, async_worker_kick_trigger_names, plan_drop_mirror_dml_triggers,
    plan_mirror_capture, plan_mirror_capture_teardown, MirrorCaptureError, MirrorCapturePlan,
    MirrorCaptureResult,
};
