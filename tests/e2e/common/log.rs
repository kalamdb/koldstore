//! Opt-in and always-on progress logging for E2E tests.

use std::time::Instant;

/// Returns true when `KOLDSTORE_E2E_VERBOSE` is set to a truthy value.
#[must_use]
pub fn verbose_enabled() -> bool {
    std::env::var("KOLDSTORE_E2E_VERBOSE")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes"))
        .unwrap_or(false)
}

fn should_log(always: bool) -> bool {
    always || verbose_enabled()
}

/// Logs a message when verbose mode is enabled.
pub fn log(message: impl AsRef<str>) {
    if verbose_enabled() {
        eprintln!("[e2e] {}", message.as_ref());
    }
}

/// Always logs a message. Useful for high-signal scenario tests whose output
/// nextest is configured to show on success.
pub fn log_always(message: impl AsRef<str>) {
    eprintln!("[e2e] {}", message.as_ref());
}

/// Logs the start of a step and its elapsed time when the guard is dropped.
#[must_use]
pub fn log_step(step: impl Into<String>) -> StepGuard {
    log_step_inner(step, false)
}

/// Like [`log_step`], but always emits output.
#[must_use]
pub fn log_step_always(step: impl Into<String>) -> StepGuard {
    log_step_inner(step, true)
}

fn log_step_inner(step: impl Into<String>, always: bool) -> StepGuard {
    let step = step.into();
    if should_log(always) {
        eprintln!("[e2e] {step} ...");
    }
    StepGuard {
        step,
        started: Instant::now(),
        always,
    }
}

/// Marks one E2E step and logs duration on drop when enabled.
#[derive(Debug)]
pub struct StepGuard {
    step: String,
    started: Instant,
    always: bool,
}

impl Drop for StepGuard {
    fn drop(&mut self) {
        if should_log(self.always) {
            eprintln!(
                "[e2e] {} finished in {:.3}s",
                self.step,
                self.started.elapsed().as_secs_f64()
            );
        }
    }
}
