//! Operational logging for pg-koldstore.
//!
//! Library crates stay PostgreSQL-free: they format messages with the stable
//! prefix `koldstore <component>: <message>` and emit through a process-wide
//! sink. The extension installs a PostgreSQL `elog` sink from `_PG_init`
//! ([`install_sink`]). Without a sink, messages are discarded so unit tests
//! and non-PG binaries stay quiet unless they install a capture sink.
//!
//! # Message syntax
//!
//! Match existing server logs:
//! - `koldstore flush: wrote+cataloged segment batch=…`
//! - `koldstore WAL applier db=…: …`
//! - `koldstore manage: managed table=… elapsed_ms=…`

use std::fmt;
use std::sync::RwLock;
use std::time::Instant;

/// Severity mapped onto PostgreSQL `LOG` / `WARNING` by the extension sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    /// Routine operational progress (`pgrx::log!` / `LOG`).
    Info,
    /// Recoverable faults and races operators should notice (`pgrx::warning!`).
    Warning,
}

/// Well-known components used in the `koldstore <component>:` prefix.
pub mod component {
    /// Hot-to-cold flush enqueue, executors, and reclaim.
    pub const FLUSH: &str = "flush";
    /// `manage_table` / migration activation.
    pub const MANAGE: &str = "manage";
    /// `unmanage_table` teardown.
    pub const UNMANAGE: &str = "unmanage";
    /// Persistent WAL applier / decode apply.
    pub const WAL: &str = "wal";
    /// Cluster supervisor / child registration.
    pub const SUPERVISOR: &str = "supervisor";
    /// Database maintenance worker.
    pub const MAINTENANCE: &str = "maintenance";
}

/// Sink installed by the PostgreSQL adapter (or tests).
pub type LogSink = fn(LogLevel, &str);

static SINK: RwLock<Option<LogSink>> = RwLock::new(None);

/// Installs the process-wide log sink. Safe to call from `_PG_init` and tests.
pub fn install_sink(sink: LogSink) {
    if let Ok(mut guard) = SINK.write() {
        *guard = Some(sink);
    }
}

/// Clears the sink (primarily for tests).
pub fn clear_sink() {
    if let Ok(mut guard) = SINK.write() {
        *guard = None;
    }
}

fn current_sink() -> Option<LogSink> {
    SINK.read().ok().and_then(|guard| *guard)
}

/// Formats a line in the canonical koldstore server-log syntax.
#[must_use]
pub fn format_line(component: &str, message: &str) -> String {
    format!("koldstore {component}: {message}")
}

/// Emits an informational (`LOG`) message when a sink is installed.
pub fn info(component: &str, message: impl AsRef<str>) {
    emit(LogLevel::Info, component, message.as_ref());
}

/// Emits a warning when a sink is installed.
pub fn warning(component: &str, message: impl AsRef<str>) {
    emit(LogLevel::Warning, component, message.as_ref());
}

/// Emits a pre-formatted `fmt::Arguments` info line.
pub fn info_args(component: &str, args: fmt::Arguments<'_>) {
    emit(LogLevel::Info, component, &format!("{args}"));
}

/// Emits a pre-formatted `fmt::Arguments` warning line.
pub fn warning_args(component: &str, args: fmt::Arguments<'_>) {
    emit(LogLevel::Warning, component, &format!("{args}"));
}

fn emit(level: LogLevel, component: &str, message: &str) {
    let Some(sink) = current_sink() else {
        return;
    };
    let line = format_line(component, message);
    sink(level, &line);
}

/// Times an operation and logs elapsed milliseconds on [`Self::finish`].
///
/// Prefer explicit [`Self::finish`] / [`Self::fail`] over `Drop` so aborting
/// `pgrx::error!` paths do not emit a misleading "completed" line.
#[derive(Debug)]
pub struct TimedOp {
    component: &'static str,
    label: String,
    started: Instant,
}

impl TimedOp {
    /// Starts timing `label` under `component`.
    #[must_use]
    pub fn start(component: &'static str, label: impl Into<String>) -> Self {
        let label = label.into();
        info(component, format!("starting {label}"));
        Self {
            component,
            label,
            started: Instant::now(),
        }
    }

    /// Elapsed time since [`Self::start`].
    #[must_use]
    pub fn elapsed_ms(&self) -> u128 {
        self.started.elapsed().as_millis()
    }

    /// Logs successful completion with elapsed time.
    pub fn finish(self, detail: impl AsRef<str>) {
        let detail = detail.as_ref();
        let elapsed_ms = self.elapsed_ms();
        if detail.is_empty() {
            info(
                self.component,
                format!("completed {}; elapsed_ms={elapsed_ms}", self.label),
            );
        } else {
            info(
                self.component,
                format!(
                    "completed {}; {detail}; elapsed_ms={elapsed_ms}",
                    self.label
                ),
            );
        }
    }

    /// Logs a warning completion (operation ended with a soft failure).
    pub fn fail(self, detail: impl AsRef<str>) {
        let elapsed_ms = self.elapsed_ms();
        warning(
            self.component,
            format!(
                "failed {}; {}; elapsed_ms={elapsed_ms}",
                self.label,
                detail.as_ref()
            ),
        );
    }
}

/// Convenience macros matching `koldstore <component>: …` call sites.
#[macro_export]
macro_rules! klog {
    ($component:expr, $($arg:tt)*) => {
        $crate::log::info_args($component, format_args!($($arg)*))
    };
}

/// Warning-level variant of [`klog!`].
#[macro_export]
macro_rules! kwarn {
    ($component:expr, $($arg:tt)*) => {
        $crate::log::warning_args($component, format_args!($($arg)*))
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, MutexGuard};

    static CAPTURE: Mutex<Vec<(LogLevel, String)>> = Mutex::new(Vec::new());
    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn capture_sink(level: LogLevel, line: &str) {
        CAPTURE
            .lock()
            .expect("capture lock")
            .push((level, line.to_string()));
    }

    fn take_capture() -> Vec<(LogLevel, String)> {
        std::mem::take(&mut *CAPTURE.lock().expect("capture lock"))
    }

    fn lock_tests() -> MutexGuard<'static, ()> {
        TEST_LOCK.lock().expect("test lock")
    }

    #[test]
    fn format_line_matches_server_prefix() {
        assert_eq!(
            format_line("flush", "enqueue retry attempt=2"),
            "koldstore flush: enqueue retry attempt=2"
        );
    }

    #[test]
    fn timed_op_logs_start_and_finish() {
        let _guard = lock_tests();
        install_sink(capture_sink);
        take_capture();
        let op = TimedOp::start(component::MANAGE, "table=public.items");
        op.finish("job=00000000-0000-0000-0000-000000000001");
        let lines = take_capture();
        clear_sink();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0].0, LogLevel::Info);
        assert!(lines[0]
            .1
            .starts_with("koldstore manage: starting table=public.items"));
        assert!(lines[1]
            .1
            .contains("koldstore manage: completed table=public.items"));
        assert!(lines[1].1.contains("elapsed_ms="));
        assert!(lines[1]
            .1
            .contains("job=00000000-0000-0000-0000-000000000001"));
    }

    #[test]
    fn without_sink_messages_are_dropped() {
        let _guard = lock_tests();
        clear_sink();
        take_capture();
        info(component::FLUSH, "should not appear");
        assert!(take_capture().is_empty());
    }
}
