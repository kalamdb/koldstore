//! Shared soak kill-switch for fatal Postgres outages.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::support::log_always;

/// Shared stop + fatal reason for soak workers / watchdog / progress loop.
#[derive(Debug, Default)]
pub struct SoakControl {
    stop: AtomicBool,
    fatal: AtomicBool,
    reason: std::sync::Mutex<Option<String>>,
}

impl SoakControl {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    #[must_use]
    pub fn should_stop(&self) -> bool {
        self.stop.load(Ordering::Relaxed) || self.fatal.load(Ordering::SeqCst)
    }

    pub fn request_stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
    }

    #[must_use]
    pub fn is_fatal(&self) -> bool {
        self.fatal.load(Ordering::SeqCst)
    }

    /// Records a fatal outage once and requests stop for all workers.
    pub fn trip_fatal(&self, reason: impl Into<String>) {
        let reason = reason.into();
        // Only the first trip wins (avoid log spam).
        if self
            .fatal
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            log_always(format!("FATAL soak abort: {reason}"));
            *self.reason.lock().expect("fatal reason") = Some(reason);
            self.stop.store(true, Ordering::SeqCst);
        }
    }

    /// Classifies a DB error; trips fatal on connection loss / admin shutdown.
    ///
    /// Returns `true` when the error is treated as fatal.
    pub fn note_db_error(&self, context: &str, err: &impl std::fmt::Display) -> bool {
        let text = err.to_string();
        let lower = text.to_ascii_lowercase();
        let fatal = lower.contains("connection closed")
            || lower.contains("connection reset")
            || lower.contains("server closed the connection")
            || lower.contains("broken pipe")
            || lower.contains("could not connect")
            || lower.contains("connection refused")
            || lower.contains("terminating connection due to administrator command")
            || lower.contains("the database system is shutting down")
            || lower.contains("the database system is starting up");
        if fatal {
            self.trip_fatal(format!(
                "{context}: Postgres connection lost ({text}). \
                 Another process may have restarted pgrx (e.g. a second \
                 scripts/run-chat-penetration.sh / cargo pgrx start)."
            ));
            true
        } else {
            // Rate-limit non-fatal spam: log at most when errors are sparse.
            log_always(format!("{context}: {text}"));
            false
        }
    }

    pub fn take_fatal_reason(&self) -> Option<String> {
        self.reason.lock().expect("fatal reason").clone()
    }
}
