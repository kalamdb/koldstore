//! Timed chat penetration soak entrypoint (`cargo nextest -p stress`).
//!
//! Requires `KOLDSTORE_STRESS_RUN=1` (set by `scripts/run-chat-penetration.sh`)
//! so accidental `cargo nextest --workspace` does not hit a dead pgrx port.

use anyhow::Result;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn chat_penetration_holds_under_flush_and_history_load() -> Result<()> {
    if std::env::var_os("KOLDSTORE_STRESS_RUN").is_none() {
        eprintln!(
            "skipping chat penetration: set KOLDSTORE_STRESS_RUN=1 \
             (use scripts/run-chat-penetration.sh)"
        );
        return Ok(());
    }
    stress::scenario::run_chat_penetration().await
}
