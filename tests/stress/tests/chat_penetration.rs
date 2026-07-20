//! Timed chat penetration soak entrypoint (`cargo nextest -p stress`).

use anyhow::Result;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn chat_penetration_holds_under_flush_and_history_load() -> Result<()> {
    stress::scenario::run_chat_penetration().await
}
