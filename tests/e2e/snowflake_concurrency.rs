#[path = "common/mod.rs"]
mod common;

use std::collections::HashSet;

use anyhow::Result;

#[tokio::test]
async fn snowflake_ids_are_unique_across_concurrent_backends() -> Result<()> {
    let target = common::local_pg_matrix()
        .into_iter()
        .next()
        .expect("local matrix should contain at least one target");
    let setup = common::wait_for_postgres(&target).await?;
    setup
        .batch_execute("CREATE EXTENSION IF NOT EXISTS koldstore;")
        .await?;

    let clients = std::env::var("KOLDSTORE_E2E_SNOWFLAKE_CLIENTS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(8);
    let ids_per_client: i32 = std::env::var("KOLDSTORE_E2E_SNOWFLAKE_IDS_PER_CLIENT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1_000);

    let mut tasks = Vec::with_capacity(clients);
    for _ in 0..clients {
        let target = target.clone();
        tasks.push(tokio::spawn(async move {
            let client = common::connect(&target).await?;
            let rows = client
                .query(
                    "SELECT snowflake_id() FROM generate_series(1, $1)",
                    &[&ids_per_client],
                )
                .await?;
            Ok::<Vec<i64>, anyhow::Error>(rows.into_iter().map(|row| row.get(0)).collect())
        }));
    }

    let mut ids = HashSet::with_capacity(clients * ids_per_client as usize);
    for task in tasks {
        for id in task.await?? {
            anyhow::ensure!(id > 0, "snowflake id must be positive");
            anyhow::ensure!(ids.insert(id), "duplicate snowflake id generated: {id}");
        }
    }

    assert_eq!(ids.len(), clients * ids_per_client as usize);
    Ok(())
}
