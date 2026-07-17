use crate::common;

use anyhow::Result;
use std::time::{Duration, Instant};

const BACKGROUND_APPLY_DEADLINE: Duration = Duration::from_secs(5);

#[tokio::test]
async fn async_mirror_applies_only_committed_wal_in_bounded_batches() -> Result<()> {
    if !common::selected_mirror_capture_mode()?.is_async() {
        common::log_always("skipping async-only mirror lifecycle assertions in strict mode");
        return Ok(());
    }
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "async_change_log_mirror").await?;
        let table_name = format!("{}_events", db.schema);
        let relation = db.relation(&table_name);
        let mirror = format!("koldstore.{table_name}__cl");

        let publication_exists: bool = db
            .client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'koldstore_async_mirror')",
                &[],
            )
            .await?
            .get(0);
        if !publication_exists {
            // A prior async cleanup on a shared E2E database may have dropped the
            // bootstrap publication. Recreate it the same way CREATE EXTENSION does.
            db.client
                .batch_execute("CREATE PUBLICATION koldstore_async_mirror")
                .await?;
        }

        db.client
            .batch_execute(&format!(
                "CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL)"
            ))
            .await?;
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass,
                  storage => $2,
                  hot_row_limit => 1000,
                  mirror_capture_mode => 'async'
                )
                "#,
                &[&relation, &db.storage_name],
            )
            .await?;

        let trigger_rows = db
            .client
            .query(
                "SELECT tgname::text FROM pg_trigger WHERE tgrelid = $1::text::regclass AND NOT tgisinternal ORDER BY tgname",
                &[&relation],
            )
            .await?;
        let triggers = trigger_rows
            .iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<Vec<_>>();
        assert_eq!(triggers, vec![format!("{table_name}__cl_pk_update_guard"),]);
        let worker_start_latency = common::wait_for_async_worker(&db.client).await?;
        common::log_always(format!(
            "async mirror worker visible after {worker_start_latency:?}"
        ));
        let published_columns: String = db
            .client
            .query_one(
                "SELECT attnames::text FROM pg_publication_tables WHERE pubname = 'koldstore_async_mirror' AND schemaname = $1 AND tablename = $2",
                &[&db.schema, &table_name],
            )
            .await?
            .get(0);
        assert_eq!(published_columns, "{id}");

        db.client
            .execute(
                &format!(
                    "INSERT INTO {relation} SELECT id, 'body-' || id FROM generate_series(1, 10000) id"
                ),
                &[],
            )
            .await?;
        let started = Instant::now();
        common::wait_for_mirror_op_count(&db.client, &mirror, 1, 10_000).await?;
        let apply_latency = started.elapsed();
        common::log_always(format!(
            "async mirror applied 10000 committed inserts after {apply_latency:?}"
        ));
        assert!(
            apply_latency <= BACKGROUND_APPLY_DEADLINE,
            "background mirror apply exceeded {BACKGROUND_APPLY_DEADLINE:?}"
        );
        assert_eq!(
            common::mirror_op_count(&db.client, &mirror, 1).await?,
            10_000
        );
        assert_eq!(
            common::wait_for_async_mirror(&db.client).await?,
            0,
            "second fence acknowledges the applied LSN"
        );

        db.client
            .batch_execute(&format!(
                "BEGIN; UPDATE {relation} SET body = 'rolled-back' WHERE id <= 50; ROLLBACK"
            ))
            .await?;
        assert_eq!(
            common::wait_for_async_mirror(&db.client).await?,
            0,
            "aborted WAL must not be decoded"
        );

        db.client
            .execute(
                &format!("UPDATE {relation} SET body = 'updated' WHERE id <= 100"),
                &[],
            )
            .await?;
        db.client
            .execute(
                &format!("DELETE FROM {relation} WHERE id BETWEEN 101 AND 200"),
                &[],
            )
            .await?;
        common::wait_for_mirror_op_count(&db.client, &mirror, 2, 100).await?;
        common::wait_for_mirror_op_count(&db.client, &mirror, 3, 100).await?;
        assert_eq!(common::mirror_op_count(&db.client, &mirror, 2).await?, 100);
        assert_eq!(common::mirror_op_count(&db.client, &mirror, 3).await?, 100);
        assert_eq!(common::wait_for_async_mirror(&db.client).await?, 0);

        let disable_while_active = db
            .client
            .query_one("SELECT koldstore.disable_async_mirror()", &[])
            .await;
        assert!(
            disable_while_active.is_err(),
            "cleanup must reject active async tables"
        );

        db.client
            .query_one(
                "SELECT koldstore.flush_table($1::text::regclass, true)::text",
                &[&relation],
            )
            .await?;
        assert_eq!(
            common::wait_for_async_mirror(&db.client).await?,
            0,
            "flush-owned heap pruning must be excluded by replication origin"
        );

        db.client
            .query_one(
                "SELECT koldstore.unmanage_table($1::text::regclass, true, true)",
                &[&relation],
            )
            .await?;

        // Database-wide disable requires no other fixture's async tables remain
        // active on the shared E2E database.
        let other_async_active: i64 = db
            .client
            .query_one(
                "SELECT count(*)::bigint FROM koldstore.schemas \
                 WHERE active \
                   AND COALESCE(options->>'mirror_capture_mode', 'strict') = 'async'",
                &[],
            )
            .await?
            .get(0);
        if other_async_active > 0 {
            common::log_always(format!(
                "skipping disable_async_mirror cleanup assertions; {other_async_active} other async table(s) still active"
            ));
            continue;
        }

        assert!(
            db.client
                .query_one("SELECT koldstore.disable_async_mirror()", &[])
                .await?
                .get::<_, bool>(0),
            "first cleanup must remove the slot/publication"
        );
        assert!(
            !db.client
                .query_one("SELECT koldstore.disable_async_mirror()", &[])
                .await?
                .get::<_, bool>(0),
            "second cleanup must be an idempotent no-op"
        );
        let cleanup_state = db
            .client
            .query_one(
                "SELECT \
                   EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'koldstore_async_mirror'), \
                   EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = koldstore.async_mirror_slot_name())",
                &[],
            )
            .await?;
        assert!(!cleanup_state.get::<_, bool>(0));
        assert!(!cleanup_state.get::<_, bool>(1));

        let reenabled_table_name = format!("{}_reenabled_events", db.schema);
        let reenabled_relation = db.relation(&reenabled_table_name);
        db.client
            .batch_execute(&format!(
                "CREATE TABLE {reenabled_relation} (id bigint PRIMARY KEY, body text NOT NULL)"
            ))
            .await?;
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass,
                  storage => $2,
                  hot_row_limit => 1000,
                  mirror_capture_mode => 'async'
                )
                "#,
                &[&reenabled_relation, &db.storage_name],
            )
            .await?;
        common::wait_for_async_worker(&db.client).await?;
        let recreated = db
            .client
            .query_one(
                "SELECT \
                   EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'koldstore_async_mirror'), \
                   EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = koldstore.async_mirror_slot_name())",
                &[],
            )
            .await?;
        assert!(recreated.get::<_, bool>(0));
        assert!(recreated.get::<_, bool>(1));

        db.client
            .query_one(
                "SELECT koldstore.unmanage_table($1::text::regclass, true, true)",
                &[&reenabled_relation],
            )
            .await?;
        assert!(db
            .client
            .query_one("SELECT koldstore.disable_async_mirror()", &[])
            .await?
            .get::<_, bool>(0));
    }
    Ok(())
}
