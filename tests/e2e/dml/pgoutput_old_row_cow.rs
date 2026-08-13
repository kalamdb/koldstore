//! Protocol probe: `REPLICA IDENTITY FULL` + pgoutput old tuples as CoW preimages.
//!
//! This is not the production capture path. KoldStore publishes PK columns only
//! and the WAL applier ignores `Update.old`. The test uses a dedicated
//! publication/slot and KoldStore's existing decoder to ask whether PostgreSQL
//! will emit a complete OLD row (including an unchanged toasted payload) when
//! identity is FULL.

use anyhow::{Context, Result};
use koldstore_wal_mirror::{decode_message, PgOutputMessage, PgOutputTuple, PgOutputValue};
use tokio_postgres::Client;

use crate::common;

fn pseudo_random_text(len: usize, mut state: u64) -> String {
    let mut output = String::with_capacity(len);
    for _ in 0..len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        output.push((b'!' + (state % 94) as u8) as char);
    }
    output
}

fn text(value: &PgOutputValue) -> Option<&str> {
    match value {
        PgOutputValue::Text(value) => Some(value.as_str()),
        PgOutputValue::Binary(bytes) => std::str::from_utf8(bytes).ok(),
        PgOutputValue::Null | PgOutputValue::UnchangedToast => None,
    }
}

fn assert_text_eq(value: &PgOutputValue, expected: &str, label: &str) -> Result<()> {
    let actual = text(value).with_context(|| format!("{label} was not materialized: {value:?}"))?;
    anyhow::ensure!(actual == expected, "{label} mismatch");
    Ok(())
}

async fn consume_pgoutput(
    client: &Client,
    slot: &str,
    publication: &str,
) -> Result<Vec<PgOutputMessage>> {
    let rows = client
        .query(
            r#"
            SELECT data
            FROM pg_catalog.pg_logical_slot_get_binary_changes(
                $1,
                NULL,
                NULL,
                'proto_version', '1',
                'publication_names', $2,
                'messages', 'false'
            )
            "#,
            &[&slot, &publication],
        )
        .await?;

    rows.into_iter()
        .map(|row| {
            let data: Vec<u8> = row.get(0);
            decode_message(&data).map_err(anyhow::Error::from)
        })
        .collect()
}

fn update_message(messages: &[PgOutputMessage]) -> Result<(&PgOutputTuple, &PgOutputTuple)> {
    let mut found = None;
    for message in messages {
        if let PgOutputMessage::Update { old, new, .. } = message {
            anyhow::ensure!(found.is_none(), "expected one UPDATE, saw more than one");
            let old = old
                .as_ref()
                .context("REPLICA IDENTITY FULL UPDATE did not contain OLD tuple")?;
            found = Some((old, new));
        }
    }
    found.context("expected one UPDATE pgoutput message")
}

fn delete_message(messages: &[PgOutputMessage]) -> Result<&PgOutputTuple> {
    messages
        .iter()
        .find_map(|message| match message {
            PgOutputMessage::Delete { old, .. } => Some(old),
            _ => None,
        })
        .context("expected DELETE pgoutput message")
}

fn row_change_count(messages: &[PgOutputMessage]) -> usize {
    messages
        .iter()
        .filter(|message| {
            matches!(
                message,
                PgOutputMessage::Insert { .. }
                    | PgOutputMessage::Update { .. }
                    | PgOutputMessage::Delete { .. }
            )
        })
        .count()
}

#[tokio::test]
async fn replica_identity_full_old_rows_are_viable_copy_on_write_preimages() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "pgoutput_cow_old_row").await?;
        let table_name = format!("{}_cow_probe", db.schema);
        let relation = db.relation(&table_name);
        let publication = format!("{}_cow_pub", db.schema);
        let slot = format!("{}_cow_slot", db.schema);

        let result = async {
            db.client
                .batch_execute(&format!(
                    r#"
                    CREATE TABLE {relation} (
                        id bigint PRIMARY KEY,
                        body text NOT NULL,
                        payload text NOT NULL
                    );
                    ALTER TABLE {relation} REPLICA IDENTITY FULL;
                    CREATE PUBLICATION {publication}
                        FOR TABLE {relation}
                        WITH (publish = 'insert, update, delete');
                    "#
                ))
                .await?;

            db.client
                .query_one(
                    "SELECT slot_name FROM pg_catalog.pg_create_logical_replication_slot($1, 'pgoutput')",
                    &[&slot],
                )
                .await?;

            let replica_identity: String = db
                .client
                .query_one(
                    "SELECT relreplident::text FROM pg_catalog.pg_class WHERE oid = $1::text::regclass",
                    &[&relation],
                )
                .await?
                .get(0);
            anyhow::ensure!(replica_identity == "f", "expected replica identity FULL");

            // Deterministic, mostly incompressible text large enough to exercise TOAST.
            let payload_a = pseudo_random_text(128 * 1024, 0x1234_5678_9abc_def0);
            let payload_b = pseudo_random_text(128 * 1024, 0xfedc_ba98_7654_3210);

            db.client
                .execute(
                    &format!("INSERT INTO {relation} (id, body, payload) VALUES (1, $1, $2)"),
                    &[&"base", &payload_a],
                )
                .await?;
            let _ = consume_pgoutput(&db.client, &slot, &publication).await?;

            // Critical CoW case: update a small column while a large toasted column is untouched.
            db.client
                .execute(
                    &format!("UPDATE {relation} SET body = $1 WHERE id = 1"),
                    &[&"body-updated"],
                )
                .await?;
            let messages = consume_pgoutput(&db.client, &slot, &publication).await?;
            let (old, new) = update_message(&messages)?;
            anyhow::ensure!(old.values.len() == 3, "OLD tuple must contain all published columns");
            anyhow::ensure!(new.values.len() == 3, "NEW tuple must contain all published columns");
            assert_text_eq(&old.values[0], "1", "OLD id")?;
            assert_text_eq(&old.values[1], "base", "OLD body")?;
            assert_text_eq(&old.values[2], &payload_a, "OLD toasted payload")?;
            assert_text_eq(&new.values[0], "1", "NEW id")?;
            assert_text_eq(&new.values[1], "body-updated", "NEW body")?;
            anyhow::ensure!(
                matches!(new.values[2], PgOutputValue::UnchangedToast)
                    || text(&new.values[2]) == Some(payload_a.as_str()),
                "NEW unchanged toasted payload must be either materialized or marked UnchangedToast; got {:?}",
                new.values[2]
            );

            // Updating the toasted value itself must expose both the previous and new full values.
            db.client
                .execute(
                    &format!("UPDATE {relation} SET payload = $1 WHERE id = 1"),
                    &[&payload_b],
                )
                .await?;
            let messages = consume_pgoutput(&db.client, &slot, &publication).await?;
            let (old, new) = update_message(&messages)?;
            assert_text_eq(&old.values[1], "body-updated", "second OLD body")?;
            assert_text_eq(&old.values[2], &payload_a, "second OLD toasted payload")?;
            assert_text_eq(&new.values[2], &payload_b, "second NEW toasted payload")?;

            // Aborted work must never appear in logical decoding.
            db.client
                .batch_execute(&format!(
                    "BEGIN; UPDATE {relation} SET body = 'must-not-decode' WHERE id = 1; ROLLBACK;"
                ))
                .await?;
            let messages = consume_pgoutput(&db.client, &slot, &publication).await?;
            anyhow::ensure!(
                row_change_count(&messages) == 0,
                "rolled-back DML appeared in pgoutput: {messages:?}"
            );

            // Savepoint rollback is another branch-workflow invariant.
            db.client
                .batch_execute(&format!(
                    "BEGIN; SAVEPOINT cow_probe; UPDATE {relation} SET body = 'savepoint-discarded' WHERE id = 1; ROLLBACK TO SAVEPOINT cow_probe; COMMIT;"
                ))
                .await?;
            let messages = consume_pgoutput(&db.client, &slot, &publication).await?;
            anyhow::ensure!(
                row_change_count(&messages) == 0,
                "savepoint-rolled-back DML appeared in pgoutput: {messages:?}"
            );

            // Multiple changes in one committed source transaction must remain inside one
            // BEGIN/COMMIT boundary, which is required for atomic preimage application.
            db.client
                .execute(
                    &format!("INSERT INTO {relation} (id, body, payload) VALUES (2, $1, $2)"),
                    &[&"peer", &payload_a],
                )
                .await?;
            let _ = consume_pgoutput(&db.client, &slot, &publication).await?;
            db.client
                .batch_execute(&format!(
                    "BEGIN; UPDATE {relation} SET body = 'txn-one' WHERE id = 1; UPDATE {relation} SET body = 'txn-two' WHERE id = 2; COMMIT;"
                ))
                .await?;
            let messages = consume_pgoutput(&db.client, &slot, &publication).await?;
            let begin_count = messages
                .iter()
                .filter(|message| matches!(message, PgOutputMessage::Begin { .. }))
                .count();
            let commit_count = messages
                .iter()
                .filter(|message| matches!(message, PgOutputMessage::Commit { .. }))
                .count();
            let updates: Vec<_> = messages
                .iter()
                .filter_map(|message| match message {
                    PgOutputMessage::Update { old, new, .. } => Some((old.as_ref(), new)),
                    _ => None,
                })
                .collect();
            anyhow::ensure!(begin_count == 1 && commit_count == 1, "expected one source transaction boundary");
            anyhow::ensure!(updates.len() == 2, "expected two UPDATEs in the committed transaction");
            anyhow::ensure!(updates.iter().all(|(old, _)| old.is_some()), "every FULL-identity UPDATE needs OLD");
            for (old, _) in updates {
                let old = old.expect("checked above");
                anyhow::ensure!(old.values.len() == 3, "transaction OLD tuple must contain all columns");
                anyhow::ensure!(
                    text(&old.values[2]).is_some(),
                    "transaction OLD toasted payload was not materialized: {:?}",
                    old.values[2]
                );
            }

            // DELETE must carry the full OLD row, including the toasted payload.
            db.client
                .execute(&format!("DELETE FROM {relation} WHERE id = 1"), &[])
                .await?;
            let messages = consume_pgoutput(&db.client, &slot, &publication).await?;
            let old = delete_message(&messages)?;
            anyhow::ensure!(old.values.len() == 3, "DELETE OLD tuple must contain all columns");
            assert_text_eq(&old.values[0], "1", "DELETE OLD id")?;
            assert_text_eq(&old.values[1], "txn-one", "DELETE OLD body")?;
            assert_text_eq(&old.values[2], &payload_b, "DELETE OLD toasted payload")?;

            Ok::<(), anyhow::Error>(())
        }
        .await;

        // Logical slots are database-global resources; clean up even when an assertion fails.
        let _ = db
            .client
            .query("SELECT pg_catalog.pg_drop_replication_slot($1)", &[&slot])
            .await;
        let _ = db
            .client
            .batch_execute(&format!("DROP PUBLICATION IF EXISTS {publication}"))
            .await;

        result?;
    }
    Ok(())
}
