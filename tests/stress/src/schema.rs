//! Wide chat DDL and optional sibling tables for multi_table / joins packs.

use anyhow::Result;
use tokio_postgres::Client;

use crate::config::StressConfig;
use crate::support::{log_always, manage_user_scoped_with_policy};

/// Relations created for one stress fixture.
#[derive(Debug, Clone)]
pub struct StressSchema {
    pub messages: String,
    pub conversations: Option<String>,
    pub receipts: Option<String>,
}

impl StressSchema {
    /// All managed relations that participate in flush / job asserts.
    #[must_use]
    pub fn managed_relations(&self) -> Vec<&str> {
        let mut out = vec![self.messages.as_str()];
        if let Some(rel) = &self.conversations {
            out.push(rel);
        }
        if let Some(rel) = &self.receipts {
            out.push(rel);
        }
        out
    }
}

/// Creates indexes + manage_table for the chat schema (and sibling tables when packed).
///
/// # Errors
///
/// Returns an error when DDL or manage_table fails.
pub async fn create_and_manage(
    client: &Client,
    schema: &str,
    storage_name: &str,
    config: &StressConfig,
) -> Result<StressSchema> {
    let messages = format!("{schema}.messages");
    let table = "messages";
    log_always(format!("creating wide messages table {messages}"));
    client
        .batch_execute(&format!(
            r#"
            CREATE TABLE {messages} (
              id bigint PRIMARY KEY,
              tenant_id text NOT NULL,
              conversation_id text NOT NULL,
              sender_id text NOT NULL,
              body text NOT NULL,
              payload jsonb NOT NULL,
              blob bytea NOT NULL,
              created_at timestamptz NOT NULL,
              updated_at timestamptz NOT NULL,
              version int NOT NULL DEFAULT 1,
              flags int NOT NULL DEFAULT 0,
              status text NOT NULL DEFAULT 'active'
            );
            CREATE INDEX {table}_tenant_conv_created_idx
              ON {messages} (tenant_id, conversation_id, created_at DESC);
            CREATE INDEX {table}_tenant_updated_idx
              ON {messages} (tenant_id, updated_at DESC);
            CREATE INDEX {table}_sender_created_idx
              ON {messages} (sender_id, created_at DESC);
            "#
        ))
        .await?;

    manage_user_scoped_with_policy(
        client,
        storage_name,
        &messages,
        "tenant_id",
        "created_at",
        config.hot_row_limit,
        config.min_flush_rows,
        config.max_rows_per_file,
    )
    .await?;

    let mut out = StressSchema {
        messages,
        conversations: None,
        receipts: None,
    };

    if config.packs.multi_table() {
        let conversations = format!("{schema}.conversations");
        log_always(format!("creating conversations table {conversations}"));
        client
            .batch_execute(&format!(
                r#"
                CREATE TABLE {conversations} (
                  id bigint PRIMARY KEY,
                  tenant_id text NOT NULL,
                  conversation_id text NOT NULL,
                  title text NOT NULL,
                  updated_at timestamptz NOT NULL,
                  version int NOT NULL DEFAULT 1
                );
                CREATE INDEX conversations_tenant_conv_idx
                  ON {conversations} (tenant_id, conversation_id);
                "#
            ))
            .await?;
        manage_user_scoped_with_policy(
            client,
            storage_name,
            &conversations,
            "tenant_id",
            "updated_at",
            config.hot_row_limit,
            config.min_flush_rows,
            config.max_rows_per_file,
        )
        .await?;

        let receipts = format!("{schema}.receipts");
        log_always(format!("creating receipts table {receipts}"));
        client
            .batch_execute(&format!(
                r#"
                CREATE TABLE {receipts} (
                  id bigint PRIMARY KEY,
                  tenant_id text NOT NULL,
                  message_id bigint NOT NULL,
                  reader_id text NOT NULL,
                  read_at timestamptz NOT NULL
                );
                CREATE INDEX receipts_tenant_message_idx
                  ON {receipts} (tenant_id, message_id);
                "#
            ))
            .await?;
        manage_user_scoped_with_policy(
            client,
            storage_name,
            &receipts,
            "tenant_id",
            "read_at",
            config.hot_row_limit,
            config.min_flush_rows,
            config.max_rows_per_file,
        )
        .await?;

        out.conversations = Some(conversations);
        out.receipts = Some(receipts);
    }

    Ok(out)
}

/// Builds a JSON payload of approximately `target_bytes`.
#[must_use]
pub fn fat_payload(target_bytes: usize, seq: u64) -> String {
    let pad_len = target_bytes.saturating_sub(64);
    let pad = "x".repeat(pad_len);
    serde_json::json!({
        "seq": seq,
        "client": "stress",
        "attachments": [{"name": "stub.bin", "size": target_bytes}],
        "pad": pad,
    })
    .to_string()
}

/// Builds a fixed-size blob.
#[must_use]
pub fn fat_blob(bytea_bytes: usize, seed: u8) -> Vec<u8> {
    vec![seed; bytea_bytes]
}
