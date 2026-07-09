//! WhatsApp / Intercom-style chat history example.
//!
//! Covers multi-tenant parallel inserts, multi-wave flushing, small Parquet
//! files + manifest checks, indexes, concurrent DML, and cold-then-delete overlay.

#[path = "support/mod.rs"]
mod support;

use anyhow::{Context, Result};
use support::{
    assert_cold_then_delete_overlay, assert_indexes_exist, assert_merge_scan_uses_cold,
    assert_multi_tenant_visibility, assert_parquet_and_manifest, force_flush_table,
    log_scenario_start, log_step, manage_user_scoped_with_policy, run_parallel_clients, set_scope,
    with_example_timeout, ExampleConfig, FlushCtx, InsertProgress,
};

const MIN_FLUSH_ROWS: i64 = 400;
const MAX_ROWS_PER_FILE: i64 = 1_000;

#[tokio::test]
async fn chat_history_parallel_tenants_flush_policy_and_cold_scrollback() -> Result<()> {
    with_example_timeout(
        "chat_history",
        chat_history_parallel_tenants_flush_policy_and_cold_scrollback_inner(),
    )
    .await
}

async fn chat_history_parallel_tenants_flush_policy_and_cold_scrollback_inner() -> Result<()> {
    support::e2e::require_pgrx_server().await?;
    let config = ExampleConfig::from_env();
    let target = support::e2e::local_pg_matrix()
        .into_iter()
        .next()
        .context("no local pg target configured")?;

    let db = support::e2e::TestDb::start(target.clone(), "chat_history").await?;
    let table_name = "messages";
    let relation = db.relation(table_name);
    log_scenario_start("chat_history", &relation, &db.storage_root, config);
    let flush = |label: &'static str| FlushCtx {
        label,
        storage_root: &db.storage_root,
    };

    {
        let _step = log_step("create messages table + indexes");
        create_messages_table(&db.client, &relation, table_name).await?;
        assert_indexes_exist(
            &db.client,
            &db.schema,
            &[
                &format!("{table_name}_tenant_conv_created_idx"),
                &format!("{table_name}_sender_created_idx"),
            ],
        )
        .await?;
    }

    // Keep policy tight so successive insert+flush waves create many small files.
    let hot_row_limit = (config.rows / 2).max(MIN_FLUSH_ROWS);
    {
        let _step = log_step(format!(
            "manage_table hot_row_limit={hot_row_limit} min_flush_rows={MIN_FLUSH_ROWS} max_rows_per_file={MAX_ROWS_PER_FILE}"
        ));
        manage_user_scoped_with_policy(
            &db.client,
            &db.storage_name,
            &relation,
            "tenant_id",
            "created_at",
            hot_row_limit,
            MIN_FLUSH_ROWS,
            MAX_ROWS_PER_FILE,
        )
        .await?;
    }

    {
        let _step = log_step(format!(
            "seed {} rows across {} tenants",
            config.rows, config.scopes
        ));
        seed_messages_parallel(&target, &relation, &config).await?;
        support::wait_for_jobs(&db.client, &relation).await?;
    }

    let mut next_id = config.rows + 1;
    let mut total_flushed = 0_i64;
    let mut waves = 0_usize;

    // Wave 1: seed alone may already overflow hot_row_limit.
    let first = support::flush_table(&db.client, &relation, Some(flush("seed-flush-1"))).await?;
    support::wait_for_jobs(&db.client, &relation).await?;
    if first > 0 {
        total_flushed += first;
        waves += 1;
    }

    // Waves 2/3: concurrent burst inserts across tenants, then flush again.
    for wave in 0..2 {
        let burst = MIN_FLUSH_ROWS + 150;
        concurrent_burst_inserts(&target, &relation, &config, next_id, burst, wave).await?;
        next_id += burst * config.scopes as i64;

        let flushed = support::flush_table(
            &db.client,
            &relation,
            Some(flush(match wave {
                0 => "burst-flush-1",
                _ => "burst-flush-2",
            })),
        )
        .await?;
        support::wait_for_jobs(&db.client, &relation).await?;
        if flushed > 0 {
            total_flushed += flushed;
            waves += 1;
        }
    }

    {
        let _step = log_step("concurrent hot UPDATE/DELETE");
        concurrent_hot_dml(&target, &relation, &config).await?;
    }

    // Verify tenant isolation before the cold-delete overlay drives additional
    // force flushes that can leave sibling tenants cold-only.
    let tenant_a = config.scope_id("tenant", 0);
    let tenant_b = config.scope_id("tenant", 1);
    assert_multi_tenant_visibility(&db.client, &relation, "tenant_id", &[&tenant_a, &tenant_b])
        .await?;

    let focus_tenant = config.scope_id("tenant", 1);
    let focus_conversation = config.scope_id("conv", 1);
    set_scope(&db.client, &focus_tenant).await?;

    // Flush → rematerialize → delete → flush: a previously cold PK deleted in hot.
    let overlay_ids =
        support::scoped_overlay_ids_from_cold(&db.client, &relation, "tenant_id", &focus_tenant, 3)
            .await?;
    assert_cold_then_delete_overlay(
        &db.client,
        &relation,
        &focus_tenant,
        "tenant_id",
        &overlay_ids,
        &|id| {
            format!(
                "INSERT INTO {relation} (tenant_id, conversation_id, id, sender_id, role, body, created_at) \
                 VALUES ('{tenant}', 'conv-overlay', {id}, 'overlay', 'user', 'overlay', now()) \
                 ON CONFLICT (id) DO UPDATE SET body = EXCLUDED.body",
                relation = relation,
                tenant = focus_tenant,
                id = id,
            )
        },
        Some(flush("overlay")),
    )
    .await?;

    let forced =
        force_flush_table(&db.client, &relation, Some(flush("force-after-overlay"))).await?;
    support::wait_for_jobs(&db.client, &relation).await?;
    total_flushed += forced;
    waves += 1;

    assert!(
        waves >= 2 && total_flushed > 0,
        "expected multi-wave flushing, got {waves} waves / {total_flushed} rows"
    );

    {
        let _step = log_step("verify parquet files + manifests on disk");
        assert_parquet_and_manifest(
            &db.client,
            &relation,
            &db.storage_root,
            MAX_ROWS_PER_FILE,
            3,
        )
        .await?;
    }

    {
        let _step = log_step("hot recent + cold scrollback queries");
        let recent = query_recent_messages(
            &db.client,
            &relation,
            &focus_tenant,
            &focus_conversation,
            50,
        )
        .await?;
        assert!(!recent.is_empty());

        let old = query_old_scrollback(
            &db.client,
            &relation,
            &focus_tenant,
            &focus_conversation,
            100,
        )
        .await?;
        assert!(!old.is_empty(), "scrollback should merge hot+cold history");
    }

    {
        let _step = log_step("EXPLAIN merge scan uses cold");
        assert_merge_scan_uses_cold(
            &db.client,
            &relation,
            &format!(
                "tenant_id = '{focus_tenant}' AND conversation_id = '{focus_conversation}' AND created_at < timestamptz '2024-06-01'"
            ),
            1,
        )
        .await?;
    }

    // Deleted ids must stay gone after later flush waves.
    for &id in &overlay_ids {
        assert_eq!(
            support::visible_pk_count(&db.client, &relation, id).await?,
            0,
            "deleted id {id} must remain invisible after force flush"
        );
    }

    // Cross-tenant isolation: a sibling tenant can still write and read its own
    // traffic after the focus tenant performs cold-delete overlay operations.
    let other_tenant = config.scope_id("tenant", 2.min(config.scopes - 1));
    set_scope(&db.client, &other_tenant).await?;
    let sibling_probe_id = next_id + 900_000;
    let sibling_probe_params: [&(dyn tokio_postgres::types::ToSql + Sync); 2] =
        [&other_tenant, &sibling_probe_id];
    db.client
        .execute(
            &format!(
                "INSERT INTO {relation} (tenant_id, conversation_id, id, sender_id, role, body, created_at) \
                 VALUES ($1, 'conv-sibling-probe', $2, 'sibling-probe', 'user', 'probe', now())"
            ),
            &sibling_probe_params,
        )
        .await?;
    let other_count = support::visible_pk_count(&db.client, &relation, sibling_probe_id).await?;
    assert!(
        other_count > 0,
        "sibling tenant {other_tenant} must retain visible history"
    );

    // Post-churn force flush: after earlier force flushes the hot side can sit
    // below hot_row_limit, so a policy-only flush may flush 0. Force guarantees
    // the newly inserted burst is moved cold without depending on excess policy.
    let post_burst = (hot_row_limit + MIN_FLUSH_ROWS).max(MIN_FLUSH_ROWS * 2);
    let rows_per_scope = (post_burst / config.scopes as i64).max(1);
    concurrent_burst_inserts(&target, &relation, &config, next_id, rows_per_scope, 9).await?;
    let post_flushed =
        force_flush_table(&db.client, &relation, Some(flush("post-churn-force"))).await?;
    assert!(
        post_flushed > 0,
        "post-churn force flush should move additional rows cold, flushed {post_flushed}"
    );

    let segments = support::load_cold_segments(&db.client, &relation).await?;
    assert!(
        segments
            .iter()
            .any(|s| s.row_count <= MAX_ROWS_PER_FILE / 2 || s.row_count == MAX_ROWS_PER_FILE),
        "expected small/batched parquet files from max_rows_per_file={MAX_ROWS_PER_FILE}"
    );

    Ok(())
}

async fn create_messages_table(
    client: &tokio_postgres::Client,
    relation: &str,
    table_name: &str,
) -> Result<()> {
    client
        .batch_execute(&format!(
            r#"
            CREATE TABLE {relation} (
              tenant_id text NOT NULL,
              conversation_id text NOT NULL,
              id bigint PRIMARY KEY,
              sender_id text NOT NULL,
              role text NOT NULL,
              body text NOT NULL,
              created_at timestamptz NOT NULL,
              edited_at timestamptz,
              deleted_at timestamptz
            );
            CREATE INDEX {table_name}_tenant_conv_created_idx
              ON {relation} (tenant_id, conversation_id, created_at DESC);
            CREATE INDEX {table_name}_sender_created_idx
              ON {relation} (tenant_id, sender_id, created_at DESC);
            "#
        ))
        .await?;
    Ok(())
}

async fn seed_messages_parallel(
    target: &support::e2e::PgTarget,
    relation: &str,
    config: &ExampleConfig,
) -> Result<()> {
    let relation = relation.to_string();
    let rows_per_scope = config.rows_per_scope();
    let scopes = config.scopes;
    let clients = config.clients;
    let progress = InsertProgress::new("seed messages", config.rows);
    run_parallel_clients(target, clients, {
        let progress = progress.clone();
        move |client_idx, client| {
            let relation = relation.clone();
            let progress = progress.clone();
            async move {
                let scopes_per_client = scopes.div_ceil(clients);
                let scope_start = client_idx * scopes_per_client;
                let scope_end = (scope_start + scopes_per_client).min(scopes);
                for scope_idx in scope_start..scope_end {
                    let tenant = format!("tenant-{scope_idx:04}");
                    let conversation = format!("conv-{scope_idx:04}");
                    let base_id = scope_idx as i64 * rows_per_scope;
                    set_scope(&client, &tenant).await?;
                    client
                        .batch_execute(&format!(
                            r#"
                        INSERT INTO {relation} (
                          tenant_id, conversation_id, id, sender_id, role, body, created_at
                        )
                        SELECT
                          '{tenant}',
                          '{conversation}',
                          {base_id} + gs,
                          'sender-' || ((gs % 5) + 1),
                          CASE WHEN gs % 7 = 0 THEN 'agent' ELSE 'user' END,
                          'message body ' || gs,
                          timestamptz '2024-01-01' + ((gs % 365) || ' days')::interval
                            + ((gs % 86400) || ' seconds')::interval
                        FROM generate_series(1, {rows_per_scope}) AS gs;
                        "#
                        ))
                        .await?;
                    progress.record(rows_per_scope);
                }
                Ok(())
            }
        }
    })
    .await?;
    progress.finish();
    Ok(())
}

async fn concurrent_burst_inserts(
    target: &support::e2e::PgTarget,
    relation: &str,
    config: &ExampleConfig,
    start_id: i64,
    rows_per_scope: i64,
    wave: usize,
) -> Result<()> {
    let relation = relation.to_string();
    let scopes = config.scopes;
    let clients = config.clients;
    let total_rows = rows_per_scope * scopes as i64;
    let progress = InsertProgress::new(format!("burst wave {wave}"), total_rows);
    run_parallel_clients(target, clients, {
        let progress = progress.clone();
        move |client_idx, client| {
            let relation = relation.clone();
            let progress = progress.clone();
            async move {
                let scopes_per_client = scopes.div_ceil(clients);
                let scope_start = client_idx * scopes_per_client;
                let scope_end = (scope_start + scopes_per_client).min(scopes);
                for scope_idx in scope_start..scope_end {
                    let tenant = format!("tenant-{scope_idx:04}");
                    let conversation = format!("conv-{scope_idx:04}");
                    let base_id = start_id + (scope_idx as i64) * rows_per_scope;
                    set_scope(&client, &tenant).await?;
                    client
                        .batch_execute(&format!(
                            r#"
                        INSERT INTO {relation} (
                          tenant_id, conversation_id, id, sender_id, role, body, created_at
                        )
                        SELECT
                          '{tenant}',
                          '{conversation}',
                          {base_id} + gs,
                          'burst-sender-{wave}',
                          'user',
                          'burst wave {wave} msg ' || gs,
                          now() - ((gs % 30) || ' minutes')::interval
                        FROM generate_series(1, {rows_per_scope}) AS gs;
                        "#
                        ))
                        .await?;
                    progress.record(rows_per_scope);
                }
                Ok(())
            }
        }
    })
    .await?;
    progress.finish();
    Ok(())
}

async fn concurrent_hot_dml(
    target: &support::e2e::PgTarget,
    relation: &str,
    config: &ExampleConfig,
) -> Result<()> {
    let relation = relation.to_string();
    let rows_per_scope = config.rows_per_scope();
    let scopes = config.scopes.min(config.clients);
    let clients = scopes;
    run_parallel_clients(target, clients, move |client_idx, client| {
        let relation = relation.clone();
        async move {
            let tenant = format!("tenant-{client_idx:04}");
            let base = client_idx as i64 * rows_per_scope;
            set_scope(&client, &tenant).await?;
            // Update a recent-ish hot row when still present; ignore missing after flush prune.
            let _ = client
                .execute(
                    &format!(
                        "UPDATE {relation}
                         SET body = body || ' [edited]', edited_at = now()
                         WHERE id = $1"
                    ),
                    &[&(base + rows_per_scope)],
                )
                .await;
            let _ = client
                .execute(
                    &format!("DELETE FROM {relation} WHERE id = $1"),
                    &[&(base + rows_per_scope - 1)],
                )
                .await;
            Ok(())
        }
    })
    .await
}

async fn query_recent_messages(
    client: &tokio_postgres::Client,
    relation: &str,
    tenant: &str,
    conversation: &str,
    limit: i64,
) -> Result<Vec<String>> {
    set_scope(client, tenant).await?;
    let rows = client
        .query(
            &format!(
                r#"
                SELECT body
                FROM {relation}
                WHERE tenant_id = $1
                  AND conversation_id = $2
                ORDER BY created_at DESC
                LIMIT $3
                "#
            ),
            &[&tenant, &conversation, &limit],
        )
        .await?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

async fn query_old_scrollback(
    client: &tokio_postgres::Client,
    relation: &str,
    tenant: &str,
    conversation: &str,
    limit: i64,
) -> Result<Vec<String>> {
    set_scope(client, tenant).await?;
    let rows = client
        .query(
            &format!(
                r#"
                SELECT body
                FROM {relation}
                WHERE tenant_id = $1
                  AND conversation_id = $2
                  AND created_at < timestamptz '2024-06-01'
                ORDER BY created_at DESC
                LIMIT $3
                "#
            ),
            &[&tenant, &conversation, &limit],
        )
        .await?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}
