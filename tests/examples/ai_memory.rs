//! AI memory and agent session history example.
//!
//! Covers large text payloads, multi-wave flush, small Parquet files + manifests,
//! multi-workspace scopes, concurrent DML, and cold-then-delete compliance deletes.

#[path = "support/mod.rs"]
mod support;

use anyhow::{Context, Result};
use support::{
    assert_cold_then_delete_overlay, assert_indexes_exist, assert_merge_scan_uses_cold,
    assert_multi_tenant_visibility, assert_parquet_and_manifest, flush_waves, force_flush_table,
    log_scenario_start, log_step, manage_user_scoped_with_policy, run_parallel_clients, set_scope,
    with_example_timeout, ExampleConfig, FlushCtx, InsertProgress,
};

const MIN_FLUSH_ROWS: i64 = 300;
const MAX_ROWS_PER_FILE: i64 = 1_000;

#[tokio::test]
async fn ai_memory_large_sessions_flush_in_batches_and_audit_cold_history() -> Result<()> {
    with_example_timeout(
        "ai_memory",
        ai_memory_large_sessions_flush_in_batches_and_audit_cold_history_inner(),
    )
    .await
}

async fn ai_memory_large_sessions_flush_in_batches_and_audit_cold_history_inner() -> Result<()> {
    support::e2e::require_pgrx_server().await?;
    let config = ExampleConfig::from_env();
    let target = support::e2e::local_pg_matrix()
        .into_iter()
        .next()
        .context("no local pg target configured")?;

    let db = support::e2e::TestDb::start(target.clone(), "ai_memory").await?;
    let table_name = "session_events";
    let relation = db.relation(table_name);
    log_scenario_start("ai_memory", &relation, &db.storage_root, config);
    let flush = |label: &'static str| FlushCtx {
        label,
        storage_root: &db.storage_root,
    };

    {
        let _step = log_step("create session_events table + indexes");
        create_session_events_table(&db.client, &relation, table_name).await?;
        assert_indexes_exist(
            &db.client,
            &db.schema,
            &[
                &format!("{table_name}_workspace_session_created_idx"),
                &format!("{table_name}_workspace_user_created_idx"),
            ],
        )
        .await?;
    }

    let hot_row_limit = (config.rows / 2).max(MIN_FLUSH_ROWS);
    {
        let _step = log_step("manage_table + seed");
        manage_user_scoped_with_policy(
            &db.client,
            &db.storage_name,
            &relation,
            "workspace_id",
            "created_at",
            hot_row_limit,
            MIN_FLUSH_ROWS,
            MAX_ROWS_PER_FILE,
        )
        .await?;
        seed_session_events_parallel(&target, &relation, &config).await?;
        support::wait_for_jobs(&db.client, &relation).await?;
    }

    let mut next_id = config.rows + 1;
    let mut waves = flush_waves(&db.client, &relation, 1, Some(flush("seed"))).await?;
    support::assert_policy_flush_progress(&db.client, &relation, "seed", &waves).await?;
    for wave in 0..2 {
        let burst = MIN_FLUSH_ROWS + 80;
        concurrent_session_bursts(&target, &relation, &config, next_id, burst, wave).await?;
        next_id += burst * config.scopes as i64;
        let label = match wave {
            0 => "burst-1",
            _ => "burst-2",
        };
        let burst_waves = flush_waves(&db.client, &relation, 1, Some(flush(label))).await?;
        support::assert_policy_flush_progress(&db.client, &relation, label, &burst_waves).await?;
        waves.extend(burst_waves);
    }
    {
        let _step = log_step("concurrent hot UPDATE/DELETE");
        concurrent_hot_dml(&target, &relation, &config).await?;
    }

    // Verify multiple workspaces are visible while the table still has a mixed
    // hot+cold shape, before the cold-delete overlay drives an extra force flush.
    let tenant_a = config.scope_id("workspace", 0);
    let tenant_b = config.scope_id("workspace", 1);
    assert_multi_tenant_visibility(
        &db.client,
        &relation,
        "workspace_id",
        &[&tenant_a, &tenant_b],
    )
    .await?;

    let focus_workspace = config.scope_id("workspace", 1);
    set_scope(&db.client, &focus_workspace).await?;
    let overlay_ids = support::scoped_overlay_ids_from_cold(
        &db.client,
        &relation,
        "workspace_id",
        &focus_workspace,
        3,
    )
    .await?;
    assert_cold_then_delete_overlay(
        &db.client,
        &relation,
        &focus_workspace,
        "workspace_id",
        &overlay_ids,
        &|id| {
            format!(
                "INSERT INTO {relation} (workspace_id, user_id, session_id, id, event_type, prompt, response, token_count, created_at) \
                 VALUES ('{ws}', 'user-overlay', 'session-overlay', {id}, 'completion', 'p', 'r', 1, now()) \
                 ON CONFLICT (id) DO UPDATE SET response = EXCLUDED.response",
                relation = relation,
                ws = focus_workspace,
                id = id,
            )
        },
        Some(flush("overlay")),
    )
    .await?;

    let forced = force_flush_table(&db.client, &relation, Some(flush("force-final"))).await?;
    support::wait_for_jobs(&db.client, &relation).await?;
    if forced > 0 {
        waves.push(forced);
    }
    assert!(
        waves.len() >= 2,
        "expected multi-wave AI flush history, got {waves:?}"
    );

    assert_parquet_and_manifest(
        &db.client,
        &relation,
        &db.storage_root,
        MAX_ROWS_PER_FILE,
        2,
    )
    .await?;

    {
        let _step = log_step("post-flush session + audit queries");
        support::timed_async(
            format!("set scope after parquet verify ({focus_workspace})"),
            set_scope(&db.client, &focus_workspace),
        )
        .await?;

        let current = support::timed_async(
            "query current session",
            query_current_session(
                &db.client,
                &relation,
                &focus_workspace,
                &config.scope_id("session", 1),
            ),
        )
        .await?;
        assert!(!current.is_empty());

        let audit = support::timed_async(
            "query audit window",
            query_audit_window(&db.client, &relation, &focus_workspace),
        )
        .await?;
        assert!(!audit.is_empty());
    }

    {
        let _step = log_step("EXPLAIN merge scan uses cold for audit window");
        assert_merge_scan_uses_cold(
            &db.client,
            &relation,
            &format!(
                "workspace_id = '{focus_workspace}' AND created_at < timestamptz '2025-02-01'"
            ),
            1,
        )
        .await?;
    }

    for &id in &overlay_ids {
        assert_eq!(
            support::visible_pk_count(&db.client, &relation, id).await?,
            0
        );
    }

    Ok(())
}

async fn create_session_events_table(
    client: &tokio_postgres::Client,
    relation: &str,
    table_name: &str,
) -> Result<()> {
    client
        .batch_execute(&format!(
            r#"
            CREATE TABLE {relation} (
              workspace_id text NOT NULL,
              user_id text NOT NULL,
              session_id text NOT NULL,
              id bigint PRIMARY KEY,
              event_type text NOT NULL,
              prompt text NOT NULL,
              response text NOT NULL,
              tool_name text,
              token_count integer NOT NULL,
              created_at timestamptz NOT NULL
            );
            CREATE INDEX {table_name}_workspace_session_created_idx
              ON {relation} (workspace_id, session_id, created_at);
            CREATE INDEX {table_name}_workspace_user_created_idx
              ON {relation} (workspace_id, user_id, created_at);
            "#
        ))
        .await?;
    Ok(())
}

async fn seed_session_events_parallel(
    target: &support::e2e::PgTarget,
    relation: &str,
    config: &ExampleConfig,
) -> Result<()> {
    let relation = relation.to_string();
    let rows_per_scope = config.rows_per_scope();
    let scopes = config.scopes;
    let clients = config.clients;
    let progress = InsertProgress::new("seed session events", config.rows);
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
                    let workspace = format!("workspace-{scope_idx:04}");
                    let session = format!("session-{scope_idx:04}");
                    let user = format!("user-{scope_idx:04}");
                    let base_id = scope_idx as i64 * rows_per_scope;
                    set_scope(&client, &workspace).await?;
                    client
                        .batch_execute(&format!(
                            r#"
                        INSERT INTO {relation} (
                          workspace_id, user_id, session_id, id, event_type,
                          prompt, response, tool_name, token_count, created_at
                        )
                        SELECT
                          '{workspace}',
                          '{user}',
                          '{session}',
                          {base_id} + gs,
                          CASE WHEN gs % 5 = 0 THEN 'tool_call' ELSE 'completion' END,
                          repeat('prompt chunk ', 20 + (gs % 10)),
                          repeat('response chunk ', 40 + (gs % 20)),
                          CASE WHEN gs % 5 = 0 THEN 'search_docs' ELSE NULL END,
                          200 + (gs % 800),
                          timestamptz '2025-01-01' + ((gs % 180) || ' days')::interval
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

async fn concurrent_session_bursts(
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
    let progress = InsertProgress::new(format!("session burst wave {wave}"), total_rows);
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
                    let workspace = format!("workspace-{scope_idx:04}");
                    let session = format!("session-{scope_idx:04}");
                    let user = format!("user-{scope_idx:04}");
                    let base_id = start_id + scope_idx as i64 * rows_per_scope;
                    set_scope(&client, &workspace).await?;
                    client
                        .batch_execute(&format!(
                            r#"
                        INSERT INTO {relation} (
                          workspace_id, user_id, session_id, id, event_type,
                          prompt, response, tool_name, token_count, created_at
                        )
                        SELECT
                          '{workspace}', '{user}', '{session}',
                          {base_id} + gs, 'completion',
                          'burst prompt {wave} ' || gs,
                          'burst response {wave} ' || gs,
                          NULL, 100 + gs, now()
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
    let clients = config.scopes.min(config.clients);
    run_parallel_clients(target, clients, move |client_idx, client| {
        let relation = relation.clone();
        async move {
            let workspace = format!("workspace-{client_idx:04}");
            let base = client_idx as i64 * rows_per_scope;
            set_scope(&client, &workspace).await?;
            let _ = client
                .execute(
                    &format!(
                        "UPDATE {relation}
                         SET response = response || ' [reviewed]'
                         WHERE id = $1"
                    ),
                    &[&(base + rows_per_scope)],
                )
                .await;
            let _ = client
                .execute(
                    &format!("DELETE FROM {relation} WHERE id = $1"),
                    &[&(base + rows_per_scope - 2)],
                )
                .await;
            Ok(())
        }
    })
    .await
}

async fn query_current_session(
    client: &tokio_postgres::Client,
    relation: &str,
    workspace: &str,
    session: &str,
) -> Result<Vec<String>> {
    set_scope(client, workspace).await?;
    let rows = client
        .query(
            &format!(
                r#"
                SELECT event_type
                FROM {relation}
                WHERE workspace_id = $1
                  AND session_id = $2
                ORDER BY created_at ASC
                LIMIT 100
                "#
            ),
            &[&workspace, &session],
        )
        .await?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

async fn query_audit_window(
    client: &tokio_postgres::Client,
    relation: &str,
    workspace: &str,
) -> Result<Vec<(String, i32)>> {
    set_scope(client, workspace).await?;
    let rows = client
        .query(
            &format!(
                r#"
                SELECT prompt, token_count
                FROM {relation}
                WHERE workspace_id = $1
                  AND created_at BETWEEN timestamptz '2025-01-01' AND timestamptz '2025-03-01'
                ORDER BY created_at
                LIMIT 200
                "#
            ),
            &[&workspace],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect())
}
