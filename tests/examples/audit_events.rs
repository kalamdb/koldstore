//! Fintech immutable audit ledger example.
//!
//! Covers multi-tenant audit history, multi-wave flush, Parquet/manifest proof
//! metadata, indexes, concurrent inserts, and cold-then-delete tombstones.
//! KoldStore is for audit/event history — not mutable account balances.

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
const MAX_ROWS_PER_FILE: i64 = 120;

#[tokio::test]
async fn audit_events_immutable_history_hot_recent_and_cold_regulator_export() -> Result<()> {
    with_example_timeout(
        "audit_events",
        audit_events_immutable_history_hot_recent_and_cold_regulator_export_inner(),
    )
    .await
}

async fn audit_events_immutable_history_hot_recent_and_cold_regulator_export_inner() -> Result<()> {
    support::e2e::require_pgrx_server().await?;
    let config = ExampleConfig::from_env();
    let target = support::e2e::local_pg_matrix()
        .into_iter()
        .next()
        .context("no local pg target configured")?;

    let db = support::e2e::TestDb::start(target.clone(), "audit_events").await?;
    let table_name = "account_events";
    let relation = db.relation(table_name);
    log_scenario_start("audit_events", &relation, &db.storage_root, config);
    let flush = |label: &'static str| FlushCtx {
        label,
        storage_root: &db.storage_root,
    };

    {
        let _step = log_step("create account_events table + indexes");
        create_account_events_table(&db.client, &relation, table_name).await?;
        assert_indexes_exist(
            &db.client,
            &db.schema,
            &[
                &format!("{table_name}_tenant_account_created_idx"),
                &format!("{table_name}_tenant_event_created_idx"),
            ],
        )
        .await?;
    }

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
        seed_audit_events_parallel(&target, &relation, &config).await?;
        support::wait_for_jobs(&db.client, &relation).await?;
    }

    let mut next_id = config.rows + 1;
    let mut waves = flush_waves(&db.client, &relation, 1, Some(flush("seed"))).await?;
    for wave in 0..2 {
        let burst = MIN_FLUSH_ROWS + 50;
        concurrent_audit_bursts(&target, &relation, &config, next_id, burst, wave).await?;
        next_id += burst * config.scopes as i64;
        waves.extend(
            flush_waves(
                &db.client,
                &relation,
                1,
                Some(flush(match wave {
                    0 => "burst-1",
                    _ => "burst-2",
                })),
            )
            .await?,
        );
    }
    {
        let _step = log_step("concurrent hot UPDATE/DELETE");
        concurrent_hot_dml(&target, &relation, &config).await?;
    }

    // Verify tenant isolation before the overlay path adds another force flush.
    let tenant_a = config.scope_id("tenant", 0);
    let tenant_b = config.scope_id("tenant", 1);
    assert_multi_tenant_visibility(&db.client, &relation, "tenant_id", &[&tenant_a, &tenant_b])
        .await?;

    let focus_tenant = config.scope_id("tenant", 1);
    let focus_account = config.scope_id("account", 1);
    set_scope(&db.client, &focus_tenant).await?;
    let overlay_ids = support::fresh_overlay_ids(next_id + 10_000, 3);
    assert_cold_then_delete_overlay(
        &db.client,
        &relation,
        &focus_tenant,
        "tenant_id",
        &overlay_ids,
        &|id| {
            format!(
                "INSERT INTO {relation} (tenant_id, account_id, id, actor_id, event_type, before_state, after_state, created_at) \
                 VALUES ('{tenant}', 'account-overlay', {id}, 'actor', 'overlay', '{{}}'::jsonb, '{{}}'::jsonb, now()) \
                 ON CONFLICT (id) DO UPDATE SET event_type = EXCLUDED.event_type",
                relation = relation,
                tenant = focus_tenant,
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
        "expected multi-wave audit flushes, got {waves:?}"
    );

    assert_parquet_and_manifest(
        &db.client,
        &relation,
        &db.storage_root,
        MAX_ROWS_PER_FILE,
        2,
    )
    .await?;

    set_scope(&db.client, &focus_tenant).await?;

    let recent =
        query_recent_security_events(&db.client, &relation, &focus_tenant, &focus_account).await?;
    let _ = recent;

    let regulator =
        query_regulator_history(&db.client, &relation, &focus_tenant, &focus_account).await?;
    assert!(
        !regulator.is_empty(),
        "regulator export must return long-range history"
    );

    assert_merge_scan_uses_cold(
        &db.client,
        &relation,
        &format!(
            "tenant_id = '{focus_tenant}' AND account_id = '{focus_account}' AND created_at < timestamptz '2023-01-01'"
        ),
        1,
    )
    .await?;

    let segments = support::load_cold_segments(&db.client, &relation).await?;
    assert!(segments.iter().all(|s| s.byte_size > 0 && s.row_count > 0));
    let manifests = support::load_manifests(&db.client, &relation).await?;
    assert!(!manifests.is_empty());
    assert!(manifests.iter().all(|m| {
        m.sync_state == "in_sync" || m.sync_state == "pending_write" || m.sync_state == "pending"
    }));

    for &id in &overlay_ids {
        assert_eq!(
            support::visible_pk_count(&db.client, &relation, id).await?,
            0
        );
    }

    Ok(())
}

async fn create_account_events_table(
    client: &tokio_postgres::Client,
    relation: &str,
    table_name: &str,
) -> Result<()> {
    client
        .batch_execute(&format!(
            r#"
            CREATE TABLE {relation} (
              tenant_id text NOT NULL,
              account_id text NOT NULL,
              id bigint PRIMARY KEY,
              actor_id text NOT NULL,
              event_type text NOT NULL,
              before_state jsonb NOT NULL,
              after_state jsonb NOT NULL,
              ip text,
              created_at timestamptz NOT NULL
            );
            CREATE INDEX {table_name}_tenant_account_created_idx
              ON {relation} (tenant_id, account_id, created_at);
            CREATE INDEX {table_name}_tenant_event_created_idx
              ON {relation} (tenant_id, event_type, created_at);
            "#
        ))
        .await?;
    Ok(())
}

async fn seed_audit_events_parallel(
    target: &support::e2e::PgTarget,
    relation: &str,
    config: &ExampleConfig,
) -> Result<()> {
    let relation = relation.to_string();
    let rows_per_scope = config.rows_per_scope();
    let scopes = config.scopes;
    let clients = config.clients;
    let progress = InsertProgress::new("seed audit events", config.rows);
    run_parallel_clients(target, clients, {
        let progress = progress.clone();
        move |client_idx, client| {
            let relation = relation.clone();
            let progress = progress.clone();
            async move {
                let scopes_per_client = (scopes + clients - 1) / clients;
                let scope_start = client_idx * scopes_per_client;
                let scope_end = (scope_start + scopes_per_client).min(scopes);
                for scope_idx in scope_start..scope_end {
                    let tenant = format!("tenant-{scope_idx:04}");
                    let account = format!("account-{scope_idx:04}");
                    let actor = format!("actor-{:02}", scope_idx % 10);
                    let base_id = scope_idx as i64 * rows_per_scope;
                    set_scope(&client, &tenant).await?;
                    client
                        .batch_execute(&format!(
                            r#"
                        INSERT INTO {relation} (
                          tenant_id, account_id, id, actor_id, event_type,
                          before_state, after_state, ip, created_at
                        )
                        SELECT
                          '{tenant}',
                          '{account}',
                          {base_id} + gs,
                          '{actor}',
                          CASE
                            WHEN gs % 9 = 0 THEN 'login'
                            WHEN gs % 9 = 1 THEN 'password_change'
                            ELSE 'profile_update'
                          END,
                          jsonb_build_object('status', 'before-' || gs),
                          jsonb_build_object('status', 'after-' || gs),
                          '192.168.' || (gs % 255) || '.1',
                          timestamptz '2021-01-01' + ((gs % 1825) || ' days')::interval
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

async fn concurrent_audit_bursts(
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
    let progress = InsertProgress::new(format!("audit burst wave {wave}"), total_rows);
    run_parallel_clients(target, clients, {
        let progress = progress.clone();
        move |client_idx, client| {
            let relation = relation.clone();
            let progress = progress.clone();
            async move {
                let scopes_per_client = (scopes + clients - 1) / clients;
                let scope_start = client_idx * scopes_per_client;
                let scope_end = (scope_start + scopes_per_client).min(scopes);
                for scope_idx in scope_start..scope_end {
                    let tenant = format!("tenant-{scope_idx:04}");
                    let account = format!("account-{scope_idx:04}");
                    let base_id = start_id + scope_idx as i64 * rows_per_scope;
                    set_scope(&client, &tenant).await?;
                    client
                        .batch_execute(&format!(
                            r#"
                        INSERT INTO {relation} (
                          tenant_id, account_id, id, actor_id, event_type,
                          before_state, after_state, ip, created_at
                        )
                        SELECT
                          '{tenant}', '{account}', {base_id} + gs, 'actor-burst',
                          'risk_review',
                          jsonb_build_object('wave', {wave}),
                          jsonb_build_object('wave', {wave}, 'gs', gs),
                          '10.0.0.1', now()
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
            let tenant = format!("tenant-{client_idx:04}");
            let base = client_idx as i64 * rows_per_scope;
            set_scope(&client, &tenant).await?;
            // Audit tables are append-heavy; still exercise hot UPDATE/DELETE paths.
            let _ = client
                .execute(
                    &format!(
                        "UPDATE {relation}
                         SET after_state = after_state || jsonb_build_object('note', 'reviewed')
                         WHERE id = $1"
                    ),
                    &[&(base + rows_per_scope)],
                )
                .await;
            let _ = client
                .execute(
                    &format!("DELETE FROM {relation} WHERE id = $1"),
                    &[&(base + rows_per_scope - 4)],
                )
                .await;
            Ok(())
        }
    })
    .await
}

async fn query_recent_security_events(
    client: &tokio_postgres::Client,
    relation: &str,
    tenant: &str,
    account: &str,
) -> Result<Vec<String>> {
    set_scope(client, tenant).await?;
    let rows = client
        .query(
            &format!(
                r#"
                SELECT event_type
                FROM {relation}
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND created_at > now() - interval '24 hours'
                ORDER BY created_at DESC
                LIMIT 100
                "#
            ),
            &[&tenant, &account],
        )
        .await?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

async fn query_regulator_history(
    client: &tokio_postgres::Client,
    relation: &str,
    tenant: &str,
    account: &str,
) -> Result<Vec<String>> {
    set_scope(client, tenant).await?;
    let rows = client
        .query(
            &format!(
                r#"
                SELECT event_type
                FROM {relation}
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND created_at BETWEEN timestamptz '2021-01-01' AND timestamptz '2026-01-01'
                ORDER BY created_at
                LIMIT 500
                "#
            ),
            &[&tenant, &account],
        )
        .await?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}
