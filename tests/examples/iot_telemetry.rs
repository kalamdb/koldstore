//! IoT fleet telemetry example.
//!
//! Covers parallel device writers, multi-wave flush, late arrivals, small Parquet
//! files + manifests, concurrent DML, and cold-then-delete overlays.

#[path = "support/mod.rs"]
mod support;

use anyhow::{Context, Result};
use support::{
    assert_cold_then_delete_overlay, assert_indexes_exist, assert_merge_scan_uses_cold,
    assert_multi_tenant_visibility, assert_parquet_and_manifest, flush_waves, force_flush_table,
    log_always, log_scenario_start, log_step, manage_user_scoped_with_policy, run_parallel_clients,
    set_scope, with_example_timeout, ExampleConfig, FlushCtx, InsertProgress,
};

const MIN_FLUSH_ROWS: i64 = 400;
const MAX_ROWS_PER_FILE: i64 = 1_000;

#[tokio::test]
async fn iot_telemetry_parallel_devices_late_arrivals_and_monthly_report() -> Result<()> {
    with_example_timeout(
        "iot_telemetry",
        iot_telemetry_parallel_devices_late_arrivals_and_monthly_report_inner(),
    )
    .await
}

async fn iot_telemetry_parallel_devices_late_arrivals_and_monthly_report_inner() -> Result<()> {
    support::e2e::require_pgrx_server().await?;
    let mut config = ExampleConfig::from_env();
    if std::env::var("KOLDSTORE_EXAMPLE_ROWS").is_err() {
        config.rows = 20_000;
    }

    let target = support::e2e::local_pg_matrix()
        .into_iter()
        .next()
        .context("no local pg target configured")?;

    let db = support::e2e::TestDb::start(target, "iot_telemetry").await?;
    let table_name = "telemetry";
    let relation = db.relation(table_name);
    log_scenario_start("iot_telemetry", &relation, &db.storage_root, config);
    let flush = |label: &'static str| FlushCtx {
        label,
        storage_root: &db.storage_root,
    };

    {
        let _step = log_step("create telemetry table + indexes");
        create_telemetry_table(&db.client, &relation, table_name).await?;
        assert_indexes_exist(
            &db.client,
            &db.schema,
            &[
                &format!("{table_name}_tenant_device_ts_idx"),
                &format!("{table_name}_tenant_event_ts_idx"),
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
            "ts",
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
        seed_telemetry_parallel(&db.target, &relation, &config).await?;
        support::wait_for_jobs(&db.client, &relation).await?;
    }

    let focus_tenant = config.scope_id("tenant", 1);
    set_scope(&db.client, &focus_tenant).await?;
    {
        let _step = log_step("insert late arrivals");
        insert_late_arrivals(&db.client, &relation, &focus_tenant, "truck-1", 800).await?;
        log_always("late arrivals: inserted 800 rows for truck-1");
    }

    let mut waves = flush_waves(&db.client, &relation, 1, Some(flush("seed"))).await?;
    {
        let _step = log_step("device burst + flush waves");
        concurrent_device_bursts(
            &db.target,
            &relation,
            &config,
            config.rows + 10_000,
            MIN_FLUSH_ROWS,
        )
        .await?;
        waves.extend(flush_waves(&db.client, &relation, 2, Some(flush("burst"))).await?);
    }
    {
        let _step = log_step("concurrent hot UPDATE/DELETE");
        concurrent_hot_dml(&db.target, &relation, &config).await?;
    }

    // Verify tenant isolation before the overlay path adds another force flush.
    let tenant_a = config.scope_id("tenant", 0);
    let tenant_b = config.scope_id("tenant", 1);
    assert_multi_tenant_visibility(&db.client, &relation, "tenant_id", &[&tenant_a, &tenant_b])
        .await?;

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
                "INSERT INTO {relation} (tenant_id, device_id, id, ts, lat, lon, speed, temperature, battery, event_type) \
                 VALUES ('{tenant}', 'truck-overlay', {id}, now(), 1.0, 2.0, 3.0, 4.0, 5.0, 'overlay') \
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
        "expected multi-wave IoT flushes, got {waves:?}"
    );

    assert_parquet_and_manifest(
        &db.client,
        &relation,
        &db.storage_root,
        MAX_ROWS_PER_FILE,
        2,
    )
    .await?;

    let live = query_live_dashboard(&db.client, &relation, &focus_tenant, "truck-1").await?;
    let _ = live;

    let monthly = query_monthly_report(&db.client, &relation, &focus_tenant, "truck-1").await?;
    assert!(
        monthly.is_some(),
        "monthly aggregate should return avg/max even when late data is present"
    );

    assert_merge_scan_uses_cold(
        &db.client,
        &relation,
        &format!(
            "tenant_id = '{focus_tenant}' AND ts BETWEEN timestamptz '2026-01-01' AND timestamptz '2026-02-01'"
        ),
        1,
    )
    .await?;

    for &id in &overlay_ids {
        assert_eq!(
            support::visible_pk_count(&db.client, &relation, id).await?,
            0
        );
    }

    Ok(())
}

async fn create_telemetry_table(
    client: &tokio_postgres::Client,
    relation: &str,
    table_name: &str,
) -> Result<()> {
    client
        .batch_execute(&format!(
            r#"
            CREATE TABLE {relation} (
              tenant_id text NOT NULL,
              device_id text NOT NULL,
              id bigint PRIMARY KEY,
              ts timestamptz NOT NULL,
              lat double precision NOT NULL,
              lon double precision NOT NULL,
              speed double precision NOT NULL,
              temperature double precision NOT NULL,
              battery double precision NOT NULL,
              event_type text NOT NULL
            );
            CREATE INDEX {table_name}_tenant_device_ts_idx
              ON {relation} (tenant_id, device_id, ts DESC);
            CREATE INDEX {table_name}_tenant_event_ts_idx
              ON {relation} (tenant_id, event_type, ts DESC);
            "#
        ))
        .await?;
    Ok(())
}

async fn seed_telemetry_parallel(
    target: &support::e2e::PgTarget,
    relation: &str,
    config: &ExampleConfig,
) -> Result<()> {
    let relation = relation.to_string();
    let rows_per_scope = config.rows_per_scope();
    let scopes = config.scopes;
    let clients = config.clients;
    let progress = InsertProgress::new("seed telemetry", config.rows);
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
                    let device = format!("truck-{}", scope_idx % 20);
                    let base_id = scope_idx as i64 * rows_per_scope;
                    set_scope(&client, &tenant).await?;
                    client
                        .batch_execute(&format!(
                            r#"
                        INSERT INTO {relation} (
                          tenant_id, device_id, id, ts, lat, lon, speed,
                          temperature, battery, event_type
                        )
                        SELECT
                          '{tenant}',
                          '{device}',
                          {base_id} + gs,
                          timestamptz '2026-01-01' + ((gs % 120) || ' hours')::interval,
                          40.0 + (gs % 100) * 0.01,
                          -74.0 - (gs % 100) * 0.01,
                          (gs % 80)::double precision,
                          -5.0 + (gs % 40)::double precision,
                          100.0 - (gs % 30)::double precision,
                          CASE WHEN gs % 11 = 0 THEN 'alert' ELSE 'ping' END
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

async fn insert_late_arrivals(
    client: &tokio_postgres::Client,
    relation: &str,
    tenant: &str,
    device: &str,
    count: i64,
) -> Result<()> {
    set_scope(client, tenant).await?;
    client
        .batch_execute(&format!(
            r#"
            INSERT INTO {relation} (
              tenant_id, device_id, id, ts, lat, lon, speed,
              temperature, battery, event_type
            )
            SELECT
              '{tenant}',
              '{device}',
              900000000 + gs,
              timestamptz '2025-12-01' + ((gs % 72) || ' hours')::interval,
              41.0, -73.5, 55.0, 12.0, 88.0, 'late_upload'
            FROM generate_series(1, {count}) AS gs;
            "#
        ))
        .await?;
    Ok(())
}

async fn concurrent_device_bursts(
    target: &support::e2e::PgTarget,
    relation: &str,
    config: &ExampleConfig,
    start_id: i64,
    rows_per_scope: i64,
) -> Result<()> {
    let relation = relation.to_string();
    let scopes = config.scopes;
    let clients = config.clients;
    let total_rows = rows_per_scope * scopes as i64;
    let progress = InsertProgress::new("device burst", total_rows);
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
                    let device = format!("truck-{}", scope_idx % 20);
                    let base_id = start_id + scope_idx as i64 * rows_per_scope;
                    set_scope(&client, &tenant).await?;
                    client
                        .batch_execute(&format!(
                            r#"
                        INSERT INTO {relation} (
                          tenant_id, device_id, id, ts, lat, lon, speed,
                          temperature, battery, event_type
                        )
                        SELECT
                          '{tenant}', '{device}', {base_id} + gs, now(),
                          42.0, -71.0, 10.0, 20.0, 90.0, 'live'
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
            let _ = client
                .execute(
                    &format!("UPDATE {relation} SET event_type = 'reviewed' WHERE id = $1"),
                    &[&(base + rows_per_scope)],
                )
                .await;
            let _ = client
                .execute(
                    &format!("DELETE FROM {relation} WHERE id = $1"),
                    &[&(base + rows_per_scope - 3)],
                )
                .await;
            Ok(())
        }
    })
    .await
}

async fn query_live_dashboard(
    client: &tokio_postgres::Client,
    relation: &str,
    tenant: &str,
    device: &str,
) -> Result<Vec<f64>> {
    set_scope(client, tenant).await?;
    let rows = client
        .query(
            &format!(
                r#"
                SELECT speed
                FROM {relation}
                WHERE tenant_id = $1
                  AND device_id = $2
                  AND ts > now() - interval '10 minutes'
                ORDER BY ts DESC
                LIMIT 50
                "#
            ),
            &[&tenant, &device],
        )
        .await?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

async fn query_monthly_report(
    client: &tokio_postgres::Client,
    relation: &str,
    tenant: &str,
    device: &str,
) -> Result<Option<(f64, f64)>> {
    set_scope(client, tenant).await?;
    let row = client
        .query_opt(
            &format!(
                r#"
                SELECT avg(speed), max(temperature)
                FROM {relation}
                WHERE tenant_id = $1
                  AND device_id = $2
                  AND ts BETWEEN timestamptz '2026-01-01' AND timestamptz '2026-02-01'
                "#
            ),
            &[&tenant, &device],
        )
        .await?;
    Ok(row.map(|row| (row.get(0), row.get(1))))
}
