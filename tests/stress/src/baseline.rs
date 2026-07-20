//! Same-run quiet baseline for latency gates.

use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use tokio_postgres::Client;

use crate::config::StressConfig;
use crate::metrics::{
    Metrics, MetricsSnapshot, OP_COLD_UPDATE, OP_HISTORY, OP_INSERT, OP_JOIN,
};
use crate::schema::{fat_blob, fat_payload, StressSchema};
use crate::support::{log_always, set_scope};

/// Runs low-concurrency probes and returns a latency snapshot.
///
/// # Errors
///
/// Returns an error when SQL probes fail.
pub async fn run_baseline(
    client: &Client,
    schema: &StressSchema,
    config: &StressConfig,
    metrics: &Arc<Metrics>,
    next_id: &mut i64,
    seed_max_id: i64,
) -> Result<MetricsSnapshot> {
    log_always(format!(
        "baseline: {} samples/op (quiet)",
        config.baseline_samples
    ));
    metrics.clear_latency_samples();

    let tenant = config.tenant_id(0);
    let conversation = config.conversation_id(0, 0);
    set_scope(client, &tenant).await?;

    for i in 0..config.baseline_samples {
        let id = *next_id;
        *next_id += 1;
        let payload = fat_payload(config.payload_bytes, i as u64);
        let blob = fat_blob(config.bytea_bytes, (i % 255) as u8);
        let body = format!("baseline-{i}");
        let sender = "baseline-sender";
        let started = Instant::now();
        client
            .execute(
                &format!(
                    "INSERT INTO {} \
                     (id, tenant_id, conversation_id, sender_id, body, payload, blob, \
                      created_at, updated_at, version, flags, status) \
                     VALUES ($1,$2,$3,$4,$5,$6::text::jsonb,$7, now(), now(), 1, 0, 'active')",
                    schema.messages
                ),
                &[
                    &id,
                    &tenant,
                    &conversation,
                    &sender,
                    &body,
                    &payload,
                    &blob,
                ],
            )
            .await
            .context("baseline insert")?;
        metrics.record(OP_INSERT, started.elapsed().as_micros() as u64);
        metrics.inserts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    for _ in 0..config.baseline_samples {
        let started = Instant::now();
        let _rows = client
            .query(
                &format!(
                    "SELECT id, body, payload, version FROM {} \
                     WHERE tenant_id = $1 AND conversation_id = $2 \
                     ORDER BY created_at DESC LIMIT 50",
                    schema.messages
                ),
                &[&tenant, &conversation],
            )
            .await
            .context("baseline history select")?;
        metrics.record(OP_HISTORY, started.elapsed().as_micros() as u64);
        metrics
            .history_selects
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    if config.packs.cold_dml() && seed_max_id > 0 {
        let cold_id = 1_i64;
        for i in 0..config.baseline_samples {
            let started = Instant::now();
            client
                .execute(
                    &format!(
                        "UPDATE {} SET version = version + 1, updated_at = now(), \
                         status = $2 WHERE id = $1",
                        schema.messages
                    ),
                    &[&cold_id, &format!("baseline-edit-{i}")],
                )
                .await
                .context("baseline cold update")?;
            metrics.record(OP_COLD_UPDATE, started.elapsed().as_micros() as u64);
            metrics
                .cold_updates
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    if config.packs.joins() {
        let conversations = schema
            .conversations
            .as_ref()
            .context("joins baseline requires conversations table")?;
        for _ in 0..config.baseline_samples {
            let started = Instant::now();
            let _rows = client
                .query(
                    &format!(
                        "SELECT m.id, c.title, m.body \
                         FROM {} m \
                         INNER JOIN {} c \
                           ON c.tenant_id = m.tenant_id \
                          AND c.conversation_id = m.conversation_id \
                         WHERE m.tenant_id = $1 \
                           AND m.conversation_id = $2 \
                         LIMIT 30",
                        schema.messages, conversations
                    ),
                    &[&tenant, &conversation],
                )
                .await
                .context("baseline join select")?;
            metrics.record(OP_JOIN, started.elapsed().as_micros() as u64);
            metrics
                .join_selects
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    let snap = metrics.snapshot();
    for (op, pct) in &snap.ops {
        log_always(format!(
            "baseline {op}: n={} p50={}us p95={}us p99={}us max={}us",
            pct.count, pct.p50_us, pct.p95_us, pct.p99_us, pct.max_us
        ));
    }
    Ok(snap)
}

/// Fails when soak p95 exceeds baseline p95 × multiplier (with absolute floor).
///
/// # Errors
///
/// Returns an error describing each violated op gate.
pub fn assert_latency_gates(
    baseline: &MetricsSnapshot,
    soak: &MetricsSnapshot,
    multiplier: f64,
    absolute_floor_us: u64,
) -> Result<()> {
    let mut failures = Vec::new();
    for (op, base) in &baseline.ops {
        if base.count == 0 {
            continue;
        }
        let Some(soak_pct) = soak.ops.get(op) else {
            failures.push(format!("soak missing latency samples for op {op}"));
            continue;
        };
        if soak_pct.count == 0 {
            failures.push(format!("soak has zero samples for op {op}"));
            continue;
        }
        let limit = ((base.p95_us as f64) * multiplier)
            .ceil()
            .max(absolute_floor_us as f64) as u64;
        if soak_pct.p95_us > limit {
            failures.push(format!(
                "{op}: soak p95 {}us > limit {}us (baseline p95 {}us × {multiplier}, floor {}us)",
                soak_pct.p95_us, limit, base.p95_us, absolute_floor_us
            ));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        anyhow::bail!("latency gates failed:\n  - {}", failures.join("\n  - "))
    }
}
