//! Parallel soak workers: writers, history readers, flush rider, and pack workers.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tokio::task::JoinHandle;
use tokio_postgres::Client;

use crate::config::StressConfig;
use crate::control::SoakControl;
use crate::e2e;
use crate::metrics::{Metrics, OP_COLD_UPDATE, OP_HISTORY, OP_INSERT, OP_JOIN};
use crate::schema::{fat_blob, fat_payload, StressSchema};
use crate::support::{flush_table, force_flush_table, log_always, set_scope, wait_for_jobs};

/// Shared mutable id allocator for inserts.
#[derive(Debug, Default)]
pub struct IdSeq {
    next: AtomicI64,
}

impl IdSeq {
    #[must_use]
    pub fn new(start: i64) -> Self {
        Self {
            next: AtomicI64::new(start),
        }
    }

    pub fn next(&self) -> i64 {
        self.next.fetch_add(1, Ordering::Relaxed)
    }

    pub fn current(&self) -> i64 {
        self.next.load(Ordering::Relaxed)
    }

    pub fn set(&self, value: i64) {
        self.next.store(value, Ordering::Relaxed);
    }
}

/// Seeds wide messages (and sibling rows when multi_table) then force-flushes.
///
/// # Errors
///
/// Returns an error when seed SQL or flush fails.
pub async fn seed_history(
    client: &Client,
    schema: &StressSchema,
    config: &StressConfig,
    ids: &IdSeq,
) -> Result<i64> {
    log_always(format!(
        "seed: ~{} rows across {} tenants × {} conversations",
        config.seed_total_rows(),
        config.tenants,
        config.conversations_per_tenant
    ));

    // Message ids occupy 1..seed_total; sibling tables use a high band so
    // cold_dml workers can target real message primary keys.
    let sibling_base = 1_000_000_000_i64;
    let mut sibling_seq = 0_i64;
    let mut seeded = 0_i64;
    for t in 0..config.tenants {
        let tenant = config.tenant_id(t);
        set_scope(client, &tenant).await?;
        if let Some(conversations) = &schema.conversations {
            for c in 0..config.conversations_per_tenant {
                let conv = config.conversation_id(t, c);
                sibling_seq += 1;
                let id = sibling_base + sibling_seq;
                let title = format!("chat-{conv}");
                client
                    .execute(
                        &format!(
                            "INSERT INTO {conversations} \
                             (id, tenant_id, conversation_id, title, updated_at, version) \
                             VALUES ($1,$2,$3,$4, now(), 1)"
                        ),
                        &[&id, &tenant, &conv, &title],
                    )
                    .await?;
            }
        }

        for c in 0..config.conversations_per_tenant {
            let conv = config.conversation_id(t, c);
            for r in 0..config.seed_rows_per_conversation {
                let id = ids.next();
                let payload = fat_payload(config.payload_bytes, id as u64);
                let blob = fat_blob(config.bytea_bytes, (id % 255) as u8);
                let body = format!("seed-{t}-{c}-{r}");
                let sender = format!("user-{}", r % 7);
                // Spread created_at into the past so history queries hit cold later.
                let age_secs = (config.seed_rows_per_conversation - r) * 60;
                client
                    .execute(
                        &format!(
                            "INSERT INTO {} \
                             (id, tenant_id, conversation_id, sender_id, body, payload, blob, \
                              created_at, updated_at, version, flags, status) \
                             VALUES ($1,$2,$3,$4,$5,$6::text::jsonb,$7, \
                                     now() - ($8::text || ' seconds')::interval, \
                                     now() - ($8::text || ' seconds')::interval, \
                                     1, 0, 'active')",
                            schema.messages
                        ),
                        &[
                            &id,
                            &tenant,
                            &conv,
                            &sender,
                            &body,
                            &payload,
                            &blob,
                            &age_secs.to_string(),
                        ],
                    )
                    .await
                    .with_context(|| format!("seed insert id={id}"))?;
                seeded += 1;

                if let Some(receipts) = &schema.receipts {
                    sibling_seq += 1;
                    let rid = sibling_base + sibling_seq;
                    let reader = format!("reader-{}", r % 5);
                    client
                        .execute(
                            &format!(
                                "INSERT INTO {receipts} \
                                 (id, tenant_id, message_id, reader_id, read_at) \
                                 VALUES ($1,$2,$3,$4, now())"
                            ),
                            &[&rid, &tenant, &id, &reader],
                        )
                        .await?;
                }
            }
        }
    }
    // Keep soak inserts above the sibling band so PKs never collide.
    ids.set(sibling_base + sibling_seq + 1);

    e2e::fence_selected_mirror(client).await?;
    // Flush all scopes: clear session user so the job is not pinned to one tenant.
    client.batch_execute("RESET koldstore.user_id").await?;
    for relation in schema.managed_relations() {
        let hot = e2e::hot_row_count(client, relation).await.unwrap_or(-1);
        let logical = e2e::row_count(client, relation).await.unwrap_or(-1);
        log_always(format!(
            "pre-flush {relation}: hot_rows={hot} logical_rows={logical}"
        ));
        let flushed = force_flush_table(client, relation).await?;
        log_always(format!("seed force-flush {relation}: {flushed} rows"));
    }
    // Second wave so more small files exist.
    for relation in schema.managed_relations() {
        let flushed = force_flush_table(client, relation).await?;
        log_always(format!("seed force-flush-2 {relation}: {flushed} rows"));
    }

    Ok(seeded)
}

/// Spawns all soak workers; caller sets `control.request_stop()` when done.
pub fn spawn_soak_workers(
    target: e2e::PgTarget,
    schema: StressSchema,
    config: StressConfig,
    ids: Arc<IdSeq>,
    metrics: Arc<Metrics>,
    control: Arc<SoakControl>,
    seed_max_id: i64,
) -> Vec<JoinHandle<Result<()>>> {
    let mut handles = Vec::new();

    for worker_id in 0..config.clients {
        let target = target.clone();
        let schema = schema.clone();
        let config = config.clone();
        let ids = Arc::clone(&ids);
        let metrics = Arc::clone(&metrics);
        let control = Arc::clone(&control);
        handles.push(tokio::spawn(async move {
            writer_loop(target, schema, config, ids, metrics, control, worker_id).await
        }));
    }

    for worker_id in 0..config.history_clients {
        let target = target.clone();
        let schema = schema.clone();
        let config = config.clone();
        let metrics = Arc::clone(&metrics);
        let control = Arc::clone(&control);
        handles.push(tokio::spawn(async move {
            history_loop(target, schema, config, metrics, control, worker_id).await
        }));
    }

    {
        let target = target.clone();
        let schema = schema.clone();
        let config = config.clone();
        let metrics = Arc::clone(&metrics);
        let control = Arc::clone(&control);
        handles.push(tokio::spawn(async move {
            flush_loop(target, schema, config, metrics, control).await
        }));
    }

    if config.packs.cold_dml() {
        for _worker_id in 0..config.cold_dml_clients {
            let target = target.clone();
            let schema = schema.clone();
            let config = config.clone();
            let metrics = Arc::clone(&metrics);
            let control = Arc::clone(&control);
            handles.push(tokio::spawn(async move {
                cold_dml_loop(target, schema, config, metrics, control, seed_max_id).await
            }));
        }
    }

    if config.packs.multi_table() {
        for worker_id in 0..config.multi_table_clients {
            let target = target.clone();
            let schema = schema.clone();
            let config = config.clone();
            let ids = Arc::clone(&ids);
            let metrics = Arc::clone(&metrics);
            let control = Arc::clone(&control);
            handles.push(tokio::spawn(async move {
                multi_table_loop(target, schema, config, ids, metrics, control, worker_id).await
            }));
        }
    }

    if config.packs.joins() {
        for worker_id in 0..config.join_clients {
            let target = target.clone();
            let schema = schema.clone();
            let config = config.clone();
            let metrics = Arc::clone(&metrics);
            let control = Arc::clone(&control);
            handles.push(tokio::spawn(async move {
                join_loop(target, schema, config, metrics, control, worker_id).await
            }));
        }
    }

    handles
}

async fn writer_loop(
    target: e2e::PgTarget,
    schema: StressSchema,
    config: StressConfig,
    ids: Arc<IdSeq>,
    metrics: Arc<Metrics>,
    control: Arc<SoakControl>,
    worker_id: usize,
) -> Result<()> {
    let client = e2e::connect(&target).await?;
    let mut seq = 0u64;
    while !control.should_stop() {
        let tenant_idx = (worker_id + seq as usize) % config.tenants;
        let conv_idx = seq as usize % config.conversations_per_tenant;
        let tenant = config.tenant_id(tenant_idx);
        let conv = config.conversation_id(tenant_idx, conv_idx);
        set_scope(&client, &tenant).await?;

        if seq % 5 == 0 {
            // Hot update on a recent id band when possible.
            let id = ids.current().saturating_sub(1).max(1);
            let started = Instant::now();
            let _ = client
                .execute(
                    &format!(
                        "UPDATE {} SET version = version + 1, updated_at = now(), \
                         body = body || '-u' WHERE id = $1 AND tenant_id = $2",
                        schema.messages
                    ),
                    &[&id, &tenant],
                )
                .await;
            metrics.record("hot_update", started.elapsed().as_micros() as u64);
            metrics.updates.fetch_add(1, Ordering::Relaxed);
        } else {
            let id = ids.next();
            let payload = fat_payload(config.payload_bytes, id as u64);
            let blob = fat_blob(config.bytea_bytes, (worker_id % 255) as u8);
            let body = format!("msg-{worker_id}-{seq}");
            let sender = format!("writer-{worker_id}");
            let started = Instant::now();
            match client
                .execute(
                    &format!(
                        "INSERT INTO {} \
                         (id, tenant_id, conversation_id, sender_id, body, payload, blob, \
                          created_at, updated_at, version, flags, status) \
                         VALUES ($1,$2,$3,$4,$5,$6::text::jsonb,$7, now(), now(), 1, 0, 'active')",
                        schema.messages
                    ),
                    &[&id, &tenant, &conv, &sender, &body, &payload, &blob],
                )
                .await
            {
                Ok(_) => {
                    metrics.record(OP_INSERT, started.elapsed().as_micros() as u64);
                    metrics.inserts.fetch_add(1, Ordering::Relaxed);
                }
                Err(err) => {
                    metrics.worker_errors.fetch_add(1, Ordering::Relaxed);
                    if control.note_db_error(&format!("writer {worker_id} insert"), &err) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }
        seq += 1;
        if !config.writer_delay.is_zero() {
            tokio::time::sleep(config.writer_delay).await;
        }
    }
    Ok(())
}

async fn history_loop(
    target: e2e::PgTarget,
    schema: StressSchema,
    config: StressConfig,
    metrics: Arc<Metrics>,
    control: Arc<SoakControl>,
    worker_id: usize,
) -> Result<()> {
    let client = e2e::connect(&target).await?;
    let mut seq = 0usize;
    while !control.should_stop() {
        let tenant_idx = (worker_id + seq) % config.tenants;
        let conv_idx = seq % config.conversations_per_tenant;
        let tenant = config.tenant_id(tenant_idx);
        let conv = config.conversation_id(tenant_idx, conv_idx);
        set_scope(&client, &tenant).await?;
        let started = Instant::now();
        match client
            .query(
                &format!(
                    "SELECT id, body, payload, blob, version, created_at FROM {} \
                     WHERE tenant_id = $1 AND conversation_id = $2 \
                     ORDER BY created_at DESC LIMIT 50",
                    schema.messages
                ),
                &[&tenant, &conv],
            )
            .await
        {
            Ok(_rows) => {
                metrics.record(OP_HISTORY, started.elapsed().as_micros() as u64);
                metrics.history_selects.fetch_add(1, Ordering::Relaxed);
            }
            Err(err) => {
                metrics.worker_errors.fetch_add(1, Ordering::Relaxed);
                if control.note_db_error(&format!("history {worker_id}"), &err) {
                    break;
                }
            }
        }
        seq += 1;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Ok(())
}

async fn flush_loop(
    target: e2e::PgTarget,
    schema: StressSchema,
    config: StressConfig,
    metrics: Arc<Metrics>,
    control: Arc<SoakControl>,
) -> Result<()> {
    let client = e2e::connect(&target).await?;
    let relations: Vec<String> = schema
        .managed_relations()
        .into_iter()
        .map(str::to_string)
        .collect();
    let mut i = 0usize;
    while !control.should_stop() {
        let relation = &relations[i % relations.len()];
        match flush_table(&client, relation).await {
            Ok(_) => {
                metrics.flushes.fetch_add(1, Ordering::Relaxed);
                let _ = wait_for_jobs(&client, relation).await;
            }
            Err(err) => {
                metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                if control.note_db_error(&format!("flush rider on {relation}"), &err) {
                    break;
                }
            }
        }
        i += 1;
        tokio::time::sleep(config.flush_interval).await;
    }
    Ok(())
}

async fn cold_dml_loop(
    target: e2e::PgTarget,
    schema: StressSchema,
    config: StressConfig,
    metrics: Arc<Metrics>,
    control: Arc<SoakControl>,
    seed_max_id: i64,
) -> Result<()> {
    let client = e2e::connect(&target).await?;
    let mut seq = 0u64;
    let max_id = seed_max_id.max(1);
    let per_tenant = (config.conversations_per_tenant as i64)
        .saturating_mul(config.seed_rows_per_conversation)
        .max(1);
    while !control.should_stop() {
        let id = ((seq % max_id as u64) + 1) as i64;
        let tenant_idx = ((id - 1) / per_tenant) as usize % config.tenants;
        let tenant = config.tenant_id(tenant_idx);
        set_scope(&client, &tenant).await?;
        if seq % 4 == 0 {
            let started = Instant::now();
            match client
                .execute(
                    &format!(
                        "DELETE FROM {} WHERE id = $1 AND tenant_id = $2",
                        schema.messages
                    ),
                    &[&id, &tenant],
                )
                .await
            {
                Ok(_) => {
                    metrics.deletes.fetch_add(1, Ordering::Relaxed);
                    metrics.cold_deletes.fetch_add(1, Ordering::Relaxed);
                    metrics.record("cold_delete", started.elapsed().as_micros() as u64);
                }
                Err(err) => {
                    metrics.worker_errors.fetch_add(1, Ordering::Relaxed);
                    if control.note_db_error("cold_dml delete", &err) {
                        break;
                    }
                }
            }
        } else {
            let started = Instant::now();
            match client
                .execute(
                    &format!(
                        "UPDATE {} SET version = version + 1, updated_at = now(), \
                         status = 'edited' WHERE id = $1 AND tenant_id = $2",
                        schema.messages
                    ),
                    &[&id, &tenant],
                )
                .await
            {
                Ok(_) => {
                    metrics.record(OP_COLD_UPDATE, started.elapsed().as_micros() as u64);
                    metrics.cold_updates.fetch_add(1, Ordering::Relaxed);
                }
                Err(err) => {
                    metrics.worker_errors.fetch_add(1, Ordering::Relaxed);
                    if control.note_db_error("cold_dml update", &err) {
                        break;
                    }
                }
            }
        }
        seq += 1;
        tokio::time::sleep(Duration::from_millis(4)).await;
    }
    Ok(())
}

async fn multi_table_loop(
    target: e2e::PgTarget,
    schema: StressSchema,
    config: StressConfig,
    ids: Arc<IdSeq>,
    metrics: Arc<Metrics>,
    control: Arc<SoakControl>,
    worker_id: usize,
) -> Result<()> {
    let Some(conversations) = schema.conversations.clone() else {
        return Ok(());
    };
    let Some(receipts) = schema.receipts.clone() else {
        return Ok(());
    };
    let client = e2e::connect(&target).await?;
    let mut seq = 0u64;
    while !control.should_stop() {
        let tenant_idx = (worker_id + seq as usize) % config.tenants;
        let conv_idx = seq as usize % config.conversations_per_tenant;
        let tenant = config.tenant_id(tenant_idx);
        let conv = config.conversation_id(tenant_idx, conv_idx);
        set_scope(&client, &tenant).await?;

        if seq % 2 == 0 {
            let _ = client
                .execute(
                    &format!(
                        "UPDATE {conversations} SET version = version + 1, updated_at = now(), \
                         title = title || '!' WHERE tenant_id = $1 AND conversation_id = $2"
                    ),
                    &[&tenant, &conv],
                )
                .await;
            metrics.updates.fetch_add(1, Ordering::Relaxed);
        } else {
            let id = ids.next();
            let message_id = ids.current().saturating_sub(2).max(1);
            let reader = format!("reader-{worker_id}");
            let _ = client
                .execute(
                    &format!(
                        "INSERT INTO {receipts} (id, tenant_id, message_id, reader_id, read_at) \
                         VALUES ($1,$2,$3,$4, now())"
                    ),
                    &[&id, &tenant, &message_id, &reader],
                )
                .await;
            metrics.inserts.fetch_add(1, Ordering::Relaxed);
        }
        seq += 1;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(())
}

async fn join_loop(
    target: e2e::PgTarget,
    schema: StressSchema,
    config: StressConfig,
    metrics: Arc<Metrics>,
    control: Arc<SoakControl>,
    worker_id: usize,
) -> Result<()> {
    let Some(conversations) = schema.conversations.clone() else {
        return Ok(());
    };
    let client = e2e::connect(&target).await?;
    let mut seq = 0usize;
    while !control.should_stop() {
        let tenant_idx = (worker_id + seq) % config.tenants;
        let conv_idx = seq % config.conversations_per_tenant;
        let tenant = config.tenant_id(tenant_idx);
        let conv = config.conversation_id(tenant_idx, conv_idx);
        set_scope(&client, &tenant).await?;
        let started = Instant::now();
        match client
            .query(
                &format!(
                    "SELECT m.id, c.title, m.body, m.created_at \
                     FROM {} m \
                     INNER JOIN {conversations} c \
                       ON c.tenant_id = m.tenant_id \
                      AND c.conversation_id = m.conversation_id \
                     WHERE m.tenant_id = $1 \
                       AND m.conversation_id = $2 \
                     LIMIT 30",
                    schema.messages
                ),
                &[&tenant, &conv],
            )
            .await
        {
            Ok(rows) => {
                metrics.record(OP_JOIN, started.elapsed().as_micros() as u64);
                metrics.join_selects.fetch_add(1, Ordering::Relaxed);
                if rows.is_empty() {
                    // Not fatal during early soak; seed should make this rare.
                }
            }
            Err(err) => {
                metrics.worker_errors.fetch_add(1, Ordering::Relaxed);
                if control.note_db_error(&format!("join {worker_id}"), &err) {
                    break;
                }
            }
        }
        seq += 1;
        tokio::time::sleep(Duration::from_millis(8)).await;
    }
    Ok(())
}

/// Spot-checks that a seeded conversation still returns rows after soak.
///
/// # Errors
///
/// Returns an error when visibility is wrong or jobs are stuck.
pub async fn assert_post_soak(
    client: &Client,
    schema: &StressSchema,
    config: &StressConfig,
) -> Result<()> {
    e2e::fence_selected_mirror(client).await?;
    for relation in schema.managed_relations() {
        e2e::assert_no_active_jobs(client, relation).await?;
    }

    let tenant = config.tenant_id(0);
    let conv = config.conversation_id(0, 0);
    set_scope(client, &tenant).await?;
    let rows = client
        .query(
            &format!(
                "SELECT count(*)::bigint FROM {} \
                 WHERE tenant_id = $1 AND conversation_id = $2",
                schema.messages
            ),
            &[&tenant, &conv],
        )
        .await?;
    let count: i64 = rows[0].get(0);
    anyhow::ensure!(
        count > 0,
        "spot-check: expected visible messages for {tenant}/{conv}, got 0"
    );

    if config.packs.joins() {
        let conversations = schema
            .conversations
            .as_ref()
            .context("joins assert needs conversations")?;
        let joined = client
            .query(
                &format!(
                    "SELECT count(*)::bigint \
                     FROM {} m \
                     INNER JOIN {conversations} c \
                       ON c.tenant_id = m.tenant_id \
                      AND c.conversation_id = m.conversation_id \
                     WHERE m.tenant_id = $1 \
                       AND m.conversation_id = $2",
                    schema.messages
                ),
                &[&tenant, &conv],
            )
            .await?;
        let jcount: i64 = joined[0].get(0);
        anyhow::ensure!(
            jcount > 0,
            "join spot-check returned 0 rows for {tenant}/{conv}"
        );
    }

    Ok(())
}
