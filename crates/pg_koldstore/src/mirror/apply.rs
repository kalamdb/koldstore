//! Bounded, set-based application of committed `pgoutput` changes.
//!
//! Ordering (idempotent under crash):
//! 1. Peek available WAL (`pg_logical_slot_peek_binary_changes`)
//! 2. Write latest-state mirror rows (Insert/Delete: PK `ON CONFLICT` upsert;
//!    Update: keyed update)
//! 3. Record durable `applied_lsn` as the exact last decoded source commit
//!    end-LSN in `koldstore.async_mirror_state` (never the global insert LSN)
//! 4. On the **next** call, advance the slot to that LSN
//!
//! A crash between steps 2 and 4 may re-peek already-applied changes; replay is
//! safe because mirror writes are latest-state upserts. Batches are capped at
//! [`koldstore_wal_mirror::APPLY_BATCH_ROWS`] and cleared on every flush.
//!
//! Flush prune fences use [`apply_bounded`] with an explicit `upto_lsn`,
//! transaction skip boundary, and `acknowledge_durable_checkpoint = false` so
//! the still-uncommitted flush transaction cannot advance the slot.

use std::collections::{HashMap, HashSet};

use koldstore_catalog::{async_managed_relation, queries::plan_async_managed_relation_by_oid};
use koldstore_common::{format_pg_lsn, next_id_after, MirrorOperation};
use koldstore_wal_mirror::{
    budget_hit, decode_message, must_flush_before_push, parse_pk_bool, parse_pk_ints,
    pg_value_text, pk_identity, plan_async_mirror_batch_delete_existing,
    plan_async_mirror_batch_update, plan_async_mirror_batch_upsert, primary_key_json,
    resolve_row_budget, resolve_time_budget, PgOutputMessage, PgOutputRelation, PgOutputTuple,
    APPLY_BATCH_ROWS,
};
use pgrx::datum::DatumWithOid;
use serde_json::Value;

use super::lifecycle::{current_slot_name, PUBLICATION_NAME};

pub use koldstore_common::{AppliedWalBoundary, WalFenceLsn};
pub use koldstore_wal_mirror::{BoundedApplyOutcome, BoundedApplyRequest, PruneSeqFloor};

const DECODE_FETCH_ROWS: std::os::raw::c_long = 8_192;

/// Failpoint name: abort during async mirror apply (worker ERROR exit).
pub const ASYNC_MIRROR_APPLY_FAILPOINT: &str = "async_mirror_apply";
/// Failpoint name: abort after at least one mirror batch SPI write, before
/// `applied_lsn` is recorded — asserts one-txn-per-tick rollback.
pub const ASYNC_MIRROR_APPLY_AFTER_BATCH_FAILPOINT: &str = "async_mirror_apply_after_batch";

#[derive(Debug, Clone)]
struct OrderColumnConfig {
    name: String,
    type_oid: u32,
}

#[derive(Debug, Clone)]
struct ManagedRelation {
    table_oid: pgrx::pg_sys::Oid,
    mirror: String,
    primary_key: Vec<String>,
    order_column: Option<OrderColumnConfig>,
    /// Cached `format_type` spellings for each primary-key column.
    pk_type_names: Option<Vec<String>>,
    /// Cached upsert SQL for typed `unnest` binds (Insert when no order key,
    /// or Insert/Update with order key).
    upsert_sql: Option<String>,
    /// Cached direct-update plus insert-missing SQL for UPDATE batches.
    update_sql: Option<String>,
    /// Cached delete-existing SQL when order_key forbids inventing tombstones.
    delete_sql: Option<String>,
}

impl ManagedRelation {
    fn invalidate_plans(&mut self) {
        self.pk_type_names = None;
        self.upsert_sql = None;
        self.update_sql = None;
        self.delete_sql = None;
    }

    fn include_order_key(&self) -> bool {
        self.order_column.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BatchKey {
    relation_id: u32,
    operation: MirrorOperation,
}

#[derive(Debug)]
struct ApplyBatch {
    key: BatchKey,
    rows: Vec<Value>,
    seen: HashSet<String>,
}

impl ApplyBatch {
    fn new(key: BatchKey) -> Self {
        Self {
            key,
            rows: Vec::with_capacity(APPLY_BATCH_ROWS),
            seen: HashSet::with_capacity(APPLY_BATCH_ROWS),
        }
    }
}

/// Applies committed WAL under an explicit fence request.
///
/// Acquires the database slot lock for the current transaction, then applies.
/// Flush finalize should prefer [`try_lock_slot`] + [`apply_bounded_locked`] so
/// encode/upload never wait on a blocked slot lock.
///
/// Scheduling is deliberately not coupled to synchronous fence calls. Durable
/// WAL and supervisor generations own background progress; this function only
/// performs the requested apply work in its current backend.
///
/// # Errors
///
/// Returns an error for malformed protocol data, stale relation metadata,
/// missing primary-key values, or an SPI/apply failure.
pub fn apply_bounded(request: BoundedApplyRequest) -> Result<BoundedApplyOutcome, String> {
    super::lifecycle::lock_slot(unsafe { pgrx::pg_sys::MyDatabaseId }.to_u32())?;
    apply_bounded_locked(request)
}

/// Applies committed WAL while the caller already holds the slot lock.
///
/// # Errors
///
/// Returns an error for malformed protocol data, stale relation metadata,
/// missing primary-key values, or an SPI/apply failure.
pub fn apply_bounded_locked(request: BoundedApplyRequest) -> Result<BoundedApplyOutcome, String> {
    let slot = current_slot_name();
    let exists = pgrx::Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = $1)",
        &[DatumWithOid::from(slot.as_str())],
    )
    .map_err(|error| error.to_string())?
    .unwrap_or(false);
    if !exists {
        return Ok(BoundedApplyOutcome {
            row_changes: 0,
            last_applied: request.skip_through,
            budget_exhausted: false,
        });
    }

    // Peek/advance use nowait slot acquire. After terminate (or worker abort),
    // advisory locks can be released before ReplicationSlotRelease — wait out
    // that window under lock_slot before touching the slot.
    super::lifecycle::wait_until_slot_inactive(&slot)
        .map_err(|error| format!("wait slot inactive before apply: {error}"))?;

    let durable = read_durable_applied_lsn()?;
    let mut seq_watermark = read_durable_seq_high_watermark()?;
    if request.acknowledge_durable_checkpoint {
        // Only acknowledge a checkpoint written by a previously committed txn.
        acknowledge_committed_apply(&slot, durable.as_ref())?;
    }

    let row_budget = row_budget_for(&request);
    let time_budget = time_budget_for(&request);
    let tick_started = std::time::Instant::now();
    // Always bound the peek. An empty stream can then advance `confirmed_flush`
    // past non-publication WAL so idle ticks do not re-decode a growing
    // restart→current gap (observed pinning a core at ~100% CPU).
    let peek_upto = request
        .upper_bound
        .unwrap_or_else(|| WalFenceLsn::new(current_flush_lsn()));

    let cursor_name = open_decode_cursor(&slot, Some(peek_upto))?;
    let mut relations = HashMap::<u32, PgOutputRelation>::new();
    let mut managed = HashMap::<u32, Option<ManagedRelation>>::new();
    let mut type_names = HashMap::<(u32, i32), String>::new();
    let mut transaction_lsn = None::<u64>;
    let mut skipping_transaction = false;
    let mut skipping_flush_origin = false;
    let mut applied_end_lsn = None::<u64>;
    let mut batch = None::<ApplyBatch>;
    let mut applied = 0_i64;
    let mut saw_row_change = false;
    let mut budget_exhausted = false;
    let mut stop_after_commit = false;
    let skip_through = request.skip_through.map(AppliedWalBoundary::get);

    // Close the named portal on every exit path (including mid-apply errors).
    let result = (|| {
        loop {
            if stop_after_commit {
                break;
            }
            let messages = fetch_decode_messages(&cursor_name)?;
            if messages.is_empty() {
                break;
            }
            for data in messages {
                if stop_after_commit {
                    break;
                }
                match decode_message(&data).map_err(|error| error.to_string())? {
                    PgOutputMessage::Begin { final_lsn, .. } => {
                        flush_batch(&mut batch, &relations, &mut managed, &mut type_names)?;
                        transaction_lsn = Some(final_lsn);
                        skipping_flush_origin = false;
                        skipping_transaction = skip_through
                            .map(|boundary| final_lsn <= boundary)
                            .unwrap_or(false);
                    }
                    PgOutputMessage::Origin { name } => {
                        // Flush prune stamps a database-scoped origin so async
                        // apply does not re-insert tombstones for rows already
                        // published to cold. Critical on PG15 (no peek
                        // origin=none filter). PG16+ stamps DoNotReplicateId
                        // instead and peeks with origin=none.
                        let database_oid = koldstore_supervisor::DatabaseOid::new(
                            unsafe { pgrx::pg_sys::MyDatabaseId }.to_u32(),
                        );
                        if super::lifecycle::is_flush_replication_origin(&name, database_oid) {
                            skipping_flush_origin = true;
                        }
                    }
                    PgOutputMessage::Commit { end_lsn, .. } => {
                        if transaction_lsn.is_none() {
                            return Err("pgoutput COMMIT arrived without BEGIN".to_string());
                        }
                        flush_batch(&mut batch, &relations, &mut managed, &mut type_names)?;
                        transaction_lsn = None;
                        // Flush-origin txns are intentionally not mirrored but must
                        // still advance applied_lsn so the slot can move past them.
                        if !skipping_transaction {
                            applied_end_lsn = Some(end_lsn);
                        }
                        skipping_transaction = false;
                        skipping_flush_origin = false;
                        // Stop only at commit boundaries so mirror + applied_lsn stay atomic.
                        if budget_hit(row_budget, time_budget, applied, tick_started.elapsed()) {
                            budget_exhausted = true;
                            stop_after_commit = true;
                        }
                    }
                    PgOutputMessage::Relation(relation) => {
                        let id = relation.id;
                        relations.insert(id, relation);
                        if let Some(Some(config)) = managed.get_mut(&id) {
                            config.invalidate_plans();
                        }
                    }
                    PgOutputMessage::Insert { relation_id, new } => {
                        if skipping_transaction || skipping_flush_origin {
                            continue;
                        }
                        if !saw_row_change {
                            crate::failpoints::hit(ASYNC_MIRROR_APPLY_FAILPOINT)?;
                            saw_row_change = true;
                        }
                        push_change(
                            &mut batch,
                            &relations,
                            &mut managed,
                            &mut type_names,
                            relation_id,
                            MirrorOperation::Insert,
                            &new,
                            transaction_lsn,
                            &request,
                            &mut seq_watermark,
                        )?;
                        applied = applied.saturating_add(1);
                    }
                    PgOutputMessage::Update {
                        relation_id, new, ..
                    } => {
                        if skipping_transaction || skipping_flush_origin {
                            continue;
                        }
                        if !saw_row_change {
                            crate::failpoints::hit(ASYNC_MIRROR_APPLY_FAILPOINT)?;
                            saw_row_change = true;
                        }
                        push_change(
                            &mut batch,
                            &relations,
                            &mut managed,
                            &mut type_names,
                            relation_id,
                            MirrorOperation::Update,
                            &new,
                            transaction_lsn,
                            &request,
                            &mut seq_watermark,
                        )?;
                        applied = applied.saturating_add(1);
                    }
                    PgOutputMessage::Delete { relation_id, old } => {
                        if skipping_transaction || skipping_flush_origin {
                            continue;
                        }
                        if !saw_row_change {
                            crate::failpoints::hit(ASYNC_MIRROR_APPLY_FAILPOINT)?;
                            saw_row_change = true;
                        }
                        push_change(
                            &mut batch,
                            &relations,
                            &mut managed,
                            &mut type_names,
                            relation_id,
                            MirrorOperation::Delete,
                            &old,
                            transaction_lsn,
                            &request,
                            &mut seq_watermark,
                        )?;
                        applied = applied.saturating_add(1);
                    }
                    PgOutputMessage::Ignored { .. } => {}
                }
            }
        }
        if transaction_lsn.is_some() {
            return Err("pgoutput stream ended before COMMIT".to_string());
        }
        if request.acknowledge_durable_checkpoint {
            if let Some(end_lsn) = applied_end_lsn {
                record_applied_lsn(end_lsn, seq_watermark)?;
            } else if applied == 0 && !budget_exhausted && request.advance_slot_on_empty {
                // No publication changes in [confirmed, peek_upto]. Advance the
                // slot so the next idle wake is an O(1) confirmed_flush check
                // instead of re-decoding the same retained WAL.
                acknowledge_slot_lsn(&slot, peek_upto.get())?;
            }
        }
        // Persist hot/mirror counters in this transaction before commit. The
        // background worker's commit path is not a reliable sole home for the
        // PRE_COMMIT SPI flush used by foreground DML triggers.
        crate::row_counter_cache::flush_pending_deltas_in_transaction()?;

        let last_applied = applied_end_lsn
            .map(AppliedWalBoundary::new)
            .or(request.skip_through)
            .or(durable);
        Ok(BoundedApplyOutcome {
            row_changes: applied,
            last_applied,
            budget_exhausted,
        })
    })();
    let _ = drop_named_cursor(&cursor_name);
    result
}

fn row_budget_for(request: &BoundedApplyRequest) -> Option<i64> {
    resolve_row_budget(
        request.max_rows,
        crate::guc::async_apply_max_rows_per_tick(),
    )
}

fn time_budget_for(request: &BoundedApplyRequest) -> Option<std::time::Duration> {
    resolve_time_budget(request.max_ms, crate::guc::async_apply_max_ms_per_tick())
}

fn open_decode_cursor(slot: &str, upper_bound: Option<WalFenceLsn>) -> Result<String, String> {
    // `origin=none` is PG16+ only. On PG15, flush prune stamps the named
    // database-scoped flush origin and apply skips those changes when ORIGIN
    // is decoded. On PG16+ the peek filter is defense-in-depth.
    let upto = upper_bound.map(|lsn| format_pg_lsn(lsn.get()));
    let upto_sql = if upto.is_some() { "$3::pg_lsn" } else { "NULL" };
    #[cfg(feature = "pg15")]
    let query = format!(
        "SELECT data FROM pg_catalog.pg_logical_slot_peek_binary_changes(\
        $1, {upto_sql}, NULL, 'proto_version', '1', 'publication_names', $2, \
        'messages', 'false')"
    );
    #[cfg(not(feature = "pg15"))]
    let query = format!(
        "SELECT data FROM pg_catalog.pg_logical_slot_peek_binary_changes(\
        $1, {upto_sql}, NULL, 'proto_version', '1', 'publication_names', $2, \
        'messages', 'false', 'origin', 'none')"
    );

    pgrx::Spi::connect_mut(|client| {
        if let Some(upto) = upto.as_ref() {
            client
                .try_open_cursor_mut(
                    &query,
                    &[
                        DatumWithOid::from(slot),
                        DatumWithOid::from(PUBLICATION_NAME),
                        DatumWithOid::from(upto.as_str()),
                    ],
                )
                .map(|cursor| cursor.detach_into_name())
        } else {
            client
                .try_open_cursor_mut(
                    &query,
                    &[
                        DatumWithOid::from(slot),
                        DatumWithOid::from(PUBLICATION_NAME),
                    ],
                )
                .map(|cursor| cursor.detach_into_name())
        }
    })
    .map_err(|error| error.to_string())
}

fn drop_named_cursor(cursor_name: &str) -> Result<(), String> {
    // Drop via portal APIs (not SPI CLOSE): on soft-fail / rollback paths an
    // SPI ERROR here would FATAL a NEVER_RESTART applier before the worker
    // soft-fail handler runs.
    let Ok(name) = std::ffi::CString::new(cursor_name) else {
        return Ok(());
    };
    unsafe {
        let portal = pgrx::pg_sys::GetPortalByName(name.as_ptr());
        if !portal.is_null() {
            pgrx::pg_sys::PortalDrop(portal, false);
        }
    }
    Ok(())
}

fn fetch_decode_messages(cursor_name: &str) -> Result<Vec<Vec<u8>>, String> {
    pgrx::Spi::connect_mut(|client| -> Result<Vec<Vec<u8>>, String> {
        let mut cursor = client
            .find_cursor(cursor_name)
            .map_err(|error| error.to_string())?;
        let tuples = cursor
            .fetch(DECODE_FETCH_ROWS)
            .map_err(|error| error.to_string())?;
        let messages = tuples
            .into_iter()
            .map(|row| {
                row.get_by_name::<Vec<u8>, &str>("data")
                    .map_err(|error| format!("read decoded cursor row: {error}"))?
                    .ok_or_else(|| "logical decoding returned NULL data".to_string())
            })
            .collect::<Result<Vec<_>, String>>()?;
        if messages.is_empty() {
            drop(cursor);
        } else {
            let returned_name = cursor.detach_into_name();
            debug_assert_eq!(returned_name, cursor_name);
        }
        Ok(messages)
    })
}

fn read_durable_applied_lsn() -> Result<Option<AppliedWalBoundary>, String> {
    let database_oid = unsafe { pgrx::pg_sys::MyDatabaseId };
    let applied_lsn = pgrx::Spi::get_one_with_args::<String>(
        "SELECT (SELECT applied_lsn::text FROM koldstore.async_mirror_state WHERE database_oid = $1)",
        &[DatumWithOid::from(database_oid)],
    )
    .map_err(|error| error.to_string())?;
    applied_lsn
        .map(|text| AppliedWalBoundary::parse(&text))
        .transpose()
}

fn acknowledge_committed_apply(
    slot: &str,
    durable: Option<&AppliedWalBoundary>,
) -> Result<(), String> {
    let Some(applied_lsn) = durable else {
        return Ok(());
    };
    acknowledge_slot_lsn(slot, applied_lsn.get())
}

/// Advances `confirmed_flush` to `target_lsn` when the slot is behind it.
///
/// No-op when already at or past the target: PostgreSQL still starts logical
/// decoding from `restart_lsn` for a redundant `pg_replication_slot_advance`,
/// which is extremely expensive when restart lags by hundreds of MB.
fn acknowledge_slot_lsn(slot: &str, target_lsn: u64) -> Result<(), String> {
    let slot_c = std::ffi::CString::new(slot).map_err(|error| error.to_string())?;
    if let Some(confirmed) = super::lifecycle::native_slot_confirmed_flush_cstr(&slot_c) {
        if confirmed >= target_lsn {
            return Ok(());
        }
    }
    let text = format_pg_lsn(target_lsn);
    pgrx::Spi::run_with_args(
        "SELECT * FROM pg_catalog.pg_replication_slot_advance($1, $2::pg_lsn)",
        &[DatumWithOid::from(slot), DatumWithOid::from(text.as_str())],
    )
    .map_err(|error| error.to_string())?;
    Ok(())
}

fn current_flush_lsn() -> u64 {
    unsafe { pgrx::pg_sys::GetFlushRecPtr(std::ptr::null_mut()) }
}

/// Captures the end of inserted WAL and forces it durable on disk.
///
/// Required so logical decoding with `upto_lsn = F` can see commits that used
/// `synchronous_commit = off`. Shared by [`wait_for_async_mirror`] and flush
/// prune fences.
///
/// Uses `XLogFlush` directly rather than SPI-polling `pg_current_wal_flush_lsn`
/// with `pg_sleep`: under the apply advisory lock the async worker is blocked,
/// so that poll can sit for a long budget per call.
///
/// The fence LSN must be the end of inserted WAL ([`inserted_wal_end_lsn`]), not
/// a raw [`GetXLogInsertRecPtr`]: at page boundaries the latter points past the
/// next page header and `XLogFlush` fails with "xlog flush request … is not
/// satisfied".
///
/// # Errors
///
/// Currently infallible; returns [`Result`] so call sites share one error path.
pub fn capture_durable_wal_fence() -> Result<WalFenceLsn, String> {
    let fence = inserted_wal_end_lsn();
    unsafe { pgrx::pg_sys::XLogFlush(fence) };
    Ok(WalFenceLsn::new(fence))
}

/// Latest inserted WAL end pointer that is safe to pass to [`XLogFlush`].
///
/// Prefer `GetXLogInsertEndRecPtr` when the running PostgreSQL exports it.
/// PG 16.13 does not; emulate the page-boundary correction instead.
fn inserted_wal_end_lsn() -> pgrx::pg_sys::XLogRecPtr {
    #[cfg(not(feature = "pg16"))]
    {
        unsafe { pgrx::pg_sys::GetXLogInsertEndRecPtr() }
    }
    #[cfg(feature = "pg16")]
    {
        // Same correction as GetXLogInsertEndRecPtr / XLogBytePosToEndRecPtr:
        // at a page boundary GetXLogInsertRecPtr sits just after the page header
        // (e.g. …/018 or …/028) while no WAL exists there yet.
        let insert = unsafe { pgrx::pg_sys::GetXLogInsertRecPtr() };
        let page_off = insert % u64::from(pgrx::pg_sys::XLOG_BLCKSZ);
        let short_phd = std::mem::size_of::<pgrx::pg_sys::XLogPageHeaderData>() as u64;
        let long_phd = std::mem::size_of::<pgrx::pg_sys::XLogLongPageHeaderData>() as u64;
        if page_off == short_phd || page_off == long_phd {
            insert - page_off
        } else {
            insert
        }
    }
}

fn read_durable_seq_high_watermark() -> Result<i64, String> {
    let database_oid = unsafe { pgrx::pg_sys::MyDatabaseId };
    Ok(pgrx::Spi::get_one_with_args::<i64>(
        "SELECT (SELECT seq_high_watermark FROM koldstore.async_mirror_state WHERE database_oid = $1)",
        &[DatumWithOid::from(database_oid)],
    )
    .map_err(|error| error.to_string())?
    .unwrap_or(0)
    .max(0))
}

fn record_applied_lsn(applied_lsn: u64, seq_high_watermark: i64) -> Result<(), String> {
    // Store the exact last decoded source commit end-LSN. Never advance to
    // `pg_current_wal_insert_lsn()`: concurrent commits can land after the peek
    // boundary but before this write, and claiming them applied would let the
    // next slot advance discard undecoded WAL (including delete tombstones).
    // Mirror apply WAL is outside the publication, so it does not need covering.
    let database_oid = unsafe { pgrx::pg_sys::MyDatabaseId };
    let lsn = format_pg_lsn(applied_lsn);
    pgrx::Spi::run_with_args(
        "INSERT INTO koldstore.async_mirror_state(\
             database_oid, applied_lsn, seq_high_watermark, updated_at\
         ) \
         VALUES ($1, $2::pg_lsn, $3, pg_catalog.clock_timestamp()) \
         ON CONFLICT (database_oid) DO UPDATE \
         SET applied_lsn = GREATEST(\
               koldstore.async_mirror_state.applied_lsn, \
               EXCLUDED.applied_lsn\
             ), \
             seq_high_watermark = GREATEST(\
               koldstore.async_mirror_state.seq_high_watermark, \
               EXCLUDED.seq_high_watermark\
             ), \
             updated_at = EXCLUDED.updated_at",
        &[
            DatumWithOid::from(database_oid),
            DatumWithOid::from(lsn.as_str()),
            DatumWithOid::from(seq_high_watermark),
        ],
    )
    .map_err(|error| error.to_string())
}

#[allow(clippy::too_many_arguments)]
fn push_change(
    batch: &mut Option<ApplyBatch>,
    relations: &HashMap<u32, PgOutputRelation>,
    managed: &mut HashMap<u32, Option<ManagedRelation>>,
    type_names: &mut HashMap<(u32, i32), String>,
    relation_id: u32,
    operation: MirrorOperation,
    tuple: &PgOutputTuple,
    transaction_lsn: Option<u64>,
    request: &BoundedApplyRequest,
    seq_watermark: &mut i64,
) -> Result<(), String> {
    if transaction_lsn.is_none() {
        return Err("pgoutput row arrived without BEGIN".to_string());
    }
    let relation = relations
        .get(&relation_id)
        .ok_or_else(|| format!("pgoutput row references unknown relation {relation_id}"))?;
    let config = managed_relation(managed, relation_id)?;
    let Some(config) = config else {
        return Ok(());
    };
    let mut row = primary_key_json(relation, &config.primary_key, tuple)?;
    if operation != MirrorOperation::Delete {
        if let Some(order) = config.order_column.as_ref() {
            let text = order_column_text(relation, order, tuple)?;
            let ty =
                koldstore_sortkey::SortKeyType::from_type_oid(order.type_oid).ok_or_else(|| {
                    format!(
                        "segment order column {} has unsupported type OID {}",
                        order.name, order.type_oid
                    )
                })?;
            let encoded = koldstore_sortkey::encode_sort_key_pg_text(ty, &text)
                .map_err(|error| error.to_string())?;
            row.insert(
                "order_key".to_string(),
                Value::Array(encoded.into_iter().map(Value::from).collect()),
            );
        }
    }
    // Allocate above the durable high-watermark (and prune floor when fencing).
    let mut floor = *seq_watermark;
    if let Some((target_oid, prune_floor)) = request.target_prune_floor {
        if config.table_oid.to_u32() == target_oid {
            floor = floor.max(prune_floor.get());
        }
    }
    let seq = next_id_after(crate::sql::session::snowflake_worker_id(), floor)
        .map_err(|error| error.to_string())?;
    *seq_watermark = seq;
    row.insert("seq".to_string(), Value::from(seq));
    let identity = pk_identity(&row);
    let key = BatchKey {
        relation_id,
        operation,
    };
    let needs_flush = match batch.as_ref() {
        Some(current) => must_flush_before_push(
            Some(&current.key),
            &key,
            current.rows.len(),
            &current.seen,
            &identity,
            APPLY_BATCH_ROWS,
        )
        .is_some(),
        None => false,
    };
    if needs_flush {
        flush_batch(batch, relations, managed, type_names)?;
    }
    let current = batch.get_or_insert_with(|| ApplyBatch::new(key));
    current.seen.insert(identity);
    current.rows.push(Value::Object(row));
    Ok(())
}

fn flush_batch(
    batch: &mut Option<ApplyBatch>,
    relations: &HashMap<u32, PgOutputRelation>,
    managed: &mut HashMap<u32, Option<ManagedRelation>>,
    type_names: &mut HashMap<(u32, i32), String>,
) -> Result<(), String> {
    let Some(batch) = batch.take() else {
        return Ok(());
    };
    if batch.rows.is_empty() {
        return Ok(());
    }
    let relation = relations
        .get(&batch.key.relation_id)
        .ok_or_else(|| "relation metadata disappeared while applying batch".to_string())?;
    let config = managed
        .get_mut(&batch.key.relation_id)
        .and_then(Option::as_mut)
        .ok_or_else(|| "managed relation disappeared while applying batch".to_string())?;
    apply_batch(
        config,
        relation,
        type_names,
        batch.key.operation,
        &batch.rows,
    )?;
    // After SPI mirror writes succeed but before applied_lsn is recorded.
    crate::failpoints::hit(ASYNC_MIRROR_APPLY_AFTER_BATCH_FAILPOINT)?;
    Ok(())
}

fn managed_relation(
    cache: &mut HashMap<u32, Option<ManagedRelation>>,
    relation_id: u32,
) -> Result<Option<&ManagedRelation>, String> {
    if let std::collections::hash_map::Entry::Vacant(entry) = cache.entry(relation_id) {
        let statement = plan_async_managed_relation_by_oid().map_err(|error| error.to_string())?;
        let json = crate::spi::select_one::<String>(
            &statement,
            &[DatumWithOid::from(pgrx::pg_sys::Oid::from(relation_id))],
        )
        .map_err(|error| error.to_string())?;
        let parsed = json.map(|json| parse_managed_relation(&json)).transpose()?;
        entry.insert(parsed);
    }
    Ok(cache.get(&relation_id).and_then(Option::as_ref))
}

fn parse_managed_relation(json: &str) -> Result<ManagedRelation, String> {
    let value: Value = serde_json::from_str(json).map_err(|error| error.to_string())?;
    let meta = async_managed_relation(&value)?;
    Ok(ManagedRelation {
        table_oid: pgrx::pg_sys::Oid::from(meta.table_oid),
        mirror: meta.mirror,
        primary_key: meta.primary_key,
        order_column: meta.order_column.map(|order| OrderColumnConfig {
            name: order.name,
            type_oid: order.type_oid,
        }),
        pk_type_names: None,
        upsert_sql: None,
        update_sql: None,
        delete_sql: None,
    })
}

fn order_column_text(
    relation: &PgOutputRelation,
    order: &OrderColumnConfig,
    tuple: &PgOutputTuple,
) -> Result<String, String> {
    let relation_index = relation
        .columns
        .iter()
        .position(|column| column.name == order.name)
        .ok_or_else(|| {
            format!(
                "pgoutput relation {}.{} does not publish segment order column {}",
                relation.namespace, relation.name, order.name
            )
        })?;
    let value = tuple
        .values
        .get(relation_index)
        .ok_or_else(|| format!("tuple omits segment order column {}", order.name))?;
    pg_value_text(value, &order.name, "segment order")
}

fn ensure_pk_type_names(
    config: &mut ManagedRelation,
    relation: &PgOutputRelation,
    type_names: &mut HashMap<(u32, i32), String>,
) -> Result<(), String> {
    if config.pk_type_names.is_some() {
        return Ok(());
    }
    let mut pk_types = Vec::with_capacity(config.primary_key.len());
    for key in &config.primary_key {
        let column = relation
            .columns
            .iter()
            .find(|column| &column.name == key)
            .ok_or_else(|| format!("primary-key column {key} has no pgoutput type"))?;
        let type_key = (column.type_oid, column.typmod);
        if let std::collections::hash_map::Entry::Vacant(entry) = type_names.entry(type_key) {
            let type_name = pgrx::Spi::get_one_with_args::<String>(
                "SELECT pg_catalog.format_type($1::oid, $2)",
                &[
                    DatumWithOid::from(pgrx::pg_sys::Oid::from(column.type_oid)),
                    DatumWithOid::from(column.typmod),
                ],
            )
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("PostgreSQL cannot format type OID {}", column.type_oid))?;
            entry.insert(type_name);
        }
        let type_name = type_names.get(&type_key).expect("type name inserted above");
        pk_types.push(type_name.clone());
    }
    config.pk_type_names = Some(pk_types);
    Ok(())
}

fn apply_batch(
    config: &mut ManagedRelation,
    relation: &PgOutputRelation,
    type_names: &mut HashMap<(u32, i32), String>,
    operation: MirrorOperation,
    rows: &[Value],
) -> Result<(), String> {
    ensure_pk_type_names(config, relation, type_names)?;
    let pk_refs = config
        .primary_key
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let pk_types = config.pk_type_names.as_ref().expect("pk types populated");
    let include_order_key = config.include_order_key();
    let sql = match operation {
        MirrorOperation::Update => {
            if config.update_sql.is_none() {
                config.update_sql = Some(
                    plan_async_mirror_batch_update(
                        &config.mirror,
                        &pk_refs,
                        pk_types,
                        include_order_key,
                    )
                    .map_err(|error| error.to_string())?,
                );
            }
            config.update_sql.as_ref().expect("update SQL cached")
        }
        MirrorOperation::Delete if include_order_key => {
            if config.delete_sql.is_none() {
                config.delete_sql = Some(
                    plan_async_mirror_batch_delete_existing(&config.mirror, &pk_refs, pk_types)
                        .map_err(|error| error.to_string())?,
                );
            }
            config.delete_sql.as_ref().expect("delete SQL cached")
        }
        MirrorOperation::Insert | MirrorOperation::Delete => {
            if config.upsert_sql.is_none() {
                config.upsert_sql = Some(
                    plan_async_mirror_batch_upsert(
                        &config.mirror,
                        &pk_refs,
                        pk_types,
                        include_order_key,
                    )
                    .map_err(|error| error.to_string())?,
                );
            }
            config.upsert_sql.as_ref().expect("upsert SQL cached")
        }
    };

    let mut pk_columns: Vec<Vec<String>> = (0..config.primary_key.len())
        .map(|_| Vec::with_capacity(rows.len()))
        .collect();
    let mut seqs = Vec::with_capacity(rows.len());
    let need_order_keys = include_order_key && operation != MirrorOperation::Delete;
    let mut order_keys = need_order_keys.then(|| Vec::<Vec<u8>>::with_capacity(rows.len()));
    for row in rows {
        let object = row
            .as_object()
            .ok_or_else(|| "async mirror batch row is not an object".to_string())?;
        for (index, key) in config.primary_key.iter().enumerate() {
            let cell = object
                .get(key)
                .ok_or_else(|| format!("async mirror batch row missing primary key {key}"))?;
            let text = match cell {
                Value::String(text) => text.clone(),
                other => other.to_string(),
            };
            pk_columns[index].push(text);
        }
        let seq = object
            .get("seq")
            .and_then(Value::as_i64)
            .ok_or_else(|| "async mirror batch row missing seq".to_string())?;
        seqs.push(seq);
        if let Some(order_keys) = order_keys.as_mut() {
            let encoded = object
                .get("order_key")
                .and_then(Value::as_array)
                .ok_or_else(|| "async mirror batch row missing order_key".to_string())?
                .iter()
                .map(|cell| {
                    cell.as_u64()
                        .and_then(|n| u8::try_from(n).ok())
                        .ok_or_else(|| "order_key byte is not u8".to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            order_keys.push(encoded);
        }
    }

    let result = pgrx::Spi::connect(|client| -> Result<(i64, i64), String> {
        let mut args: Vec<DatumWithOid<'_>> = Vec::with_capacity(pk_columns.len() + 3);
        args.push(DatumWithOid::from(operation.code()));
        for (index, column) in pk_columns.iter().enumerate() {
            push_typed_pk_array_arg(&mut args, &pk_types[index], column)?;
        }
        args.push(DatumWithOid::from(seqs.clone()));
        if let Some(order_keys) = order_keys.as_ref() {
            args.push(DatumWithOid::from(order_keys.clone()));
        }
        let table = client
            .select(sql, None, &args)
            .map_err(|error| format!("execute async mirror batch: {error}"))?;
        if table.is_empty() {
            return Err("async mirror batch returned no result row".to_string());
        }
        let row = table.first();
        let affected = row
            .get::<i64>(1)
            .map_err(|error| format!("read async batch affected count: {error}"))?
            .unwrap_or(0);
        let existing = row
            .get::<i64>(2)
            .map_err(|error| format!("read async batch existing count: {error}"))?
            .unwrap_or(0);
        Ok((affected, existing))
    })?;

    // Hot and mirror counters update together with the apply transaction.
    // Deltas are derived from batch results so WAL replay after a crash does
    // not double-count (replayed upserts see existing rows; deletes affect 0).
    // Updates use direct writes with an insert-missing fallback, but do not
    // change hot/mirror live counts.
    let hot_delta = match operation {
        MirrorOperation::Insert => result.0.saturating_sub(result.1),
        MirrorOperation::Delete => -result.1,
        MirrorOperation::Update => 0,
    };
    let mirror_delta = if operation == MirrorOperation::Insert {
        result.0.saturating_sub(result.1)
    } else {
        0
    };
    crate::row_counter_cache::record_delta(config.table_oid, hot_delta, mirror_delta);
    Ok(())
}

/// Binds one primary-key column as a typed array when the SQL planner emitted
/// `{type}[]`, otherwise as `text[]` for the cast fallback path.
fn push_typed_pk_array_arg(
    args: &mut Vec<DatumWithOid<'_>>,
    type_name: &str,
    cells: &[String],
) -> Result<(), String> {
    let normalized = type_name.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "bigint" | "int8" => {
            let values = parse_pk_ints::<i64>(cells, type_name)?;
            args.push(DatumWithOid::from(values));
        }
        "integer" | "int4" | "int" => {
            let values = parse_pk_ints::<i32>(cells, type_name)?;
            args.push(DatumWithOid::from(values));
        }
        "smallint" | "int2" => {
            let values = parse_pk_ints::<i16>(cells, type_name)?;
            args.push(DatumWithOid::from(values));
        }
        "boolean" | "bool" => {
            let values = cells
                .iter()
                .map(|cell| parse_pk_bool(cell))
                .collect::<Result<Vec<_>, _>>()?;
            args.push(DatumWithOid::from(values));
        }
        "text" | "varchar" | "character varying" | "name" => {
            args.push(DatumWithOid::from(cells.to_vec()));
        }
        _ => {
            // uuid / float / domains / uncommon scalars: SQL casts from text[].
            args.push(DatumWithOid::from(cells.to_vec()));
        }
    }
    Ok(())
}

/// Applies committed WAL available at the fence boundary and returns row changes.
///
/// SQL contract: `koldstore.wait_for_async_mirror()` is an **optional** strong-
/// consistency fence for callers that need the mirror caught up before a read
/// or benchmark sample. It is **not** on the flush hot path: queue-mode
/// `flush_table` and auto-flush enqueue durable work and return.
///
/// Captures a durable WAL upper bound at call time and applies through that
/// bound only. Concurrent commits after the fence LSN are not waited on —
/// callers that need a later boundary must fence again. This is synchronous
/// apply work for the backlog at the fence, not an idle wait for writers to stop.
///
/// Timeouts remain as safety nets when an apply pass cannot finish the fence.
#[pgrx::pg_extern(name = "wait_for_async_mirror", schema = "koldstore")]
pub fn wait_for_async_mirror() -> i64 {
    const IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);
    const HARD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3600);
    let fence = capture_durable_wal_fence()
        .unwrap_or_else(|error| pgrx::error!("async mirror fence LSN capture failed: {error}"));
    let started = std::time::Instant::now();
    let mut last_progress = started;
    let mut total = 0_i64;
    loop {
        let outcome = apply_bounded(BoundedApplyRequest::upto_fence(fence))
            .unwrap_or_else(|error| pgrx::error!("async mirror apply failed: {error}"));
        total = total.saturating_add(outcome.row_changes);
        // Finished the fixed fence (or nothing left at this bound).
        if !outcome.budget_exhausted {
            break;
        }
        if outcome.row_changes > 0 {
            last_progress = std::time::Instant::now();
        }
        if last_progress.elapsed() >= IDLE_TIMEOUT || started.elapsed() >= HARD_TIMEOUT {
            pgrx::error!(
                "async mirror fence timed out after {}s with {total} row changes applied \
                 (fence_lsn={})",
                started.elapsed().as_secs(),
                format_pg_lsn(fence.get())
            );
        }
    }
    total
}
