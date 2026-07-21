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
//! [`koldstore_mirror::APPLY_BATCH_ROWS`] and cleared on every flush.
//!
//! Flush prune fences use [`apply_bounded`] with an explicit `upto_lsn`,
//! transaction skip boundary, and `acknowledge_durable_checkpoint = false` so
//! the still-uncommitted flush transaction cannot advance the slot.

use std::collections::{HashMap, HashSet};

use koldstore_catalog::queries::plan_async_managed_relation_by_oid;
use koldstore_common::{format_pg_lsn, next_id_after, MirrorOperation};
use koldstore_mirror::{
    decode_message, must_flush_before_push, pk_identity, plan_async_mirror_batch_update,
    plan_async_mirror_batch_upsert, primary_key_json, PgOutputMessage, PgOutputRelation,
    PgOutputTuple, APPLY_BATCH_ROWS,
};
use pgrx::datum::DatumWithOid;
use serde_json::Value;

use super::lifecycle::{current_slot_name, PUBLICATION_NAME};

pub use koldstore_common::{AppliedWalBoundary, WalFenceLsn};

const DECODE_FETCH_ROWS: std::os::raw::c_long = 8_192;

/// Failpoint name: abort during async mirror apply (worker ERROR exit).
pub const ASYNC_MIRROR_APPLY_FAILPOINT: &str = "async_mirror_apply";
/// Failpoint name: abort after at least one mirror batch SPI write, before
/// `applied_lsn` is recorded — asserts one-txn-per-tick rollback.
pub const ASYNC_MIRROR_APPLY_AFTER_BATCH_FAILPOINT: &str = "async_mirror_apply_after_batch";

/// Target-table mirror seq must be strictly greater than this floor after fence apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PruneSeqFloor(i64);

impl PruneSeqFloor {
    /// Wraps a mirror `max_seq` watermark.
    #[must_use]
    pub const fn new(max_seq: i64) -> Self {
        Self(max_seq)
    }

    /// Returns the raw floor value.
    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }
}

/// Request for a single bounded (or unbounded) async mirror apply pass.
#[derive(Debug, Clone)]
pub struct BoundedApplyRequest {
    /// When set, pass as `upto_lsn` to logical decoding.
    pub upper_bound: Option<WalFenceLsn>,
    /// Skip whole pgoutput transactions with `end_lsn <= skip_through`.
    pub skip_through: Option<AppliedWalBoundary>,
    /// When true, advance the slot to the previously committed durable checkpoint
    /// and record a new pending `applied_lsn`. Flush prune fences must use false.
    pub acknowledge_durable_checkpoint: bool,
    /// When set, allocate sequences for this table strictly above the floor.
    pub target_prune_floor: Option<(pgrx::pg_sys::Oid, PruneSeqFloor)>,
    /// Optional row budget override. `None` uses the background GUC; `Some(0)`
    /// means unlimited; `Some(n > 0)` caps source row changes in this pass.
    pub max_rows: Option<i64>,
    /// Optional wall-time budget override (milliseconds). Same semantics as
    /// [`Self::max_rows`].
    pub max_ms: Option<i64>,
}

impl BoundedApplyRequest {
    /// Default worker apply request (honors per-tick GUC budgets).
    #[must_use]
    pub fn available() -> Self {
        Self {
            upper_bound: None,
            skip_through: None,
            acknowledge_durable_checkpoint: true,
            target_prune_floor: None,
            max_rows: None,
            max_ms: None,
        }
    }

    /// Explicit fence / flush request: drain all peekable WAL in this pass.
    #[must_use]
    pub fn available_unlimited() -> Self {
        Self {
            upper_bound: None,
            skip_through: None,
            acknowledge_durable_checkpoint: true,
            target_prune_floor: None,
            max_rows: Some(0),
            max_ms: Some(0),
        }
    }
}

/// Outcome of a bounded apply pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoundedApplyOutcome {
    /// Source row-change messages applied (skipped transactions excluded).
    pub row_changes: i64,
    /// Exact last decoded commit end-LSN, or the request skip / durable boundary
    /// when the pass was empty. Never promoted to the fence upper bound alone.
    pub last_applied: Option<AppliedWalBoundary>,
    /// True when a row/time budget stopped the pass with more WAL remaining.
    pub budget_exhausted: bool,
}

#[derive(Debug, Clone)]
struct ManagedRelation {
    table_oid: pgrx::pg_sys::Oid,
    mirror: String,
    primary_key: Vec<String>,
    /// Cached `format_type` spellings for each primary-key column.
    pk_type_names: Option<Vec<String>>,
    /// Cached upsert SQL for typed `unnest` binds (Insert/Update/Delete).
    upsert_sql: Option<String>,
    /// Cached direct-update plus insert-missing SQL for UPDATE batches.
    update_sql: Option<String>,
}

impl ManagedRelation {
    fn invalidate_plans(&mut self) {
        self.pk_type_names = None;
        self.upsert_sql = None;
        self.update_sql = None;
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

/// Applies all currently available committed WAL for async managed tables.
///
/// The return value is the number of source row-change messages applied. Rust
/// allocations and SPI writes are bounded to 8K rows per batch; PostgreSQL may
/// spill the decoding SRF tuplestore according to `work_mem` for a very large
/// source transaction.
///
/// # Errors
///
/// Returns an error for malformed protocol data, stale relation metadata,
/// missing primary-key values, or an SPI/apply failure.
pub fn apply_available() -> Result<i64, String> {
    Ok(apply_bounded(BoundedApplyRequest::available())?.row_changes)
}

/// Applies committed WAL under an explicit fence request.
///
/// # Errors
///
/// Returns an error for malformed protocol data, stale relation metadata,
/// missing primary-key values, or an SPI/apply failure.
pub fn apply_bounded(request: BoundedApplyRequest) -> Result<BoundedApplyOutcome, String> {
    // Frontend fences (flush / wait_for_async_mirror) re-attach the applier after
    // postmaster restart. The applier itself must not re-enter ensure: that takes
    // the worker-registration xact lock held by an in-progress manage_table.
    if !is_background_worker() {
        crate::database_worker::ensure_async_mirror_worker_once_if_needed();
    }
    super::lifecycle::lock_apply(unsafe { pgrx::pg_sys::MyDatabaseId }.to_u32())?;
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
    // that window under lock_apply before touching the slot.
    super::lifecycle::wait_until_slot_inactive(&slot)
        .map_err(|error| format!("wait slot inactive before apply: {error}"))?;

    let durable = read_durable_applied_lsn()?;
    if request.acknowledge_durable_checkpoint {
        // Only acknowledge a checkpoint written by a previously committed txn.
        acknowledge_committed_apply(&slot, durable.as_ref())?;
    }

    let row_budget = resolve_row_budget(&request);
    let time_budget = resolve_time_budget(&request);
    let tick_started = std::time::Instant::now();

    let cursor_name = open_decode_cursor(&slot, request.upper_bound)?;
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
                        flush_batch(
                            &mut batch,
                            &relations,
                            &mut managed,
                            &mut type_names,
                            final_lsn,
                            &request,
                        )?;
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
                        let database_oid = koldstore_worker::DatabaseOid::new(
                            unsafe { pgrx::pg_sys::MyDatabaseId }.to_u32(),
                        );
                        if super::lifecycle::is_flush_replication_origin(&name, database_oid) {
                            skipping_flush_origin = true;
                        }
                    }
                    PgOutputMessage::Commit { end_lsn, .. } => {
                        let lsn = transaction_lsn
                            .ok_or_else(|| "pgoutput COMMIT arrived without BEGIN".to_string())?;
                        flush_batch(
                            &mut batch,
                            &relations,
                            &mut managed,
                            &mut type_names,
                            lsn,
                            &request,
                        )?;
                        transaction_lsn = None;
                        // Flush-origin txns are intentionally not mirrored but must
                        // still advance applied_lsn so the slot can move past them.
                        if !skipping_transaction {
                            applied_end_lsn = Some(end_lsn);
                        }
                        skipping_transaction = false;
                        skipping_flush_origin = false;
                        // Stop only at commit boundaries so mirror + applied_lsn stay atomic.
                        if budget_hit(row_budget, time_budget, applied, tick_started) {
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
                record_applied_lsn(end_lsn)?;
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

fn resolve_row_budget(request: &BoundedApplyRequest) -> Option<i64> {
    match request.max_rows {
        Some(0) => None,
        Some(limit) if limit > 0 => Some(limit),
        Some(_) => None,
        None => {
            let guc = crate::guc::async_apply_max_rows_per_tick();
            if guc > 0 {
                Some(guc)
            } else {
                None
            }
        }
    }
}

fn resolve_time_budget(request: &BoundedApplyRequest) -> Option<std::time::Duration> {
    let ms = match request.max_ms {
        Some(0) => return None,
        Some(limit) if limit > 0 => limit,
        Some(_) => return None,
        None => {
            let guc = crate::guc::async_apply_max_ms_per_tick();
            if guc > 0 {
                guc
            } else {
                return None;
            }
        }
    };
    Some(std::time::Duration::from_millis(
        u64::try_from(ms).unwrap_or(0),
    ))
}

fn budget_hit(
    row_budget: Option<i64>,
    time_budget: Option<std::time::Duration>,
    applied: i64,
    started: std::time::Instant,
) -> bool {
    if let Some(limit) = row_budget {
        if applied >= limit {
            return true;
        }
    }
    if let Some(limit) = time_budget {
        if started.elapsed() >= limit {
            return true;
        }
    }
    false
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
    let text = format_pg_lsn(applied_lsn.get());
    pgrx::Spi::run_with_args(
        "SELECT * FROM pg_catalog.pg_replication_slot_advance($1, $2::pg_lsn)",
        &[DatumWithOid::from(slot), DatumWithOid::from(text.as_str())],
    )
    .map_err(|error| error.to_string())?;
    Ok(())
}

fn record_applied_lsn(applied_lsn: u64) -> Result<(), String> {
    // Store the exact last decoded source commit end-LSN. Never advance to
    // `pg_current_wal_insert_lsn()`: concurrent commits can land after the peek
    // boundary but before this write, and claiming them applied would let the
    // next slot advance discard undecoded WAL (including delete tombstones).
    // Mirror apply WAL is outside the publication, so it does not need covering.
    let database_oid = unsafe { pgrx::pg_sys::MyDatabaseId };
    let lsn = format_pg_lsn(applied_lsn);
    pgrx::Spi::run_with_args(
        "INSERT INTO koldstore.async_mirror_state(database_oid, applied_lsn, updated_at) \
         VALUES ($1, $2::pg_lsn, pg_catalog.clock_timestamp()) \
         ON CONFLICT (database_oid) DO UPDATE \
         SET applied_lsn = GREATEST(\
               koldstore.async_mirror_state.applied_lsn, \
               EXCLUDED.applied_lsn\
             ), \
             updated_at = EXCLUDED.updated_at",
        &[
            DatumWithOid::from(database_oid),
            DatumWithOid::from(lsn.as_str()),
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
) -> Result<(), String> {
    let lsn = transaction_lsn.ok_or_else(|| "pgoutput row arrived without BEGIN".to_string())?;
    let relation = relations
        .get(&relation_id)
        .ok_or_else(|| format!("pgoutput row references unknown relation {relation_id}"))?;
    let config = managed_relation(managed, relation_id)?;
    let Some(config) = config else {
        return Ok(());
    };
    let mut row = primary_key_json(relation, &config.primary_key, tuple)?;
    // Always allocate seq in Rust (floor path stays strictly above prune watermark).
    let seq = if let Some((target_oid, floor)) = request.target_prune_floor {
        if config.table_oid == target_oid {
            next_id_after(crate::sql::session::snowflake_worker_id(), floor.get())
                .map_err(|error| error.to_string())?
        } else {
            crate::sql::session::snowflake_id()
        }
    } else {
        crate::sql::session::snowflake_id()
    };
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
        flush_batch(batch, relations, managed, type_names, lsn, request)?;
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
    _commit_lsn: u64,
    request: &BoundedApplyRequest,
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
        request,
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
    let table_oid = value
        .get("table_oid")
        .and_then(Value::as_str)
        .ok_or_else(|| "async schema metadata has no table_oid".to_string())?
        .parse::<u32>()
        .map(pgrx::pg_sys::Oid::from)
        .map_err(|error| error.to_string())?;
    let mirror = value
        .get("mirror")
        .and_then(Value::as_str)
        .ok_or_else(|| "async schema metadata has no mirror relation".to_string())?
        .to_string();
    let primary_key = value
        .get("primary_key")
        .and_then(Value::as_array)
        .ok_or_else(|| "async schema metadata has no primary key".to_string())?
        .iter()
        .map(|column| {
            column
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| "async primary key contains a non-string".to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ManagedRelation {
        table_oid,
        mirror,
        primary_key,
        pk_type_names: None,
        upsert_sql: None,
        update_sql: None,
    })
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
    _request: &BoundedApplyRequest,
) -> Result<(), String> {
    ensure_pk_type_names(config, relation, type_names)?;
    let pk_refs = config
        .primary_key
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let pk_types = config.pk_type_names.as_ref().expect("pk types populated");
    let sql = if operation == MirrorOperation::Update {
        if config.update_sql.is_none() {
            config.update_sql = Some(
                plan_async_mirror_batch_update(&config.mirror, &pk_refs, pk_types, "unused")
                    .map_err(|error| error.to_string())?,
            );
        }
        config.update_sql.as_ref().expect("update SQL cached")
    } else {
        if config.upsert_sql.is_none() {
            config.upsert_sql = Some(
                plan_async_mirror_batch_upsert(&config.mirror, &pk_refs, pk_types)
                    .map_err(|error| error.to_string())?,
            );
        }
        config.upsert_sql.as_ref().expect("upsert SQL cached")
    };

    let mut pk_columns: Vec<Vec<String>> = (0..config.primary_key.len())
        .map(|_| Vec::with_capacity(rows.len()))
        .collect();
    let mut seqs = Vec::with_capacity(rows.len());
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
    }

    let result = pgrx::Spi::connect(|client| -> Result<(i64, i64), String> {
        let mut args: Vec<DatumWithOid<'_>> = Vec::with_capacity(pk_columns.len() + 2);
        args.push(DatumWithOid::from(operation.code()));
        for column in &pk_columns {
            args.push(DatumWithOid::from(column.clone()));
        }
        args.push(DatumWithOid::from(seqs.clone()));
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

fn is_background_worker() -> bool {
    unsafe { !pgrx::pg_sys::MyBgworkerEntry.is_null() }
}

/// Applies available committed WAL and returns the number of row changes.
///
/// SQL contract: `koldstore.wait_for_async_mirror()` is the explicit strong
/// consistency fence for async mode and benchmark accounting. It loops with an
/// unlimited per-pass budget until the stream is idle. The timeout is
/// **idle-based**: progress (applied row changes) resets it, so a large catch-up
/// that keeps applying does not fail solely because wall time exceeded 300s.
#[pgrx::pg_extern(name = "wait_for_async_mirror", schema = "koldstore")]
pub fn wait_for_async_mirror() -> i64 {
    const IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);
    const HARD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3600);
    let started = std::time::Instant::now();
    let mut last_progress = started;
    let mut total = 0_i64;
    loop {
        let outcome = apply_bounded(BoundedApplyRequest::available_unlimited())
            .unwrap_or_else(|error| pgrx::error!("async mirror apply failed: {error}"));
        total = total.saturating_add(outcome.row_changes);
        if outcome.row_changes == 0 && !outcome.budget_exhausted {
            break;
        }
        if outcome.row_changes > 0 {
            last_progress = std::time::Instant::now();
        }
        if last_progress.elapsed() >= IDLE_TIMEOUT || started.elapsed() >= HARD_TIMEOUT {
            pgrx::error!(
                "async mirror fence timed out after {}s with {total} row changes applied",
                started.elapsed().as_secs()
            );
        }
    }
    total
}
