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
use koldstore_common::{
    format_pg_lsn, next_id_after, quote_ident, snowflake_id_call_expression, MirrorOperation,
};
use koldstore_mirror::{
    decode_message, must_flush_before_push, pk_identity, plan_async_mirror_batch_insert,
    plan_async_mirror_batch_update, primary_key_json, PgOutputMessage, PgOutputRelation,
    PgOutputTuple, APPLY_BATCH_ROWS,
};
use pgrx::datum::DatumWithOid;
use serde_json::Value;

use super::lifecycle::{current_slot_name, PUBLICATION_NAME};

pub use koldstore_common::{AppliedWalBoundary, WalFenceLsn};

const DECODE_FETCH_ROWS: std::os::raw::c_long = 8_192;

/// Failpoint name: abort during async mirror apply (worker ERROR exit).
pub const ASYNC_MIRROR_APPLY_FAILPOINT: &str = "async_mirror_apply";

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
}

impl BoundedApplyRequest {
    /// Default worker / `wait_for_async_mirror` / phase-0 selection fence request.
    #[must_use]
    pub fn available() -> Self {
        Self {
            upper_bound: None,
            skip_through: None,
            acknowledge_durable_checkpoint: true,
            target_prune_floor: None,
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
}

#[derive(Debug, Clone)]
struct ManagedRelation {
    table_oid: pgrx::pg_sys::Oid,
    mirror: String,
    primary_key: Vec<String>,
    /// Cached `jsonb_to_recordset` column DDL (`"id" bigint, ...`).
    record_columns: Option<Vec<String>>,
    /// Cached insert batch SQL for the current relation type fingerprint.
    insert_sql: Option<String>,
    /// Cached update/delete batch SQL for the current relation type fingerprint.
    update_sql: Option<String>,
    /// Floor-aware insert SQL using `incoming."seq"`.
    floor_insert_sql: Option<String>,
    /// Floor-aware update SQL using `incoming."seq"`.
    floor_update_sql: Option<String>,
}

impl ManagedRelation {
    fn invalidate_plans(&mut self) {
        self.record_columns = None;
        self.insert_sql = None;
        self.update_sql = None;
        self.floor_insert_sql = None;
        self.floor_update_sql = None;
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
        });
    }

    let durable = read_durable_applied_lsn()?;
    if request.acknowledge_durable_checkpoint {
        // Only acknowledge a checkpoint written by a previously committed txn.
        acknowledge_committed_apply(&slot, durable.as_ref())?;
    }

    let cursor_name = open_decode_cursor(&slot, request.upper_bound)?;
    let mut relations = HashMap::<u32, PgOutputRelation>::new();
    let mut managed = HashMap::<u32, Option<ManagedRelation>>::new();
    let mut type_names = HashMap::<(u32, i32), String>::new();
    let mut transaction_lsn = None::<u64>;
    let mut skipping_transaction = false;
    let mut applied_end_lsn = None::<u64>;
    let mut batch = None::<ApplyBatch>;
    let mut applied = 0_i64;
    let mut saw_row_change = false;
    let skip_through = request.skip_through.map(AppliedWalBoundary::get);

    // Close the named portal on every exit path (including mid-apply errors).
    let result = (|| {
        loop {
            let messages = fetch_decode_messages(&cursor_name)?;
            if messages.is_empty() {
                break;
            }
            for data in messages {
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
                        skipping_transaction = skip_through
                            .map(|boundary| final_lsn <= boundary)
                            .unwrap_or(false);
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
                        if !skipping_transaction {
                            applied_end_lsn = Some(end_lsn);
                        }
                        skipping_transaction = false;
                    }
                    PgOutputMessage::Relation(relation) => {
                        let id = relation.id;
                        relations.insert(id, relation);
                        if let Some(Some(config)) = managed.get_mut(&id) {
                            config.invalidate_plans();
                        }
                    }
                    PgOutputMessage::Insert { relation_id, new } => {
                        if skipping_transaction {
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
                        if skipping_transaction {
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
                        if skipping_transaction {
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
        })
    })();
    let _ = drop_named_cursor(&cursor_name);
    result
}

fn open_decode_cursor(slot: &str, upper_bound: Option<WalFenceLsn>) -> Result<String, String> {
    // `origin=none` is PG16+ only. On PG15, flush prune still uses
    // `DoNotReplicateId`, which keeps those WAL records out of logical decoding
    // entirely; the filter is defense-in-depth for ordinary origin-stamped WAL.
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
    pgrx::Spi::connect_mut(|client| {
        if let Ok(cursor) = client.find_cursor(cursor_name) {
            drop(cursor);
        }
        Ok(())
    })
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
    if let Some((target_oid, floor)) = request.target_prune_floor {
        if config.table_oid == target_oid {
            let seq = next_id_after(crate::sql::session::snowflake_worker_id(), floor.get())
                .map_err(|error| error.to_string())?;
            row.insert("seq".to_string(), Value::from(seq));
        }
    }
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
    commit_lsn: u64,
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
        commit_lsn,
        request,
    )
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
        record_columns: None,
        insert_sql: None,
        update_sql: None,
        floor_insert_sql: None,
        floor_update_sql: None,
    })
}

fn ensure_record_columns(
    config: &mut ManagedRelation,
    relation: &PgOutputRelation,
    type_names: &mut HashMap<(u32, i32), String>,
) -> Result<(), String> {
    if config.record_columns.is_some() {
        return Ok(());
    }
    let mut record_columns = Vec::with_capacity(config.primary_key.len());
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
        record_columns.push(format!("{} {type_name}", quote_ident(key)));
    }
    config.record_columns = Some(record_columns);
    Ok(())
}

fn uses_floor_seq(config: &ManagedRelation, request: &BoundedApplyRequest) -> bool {
    request
        .target_prune_floor
        .map(|(oid, _)| oid == config.table_oid)
        .unwrap_or(false)
}

fn apply_batch(
    config: &mut ManagedRelation,
    relation: &PgOutputRelation,
    type_names: &mut HashMap<(u32, i32), String>,
    operation: MirrorOperation,
    rows: &[Value],
    commit_lsn: u64,
    request: &BoundedApplyRequest,
) -> Result<(), String> {
    ensure_record_columns(config, relation, type_names)?;
    let use_floor = uses_floor_seq(config, request);
    // Latest-state deletes must upsert: an UPDATE-only apply silently no-ops when
    // the mirror row was already pruned (or never present), dropping the tombstone
    // the prune fence relies on.
    let uses_upsert = matches!(operation, MirrorOperation::Insert | MirrorOperation::Delete);
    let needs_plan = if uses_upsert {
        if use_floor {
            config.floor_insert_sql.is_none()
        } else {
            config.insert_sql.is_none()
        }
    } else if use_floor {
        config.floor_update_sql.is_none()
    } else {
        config.update_sql.is_none()
    };
    if needs_plan {
        let mut record_columns = config
            .record_columns
            .as_ref()
            .expect("record columns populated")
            .clone();
        if use_floor {
            record_columns.push(format!("{} bigint", quote_ident("seq")));
        }
        let pk_refs = config
            .primary_key
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let seq_expression = if use_floor {
            format!("incoming.{}", quote_ident("seq"))
        } else {
            snowflake_id_call_expression().to_string()
        };
        let planned = if uses_upsert {
            plan_async_mirror_batch_insert(
                &config.mirror,
                &pk_refs,
                &record_columns,
                &seq_expression,
            )
        } else {
            plan_async_mirror_batch_update(
                &config.mirror,
                &pk_refs,
                &record_columns,
                &seq_expression,
            )
        }
        .map_err(|error| error.to_string())?;
        if uses_upsert {
            if use_floor {
                config.floor_insert_sql = Some(planned);
            } else {
                config.insert_sql = Some(planned);
            }
        } else if use_floor {
            config.floor_update_sql = Some(planned);
        } else {
            config.update_sql = Some(planned);
        }
    }
    let sql = if uses_upsert {
        if use_floor {
            config.floor_insert_sql.as_ref().expect("floor insert SQL")
        } else {
            config.insert_sql.as_ref().expect("insert SQL cached")
        }
    } else if use_floor {
        config.floor_update_sql.as_ref().expect("floor update SQL")
    } else {
        config.update_sql.as_ref().expect("update SQL cached")
    };
    let rows_json = serde_json::to_string(rows).map_err(|error| error.to_string())?;
    let lsn = format_pg_lsn(commit_lsn);
    let result = pgrx::Spi::connect(|client| -> Result<(i64, i64), String> {
        let table = client
            .select(
                sql,
                None,
                &[
                    DatumWithOid::from(rows_json.as_str()),
                    DatumWithOid::from(operation.code()),
                    DatumWithOid::from(lsn.as_str()),
                ],
            )
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
    let hot_delta = match operation {
        MirrorOperation::Insert => result.0.saturating_sub(result.1),
        // Upsert tombstone: shrink hot only when a mirror row already existed.
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
/// consistency fence for async mode and benchmark accounting.
#[pgrx::pg_extern(name = "wait_for_async_mirror", schema = "koldstore")]
pub fn wait_for_async_mirror() -> i64 {
    apply_available().unwrap_or_else(|error| pgrx::error!("async mirror apply failed: {error}"))
}
