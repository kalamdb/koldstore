//! Bounded, set-based application of committed `pgoutput` changes.
//!
//! Logical decoding, mirror writes, and the durable applied-LSN checkpoint
//! share the caller's PostgreSQL transaction. Slot advancement is deliberately
//! deferred until the next call: a crash can leave WAL retained, but can never
//! acknowledge source WAL before its mirror effect is durable.

use std::collections::{HashMap, HashSet};

use koldstore_common::{quote_ident, MirrorOperation};
use pgrx::datum::DatumWithOid;
use serde_json::{Map, Value};

use super::lifecycle::{current_slot_name, PUBLICATION_NAME};
use super::protocol::{
    decode_message, PgOutputMessage, PgOutputRelation, PgOutputTuple, PgOutputValue,
};

const DECODE_FETCH_ROWS: std::os::raw::c_long = 8_192;
const APPLY_BATCH_ROWS: usize = 8_192;

#[derive(Debug, Clone)]
struct ManagedRelation {
    table_oid: pgrx::pg_sys::Oid,
    mirror: String,
    primary_key: Vec<String>,
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
    super::lifecycle::lock_apply(unsafe { pgrx::pg_sys::MyDatabaseId }.to_u32())?;
    let slot = current_slot_name();
    let exists = pgrx::Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = $1)",
        &[DatumWithOid::from(slot.as_str())],
    )
    .map_err(|error| error.to_string())?
    .unwrap_or(false);
    if !exists {
        return Ok(0);
    }

    acknowledge_committed_apply(&slot)?;

    let query = "SELECT data FROM pg_catalog.pg_logical_slot_peek_binary_changes(\
        $1, NULL, NULL, 'proto_version', '1', 'publication_names', $2, \
        'messages', 'false', 'origin', 'none')";
    let cursor_name = pgrx::Spi::connect_mut(|client| {
        client
            .try_open_cursor_mut(
                query,
                &[
                    DatumWithOid::from(slot.as_str()),
                    DatumWithOid::from(PUBLICATION_NAME),
                ],
            )
            .map(|cursor| cursor.detach_into_name())
    })
    .map_err(|error| error.to_string())?;
    let mut relations = HashMap::<u32, PgOutputRelation>::new();
    let mut managed = HashMap::<u32, Option<ManagedRelation>>::new();
    let mut transaction_lsn = None::<u64>;
    let mut applied_end_lsn = None::<u64>;
    let mut batch = None::<ApplyBatch>;
    let mut applied = 0_i64;

    loop {
        let messages = fetch_decode_messages(&cursor_name)?;
        if messages.is_empty() {
            break;
        }
        for data in messages {
            match decode_message(&data).map_err(|error| error.to_string())? {
                PgOutputMessage::Begin { final_lsn, .. } => {
                    flush_batch(&mut batch, &relations, &mut managed, final_lsn)?;
                    transaction_lsn = Some(final_lsn);
                }
                PgOutputMessage::Commit { end_lsn, .. } => {
                    let lsn = transaction_lsn
                        .ok_or_else(|| "pgoutput COMMIT arrived without BEGIN".to_string())?;
                    flush_batch(&mut batch, &relations, &mut managed, lsn)?;
                    transaction_lsn = None;
                    applied_end_lsn = Some(end_lsn);
                }
                PgOutputMessage::Relation(relation) => {
                    relations.insert(relation.id, relation);
                }
                PgOutputMessage::Insert { relation_id, new } => {
                    push_change(
                        &mut batch,
                        &relations,
                        &mut managed,
                        relation_id,
                        MirrorOperation::Insert,
                        &new,
                        transaction_lsn,
                    )?;
                    applied = applied.saturating_add(1);
                }
                PgOutputMessage::Update {
                    relation_id, new, ..
                } => {
                    push_change(
                        &mut batch,
                        &relations,
                        &mut managed,
                        relation_id,
                        MirrorOperation::Update,
                        &new,
                        transaction_lsn,
                    )?;
                    applied = applied.saturating_add(1);
                }
                PgOutputMessage::Delete { relation_id, old } => {
                    push_change(
                        &mut batch,
                        &relations,
                        &mut managed,
                        relation_id,
                        MirrorOperation::Delete,
                        &old,
                        transaction_lsn,
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
    if let Some(end_lsn) = applied_end_lsn {
        record_applied_lsn(end_lsn)?;
    }
    Ok(applied)
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

fn acknowledge_committed_apply(slot: &str) -> Result<(), String> {
    let database_oid = unsafe { pgrx::pg_sys::MyDatabaseId };
    let applied_lsn = pgrx::Spi::get_one_with_args::<String>(
        "SELECT (SELECT applied_lsn::text FROM koldstore.async_mirror_state WHERE database_oid = $1)",
        &[DatumWithOid::from(database_oid)],
    )
    .map_err(|error| error.to_string())?;
    if let Some(applied_lsn) = applied_lsn {
        pgrx::Spi::run_with_args(
            "SELECT * FROM pg_catalog.pg_replication_slot_advance($1, $2::pg_lsn)",
            &[
                DatumWithOid::from(slot),
                DatumWithOid::from(applied_lsn.as_str()),
            ],
        )
        .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn record_applied_lsn(applied_lsn: u64) -> Result<(), String> {
    let database_oid = unsafe { pgrx::pg_sys::MyDatabaseId };
    let lsn = format_lsn(applied_lsn);
    pgrx::Spi::run_with_args(
        "INSERT INTO koldstore.async_mirror_state(database_oid, applied_lsn, updated_at) \
         VALUES (\
           $1, \
           GREATEST($2::pg_lsn, pg_catalog.pg_current_wal_insert_lsn()), \
           pg_catalog.clock_timestamp()\
         ) \
         ON CONFLICT (database_oid) DO UPDATE \
         SET applied_lsn = EXCLUDED.applied_lsn, updated_at = EXCLUDED.updated_at",
        &[
            DatumWithOid::from(database_oid),
            DatumWithOid::from(lsn.as_str()),
        ],
    )
    .map_err(|error| error.to_string())
}

fn push_change(
    batch: &mut Option<ApplyBatch>,
    relations: &HashMap<u32, PgOutputRelation>,
    managed: &mut HashMap<u32, Option<ManagedRelation>>,
    relation_id: u32,
    operation: MirrorOperation,
    tuple: &PgOutputTuple,
    transaction_lsn: Option<u64>,
) -> Result<(), String> {
    let lsn = transaction_lsn.ok_or_else(|| "pgoutput row arrived without BEGIN".to_string())?;
    let relation = relations
        .get(&relation_id)
        .ok_or_else(|| format!("pgoutput row references unknown relation {relation_id}"))?;
    let config = managed_relation(managed, relation_id)?;
    let Some(config) = config else {
        return Ok(());
    };
    let row = primary_key_json(relation, config, tuple)?;
    let identity = serde_json::to_string(&row).map_err(|error| error.to_string())?;
    let key = BatchKey {
        relation_id,
        operation,
    };
    let must_flush = batch.as_ref().is_some_and(|current| {
        current.key != key
            || current.rows.len() >= APPLY_BATCH_ROWS
            || current.seen.contains(&identity)
    });
    if must_flush {
        flush_batch(batch, relations, managed, lsn)?;
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
    commit_lsn: u64,
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
    let config = managed_relation(managed, batch.key.relation_id)?
        .ok_or_else(|| "managed relation disappeared while applying batch".to_string())?;
    apply_batch(
        config,
        relation,
        batch.key.operation,
        &batch.rows,
        commit_lsn,
    )
}

fn managed_relation(
    cache: &mut HashMap<u32, Option<ManagedRelation>>,
    relation_id: u32,
) -> Result<Option<&ManagedRelation>, String> {
    if let std::collections::hash_map::Entry::Vacant(entry) = cache.entry(relation_id) {
        let json = pgrx::Spi::get_one_with_args::<String>(
            "SELECT (SELECT jsonb_build_object(\
                'table_oid', s.table_oid::text, \
                'mirror', s.mirror_relation::text, \
                'primary_key', s.primary_key\
             )::text \
             FROM koldstore.schemas s \
             WHERE s.active AND s.table_oid = $1 \
               AND COALESCE(s.options->>'mirror_capture_mode', 'strict') = 'async' \
             LIMIT 1)",
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
    })
}

fn primary_key_json(
    relation: &PgOutputRelation,
    config: &ManagedRelation,
    tuple: &PgOutputTuple,
) -> Result<Map<String, Value>, String> {
    let key_columns = relation
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| config.primary_key.iter().any(|key| key == &column.name))
        .collect::<Vec<_>>();
    if key_columns.len() != config.primary_key.len() {
        return Err(format!(
            "pgoutput relation {}.{} does not publish every managed primary-key column",
            relation.namespace, relation.name
        ));
    }
    let compact_old_key =
        tuple.values.len() == key_columns.len() && tuple.values.len() != relation.columns.len();
    let mut row = Map::with_capacity(config.primary_key.len());
    for key in &config.primary_key {
        let (relation_index, _) = key_columns
            .iter()
            .find(|(_, column)| &column.name == key)
            .ok_or_else(|| format!("primary-key column {key} missing from relation metadata"))?;
        let tuple_index = if compact_old_key {
            key_columns
                .iter()
                .position(|(index, _)| index == relation_index)
                .expect("key position exists")
        } else {
            *relation_index
        };
        let value = tuple
            .values
            .get(tuple_index)
            .ok_or_else(|| format!("tuple omits primary-key column {key}"))?;
        row.insert(key.clone(), pg_value_json(value, key)?);
    }
    Ok(row)
}

fn pg_value_json(value: &PgOutputValue, column: &str) -> Result<Value, String> {
    match value {
        PgOutputValue::Null => Err(format!("primary-key column {column} is NULL")),
        PgOutputValue::UnchangedToast => Err(format!(
            "primary-key column {column} was emitted as unchanged TOAST"
        )),
        PgOutputValue::Text(bytes) => String::from_utf8(bytes.clone())
            .map(Value::String)
            .map_err(|error| error.to_string()),
        PgOutputValue::Binary(_) => Err("binary pgoutput values are not requested".to_string()),
    }
}

fn apply_batch(
    config: &ManagedRelation,
    relation: &PgOutputRelation,
    operation: MirrorOperation,
    rows: &[Value],
    commit_lsn: u64,
) -> Result<(), String> {
    let mut record_columns = Vec::with_capacity(config.primary_key.len());
    for key in &config.primary_key {
        let column = relation
            .columns
            .iter()
            .find(|column| &column.name == key)
            .ok_or_else(|| format!("primary-key column {key} has no pgoutput type"))?;
        let type_name = pgrx::Spi::get_one_with_args::<String>(
            "SELECT pg_catalog.format_type($1::oid, $2)",
            &[
                DatumWithOid::from(pgrx::pg_sys::Oid::from(column.type_oid)),
                DatumWithOid::from(column.typmod),
            ],
        )
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("PostgreSQL cannot format type OID {}", column.type_oid))?;
        record_columns.push(format!("{} {type_name}", quote_ident(key)));
    }
    let quoted_keys = config
        .primary_key
        .iter()
        .map(|key| quote_ident(key))
        .collect::<Vec<_>>();
    let select_keys = quoted_keys
        .iter()
        .map(|key| format!("incoming.{key}"))
        .collect::<Vec<_>>()
        .join(", ");
    let conflict_keys = quoted_keys.join(", ");
    let insert_columns = format!("{conflict_keys}, \"seq\", \"op\", \"commit_lsn\"");
    let incoming = format!(
        "WITH incoming AS (\
           SELECT * FROM pg_catalog.jsonb_to_recordset($1::jsonb) AS x({})\
         )",
        record_columns.join(", ")
    );
    let sql = if operation == MirrorOperation::Insert {
        format!(
            "{incoming}, existing AS (\
               SELECT count(*)::bigint AS count FROM incoming \
               JOIN {} AS mirror USING ({conflict_keys})\
             ), applied AS (\
               INSERT INTO {} ({insert_columns}) \
               SELECT {select_keys}, {}, $2::smallint, $3::pg_lsn \
               FROM incoming \
               ON CONFLICT ({conflict_keys}) DO UPDATE \
               SET \"seq\" = EXCLUDED.\"seq\", \
                   \"op\" = EXCLUDED.\"op\", \
                   \"commit_lsn\" = EXCLUDED.\"commit_lsn\" \
               RETURNING 1\
             ) \
             SELECT (SELECT count(*)::bigint FROM applied), \
                    (SELECT count FROM existing)",
            config.mirror,
            config.mirror,
            koldstore_common::snowflake_id_call_expression(),
        )
    } else {
        let join = quoted_keys
            .iter()
            .map(|key| format!("mirror.{key} = incoming.{key}"))
            .collect::<Vec<_>>()
            .join(" AND ");
        format!(
            "{incoming}, applied AS (\
               UPDATE {} AS mirror \
               SET \"seq\" = {}, \"op\" = $2::smallint, \"commit_lsn\" = $3::pg_lsn \
               FROM incoming WHERE {join} RETURNING 1\
             ) \
             SELECT count(*)::bigint, count(*)::bigint FROM applied",
            config.mirror,
            koldstore_common::snowflake_id_call_expression(),
        )
    };
    let rows_json = serde_json::to_string(rows).map_err(|error| error.to_string())?;
    let lsn = format_lsn(commit_lsn);
    let result = pgrx::Spi::connect(|client| -> Result<(i64, i64), String> {
        let table = client
            .select(
                &sql,
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

    let hot_delta = match operation {
        MirrorOperation::Insert => result.0,
        MirrorOperation::Update => 0,
        MirrorOperation::Delete => -result.0,
    };
    let mirror_delta = if operation == MirrorOperation::Insert {
        result.0.saturating_sub(result.1)
    } else {
        0
    };
    crate::row_counter_cache::record_delta(config.table_oid, hot_delta, mirror_delta);
    Ok(())
}

fn format_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xffff_ffff)
}

/// Applies available committed WAL and returns the number of row changes.
///
/// SQL contract: `koldstore.wait_for_async_mirror()` is the explicit strong
/// consistency fence for async mode and benchmark accounting.
#[pgrx::pg_extern(name = "wait_for_async_mirror", schema = "koldstore")]
pub fn wait_for_async_mirror() -> i64 {
    apply_available().unwrap_or_else(|error| pgrx::error!("async mirror apply failed: {error}"))
}
