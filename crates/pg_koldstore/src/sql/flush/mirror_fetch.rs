//! SPI mirror-row fetch and typed tuple decode for flush.

use koldstore_common::{CellValue, SqlParamType, SqlStatement};
use koldstore_flush::MirrorFlushPageCursor;
use koldstore_migrate::order::CatalogColumn;
use koldstore_parquet::{jsonb_cell_to_utf8, pg_bytea_hex, FlushMirrorRow};
use koldstore_schema::PgType;
use pgrx::datum::DatumWithOid;

/// Maps a catalog column type onto a flush keyset [`SqlParamType`].
///
/// # Errors
///
/// Returns an error when the type cannot be bound for ordered flush keyset paging.
pub(super) fn flush_keyset_param_type(pg_type: PgType) -> Result<SqlParamType, String> {
    match pg_type {
        PgType::Bool => Ok(SqlParamType::Boolean),
        PgType::Int2 | PgType::Int4 => Ok(SqlParamType::Integer),
        PgType::Int8 => Ok(SqlParamType::BigInt),
        PgType::Text | PgType::Numeric | PgType::Jsonb | PgType::Bytea | PgType::TextArray => {
            Ok(SqlParamType::Text)
        }
        PgType::Uuid => Ok(SqlParamType::Uuid),
        PgType::Timestamptz | PgType::Float4 | PgType::Float8 => Err(format!(
            "ordered flush keyset does not support primary-key type {pg_type:?}"
        )),
    }
}

/// Owned SPI bind values so [`DatumWithOid`] can borrow without temporary drop.
enum OwnedBind {
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Text(String),
    Bytes(Vec<u8>),
    Uuid(pgrx::Uuid),
}

/// Fetches one keyset page of mirror rows selected for flush.
///
/// `fetch_limit` is the SPI `LIMIT` (typically
/// [`koldstore_flush::flush_mirror_fetch_limit`]) so callers can keep peak
/// decode memory near one Parquet segment.
///
/// # Errors
///
/// Returns an error when SPI preparation or execution fails.
pub(super) fn fetch_mirror_batch(
    columns: &[CatalogColumn],
    primary_key_columns: &[String],
    statement: &SqlStatement,
    max_seq: i64,
    cursor: &MirrorFlushPageCursor,
    fetch_limit: i64,
    include_order_key: bool,
) -> Result<Vec<FlushMirrorRow>, String> {
    let limit = fetch_limit.max(1);
    let spi_statement = crate::spi::SpiStatement::read_with_params(
        statement.operation.as_str(),
        &statement.sql,
        statement.param_types.clone(),
    )
    .map_err(|error| error.to_string())?;

    let owned = build_owned_binds(columns, primary_key_columns, max_seq, cursor, limit)?;
    let args = owned_binds_to_datums(&owned);
    crate::spi::execute_prepared(&spi_statement, &args, |tuples| {
        decode_mirror_batch(tuples, columns, include_order_key)
    })
    .map_err(|error| error.to_string())
}

fn build_owned_binds(
    columns: &[CatalogColumn],
    primary_key_columns: &[String],
    max_seq: i64,
    cursor: &MirrorFlushPageCursor,
    limit: i64,
) -> Result<Vec<OwnedBind>, String> {
    match cursor {
        MirrorFlushPageCursor::AfterSeq { after_seq } => Ok(vec![
            OwnedBind::Int64(max_seq),
            OwnedBind::Int64(*after_seq),
            OwnedBind::Int64(limit),
        ]),
        MirrorFlushPageCursor::AfterOrderKey {
            after_order_key,
            after_pk_values,
            after_seq,
        } => {
            let first_page = after_order_key.is_none();
            if !first_page && after_pk_values.len() != primary_key_columns.len() {
                return Err(format!(
                    "ordered flush cursor has {} pk values but table has {}",
                    after_pk_values.len(),
                    primary_key_columns.len()
                ));
            }
            let mut binds = Vec::with_capacity(4 + primary_key_columns.len());
            binds.push(OwnedBind::Int64(max_seq));
            binds.push(OwnedBind::Bool(first_page));
            binds.push(OwnedBind::Bytes(
                after_order_key.clone().unwrap_or_default(),
            ));
            for (index, pk_name) in primary_key_columns.iter().enumerate() {
                let column = columns
                    .iter()
                    .find(|column| column.name == *pk_name)
                    .ok_or_else(|| {
                        format!("primary-key column `{pk_name}` missing from catalog")
                    })?;
                let value = if first_page {
                    default_cell_value(column.pg_type)
                } else {
                    after_pk_values
                        .get(index)
                        .cloned()
                        .unwrap_or(CellValue::Null)
                };
                binds.push(cell_value_to_owned_bind(&value, column.pg_type)?);
            }
            binds.push(OwnedBind::Int64(*after_seq));
            binds.push(OwnedBind::Int64(limit));
            Ok(binds)
        }
    }
}

fn owned_binds_to_datums(binds: &[OwnedBind]) -> Vec<DatumWithOid<'_>> {
    binds
        .iter()
        .map(|bind| match bind {
            OwnedBind::Bool(flag) => DatumWithOid::from(*flag),
            OwnedBind::Int32(n) => DatumWithOid::from(*n),
            OwnedBind::Int64(n) => DatumWithOid::from(*n),
            OwnedBind::Text(text) => DatumWithOid::from(text.as_str()),
            OwnedBind::Bytes(bytes) => DatumWithOid::from(bytes.as_slice()),
            OwnedBind::Uuid(uuid) => DatumWithOid::from(*uuid),
        })
        .collect()
}

fn default_cell_value(pg_type: PgType) -> CellValue {
    match pg_type {
        PgType::Bool => CellValue::Bool(false),
        PgType::Int2 => CellValue::Int16(0),
        PgType::Int4 => CellValue::Int32(0),
        PgType::Int8 | PgType::Timestamptz => CellValue::Int64(0),
        PgType::Float4 => CellValue::Float32(0.0),
        PgType::Float8 => CellValue::Float64(0.0),
        PgType::Text
        | PgType::Uuid
        | PgType::Jsonb
        | PgType::Bytea
        | PgType::Numeric
        | PgType::TextArray => CellValue::Utf8(String::new()),
    }
}

fn cell_value_to_owned_bind(value: &CellValue, pg_type: PgType) -> Result<OwnedBind, String> {
    match (pg_type, value) {
        (PgType::Bool, CellValue::Bool(flag)) => Ok(OwnedBind::Bool(*flag)),
        (PgType::Int2, CellValue::Int16(n)) => Ok(OwnedBind::Int32(i32::from(*n))),
        (PgType::Int2, CellValue::Int32(n)) => Ok(OwnedBind::Int32(*n)),
        (PgType::Int2, CellValue::Int64(n)) => {
            Ok(OwnedBind::Int32(i32::try_from(*n).map_err(|_| {
                "int2 keyset value out of range".to_string()
            })?))
        }
        (PgType::Int4, CellValue::Int32(n)) => Ok(OwnedBind::Int32(*n)),
        (PgType::Int4, CellValue::Int16(n)) => Ok(OwnedBind::Int32(i32::from(*n))),
        (PgType::Int4, CellValue::Int64(n)) => {
            Ok(OwnedBind::Int32(i32::try_from(*n).map_err(|_| {
                "int4 keyset value out of range".to_string()
            })?))
        }
        (PgType::Int8, CellValue::Int64(n)) => Ok(OwnedBind::Int64(*n)),
        (PgType::Int8, CellValue::Int32(n)) => Ok(OwnedBind::Int64(i64::from(*n))),
        (PgType::Int8, CellValue::Int16(n)) => Ok(OwnedBind::Int64(i64::from(*n))),
        (
            PgType::Text | PgType::Numeric | PgType::Jsonb | PgType::TextArray | PgType::Bytea,
            CellValue::Utf8(text),
        ) => Ok(OwnedBind::Text(text.clone())),
        (PgType::Uuid, CellValue::Utf8(text)) => {
            let uuid = uuid::Uuid::parse_str(text)
                .map_err(|error| format!("invalid uuid keyset value: {error}"))?;
            Ok(OwnedBind::Uuid(crate::spi::uuid_to_pgrx(uuid)))
        }
        (_, CellValue::Null) => {
            Err("ordered flush keyset primary key must not be null".to_string())
        }
        (pg_type, value) => Err(format!(
            "unsupported ordered flush keyset bind for {pg_type:?} value {value:?}"
        )),
    }
}

fn decode_mirror_batch(
    tuples: pgrx::spi::SpiTupleTable<'_>,
    columns: &[CatalogColumn],
    include_order_key: bool,
) -> pgrx::spi::Result<Vec<FlushMirrorRow>> {
    // Column layout from plan_mirror_flush_selection_batch:
    //   1..=N  application columns (catalog order)
    //   N+1    seq
    //   N+2    op
    //   N+3    order_key (optional)
    let seq_ordinal = columns.len() + 1;
    let op_ordinal = columns.len() + 2;
    let mut rows = Vec::with_capacity(tuples.len());
    for tuple in tuples {
        rows.push(decode_mirror_row(
            &tuple,
            columns,
            seq_ordinal,
            op_ordinal,
            include_order_key,
        )?);
    }
    Ok(rows)
}

fn decode_mirror_row(
    tuple: &pgrx::spi::SpiHeapTupleData<'_>,
    columns: &[CatalogColumn],
    seq_ordinal: usize,
    op_ordinal: usize,
    include_order_key: bool,
) -> pgrx::spi::Result<FlushMirrorRow> {
    // PERFORMANCE: Ordinal access avoids per-column name lookups (SPI_fnumber).
    let seq = tuple
        .get::<i64>(seq_ordinal)?
        .ok_or_else(|| crate::spi::missing_attribute("seq"))?;
    let op = tuple
        .get::<i16>(op_ordinal)?
        .ok_or_else(|| crate::spi::missing_attribute("op"))?;
    let mut values = Vec::with_capacity(columns.len());
    for (index, column) in columns.iter().enumerate() {
        values.push(read_column(tuple, column, index + 1)?);
    }
    let order_key = include_order_key
        .then(|| tuple.get::<Vec<u8>>(columns.len() + 3))
        .transpose()?
        .flatten();
    Ok(FlushMirrorRow {
        seq,
        op,
        values,
        order_key,
    })
}

fn read_column(
    tuple: &pgrx::spi::SpiHeapTupleData<'_>,
    column: &CatalogColumn,
    ordinal: usize,
) -> pgrx::spi::Result<CellValue> {
    let value = match column.pg_type {
        PgType::Bool => tuple
            .get::<bool>(ordinal)?
            .map(CellValue::Bool)
            .unwrap_or(CellValue::Null),
        PgType::Int2 => tuple
            .get::<i16>(ordinal)?
            .map(CellValue::Int16)
            .unwrap_or(CellValue::Null),
        PgType::Int4 => tuple
            .get::<i32>(ordinal)?
            .map(CellValue::Int32)
            .unwrap_or(CellValue::Null),
        PgType::Int8 => tuple
            .get::<i64>(ordinal)?
            .map(CellValue::Int64)
            .unwrap_or(CellValue::Null),
        PgType::Float4 => tuple
            .get::<f32>(ordinal)?
            .map(CellValue::Float32)
            .unwrap_or(CellValue::Null),
        PgType::Float8 => tuple
            .get::<f64>(ordinal)?
            .map(CellValue::Float64)
            .unwrap_or(CellValue::Null),
        PgType::Text => tuple
            .get::<String>(ordinal)?
            .map(CellValue::Utf8)
            .unwrap_or(CellValue::Null),
        PgType::Uuid => tuple
            .get::<pgrx::Uuid>(ordinal)?
            .map(|uuid| CellValue::Utf8(uuid.to_string()))
            .unwrap_or(CellValue::Null),
        PgType::Jsonb => tuple
            .get::<pgrx::JsonB>(ordinal)?
            .map(|json| CellValue::Utf8(jsonb_cell_to_utf8(&json.0)))
            .unwrap_or(CellValue::Null),
        PgType::Bytea => tuple
            .get::<Vec<u8>>(ordinal)?
            .map(|bytes| CellValue::Utf8(pg_bytea_hex(&bytes)))
            .unwrap_or(CellValue::Null),
        PgType::Numeric | PgType::TextArray => tuple
            .get::<String>(ordinal)?
            .map(CellValue::Utf8)
            .unwrap_or(CellValue::Null),
        PgType::Timestamptz => {
            // Keep CellValue in PostgreSQL-epoch micros (same as Datum / hot).
            // Arrow TimestampMicrosecond is Unix-epoch; convert here only.
            match tuple.get::<pgrx::datum::TimestampWithTimeZone>(ordinal)? {
                Some(timestamp) => CellValue::TimestamptzMicros(timestamp.into_inner()),
                None => CellValue::Null,
            }
        }
    };
    Ok(value)
}
