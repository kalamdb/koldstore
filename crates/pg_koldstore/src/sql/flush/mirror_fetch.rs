//! SPI mirror-row fetch and typed tuple decode for flush.

use koldstore_common::SqlStatement;
use koldstore_migrate::order::CatalogColumn;
use koldstore_parquet::{FlushColumnValue, FlushMirrorRow};
use koldstore_schema::PgType;

/// Microseconds between the Unix epoch (1970-01-01) and the PostgreSQL epoch
/// (2000-01-01). Used to convert `timestamptz` datums without string round-trips.
const PG_EPOCH_OFFSET_MICROS: i64 = 946_684_800_000_000;

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
    statement: &SqlStatement,
    max_seq: i64,
    after_seq: i64,
    fetch_limit: i64,
) -> Result<Vec<FlushMirrorRow>, String> {
    use pgrx::datum::DatumWithOid;

    let limit = fetch_limit.max(1);
    let spi_statement = crate::spi::SpiStatement::read_with_params(
        statement.operation.as_str(),
        &statement.sql,
        statement.param_types.clone(),
    )
    .map_err(|error| error.to_string())?;
    crate::spi::execute_prepared(
        &spi_statement,
        &[
            DatumWithOid::from(max_seq),
            DatumWithOid::from(after_seq),
            DatumWithOid::from(limit),
        ],
        |tuples| decode_mirror_batch(tuples, columns),
    )
    .map_err(|error| error.to_string())
}

fn decode_mirror_batch(
    tuples: pgrx::spi::SpiTupleTable<'_>,
    columns: &[CatalogColumn],
) -> pgrx::spi::Result<Vec<FlushMirrorRow>> {
    // Column layout from plan_mirror_flush_selection_batch:
    //   1..=N  application columns (catalog order)
    //   N+1    seq
    //   N+2    op
    //   N+3    deleted
    let seq_ordinal = columns.len() + 1;
    let op_ordinal = columns.len() + 2;
    let mut rows = Vec::with_capacity(tuples.len());
    for tuple in tuples {
        rows.push(decode_mirror_row(&tuple, columns, seq_ordinal, op_ordinal)?);
    }
    Ok(rows)
}

fn decode_mirror_row(
    tuple: &pgrx::spi::SpiHeapTupleData<'_>,
    columns: &[CatalogColumn],
    seq_ordinal: usize,
    op_ordinal: usize,
) -> pgrx::spi::Result<FlushMirrorRow> {
    // PERFORMANCE: Ordinal access avoids per-column name lookups (SPI_fnumber).
    let seq = tuple
        .get::<i64>(seq_ordinal)?
        .ok_or_else(|| missing_attribute("seq"))?;
    let op = tuple
        .get::<i16>(op_ordinal)?
        .ok_or_else(|| missing_attribute("op"))?;
    let mut values = Vec::with_capacity(columns.len());
    for (index, column) in columns.iter().enumerate() {
        values.push(read_column(tuple, column, index + 1)?);
    }
    Ok(FlushMirrorRow { seq, op, values })
}

fn missing_attribute(name: &str) -> pgrx::spi::SpiError {
    pgrx::spi::SpiError::DatumError(pgrx::datum::TryFromDatumError::NoSuchAttributeName(
        name.to_string(),
    ))
}

fn read_column(
    tuple: &pgrx::spi::SpiHeapTupleData<'_>,
    column: &CatalogColumn,
    ordinal: usize,
) -> pgrx::spi::Result<FlushColumnValue> {
    let value = match column.pg_type {
        PgType::Bool => tuple
            .get::<bool>(ordinal)?
            .map(FlushColumnValue::Bool)
            .unwrap_or(FlushColumnValue::Null),
        PgType::Int2 => tuple
            .get::<i16>(ordinal)?
            .map(FlushColumnValue::Int16)
            .unwrap_or(FlushColumnValue::Null),
        PgType::Int4 => tuple
            .get::<i32>(ordinal)?
            .map(FlushColumnValue::Int32)
            .unwrap_or(FlushColumnValue::Null),
        PgType::Int8 => tuple
            .get::<i64>(ordinal)?
            .map(FlushColumnValue::Int64)
            .unwrap_or(FlushColumnValue::Null),
        PgType::Float4 => tuple
            .get::<f32>(ordinal)?
            .map(FlushColumnValue::Float32)
            .unwrap_or(FlushColumnValue::Null),
        PgType::Float8 => tuple
            .get::<f64>(ordinal)?
            .map(FlushColumnValue::Float64)
            .unwrap_or(FlushColumnValue::Null),
        PgType::Text => tuple
            .get::<String>(ordinal)?
            .map(FlushColumnValue::Utf8)
            .unwrap_or(FlushColumnValue::Null),
        PgType::Uuid => tuple
            .get::<pgrx::Uuid>(ordinal)?
            .map(|uuid| FlushColumnValue::Utf8(uuid.to_string()))
            .unwrap_or(FlushColumnValue::Null),
        PgType::Jsonb => tuple
            .get::<pgrx::JsonB>(ordinal)?
            .map(|json| FlushColumnValue::Utf8(json_to_utf8(&json.0)))
            .unwrap_or(FlushColumnValue::Null),
        PgType::Bytea => tuple
            .get::<Vec<u8>>(ordinal)?
            .map(|bytes| FlushColumnValue::Utf8(bytea_to_pg_hex(&bytes)))
            .unwrap_or(FlushColumnValue::Null),
        PgType::Numeric | PgType::TextArray => tuple
            .get::<String>(ordinal)?
            .map(FlushColumnValue::Utf8)
            .unwrap_or(FlushColumnValue::Null),
        PgType::Timestamptz => {
            // PERFORMANCE: Convert PG epoch micros → Unix micros directly.
            // Avoids to_iso_string + chrono RFC3339 parse per timestamp cell.
            match tuple.get::<pgrx::datum::TimestampWithTimeZone>(ordinal)? {
                Some(timestamp) => {
                    let pg_micros = timestamp.into_inner();
                    FlushColumnValue::TimestamptzMicros(
                        pg_micros.saturating_add(PG_EPOCH_OFFSET_MICROS),
                    )
                }
                None => FlushColumnValue::Null,
            }
        }
    };
    Ok(value)
}

fn json_to_utf8(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(text) => text.clone(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

/// Encodes raw bytes in PostgreSQL `bytea` hex output form (`\xdeadbeef`).
fn bytea_to_pg_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(2 + bytes.len() * 2);
    out.push_str("\\x");
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}
