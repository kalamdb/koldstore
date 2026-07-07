//! Mirror row selection and cold-record planning for flush writes.

use koldstore_common::QualifiedTableName;
use koldstore_flush::ops::plan_mirror_flush_selection;
use koldstore_mirror::MirrorSelectionRow;

pub(super) struct FlushWriteInput {
    pub columns: Vec<koldstore_parquet::PgColumn>,
    pub rows: Vec<koldstore_parquet::CleanColdRecordPlan>,
    pub cleanup_rows: Vec<serde_json::Value>,
}

pub(super) struct FlushWriteChunk {
    pub rows: Vec<koldstore_parquet::CleanColdRecordPlan>,
    pub cleanup_rows: Vec<serde_json::Value>,
}

pub(super) fn chunk_flush_write_input(
    write_input: &FlushWriteInput,
    max_rows_per_file: usize,
) -> Vec<FlushWriteChunk> {
    if write_input.rows.is_empty() {
        return Vec::new();
    }
    let chunk_size = max_rows_per_file.max(1);
    write_input
        .rows
        .chunks(chunk_size)
        .zip(write_input.cleanup_rows.chunks(chunk_size))
        .map(|(rows, cleanup_rows)| FlushWriteChunk {
            rows: rows.to_vec(),
            cleanup_rows: cleanup_rows.to_vec(),
        })
        .collect()
}

pub(super) fn flush_write_input(
    table_oid: pgrx::pg_sys::Oid,
    schema_version: u32,
    primary_key_columns: &[String],
    columns: &[koldstore_migrate::order::CatalogColumn],
    max_seq: i64,
) -> Result<FlushWriteInput, String> {
    use pgrx::datum::DatumWithOid;

    let relation = crate::catalog::resolve::qualified_relation_name(table_oid)?;
    let table = QualifiedTableName::parse(&relation).map_err(|error| error.to_string())?;
    let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "managed schema has no change-log mirror".to_string())?;
    let mirror = QualifiedTableName::from_table_name(&snapshot.mirror_relation);
    let base_columns = columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let selection =
        plan_mirror_flush_selection(&table, &mirror, primary_key_columns, &base_columns, None)
            .map_err(|error| error.to_string())?;
    let statement = crate::spi::SpiStatement::read_with_params(
        "select flush rows as json",
        &format!(
            "SELECT COALESCE(jsonb_agg(to_jsonb(selected) ORDER BY selected.seq)::text, '[]') FROM ({}) AS selected",
            selection.statement.sql
        ),
        selection.statement.param_types.clone(),
    )
    .map_err(|error| error.to_string())?;
    let json = crate::spi::execute_prepared(
        &statement,
        &[DatumWithOid::from(max_seq)],
        crate::spi::first_row::<String>,
    )
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "flush row selection returned no rows".to_string())?;
    let values: Vec<MirrorSelectionRow> =
        serde_json::from_str(&json).map_err(|error| error.to_string())?;
    let columns = columns
        .iter()
        .map(|column| {
            koldstore_parquet::PgColumn::new(column.name.clone(), column.pg_type, true)
        })
        .collect::<Vec<_>>();
    let rows = values
        .iter()
        .cloned()
        .map(|value| flush_row_plan(value, &base_columns, primary_key_columns, schema_version))
        .collect::<Result<Vec<_>, _>>()?;
    let cleanup_rows = values
        .iter()
        .map(|value| cleanup_row_json(value, primary_key_columns))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(FlushWriteInput {
        columns,
        rows,
        cleanup_rows,
    })
}

fn flush_row_plan(
    row: MirrorSelectionRow,
    base_columns: &[String],
    primary_key_columns: &[String],
    schema_version: u32,
) -> Result<koldstore_parquet::CleanColdRecordPlan, String> {
    let op = i16::try_from(row.op).map_err(|error| error.to_string())?;
    let row_values = base_columns
        .iter()
        .map(|column| {
            (
                column.clone(),
                row.fields
                    .get(column)
                    .cloned()
                    .unwrap_or(serde_json::Value::Null),
            )
        })
        .collect::<Vec<_>>();
    koldstore_parquet::plan_clean_cold_record(
        row_values,
        primary_key_columns,
        row.seq,
        op,
        schema_version,
    )
}

fn cleanup_row_json(
    row: &MirrorSelectionRow,
    primary_key_columns: &[String],
) -> Result<serde_json::Value, String> {
    let op = i16::try_from(row.op).map_err(|error| error.to_string())?;
    let mut cleanup = serde_json::Map::new();
    for column in primary_key_columns {
        let value = row
            .fields
            .get(column)
            .ok_or_else(|| format!("flush row is missing primary-key field `{column}`"))?;
        cleanup.insert(
            column.clone(),
            serde_json::Value::String(value_to_cleanup_text(value)?),
        );
    }
    cleanup.insert("seq".to_string(), serde_json::json!(row.seq));
    cleanup.insert("op".to_string(), serde_json::json!(op));
    Ok(serde_json::Value::Object(cleanup))
}

fn value_to_cleanup_text(value: &serde_json::Value) -> Result<String, String> {
    match value {
        serde_json::Value::Null => {
            Err("cleanup row cannot contain null primary-key values".to_string())
        }
        serde_json::Value::String(text) => Ok(text.clone()),
        serde_json::Value::Number(number) => Ok(number.to_string()),
        serde_json::Value::Bool(flag) => Ok(flag.to_string()),
        other => serde_json::to_string(other).map_err(|error| error.to_string()),
    }
}
