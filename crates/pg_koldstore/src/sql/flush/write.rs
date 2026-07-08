//! Mirror row selection SPI adapter for flush writes.

use koldstore_common::QualifiedTableName;
use koldstore_flush::{ops::plan_mirror_flush_selection, plan_flush_write_input};
use koldstore_mirror::MirrorSelectionRow;

pub(super) use koldstore_flush::{chunk_flush_write_input, FlushWriteChunk, FlushWriteInput};

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
        .map(|column| koldstore_parquet::PgColumn::new(column.name.clone(), column.pg_type, true))
        .collect::<Vec<_>>();
    plan_flush_write_input(
        values,
        columns,
        &base_columns,
        primary_key_columns,
        schema_version,
    )
}
