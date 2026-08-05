//! Table integrity verification SPI adapters and SQL entrypoint.
//!
//! Plans live in `koldstore-catalog::integrity`; this module executes them and
//! exposes `koldstore.verify_table_integrity`.

use koldstore_catalog::plan_verify_table_integrity;

/// Runs cheap catalog integrity checks for one managed table.
///
/// SQL contract:
/// `koldstore.verify_table_integrity(table_name regclass) → jsonb`.
///
/// Returns a structured report:
/// `{ "table_oid": <oid>, "ok": <bool>, "checks": [ { "name", "ok", "detail" }, ... ] }`.
///
/// v1 checks (flag only; never auto-repair):
/// - exactly one active managed schema
/// - at most one active flush job (`pending`/`running`)
/// - no active cold segments missing checksum/path
/// - pending segments older than `koldstore.pending_segment_ttl_seconds`
/// - active segment row-group array cardinality matches `row_group_count`
/// - no duplicate active `(writer_job_id, pass_id, segment_ordinal)`
/// - active segments have positive `byte_size`
///
/// Segment-id samples in details are capped (see catalog plan) with `truncated`.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(
    name = "verify_table_integrity",
    schema = "koldstore",
    security_definer
)]
pub fn verify_table_integrity_pg(table_name: pgrx::PgRelation) -> pgrx::JsonB {
    verify_table_integrity_impl(table_name.oid())
        .map(pgrx::JsonB)
        .unwrap_or_else(|error| pgrx::error!("verify table integrity failed: {error}"))
}

#[cfg(feature = "pg")]
fn verify_table_integrity_impl(
    table_oid: pgrx::pg_sys::Oid,
) -> Result<serde_json::Value, String> {
    use pgrx::datum::DatumWithOid;

    let statement = plan_verify_table_integrity().map_err(|error| error.to_string())?;
    let ttl_seconds = crate::guc::pending_segment_ttl_seconds();
    let text = crate::spi::select_one::<String>(
        &statement,
        &[
            DatumWithOid::from(table_oid),
            DatumWithOid::from(ttl_seconds),
        ],
    )
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "verify_table_integrity returned no row".to_string())?;
    serde_json::from_str(&text).map_err(|error| error.to_string())
}
