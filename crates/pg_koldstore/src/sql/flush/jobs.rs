//! Flush job lifecycle SPI adapters.

use koldstore_flush::{
    plan_insert_inline_flush_job, plan_lookup_active_inline_flush_job,
    plan_mark_inline_flush_job_completed, plan_mark_inline_flush_job_failed,
    plan_mark_inline_flush_job_running,
};

#[derive(serde::Deserialize)]
struct PendingFlushJobWire {
    id: String,
    #[serde(default)]
    force: bool,
}

pub(super) fn ensure_flush_job(table_oid: pgrx::pg_sys::Oid) -> Result<(uuid::Uuid, bool), String> {
    use pgrx::datum::DatumWithOid;

    let lookup = plan_lookup_active_inline_flush_job().map_err(|error| error.to_string())?;
    let existing = crate::spi::select_one::<String>(&lookup, &[DatumWithOid::from(table_oid)])
        .map_err(|error| error.to_string())?
        .filter(|value| !value.is_empty());
    if let Some(existing) = existing {
        let wire: PendingFlushJobWire =
            serde_json::from_str(&existing).map_err(|error| error.to_string())?;
        return Ok((
            uuid::Uuid::parse_str(&wire.id).map_err(|error| error.to_string())?,
            wire.force,
        ));
    }

    let job_id = uuid::Uuid::new_v4();
    let insert = plan_insert_inline_flush_job().map_err(|error| error.to_string())?;
    crate::spi::update(
        &insert,
        &[
            DatumWithOid::from(pgrx::Uuid::from_bytes(*job_id.as_bytes())),
            DatumWithOid::from(table_oid),
        ],
    )
    .map_err(|error| error.to_string())?;
    Ok((job_id, false))
}

pub(super) fn mark_flush_job_running(
    job_id: uuid::Uuid,
    table_oid: pgrx::pg_sys::Oid,
) -> Result<(), String> {
    use pgrx::datum::DatumWithOid;

    let statement = plan_mark_inline_flush_job_running().map_err(|error| error.to_string())?;
    crate::spi::update(
        &statement,
        &[
            DatumWithOid::from(pgrx::Uuid::from_bytes(*job_id.as_bytes())),
            DatumWithOid::from(table_oid),
        ],
    )
    .map_err(|error| error.to_string())?;
    Ok(())
}

pub(super) fn mark_flush_job_completed(
    job_id: uuid::Uuid,
    table_oid: pgrx::pg_sys::Oid,
    rows_flushed: i64,
    checkpoint_seq: i64,
    checkpoint_commit_seq: i64,
) -> Result<(), String> {
    use pgrx::datum::DatumWithOid;

    let statement = plan_mark_inline_flush_job_completed().map_err(|error| error.to_string())?;
    crate::spi::update(
        &statement,
        &[
            DatumWithOid::from(pgrx::Uuid::from_bytes(*job_id.as_bytes())),
            DatumWithOid::from(table_oid),
            DatumWithOid::from(rows_flushed),
            DatumWithOid::from(checkpoint_seq),
            DatumWithOid::from(checkpoint_commit_seq),
        ],
    )
    .map_err(|error| error.to_string())?;
    Ok(())
}

pub(super) fn mark_flush_job_failed(
    job_id: uuid::Uuid,
    table_oid: pgrx::pg_sys::Oid,
    error_trace: &str,
) -> Result<(), String> {
    use pgrx::datum::DatumWithOid;

    let statement = plan_mark_inline_flush_job_failed().map_err(|error| error.to_string())?;
    crate::spi::update(
        &statement,
        &[
            DatumWithOid::from(pgrx::Uuid::from_bytes(*job_id.as_bytes())),
            DatumWithOid::from(table_oid),
            DatumWithOid::from(error_trace),
        ],
    )
    .map_err(|error| error.to_string())?;
    Ok(())
}
