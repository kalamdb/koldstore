//! Flush job lifecycle helpers.

#[derive(serde::Deserialize)]
struct PendingFlushJobWire {
    id: String,
    #[serde(default)]
    force: bool,
}

pub(super) fn ensure_flush_job(
    table_oid: pgrx::pg_sys::Oid,
) -> Result<(uuid::Uuid, bool), String> {
    use pgrx::datum::DatumWithOid;

    let existing = pgrx::Spi::get_one_with_args::<String>(
        r#"
SELECT COALESCE((
    SELECT jsonb_build_object(
        'id', id::text,
        'force', COALESCE((payload->>'force')::boolean, false)
    )::text
    FROM koldstore.jobs
    WHERE table_oid = $1::oid
      AND scope_key = ''
      AND job_type = 'flush'
      AND status IN ('pending', 'running')
    ORDER BY updated_at, id
    LIMIT 1
), '')
"#,
        &[DatumWithOid::from(table_oid)],
    )
    .map_err(|error| error.to_string())?;
    if let Some(existing) = existing.filter(|value| !value.is_empty()) {
        let wire: PendingFlushJobWire = serde_json::from_str(&existing)
            .map_err(|error| error.to_string())?;
        return Ok((
            uuid::Uuid::parse_str(&wire.id).map_err(|error| error.to_string())?,
            wire.force,
        ));
    }

    let job_id = uuid::Uuid::new_v4();
    pgrx::Spi::run_with_args(
        r#"
INSERT INTO koldstore.jobs (
    id,
    table_oid,
    scope_key,
    job_type,
    status,
    phase,
    payload
)
VALUES (
    $1::uuid,
    $2::oid,
    '',
    'flush',
    'pending',
    'pending',
    jsonb_build_object('force', false)
)
"#,
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

    pgrx::Spi::run_with_args(
        r#"
UPDATE koldstore.jobs
SET status = 'running',
    phase = 'writing',
    attempts = CASE WHEN attempts = 0 THEN 1 ELSE attempts END,
    last_heartbeat_at = now(),
    updated_at = now()
WHERE id = $1::uuid
  AND table_oid = $2::oid
  AND job_type = 'flush'
  AND status IN ('pending', 'running')
"#,
        &[
            DatumWithOid::from(pgrx::Uuid::from_bytes(*job_id.as_bytes())),
            DatumWithOid::from(table_oid),
        ],
    )
    .map_err(|error| error.to_string())
}

pub(super) fn mark_flush_job_completed(
    job_id: uuid::Uuid,
    table_oid: pgrx::pg_sys::Oid,
    rows_flushed: i64,
    checkpoint_seq: i64,
    checkpoint_commit_seq: i64,
) -> Result<(), String> {
    use pgrx::datum::DatumWithOid;

    pgrx::Spi::run_with_args(
        r#"
UPDATE koldstore.jobs
SET status = 'completed',
    phase = 'finished',
    rows_processed = $3::bigint,
    rows_flushed = $3::bigint,
    checkpoint_seq = $4::bigint,
    checkpoint_commit_seq = $5::bigint,
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_heartbeat_at = now(),
    updated_at = now()
WHERE id = $1::uuid
  AND table_oid = $2::oid
  AND job_type = 'flush'
  AND status IN ('pending', 'running')
"#,
        &[
            DatumWithOid::from(pgrx::Uuid::from_bytes(*job_id.as_bytes())),
            DatumWithOid::from(table_oid),
            DatumWithOid::from(rows_flushed),
            DatumWithOid::from(checkpoint_seq),
            DatumWithOid::from(checkpoint_commit_seq),
        ],
    )
    .map_err(|error| error.to_string())
}
