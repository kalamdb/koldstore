//! Exact clock deadlines for time-based auto-flush policies.
//!
//! `OlderThan` can become eligible while the database is otherwise idle.  The
//! scheduler therefore needs a clock wake, but it does not need polling: the
//! Snowflake sequence encodes the mutation timestamp.  We inspect only the
//! minimum batch of oldest mirror rows required to produce a real Parquet file
//! and arm the supervisor for the instant that batch crosses the age boundary.

use koldstore_common::{quote_qualified_ident, unix_millis_from_id, FlushPolicy, MoveAfter};
use pgrx::datum::DatumWithOid;

/// Returns the earliest wall-clock instant at which the current mirror can
/// satisfy an `OlderThan` flush without any additional DML.
///
/// `None` means either this is not an `OlderThan` policy or the mirror does not
/// yet contain enough rows to form the minimum flushable batch.  In that case
/// future source WAL will wake maintenance and recompute the deadline.
pub(crate) fn next_older_than_due_at_ms(
    table_oid: pgrx::pg_sys::Oid,
    policy: &FlushPolicy,
) -> Result<Option<i64>, String> {
    let FlushPolicy::OlderThan {
        age,
        min_flush_rows,
        max_rows_per_file,
        max_rows_per_flush,
    } = policy
    else {
        return Ok(None);
    };

    let required_rows = (*min_flush_rows).max(*max_rows_per_file).max(1);
    if required_rows > *max_rows_per_flush {
        // A persisted policy with mutually incompatible bounds cannot become
        // flushable merely by waiting.  New configuration/DML will re-evaluate.
        return Ok(None);
    }
    let required_rows_i64 = i64::try_from(required_rows)
        .map_err(|_| "OlderThan minimum batch exceeds PostgreSQL bigint range".to_string())?;

    let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "managed schema has no change-log mirror".to_string())?;
    let mirror = quote_qualified_ident(&snapshot.mirror_relation);

    // Read only the first `required_rows` entries through the mirror seq index.
    // The max seq of this bounded prefix is the last row needed to form the
    // minimum viable flush batch.  If fewer rows exist, time alone cannot make
    // this table due, so no clock wake is necessary.
    let (count, threshold_seq) = pgrx::Spi::connect(|client| {
        let sql = format!(
            "SELECT count(*)::bigint, max(seq)::bigint \
             FROM (SELECT seq FROM {mirror} ORDER BY seq LIMIT $1) oldest"
        );
        let row = client
            .select(&sql, None, &[DatumWithOid::from(required_rows_i64)])
            .map_err(|error| error.to_string())?
            .first();
        let count = row
            .get::<i64>(1)
            .map_err(|error| error.to_string())?
            .unwrap_or(0);
        let seq = row.get::<i64>(2).map_err(|error| error.to_string())?;
        Ok::<_, String>((count, seq))
    })?;

    if count < required_rows_i64 {
        return Ok(None);
    }
    let Some(threshold_seq) = threshold_seq else {
        return Ok(None);
    };
    let threshold_ms = unix_millis_from_id(threshold_seq)
        .ok_or_else(|| format!("invalid mirror Snowflake sequence {threshold_seq}"))?;
    add_move_after_ms(threshold_ms, *age).map(Some)
}

/// Adds the persisted PostgreSQL interval with PostgreSQL's own calendar rules.
///
/// Months/days deliberately stay out of Rust duration arithmetic: `MoveAfter`
/// preserves native interval components, so PostgreSQL remains authoritative for
/// calendar-month and DST semantics.
fn add_move_after_ms(timestamp_ms: i64, age: MoveAfter) -> Result<i64, String> {
    pgrx::Spi::get_one_with_args::<i64>(
        "SELECT floor(extract(epoch FROM (\
             to_timestamp($1::double precision / 1000.0) + \
             make_interval(\
                 months => $2::int, \
                 days => $3::int, \
                 secs => $4::double precision / 1000000.0\
             )\
         )) * 1000)::bigint",
        &[
            DatumWithOid::from(timestamp_ms),
            DatumWithOid::from(age.months),
            DatumWithOid::from(age.days),
            DatumWithOid::from(age.microseconds),
        ],
    )
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "failed to compute OlderThan next due timestamp".to_string())
}
