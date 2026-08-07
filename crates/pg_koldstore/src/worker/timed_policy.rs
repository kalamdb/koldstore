//! Exact clock scheduling for time-based auto-flush policies.
//!
//! `OlderThan` can become eligible while the database is otherwise idle, but it
//! does not require polling. Mirror Snowflake ids encode time. One bounded
//! index-ordered scan computes both current eligibility and the timestamp when
//! the minimum viable batch will become old enough.

use koldstore_common::{
    minimum_id_at_unix_millis, quote_qualified_ident, unix_millis_from_id, FlushPolicy, MoveAfter,
};
use pgrx::datum::DatumWithOid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OlderThanEvaluation {
    pub due: bool,
    pub next_due_at_ms: Option<i64>,
}

impl OlderThanEvaluation {
    const NOT_APPLICABLE: Self = Self {
        due: false,
        next_due_at_ms: None,
    };
}

/// Evaluates one `OlderThan` policy with a single bounded mirror seq-index scan.
///
/// The scan is capped by `max_rows_per_flush`, so eligibility never materializes
/// the whole mirror. It returns both:
/// - how many of the bounded oldest rows are already older than the cutoff; and
/// - the sequence of the last row required for the minimum viable batch.
///
/// Therefore a not-yet-due table does not need a second mirror query to schedule
/// its next exact clock wake.
pub(crate) fn evaluate_older_than(
    table_oid: pgrx::pg_sys::Oid,
    policy: &FlushPolicy,
) -> Result<OlderThanEvaluation, String> {
    let FlushPolicy::OlderThan {
        age,
        min_flush_rows,
        max_rows_per_file,
        max_rows_per_flush,
    } = policy
    else {
        return Ok(OlderThanEvaluation::NOT_APPLICABLE);
    };

    let required_rows = (*min_flush_rows).max(*max_rows_per_file).max(1);
    if required_rows > *max_rows_per_flush || *max_rows_per_flush == 0 {
        // Time alone cannot make an internally inconsistent policy flushable.
        return Ok(OlderThanEvaluation::NOT_APPLICABLE);
    }
    let scan_rows = i64::try_from(*max_rows_per_flush)
        .map_err(|_| "OlderThan max_rows_per_flush exceeds PostgreSQL bigint range".to_string())?;
    let required_rows_i64 = i64::try_from(required_rows)
        .map_err(|_| "OlderThan minimum batch exceeds PostgreSQL bigint range".to_string())?;

    let cutoff_ms = subtract_move_after_from_now_ms(*age)?;
    let Some(cutoff_seq) = minimum_id_at_unix_millis(cutoff_ms) else {
        return Ok(OlderThanEvaluation::NOT_APPLICABLE);
    };

    let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "managed schema has no change-log mirror".to_string())?;
    let mirror = quote_qualified_ident(&snapshot.mirror_relation);

    let (eligible_count, threshold_count, threshold_seq) = pgrx::Spi::connect(|client| {
        let sql = format!(
            r#"
WITH oldest AS MATERIALIZED (
    SELECT seq
    FROM {mirror}
    ORDER BY seq
    LIMIT $1
),
eligible AS (
    SELECT count(*)::bigint AS row_count
    FROM oldest
    WHERE seq < $2
),
threshold_rows AS (
    SELECT seq
    FROM oldest
    ORDER BY seq
    LIMIT $3
)
SELECT (SELECT row_count FROM eligible)::bigint,
       (SELECT count(*)::bigint FROM threshold_rows),
       (SELECT max(seq)::bigint FROM threshold_rows)
"#
        );
        let row = client
            .select(
                &sql,
                None,
                &[
                    DatumWithOid::from(scan_rows),
                    DatumWithOid::from(cutoff_seq),
                    DatumWithOid::from(required_rows_i64),
                ],
            )
            .map_err(|error| error.to_string())?
            .first();
        let eligible_count = row
            .get::<i64>(1)
            .map_err(|error| error.to_string())?
            .unwrap_or(0);
        let threshold_count = row
            .get::<i64>(2)
            .map_err(|error| error.to_string())?
            .unwrap_or(0);
        let threshold_seq = row.get::<i64>(3).map_err(|error| error.to_string())?;
        Ok::<_, String>((eligible_count, threshold_count, threshold_seq))
    })?;

    let due = eligible_count >= i64::try_from(*min_flush_rows).unwrap_or(i64::MAX)
        && koldstore_flush::selected_rows_meet_file_minimum(
            u64::try_from(eligible_count.max(0)).unwrap_or(0),
            *max_rows_per_file,
        );
    if due {
        return Ok(OlderThanEvaluation {
            due: true,
            next_due_at_ms: None,
        });
    }

    if threshold_count < required_rows_i64 {
        // Not enough rows exist yet. Time cannot create rows; the next source WAL
        // event will re-evaluate this touched database.
        return Ok(OlderThanEvaluation::NOT_APPLICABLE);
    }
    let Some(threshold_seq) = threshold_seq else {
        return Ok(OlderThanEvaluation::NOT_APPLICABLE);
    };
    let threshold_ms = unix_millis_from_id(threshold_seq)
        .ok_or_else(|| format!("invalid mirror Snowflake sequence {threshold_seq}"))?;
    let next_due_at_ms = add_move_after_ms(threshold_ms, *age)?;
    Ok(OlderThanEvaluation {
        due: false,
        next_due_at_ms: Some(next_due_at_ms),
    })
}

fn subtract_move_after_from_now_ms(age: MoveAfter) -> Result<i64, String> {
    pgrx::Spi::get_one_with_args::<i64>(
        "SELECT floor(extract(epoch FROM (\
             statement_timestamp() - \
             make_interval(\
                 months => $1::int, \
                 days => $2::int, \
                 secs => $3::double precision / 1000000.0\
             )\
         )) * 1000)::bigint",
        &[
            DatumWithOid::from(age.months),
            DatumWithOid::from(age.days),
            DatumWithOid::from(age.microseconds),
        ],
    )
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "failed to compute OlderThan cutoff timestamp".to_string())
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
