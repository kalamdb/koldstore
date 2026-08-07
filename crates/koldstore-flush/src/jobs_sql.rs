//! Shared SQL fragments for flush job rows in `koldstore.jobs`.
//!
//! Keeps enqueue (`ops`) and flush job lifecycle (`table_jobs`) aligned on the
//! same active-job status / conflict predicates without merging their APIs.

macro_rules! active_flush_job_statuses_sql {
    () => {
        "status IN ('pending', 'running')"
    };
}

/// Partial unique-index predicate for one active flush job per table/scope.
///
/// Used by `enqueue_flush_job` `ON CONFLICT … WHERE` and mirrored in lookup SQL.
pub const ACTIVE_FLUSH_JOB_CONFLICT_PREDICATE: &str =
    concat!("job_type = 'flush' AND ", active_flush_job_statuses_sql!());

/// Builds a `mirror."op"` filter for flush selection / cleanup SQL.
///
/// Returns `None` when `ops` is empty (no op restriction).
#[must_use]
pub(crate) fn mirror_ops_where_clause(ops: &[i16]) -> Option<String> {
    if ops.is_empty() {
        return None;
    }
    Some(if ops.len() == 1 {
        format!("mirror.\"op\" = {}", ops[0])
    } else {
        let literals = ops
            .iter()
            .map(i16::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        format!("mirror.\"op\" IN ({literals})")
    })
}
