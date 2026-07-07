//! Flush statistics and policy resolution.

use koldstore_common::SeqId;
use koldstore_flush::policy::{select_mirror_flush_candidates, FlushPolicy, MirrorPolicyRow};
use koldstore_mirror::{MirrorPolicyRowJson, MirrorSeqStats};

#[derive(Debug)]
pub(super) struct FlushStats {
    pub row_count: i64,
    pub min_seq: i64,
    pub max_seq: i64,
    pub min_commit_seq: i64,
    pub max_commit_seq: i64,
}

impl From<MirrorSeqStats> for FlushStats {
    fn from(stats: MirrorSeqStats) -> Self {
        Self {
            row_count: stats.row_count,
            min_seq: stats.min_seq,
            max_seq: stats.max_seq,
            min_commit_seq: stats.min_commit_seq,
            max_commit_seq: stats.max_commit_seq,
        }
    }
}

pub(super) fn flush_stats(table_oid: pgrx::pg_sys::Oid) -> Result<FlushStats, String> {
    use koldstore_mirror::{plan_mirror_stats, MirrorRelation};

    let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "managed schema has no change-log mirror".to_string())?;
    let mirror = MirrorRelation::new(snapshot.mirror_relation);
    let stats = koldstore_mirror::mirror_to_sql(plan_mirror_stats(&mirror))
        .map_err(|error| error.to_string())?;
    let json = crate::spi::execute_prepared(&stats, &[], crate::spi::first_row::<String>)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "flush stats lookup returned no rows".to_string())?;
    let stats: MirrorSeqStats =
        serde_json::from_str(&json).map_err(|error| error.to_string())?;
    Ok(stats.into())
}

pub(super) fn resolve_flush_stats(
    table_oid: pgrx::pg_sys::Oid,
    force: bool,
) -> Result<FlushStats, String> {
    let all = flush_stats(table_oid)?;
    if all.row_count == 0 || force {
        return Ok(all);
    }
    let Some(policy) = active_flush_policy(table_oid)? else {
        return Ok(all);
    };
    let rows = load_mirror_policy_rows(table_oid)?;
    let candidates = select_mirror_flush_candidates(&policy, &rows);
    if candidates.is_empty() {
        return FlushStats::empty();
    }
    let seqs = candidates
        .iter()
        .map(|row| row.seq.get())
        .collect::<Vec<_>>();
    let min_seq = *seqs.iter().min().expect("flush candidates are non-empty");
    let max_seq = *seqs.iter().max().expect("flush candidates are non-empty");
    Ok(FlushStats {
        row_count: i64::try_from(candidates.len()).map_err(|error| error.to_string())?,
        min_seq,
        max_seq,
        min_commit_seq: min_seq,
        max_commit_seq: max_seq,
    })
}

pub(super) fn active_flush_policy(
    table_oid: pgrx::pg_sys::Oid,
) -> Result<Option<FlushPolicy>, String> {
    use pgrx::datum::DatumWithOid;

    let options = pgrx::Spi::get_one_with_args::<pgrx::JsonB>(
        r#"
SELECT options
FROM koldstore.schemas
WHERE table_oid = $1::oid
  AND active
ORDER BY version DESC
LIMIT 1
"#,
        &[DatumWithOid::from(table_oid)],
    )
    .map_err(|error| error.to_string())?;
    let Some(options) = options else {
        return Ok(None);
    };
    Ok(FlushPolicy::from_value(&options.0))
}

fn load_mirror_policy_rows(table_oid: pgrx::pg_sys::Oid) -> Result<Vec<MirrorPolicyRow>, String> {
    use koldstore_mirror::MirrorRelation;

    let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "managed schema has no change-log mirror".to_string())?;
    let mirror = MirrorRelation::new(snapshot.mirror_relation);
    let pk_json = snapshot
        .primary_key_columns
        .iter()
        .map(|column| format!("'{column}', mirror.\"{column}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        r#"
SELECT COALESCE(jsonb_agg(
    jsonb_build_object(
        'pk_json', jsonb_build_object({pk_json}),
        'seq', mirror."seq"
    )
    ORDER BY mirror."seq"
)::text, '[]')
FROM {mirror} AS mirror
"#,
        mirror = mirror.quoted()
    );
    let json = pgrx::Spi::get_one::<String>(&sql)
        .map_err(|error| error.to_string())?
        .unwrap_or_else(|| "[]".to_string());
    let values: Vec<MirrorPolicyRowJson> =
        serde_json::from_str(&json).map_err(|error| error.to_string())?;
    values
        .into_iter()
        .map(|row| {
            Ok(MirrorPolicyRow {
                pk_json: row.pk_json,
                seq: SeqId::new(row.seq).map_err(|error| error.to_string())?,
            })
        })
        .collect::<Result<Vec<_>, _>>()
}

impl FlushStats {
    const fn empty() -> Result<Self, String> {
        Ok(Self {
            row_count: 0,
            min_seq: 0,
            max_seq: 0,
            min_commit_seq: 0,
            max_commit_seq: 0,
        })
    }
}

pub(super) fn flush_stats_for_rows(
    rows: &[koldstore_parquet::CleanColdRecordPlan],
) -> Result<FlushStats, String> {
    let seqs = rows
        .iter()
        .map(|row| {
            row.values
                .get(koldstore_parquet::ColdMetadataColumn::Seq.name())
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| "flush row is missing integer field `seq`".to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let min_seq = *seqs.iter().min().expect("flush chunk is non-empty");
    let max_seq = *seqs.iter().max().expect("flush chunk is non-empty");
    Ok(FlushStats {
        row_count: i64::try_from(rows.len()).map_err(|error| error.to_string())?,
        min_seq,
        max_seq,
        min_commit_seq: min_seq,
        max_commit_seq: max_seq,
    })
}
