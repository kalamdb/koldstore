//! Flush statistics SPI adapters.

pub(super) use koldstore_flush::{flush_stats_for_rows, FlushStats};

use koldstore_flush::policy::FlushPolicy;
use koldstore_flush::{
    decode_mirror_policy_rows, resolve_flush_stats as resolve_flush_stats_from_policy,
};
use koldstore_mirror::{
    mirror_to_sql, plan_mirror_policy_rows, plan_mirror_stats, MirrorRelation, MirrorSeqStats,
};

pub(super) fn flush_stats(table_oid: pgrx::pg_sys::Oid) -> Result<FlushStats, String> {
    let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "managed schema has no change-log mirror".to_string())?;
    let mirror = MirrorRelation::new(snapshot.mirror_relation);
    let stats = mirror_to_sql(plan_mirror_stats(&mirror)).map_err(|error| error.to_string())?;
    let json = crate::spi::execute_prepared(&stats, &[], crate::spi::first_row::<String>)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "flush stats lookup returned no rows".to_string())?;
    let stats: MirrorSeqStats = serde_json::from_str(&json).map_err(|error| error.to_string())?;
    Ok(stats.into())
}

pub(super) fn resolve_flush_stats(
    table_oid: pgrx::pg_sys::Oid,
    force: bool,
) -> Result<FlushStats, String> {
    let all = flush_stats(table_oid)?;
    let policy = active_flush_policy(table_oid)?;
    let policy_rows = load_mirror_policy_rows(table_oid)?;
    Ok(resolve_flush_stats_from_policy(
        all,
        force,
        policy.as_ref(),
        &policy_rows,
    ))
}

pub(super) fn active_flush_policy(
    table_oid: pgrx::pg_sys::Oid,
) -> Result<Option<FlushPolicy>, String> {
    use pgrx::datum::DatumWithOid;

    let statement = koldstore_catalog::queries::plan_active_flush_policy_options()
        .map_err(|error| error.to_string())?;
    let options =
        crate::spi::select_one::<pgrx::JsonB>(&statement, &[DatumWithOid::from(table_oid)])
            .map_err(|error| error.to_string())?;
    let Some(options) = options else {
        return Ok(None);
    };
    Ok(FlushPolicy::from_value(&options.0))
}

fn load_mirror_policy_rows(
    table_oid: pgrx::pg_sys::Oid,
) -> Result<Vec<koldstore_flush::policy::MirrorPolicyRow>, String> {
    let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "managed schema has no change-log mirror".to_string())?;
    let mirror = MirrorRelation::new(snapshot.mirror_relation);
    let primary_key: Vec<&str> = snapshot
        .primary_key_columns
        .iter()
        .map(String::as_str)
        .collect();
    let statement = mirror_to_sql(
        plan_mirror_policy_rows(&mirror, &primary_key).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    let json = crate::spi::execute_prepared(&statement, &[], crate::spi::first_row::<String>)
        .map_err(|error| error.to_string())?
        .unwrap_or_else(|| "[]".to_string());
    decode_mirror_policy_rows(&json)
}
