//! Catalog-backed cold order frontier (SPI only; no Parquet).
//!
//! Loads competitive composite bounds from `koldstore.cold_segment_order_index`
//! for progressive ordered merge. Preparing the frontier does not open objects.

use koldstore_merge::scan::OrderDirection;
use pgrx::datum::DatumWithOid;
use pgrx::pg_sys;

/// Best unopened cold composite bound for `direction`, if any row exists.
pub(super) fn load_cold_best_bound(
    table_oid: pg_sys::Oid,
    scope_key: &str,
    sort_order_id: i32,
    direction: OrderDirection,
) -> Result<Option<Vec<u8>>, String> {
    let sql = match direction {
        OrderDirection::Asc => {
            "SELECT min_composite_key FROM koldstore.cold_segment_order_index \
             WHERE table_oid = $1::oid AND scope_key = $2::text AND sort_order_id = $3::integer \
               AND min_composite_key IS NOT NULL \
             ORDER BY min_composite_key ASC NULLS LAST LIMIT 1"
        }
        OrderDirection::Desc => {
            "SELECT max_composite_key FROM koldstore.cold_segment_order_index \
             WHERE table_oid = $1::oid AND scope_key = $2::text AND sort_order_id = $3::integer \
               AND max_composite_key IS NOT NULL \
             ORDER BY max_composite_key DESC NULLS LAST LIMIT 1"
        }
    };
    let statement = koldstore_common::SqlStatement::read("cold order frontier bound", sql)
        .map_err(|e| e.to_string())?;
    crate::spi::select_one::<Vec<u8>>(
        &statement,
        &[
            DatumWithOid::from(table_oid),
            DatumWithOid::from(scope_key.to_string()),
            DatumWithOid::from(sort_order_id),
        ],
    )
    .map_err(|e| e.to_string())
}
