//! Catalog-backed cold order frontier (SPI only; no Parquet).
//!
//! Loads competitive composite bounds from `koldstore.cold_segment_order_index`
//! for progressive ordered merge. Preparing the frontier does not open objects.
//! Row-group competitiveness uses [`koldstore_merge::scan::select_competitive_row_groups`].

use koldstore_merge::scan::{select_competitive_row_groups, OrderDirection};
use pgrx::datum::DatumWithOid;
use pgrx::pg_sys;

/// One Sort Key V1 composite bound per Parquet row group (`NULL` = unknown).
type RowGroupCompositeBounds = Vec<Option<Vec<u8>>>;
/// Min/max composite bound arrays from `cold_segment_order_index`.
type OrderIndexRowGroupBounds = (RowGroupCompositeBounds, RowGroupCompositeBounds);

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
    let statement = koldstore_common::SqlStatement::read_with_params(
        "cold order frontier bound",
        sql,
        [
            koldstore_common::SqlParamType::Oid,
            koldstore_common::SqlParamType::Text,
            koldstore_common::SqlParamType::Integer,
        ],
    )
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

/// Competitive row-group indexes for a segment object path vs a hot frontier key.
///
/// Returns `None` when no order-index row exists for the path. An empty `Vec`
/// means the index exists but no row group competes.
pub(super) fn competitive_row_groups_for_path(
    table_oid: pg_sys::Oid,
    scope_key: &str,
    sort_order_id: i32,
    object_path: &str,
    direction: OrderDirection,
    hot_key: Option<&[u8]>,
) -> Result<Option<Vec<usize>>, String> {
    let sql = "SELECT csoi.row_group_min_composite_keys, csoi.row_group_max_composite_keys \
               FROM koldstore.cold_segment_order_index csoi \
               JOIN koldstore.cold_segments cs ON cs.segment_id = csoi.segment_id \
               WHERE cs.table_oid = $1::oid AND cs.scope_key = $2::text \
                 AND csoi.sort_order_id = $3::integer AND cs.path = $4::text \
               LIMIT 1";
    let statement = koldstore_common::SqlStatement::read_with_params(
        "cold order frontier row groups by path",
        sql,
        [
            koldstore_common::SqlParamType::Oid,
            koldstore_common::SqlParamType::Text,
            koldstore_common::SqlParamType::Integer,
            koldstore_common::SqlParamType::Text,
        ],
    )
    .map_err(|e| e.to_string())?;
    let args = [
        DatumWithOid::from(table_oid),
        DatumWithOid::from(scope_key.to_string()),
        DatumWithOid::from(sort_order_id),
        DatumWithOid::from(object_path.to_string()),
    ];
    let decoded: Option<OrderIndexRowGroupBounds> =
        crate::spi::execute_prepared(&statement, &args, |tuples| {
            let Some(tuple) = tuples.into_iter().next() else {
                return Ok(None);
            };
            let mins = tuple
                .get::<pgrx::Array<&[u8]>>(1)?
                .map(|arr| arr.iter().map(|value| value.map(<[u8]>::to_vec)).collect())
                .unwrap_or_default();
            let maxs = tuple
                .get::<pgrx::Array<&[u8]>>(2)?
                .map(|arr| arr.iter().map(|value| value.map(<[u8]>::to_vec)).collect())
                .unwrap_or_default();
            Ok(Some((mins, maxs)))
        })
        .map_err(|e| e.to_string())?;
    let Some((mins, maxs)) = decoded else {
        return Ok(None);
    };
    Ok(Some(select_competitive_row_groups(
        direction, hot_key, &mins, &maxs,
    )))
}
