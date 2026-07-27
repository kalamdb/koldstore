//! PostgreSQL datum boundary for Sort Key V1 encoding.
//!
//! Strict mirror triggers call this polymorphic helper after manage-time type
//! validation. The codec itself remains owned by `koldstore-sortkey`.

#[cfg(feature = "pg")]
use koldstore_sortkey::{encode_sort_key, SortKeyValue};
#[cfg(feature = "pg")]
use pgrx::datum::AnyElement;

/// Encodes one supported PostgreSQL scalar as Sort Key V1 `bytea`.
///
/// SQL contract: the input is non-null and has one of the allowlisted type
/// OIDs validated for `segment_order_column_id`.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(
    name = "internal_encode_sort_key",
    schema = "koldstore",
    immutable,
    parallel_safe
)]
pub fn internal_encode_sort_key(value: AnyElement) -> Vec<u8> {
    let type_oid = u32::from(value.oid());
    let encoded = unsafe {
        match type_oid {
            16 => AnyElement::into::<bool>(&value).map(SortKeyValue::Bool),
            21 => AnyElement::into::<i16>(&value).map(SortKeyValue::Int2),
            23 => AnyElement::into::<i32>(&value).map(SortKeyValue::Int4),
            20 => AnyElement::into::<i64>(&value).map(SortKeyValue::Int8),
            1082 => AnyElement::into::<pgrx::datum::Date>(&value)
                .map(|date| SortKeyValue::Date(date.into_inner())),
            1114 => AnyElement::into::<pgrx::datum::Timestamp>(&value)
                .map(|timestamp| SortKeyValue::Timestamp(timestamp.into_inner())),
            1184 => AnyElement::into::<pgrx::datum::TimestampWithTimeZone>(&value)
                .map(|timestamp| SortKeyValue::Timestamptz(timestamp.into_inner())),
            2950 => AnyElement::into::<pgrx::datum::Uuid>(&value).map(|uuid| {
                SortKeyValue::Uuid(uuid::Uuid::from_bytes(*uuid.as_bytes()))
            }),
            _ => pgrx::error!("unsupported Sort Key V1 PostgreSQL type OID {type_oid}"),
        }
    }
    .unwrap_or_else(|| pgrx::error!("cannot encode NULL as a segment order key"));

    encode_sort_key(&encoded)
        .unwrap_or_else(|error| pgrx::error!("failed to encode Sort Key V1 value: {error}"))
}
