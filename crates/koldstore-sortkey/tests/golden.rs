//! Golden vectors for KoldStore Sort Key V1.
//!
//! These exact byte sequences are part of the persisted catalog contract.
//! Updating the `storekey` dependency must not silently change them.

use koldstore_sortkey::{
    decode_sort_key, encode_sort_key, encode_sort_key_json, encode_sort_key_pg_text, SortKeyType,
    SortKeyValue, CODEC_VERSION,
};
use serde_json::json;
use uuid::Uuid;

#[test]
fn codec_version_is_one() {
    assert_eq!(CODEC_VERSION, 1);
}

#[test]
fn int8_ordering_and_golden_bytes() {
    let neg = encode_sort_key(&SortKeyValue::Int8(i64::MIN)).unwrap();
    let minus_one = encode_sort_key(&SortKeyValue::Int8(-1)).unwrap();
    let zero = encode_sort_key(&SortKeyValue::Int8(0)).unwrap();
    let one = encode_sort_key(&SortKeyValue::Int8(1)).unwrap();
    let max = encode_sort_key(&SortKeyValue::Int8(i64::MAX)).unwrap();

    assert_eq!(minus_one, hex("7fffffffffffffff"));
    assert_eq!(zero, hex("8000000000000000"));
    assert_eq!(one, hex("8000000000000001"));
    assert!(neg < minus_one && minus_one < zero && zero < one && one < max);

    assert_eq!(
        decode_sort_key(SortKeyType::Int8, &zero).unwrap(),
        SortKeyValue::Int8(0)
    );
}

#[test]
fn bool_int_and_uuid_round_trips() {
    assert_eq!(
        encode_sort_key(&SortKeyValue::Bool(false)).unwrap()
            < encode_sort_key(&SortKeyValue::Bool(true)).unwrap(),
        true
    );
    assert_eq!(
        encode_sort_key(&SortKeyValue::Int2(-1)).unwrap()
            < encode_sort_key(&SortKeyValue::Int2(0)).unwrap(),
        true
    );
    assert_eq!(
        encode_sort_key(&SortKeyValue::Int4(-1)).unwrap()
            < encode_sort_key(&SortKeyValue::Int4(1)).unwrap(),
        true
    );

    let uuid = Uuid::nil();
    let encoded = encode_sort_key(&SortKeyValue::Uuid(uuid)).unwrap();
    assert_eq!(encoded, vec![0; 16]);
    assert_eq!(
        decode_sort_key(SortKeyType::Uuid, &encoded).unwrap(),
        SortKeyValue::Uuid(uuid)
    );
}

#[test]
fn date_and_timestamptz_epoch_boundaries() {
    // PostgreSQL epoch day 0 == 2000-01-01.
    let epoch = encode_sort_key(&SortKeyValue::Date(0)).unwrap();
    let before = encode_sort_key(&SortKeyValue::Date(-1)).unwrap();
    let after = encode_sort_key(&SortKeyValue::Date(1)).unwrap();
    assert!(before < epoch && epoch < after);

    let ts_epoch = encode_sort_key(&SortKeyValue::Timestamptz(0)).unwrap();
    let ts_before = encode_sort_key(&SortKeyValue::Timestamptz(-1)).unwrap();
    let ts_after = encode_sort_key(&SortKeyValue::Timestamptz(1)).unwrap();
    assert!(ts_before < ts_epoch && ts_epoch < ts_after);
    assert_eq!(
        ts_epoch,
        encode_sort_key(&SortKeyValue::Timestamp(0)).unwrap()
    );
}

#[test]
fn json_helpers_match_typed_encoding() {
    let encoded = encode_sort_key_json(SortKeyType::Int8, &json!(42)).unwrap();
    assert_eq!(encoded, encode_sort_key(&SortKeyValue::Int8(42)).unwrap());

    let uuid = "00000000-0000-0000-0000-000000000001";
    let encoded = encode_sort_key_json(SortKeyType::Uuid, &json!(uuid)).unwrap();
    assert_eq!(
        encoded,
        encode_sort_key(&SortKeyValue::Uuid(Uuid::parse_str(uuid).unwrap())).unwrap()
    );

    let rfc3339 = "2024-01-01T00:01:00+00:00";
    let encoded = encode_sort_key_json(SortKeyType::Timestamptz, &json!(rfc3339)).unwrap();
    let unix = chrono::DateTime::parse_from_rfc3339(rfc3339)
        .unwrap()
        .timestamp_micros();
    let pg_micros = unix - koldstore_sortkey::PG_EPOCH_MICROS_FROM_UNIX;
    assert_eq!(
        encoded,
        encode_sort_key(&SortKeyValue::Timestamptz(pg_micros)).unwrap()
    );
    assert_eq!(
        encode_sort_key_json(SortKeyType::Timestamptz, &json!(pg_micros)).unwrap(),
        encoded
    );
}

#[test]
fn pg_text_helpers_match_typed_encoding() {
    assert_eq!(
        encode_sort_key_pg_text(SortKeyType::Bool, "t").unwrap(),
        encode_sort_key(&SortKeyValue::Bool(true)).unwrap()
    );
    assert_eq!(
        encode_sort_key_pg_text(SortKeyType::Int8, "42").unwrap(),
        encode_sort_key(&SortKeyValue::Int8(42)).unwrap()
    );
    assert_eq!(
        encode_sort_key_pg_text(SortKeyType::Uuid, "550e8400-e29b-41d4-a716-446655440000").unwrap(),
        encode_sort_key(&SortKeyValue::Uuid(
            Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()
        ))
        .unwrap()
    );
}

#[test]
fn type_oid_round_trips() {
    for ty in [
        SortKeyType::Bool,
        SortKeyType::Int2,
        SortKeyType::Int4,
        SortKeyType::Int8,
        SortKeyType::Date,
        SortKeyType::Timestamp,
        SortKeyType::Timestamptz,
        SortKeyType::Uuid,
    ] {
        assert_eq!(SortKeyType::from_type_oid(ty.type_oid()), Some(ty));
    }
    assert_eq!(SortKeyType::from_type_oid(25), None); // text
}

fn hex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}
