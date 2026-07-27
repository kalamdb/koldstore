//! Storekey cold-segment index encoding contracts.

use std::collections::BTreeMap;

use koldstore_common::{ColumnId, ColumnRef};
use koldstore_flush::encode_indexed_column_bounds;
use koldstore_sortkey::{decode_sort_key, SortKeyType, SortKeyValue, CODEC_VERSION};
use serde_json::json;

#[test]
fn supported_bounds_encode_by_stable_id_and_unsupported_bounds_are_skipped() {
    let id = ColumnRef::new(ColumnId::from_attnum(1), "id");
    let body = ColumnRef::new(ColumnId::from_attnum(2), "body");
    let bounds = BTreeMap::from([
        ("id".to_string(), (json!(-10), json!(100))),
        ("body".to_string(), (json!("a"), json!("z"))),
    ]);
    let type_oids = BTreeMap::from([(id.column_id, 20), (body.column_id, 25)]);

    let encoded = encode_indexed_column_bounds(&bounds, &[id, body], &type_oids).unwrap();

    assert_eq!(encoded.len(), 1);
    assert_eq!(encoded[0].column_id, ColumnId::from_attnum(1));
    assert_eq!(encoded[0].type_oid, 20);
    assert_eq!(encoded[0].codec_version, CODEC_VERSION);
    assert_eq!(
        decode_sort_key(SortKeyType::Int8, &encoded[0].min_value).unwrap(),
        SortKeyValue::Int8(-10)
    );
    assert_eq!(
        decode_sort_key(SortKeyType::Int8, &encoded[0].max_value).unwrap(),
        SortKeyValue::Int8(100)
    );
}
