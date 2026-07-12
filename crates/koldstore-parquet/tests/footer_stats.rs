//! Footer-derived catalog stats aggregation tests.

use koldstore_common::ColumnId;
use koldstore_parquet::{
    catalog_stats_from_parquet_bytes, encode_parquet_segment_bytes, plan_clean_cold_record,
    record_batch_from_clean_cold_records, PgColumn, PgType,
};
use serde_json::json;

fn column_id(value: u64) -> ColumnId {
    ColumnId::new(value).unwrap()
}

#[test]
fn multi_row_group_footer_stats_aggregate_min_max_by_column_id() {
    let columns = [
        PgColumn::new(column_id(1), "id", PgType::Int8, false),
        PgColumn::new(column_id(2), "status", PgType::Text, false),
    ];
    let plans: Vec<_> = (1..=2_500i64)
        .map(|id| {
            plan_clean_cold_record(
                [
                    ("id", json!(id)),
                    ("status", json!(if id < 1000 { "early" } else { "late" })),
                ],
                ["id"],
                id,
                1,
                1,
            )
            .unwrap()
        })
        .collect();
    let batch = record_batch_from_clean_cold_records(&columns, &plans).unwrap();
    let bytes = encode_parquet_segment_bytes(
        &batch,
        &["id".to_string()],
        &["id".to_string(), "status".to_string()],
        "zstd",
    )
    .unwrap();

    let stats = catalog_stats_from_parquet_bytes(&bytes, &columns).unwrap();
    assert_eq!(stats[&column_id(1)].min, json!(1));
    assert_eq!(stats[&column_id(1)].max, json!(2_500));
    assert_eq!(stats[&column_id(2)].min, json!("early"));
    assert_eq!(stats[&column_id(2)].max, json!("late"));
}

#[test]
fn null_only_indexed_column_is_omitted_fail_open() {
    let columns = [
        PgColumn::new(column_id(1), "id", PgType::Int8, false),
        PgColumn::new(column_id(2), "note", PgType::Text, true),
    ];
    let plans =
        vec![
            plan_clean_cold_record([("id", json!(1)), ("note", json!(null))], ["id"], 1, 1, 1)
                .unwrap(),
        ];
    let batch = record_batch_from_clean_cold_records(&columns, &plans).unwrap();
    let bytes =
        encode_parquet_segment_bytes(&batch, &["id".to_string()], &["note".to_string()], "zstd")
            .unwrap();

    let stats = catalog_stats_from_parquet_bytes(&bytes, &columns).unwrap();
    assert!(stats.contains_key(&column_id(1)));
    assert!(
        !stats.contains_key(&column_id(2)),
        "null-only column must be omitted, not published as false bounds"
    );
}

#[test]
fn timestamptz_footer_stats_convert_to_rfc3339() {
    let columns = [
        PgColumn::new(column_id(1), "id", PgType::Int8, false),
        PgColumn::new(column_id(2), "created_at", PgType::Timestamptz, false),
    ];
    let plans = vec![
        plan_clean_cold_record(
            [
                ("id", json!(1)),
                ("created_at", json!("2024-01-01T00:00:00+00:00")),
            ],
            ["id"],
            1,
            1,
            1,
        )
        .unwrap(),
        plan_clean_cold_record(
            [
                ("id", json!(2)),
                ("created_at", json!("2024-06-15T12:30:00+00:00")),
            ],
            ["id"],
            2,
            1,
            1,
        )
        .unwrap(),
    ];
    let batch = record_batch_from_clean_cold_records(&columns, &plans).unwrap();
    let bytes = encode_parquet_segment_bytes(
        &batch,
        &["id".to_string()],
        &["created_at".to_string()],
        "zstd",
    )
    .unwrap();

    let stats = catalog_stats_from_parquet_bytes(&bytes, &columns).unwrap();
    let created = &stats[&column_id(2)];
    assert!(
        created.min.as_str().unwrap().starts_with("2024-01-01"),
        "min={}",
        created.min
    );
    assert!(
        created.max.as_str().unwrap().starts_with("2024-06-15"),
        "max={}",
        created.max
    );
}
