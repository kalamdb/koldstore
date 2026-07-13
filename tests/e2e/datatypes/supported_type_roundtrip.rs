//! E2E: all manage-supported column types survive flush + merge-scan round-trip.

#[path = "../common/mod.rs"]
mod common;

use anyhow::{Context, Result};

#[tokio::test]
async fn supported_types_round_trip_through_flush_and_merge_scan() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "supported_type_roundtrip").await?;
        let relation = format!("{}.type_roundtrip", db.schema);

        db.client
            .batch_execute(&format!(
                r#"
                CREATE TABLE {relation} (
                  id bigint PRIMARY KEY,
                  c_bool boolean NOT NULL,
                  c_int2 smallint NOT NULL,
                  c_int4 integer NOT NULL,
                  c_int8 bigint NOT NULL,
                  c_float4 real NOT NULL,
                  c_float8 double precision NOT NULL,
                  c_text text NOT NULL,
                  c_varchar varchar(32) NOT NULL,
                  c_uuid uuid NOT NULL,
                  c_jsonb jsonb NOT NULL,
                  c_timestamptz timestamptz NOT NULL,
                  c_text_null text,
                  c_uuid_null uuid,
                  c_jsonb_null jsonb,
                  c_timestamptz_null timestamptz
                );
                "#
            ))
            .await?;

        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name         => $1::text::regclass,
                  storage            => $2,
                  hot_row_limit      => 10,
                  min_flush_rows     => 1,
                  max_rows_per_file  => 1000,
                  compression        => 'zstd',
                  migration_order_by => 'id'
                )
                "#,
                &[&relation, &db.storage_name],
            )
            .await?;

        db.client
            .batch_execute(&format!(
                r#"
                INSERT INTO {relation} (
                  id, c_bool, c_int2, c_int4, c_int8, c_float4, c_float8,
                  c_text, c_varchar, c_uuid, c_jsonb, c_timestamptz,
                  c_text_null, c_uuid_null, c_jsonb_null, c_timestamptz_null
                ) VALUES (
                  1, true, 2, 3, 4, 1.5::real, 2.5::double precision,
                  'hello', 'varchar-1',
                  'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'::uuid,
                  '{{"k":1,"nested":{{"ok":true}},"arr":[1,2,3]}}'::jsonb,
                  timestamptz '2024-06-15 12:30:45+00',
                  NULL, NULL, NULL, NULL
                ), (
                  2, false, -2, -3, -4, -1.5::real, -2.5::double precision,
                  'world', 'varchar-2',
                  '11111111-2222-3333-4444-555555555555'::uuid,
                  '{{"row":2}}'::jsonb,
                  timestamptz '2024-06-15 12:30:45+00',
                  'present',
                  '99999999-8888-7777-6666-555555555555'::uuid,
                  '{{"nullable":true}}'::jsonb,
                  timestamptz '2024-06-15 12:30:45+00'
                );

                INSERT INTO {relation} (
                  id, c_bool, c_int2, c_int4, c_int8, c_float4, c_float8,
                  c_text, c_varchar, c_uuid, c_jsonb, c_timestamptz
                )
                SELECT
                  gs,
                  (gs % 2) = 0,
                  (gs % 100)::smallint,
                  gs::integer,
                  gs,
                  gs::real,
                  gs::double precision,
                  'pad-' || gs::text,
                  left(gs::text, 32),
                  md5(gs::text)::uuid,
                  jsonb_build_object('pad', gs),
                  timestamptz '2020-01-01' + (gs || ' seconds')::interval
                FROM generate_series(100, 130) AS gs;
                "#
            ))
            .await?;

        let flushed: i64 = {
            let job_id: String = db
                .client
                .query_one(
                    "SELECT koldstore.flush_table($1::text::regclass, force => true)::text",
                    &[&relation],
                )
                .await?
                .get(0);
            db.client
                .query_one(
                    "SELECT rows_flushed FROM koldstore.jobs WHERE id = $1::text::uuid",
                    &[&job_id],
                )
                .await?
                .get(0)
        };
        assert!(
            flushed >= 2,
            "expected seeded rows to flush cold, got rows_flushed={flushed}"
        );
        common::assert_cold_metadata_present(&db.client, &relation).await?;

        let ok: bool = db
            .client
            .query_one(
                &format!(
                    r#"
                    SELECT
                      c_bool IS TRUE
                      AND c_int2 = 2
                      AND c_int4 = 3
                      AND c_int8 = 4
                      AND abs(c_float4 - 1.5::real) < 0.0001
                      AND abs(c_float8 - 2.5::double precision) < 0.0000001
                      AND c_text = 'hello'
                      AND c_varchar = 'varchar-1'
                      AND c_uuid = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'::uuid
                      AND c_jsonb @> '{{"k":1,"nested":{{"ok":true}},"arr":[1,2,3]}}'::jsonb
                      AND c_jsonb <@ '{{"k":1,"nested":{{"ok":true}},"arr":[1,2,3]}}'::jsonb
                      AND c_timestamptz = timestamptz '2024-06-15 12:30:45+00'
                      AND c_text_null IS NULL
                      AND c_uuid_null IS NULL
                      AND c_jsonb_null IS NULL
                      AND c_timestamptz_null IS NULL
                    FROM {relation}
                    WHERE id = 1
                    "#
                ),
                &[],
            )
            .await
            .context("read cold/hot merged row id=1")?
            .get(0);
        if !ok {
            let detail = db
                .client
                .query_one(
                    &format!(
                        r#"
                        SELECT
                          c_bool, c_int2, c_int4, c_int8, c_float4, c_float8,
                          c_text, c_varchar, c_uuid::text, c_jsonb::text,
                          c_timestamptz::text,
                          c_text_null IS NULL, c_uuid_null IS NULL,
                          c_jsonb_null IS NULL, c_timestamptz_null IS NULL
                        FROM {relation}
                        WHERE id = 1
                        "#
                    ),
                    &[],
                )
                .await?;
            panic!("row id=1 type values must round-trip through cold; got {detail:?}");
        }

        let ok2: bool = db
            .client
            .query_one(
                &format!(
                    r#"
                    SELECT
                      c_text_null = 'present'
                      AND c_uuid_null = '99999999-8888-7777-6666-555555555555'::uuid
                      AND c_jsonb_null @> '{{"nullable":true}}'::jsonb
                      AND c_jsonb_null <@ '{{"nullable":true}}'::jsonb
                      AND c_timestamptz_null = timestamptz '2024-06-15 12:30:45+00'
                    FROM {relation}
                    WHERE id = 2
                    "#
                ),
                &[],
            )
            .await?
            .get(0);
        assert!(ok2, "row id=2 nullable values must round-trip through cold");

        let plan = common::explain(
            &db.client,
            &format!("SELECT * FROM {relation} WHERE id = 1"),
        )
        .await?;
        common::assert_kold_merge_scan_explain(&plan)?;
    }

    Ok(())
}
