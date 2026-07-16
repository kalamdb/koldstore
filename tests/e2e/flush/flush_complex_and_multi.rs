//! Rich-types flush and parallel multi-table flush coverage.

use anyhow::Result;
use tokio::task::JoinHandle;

use crate::common;
use crate::flush::harness::{
    assert_flush_load_invariants, connect_peer, create_rich_types_table, flush_table_on,
    run_mixed_worker,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_complex_rich_types_roundtrips_after_prune() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "flush_rich").await?;
        let table = create_rich_types_table(&db, "rich_items", 80).await?;
        db.manage_shared(&table.relation, "id").await?;

        let flushed = db.flush_table(&table.relation).await?;
        assert_eq!(flushed, 80);
        common::assert_cold_metadata_present(&db.client, &table.relation).await?;
        common::assert_no_active_jobs(&db.client, &table.relation).await?;
        common::assert_flush_pruned_hot_storage(&db.client, &table.relation, 80).await?;

        // Semantic checks. Cold jsonb may arrive as a JSON string scalar; unwrap when needed.
        let roundtrip = db
            .client
            .query_one(
                &format!(
                    r#"
                    SELECT
                      (CASE
                         WHEN jsonb_typeof(payload) = 'string'
                           THEN (payload #>> '{{}}')::jsonb
                         ELSE payload
                       END ->> 'row')::bigint,
                      CASE
                        WHEN jsonb_typeof(payload) = 'string'
                          THEN (payload #>> '{{}}')::jsonb ->> 'kind'
                        ELSE payload ->> 'kind'
                      END,
                      tag = md5('rich-7')::uuid,
                      abs(amount - 0.7) < 1e-9,
                      flag = false,
                      note
                    FROM {}
                    WHERE id = 7
                    "#,
                    table.relation
                ),
                &[],
            )
            .await?;
        assert_eq!(roundtrip.get::<_, i64>(0), 7);
        assert_eq!(roundtrip.get::<_, String>(1), "odd");
        assert!(roundtrip.get::<_, bool>(2), "uuid tag mismatch");
        assert!(roundtrip.get::<_, bool>(3), "amount mismatch");
        assert!(roundtrip.get::<_, bool>(4), "flag mismatch");
        assert_eq!(roundtrip.get::<_, Option<String>>(5), Some("note-7".into()));

        // id=15: note NULL (15%3=0), payload_null NULL (15%5=0), amount_null present (15%4!=0)
        let nulls = db
            .client
            .query_one(
                &format!(
                    r#"
                    SELECT
                      note IS NULL,
                      payload_null IS NULL,
                      amount_null IS NOT NULL
                    FROM {}
                    WHERE id = 15
                    "#,
                    table.relation
                ),
                &[],
            )
            .await?;
        assert!(nulls.get::<_, bool>(0), "note null for id=15");
        assert!(nulls.get::<_, bool>(1), "payload_null null for id=15");
        assert!(nulls.get::<_, bool>(2), "amount_null present for id=15");

        // id=8: payload_null object present (8%5!=0), amount_null NULL (8%4=0)
        let partial = db
            .client
            .query_one(
                &format!(
                    r#"
                    SELECT
                      CASE
                        WHEN payload_null IS NULL THEN false
                        WHEN jsonb_typeof(payload_null) = 'string'
                          THEN ((payload_null #>> '{{}}')::jsonb ->> 'nullable') = '8'
                        ELSE (payload_null ->> 'nullable') = '8'
                      END,
                      amount_null IS NULL
                    FROM {}
                    WHERE id = 8
                    "#,
                    table.relation
                ),
                &[],
            )
            .await?;
        assert!(partial.get::<_, bool>(0), "payload_null for id=8");
        assert!(partial.get::<_, bool>(1), "amount_null null for id=8");

        let plan = common::explain(
            &db.client,
            &format!(
                "SELECT id, payload, amount FROM {} WHERE id = 7",
                table.relation
            ),
        )
        .await?;
        common::assert_kold_merge_scan_explain(&plan)?;
        common::assert_kold_merge_scan_cold_reads(&plan, "manifest.json", 1)?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flush_multiple_tables_in_parallel_with_light_dml() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "flush_multi").await?;

        // Traffic table absorbs concurrent DML so parallel flushes are not racing
        // their own selected row sets (selection/write mismatch).
        let traffic = db.create_indexed_items_table("multi_traffic", 32).await?;
        db.manage_shared(&traffic.relation, "id").await?;

        let mut relations = Vec::new();
        for name in ["multi_a", "multi_b", "multi_c", "multi_d"] {
            let table = db.create_indexed_items_table(name, 48).await?;
            db.manage_shared(&table.relation, "id").await?;
            relations.push(table.relation);
        }

        let dml_client = connect_peer(&db).await?;
        let dml_relation = traffic.relation.clone();
        let dml_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
            run_mixed_worker(dml_client, dml_relation, 0, Some(15), None).await
        });

        let mut flush_handles = Vec::new();
        for relation in &relations {
            let peer = connect_peer(&db).await?;
            let relation = relation.clone();
            flush_handles.push(tokio::spawn(async move {
                flush_table_on(&peer, &relation).await
            }));
        }

        let mut flushed_total = 0i64;
        for (idx, handle) in flush_handles.into_iter().enumerate() {
            let rows = handle.await??;
            assert!(
                rows > 0,
                "parallel flush for table index {idx} returned rows_flushed={rows}"
            );
            flushed_total += rows;
        }
        assert!(
            flushed_total >= 48 * 4,
            "expected each table to flush its seed batch, total={flushed_total}"
        );

        dml_handle.await??;

        for relation in &relations {
            common::assert_no_active_jobs(&db.client, relation).await?;
            common::assert_cold_metadata_present(&db.client, relation).await?;
            common::assert_pk_unique(&db.client, relation, &["id"]).await?;
        }

        // Flush traffic last so concurrent DML artifacts are archived too.
        let traffic_flushed = db.flush_table(&traffic.relation).await?;
        assert!(traffic_flushed > 0);
        assert_flush_load_invariants(&db.client, &traffic.relation).await?;
    }

    Ok(())
}
