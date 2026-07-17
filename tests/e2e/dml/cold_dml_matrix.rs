use crate::common;

use anyhow::Result;
use koldstore_common::TableName;
use koldstore_merge::dml::{
    plan_delete_row, plan_hydrate_pk, plan_update_row, ColdUpdateOutcome, DeleteInputState,
    DeleteRowRequest, HydratePkRequest, UpdateRowRequest,
};
use serde_json::json;

#[test]
fn cold_dml_matrix_targets_active_pgrx_versions() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    assert_eq!(
        common::local_pg_matrix()
            .into_iter()
            .map(|target| target.version)
            .collect::<Vec<_>>(),
        common::expected_pg_versions()
    );
}

#[test]
fn cold_dml_plans_cover_hydrate_update_delete_and_no_default_object_reads() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    let table_name = TableName::parse("app.items").unwrap();
    let hydrate = plan_hydrate_pk(
        &HydratePkRequest {
            table_name: table_name.clone(),
            pk_json: json!({"id": 1}),
        },
        true,
    );
    assert_eq!(hydrate.affected_rows, 1);
    assert!(hydrate.cold_lookup_performed);

    let update = UpdateRowRequest {
        table_name: table_name.clone(),
        pk_json: json!({"id": 1}),
        patch_json: json!({"title": "updated"}),
        lookup_cold: true,
    };
    assert_eq!(
        plan_update_row(&update, false, true),
        ColdUpdateOutcome::ColdLookupAndUpdate
    );

    let delete = plan_delete_row(
        &DeleteRowRequest {
            table_name,
            pk_json: json!({"id": 1}),
            allow_may_contain: true,
        },
        DeleteInputState::ColdExactLocalHint,
    );
    assert_eq!(delete.affected_rows, 1);
    assert!(delete.tombstone_written);
    common::assertions::assert_no_object_store_reads(delete.cold_lookup_performed as i64).unwrap();
}

#[tokio::test]
async fn standard_hot_dml_on_managed_table_updates_change_log_mirror_on_pgrx() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "cold_dml_matrix").await?;
        let table = db.create_indexed_items_table("dml_items", 20).await?;
        db.manage_shared(&table.relation, "id").await?;
        db.flush_table(&table.relation).await?;

        db.client
            .batch_execute(&format!(
                r#"
                INSERT INTO {relation} (id, account_id, title, qty, category)
                VALUES
                    (1000, 7, 'inserted-hot', 7, 'hot'),
                    (1001, 7, 'second-hot', 8, 'hot'),
                    (1002, 7, 'third-hot', 9, 'hot');

                UPDATE {relation}
                SET qty = 77
                WHERE id = 1000;
                "#,
                relation = table.relation
            ))
            .await?;
        common::fence_selected_mirror(&db.client).await?;

        let mirror = format!("koldstore.{}__cl", table.table_name);
        let row = db
            .client
            .query_one(
                &format!(
                    r#"
                    SELECT count(*)
                    FROM {mirror}
                    "#,
                    mirror = mirror
                ),
                &[],
            )
            .await?;
        assert!(row.get::<_, i64>(0) >= 3);
        common::assert_system_columns_absent(&db.client, &table.relation).await?;
        common::assertions::assert_no_duplicate_hot_pk(&db.client, &table.relation, "id").await?;

        let plan = common::explain(
            &db.client,
            &format!(
                "SELECT id FROM {} WHERE title = 'inserted-hot'",
                table.relation
            ),
        )
        .await?;
        common::assert_kold_merge_scan_explain(&plan)?;
        common::assert_kold_merge_scan_cold_reads(&plan, "manifest.json", 1)?;
        assert!(
            plan.contains("Filter:") && plan.contains("inserted-hot"),
            "expected filtered merge scan plan, got:\n{plan}"
        );
    }

    Ok(())
}
