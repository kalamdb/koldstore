#[path = "../common/mod.rs"]
mod common;

#[path = "fixtures.rs"]
mod fixtures;

use anyhow::Result;
use fixtures::{
    assert_join_pair, assert_join_plan_reads_cold_storage, create_plain_order_lines_table,
    setup_koldstore_items_with_mixed_storage, JoinKind,
};

#[tokio::test]
async fn plain_pg_table_can_drive_join_against_koldstore_items() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "pg_driver_join").await?;
        let items = setup_koldstore_items_with_mixed_storage(&db, "join_items_driver").await?;
        let order_lines = create_plain_order_lines_table(&db, "join_order_lines_plain").await?;

        assert_join_pair(
            &db,
            JoinKind::Inner,
            &order_lines,
            &items.relation,
            "item_id",
            "id",
            20,
            1,
        )
        .await?;

        let cold = db
            .client
            .query_one(
                &format!(
                    r#"
                    SELECT ol.id, i.title
                    FROM {order_lines} AS ol
                    INNER JOIN {items} AS i
                      ON ol.item_id = i.id
                    WHERE ol.id = 3
                    "#,
                    order_lines = order_lines,
                    items = items.relation
                ),
                &[],
            )
            .await?;
        assert_eq!(cold.get::<_, i64>(0), 3);
        assert_eq!(cold.get::<_, String>(1), "item-000003");

        let sql = format!(
            r#"
            SELECT ol.id, i.title
            FROM {order_lines} AS ol
            INNER JOIN {items} AS i
              ON ol.item_id = i.id
            WHERE ol.id IN (1, 3)
            "#,
            order_lines = order_lines,
            items = items.relation
        );
        assert_join_plan_reads_cold_storage(&db.client, &sql, "pg-driver cold rows", 1, 1).await?;
    }

    Ok(())
}
