use crate::common;

use super::fixtures::{
    assert_join_pair, assert_koldstore_koldstore_join_samples,
    setup_koldstore_items_with_mixed_storage, setup_koldstore_order_lines_with_mixed_storage,
    JoinKind,
};
use anyhow::Result;

#[tokio::test]
async fn koldstore_table_joins_another_koldstore_table_across_all_join_kinds() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "koldstore_koldstore_join").await?;
        let items = setup_koldstore_items_with_mixed_storage(&db, "join_items_left").await?;
        let order_lines =
            setup_koldstore_order_lines_with_mixed_storage(&db, "join_order_lines").await?;

        assert_koldstore_koldstore_join_samples(&db, &order_lines.relation, &items.relation)
            .await?;

        assert_join_pair(
            &db,
            JoinKind::Inner,
            &order_lines.relation,
            &items.relation,
            "item_id",
            "id",
            22,
            1,
        )
        .await?;
        assert_join_pair(
            &db,
            JoinKind::Left,
            &order_lines.relation,
            &items.relation,
            "item_id",
            "id",
            22,
            1,
        )
        .await?;
        // Item 1002 has no order line; item 3 matches two order lines.
        assert_join_pair(
            &db,
            JoinKind::Right,
            &order_lines.relation,
            &items.relation,
            "item_id",
            "id",
            23,
            1,
        )
        .await?;
        assert_join_pair(
            &db,
            JoinKind::Full,
            &order_lines.relation,
            &items.relation,
            "item_id",
            "id",
            23,
            1,
        )
        .await?;
    }

    Ok(())
}
