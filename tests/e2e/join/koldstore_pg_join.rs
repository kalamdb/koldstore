#[path = "../common/mod.rs"]
mod common;

#[path = "fixtures.rs"]
mod fixtures;

use anyhow::Result;
use fixtures::{assert_join_pair, assert_koldstore_pg_join_samples, create_plain_accounts_table, setup_koldstore_items_with_mixed_storage, JoinKind};

#[tokio::test]
async fn koldstore_table_joins_plain_pg_table_across_all_join_kinds() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "koldstore_pg_join").await?;
        let items = setup_koldstore_items_with_mixed_storage(&db, "join_items").await?;
        let accounts = create_plain_accounts_table(&db, "join_accounts").await?;

        assert_koldstore_pg_join_samples(&db, &items.relation, &accounts).await?;

        // 20 cold rows + 2 hot rows, all with matching accounts.
        assert_join_pair(
            &db,
            JoinKind::Inner,
            &items.relation,
            &accounts,
            "account_id",
            "account_id",
            22,
            1,
        )
        .await?;
        assert_join_pair(
            &db,
            JoinKind::Left,
            &items.relation,
            &accounts,
            "account_id",
            "account_id",
            22,
            1,
        )
        .await?;
        // Accounts include one orphan row (account_id = 50) with no items.
        // Multiple items can share an account_id, so RIGHT/FULL exceed account cardinality.
        assert_join_pair(
            &db,
            JoinKind::Right,
            &items.relation,
            &accounts,
            "account_id",
            "account_id",
            23,
            1,
        )
        .await?;
        assert_join_pair(
            &db,
            JoinKind::Full,
            &items.relation,
            &accounts,
            "account_id",
            "account_id",
            23,
            1,
        )
        .await?;
    }

    Ok(())
}
