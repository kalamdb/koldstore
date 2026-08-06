//! Join plans must expose KoldMergeScan JSON tracing under ANALYZE FORMAT JSON.

use anyhow::Result;

use crate::common;

use super::fixtures::{
    create_plain_accounts_table, join_sql, setup_koldstore_items_with_mixed_storage, JoinKind,
};

#[tokio::test]
async fn koldstore_plain_join_explain_json_includes_merge_scan_tracing() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "join_explain_json").await?;
        let items = setup_koldstore_items_with_mixed_storage(&db, "join_json_items").await?;
        let accounts = create_plain_accounts_table(&db, "join_json_accounts").await?;

        let sql = join_sql(
            JoinKind::Inner,
            &items.relation,
            &accounts,
            "account_id",
            "account_id",
        );
        let plan_json = common::explain_analyze_json(&db.client, &sql).await?;
        common::log(format!("join explain JSON:\n{plan_json}"));
        common::assert_kold_merge_scan_explain_json_tracing(&plan_json)?;
        anyhow::ensure!(
            plan_json.contains("\"Node Type\": \"Nested Loop\"")
                || plan_json.contains("\"Node Type\": \"Hash Join\"")
                || plan_json.contains("\"Node Type\": \"Merge Join\""),
            "expected a join node in FORMAT JSON plan, got:\n{plan_json}"
        );
    }

    Ok(())
}
