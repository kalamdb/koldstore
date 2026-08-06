use crate::common;

use anyhow::Result;

#[tokio::test]
async fn demigrate_catalog_deactivation_cancels_jobs_and_preserves_heap_rows_on_pgrx() -> Result<()>
{
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "demigrate_matrix").await?;
        let table = db.create_indexed_items_table("demigrate_items", 40).await?;
        db.manage_shared(&table.relation, "id").await?;
        db.flush_table(&table.relation).await?;
        db.insert_pending_flush_job(&table.relation).await?;
        assert_eq!(
            common::active_job_count(&db.client, &table.relation).await?,
            1
        );

        let deactivated = db
            .client
            .query_one(
                "SELECT koldstore.unmanage_table($1::text::regclass, true, false)",
                &[&table.relation],
            )
            .await?;
        assert_eq!(deactivated.get::<_, i64>(0), 1);

        assert_eq!(
            common::active_job_count(&db.client, &table.relation).await?,
            0
        );
        let active_schema_rows = db
            .client
            .query_one(
                "SELECT count(*) FROM koldstore.schemas WHERE table_oid = $1::text::regclass::oid AND active",
                &[&table.relation],
            )
            .await?
            .get::<_, i64>(0);
        assert_eq!(active_schema_rows, 0);
        assert_eq!(common::row_count(&db.client, &table.relation).await?, 40);

        let system_columns = db
            .client
            .query_one(
                "SELECT count(*) FROM pg_attribute WHERE attrelid = $1::text::regclass AND attname = ANY($2) AND NOT attisdropped",
                &[&table.relation, &&["_seq", "_commit_seq", "_deleted"][..]],
            )
            .await?
            .get::<_, i64>(0);
        assert_eq!(system_columns, 0);

        let mirror = format!("koldstore.{}__cl", table.table_name);
        let mirror_exists = db
            .client
            .query_one("SELECT to_regclass($1)::oid IS NOT NULL", &[&mirror])
            .await?
            .get::<_, bool>(0);
        assert!(!mirror_exists);
    }

    Ok(())
}
