#[path = "../common/mod.rs"]
mod common;

use anyhow::Result;

#[test]
fn flush_recovery_plan_deletes_orphan_temp_and_quarantines_unmanifested_final() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    use koldstore_flush::recovery::{
        plan_recovery_actions, ObjectPath, OrphanObject, RecoveryAction,
    };

    let plan = plan_recovery_actions([
        OrphanObject::new(
            ObjectPath::parse("app/items/.tmp/writer/batch-0.parquet.tmp").unwrap(),
            false,
        ),
        OrphanObject::new(
            ObjectPath::parse("app/items/batch-0.parquet").unwrap(),
            false,
        ),
        OrphanObject::new(
            ObjectPath::parse("app/items/batch-1.parquet").unwrap(),
            true,
        ),
    ]);

    assert_eq!(plan.actions.len(), 2);
    assert_eq!(plan.actions[0].action, RecoveryAction::DeleteTemp);
    assert_eq!(plan.actions[1].action, RecoveryAction::QuarantineFinal);
    assert!(plan
        .actions
        .iter()
        .all(|action| !action.manifest_referenced));
    assert!(ObjectPath::parse("").is_err());
    assert!(ObjectPath::parse("../escape.parquet").is_err());
}

#[tokio::test]
async fn flush_recovery_can_distinguish_manifested_and_orphaned_files_on_pgrx() -> Result<()> {
    use koldstore_flush::recovery::{
        plan_recovery_actions, ObjectPath, OrphanObject, RecoveryAction,
    };

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "flush_recovery").await?;
        let table = db.create_indexed_items_table("recovery_items", 24).await?;
        db.migrate_shared(&table.relation, "id").await?;
        db.flush_table(&table.relation).await?;

        let manifest_row = db
            .client
            .query_one(
                r#"
                SELECT m.manifest_path, cs.object_path
                FROM koldstore.manifest m
                JOIN koldstore.cold_segments cs
                  ON cs.table_oid = m.table_oid
                 AND cs.scope_key = m.scope_key
                WHERE m.table_oid = $1::text::regclass::oid
                LIMIT 1
                "#,
                &[&table.relation],
            )
            .await?;
        let manifested_segment = manifest_row.get::<_, String>(1);
        let orphan_temp = format!("{}/.tmp/writer/orphan.parquet.tmp", db.schema);
        let orphan_final = format!("{}/orphan-final.parquet", db.schema);
        if let Some(parent) = db.storage_root.join(&orphan_temp).parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(db.storage_root.join(&orphan_temp), b"temp")?;
        std::fs::write(db.storage_root.join(&orphan_final), b"final")?;

        let plan = plan_recovery_actions([
            OrphanObject::new(ObjectPath::parse(&orphan_temp)?, false),
            OrphanObject::new(ObjectPath::parse(&orphan_final)?, false),
            OrphanObject::new(ObjectPath::parse(&manifested_segment)?, true),
        ]);

        assert_eq!(plan.actions.len(), 2);
        assert_eq!(plan.actions[0].action, RecoveryAction::DeleteTemp);
        assert_eq!(plan.actions[1].action, RecoveryAction::QuarantineFinal);
        common::assert_cold_metadata_present(&db.client, &table.relation).await?;
    }

    Ok(())
}
