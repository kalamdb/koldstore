use crate::common;

use anyhow::Result;
use serde_json::Value;
use std::collections::BTreeSet;

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
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "flush_recovery").await?;
        let table = db.create_indexed_items_table("recovery_items", 24).await?;
        db.manage_shared(&table.relation, "id").await?;
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
        let manifest_path = manifest_row.get::<_, String>(0);
        let table_prefix = std::path::Path::new(&manifest_path)
            .parent()
            .expect("manifest path has table prefix")
            .to_string_lossy();
        let orphan_temp = format!("{table_prefix}/.tmp/writer/orphan.parquet.tmp");
        let orphan_final = format!("{table_prefix}/orphan-final.parquet");
        if let Some(parent) = db.storage_root.join(&orphan_temp).parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(db.storage_root.join(&orphan_temp), b"temp")?;
        std::fs::write(db.storage_root.join(&orphan_final), b"final")?;

        let dry_run: i64 = db
            .client
            .query_one(
                "SELECT koldstore.recover_segments($1::text::regclass, true)",
                &[&table.relation],
            )
            .await?
            .get(0);
        assert_eq!(dry_run, 2);
        assert!(db.storage_root.join(&orphan_temp).exists());
        assert!(db.storage_root.join(&orphan_final).exists());

        let recovered: i64 = db
            .client
            .query_one(
                "SELECT koldstore.recover_segments($1::text::regclass, false)",
                &[&table.relation],
            )
            .await?
            .get(0);
        assert_eq!(recovered, 2);
        assert!(!db.storage_root.join(&orphan_temp).exists());
        assert!(!db.storage_root.join(&orphan_final).exists());
        let quarantine_prefix = "orphan-final.parquet.quarantine.";
        assert!(
            std::fs::read_dir(db.storage_root.join(table_prefix.as_ref()))?
                .filter_map(Result::ok)
                .any(|entry| entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(quarantine_prefix))
        );
        common::assert_cold_metadata_present(&db.client, &table.relation).await?;
    }

    Ok(())
}

#[tokio::test]
async fn flush_retry_rebuilds_manifest_from_catalog_instead_of_appending_stale_file() -> Result<()>
{
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "flush_manifest_retry").await?;
        let table = db.create_indexed_items_table("retry_items", 16).await?;
        db.manage_shared(&table.relation, "id").await?;
        db.flush_table(&table.relation).await?;

        let manifest_path: String = db
            .client
            .query_one(
                r#"
                SELECT manifest_path
                FROM koldstore.manifest
                WHERE table_oid = $1::text::regclass::oid
                "#,
                &[&table.relation],
            )
            .await?
            .get(0);
        let absolute_manifest_path = db.storage_root.join(&manifest_path);
        let mut manifest: Value =
            serde_json::from_str(&std::fs::read_to_string(&absolute_manifest_path)?)?;
        let first_segment = manifest["segments"][0].clone();
        manifest["segments"]
            .as_array_mut()
            .expect("manifest segments should be an array")
            .push(first_segment);
        std::fs::write(
            &absolute_manifest_path,
            serde_json::to_vec_pretty(&manifest)?,
        )?;

        // In async mode, stop the background applier so the next INSERT stays in
        // WAL until flush's own fence applies it in the same transaction. That is
        // the path where pending counter deltas must be visible to flush selection.
        if common::selected_mirror_capture_mode()?.is_async() {
            let dbname: String = db
                .client
                .query_one("SELECT current_database()", &[])
                .await?
                .get(0);
            db.client
                .batch_execute(&format!(
                    "ALTER DATABASE \"{dbname}\" SET koldstore.internal_async_mirror_worker = off"
                ))
                .await?;
            let _ = common::terminate_async_worker(&db.client).await?;
            // Give the launcher a moment; with the GUC off it should not restart.
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        db.client
            .batch_execute(&format!(
                r#"
                INSERT INTO {} (id, account_id, title, qty, category)
                SELECT g, g % 17, 'retry-' || g, g % 100, 'retry'
                FROM generate_series(100, 103) AS g
                "#,
                table.relation
            ))
            .await?;
        let flush_result = db.flush_table(&table.relation).await;

        if common::selected_mirror_capture_mode()?.is_async() {
            let dbname: String = db
                .client
                .query_one("SELECT current_database()", &[])
                .await?
                .get(0);
            db.client
                .batch_execute(&format!(
                    "ALTER DATABASE \"{dbname}\" RESET koldstore.internal_async_mirror_worker"
                ))
                .await?;
        }
        flush_result?;

        let rebuilt: Value =
            serde_json::from_str(&std::fs::read_to_string(&absolute_manifest_path)?)?;
        let segments = rebuilt["segments"]
            .as_array()
            .expect("manifest segments should be an array");
        let unique_batches = segments
            .iter()
            .map(|segment| {
                segment["batch"]
                    .as_i64()
                    .expect("batch should be an integer")
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            segments.len(),
            unique_batches.len(),
            "manifest should not retain duplicate stale segment entries: {rebuilt}"
        );
    }

    Ok(())
}
