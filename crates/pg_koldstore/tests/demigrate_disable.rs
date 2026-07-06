#[test]
fn demigration_sql_deactivates_managed_metadata() {
    use pg_koldstore::migrate::rehydrate::{plan_catalog_deactivation, plan_flush_deactivation};

    let catalog = plan_catalog_deactivation(42).unwrap();
    let flush = plan_flush_deactivation(42).unwrap();

    assert_eq!(
        catalog.sql,
        "UPDATE koldstore.schemas SET active = false WHERE table_oid = $1 AND active = true"
    );
    assert_eq!(
        flush.sql,
        "UPDATE koldstore.jobs SET status = 'cancelled', updated_at = now() WHERE table_oid = $1 AND status IN ('pending', 'running')"
    );
}

#[test]
fn migration_rollback_cleanup_removes_partial_catalog_rows_mirror_and_legacy_system_columns() {
    use pg_koldstore::migrate::rollback::RollbackCleanup;
    use pg_koldstore::migrate::QualifiedTableName;
    use pg_koldstore::spi::SpiAccess;

    let table = QualifiedTableName::parse("app.items").unwrap();
    let cleanup = RollbackCleanup::for_table(
        table.clone(),
        42,
        vec![
            "_seq".to_string(),
            "_commit_seq".to_string(),
            "_deleted".to_string(),
        ],
    )
    .with_mirror_table(QualifiedTableName::parse("koldstore.items__cl").unwrap());

    let plan = cleanup.plan().unwrap();

    assert_eq!(plan.table_oid, 42);
    assert!(plan
        .statements
        .iter()
        .all(|statement| statement.access == SpiAccess::ReadWrite));
    assert_eq!(
        plan.statements
            .iter()
            .map(|statement| statement.sql.as_str())
            .collect::<Vec<_>>(),
        vec![
            "DROP TABLE IF EXISTS \"koldstore\".\"items__cl\"",
            "DELETE FROM koldstore.cold_pk_hints WHERE table_oid = $1",
            "DELETE FROM koldstore.cold_segments WHERE table_oid = $1",
            "DELETE FROM koldstore.manifest WHERE table_oid = $1",
            "DELETE FROM koldstore.schemas WHERE table_oid = $1",
            "ALTER TABLE ONLY \"app\".\"items\" DROP COLUMN IF EXISTS \"_seq\", DROP COLUMN IF EXISTS \"_commit_seq\", DROP COLUMN IF EXISTS \"_deleted\""
        ]
    );
}
