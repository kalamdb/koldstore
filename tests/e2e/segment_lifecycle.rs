#[path = "common/mod.rs"]
mod common;

use anyhow::Result;
use koldstore_catalog::SegmentVisibility;
use koldstore_flush::recovery::{
    classify_orphan_object, is_cold_segment_object_path, ObjectPath, OrphanObject, RecoveryAction,
};
use koldstore_manifest::{LifecycleHook, SegmentStatus};

#[test]
fn lifecycle_transitions_and_orphan_classification() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    assert!(SegmentVisibility::Staged
        .transition(SegmentVisibility::Published)
        .is_ok());
    assert!(SegmentVisibility::Published
        .transition(SegmentVisibility::Superseded)
        .is_ok());
    assert_eq!(
        LifecycleHook::Supersede
            .apply(SegmentStatus::Published)
            .unwrap(),
        SegmentStatus::Superseded
    );
    assert!(is_cold_segment_object_path(
        "app/items/segment-0001.parquet"
    ));
    assert!(!is_cold_segment_object_path(
        "app/items/.tmp/writer/segment-0001.parquet.tmp"
    ));

    let plan = koldstore_flush::recovery::plan_recovery_actions([
        OrphanObject::new(
            ObjectPath::parse("app/items/.tmp/w/segment-0001.parquet.tmp").unwrap(),
            false,
        ),
        OrphanObject::new(
            ObjectPath::parse("app/items/segment-0009.parquet").unwrap(),
            false,
        ),
        OrphanObject::new(
            ObjectPath::parse("app/items/segment-0001.parquet").unwrap(),
            true,
        ),
    ]);
    assert_eq!(plan.actions.len(), 2);
    assert_eq!(plan.actions[0].action, RecoveryAction::DeleteTemp);
    assert_eq!(plan.actions[1].action, RecoveryAction::QuarantineFinal);
    assert_eq!(
        classify_orphan_object("app/items/segment-0001.parquet", true),
        None
    );
}

#[tokio::test]
async fn flush_publishes_segment_names_and_visible_status() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "segment_lifecycle").await?;
        let relation = db.relation("lifecycle_items");
        db.client
            .batch_execute(&format!(
                "CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL)"
            ))
            .await?;
        db.manage_shared(&relation, "id").await?;
        db.client
            .batch_execute(&format!(
                "INSERT INTO {relation} (id, body) VALUES (1, 'one'), (2, 'two')"
            ))
            .await?;
        db.flush_table(&relation).await?;

        let row = db
            .client
            .query_one(
                r#"
                SELECT object_path, status
                FROM koldstore.segments
                WHERE table_oid = $1::text::regclass::oid
                ORDER BY batch_number, segment_id
                LIMIT 1
                "#,
                &[&relation],
            )
            .await?;
        let object_path: String = row.get(0);
        let status: String = row.get(1);
        assert!(
            object_path.contains("segment-") && object_path.ends_with(".parquet"),
            "unexpected path {object_path}"
        );
        assert!(
            object_path
                .rsplit('/')
                .next()
                .is_some_and(|name| name.starts_with("segment-")
                    && name.len() >= "segment-0000.parquet".len()),
            "expected zero-padded segment name in {object_path}"
        );
        assert_eq!(status, "published");

        let count: i64 = db
            .client
            .query_one(
                r#"
                SELECT count(*)::bigint
                FROM koldstore.segments
                WHERE table_oid = $1::text::regclass::oid
                  AND status = 'staged'
                "#,
                &[&relation],
            )
            .await?
            .get(0);
        assert_eq!(count, 0, "no staged segments should remain after flush");
    }
    Ok(())
}
