use crate::common;

use anyhow::Result;
use koldstore_common::{ChangeSource, MirrorChange, MirrorOperation, ScopeKey, SeqId};
use koldstore_merge::events;
use serde_json::json;

fn change(id: i64, seq: i64, operation: MirrorOperation, source: ChangeSource) -> MirrorChange {
    MirrorChange {
        table_oid: 42,
        scope_key: None,
        pk_json: json!({"id": id}),
        operation,
        seq: SeqId::new(seq).unwrap(),
        deleted: operation.is_delete(),
        row_image_json: (!operation.is_delete()).then(|| json!({"id": id, "seq": seq})),
        source,
    }
}

#[test]
fn change_feed_merges_hot_mirror_and_cold_metadata_as_latest_state() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    let changes = vec![
        change(1, 10, MirrorOperation::Insert, ChangeSource::ColdRecord),
        change(1, 20, MirrorOperation::Update, ChangeSource::ColdRecord),
        change(1, 30, MirrorOperation::Delete, ChangeSource::HotMirror),
        change(2, 25, MirrorOperation::Insert, ChangeSource::HotMirror),
    ];

    let result = events::changes_since(&changes, 42, None, 0, Some(10), None).unwrap();

    assert_eq!(
        result
            .iter()
            .map(|change| (change.pk_json.clone(), change.seq.get(), change.operation))
            .collect::<Vec<_>>(),
        vec![
            (json!({"id": 2}), 25, MirrorOperation::Insert),
            (json!({"id": 1}), 30, MirrorOperation::Delete),
        ]
    );
    assert!(result[1].deleted);
}

#[tokio::test]
async fn change_feed_reads_table_specific_mirror_without_row_events_on_pgrx() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "change_feed").await?;
        let table = db
            .create_indexed_items_table("change_feed_items", 2)
            .await?;
        db.manage_shared(&table.relation, "id").await?;

        let mirror = format!("koldstore.{}__cl", table.table_name);
        let rows = db
            .client
            .query(
                &format!(
                    r#"
                    SELECT op, seq, (op = 3) AS deleted
                    FROM {mirror}
                    WHERE seq > $1
                    ORDER BY seq
                    "#
                ),
                &[&0_i64],
            )
            .await?;

        let changes = rows
            .into_iter()
            .map(|row| {
                (
                    row.get::<_, i16>(0),
                    row.get::<_, i64>(1),
                    row.get::<_, bool>(2),
                )
            })
            .collect::<Vec<_>>();

        assert!(!changes.is_empty());
        let row_events_exists = db
            .client
            .query_one(
                "SELECT to_regclass('koldstore.row_events') IS NOT NULL",
                &[],
            )
            .await?
            .get::<_, bool>(0);
        assert!(!row_events_exists);
    }

    Ok(())
}

#[test]
fn user_scope_change_feed_filters_scope_before_latest_state_resolution() {
    let scoped = vec![
        MirrorChange {
            scope_key: Some(ScopeKey::new("user-a").unwrap()),
            ..change(1, 10, MirrorOperation::Insert, ChangeSource::ColdRecord)
        },
        MirrorChange {
            scope_key: Some(ScopeKey::new("user-b").unwrap()),
            ..change(1, 20, MirrorOperation::Update, ChangeSource::HotMirror)
        },
    ];

    let result = events::changes_since(
        &scoped,
        42,
        Some(&ScopeKey::new("user-a").unwrap()),
        0,
        None,
        None,
    )
    .unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].scope_key.as_ref().unwrap().as_str(), "user-a");
    assert_eq!(result[0].seq.get(), 10);
}
