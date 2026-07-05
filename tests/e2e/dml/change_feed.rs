#[path = "../common/mod.rs"]
mod common;

use anyhow::Result;
use koldstore_core::{CommitSeq, LogicalPk, PkColumn, RowOperation, SeqId};
use pg_koldstore::sql::events;
use serde_json::json;

#[test]
fn change_feed_orders_flush_cold_delete_hydrate_and_demigration_boundary_events() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    let pk =
        LogicalPk::from_json_object(&json!({"id": 7}), &[PkColumn::new("id").unwrap()]).unwrap();
    let event = |op, seq, commit_seq, payload| {
        events::append_row_event(
            42,
            None,
            &pk,
            op,
            SeqId::new(seq).unwrap(),
            CommitSeq::new(commit_seq).unwrap(),
            payload,
        )
    };
    let out_of_order_events = vec![
        event(
            RowOperation::Revive,
            4,
            40,
            Some(json!({"boundary": "demigrate"})),
        ),
        event(RowOperation::Delete, 3, 30, None),
        event(
            RowOperation::Update,
            2,
            20,
            Some(json!({"source": "hydrate"})),
        ),
        event(
            RowOperation::Insert,
            1,
            10,
            Some(json!({"source": "flush"})),
        ),
    ];

    let changes = events::changes_since(&out_of_order_events, 42, None, 0, Some(10), None).unwrap();

    assert_eq!(
        changes
            .iter()
            .map(|event| (event.op, event.commit_seq.get()))
            .collect::<Vec<_>>(),
        vec![
            (RowOperation::Insert, 10),
            (RowOperation::Update, 20),
            (RowOperation::Delete, 30),
            (RowOperation::Revive, 40),
        ]
    );
    assert!(changes[2].deleted);
}

#[tokio::test]
async fn change_feed_events_are_persisted_and_ordered_on_pgrx() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "change_feed").await?;
        let table = db
            .create_indexed_items_table("change_feed_items", 2)
            .await?;
        db.migrate_shared(&table.relation, "id").await?;

        db.client
            .execute(
                r#"
                INSERT INTO koldstore.row_events
                  (table_oid, pk_hash, pk_json, op, seq, commit_seq, deleted, row_image_json)
                VALUES
                  ($1::text::regclass::oid, decode('03', 'hex'), '{"id":3}'::jsonb, 'revive', 4, 40, false, '{"boundary":"demigrate"}'::jsonb),
                  ($1::text::regclass::oid, decode('02', 'hex'), '{"id":2}'::jsonb, 'delete', 3, 30, true, NULL),
                  ($1::text::regclass::oid, decode('01', 'hex'), '{"id":1}'::jsonb, 'update', 2, 20, false, '{"source":"hydrate"}'::jsonb)
                "#,
                &[&table.relation],
            )
            .await?;

        let rows = db
            .client
            .query(
                r#"
                SELECT op, commit_seq, deleted
                FROM koldstore.row_events
                WHERE table_oid = $1::text::regclass::oid
                  AND commit_seq > 10
                ORDER BY commit_seq
                "#,
                &[&table.relation],
            )
            .await?;
        let changes = rows
            .into_iter()
            .map(|row| {
                (
                    row.get::<_, String>(0),
                    row.get::<_, i64>(1),
                    row.get::<_, bool>(2),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(
            changes,
            vec![
                ("update".to_string(), 20, false),
                ("delete".to_string(), 30, true),
                ("revive".to_string(), 40, false),
            ]
        );
    }

    Ok(())
}
