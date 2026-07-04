use koldstore_core::{CommitSeq, LogicalPk, PkColumn, RowOperation, SeqId};
use pg_koldstore::sql::events;
use serde_json::json;

#[test]
fn change_feed_orders_flush_cold_delete_hydrate_and_demigration_boundary_events() {
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
