use koldstore_common::SqlParamType;
use koldstore_common::{ChangeSource, MirrorChange, MirrorOperation, ScopeKey, SeqId};
use koldstore_merge::events;
use serde_json::json;

fn change(
    table_oid: u32,
    scope: Option<&str>,
    id: i64,
    seq: i64,
    operation: MirrorOperation,
    source: ChangeSource,
) -> MirrorChange {
    MirrorChange {
        table_oid,
        scope_key: scope.map(|scope| ScopeKey::new(scope).unwrap()),
        pk_json: json!({"id": id}),
        operation,
        seq: SeqId::new(seq).unwrap(),
        deleted: operation.is_delete(),
        row_image_json: (!operation.is_delete()).then(|| json!({"id": id, "body": seq})),
        source,
    }
}

#[test]
fn changes_since_filters_table_scope_orders_by_seq_and_keeps_latest_state() {
    let changes = vec![
        change(
            42,
            Some("user-a"),
            1,
            10,
            MirrorOperation::Insert,
            ChangeSource::ColdRecord,
        ),
        change(
            42,
            Some("user-a"),
            1,
            20,
            MirrorOperation::Update,
            ChangeSource::HotMirror,
        ),
        change(
            42,
            Some("user-b"),
            2,
            30,
            MirrorOperation::Update,
            ChangeSource::HotMirror,
        ),
        change(
            99,
            Some("user-a"),
            3,
            40,
            MirrorOperation::Update,
            ChangeSource::ColdRecord,
        ),
    ];

    let result = events::changes_since(
        &changes,
        42,
        Some(&ScopeKey::new("user-a").unwrap()),
        0,
        Some(10),
        None,
    )
    .unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].seq.get(), 20);
    assert_eq!(result[0].operation, MirrorOperation::Update);
    assert_eq!(result[0].source, ChangeSource::HotMirror);
}

#[test]
fn changes_since_validates_limit_and_reports_seq_retention_gap() {
    let changes = [change(
        42,
        None,
        1,
        10,
        MirrorOperation::Insert,
        ChangeSource::ColdRecord,
    )];

    let defaulted = events::changes_since(&changes, 42, None, 0, None, None).unwrap();
    assert_eq!(defaulted.len(), 1);

    let invalid = events::changes_since(&changes, 42, None, 0, Some(0), None).unwrap_err();
    assert_eq!(invalid.to_string(), "limit_rows must be positive");

    let gap = events::changes_since(
        &changes,
        42,
        None,
        5,
        Some(10),
        Some(SeqId::new(10).unwrap()),
    )
    .unwrap_err();
    assert_eq!(
        gap.to_string(),
        "change records before sequence 10 are no longer retained"
    );
}

#[test]
fn mirror_backed_changes_since_plan_reads_mirror_and_not_row_events() {
    let table = koldstore_migrate::QualifiedTableName::parse("app.items").unwrap();
    let mirror = koldstore_migrate::QualifiedTableName::parse("koldstore.items__cl").unwrap();
    let plan =
        events::plan_mirror_changes_since(&table, &mirror, &["id".to_string()], Some("tenant_id"))
            .unwrap();

    assert!(plan
        .statement
        .sql
        .contains("FROM \"koldstore\".\"items__cl\" AS mirror"));
    assert!(plan.statement.sql.contains("mirror.\"seq\" > $1::bigint"));
    assert!(plan
        .statement
        .sql
        .contains("\"mirror\".\"tenant_id\"::text = $2::text"));
    assert!(plan.statement.sql.contains("ORDER BY mirror.\"seq\" ASC"));
    assert!(!plan.statement.sql.contains("row_events"));
    assert_eq!(plan.scope_parameter_index, Some(2));
    assert_eq!(
        plan.statement.param_types,
        vec![
            SqlParamType::BigInt,
            SqlParamType::Text,
            SqlParamType::Integer
        ]
    );
}
