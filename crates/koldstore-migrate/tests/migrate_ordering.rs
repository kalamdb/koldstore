use koldstore_migrate::order::{
    choose_migration_ordering, CatalogColumn, CatalogPrimaryKey, MigrationOrdering,
    MigrationOrderingRequest, OrderingSource,
};

#[test]
fn migration_uses_auto_increment_single_primary_key_as_oldest_first_order() {
    let request = MigrationOrderingRequest {
        primary_key: CatalogPrimaryKey::single(1, "id"),
        columns: vec![
            CatalogColumn::bigint(1, "id").primary_key().identity(),
            CatalogColumn::text(2, "body"),
        ],
        explicit_order_column: None,
    };

    let ordering = choose_migration_ordering(&request).unwrap();

    assert_eq!(
        ordering,
        MigrationOrdering {
            column: "id".to_string(),
            source: OrderingSource::AutoIncrementPrimaryKey,
            ascending_oldest_first: true,
        }
    );
}

#[test]
fn migration_accepts_explicit_timestamp_order_column_when_primary_key_is_not_incremental() {
    let request = MigrationOrderingRequest {
        primary_key: CatalogPrimaryKey::single(1, "uuid"),
        columns: vec![
            CatalogColumn::uuid(1, "uuid").primary_key(),
            CatalogColumn::timestamp(2, "created_at"),
        ],
        explicit_order_column: Some("created_at".to_string()),
    };

    let ordering = choose_migration_ordering(&request).unwrap();

    assert_eq!(ordering.column, "created_at");
    assert_eq!(ordering.source, OrderingSource::ExplicitColumn);
    assert!(ordering.ascending_oldest_first);
}

#[test]
fn migration_rejects_existing_rows_without_stable_ordering_indicator() {
    let request = MigrationOrderingRequest {
        primary_key: CatalogPrimaryKey::single(1, "uuid"),
        columns: vec![
            CatalogColumn::uuid(1, "uuid").primary_key(),
            CatalogColumn::text(2, "body"),
        ],
        explicit_order_column: None,
    };

    let error = choose_migration_ordering(&request).unwrap_err();

    assert_eq!(
        error.to_string(),
        "existing table migration requires an auto-increment primary key or explicit order column"
    );
}

#[test]
fn migration_rejects_explicit_order_column_that_is_not_orderable() {
    let request = MigrationOrderingRequest {
        primary_key: CatalogPrimaryKey::single(1, "id"),
        columns: vec![
            CatalogColumn::bigint(1, "id").primary_key(),
            CatalogColumn::jsonb(2, "payload"),
        ],
        explicit_order_column: Some("payload".to_string()),
    };

    let error = choose_migration_ordering(&request).unwrap_err();

    assert_eq!(
        error.to_string(),
        "migration order column `payload` has unsupported type `jsonb`"
    );
}
