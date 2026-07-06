#[test]
fn spec_edge_cases_have_regression_coverage_markers() {
    let spec = include_str!("../../../specs/001-pg-kalam-hot-cold-storage/spec.md");

    for edge_case in [
        "Migrating without a primary key",
        "unsupported data types",
        "inbound/outbound FKs",
        "Concurrent writes to same PK",
        "gaps are allowed",
        "Object store unavailable during flush",
        "Object store unavailable during SELECT requiring cold",
        "orphan and cleans or quarantines",
        "Standard SQL cold-only UPDATE",
        "Cold-only DELETE",
        "Reinsert after hot tombstone",
        "Mutable app-column filter",
        "COPY FROM",
        "COPY TO",
        "Logical replication",
    ] {
        assert!(spec.contains(edge_case), "missing edge case: {edge_case}");
    }

    assert!(!koldstore_migrate::constraints::primary_key_shape_supported(&[]));
    assert!(!koldstore_migrate::constraints::type_supported("bytea"));
    assert!(!koldstore_migrate::constraints::fk_policy_allowed(
        true, true, false
    ));
    assert_eq!(
        koldstore_migrate::rehydrate::DemigrateOptions {
            rehydrate: false,
            drop_cold: false,
        }
        .mode(),
        koldstore_migrate::rehydrate::DemigrationMode::ArchiveDetach
    );
    assert!(matches!(
        koldstore_flush::ops::classify_command("EXPORT TABLE app.items"),
        Some(koldstore_flush::ops::OpsCommand::ExportTable { .. })
    ));
}
