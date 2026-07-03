#[test]
fn spec_edge_cases_have_regression_coverage_markers() {
    let spec = include_str!("../../../specs/001-pg-kalam-hot-cold-storage/spec.md");
    let sql = include_str!("../../../sql/koldstore--0.1.0.sql");

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

    for implementation_marker in [
        "managed tables require a PRIMARY KEY",
        "unsupported PostgreSQL type",
        "FK constraints are hot-only",
        "standard SQL cold-only UPDATE affects 0 rows in MVP",
        "archive-detach mode",
        "EXPORT TABLE",
    ] {
        assert!(
            sql.contains(implementation_marker),
            "missing implementation marker: {implementation_marker}"
        );
    }
}
