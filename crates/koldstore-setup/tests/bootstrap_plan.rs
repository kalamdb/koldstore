//! Tests for typed extension setup plans.

use std::collections::BTreeSet;

use koldstore_setup::{
    missing_catalog_indexes, missing_catalog_tables, BootstrapObjectKind, BootstrapPlan,
    REQUIRED_CATALOG_INDEXES, REQUIRED_CATALOG_TABLES,
};

const INSTALL_SQL: &str = include_str!("../../pg_koldstore/sql/koldstore--0.1.0.sql");

#[test]
fn canonical_install_sql_has_required_setup_objects() {
    let plan = BootstrapPlan::from_sql(INSTALL_SQL);

    assert!(missing_catalog_tables(&plan).is_empty());
    assert!(missing_catalog_indexes(&plan).is_empty());
    assert!(plan.contains_object(BootstrapObjectKind::Schema, "koldstore"));
    assert!(plan.contains_object(
        BootstrapObjectKind::CompositeType,
        "koldstore.managed_table_info"
    ));
    assert!(plan.contains_object(BootstrapObjectKind::CompositeType, "koldstore.dml_result"));
    assert!(plan.contains_object(BootstrapObjectKind::CompositeType, "koldstore.change_event"));
    assert!(plan.contains_object(BootstrapObjectKind::Sequence, "koldstore.global_seq"));
    assert!(plan.contains_object(BootstrapObjectKind::Sequence, "koldstore.global_commit_seq"));
}

#[test]
fn cold_segment_stats_uses_stable_column_id_identity() {
    let table_start = INSTALL_SQL
        .find("CREATE TABLE IF NOT EXISTS koldstore.cold_segment_stats")
        .unwrap();
    let table_sql = &INSTALL_SQL[table_start..];
    let table_end = table_sql.find(");").unwrap();
    let table_sql = &table_sql[..table_end];

    assert!(table_sql.contains("column_id smallint NOT NULL"));
    assert!(table_sql.contains("PRIMARY KEY (segment_id, column_id)"));
    assert!(!table_sql.contains("column_name"));
    assert!(INSTALL_SQL
        .contains("ON koldstore.cold_segment_stats (table_oid, scope_key, column_id, segment_id)"));
}

#[test]
fn canonical_install_sql_has_no_duplicate_named_objects() {
    let plan = BootstrapPlan::from_sql(INSTALL_SQL);

    assert_eq!(plan.duplicate_object_names(), Vec::<String>::new());
}

#[test]
fn catalog_index_specs_point_at_installed_tables() {
    let table_names = REQUIRED_CATALOG_TABLES
        .iter()
        .map(|table| table.name)
        .collect::<BTreeSet<_>>();

    for index in REQUIRED_CATALOG_INDEXES {
        assert!(
            table_names.contains(index.table),
            "index {} targets unknown table {}",
            index.name,
            index.table
        );
    }
}
