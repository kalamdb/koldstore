#[test]
fn sql_extension_exposes_shared_greenfield_migration_contract() {
    let sql = include_str!("../../../sql/koldstore--0.1.0.sql");

    for needle in [
        "CREATE OR REPLACE FUNCTION SNOWFLAKE_ID()",
        "CREATE OR REPLACE FUNCTION koldstore_version()",
        "CREATE OR REPLACE FUNCTION koldstore.register_storage",
        "CREATE OR REPLACE FUNCTION koldstore.migrate_table",
        "ALTER TABLE %s ADD COLUMN IF NOT EXISTS _seq bigint",
        "ALTER TABLE %s ADD COLUMN IF NOT EXISTS _commit_seq bigint",
        "ALTER TABLE %s ADD COLUMN IF NOT EXISTS _deleted boolean NOT NULL DEFAULT false",
        "PRIMARY KEY",
    ] {
        assert!(
            sql.contains(needle),
            "missing SQL contract fragment: {needle}"
        );
    }
}
