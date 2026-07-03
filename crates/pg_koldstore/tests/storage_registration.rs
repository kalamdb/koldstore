#[test]
fn sql_extension_exposes_storage_registration_and_redaction_contract() {
    let sql = include_str!("../../../sql/koldstore--0.1.0.sql");

    for needle in [
        "CREATE OR REPLACE FUNCTION koldstore.register_storage",
        "CREATE OR REPLACE FUNCTION koldstore.alter_storage_credentials",
        "RETURNS uuid",
        "jsonb_strip_nulls",
        "credentials = EXCLUDED.credentials",
        "REVOKE ALL ON koldstore.storage FROM PUBLIC",
    ] {
        assert!(
            sql.contains(needle),
            "missing storage SQL fragment: {needle}"
        );
    }
}
