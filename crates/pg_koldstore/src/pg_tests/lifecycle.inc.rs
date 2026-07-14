#[pg_test]
fn extension_reports_non_empty_version() {
    let version = spi_get_text("SELECT koldstore_version()");
    assert!(!version.is_empty(), "koldstore_version must be non-empty");
}

#[pg_test]
fn snowflake_id_is_monotonic() {
    let first = spi_get_i64("SELECT snowflake_id()");
    let second = spi_get_i64("SELECT snowflake_id()");
    assert!(second > first, "snowflake_id must advance: {first} then {second}");
}

#[pg_test]
fn koldstore_user_id_guc_roundtrips() {
    // `koldstore_user_id()` SQL helper is still a stub; validate the GUC itself.
    Spi::run("SET koldstore.user_id = '42'").expect("set user_id");
    let value = spi_get_text("SHOW koldstore.user_id");
    assert_eq!(value, "42");
    Spi::run("RESET koldstore.user_id").expect("reset user_id");
}
