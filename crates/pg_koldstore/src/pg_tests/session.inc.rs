#[pg_test]
fn cold_reads_guc_accepts_documented_values() {
    assert!(spi_succeeds("SET koldstore.cold_reads = 'auto'"));
    assert!(spi_succeeds("SET koldstore.cold_reads = 'on'"));
    assert!(spi_succeeds("SET koldstore.cold_reads = 'off'"));
    let current = spi_get_text("SHOW koldstore.cold_reads");
    assert_eq!(current, "off");
    Spi::run("RESET koldstore.cold_reads").expect("reset cold_reads");
}

#[pg_test]
fn failpoint_guc_defaults_to_empty() {
    let current = spi_get_text("SHOW koldstore.failpoint");
    assert!(
        current.is_empty(),
        "failpoint GUC must default to empty/disabled, got {current:?}"
    );
}
