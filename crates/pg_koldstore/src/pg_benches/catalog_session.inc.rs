// Session helpers, catalog, and planner paths that every backend may touch.

#[pg_bench(sample_size = 50, measurement_time_ms = 1_000, warm_up_time_ms = 200)]
fn extension_version_spi(b: &mut Bencher) {
    b.iter(|| {
        let version = Spi::get_one::<String>("SELECT koldstore_version()")
            .expect("version spi")
            .expect("version non-null");
        black_box(version);
    });
}

#[pg_bench(sample_size = 50, measurement_time_ms = 1_000, warm_up_time_ms = 200)]
fn snowflake_id_spi(b: &mut Bencher) {
    b.iter(|| {
        let id = spi_get_i64("SELECT snowflake_id()");
        black_box(id);
    });
}

#[pg_bench(sample_size = 40, measurement_time_ms = 1_000, warm_up_time_ms = 200)]
fn session_guc_user_id_roundtrip(b: &mut Bencher) {
    b.iter(|| {
        Spi::run("SET koldstore.user_id = '42'").expect("set user_id");
        let value = spi_get_text("SHOW koldstore.user_id");
        Spi::run("RESET koldstore.user_id").expect("reset user_id");
        black_box(value);
    });
}

fn prepare_managed_for_catalog() {
    let relation = prepare_managed_messages("catalog", false);
    seed_rows(&relation, 50);
}

#[pg_bench(
    setup = prepare_managed_for_catalog,
    sample_size = 40,
    measurement_time_ms = 2_000,
    warm_up_time_ms = 500
)]
fn describe_table_spi(b: &mut Bencher) {
    let relation = ctx("relation");
    b.iter(move || {
        let described = Spi::get_one::<pgrx::JsonB>(&format!(
            "SELECT koldstore.describe_table('{relation}'::regclass)"
        ))
        .expect("describe_table")
        .expect("describe_table non-null");
        black_box(described.0.to_string());
    });
}

#[pg_bench(
    setup = prepare_managed_for_catalog,
    sample_size = 40,
    measurement_time_ms = 2_000,
    warm_up_time_ms = 500
)]
fn explain_managed_select(b: &mut Bencher) {
    let relation = ctx("relation");
    b.iter(move || {
        let plan = spi_get_explain(&format!(
            "EXPLAIN (COSTS OFF) SELECT body FROM {relation} WHERE id = 1"
        ));
        black_box(plan);
    });
}
