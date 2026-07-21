// Lifecycle entrypoints: manage / unmanage / flush. Heavier; fewer samples.

#[pg_bench(
    transaction = "subtransaction_per_batch",
    sample_size = 15,
    measurement_time_ms = 3_000,
    warm_up_time_ms = 500
)]
fn lifecycle_manage_table(b: &mut Bencher) {
    b.iter_batched(
        || {
            let suffix = unique_suffix("manage");
            let schema = format!("pgbench_{suffix}");
            let relation = format!("{schema}.messages");
            let storage = register_temp_storage(&suffix);
            create_messages_table(&schema, "messages");
            (relation, storage)
        },
        |(relation, storage)| {
            manage_shared(&relation, &storage);
            black_box(());
        },
        BatchSize::PerIteration,
    );
}

#[pg_bench(
    transaction = "subtransaction_per_batch",
    sample_size = 15,
    measurement_time_ms = 3_000,
    warm_up_time_ms = 500
)]
fn lifecycle_unmanage_table(b: &mut Bencher) {
    b.iter_batched(
        || {
            let suffix = unique_suffix("unmanage");
            let schema = format!("pgbench_{suffix}");
            let relation = format!("{schema}.messages");
            let storage = register_temp_storage(&suffix);
            create_messages_table(&schema, "messages");
            manage_shared(&relation, &storage);
            seed_rows(&relation, 20);
            relation
        },
        |relation| {
            Spi::run(&format!(
                "SELECT koldstore.unmanage_table('{relation}'::regclass)"
            ))
            .expect("unmanage_table");
            black_box(());
        },
        BatchSize::PerIteration,
    );
}

fn prepare_flushable_table() {
    let relation = prepare_managed_messages("flush", true);
    seed_rows(&relation, 200);
}

#[pg_bench(
    setup = prepare_flushable_table,
    transaction = "subtransaction_per_batch",
    sample_size = 10,
    measurement_time_ms = 4_000,
    warm_up_time_ms = 500
)]
fn lifecycle_flush_table_force(b: &mut Bencher) {
    let relation = ctx("relation");
    let relation_setup = relation.clone();
    b.iter_batched(
        move || {
            seed_rows(&relation_setup, 200);
        },
        move |_| {
            let flushed = flush_table_rows(&relation);
            black_box(flushed);
        },
        BatchSize::PerIteration,
    );
}
