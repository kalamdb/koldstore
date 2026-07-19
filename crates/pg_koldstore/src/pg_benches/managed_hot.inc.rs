// Managed hot-path benches. Compare absolute times to plain_heap_* for overhead.

fn prepare_managed_hot_table() {
    let _ = prepare_managed_messages("hot", false);
}

#[pg_bench(
    setup = prepare_managed_hot_table,
    transaction = "subtransaction_per_batch",
    sample_size = 30,
    measurement_time_ms = 2_000,
    warm_up_time_ms = 500
)]
fn managed_hot_insert_batch(b: &mut Bencher) {
    let relation = ctx("relation");
    let relation_setup = relation.clone();
    b.iter_batched(
        move || {
            let base = spi_get_i64(&format!(
                "SELECT COALESCE(MAX(id), 0) FROM {relation_setup}"
            ));
            (base + 1)..(base + 33)
        },
        move |ids| {
            for id in ids {
                Spi::run(&format!(
                    "INSERT INTO {relation} (id, body) VALUES ({id}, 'bench-{id}')"
                ))
                .expect("insert");
            }
            black_box(());
        },
        BatchSize::SmallInput,
    );
}

#[pg_bench(
    setup = prepare_managed_hot_table,
    sample_size = 40,
    measurement_time_ms = 2_000,
    warm_up_time_ms = 500
)]
fn managed_hot_count_scan(b: &mut Bencher) {
    let relation = ctx("relation");
    seed_rows(&relation, 500);
    b.iter(move || {
        let count = spi_get_i64(&format!("SELECT COUNT(*)::bigint FROM {relation}"));
        black_box(count);
    });
}

#[pg_bench(
    setup = prepare_managed_hot_table,
    sample_size = 50,
    measurement_time_ms = 2_000,
    warm_up_time_ms = 500
)]
fn managed_hot_pk_lookup(b: &mut Bencher) {
    let relation = ctx("relation");
    seed_rows(&relation, 1_000);
    b.iter(move || {
        let body = spi_get_text(&format!(
            "SELECT body FROM {relation} WHERE id = 500"
        ));
        black_box(body);
    });
}

#[pg_bench(
    setup = prepare_managed_hot_table,
    transaction = "subtransaction_per_iteration",
    sample_size = 40,
    measurement_time_ms = 2_000,
    warm_up_time_ms = 500
)]
fn managed_hot_update_by_pk(b: &mut Bencher) {
    let relation = ctx("relation");
    seed_rows(&relation, 1_000);
    b.iter(move || {
        Spi::run(&format!(
            "UPDATE {relation} SET body = 'upd-' || id::text WHERE id = 500"
        ))
        .expect("update");
        black_box(());
    });
}

#[pg_bench(
    setup = prepare_managed_hot_table,
    transaction = "subtransaction_per_iteration",
    sample_size = 30,
    measurement_time_ms = 2_000,
    warm_up_time_ms = 500
)]
fn managed_hot_delete_by_pk(b: &mut Bencher) {
    let relation = ctx("relation");
    seed_rows(&relation, 1_000);
    // Each iteration deletes id=1; subtransaction rollback restores the row.
    b.iter(move || {
        Spi::run(&format!("DELETE FROM {relation} WHERE id = 1")).expect("delete");
        black_box(());
    });
}

#[pg_bench(
    setup = prepare_managed_hot_table,
    sample_size = 30,
    measurement_time_ms = 2_000,
    warm_up_time_ms = 500
)]
fn managed_hot_ordered_limit(b: &mut Bencher) {
    let relation = ctx("relation");
    seed_rows(&relation, 1_000);
    b.iter(move || {
        let body = spi_get_text(&format!(
            "SELECT body FROM {relation} ORDER BY id DESC LIMIT 1"
        ));
        black_box(body);
    });
}
