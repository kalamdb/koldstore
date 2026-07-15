fn mirror_name(table: &str) -> String {
    format!("koldstore.{table}__cl")
}

fn reported_mirror_rows(relation: &str) -> i64 {
    spi_get_i64(&format!(
        r#"
        SELECT COALESCE(m.mirror_row_count, 0)::bigint
        FROM koldstore.manifest m
        WHERE m.table_oid = '{relation}'::regclass
          AND m.scope_key = ''
        "#
    ))
}

#[pg_test]
fn mirror_tracks_insert_update_delete_reinsert_and_rollback() {
    let suffix = unique_suffix("mirror");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let mirror = mirror_name(table);
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    manage_shared(&relation, &storage);

    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (1, 'one')"
    ))
    .expect("insert");
    let insert_op = spi_get_i64(&format!("SELECT op::bigint FROM {mirror} WHERE id = 1"));
    let insert_seq = spi_get_i64(&format!("SELECT seq FROM {mirror} WHERE id = 1"));
    assert_eq!(insert_op, 1);

    Spi::run(&format!(
        "UPDATE {relation} SET body = 'two' WHERE id = 1"
    ))
    .expect("update");
    let update_op = spi_get_i64(&format!("SELECT op::bigint FROM {mirror} WHERE id = 1"));
    let update_seq = spi_get_i64(&format!("SELECT seq FROM {mirror} WHERE id = 1"));
    assert_eq!(update_op, 2);
    assert!(update_seq > insert_seq);

    Spi::run(&format!("DELETE FROM {relation} WHERE id = 1")).expect("delete");
    let delete_op = spi_get_i64(&format!("SELECT op::bigint FROM {mirror} WHERE id = 1"));
    let delete_seq = spi_get_i64(&format!("SELECT seq FROM {mirror} WHERE id = 1"));
    assert_eq!(delete_op, 3);
    assert!(delete_seq > update_seq);

    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (1, 'again')"
    ))
    .expect("reinsert");
    let reinsert_op = spi_get_i64(&format!("SELECT op::bigint FROM {mirror} WHERE id = 1"));
    let reinsert_seq = spi_get_i64(&format!("SELECT seq FROM {mirror} WHERE id = 1"));
    assert_eq!(reinsert_op, 1);
    assert!(reinsert_seq > delete_seq);
    let physical_mirror_rows =
        spi_get_i64(&format!("SELECT count(*)::bigint FROM {mirror} WHERE id = 1"));
    let reported = reported_mirror_rows(&relation);
    assert_eq!(physical_mirror_rows, 1);
    assert_eq!(reported, physical_mirror_rows);
    assert_eq!(reported, 1);
}

#[pg_test]
#[should_panic(expected = "does not support primary-key updates")]
fn managed_primary_key_mutation_is_rejected() {
    let suffix = unique_suffix("pkmut");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    manage_shared(&relation, &storage);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (1, 'one')"
    ))
    .expect("insert");

    let _ = Spi::run(&format!("UPDATE {relation} SET id = 2 WHERE id = 1"));
}

#[pg_test]
fn managed_primary_key_noop_assignment_is_allowed() {
    let suffix = unique_suffix("pknoop");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let mirror = mirror_name(table);
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    manage_shared(&relation, &storage);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (1, 'one')"
    ))
    .expect("insert");
    let before_seq = spi_get_i64(&format!("SELECT seq FROM {mirror} WHERE id = 1"));

    Spi::run(&format!("UPDATE {relation} SET id = id WHERE id = 1")).expect("noop pk update");

    assert_eq!(
        spi_get_i64(&format!("SELECT id FROM {relation} WHERE id = 1")),
        1
    );
    let after_seq = spi_get_i64(&format!("SELECT seq FROM {mirror} WHERE id = 1"));
    assert!(after_seq > before_seq);
}

#[pg_test]
fn mirror_bulk_update_and_delete_keep_latest_state() {
    let suffix = unique_suffix("bulkdml");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let mirror = mirror_name(table);
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    manage_shared(&relation, &storage);

    Spi::run(&format!(
        "INSERT INTO {relation} (id, body)
         SELECT g, 'row-' || g::text
         FROM generate_series(1, 1000) AS g"
    ))
    .expect("bulk insert");
    let insert_max_seq = spi_get_i64(&format!("SELECT max(seq) FROM {mirror}"));
    assert_eq!(
        spi_get_i64(&format!("SELECT count(*)::bigint FROM {mirror}")),
        1000
    );

    Spi::run(&format!(
        "UPDATE {relation} SET body = 'updated-' || id::text"
    ))
    .expect("bulk update");
    assert_eq!(
        spi_get_i64(&format!("SELECT count(*)::bigint FROM {mirror}")),
        1000
    );
    assert_eq!(
        spi_get_i64(&format!(
            "SELECT count(*)::bigint FROM {mirror} WHERE op = 2"
        )),
        1000
    );
    let update_min_seq = spi_get_i64(&format!("SELECT min(seq) FROM {mirror}"));
    let update_max_seq = spi_get_i64(&format!("SELECT max(seq) FROM {mirror}"));
    assert!(update_min_seq > insert_max_seq);

    Spi::run(&format!("DELETE FROM {relation}")).expect("bulk delete");
    assert_eq!(
        spi_get_i64(&format!("SELECT count(*)::bigint FROM {relation}")),
        0
    );
    assert_eq!(
        spi_get_i64(&format!("SELECT count(*)::bigint FROM {mirror}")),
        1000
    );
    assert_eq!(
        spi_get_i64(&format!(
            "SELECT count(*)::bigint FROM {mirror} WHERE op = 3"
        )),
        1000
    );
    let delete_min_seq = spi_get_i64(&format!("SELECT min(seq) FROM {mirror}"));
    assert!(delete_min_seq > update_max_seq);
}

#[pg_test]
fn mirror_insert_size_boundaries_keep_one_row_per_pk() {
    let suffix = unique_suffix("insbnd");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let mirror = mirror_name(table);
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    manage_shared(&relation, &storage);

    for &n in &[1_i64, 32, 33, 1000] {
        Spi::run(&format!("DELETE FROM {relation}")).expect("clear heap");
        Spi::run(&format!(
            "INSERT INTO {relation} (id, body)
             SELECT g, 'n-' || g::text
             FROM generate_series(1, {n}) AS g"
        ))
        .expect("sized insert");
        assert_eq!(
            spi_get_i64(&format!("SELECT count(*)::bigint FROM {mirror}")),
            n,
            "mirror rows for insert size {n}"
        );
        assert_eq!(
            spi_get_i64(&format!(
                "SELECT count(*)::bigint FROM {mirror} WHERE op = 1"
            )),
            n,
            "insert op for size {n}"
        );
        assert_eq!(
            reported_mirror_rows(&relation),
            n,
            "reported mirror counter for size {n}"
        );
    }
}

#[pg_test]
fn mirror_bulk_reinsert_over_tombstones_keeps_counter_exact() {
    let suffix = unique_suffix("rebulk");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let mirror = mirror_name(table);
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    manage_shared(&relation, &storage);

    Spi::run(&format!(
        "INSERT INTO {relation} (id, body)
         SELECT g, 'row-' || g::text
         FROM generate_series(1, 40) AS g"
    ))
    .expect("seed");
    Spi::run(&format!("DELETE FROM {relation}")).expect("tombstone");
    assert_eq!(reported_mirror_rows(&relation), 40);

    Spi::run(&format!(
        "INSERT INTO {relation} (id, body)
         SELECT g, 'again-' || g::text
         FROM generate_series(1, 40) AS g"
    ))
    .expect("bulk reinsert");

    let physical = spi_get_i64(&format!("SELECT count(*)::bigint FROM {mirror}"));
    let reported = reported_mirror_rows(&relation);
    assert_eq!(physical, 40);
    assert_eq!(reported, physical);
    assert_eq!(
        spi_get_i64(&format!(
            "SELECT count(*)::bigint FROM {mirror} WHERE op = 1"
        )),
        40
    );
}

#[pg_test]
fn transaction_commit_persists_user_and_mirror_rows() {
    let suffix = unique_suffix("commit");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let mirror = mirror_name(table);
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    manage_shared(&relation, &storage);

    // Outer pg_test transaction already wraps this function; validate visibility inside it.
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (10, 'committed')"
    ))
    .expect("insert");

    assert_eq!(
        spi_get_text(&format!("SELECT body FROM {relation} WHERE id = 10")),
        "committed"
    );
    assert_eq!(
        spi_get_i64(&format!("SELECT count(*)::bigint FROM {mirror} WHERE id = 10")),
        1
    );
}
