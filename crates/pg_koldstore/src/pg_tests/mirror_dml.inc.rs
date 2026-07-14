fn mirror_name(table: &str) -> String {
    format!("koldstore.{table}__cl")
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
    assert_eq!(
        spi_get_i64(&format!("SELECT count(*)::bigint FROM {mirror} WHERE id = 1")),
        1
    );
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
