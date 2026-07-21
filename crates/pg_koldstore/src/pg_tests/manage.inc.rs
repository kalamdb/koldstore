#[pg_test]
fn manage_describe_flush_unmanage_roundtrip_preserves_values() {
    let suffix = unique_suffix("lifecycle");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    manage_shared(&relation, &storage);

    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (1, 'alpha'), (2, 'beta')"
    ))
    .expect("insert rows");

    let before = spi_get_text(&format!(
        "SELECT string_agg(body, ',' ORDER BY id) FROM {relation}"
    ));
    assert_eq!(before, "alpha,beta");

    let described = Spi::get_one::<pgrx::JsonB>(&format!(
        "SELECT koldstore.describe_table('{relation}'::regclass)"
    ))
    .expect("describe_table")
    .expect("describe_table non-null");
    let described_json = described.0.to_string();
    assert!(
        described_json.contains("storage_binding") && described_json.contains("mirror_rows"),
        "describe_table should report managed storage/mirror state: {described_json}"
    );

    let flushed = flush_table_rows(&relation, true);
    assert!(flushed >= 0, "flush_table should record rows_flushed");

    let after = spi_get_text(&format!(
        "SELECT string_agg(body, ',' ORDER BY id) FROM {relation}"
    ));
    assert_eq!(
        before, after,
        "query result before flush must equal result after flush"
    );

    Spi::run(&format!(
        "SELECT koldstore.unmanage_table('{relation}'::regclass)"
    ))
    .expect("unmanage_table");

    let still_readable = spi_get_text(&format!(
        "SELECT string_agg(body, ',' ORDER BY id) FROM {relation}"
    ));
    assert_eq!(still_readable, "alpha,beta");
}

#[pg_test]
#[should_panic(expected = "managed tables require a primary key")]
fn manage_rejects_table_without_primary_key() {
    let suffix = unique_suffix("nopk");
    let schema = format!("pgtest_{suffix}");
    let storage = register_temp_storage(&suffix);
    Spi::run(&format!("CREATE SCHEMA {schema}")).expect("schema");
    Spi::run(&format!(
        "CREATE TABLE {schema}.no_pk (id bigint, body text)"
    ))
    .expect("create no_pk");

    let _ = Spi::run(&format!(
        r#"
        SELECT koldstore.manage_table(
          table_name => '{schema}.no_pk'::regclass,
          storage => '{storage}',
          hot_row_limit => 1000
        )
        "#
    ));
}

#[pg_test]
#[should_panic(expected = "unsupported PostgreSQL type: tsvector")]
fn manage_rejects_unsupported_column_type() {
    let suffix = unique_suffix("badtype");
    let schema = format!("pgtest_{suffix}");
    let storage = register_temp_storage(&suffix);
    Spi::run(&format!("CREATE SCHEMA {schema}")).expect("schema");
    Spi::run(&format!(
        "CREATE TABLE {schema}.bad_types (id bigint PRIMARY KEY, search tsvector)"
    ))
    .expect("create bad_types");

    let _ = Spi::run(&format!(
        r#"
        SELECT koldstore.manage_table(
          table_name => '{schema}.bad_types'::regclass,
          storage => '{storage}',
          hot_row_limit => 1000
        )
        "#
    ));
}

#[pg_test]
fn manage_table_accepts_unqualified_name_via_search_path() {
    let suffix = unique_suffix("textname");
    let schema = format!("pgtest_{suffix}");
    let storage = register_temp_storage(&suffix);
    create_messages_table(&schema, "messages");
    Spi::run(&format!("SET search_path TO {schema}, public")).expect("search_path");

    Spi::run(&format!(
        r#"
        SELECT koldstore.manage_table(
          table_name => 'messages',
          storage => '{storage}',
          hot_row_limit => 1000,
          migration_order_by => 'id'
        )
        "#
    ))
    .expect("manage_table with unqualified regclass name");

    let managed = spi_get_text(&format!(
        "SELECT count(*)::text FROM koldstore.schemas WHERE table_oid = '{schema}.messages'::regclass::oid AND active"
    ));
    assert_eq!(managed, "1");
}

#[pg_test]
fn flush_table_accepts_unqualified_name_via_search_path() {
    let suffix = unique_suffix("flushname");
    let schema = format!("pgtest_{suffix}");
    let storage = register_temp_storage(&suffix);
    create_messages_table(&schema, "messages");
    Spi::run(&format!("SET search_path TO {schema}, public")).expect("search_path");
    manage_shared("messages", &storage);
    Spi::run("INSERT INTO messages (id, body) VALUES (1, 'a'), (2, 'b')").expect("insert");

    Spi::run("SELECT koldstore.flush_table(table_name => 'messages', force => true)")
        .expect("flush_table with unqualified regclass name");
}

#[pg_test]
#[should_panic(expected = "cross-database references are not implemented")]
fn manage_table_rejects_cross_database_name() {
    let suffix = unique_suffix("crossdb");
    let storage = register_temp_storage(&suffix);
    let _ = Spi::run(&format!(
        r#"
        SELECT koldstore.manage_table(
          table_name => 'chat.public.messages',
          storage => '{storage}',
          hot_row_limit => 1000
        )
        "#
    ));
}

#[pg_test]
#[should_panic(expected = "does not exist")]
fn manage_table_missing_name_errors_clearly() {
    let suffix = unique_suffix("missing");
    let storage = register_temp_storage(&suffix);
    let _ = Spi::run(&format!(
        r#"
        SELECT koldstore.manage_table(
          table_name => 'no_such_table_xyz',
          storage => '{storage}',
          hot_row_limit => 1000
        )
        "#
    ));
}

#[pg_test]
fn supported_datatypes_and_nulls_roundtrip() {
    let suffix = unique_suffix("types");
    let schema = format!("pgtest_{suffix}");
    let storage = register_temp_storage(&suffix);
    Spi::run(&format!("CREATE SCHEMA {schema}")).expect("schema");
    Spi::run(&format!(
        r#"
        CREATE TABLE {schema}.typed (
          id bigint PRIMARY KEY,
          flag boolean,
          amount bigint,
          payload jsonb,
          note text
        )
        "#
    ))
    .expect("create typed");
    manage_shared(&format!("{schema}.typed"), &storage);

    Spi::run(&format!(
        r#"
        INSERT INTO {schema}.typed (id, flag, amount, payload, note)
        VALUES
          (1, true, 12, '{{"a":1}}'::jsonb, 'one'),
          (2, NULL, NULL, NULL, NULL)
        "#
    ))
    .expect("insert typed rows");

    let flag = spi_get_text(&format!(
        "SELECT coalesce(flag::text, 'null') FROM {schema}.typed WHERE id = 2"
    ));
    assert_eq!(flag, "null");
    let note = spi_get_text(&format!(
        "SELECT coalesce(note, 'null') FROM {schema}.typed WHERE id = 2"
    ));
    assert_eq!(note, "null");
    let payload = spi_get_text(&format!(
        "SELECT payload->>'a' FROM {schema}.typed WHERE id = 1"
    ));
    assert_eq!(payload, "1");
}
