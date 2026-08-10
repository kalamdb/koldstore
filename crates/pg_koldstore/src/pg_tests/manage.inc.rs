#[pg_test]
fn manage_populated_table_is_independent_of_caller_search_path() {
    let suffix = unique_suffix("manage_search_path");
    let schema = format!("pgtest_{suffix}");
    let table = format!("messages_{suffix}");
    let relation = format!("{schema}.{table}");
    let mirror = change_log_mirror_relation(&relation);
    let storage = register_temp_storage(&suffix);
    create_messages_table(&schema, &table);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (1, 'alpha'), (2, 'beta')"
    ))
    .expect("seed populated table");
    Spi::run("SET LOCAL search_path = pg_catalog").expect("restrict caller search path");

    Spi::run(&format!(
        r#"
        SELECT koldstore.manage_table(
          table_name => '{relation}',
          storage => '{storage}',
          hot_row_limit => 1000,
          min_flush_rows => 1,
          max_rows_per_file => 1000,
          migration_order_by => 'id'
        )
        "#
    ))
    .expect("manage populated table with restricted search path");

    assert_eq!(
        spi_get_i64(&format!("SELECT count(*) FROM {mirror}")),
        2,
        "activation backfill should initialize every existing row"
    );
}

#[pg_test]
fn manage_same_named_tables_in_distinct_schemas_uses_distinct_mirrors() {
    let suffix = unique_suffix("mirror_schema_collision");
    let first_schema = format!("first_{suffix}");
    let second_schema = format!("second_{suffix}");
    let first_relation = format!("{first_schema}.messages");
    let second_relation = format!("{second_schema}.messages");
    let first_mirror = format!("koldstore.{first_schema}_messages__cl");
    let second_mirror = format!("koldstore.{second_schema}_messages__cl");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&first_schema, "messages");
    create_messages_table(&second_schema, "messages");
    manage_shared(&first_relation, &storage);
    manage_shared(&second_relation, &storage);

    assert_ne!(first_mirror, second_mirror);
    assert_eq!(spi_get_i64(&format!("SELECT count(*) FROM {first_mirror}")), 0);
    assert_eq!(spi_get_i64(&format!("SELECT count(*) FROM {second_mirror}")), 0);

    Spi::run(&format!(
        "SELECT koldstore.unmanage_table('{first_relation}'::regclass)"
    ))
    .expect("unmanage first relation");
    assert_eq!(
        spi_get_i64(&format!(
            "SELECT (to_regclass('{second_mirror}') IS NOT NULL)::int"
        )),
        1,
        "unmanaging one source must not drop another source's mirror"
    );
}

#[pg_test]
fn unmanage_refuses_a_legacy_shared_mirror() {
    let suffix = unique_suffix("legacy_shared_mirror");
    let first_schema = format!("first_{suffix}");
    let second_schema = format!("second_{suffix}");
    let first_relation = format!("{first_schema}.messages");
    let second_relation = format!("{second_schema}.messages");
    let first_mirror = format!("koldstore.{first_schema}_messages__cl");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&first_schema, "messages");
    create_messages_table(&second_schema, "messages");
    manage_shared(&first_relation, &storage);
    manage_shared(&second_relation, &storage);
    Spi::run(&format!(
        "UPDATE koldstore.schemas \
         SET mirror_relation = '{first_mirror}'::regclass \
         WHERE table_oid = '{second_relation}'::regclass"
    ))
    .expect("simulate a legacy shared mirror catalog row");

    Spi::run(&format!(
        r#"
        DO $$
        BEGIN
          BEGIN
            PERFORM koldstore.unmanage_table('{first_relation}'::regclass);
            RAISE EXCEPTION 'unmanage unexpectedly accepted a shared mirror';
          EXCEPTION WHEN OTHERS THEN
            IF SQLERRM = 'unmanage unexpectedly accepted a shared mirror' THEN
              RAISE;
            END IF;
          END;
        END
        $$;
        "#
    ))
    .expect("unmanage must fail closed when another active table owns the same mirror");
    assert_eq!(
        spi_get_i64(&format!(
            "SELECT (to_regclass('{first_mirror}') IS NOT NULL)::int"
        )),
        1,
        "a failed unmanage must leave the shared mirror intact"
    );
}

#[pg_test]
fn managed_mirror_follows_source_table_and_schema_renames() {
    let suffix = unique_suffix("mirror_rename");
    let original_schema = format!("original_{suffix}");
    let moved_schema = format!("moved_{suffix}");
    let renamed_schema = format!("renamed_{suffix}");
    let original_relation = format!("{original_schema}.messages");
    let moved_relation = format!("{moved_schema}.events");
    let renamed_relation = format!("{renamed_schema}.events");
    let original_mirror = format!("koldstore.{original_schema}_messages__cl");
    let table_renamed_mirror = format!("koldstore.{original_schema}_events__cl");
    let moved_mirror = format!("koldstore.{moved_schema}_events__cl");
    let schema_renamed_mirror = format!("koldstore.{renamed_schema}_events__cl");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&original_schema, "messages");
    manage_shared(&original_relation, &storage);
    Spi::run(&format!("ALTER TABLE {original_relation} RENAME TO events"))
        .expect("rename managed table");
    assert_eq!(
        spi_get_i64(&format!(
            "SELECT (to_regclass('{table_renamed_mirror}') IS NOT NULL)::int"
        )),
        1,
        "table rename must rename its mirror"
    );
    assert_eq!(
        spi_get_i64(&format!(
            "SELECT (to_regclass('{original_mirror}') IS NULL)::int"
        )),
        1,
        "old generated mirror name must be released"
    );

    Spi::run(&format!("CREATE SCHEMA {moved_schema}")).expect("create target schema");
    Spi::run(&format!("ALTER TABLE {original_schema}.events SET SCHEMA {moved_schema}"))
        .expect("move managed table to another schema");
    assert_eq!(
        spi_get_i64(&format!(
            "SELECT (to_regclass('{moved_mirror}') IS NOT NULL)::int"
        )),
        1,
        "moving a table to another schema must rename its mirror"
    );
    assert_eq!(
        spi_get_i64(&format!(
            "SELECT (to_regclass('{table_renamed_mirror}') IS NULL)::int"
        )),
        1,
        "moving a table must release the prior schema-qualified mirror name"
    );
    assert_eq!(
        spi_get_i64(&format!("SELECT count(*) FROM {moved_relation}")),
        0
    );

    Spi::run(&format!("ALTER SCHEMA {moved_schema} RENAME TO {renamed_schema}"))
        .expect("rename schema containing managed table");
    assert_eq!(
        spi_get_i64(&format!(
            "SELECT (to_regclass('{schema_renamed_mirror}') IS NOT NULL)::int"
        )),
        1,
        "schema rename must rename its managed mirrors"
    );

    create_messages_table(&renamed_schema, "messages");
    manage_shared(&format!("{renamed_schema}.messages"), &storage);
    assert_eq!(
        spi_get_i64(&format!(
            "SELECT (to_regclass('koldstore.{renamed_schema}_messages__cl') IS NOT NULL)::int"
        )),
        1,
        "a new table may reuse the source's former name"
    );
    assert_eq!(
        spi_get_i64(&format!("SELECT count(*) FROM {renamed_relation}")),
        0
    );
}

#[pg_test]
fn alter_table_manages_and_replaces_flush_policy() {
    // register_temp_storage pre-provisions the async slot. ALTER TABLE now also
    // calls prepare_capture before SPI, but #[pg_test] is one transaction so
    // register_storage would still assign an XID before that path runs.
    let suffix = unique_suffix("alterpolicy");
    let schema = format!("pgtest_{suffix}");
    let relation = format!("{schema}.messages");
    let storage = register_temp_storage(&suffix);
    create_messages_table(&schema, "messages");

    Spi::run(&format!(r#"
        ALTER TABLE {relation} SET (
          koldstore_enabled = true,
          koldstore_storage = '{storage}',
          koldstore_hot_row_limit = 1000,
          koldstore_min_flush_rows = 10,
          koldstore_max_rows_per_file = 1000
        )
    "#)).expect("manage through ALTER TABLE");
    let options = Spi::get_one::<pgrx::JsonB>(&format!(
        "SELECT options FROM koldstore.schemas WHERE table_oid='{relation}'::regclass"
    )).unwrap().unwrap().0;
    assert_eq!(options["flush_policy"]["type"], "row_limit");
    Spi::run(&format!(
        "ALTER TABLE {relation} SET (fillfactor = 80, koldstore_move_after = 'P90D')"
    ))
        .expect("replace policy through ALTER TABLE");
    assert!(Spi::get_one::<bool>(&format!("SELECT 'fillfactor=80' = ANY(reloptions) FROM pg_class WHERE oid='{relation}'::regclass")).unwrap().unwrap());
    let policy_type = spi_get_text(&format!(
        "SELECT options->'flush_policy'->>'type' FROM koldstore.schemas WHERE table_oid='{relation}'::regclass"
    ));
    assert_eq!(policy_type, "older_than");
}

#[pg_test]
fn alter_table_manages_a_populated_table() {
    // register_temp_storage pre-provisions the async slot before any SPI write.
    let suffix = unique_suffix("alterpopulated");
    let schema = format!("pgtest_{suffix}");
    let relation = format!("{schema}.messages");
    let storage = register_temp_storage(&suffix);
    Spi::run(&format!("CREATE SCHEMA {schema}")).expect("create schema");
    Spi::run(&format!(
        "CREATE TABLE {relation} (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, body text NOT NULL)"
    ))
    .expect("create identity-backed table");
    Spi::run(&format!(
        "INSERT INTO {relation} (body) VALUES ('alpha'), ('beta')"
    ))
    .expect("seed populated table");
    // Seed INSERT assigned an XID; keep the WAL applier off the apply lock for
    // the rest of this uncommitted #[pg_test] transaction.
    hold_apply_lock_for_populated_manage();

    Spi::run(&format!(
        r#"
        ALTER TABLE {relation} SET (
          koldstore_enabled = true,
          koldstore_storage = '{storage}',
          koldstore_hot_row_limit = 1,
          koldstore_min_flush_rows = 1,
          koldstore_max_rows_per_file = 1000,
          koldstore_max_rows_per_flush = 1
        )
        "#
    ))
    .expect("manage populated table through ALTER TABLE");

    assert_eq!(
        spi_get_text(&format!(
            "SELECT string_agg(body, ',' ORDER BY id) FROM {relation}"
        )),
        "alpha,beta"
    );
}

#[pg_test]
fn manage_describe_flush_unmanage_roundtrip_preserves_values() {
    let suffix = unique_suffix("lifecycle");
    let schema = format!("pgtest_{suffix}");
    let table = "messages";
    let relation = format!("{schema}.{table}");
    let storage = register_temp_storage(&suffix);

    create_messages_table(&schema, table);
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body) VALUES (1, 'alpha'), (2, 'beta')"
    ))
    .expect("insert rows");
    manage_shared(&relation, &storage);

    let before = spi_get_text(&format!(
        "SELECT string_agg(body, ',' ORDER BY id) FROM {relation}"
    ));
    assert_eq!(before, "alpha,beta");

    let described = Spi::get_one::<pgrx::JsonB>(&format!(
        "SELECT koldstore.table_status('{relation}'::regclass)"
    ))
    .expect("table_status")
    .expect("table_status non-null");
    let described_json = described.0.to_string();
    assert!(
        described_json.contains("storage_binding") && described_json.contains("mirror_rows"),
        "table_status should report managed storage/mirror state: {described_json}"
    );

    let flushed = flush_table_rows(&relation, true);
    assert!(flushed >= 2, "flush_table should publish seeded rows");

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
