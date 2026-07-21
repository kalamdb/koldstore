use crate::common;

use anyhow::{Context, Result};
use koldstore_common::TableKind;
use tokio_postgres::Client;

#[test]
fn user_scope_matrix_targets_active_pgrx_versions() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    assert_eq!(
        common::local_pg_matrix()
            .into_iter()
            .map(|target| target.version)
            .collect::<Vec<_>>(),
        common::expected_pg_versions()
    );
}

#[test]
fn user_scope_matrix_contract_covers_missing_scope_and_cross_scope_denial() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    let missing =
        koldstore::hooks::planner::plan_scope_key_for_read(TableKind::User, None).unwrap_err();
    assert_eq!(missing.to_string(), "koldstore.user_id is not set");

    let planned =
        koldstore::hooks::planner::plan_scope_key_for_read(TableKind::User, Some("user-a"))
            .unwrap()
            .unwrap();
    let row_scope = koldstore_common::ScopeKey::new("user-b").unwrap();
    let denied = koldstore::hooks::executor::enforce_dml_scope(
        TableKind::User,
        Some(planned.as_str()),
        Some(&row_scope),
    )
    .unwrap_err();

    assert_eq!(
        denied.to_string(),
        "row scope `user-b` does not match koldstore.user_id `user-a`"
    );
}

#[tokio::test]
async fn user_scope_migration_installs_fail_closed_policy_on_pgrx() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "user_scope_matrix").await?;
        let table = db.create_user_notes_table("scope_matrix_notes").await?;
        db.manage_user_scoped(&table.relation, "user_id").await?;

        let schema_row = db
            .client
            .query_one(
                r#"
                SELECT table_type, scope_column
                FROM koldstore.schemas
                WHERE table_oid = $1::text::regclass::oid
                  AND active
                "#,
                &[&table.relation],
            )
            .await?;
        assert_eq!(schema_row.get::<_, String>(0), "user");
        assert_eq!(
            schema_row.get::<_, Option<String>>(1).as_deref(),
            Some("user_id")
        );

        let policy_row = db
            .client
            .query_one(
                r#"
                SELECT c.relrowsecurity, count(p.policyname)::bigint
                FROM pg_class c
                LEFT JOIN pg_policies p
                  ON p.schemaname = $2
                 AND p.tablename = $3
                 AND p.policyname = 'koldstore_user_scope_fail_closed'
                WHERE c.oid = $1::text::regclass
                GROUP BY c.relrowsecurity
                "#,
                &[&table.relation, &db.schema, &table.table_name],
            )
            .await?;
        assert!(policy_row.get::<_, bool>(0));
        assert_eq!(policy_row.get::<_, i64>(1), 1);

        db.client
            .batch_execute("SET koldstore.user_id = 'user-a'")
            .await?;
        let active_scope = db
            .client
            .query_one("SELECT current_setting('koldstore.user_id', false)", &[])
            .await?
            .get::<_, String>(0);
        assert_eq!(active_scope, "user-a");
    }

    Ok(())
}

#[tokio::test]
async fn non_owner_rls_is_enforced_for_hot_cold_and_mixed_rows() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "user_scope_rls").await?;
        let table = db.create_user_notes_table("rls_notes").await?;
        db.client
            .batch_execute(&format!(
                "DROP INDEX {schema}.{title_index}; \
                 ALTER TABLE {relation} DROP COLUMN title; \
                 ALTER TABLE {relation} ADD COLUMN title text NOT NULL DEFAULT 'restored'; \
                 CREATE INDEX {title_index} ON {relation} (user_id, title); \
                 CREATE OPERATOR {schema}.= ( \
                   LEFTARG = bigint, RIGHTARG = bigint, FUNCTION = pg_catalog.int8ne \
                 );",
                relation = table.relation,
                schema = db.schema,
                title_index = table.title_index,
            ))
            .await?;
        db.manage_user_scoped(&table.relation, "user_id").await?;
        db.client
            .execute(
                "SELECT koldstore.set_table_auto_flush($1::text::regclass, false)",
                &[&table.relation],
            )
            .await?;
        db.client
            .execute(
                &format!(
                    "INSERT INTO {} (id, user_id, title, body) VALUES (6, '', 'empty', 'empty')",
                    table.relation
                ),
                &[],
            )
            .await?;

        let app_role = format!("{}_app", db.schema);
        db.client
            .batch_execute(&format!(
                r#"
                ALTER TABLE {relation} FORCE ROW LEVEL SECURITY;
                CREATE POLICY koldstore_body_guard ON {relation}
                  AS RESTRICTIVE FOR SELECT USING (body <> 'blocked');
                CREATE ROLE {app_role};
                GRANT USAGE ON SCHEMA {schema} TO {app_role};
                GRANT SELECT ON {relation} TO {app_role};
                SET ROLE {app_role};
                "#,
                relation = table.relation,
                schema = db.schema,
            ))
            .await?;

        assert_user_scope_visibility(&db.client, &table.relation, &[1, 2], &[3])
            .await
            .context("hot-only RLS visibility")?;
        db.client.batch_execute("RESET ROLE").await?;

        common::fence_async_mirror_if_needed(&db.client).await?;
        let flushed = db.flush_table(&table.relation).await?;
        anyhow::ensure!(flushed > 0, "expected a cold flush, flushed {flushed}");
        common::assert_no_active_jobs(&db.client, &table.relation).await?;
        db.client
            .batch_execute("SET koldstore.user_id = 'user-a'")
            .await?;
        let whole_rows = db
            .client
            .query(
                &format!(
                    "SELECT (row_value).id FROM (SELECT notes AS row_value FROM {} AS notes OFFSET 0) q ORDER BY 1",
                    table.relation
                ),
                &[],
            )
            .await
            .context("whole-row cold projection with a dropped column")?;
        let whole_row_ids = whole_rows
            .into_iter()
            .map(|row| row.get::<_, i64>(0))
            .collect::<Vec<_>>();
        anyhow::ensure!(
            whole_row_ids == [1, 2, 3, 6],
            "whole-row cold projection returned {whole_row_ids:?}"
        );

        db.client
            .batch_execute(&format!("SET ROLE {app_role}"))
            .await?;
        assert_user_scope_visibility(&db.client, &table.relation, &[1, 2], &[3])
            .await
            .context("cold-only RLS visibility")?;
        db.client
            .batch_execute("SET koldstore.user_id = 'user-a'")
            .await?;
        let body_filter_ids = db
            .client
            .query(
                &format!("SELECT id FROM {} WHERE body = 'a1'", table.relation),
                &[],
            )
            .await?
            .into_iter()
            .map(|row| row.get::<_, i64>(0))
            .collect::<Vec<_>>();
        anyhow::ensure!(
            body_filter_ids == [1],
            "post-gap body filter returned {body_filter_ids:?}"
        );
        let any_ids = db
            .client
            .query(
                &format!(
                    "SELECT id FROM {} WHERE id < ANY (ARRAY[2, 5]::bigint[]) ORDER BY id",
                    table.relation
                ),
                &[],
            )
            .await?
            .into_iter()
            .map(|row| row.get::<_, i64>(0))
            .collect::<Vec<_>>();
        anyhow::ensure!(any_ids == [1, 2], "scalar ANY returned {any_ids:?}");
        let custom_operator_ids = db
            .client
            .query(
                &format!(
                    "SELECT id FROM {relation} WHERE id OPERATOR({schema}.=) 1 ORDER BY id",
                    relation = table.relation,
                    schema = db.schema,
                ),
                &[],
            )
            .await?
            .into_iter()
            .map(|row| row.get::<_, i64>(0))
            .collect::<Vec<_>>();
        anyhow::ensure!(
            custom_operator_ids == [2],
            "custom equality operator returned {custom_operator_ids:?}"
        );
        db.client.batch_execute("RESET ROLE").await?;

        db.client
            .batch_execute(&format!(
                r#"
                INSERT INTO {relation} (id, user_id, title, body)
                VALUES
                  (4, 'user-a', 'hot-a', 'a-hot'),
                  (5, 'user-b', 'hot-b', 'b-hot');
                "#,
                relation = table.relation,
            ))
            .await?;
        let moved = db
            .client
            .execute(
                &format!(
                    "INSERT INTO {} (id, user_id, title, body) VALUES (1, 'user-b', 'moved-b', 'moved-hot')",
                    table.relation
                ),
                &[],
            )
            .await?;
        anyhow::ensure!(
            moved == 1,
            "expected one scope-changing reinsert, moved {moved}"
        );
        common::fence_async_mirror_if_needed(&db.client).await?;

        db.client
            .batch_execute(&format!("SET ROLE {app_role}"))
            .await?;
        assert_user_scope_visibility(&db.client, &table.relation, &[2, 4], &[1, 3, 5])
            .await
            .context("hot+cold RLS visibility")?;
        db.client.batch_execute("RESET ROLE").await?;
    }

    Ok(())
}

#[tokio::test]
async fn text_pk_pushdown_is_safe_with_nonconforming_strings() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "user_scope_text_pk").await?;
        let relation = db.relation("rls_text_keys");
        let key = "key\\' OR true --";
        db.client
            .batch_execute(&format!(
                "CREATE TABLE {relation} ( \
                   id text PRIMARY KEY, user_id text NOT NULL, payload text NOT NULL, \
                   migration_seq bigint NOT NULL \
                 );"
            ))
            .await?;
        db.client
            .execute(
                &format!(
                    "INSERT INTO {relation} (id, user_id, payload, migration_seq) \
                     VALUES ($1, 'user-a', 'cold-a', 1)"
                ),
                &[&key],
            )
            .await?;
        let mode = common::selected_mirror_capture_mode()?.as_str().to_string();
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass,
                  storage => $2,
                  hot_row_limit => NULL,
                  table_type => 'user',
                  scope_column => 'user_id',
                  migration_order_by => 'migration_seq',
                  mirror_capture_mode => $3
                )
                "#,
                &[&relation, &db.storage_name, &mode],
            )
            .await?;
        db.client
            .execute(
                "SELECT koldstore.set_table_auto_flush($1::text::regclass, false)",
                &[&relation],
            )
            .await?;
        db.client
            .batch_execute(&format!("ALTER TABLE {relation} FORCE ROW LEVEL SECURITY"))
            .await?;
        common::fence_async_mirror_if_needed(&db.client).await?;
        anyhow::ensure!(
            db.flush_table(&relation).await? > 0,
            "expected text-PK flush"
        );
        db.client
            .execute(
                &format!(
                    "INSERT INTO {relation} (id, user_id, payload, migration_seq) \
                     VALUES ($1, 'user-b', 'hot-b', 2)"
                ),
                &[&key],
            )
            .await?;
        common::fence_async_mirror_if_needed(&db.client).await?;

        let app_role = format!("{}_app", db.schema);
        db.client
            .batch_execute(&format!(
                "CREATE ROLE {app_role}; \
                 GRANT USAGE ON SCHEMA {schema} TO {app_role}; \
                 GRANT SELECT ON {relation} TO {app_role}; \
                 SET standard_conforming_strings = off; \
                 SET ROLE {app_role};",
                schema = db.schema,
            ))
            .await?;
        db.client
            .batch_execute("SET koldstore.user_id = 'user-a'")
            .await?;
        let user_a = db
            .client
            .query(&format!("SELECT id FROM {relation} WHERE id = $1"), &[&key])
            .await?;
        anyhow::ensure!(
            user_a.is_empty(),
            "old user scope saw the hot-moved text PK"
        );

        db.client
            .batch_execute("SET koldstore.user_id = 'user-b'")
            .await?;
        let user_b = db
            .client
            .query(&format!("SELECT id FROM {relation} WHERE id = $1"), &[&key])
            .await?;
        anyhow::ensure!(user_b.len() == 1, "new user scope did not see the text PK");
        db.client
            .batch_execute("RESET ROLE; RESET standard_conforming_strings")
            .await?;
    }

    Ok(())
}

#[tokio::test]
async fn nondeterministic_collation_pk_is_rejected_before_scope_moving_merge() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "user_scope_text_collation").await?;
        let has_icu = db
            .client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_collation WHERE collprovider = 'i')",
                &[],
            )
            .await?
            .get::<_, bool>(0);
        if !has_icu {
            continue;
        }

        let relation = db.relation("rls_collated_text_keys");
        let collation = format!("{}.koldstore_case_insensitive", db.schema);
        db.client
            .batch_execute(&format!(
                "CREATE COLLATION {collation} ( \
                   provider = icu, locale = 'und-u-ks-level2', deterministic = false \
                 ); \
                 CREATE TABLE {relation} ( \
                   id text COLLATE {collation} PRIMARY KEY, \
                   user_id text NOT NULL, payload text NOT NULL, \
                   migration_seq bigint NOT NULL \
                 ); \
                 INSERT INTO {relation} (id, user_id, payload, migration_seq) \
                 VALUES ('CaseKey', 'user-a', 'cold-a', 1);"
            ))
            .await?;
        let mode = common::selected_mirror_capture_mode()?.as_str().to_string();
        let error = db
            .client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass,
                  storage => $2,
                  hot_row_limit => NULL,
                  table_type => 'user',
                  scope_column => 'user_id',
                  migration_order_by => 'migration_seq',
                  mirror_capture_mode => $3
                )
                "#,
                &[&relation, &db.storage_name, &mode],
            )
            .await
            .expect_err("nondeterministic primary-key collation must fail closed");
        let message = error
            .as_db_error()
            .map(|error| error.message().to_string())
            .unwrap_or_else(|| error.to_string());
        anyhow::ensure!(
            message.contains("unsupported nondeterministic collation"),
            "unexpected manage_table error: {message}"
        );
        let registrations = db
            .client
            .query_one(
                "SELECT count(*) FROM koldstore.schemas WHERE table_oid = $1::text::regclass::oid",
                &[&relation],
            )
            .await?
            .get::<_, i64>(0);
        anyhow::ensure!(
            registrations == 0,
            "rejected nondeterministic PK left {registrations} schema registrations"
        );
    }

    Ok(())
}

async fn assert_user_scope_visibility(
    client: &Client,
    relation: &str,
    expected_user_a: &[i64],
    expected_user_b: &[i64],
) -> Result<()> {
    client.batch_execute("RESET koldstore.user_id").await?;
    let missing_scope_count = client
        .query_one(&format!("SELECT count(*) FROM {relation}"), &[])
        .await?
        .get::<_, i64>(0);
    anyhow::ensure!(
        missing_scope_count == 0,
        "missing koldstore.user_id must fail closed, returned {missing_scope_count} rows"
    );

    for (scope, expected) in [("user-a", expected_user_a), ("user-b", expected_user_b)] {
        client
            .batch_execute(&format!("SET koldstore.user_id = '{scope}'"))
            .await?;
        let rows = client
            .query(&format!("SELECT id FROM {relation} ORDER BY id"), &[])
            .await?;
        let actual = rows
            .into_iter()
            .map(|row| row.get::<_, i64>(0))
            .collect::<Vec<_>>();
        anyhow::ensure!(
            actual == expected,
            "scope {scope} returned ids {actual:?}, expected {expected:?}"
        );
    }

    Ok(())
}
