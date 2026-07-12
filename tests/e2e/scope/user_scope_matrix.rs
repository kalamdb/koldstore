//! User-scoped manage_table and flush path coverage.
//!
//! Covers:
//! - reject `table_type => 'user'` without `scope_column`
//! - manage with an application-owned scope column
//! - flush splitting into multiple cold object paths

#[path = "../common/mod.rs"]
mod common;

use anyhow::Result;
use koldstore_common::TableKind;

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
        pg_koldstore::hooks::planner::plan_scope_key_for_read(TableKind::User, None).unwrap_err();
    assert_eq!(missing.to_string(), "koldstore.user_id is not set");

    let planned =
        pg_koldstore::hooks::planner::plan_scope_key_for_read(TableKind::User, Some("user-a"))
            .unwrap()
            .unwrap();
    let row_scope = koldstore_common::ScopeKey::new("user-b").unwrap();
    let denied = pg_koldstore::hooks::executor::enforce_dml_scope(
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
async fn user_manage_without_scope_column_fails_on_pgrx() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "user_scope_missing").await?;
        let relation = db.relation("notes_missing_scope");
        db.client
            .batch_execute(&format!(
                r#"
                CREATE TABLE {relation} (
                  id bigint PRIMARY KEY,
                  tenant_id text NOT NULL,
                  content text NOT NULL
                );
                "#
            ))
            .await?;

        let error = db
            .client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name     => $1::text::regclass,
                  storage        => $2,
                  hot_row_limit  => NULL,
                  table_type     => 'user',
                  scope_column   => NULL
                )
                "#,
                &[&relation, &db.storage_name],
            )
            .await
            .expect_err("user manage_table without scope_column must fail");

        let message = error.as_db_error().map_or_else(
            || error.to_string(),
            |db_error| db_error.message().to_string(),
        );
        assert!(
            message.contains("user-scoped manage_table requires scope_column"),
            "unexpected error: {message}"
        );
    }

    Ok(())
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
async fn user_scoped_flush_writes_multiple_object_paths_on_pgrx() -> Result<()> {
    const SCOPE_COLUMN: &str = "tenant_id";
    const ROW_COUNT: i64 = 6;
    const MAX_ROWS_PER_FILE: i64 = 2;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "user_scope_flush_paths").await?;
        let relation = db.relation("tenant_notes");
        let table_name = "tenant_notes";

        db.client
            .batch_execute(&format!(
                r#"
                SET koldstore.min_max_rows_per_file = {MAX_ROWS_PER_FILE};
                CREATE TABLE {relation} (
                  id bigint PRIMARY KEY,
                  {SCOPE_COLUMN} text NOT NULL,
                  content text NOT NULL
                );
                INSERT INTO {relation} (id, {SCOPE_COLUMN}, content)
                VALUES
                  (1, 'tenant-a', 'a1'),
                  (2, 'tenant-a', 'a2'),
                  (3, 'tenant-a', 'a3'),
                  (4, 'tenant-b', 'b1'),
                  (5, 'tenant-b', 'b2'),
                  (6, 'tenant-b', 'b3');
                ANALYZE {relation};
                "#
            ))
            .await?;

        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name         => $1::text::regclass,
                  storage            => $2,
                  hot_row_limit      => 1,
                  min_flush_rows     => 1,
                  max_rows_per_file  => $3,
                  table_type         => 'user',
                  scope_column       => $4,
                  migration_order_by => 'id'
                )
                "#,
                &[
                    &relation,
                    &db.storage_name,
                    &MAX_ROWS_PER_FILE,
                    &SCOPE_COLUMN,
                ],
            )
            .await?;
        common::assert_system_columns_absent(&db.client, &relation).await?;
        common::assert_catalog_has_active_schema(&db.client, &relation).await?;

        let scope_row = db
            .client
            .query_one(
                r#"
                SELECT scope_column
                FROM koldstore.schemas
                WHERE table_oid = $1::text::regclass::oid
                  AND active
                "#,
                &[&relation],
            )
            .await?;
        assert_eq!(
            scope_row.get::<_, Option<String>>(0).as_deref(),
            Some(SCOPE_COLUMN)
        );

        let job_id: String = db
            .client
            .query_one(
                "SELECT koldstore.flush_table($1::text::regclass, true)::text",
                &[&relation],
            )
            .await?
            .get(0);
        let flushed: i64 = db
            .client
            .query_one(
                "SELECT rows_flushed FROM koldstore.jobs WHERE id = $1::text::uuid",
                &[&job_id],
            )
            .await?
            .get(0);
        assert_eq!(flushed, ROW_COUNT);
        common::assert_cold_metadata_present(&db.client, &relation).await?;
        common::assert_flush_pruned_hot_storage(&db.client, &relation, ROW_COUNT).await?;
        common::assert_no_active_jobs(&db.client, &relation).await?;

        let segments = db
            .client
            .query(
                r#"
                SELECT object_path, row_count
                FROM koldstore.segments
                WHERE table_oid = $1::text::regclass::oid
                  AND status = 'published'
                ORDER BY batch_number
                "#,
                &[&relation],
            )
            .await?;
        assert!(
            segments.len() as i64 >= 2,
            "expected multiple cold object paths for {relation}, got {}",
            segments.len()
        );

        let mut paths = Vec::with_capacity(segments.len());
        let mut total_rows = 0_i64;
        for segment in &segments {
            let object_path: String = segment.get(0);
            let row_count: i64 = segment.get(1);
            assert!(
                row_count > 0 && row_count <= MAX_ROWS_PER_FILE,
                "segment {object_path} row_count {row_count} exceeds max_rows_per_file {MAX_ROWS_PER_FILE}"
            );
            assert!(
                object_path.contains(table_name)
                    && object_path.contains("segment-")
                    && object_path.ends_with(".parquet"),
                "unexpected object path {object_path}"
            );
            let parquet_path = db.storage_root.join(&object_path);
            assert!(
                parquet_path.exists(),
                "missing cold object {}",
                parquet_path.display()
            );
            paths.push(object_path);
            total_rows = total_rows.saturating_add(row_count);
        }
        assert_eq!(total_rows, ROW_COUNT);

        let unique_paths = paths.iter().collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            unique_paths.len(),
            paths.len(),
            "cold segment object paths must be distinct: {paths:?}"
        );
        assert!(
            unique_paths.len() >= 2,
            "flush must write multiple storage paths, got {paths:?}"
        );
    }

    Ok(())
}
