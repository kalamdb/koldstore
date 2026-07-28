use crate::common;

use anyhow::Result;

#[tokio::test]
async fn alter_table_add_nullable_column_refreshes_schema_and_reads_old_cold_rows() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "schema_evolution_add").await?;
        let table = db.create_indexed_items_table("evolve_items", 12).await?;
        db.manage_shared(&table.relation, "id").await?;
        assert_eq!(db.flush_table(&table.relation).await?, 12);

        // Stop the async applier before ALTER/INSERT so those commits stay in WAL
        // until flush's fence applies them in the same transaction. That is the
        // path where pending counter deltas must be visible to flush selection.
        if common::selected_mirror_capture_mode()?.is_async() {
            let dbname: String = db
                .client
                .query_one("SELECT current_database()", &[])
                .await?
                .get(0);
            db.client
                .batch_execute(&format!(
                    "ALTER DATABASE \"{dbname}\" SET koldstore.internal_async_mirror_worker = off; \
                     SET koldstore.internal_async_mirror_worker = off"
                ))
                .await?;
            let _ = common::terminate_async_worker(&db.client).await?;
        }

        db.client
            .batch_execute(&format!(
                r#"
                ALTER TABLE {} ADD COLUMN note text;
                INSERT INTO {} (id, account_id, title, qty, category, note)
                VALUES
                  (100, 1, 'new-100', 10, 'new', 'after-alter'),
                  (101, 1, 'new-101', 11, 'new', 'after-alter-2');
                "#,
                table.relation, table.relation
            ))
            .await?;

        let flushed = db.flush_table(&table.relation).await;
        if common::selected_mirror_capture_mode()?.is_async() {
            let dbname: String = db
                .client
                .query_one("SELECT current_database()", &[])
                .await?
                .get(0);
            db.client
                .batch_execute(&format!(
                    "ALTER DATABASE \"{dbname}\" RESET koldstore.internal_async_mirror_worker; \
                     RESET koldstore.internal_async_mirror_worker"
                ))
                .await?;
        }
        assert_eq!(flushed?, 2);

        let schema = db
            .client
            .query_one(
                r#"
                SELECT version, columns::text
                FROM koldstore.schemas
                WHERE table_oid = $1::text::regclass::oid
                  AND active
                "#,
                &[&table.relation],
            )
            .await?;
        let version: i32 = schema.get(0);
        let columns_text: String = schema.get(1);
        let columns: serde_json::Value = serde_json::from_str(&columns_text)?;
        assert_eq!(version, 2);
        assert!(columns
            .as_array()
            .is_some_and(|columns| columns.iter().any(|column| {
                column.get("name").and_then(serde_json::Value::as_str) == Some("note")
            })));

        let rows = db
            .client
            .query(
                &format!(
                    "SELECT id, note FROM {} WHERE id IN (1, 100) ORDER BY id",
                    table.relation
                ),
                &[],
            )
            .await?;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get::<_, i64>(0), 1);
        assert_eq!(rows[0].get::<_, Option<String>>(1), None);
        assert_eq!(rows[1].get::<_, i64>(0), 100);
        assert_eq!(
            rows[1].get::<_, Option<String>>(1).as_deref(),
            Some("after-alter")
        );
    }

    Ok(())
}

#[tokio::test]
async fn rename_column_preserves_column_id_and_reads_across_schema_versions() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "schema_evolution_rename").await?;
        let table = db.create_indexed_items_table("rename_items", 10).await?;
        db.manage_shared(&table.relation, "id").await?;
        assert_eq!(db.flush_table(&table.relation).await?, 10);

        let before = db
            .client
            .query_one(
                r#"
                SELECT version,
                       (
                         SELECT c->>'column_id'
                         FROM jsonb_array_elements(columns) AS c
                         WHERE c->>'name' = 'qty'
                       ) AS qty_column_id,
                       (
                         SELECT count(*)
                         FROM koldstore.cold_segment_index st
                         JOIN koldstore.cold_segments cs
                           ON cs.segment_id = st.segment_id
                         WHERE cs.table_oid = s.table_oid
                           AND cs.status = 'active'
                           AND st.column_id = (
                             SELECT (c->>'column_id')::smallint
                             FROM jsonb_array_elements(s.columns) AS c
                             WHERE c->>'name' = 'qty'
                           )
                       ) AS qty_stats_rows,
                       (
                         SELECT count(*)
                         FROM koldstore.cold_segment_index st
                         JOIN koldstore.cold_segments cs
                           ON cs.segment_id = st.segment_id
                         WHERE cs.table_oid = s.table_oid
                           AND cs.status = 'active'
                           AND st.column_id = (
                             SELECT (c->>'column_id')::smallint
                             FROM jsonb_array_elements(s.columns) AS c
                             WHERE c->>'name' = 'title'
                           )
                       ) AS title_stats_rows
                FROM koldstore.schemas s
                WHERE table_oid = $1::text::regclass::oid
                  AND active
                "#,
                &[&table.relation],
            )
            .await?;
        let version_before: i32 = before.get(0);
        let qty_column_id: String = before.get(1);
        let qty_stats_before: i64 = before.get(2);
        let title_stats_before: i64 = before.get(3);
        assert_eq!(version_before, 1);
        assert!(
            qty_stats_before > 0,
            "expected Sort Key index rows for integer qty before rename"
        );
        assert_eq!(
            title_stats_before, 0,
            "text title must not produce Sort Key V1 index rows"
        );

        db.client
            .batch_execute(&format!(
                r#"
                ALTER TABLE {} RENAME COLUMN title TO headline;
                ALTER TABLE {} RENAME COLUMN qty TO amount;
                INSERT INTO {} (id, account_id, headline, amount, category)
                VALUES (100, 1, 'after-rename', 10, 'new');
                "#,
                table.relation, table.relation, table.relation
            ))
            .await?;
        assert_eq!(db.flush_table(&table.relation).await?, 1);

        let after = db
            .client
            .query_one(
                r#"
                SELECT version,
                       (
                         SELECT c->>'column_id'
                         FROM jsonb_array_elements(columns) AS c
                         WHERE c->>'name' = 'amount'
                       ) AS amount_column_id,
                       (
                         SELECT count(*)
                         FROM jsonb_array_elements(columns) AS c
                         WHERE c->>'name' IN ('title', 'qty')
                       ) AS old_name_rows,
                       (
                         SELECT count(*)
                         FROM koldstore.cold_segment_index st
                         JOIN koldstore.cold_segments cs
                           ON cs.segment_id = st.segment_id
                         WHERE cs.table_oid = s.table_oid
                           AND cs.status = 'active'
                           AND st.column_id = $2::smallint
                       ) AS stats_rows_for_id
                FROM koldstore.schemas s
                WHERE table_oid = $1::text::regclass::oid
                  AND active
                "#,
                &[&table.relation, &qty_column_id.parse::<i16>()?],
            )
            .await?;
        let version_after: i32 = after.get(0);
        let amount_column_id: String = after.get(1);
        let old_name_rows: i64 = after.get(2);
        let stats_rows_for_id: i64 = after.get(3);
        assert!(
            version_after > version_before,
            "rename should refresh schema version"
        );
        assert_eq!(amount_column_id, qty_column_id);
        assert_eq!(old_name_rows, 0);
        assert!(
            stats_rows_for_id >= qty_stats_before,
            "rename must keep cold index rows attached to the same column_id"
        );

        let rows = db
            .client
            .query(
                &format!(
                    "SELECT id, headline FROM {} WHERE id IN (1, 100) ORDER BY id",
                    table.relation
                ),
                &[],
            )
            .await?;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get::<_, i64>(0), 1);
        assert_eq!(
            rows[0].get::<_, String>(1),
            "item-000001",
            "old cold rows must remain readable under the renamed column"
        );
        assert_eq!(rows[1].get::<_, i64>(0), 100);
        assert_eq!(rows[1].get::<_, String>(1), "after-rename");
    }

    Ok(())
}

#[tokio::test]
async fn drop_and_add_same_column_name_uses_new_column_id() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "schema_evolution_drop_add").await?;
        let table = db.create_indexed_items_table("drop_add_items", 6).await?;
        db.manage_shared(&table.relation, "id").await?;
        assert_eq!(db.flush_table(&table.relation).await?, 6);

        let before = db
            .client
            .query_one(
                r#"
                SELECT
                  (SELECT c->>'column_id'
                   FROM jsonb_array_elements(columns) AS c
                   WHERE c->>'name' = 'qty') AS qty_column_id,
                  (
                    SELECT count(*)
                    FROM koldstore.cold_segment_index st
                    JOIN koldstore.cold_segments cs
                      ON cs.segment_id = st.segment_id
                    WHERE cs.table_oid = s.table_oid
                      AND cs.status = 'active'
                      AND st.column_id = (
                        SELECT (c->>'column_id')::smallint
                        FROM jsonb_array_elements(s.columns) AS c
                        WHERE c->>'name' = 'qty'
                      )
                  ) AS qty_stats_rows
                FROM koldstore.schemas s
                WHERE table_oid = $1::text::regclass::oid
                  AND active
                "#,
                &[&table.relation],
            )
            .await?;
        let old_column_id: String = before.get(0);
        let old_stats_rows: i64 = before.get(1);
        assert!(
            old_stats_rows > 0,
            "qty is indexed so flush should write cold_segment_index for it"
        );

        db.client
            .batch_execute(&format!(
                r#"
                DROP INDEX IF EXISTS {schema}.{table_name}_qty_idx;
                ALTER TABLE {relation} DROP COLUMN qty;
                ALTER TABLE {relation} ADD COLUMN qty integer NOT NULL DEFAULT 42;
                INSERT INTO {relation} (id, account_id, title, qty, category)
                VALUES (200, 1, 'post-drop-add', 42, 'new');
                "#,
                schema = db.schema,
                table_name = table.table_name,
                relation = table.relation
            ))
            .await?;
        assert_eq!(db.flush_table(&table.relation).await?, 1);

        let after = db
            .client
            .query_one(
                r#"
                SELECT
                  (SELECT c->>'column_id'
                   FROM jsonb_array_elements(columns) AS c
                   WHERE c->>'name' = 'qty') AS qty_column_id,
                  (
                    SELECT count(*)
                    FROM koldstore.cold_segment_index st
                    JOIN koldstore.cold_segments cs
                      ON cs.segment_id = st.segment_id
                    WHERE cs.table_oid = s.table_oid
                      AND cs.status = 'active'
                      AND st.column_id = $2::smallint
                  ) AS old_id_stats_rows
                FROM koldstore.schemas s
                WHERE table_oid = $1::text::regclass::oid
                  AND active
                "#,
                &[&table.relation, &old_column_id.parse::<i16>()?],
            )
            .await?;
        let new_column_id: String = after.get(0);
        let old_id_stats_rows: i64 = after.get(1);
        assert_ne!(
            new_column_id, old_column_id,
            "drop+add must allocate a new column_id even when the name is reused"
        );
        assert!(
            old_id_stats_rows > 0,
            "old cold stats remain under the retired column_id and must not be reused as the new identity"
        );

        let rows = db
            .client
            .query(
                &format!(
                    "SELECT id, qty FROM {} WHERE id IN (1, 200) ORDER BY id",
                    table.relation
                ),
                &[],
            )
            .await?;
        assert_eq!(rows.len(), 2);
        // Pre-drop cold rows lack the new attnum; they materialize NULL rather than
        // reusing retired-column stats/identity.
        assert_eq!(rows[0].get::<_, Option<i32>>(1), None);
        assert_eq!(rows[1].get::<_, Option<i32>>(1), Some(42));
    }

    Ok(())
}

#[tokio::test]
async fn unsupported_alter_table_type_records_error_job_without_pruning_hot_rows() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "schema_evolution_reject").await?;
        let table = db.create_indexed_items_table("reject_items", 8).await?;
        db.manage_shared(&table.relation, "id").await?;

        db.client
            .batch_execute(&format!(
                "ALTER TABLE {} ADD COLUMN search tsvector",
                table.relation
            ))
            .await?;
        let flushed = db.flush_table(&table.relation).await?;
        assert_eq!(flushed, 0);

        let job = db
            .client
            .query_one(
                r#"
                SELECT status, phase, error_trace
                FROM koldstore.jobs
                WHERE table_oid = $1::text::regclass::oid
                  AND job_type = 'flush'
                ORDER BY updated_at DESC
                LIMIT 1
                "#,
                &[&table.relation],
            )
            .await?;
        assert_eq!(job.get::<_, String>(0), "error");
        assert_eq!(job.get::<_, String>(1), "failed");
        let error_trace = job.get::<_, Option<String>>(2).unwrap_or_default();
        assert!(
            error_trace.contains("unsupported PostgreSQL type: tsvector"),
            "unexpected error_trace: {error_trace}"
        );
        // Managed-table SELECT goes through merge scan, which re-introspects the
        // live catalog and cannot decode unsupported types. Prove hot rows were
        // not pruned via the change-log mirror and cold-segment catalog instead.
        let mirror = common::change_log_mirror_relation(&table.relation);
        assert_eq!(common::row_count(&db.client, &mirror).await?, 8);
        assert_eq!(
            common::cold_segment_count(&db.client, &table.relation).await?,
            0
        );
    }

    Ok(())
}

#[tokio::test]
async fn rename_primary_key_keeps_dml_and_cold_reads_working() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "schema_evolution_pk_rename").await?;
        let table = db.create_indexed_items_table("pk_rename_items", 8).await?;
        db.manage_shared(&table.relation, "id").await?;
        assert_eq!(db.flush_table(&table.relation).await?, 8);

        db.client
            .batch_execute(&format!(
                r#"
                ALTER TABLE {} RENAME COLUMN id TO item_id;
                INSERT INTO {} (item_id, account_id, title, qty, category)
                VALUES (100, 1, 'after-pk-rename', 10, 'new');
                "#,
                table.relation, table.relation
            ))
            .await?;
        common::fence_selected_mirror(&db.client).await?;
        assert_eq!(db.flush_table(&table.relation).await?, 1);

        let schema = db
            .client
            .query_one(
                r#"
                SELECT version,
                       (
                         SELECT c->>'name'
                         FROM jsonb_array_elements(primary_key) AS c
                         LIMIT 1
                       ) AS pk_name,
                       (
                         SELECT c->>'column_id'
                         FROM jsonb_array_elements(primary_key) AS c
                         LIMIT 1
                       ) AS pk_column_id
                FROM koldstore.schemas
                WHERE table_oid = $1::text::regclass::oid
                  AND active
                "#,
                &[&table.relation],
            )
            .await?;
        assert!(schema.get::<_, i32>(0) >= 2);
        assert_eq!(schema.get::<_, String>(1), "item_id");
        assert_eq!(schema.get::<_, String>(2), "1");

        let mirror = common::change_log_mirror_relation(&table.relation);
        let mirror_table = mirror.rsplit('.').next().unwrap_or(mirror.as_str());
        let mirror_has_new_pk: bool = db
            .client
            .query_one(
                "SELECT EXISTS (
                   SELECT 1 FROM information_schema.columns
                   WHERE table_schema = 'koldstore'
                     AND table_name = $1
                     AND column_name = 'item_id'
                 )",
                &[&mirror_table],
            )
            .await?
            .get(0);
        assert!(
            mirror_has_new_pk,
            "mirror PK column must rename with the source PK"
        );

        let rows = db
            .client
            .query(
                &format!(
                    "SELECT item_id, title FROM {} WHERE item_id IN (1, 100) ORDER BY item_id",
                    table.relation
                ),
                &[],
            )
            .await?;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get::<_, i64>(0), 1);
        assert_eq!(rows[1].get::<_, i64>(0), 100);
        assert_eq!(rows[1].get::<_, String>(1), "after-pk-rename");
    }

    Ok(())
}

#[tokio::test]
async fn rename_scope_column_keeps_rls_and_catalog_name_current() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "schema_evolution_scope_rename").await?;
        let table = db.create_user_notes_table("scope_rename_notes").await?;
        db.manage_user_scoped(&table.relation, "user_id").await?;

        db.client
            .batch_execute(&format!(
                r#"
                SET koldstore.user_id = 'user-a';
                ALTER TABLE {} RENAME COLUMN user_id TO owner_id;
                INSERT INTO {} (id, owner_id, title, body)
                VALUES (4, 'user-a', 'gamma', 'after-rename');
                "#,
                table.relation, table.relation
            ))
            .await?;
        common::fence_selected_mirror(&db.client).await?;

        let schema = db
            .client
            .query_one(
                r#"
                SELECT scope_column,
                       (options->>'scope_column_id')::smallint AS scope_column_id
                FROM koldstore.schemas
                WHERE table_oid = $1::text::regclass::oid
                  AND active
                "#,
                &[&table.relation],
            )
            .await?;
        assert_eq!(
            schema.get::<_, Option<String>>(0).as_deref(),
            Some("owner_id")
        );
        assert_eq!(schema.get::<_, i16>(1), 2);

        let policy = db
            .client
            .query_one(
                r#"
                SELECT qual
                FROM pg_policies
                WHERE schemaname = $1
                  AND tablename = $2
                  AND policyname = 'koldstore_user_scope_fail_closed'
                "#,
                &[&db.schema, &table.table_name],
            )
            .await?;
        let qual: String = policy.get(0);
        assert!(
            qual.contains("owner_id"),
            "RLS policy must reference renamed scope column, got {qual}"
        );

        let inserted: i64 = db
            .client
            .query_one(
                &format!("SELECT count(*) FROM {} WHERE id = 4", table.relation),
                &[],
            )
            .await?
            .get(0);
        assert_eq!(inserted, 1, "insert after scope rename must succeed");
    }

    Ok(())
}
