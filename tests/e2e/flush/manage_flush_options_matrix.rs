//! E2E: manage_table / flush / GUC / session option coverage.

#[path = "../common/mod.rs"]
mod common;

use anyhow::Result;
use parquet::basic::Compression;
use parquet::file::reader::{FileReader, SerializedFileReader};

#[tokio::test]
async fn manage_flush_and_session_options_matrix() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "manage_flush_options").await?;

        // Session helpers.
        let version: String = db
            .client
            .query_one("SELECT koldstore_version()", &[])
            .await?
            .get(0);
        assert!(!version.is_empty());

        // Load the extension .so before setting the registered user_id GUC.
        db.client
            .batch_execute("SET koldstore.user_id = 'tenant-options'")
            .await?;
        let user_id: Option<String> = db
            .client
            .query_one("SELECT koldstore_user_id()", &[])
            .await?
            .get(0);
        assert_eq!(user_id.as_deref(), Some("tenant-options"));
        db.client.batch_execute("RESET koldstore.user_id").await?;
        let cleared: Option<String> = db
            .client
            .query_one("SELECT koldstore_user_id()", &[])
            .await?
            .get(0);
        assert!(
            cleared.is_none(),
            "reset user_id should clear koldstore_user_id()"
        );

        // Compression matrix: snappy + zstd both produce readable cold files.
        for compression in ["snappy", "zstd"] {
            let relation = format!("{}.opts_{compression}", db.schema);
            db.client
                .batch_execute(&format!(
                    r#"
                    DROP TABLE IF EXISTS {relation};
                    CREATE TABLE {relation} (
                      id bigint PRIMARY KEY,
                      body text NOT NULL
                    );
                    INSERT INTO {relation} (id, body)
                    SELECT gs, 'body-' || gs::text
                    FROM generate_series(1, 40) AS gs;
                    "#
                ))
                .await?;

            db.client
                .execute(
                    r#"
                    SELECT koldstore.manage_table(
                      table_name          => $1::text::regclass,
                      storage             => $2,
                      hot_row_limit       => 10,
                      min_flush_rows      => 1,
                      max_rows_per_file   => 1000,
                      target_file_size_mb => 256,
                      compression         => $3,
                      migration_order_by  => 'id'
                    )
                    "#,
                    &[&relation, &db.storage_name, &compression],
                )
                .await?;

            let flushed = db.flush_table(&relation).await?;
            assert!(flushed > 0, "{compression}: expected flush rows");

            let parquet_rel: String = db
                .client
                .query_one(
                    r#"
                    SELECT cs.object_path
                    FROM koldstore.segments cs
                    WHERE cs.table_oid = $1::text::regclass::oid
                      AND cs.status = 'published'
                    ORDER BY cs.batch_number
                    LIMIT 1
                    "#,
                    &[&relation],
                )
                .await?
                .get(0);
            let parquet_path = db.storage_root.join(&parquet_rel);
            let file = std::fs::File::open(&parquet_path)?;
            let reader = SerializedFileReader::new(file)?;
            let codec = reader.metadata().row_group(0).column(0).compression();
            match compression {
                "snappy" => assert_eq!(codec, Compression::SNAPPY),
                "zstd" => assert!(matches!(codec, Compression::ZSTD(_))),
                other => panic!("unexpected compression fixture {other}"),
            }

            let count: i64 = db
                .client
                .query_one(&format!("SELECT count(*)::bigint FROM {relation}"), &[])
                .await?
                .get(0);
            assert_eq!(count, 40);

            // cold_reads=on must still return correct counts.
            db.client
                .batch_execute("SET koldstore.cold_reads = 'on'")
                .await?;
            let count_on: i64 = db
                .client
                .query_one(&format!("SELECT count(*)::bigint FROM {relation}"), &[])
                .await?
                .get(0);
            assert_eq!(count_on, 40);
            db.client
                .batch_execute("SET koldstore.cold_reads = 'auto'")
                .await?;
        }

        // Custom storage path templates land objects under the configured prefix.
        let custom_name = format!("{}-custom-paths", db.storage_name);
        let custom_root = db.storage_root.join("custom-template-root");
        std::fs::create_dir_all(&custom_root)?;
        let custom_root_path = custom_root.to_string_lossy().into_owned();
        db.client
            .execute(
                r#"
                SELECT koldstore.register_storage(
                  name                 => $1,
                  storage_type         => 'filesystem',
                  base_path            => $2,
                  credentials          => '{}'::jsonb,
                  config               => '{}'::jsonb,
                  shared_path_template => 'shared/{namespace}/{tableName}/',
                  user_path_template   => 'user/{namespace}/{tableName}/{scopeId}/'
                )
                "#,
                &[&custom_name, &custom_root_path],
            )
            .await?;

        let relation = format!("{}.template_paths", db.schema);
        db.client
            .batch_execute(&format!(
                r#"
                CREATE TABLE {relation} (
                  id bigint PRIMARY KEY,
                  body text NOT NULL
                );
                INSERT INTO {relation} (id, body) VALUES (1, 'a'), (2, 'b'), (3, 'c');
                "#
            ))
            .await?;
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name     => $1::text::regclass,
                  storage        => $2,
                  hot_row_limit  => NULL,
                  migration_order_by => 'id'
                )
                "#,
                &[&relation, &custom_name],
            )
            .await?;
        let flushed = db.flush_table(&relation).await?;
        assert_eq!(flushed, 3);

        let object_path: String = db
            .client
            .query_one(
                r#"
                SELECT cs.object_path
                FROM koldstore.segments cs
                WHERE cs.table_oid = $1::text::regclass::oid
                  AND cs.status = 'published'
                LIMIT 1
                "#,
                &[&relation],
            )
            .await?
            .get(0);
        assert!(
            !object_path.is_empty(),
            "expected a published segment object path"
        );
        assert!(
            custom_root.join(&object_path).exists(),
            "missing object under custom root: {}",
            custom_root.join(&object_path).display()
        );

        let templates = db
            .client
            .query_one(
                r#"
                SELECT shared_path_template, user_path_template
                FROM koldstore.storage
                WHERE name = $1
                "#,
                &[&custom_name],
            )
            .await?;
        assert_eq!(
            templates.get::<_, String>(0),
            "shared/{namespace}/{tableName}/"
        );
        assert_eq!(
            templates.get::<_, String>(1),
            "user/{namespace}/{tableName}/{scopeId}/"
        );

        // enqueue_flush_job + recover_segments dry-run stay callable after flush.
        let enqueued: i64 = db
            .client
            .query_one(
                "SELECT koldstore.enqueue_flush_job(table_name => $1::text::regclass, force => true)",
                &[&relation],
            )
            .await?
            .get(0);
        assert!(enqueued >= 0);
        let recovered: i64 = db
            .client
            .query_one(
                "SELECT koldstore.recover_segments(table_name => $1::text::regclass, dry_run => true)",
                &[&relation],
            )
            .await?
            .get(0);
        assert!(recovered >= 0);
    }

    Ok(())
}
