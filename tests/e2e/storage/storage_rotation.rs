use crate::common;

use anyhow::Result;

#[test]
fn storage_rotation_contract_keeps_existing_object_paths_stable() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    let registration = koldstore_storage::registration::StorageRegistration {
        name: "local-minio".to_string(),
        storage_type: "s3".to_string(),
        base_path: "s3://koldstore-test/".to_string(),
        credentials: serde_json::json!({"access_key_id": "old"}),
        config: serde_json::json!({"endpoint": "http://localhost:9000"}),
        shared_path_template: "{namespace}/{tableName}/".to_string(),
        user_path_template: "{namespace}/{tableName}/{scopeId}/".to_string(),
    };
    let old_path = registration.render_shared_prefix("app", "items").unwrap();
    let rotation = koldstore_storage::registration::alter_storage_credentials_plan(
        "local-minio",
        serde_json::json!({"access_key_id": "new"}),
    )
    .unwrap();

    assert_eq!(old_path, "app/items/");
    assert_eq!(rotation.storage_name, "local-minio");
    assert!(rotation.statement.sql.contains("SET credentials = $2"));
    assert!(!rotation.statement.sql.contains("base_path"));
}

#[tokio::test]
async fn storage_rotation_and_session_functions_work_on_pg_matrix() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let server = common::PgrxServer::start(target).await?;
        server
            .client
            .batch_execute("CREATE EXTENSION IF NOT EXISTS koldstore;")
            .await?;

        assert_session_functions(&server.client).await?;
        assert_storage_rotation(&server.client, server.target.version).await?;
    }

    Ok(())
}

async fn assert_session_functions(client: &tokio_postgres::Client) -> Result<()> {
    let row = client
        .query_one(
            "SELECT koldstore_version(), snowflake_id(), snowflake_id()",
            &[],
        )
        .await?;

    let version = row.get::<_, String>(0);
    let first_id = row.get::<_, i64>(1);
    let second_id = row.get::<_, i64>(2);

    assert!(!version.is_empty());
    assert!(first_id > 0);
    assert!(second_id > first_id);
    Ok(())
}

async fn assert_storage_rotation(client: &tokio_postgres::Client, pg_version: u16) -> Result<()> {
    let storage_name = format!("rotation-local-pg{pg_version}");
    let base_path = std::env::temp_dir()
        .join(format!("pg-koldstore-rotation-{pg_version}"))
        .to_string_lossy()
        .into_owned();

    client
        .execute(
            r#"
            SELECT koldstore.register_storage(
              $1,
              'filesystem',
              $2,
              '{"token":"old"}'::jsonb,
              '{"region":"local"}'::jsonb
            )
            "#,
            &[&storage_name, &base_path],
        )
        .await?;

    let before = storage_record(client, &storage_name).await?;
    assert_eq!(before.base_path, base_path);
    assert_eq!(before.credentials, serde_json::json!({"token": "old"}));
    assert_eq!(before.config, serde_json::json!({"region": "local"}));

    client
        .execute(
            r#"
            SELECT koldstore.alter_storage_credentials(
              $1,
              '{"token":"new","rotated":true}'::jsonb
            )
            "#,
            &[&storage_name],
        )
        .await?;

    let after = storage_record(client, &storage_name).await?;
    assert_eq!(after.base_path, before.base_path);
    assert_eq!(after.config, before.config);
    assert_eq!(after.shared_path_template, before.shared_path_template);
    assert_eq!(after.user_path_template, before.user_path_template);
    assert_eq!(
        after.credentials,
        serde_json::json!({"token": "new", "rotated": true})
    );
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
struct StorageRecord {
    base_path: String,
    credentials: serde_json::Value,
    config: serde_json::Value,
    shared_path_template: String,
    user_path_template: String,
}

async fn storage_record(
    client: &tokio_postgres::Client,
    storage_name: &str,
) -> Result<StorageRecord> {
    let row = client
        .query_one(
            r#"
            SELECT
              base_path,
              credentials::text,
              config::text,
              shared_path_template,
              user_path_template
            FROM koldstore.storage
            WHERE name = $1
            "#,
            &[&storage_name],
        )
        .await?;

    Ok(StorageRecord {
        base_path: row.get(0),
        credentials: serde_json::from_str(&row.get::<_, String>(1))?,
        config: serde_json::from_str(&row.get::<_, String>(2))?,
        shared_path_template: row.get(3),
        user_path_template: row.get(4),
    })
}
