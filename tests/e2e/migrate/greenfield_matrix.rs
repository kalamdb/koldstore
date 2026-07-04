#[path = "../common/mod.rs"]
mod common;

use anyhow::Result;

#[test]
fn greenfield_matrix_targets_postgresql_15_16_17() {
    let versions: Vec<u16> = common::local_pg_matrix()
        .into_iter()
        .map(|target| target.version)
        .collect();

    assert_eq!(versions, common::expected_pg_versions());
}

#[test]
fn greenfield_matrix_covers_shared_and_user_scoped_workflows() {
    let scenarios = greenfield_scenarios();

    assert_eq!(scenarios.len(), 2);
    assert!(scenarios
        .iter()
        .any(|scenario| scenario.table_type == "shared"));
    assert!(scenarios
        .iter()
        .any(|scenario| scenario.table_type == "user"));
    assert!(scenarios
        .iter()
        .any(|scenario| scenario.scope_column == Some("user_id")));
}

#[tokio::test]
async fn greenfield_shared_and_user_scoped_tables_work_on_pg_matrix() -> Result<()> {
    for target in common::local_pg_matrix() {
        let client = common::wait_for_postgres(&target).await?;
        install_storage_fixture(&client).await?;

        for scenario in greenfield_scenarios() {
            run_greenfield_scenario(&client, &scenario, target.version).await?;
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GreenfieldScenario {
    schema_name: &'static str,
    table_name: &'static str,
    table_type: &'static str,
    scope_column: Option<&'static str>,
}

fn greenfield_scenarios() -> [GreenfieldScenario; 2] {
    [
        GreenfieldScenario {
            schema_name: "app",
            table_name: "shared_items",
            table_type: "shared",
            scope_column: None,
        },
        GreenfieldScenario {
            schema_name: "app",
            table_name: "user_notes",
            table_type: "user",
            scope_column: Some("user_id"),
        },
    ]
}

async fn install_storage_fixture(client: &tokio_postgres::Client) -> Result<()> {
    client
        .batch_execute(
            r#"
            CREATE EXTENSION IF NOT EXISTS koldstore;

            SELECT koldstore.register_storage(
              'local-minio',
              's3',
              's3://koldstore-test/',
              '{"access_key_id":"minioadmin","secret_access_key":"minioadmin"}'::jsonb,
              '{"endpoint":"http://localhost:9000","region":"us-east-1","path_style":true}'::jsonb
            );
            "#,
        )
        .await?;
    Ok(())
}

async fn run_greenfield_scenario(
    client: &tokio_postgres::Client,
    scenario: &GreenfieldScenario,
    pg_version: u16,
) -> Result<()> {
    let relation = format!(
        "{}.{}_pg{}",
        scenario.schema_name, scenario.table_name, pg_version
    );
    let create_table = if scenario.table_type == "shared" {
        format!(
            r#"
            CREATE SCHEMA IF NOT EXISTS {schema};
            DROP TABLE IF EXISTS {relation};
            CREATE TABLE {relation} (
              id bigint PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
              title text NOT NULL,
              value integer
            );
            "#,
            schema = scenario.schema_name,
            relation = relation,
        )
    } else {
        format!(
            r#"
            CREATE SCHEMA IF NOT EXISTS {schema};
            DROP TABLE IF EXISTS {relation};
            CREATE TABLE {relation} (
              id bigint PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
              user_id text NOT NULL,
              content text NOT NULL
            );
            "#,
            schema = scenario.schema_name,
            relation = relation,
        )
    };

    client.batch_execute(&create_table).await?;

    client
        .execute(
            "SELECT koldstore.migrate_table($1::text::regclass, $2, 'local-minio', NULL, $3)",
            &[&relation, &scenario.table_type, &scenario.scope_column],
        )
        .await?;

    if let Some(scope_column) = scenario.scope_column {
        client
            .execute(
                &format!(
                    "INSERT INTO {relation} ({scope_column}, content) VALUES ('tenant-a', 'hello')"
                ),
                &[],
            )
            .await?;
    } else {
        client
            .execute(
                &format!("INSERT INTO {relation} (title, value) VALUES ('hello', 1)"),
                &[],
            )
            .await?;
    }

    let system_column_names: &[&str] = &["_seq", "_commit_seq", "_deleted"];
    let system_columns = client
        .query_one(
            "SELECT count(*) FROM pg_attribute WHERE attrelid = $1::text::regclass AND attname = ANY($2)",
            &[&relation, &system_column_names],
        )
        .await?
        .get::<_, i64>(0);
    assert_eq!(system_columns, 3);

    common::assertions::assert_no_duplicate_hot_pk(client, &relation, "id").await?;
    Ok(())
}
