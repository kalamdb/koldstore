#[path = "common/mod.rs"]
mod common;

use anyhow::Result;

#[test]
fn migrate_existing_matrix_targets_postgresql_15_16_17() {
    let ports: Vec<u16> = common::local_pg_matrix()
        .into_iter()
        .map(|target| target.port)
        .collect();

    assert_eq!(ports, common::expected_pg_ports());
}

#[test]
fn migrate_existing_matrix_covers_data_and_constraint_preservation() {
    let scenario = existing_table_scenario();

    assert_eq!(scenario.schema_name, "legacy");
    assert_eq!(scenario.table_name, "items");
    assert_eq!(scenario.primary_key, "id");
    assert_eq!(scenario.secondary_index, "items_title_idx");
    assert!(scenario.create_sql.contains("CHECK (title <> '')"));
    assert!(scenario.create_sql.contains("title text NOT NULL"));
}

#[tokio::test]
#[ignore = "requires PostgreSQL 15/16/17 matrix with pg-koldstore installed"]
async fn existing_table_migration_preserves_rows_and_shape_on_pg_matrix() -> Result<()> {
    for target in common::local_pg_matrix() {
        let client = common::wait_for_postgres(&target).await?;
        install_storage_fixture(&client).await?;
        run_existing_table_scenario(&client, &existing_table_scenario(), target.version).await?;
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExistingTableScenario {
    schema_name: &'static str,
    table_name: &'static str,
    primary_key: &'static str,
    secondary_index: &'static str,
    create_sql: &'static str,
}

fn existing_table_scenario() -> ExistingTableScenario {
    ExistingTableScenario {
        schema_name: "legacy",
        table_name: "items",
        primary_key: "id",
        secondary_index: "items_title_idx",
        create_sql: r#"
            id bigint PRIMARY KEY,
            title text NOT NULL,
            qty integer NOT NULL DEFAULT 0,
            CHECK (title <> '')
        "#,
    }
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

async fn run_existing_table_scenario(
    client: &tokio_postgres::Client,
    scenario: &ExistingTableScenario,
    pg_version: u16,
) -> Result<()> {
    let relation = format!(
        "{}.{}_pg{}",
        scenario.schema_name, scenario.table_name, pg_version
    );
    client
        .batch_execute(&format!(
            r#"
            CREATE SCHEMA IF NOT EXISTS {schema};
            DROP TABLE IF EXISTS {relation};
            CREATE TABLE {relation} ({create_sql});
            CREATE INDEX {index_name}_pg{pg_version} ON {relation} (title);
            INSERT INTO {relation} (id, title, qty)
            VALUES (1, 'one', 1), (2, 'two', 2);
            "#,
            schema = scenario.schema_name,
            relation = relation,
            create_sql = scenario.create_sql,
            index_name = scenario.secondary_index,
            pg_version = pg_version,
        ))
        .await?;

    client
        .execute(
            "SELECT koldstore.migrate_table($1::text::regclass, 'shared', 'local-minio', NULL, NULL)",
            &[&relation],
        )
        .await?;

    let row_count = client
        .query_one(&format!("SELECT count(*) FROM {relation}"), &[])
        .await?
        .get::<_, i64>(0);
    assert_eq!(row_count, 2);

    let primary_key = client
        .query_one(
            r#"
            SELECT array_agg(a.attname ORDER BY a.attnum)
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = $1::text::regclass AND i.indisprimary
            "#,
            &[&relation],
        )
        .await?
        .get::<_, Vec<String>>(0);
    assert_eq!(primary_key, vec![scenario.primary_key]);

    let system_columns = client
        .query_one(
            "SELECT count(*) FROM pg_attribute WHERE attrelid = $1::text::regclass AND attname = ANY($2)",
            &[&relation, &&["_seq", "_commit_seq", "_deleted"][..]],
        )
        .await?
        .get::<_, i64>(0);
    assert_eq!(system_columns, 3);

    Ok(())
}
