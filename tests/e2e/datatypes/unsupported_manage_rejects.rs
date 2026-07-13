//! E2E: manage_table rejects unsupported column types and flush-unsafe constraints.

#[path = "../common/mod.rs"]
mod common;

use anyhow::Result;

fn db_error_message(error: &tokio_postgres::Error) -> String {
    error
        .as_db_error()
        .map_or_else(|| error.to_string(), |db| db.message().to_string())
}

#[tokio::test]
async fn manage_table_rejects_unsupported_column_types() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "unsupported_manage_rejects").await?;

        for (label, ddl_type) in [
            ("bytea_col", "bytea"),
            ("numeric_col", "numeric"),
            ("inet_col", "inet"),
        ] {
            let relation = format!("{}.reject_{label}", db.schema);
            db.client
                .batch_execute(&format!(
                    r#"
                    DROP TABLE IF EXISTS {relation};
                    CREATE TABLE {relation} (
                      id bigint PRIMARY KEY,
                      {label} {ddl_type}
                    );
                    "#
                ))
                .await?;

            let err = db
                .client
                .execute(
                    r#"
                    SELECT koldstore.manage_table(
                      table_name     => $1::text::regclass,
                      storage        => $2,
                      hot_row_limit  => NULL,
                      migration_order_by => 'id'
                    )
                    "#,
                    &[&relation, &db.storage_name],
                )
                .await
                .expect_err(&format!("manage_table must reject {ddl_type}"));
            let message = db_error_message(&err).to_lowercase();
            assert!(
                message.contains("unsupported") || message.contains(ddl_type),
                "unexpected reject message for {ddl_type}: {message}"
            );
        }
    }

    Ok(())
}

#[tokio::test]
async fn manage_table_rejects_non_pk_unique_when_flush_enabled() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "unique_constraint_reject").await?;
        let relation = format!("{}.reject_unique", db.schema);

        db.client
            .batch_execute(&format!(
                r#"
                CREATE TABLE {relation} (
                  id bigint PRIMARY KEY,
                  email text NOT NULL UNIQUE,
                  body text NOT NULL
                );
                "#
            ))
            .await?;

        let err = db
            .client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name     => $1::text::regclass,
                  storage        => $2,
                  hot_row_limit  => 1000,
                  min_flush_rows => 1,
                  migration_order_by => 'id'
                )
                "#,
                &[&relation, &db.storage_name],
            )
            .await
            .expect_err("manage_table must reject non-PK UNIQUE when hot_row_limit is set");
        let message = db_error_message(&err).to_lowercase();
        assert!(
            message.contains("unique") || message.contains("constraint"),
            "unexpected reject message: {message}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn manage_table_rejects_invalid_compression() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "bad_compression_reject").await?;
        let table = db
            .create_indexed_items_table("bad_compression_items", 0)
            .await?;

        let err = db
            .client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name     => $1::text::regclass,
                  storage        => $2,
                  hot_row_limit  => NULL,
                  compression    => 'brotli',
                  migration_order_by => 'id'
                )
                "#,
                &[&table.relation, &db.storage_name],
            )
            .await
            .expect_err("manage_table must reject unsupported compression");
        let message = db_error_message(&err).to_lowercase();
        assert!(
            message.contains("compression") || message.contains("brotli"),
            "unexpected reject message: {message}"
        );
    }

    Ok(())
}
