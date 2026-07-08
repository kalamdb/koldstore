//! Clean-schema regression-test fixtures.

/// SQL for a single-column primary-key fixture table.
#[must_use]
pub fn single_pk_table_sql(relation: &str) -> String {
    format!(
        r#"
        CREATE TABLE {relation} (
          id bigint PRIMARY KEY,
          body text NOT NULL
        );
        "#
    )
}

/// SQL for a composite primary-key fixture table.
#[must_use]
pub fn composite_pk_table_sql(relation: &str) -> String {
    format!(
        r#"
        CREATE TABLE {relation} (
          tenant_id uuid NOT NULL,
          id bigint NOT NULL,
          body text NOT NULL,
          PRIMARY KEY (tenant_id, id)
        );
        "#
    )
}

/// SQL for a user-scoped fixture table with an application-owned scope column.
#[must_use]
pub fn user_scoped_table_sql(relation: &str, scope_column: &str) -> String {
    format!(
        r#"
        CREATE TABLE {relation} (
          id bigint PRIMARY KEY,
          {scope_column} text NOT NULL,
          body text NOT NULL
        );
        "#
    )
}
