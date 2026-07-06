use pg_koldstore::sql::session::{primary_key_default_clause, snowflake_default_expression};

#[test]
fn snowflake_default_expression_matches_public_sql_function() {
    assert_eq!(snowflake_default_expression(), "SNOWFLAKE_ID()");
}

#[test]
fn primary_key_default_clause_quotes_safe_greenfield_identifier() {
    assert_eq!(
        primary_key_default_clause("id").unwrap(),
        "\"id\" bigint PRIMARY KEY DEFAULT SNOWFLAKE_ID()"
    );
    assert_eq!(
        primary_key_default_clause("_item_id").unwrap(),
        "\"_item_id\" bigint PRIMARY KEY DEFAULT SNOWFLAKE_ID()"
    );
}

#[test]
fn primary_key_default_clause_rejects_unsafe_identifier() {
    assert!(primary_key_default_clause("").is_err());
    assert!(primary_key_default_clause("not safe").is_err());
    assert!(primary_key_default_clause("id; drop table app.items").is_err());
}
