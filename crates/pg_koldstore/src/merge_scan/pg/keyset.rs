//! Keyset predicates for paged hot merge SPI.
//!
//! Keeps paging SQL free of `OFFSET` so later pages stay index-friendly.
//! Primary-key columns are NOT NULL on managed tables, so row comparison is safe.

use koldstore_common::{escape_sql_literal, quote_ident, LogicalPk, PkColumn};
use serde_json::Value;

/// Builds `(alias."c1", …) > (lit1, …)` for the last emitted primary key.
///
/// Literals are typed only by PostgreSQL's row-comparison assignment casts from
/// the left-hand projected columns. Unsupported JSON shapes fail closed.
///
/// # Errors
///
/// Returns an error when the key shape mismatches, a PK value is null/complex,
/// or a column name is unsafe to quote.
pub(super) fn keyset_after_predicate(
    alias: &str,
    pk_columns: &[PkColumn],
    after: &LogicalPk,
) -> Result<String, String> {
    if pk_columns.is_empty() {
        return Err("keyset paging requires a primary key".to_string());
    }
    if after.columns().len() != pk_columns.len() {
        return Err(format!(
            "keyset PK arity mismatch: expected {}, got {}",
            pk_columns.len(),
            after.columns().len()
        ));
    }
    let mut left = Vec::with_capacity(pk_columns.len());
    let mut right = Vec::with_capacity(pk_columns.len());
    for (expected, (column, value)) in pk_columns.iter().zip(after.columns()) {
        if expected.as_str() != column.as_str() {
            return Err(format!(
                "keyset PK order mismatch: expected {}, got {}",
                expected.as_str(),
                column.as_str()
            ));
        }
        left.push(format!("{alias}.{}", quote_ident(column.as_str())));
        right.push(pk_json_value_sql_literal(value.as_json())?);
    }
    Ok(format!("({}) > ({})", left.join(", "), right.join(", ")))
}

/// Encodes one PK JSON cell as a SQL scalar literal for keyset comparison.
///
/// # Errors
///
/// Returns an error for null, array, or object values.
pub(super) fn pk_json_value_sql_literal(value: &Value) -> Result<String, String> {
    match value {
        Value::Null => Err("primary-key value for keyset paging cannot be null".to_string()),
        Value::Bool(flag) => Ok(if *flag {
            "TRUE".to_string()
        } else {
            "FALSE".to_string()
        }),
        Value::Number(number) => Ok(number.to_string()),
        Value::String(text) => Ok(format!("'{}'", escape_sql_literal(text))),
        Value::Array(_) | Value::Object(_) => Err(format!(
            "unsupported primary-key JSON for keyset paging: {value}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::{keyset_after_predicate, pk_json_value_sql_literal};
    use koldstore_common::{LogicalPk, PkColumn, PkValue};
    use serde_json::{json, Value};

    #[test]
    fn keyset_predicate_uses_row_comparison() {
        let columns = vec![
            PkColumn::new("tenant_id").unwrap(),
            PkColumn::new("id").unwrap(),
        ];
        let after = LogicalPk::new(vec![
            (
                PkColumn::new("tenant_id").unwrap(),
                PkValue::new(json!("a")).unwrap(),
            ),
            (
                PkColumn::new("id").unwrap(),
                PkValue::new(json!(10)).unwrap(),
            ),
        ])
        .unwrap();

        let predicate = keyset_after_predicate("proj", &columns, &after).unwrap();
        assert_eq!(predicate, "(proj.\"tenant_id\", proj.\"id\") > ('a', 10)");
    }

    #[test]
    fn keyset_literal_escapes_quotes() {
        assert_eq!(
            pk_json_value_sql_literal(&json!("o'brien")).unwrap(),
            "'o''brien'"
        );
    }

    #[test]
    fn keyset_rejects_null_pk_values() {
        assert!(pk_json_value_sql_literal(&Value::Null).is_err());
    }
}
