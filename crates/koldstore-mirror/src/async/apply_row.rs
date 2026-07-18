//! Pure helpers that map decoded `pgoutput` tuples into mirror batch row JSON.
//!
//! SPI execution and managed-relation lookup stay in `pg_koldstore`.

use serde_json::{Map, Value};

use super::pgoutput::{PgOutputRelation, PgOutputTuple, PgOutputValue};

/// Compact PK identity for in-batch dedupe (ordered values, NUL-separated).
#[must_use]
pub fn pk_identity(row: &Map<String, Value>) -> String {
    let mut identity = String::new();
    for (key, value) in row {
        if key == "seq" {
            continue;
        }
        if !identity.is_empty() {
            identity.push('\0');
        }
        match value {
            Value::String(text) => identity.push_str(text),
            other => identity.push_str(&other.to_string()),
        }
    }
    identity
}

/// Builds a primary-key JSON object from a decoded `pgoutput` tuple.
///
/// Uses linear column lookup (typical PK width is tiny) to avoid per-row
/// `HashMap` allocation on the apply hot path.
///
/// # Errors
///
/// Returns an error when a managed primary-key column is missing from the
/// relation, omitted from the tuple, NULL, or emitted as unchanged TOAST.
pub fn primary_key_json(
    relation: &PgOutputRelation,
    primary_key: &[String],
    tuple: &PgOutputTuple,
) -> Result<Map<String, Value>, String> {
    let mut key_columns = Vec::with_capacity(primary_key.len());
    for key in primary_key {
        let relation_index = relation
            .columns
            .iter()
            .position(|column| column.name == *key)
            .ok_or_else(|| {
                format!(
                    "pgoutput relation {}.{} does not publish managed primary-key column {key}",
                    relation.namespace, relation.name
                )
            })?;
        key_columns.push(relation_index);
    }
    let compact_old_key =
        tuple.values.len() == key_columns.len() && tuple.values.len() != relation.columns.len();
    let mut row = Map::with_capacity(primary_key.len());
    for (key_position, key) in primary_key.iter().enumerate() {
        let relation_index = key_columns[key_position];
        let tuple_index = if compact_old_key {
            key_position
        } else {
            relation_index
        };
        let value = tuple
            .values
            .get(tuple_index)
            .ok_or_else(|| format!("tuple omits primary-key column {key}"))?;
        row.insert(key.clone(), pg_value_json(value, key)?);
    }
    Ok(row)
}

/// Converts one `pgoutput` value into a JSON cell for mirror batch apply.
///
/// # Errors
///
/// Returns an error for NULL, unchanged TOAST, binary, or non-UTF8 text.
pub fn pg_value_json(value: &PgOutputValue, column: &str) -> Result<Value, String> {
    match value {
        PgOutputValue::Null => Err(format!("primary-key column {column} is NULL")),
        PgOutputValue::UnchangedToast => Err(format!(
            "primary-key column {column} was emitted as unchanged TOAST"
        )),
        PgOutputValue::Text(bytes) => std::str::from_utf8(bytes)
            .map(|text| Value::String(text.to_string()))
            .map_err(|error| error.to_string()),
        PgOutputValue::Binary(_) => Err("binary pgoutput values are not requested".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::{pk_identity, primary_key_json};
    use crate::r#async::pgoutput::{
        PgOutputColumn, PgOutputRelation, PgOutputTuple, PgOutputValue,
    };
    use serde_json::{json, Map};

    #[test]
    fn pk_identity_skips_seq_and_joins_values() {
        let mut row = Map::new();
        row.insert("id".into(), json!("a"));
        row.insert("seq".into(), json!(9));
        row.insert("tenant".into(), json!("t1"));
        assert_eq!(pk_identity(&row), "a\0t1");
    }

    #[test]
    fn primary_key_json_reads_compact_old_tuple() {
        let relation = PgOutputRelation {
            id: 1,
            namespace: "public".into(),
            name: "items".into(),
            replica_identity: b'd',
            columns: vec![
                PgOutputColumn {
                    key: true,
                    name: "id".into(),
                    type_oid: 20,
                    typmod: -1,
                },
                PgOutputColumn {
                    key: false,
                    name: "body".into(),
                    type_oid: 25,
                    typmod: -1,
                },
            ],
        };
        let tuple = PgOutputTuple {
            values: vec![PgOutputValue::Text(b"42".to_vec())],
        };
        let row = primary_key_json(&relation, &["id".into()], &tuple).unwrap();
        assert_eq!(row.get("id"), Some(&json!("42")));
    }
}
