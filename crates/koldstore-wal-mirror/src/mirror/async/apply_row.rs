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

/// Converts one `pgoutput` value into a UTF-8 text cell.
///
/// # Errors
///
/// Returns an error for NULL, unchanged TOAST, binary, or non-UTF8 text.
pub fn pg_value_text(value: &PgOutputValue, column: &str, role: &str) -> Result<String, String> {
    match value {
        PgOutputValue::Null => Err(format!("{role} column {column} is NULL")),
        PgOutputValue::UnchangedToast => Err(format!(
            "{role} column {column} was emitted as unchanged TOAST"
        )),
        PgOutputValue::Text(bytes) => std::str::from_utf8(bytes)
            .map(str::to_string)
            .map_err(|error| error.to_string()),
        PgOutputValue::Binary(_) => {
            Err(format!("{role} column {column} arrived as binary pgoutput"))
        }
    }
}

/// Converts one `pgoutput` value into a JSON cell for mirror batch apply.
///
/// # Errors
///
/// Returns an error for NULL, unchanged TOAST, binary, or non-UTF8 text.
pub fn pg_value_json(value: &PgOutputValue, column: &str) -> Result<Value, String> {
    Ok(Value::String(pg_value_text(value, column, "primary-key")?))
}

/// Parses primary-key text cells into typed integers for SPI array binds.
///
/// # Errors
///
/// Returns an error when any cell fails to parse as `T`.
pub fn parse_pk_ints<T>(cells: &[String], type_name: &str) -> Result<Vec<T>, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    cells
        .iter()
        .map(|cell| {
            cell.parse::<T>()
                .map_err(|error| format!("async mirror PK {type_name} value `{cell}`: {error}"))
        })
        .collect()
}

/// Parses one PostgreSQL boolean text form used by pgoutput / SPI binds.
///
/// # Errors
///
/// Returns an error when the cell is not a recognized boolean literal.
pub fn parse_pk_bool(cell: &str) -> Result<bool, String> {
    match cell.trim().to_ascii_lowercase().as_str() {
        "t" | "true" | "1" | "yes" | "on" => Ok(true),
        "f" | "false" | "0" | "no" | "off" => Ok(false),
        other => Err(format!("async mirror PK boolean value `{other}`")),
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_pk_bool, parse_pk_ints, pk_identity, primary_key_json};
    use crate::mirror::r#async::pgoutput::{
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

    #[test]
    fn parse_pk_bool_accepts_pg_forms() {
        assert_eq!(parse_pk_bool("t").unwrap(), true);
        assert_eq!(parse_pk_bool("FALSE").unwrap(), false);
        assert!(parse_pk_bool("maybe").is_err());
        assert_eq!(
            parse_pk_ints::<i32>(&["1".into(), "2".into()], "int4").unwrap(),
            vec![1, 2]
        );
    }
}
