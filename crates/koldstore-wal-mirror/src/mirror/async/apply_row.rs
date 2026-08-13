//! Pure helpers that map decoded `pgoutput` tuples into typed bind columns.
//!
//! Apply batches stay columnar (native int/bool arrays or text, `seq`, optional
//! `order_key` bytes). JSON is not part of this path. In-batch identity is a
//! typed [`PkIdentity`]: a single builtin int/bool is an inline key, so the
//! common `bigint` PK path never builds a String or hashes heap bytes. SPI
//! execution and managed-relation lookup stay in `pg_koldstore`.

use super::pgoutput::{PgOutputRelation, PgOutputTuple, PgOutputValue};

/// PostgreSQL `bool` type OID.
pub const BOOLOID: u32 = 16;
/// PostgreSQL `int2` type OID.
pub const INT2OID: u32 = 21;
/// PostgreSQL `int4` type OID.
pub const INT4OID: u32 = 23;
/// PostgreSQL `int8` type OID.
pub const INT8OID: u32 = 20;

/// One primary-key column's values ready for a typed SPI `unnest` bind.
///
/// Builtin int/bool OIDs parse once here. Domains, uuid, and other scalars stay
/// text so SQL can still cast (`pk_array_bind_kind` TextThenCast).
#[derive(Debug, Clone, PartialEq)]
pub enum PkBindColumn {
    /// `bigint` / `int8`.
    Int8(Vec<i64>),
    /// `integer` / `int4`.
    Int4(Vec<i32>),
    /// `smallint` / `int2`.
    Int2(Vec<i16>),
    /// `boolean`.
    Bool(Vec<bool>),
    /// Text-format cells, including types bound as `text[]` then cast.
    Text(Vec<String>),
}

impl PkBindColumn {
    /// Allocates an empty column batch for `type_oid`.
    #[must_use]
    pub fn with_capacity(type_oid: u32, cap: usize) -> Self {
        match type_oid {
            INT8OID => Self::Int8(Vec::with_capacity(cap)),
            INT4OID => Self::Int4(Vec::with_capacity(cap)),
            INT2OID => Self::Int2(Vec::with_capacity(cap)),
            BOOLOID => Self::Bool(Vec::with_capacity(cap)),
            _ => Self::Text(Vec::with_capacity(cap)),
        }
    }

    /// Appends one already-parsed PK cell. Builtin ints/bools are not reparsed.
    ///
    /// # Errors
    ///
    /// Returns an error when `cell` does not match this column's bind type.
    pub fn push_cell(&mut self, cell: PkCell, column: &str) -> Result<(), String> {
        match (&mut *self, cell) {
            (Self::Int8(values), PkCell::Int8(value)) => values.push(value),
            (Self::Int4(values), PkCell::Int4(value)) => values.push(value),
            (Self::Int2(values), PkCell::Int2(value)) => values.push(value),
            (Self::Bool(values), PkCell::Bool(value)) => values.push(value),
            (Self::Text(values), PkCell::Text(value)) => values.push(value),
            (expected, got) => {
                return Err(format!(
                    "async mirror PK bind mismatch for {column}: expected {}, got {}",
                    bind_kind(expected),
                    cell_kind(&got)
                ));
            }
        }
        Ok(())
    }

    /// Appends one pgoutput text cell, parsing native scalars immediately.
    ///
    /// # Errors
    ///
    /// Returns an error when the cell cannot be parsed as the column's type.
    pub fn push_text(&mut self, cell: String, column: &str) -> Result<(), String> {
        let type_oid = match self {
            Self::Int8(_) => INT8OID,
            Self::Int4(_) => INT4OID,
            Self::Int2(_) => INT2OID,
            Self::Bool(_) => BOOLOID,
            Self::Text(_) => 0,
        };
        self.push_cell(PkCell::from_pg_text(type_oid, cell, column)?, column)
    }

    /// Number of staged values.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::Int8(values) => values.len(),
            Self::Int4(values) => values.len(),
            Self::Int2(values) => values.len(),
            Self::Bool(values) => values.len(),
            Self::Text(values) => values.len(),
        }
    }

    /// Returns true when no values are staged.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// One parsed primary-key scalar taken from a pgoutput tuple.
///
/// Builtin int/bool OIDs parse at extract. Everything else stays owned text.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PkCell {
    /// `bigint` / `int8`.
    Int8(i64),
    /// `integer` / `int4`.
    Int4(i32),
    /// `smallint` / `int2`.
    Int2(i16),
    /// `boolean`.
    Bool(bool),
    /// Text-format cell, including uuid/domain/other scalars.
    Text(String),
}

impl PkCell {
    /// Parses one pgoutput text cell using the column's type OID.
    ///
    /// # Errors
    ///
    /// Returns an error when a builtin int/bool cell cannot be parsed.
    pub fn from_pg_text(type_oid: u32, text: String, column: &str) -> Result<Self, String> {
        match type_oid {
            INT8OID => Ok(Self::Int8(text.parse::<i64>().map_err(|error| {
                format!("async mirror PK int8 value `{text}` for {column}: {error}")
            })?)),
            INT4OID => Ok(Self::Int4(text.parse::<i32>().map_err(|error| {
                format!("async mirror PK int4 value `{text}` for {column}: {error}")
            })?)),
            INT2OID => Ok(Self::Int2(text.parse::<i16>().map_err(|error| {
                format!("async mirror PK int2 value `{text}` for {column}: {error}")
            })?)),
            BOOLOID => Ok(Self::Bool(parse_pk_bool(&text)?)),
            _ => Ok(Self::Text(text)),
        }
    }
}

/// In-batch PK identity. Single builtin scalars are inline; text/composite heap.
///
/// Callers must pass only primary-key cells. `seq` and `order_key` are not part
/// of identity: latest-state batches flush on PK collision, not on payload.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PkIdentity {
    /// Single `bigint` key. The common managed-table path; no heap.
    Int8(i64),
    /// Single `integer` key.
    Int4(i32),
    /// Single `smallint` key.
    Int2(i16),
    /// Single `boolean` key.
    Bool(bool),
    /// Single non-builtin scalar (uuid, domain, text, …).
    Text(Box<str>),
    /// Composite primary key. Boxed so HashSet keys stay small.
    Composite(Box<[PkCell]>),
}

impl PkIdentity {
    /// Builds identity from catalog-ordered PK cells.
    ///
    /// A single builtin int/bool is stored inline. A single text cell is one
    /// `Box<str>`. Two or more cells are a boxed slice so HashSet keys stay small.
    #[must_use]
    pub fn from_cells(cells: &[PkCell]) -> Self {
        match cells {
            [PkCell::Int8(value)] => Self::Int8(*value),
            [PkCell::Int4(value)] => Self::Int4(*value),
            [PkCell::Int2(value)] => Self::Int2(*value),
            [PkCell::Bool(value)] => Self::Bool(*value),
            [PkCell::Text(value)] => Self::Text(value.clone().into_boxed_str()),
            other => Self::Composite(other.to_vec().into_boxed_slice()),
        }
    }
}

/// Compact PK identity for in-batch dedupe.
///
/// Equivalent to [`PkIdentity::from_cells`].
#[must_use]
pub fn pk_identity(pk_cells: &[PkCell]) -> PkIdentity {
    PkIdentity::from_cells(pk_cells)
}

fn bind_kind(column: &PkBindColumn) -> &'static str {
    match column {
        PkBindColumn::Int8(_) => "int8",
        PkBindColumn::Int4(_) => "int4",
        PkBindColumn::Int2(_) => "int2",
        PkBindColumn::Bool(_) => "bool",
        PkBindColumn::Text(_) => "text",
    }
}

fn cell_kind(cell: &PkCell) -> &'static str {
    match cell {
        PkCell::Int8(_) => "int8",
        PkCell::Int4(_) => "int4",
        PkCell::Int2(_) => "int2",
        PkCell::Bool(_) => "bool",
        PkCell::Text(_) => "text",
    }
}

/// Resolves managed PK names to relation-tuple indexes.
///
/// # Errors
///
/// Returns an error when a managed primary-key column is missing from the
/// relation.
pub fn pk_column_indexes(
    relation: &PgOutputRelation,
    primary_key: &[String],
) -> Result<Vec<usize>, String> {
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
    Ok(key_columns)
}

/// Type OIDs for `key_columns` in catalog PK order.
///
/// # Errors
///
/// Returns an error when an index is out of range for the relation.
pub fn pk_type_oids(
    relation: &PgOutputRelation,
    key_columns: &[usize],
) -> Result<Vec<u32>, String> {
    key_columns
        .iter()
        .map(|&index| {
            relation
                .columns
                .get(index)
                .map(|column| column.type_oid)
                .ok_or_else(|| format!("primary-key column index {index} is out of range"))
        })
        .collect()
}

/// Extracts ordered primary-key cells from a decoded `pgoutput` tuple.
///
/// Compact old-key tuples (PK columns only) are read in catalog order.
/// Builtin int/bool cells parse here; the tuple text is dropped.
///
/// # Errors
///
/// Returns an error when a managed primary-key column is omitted from the
/// tuple, NULL, emitted as unchanged TOAST, or fails native parse.
pub fn primary_key_cells(
    relation: &PgOutputRelation,
    primary_key: &[String],
    key_columns: &[usize],
    type_oids: &[u32],
    tuple: &mut PgOutputTuple,
) -> Result<Vec<PkCell>, String> {
    if key_columns.len() != primary_key.len() || type_oids.len() != primary_key.len() {
        return Err("primary-key index count does not match column names".to_string());
    }
    let compact_old_key =
        tuple.values.len() == key_columns.len() && tuple.values.len() != relation.columns.len();
    let mut cells = Vec::with_capacity(primary_key.len());
    for (key_position, key) in primary_key.iter().enumerate() {
        let relation_index = key_columns[key_position];
        let tuple_index = if compact_old_key {
            key_position
        } else {
            relation_index
        };
        let value = tuple
            .values
            .get_mut(tuple_index)
            .ok_or_else(|| format!("tuple omits primary-key column {key}"))?;
        let text = take_pg_value_text(value, key, "primary-key")?;
        cells.push(PkCell::from_pg_text(type_oids[key_position], text, key)?);
    }
    Ok(cells)
}

/// Takes PK cells and, when requested, the segment-order column's pgoutput text.
///
/// The order column is peeked **before** PK cells are taken. Taking replaces PK
/// tuple slots with NULL, and `migration_order_by` is often the PK itself (`id`).
///
/// # Errors
///
/// Returns an error when PK extract fails or the order column is missing, NULL,
/// unchanged TOAST, or binary.
pub fn take_pk_cells_and_order_text(
    relation: &PgOutputRelation,
    primary_key: &[String],
    key_columns: &[usize],
    type_oids: &[u32],
    order_column: Option<&str>,
    tuple: &mut PgOutputTuple,
) -> Result<(Vec<PkCell>, Option<String>), String> {
    let order_text = match order_column {
        Some(name) => Some(order_column_text(relation, name, tuple)?),
        None => None,
    };
    let cells = primary_key_cells(relation, primary_key, key_columns, type_oids, tuple)?;
    Ok((cells, order_text))
}

/// Reads one published column as pgoutput text without taking it.
///
/// # Errors
///
/// Returns an error when the column is unpublished, omitted, NULL, unchanged
/// TOAST, or binary.
pub fn order_column_text(
    relation: &PgOutputRelation,
    column: &str,
    tuple: &PgOutputTuple,
) -> Result<String, String> {
    let relation_index = relation
        .columns
        .iter()
        .position(|candidate| candidate.name == column)
        .ok_or_else(|| {
            format!(
                "pgoutput relation {}.{} does not publish segment order column {column}",
                relation.namespace, relation.name
            )
        })?;
    let value = tuple
        .values
        .get(relation_index)
        .ok_or_else(|| format!("tuple omits segment order column {column}"))?;
    pg_value_text(value, column, "segment order")
}

/// Converts one `pgoutput` value into a UTF-8 text cell without taking it.
///
/// # Errors
///
/// Returns an error for NULL, unchanged TOAST, or binary.
pub fn pg_value_text(value: &PgOutputValue, column: &str, role: &str) -> Result<String, String> {
    match value {
        PgOutputValue::Null => Err(format!("{role} column {column} is NULL")),
        PgOutputValue::UnchangedToast => Err(format!(
            "{role} column {column} was emitted as unchanged TOAST"
        )),
        PgOutputValue::Text(text) => Ok(text.clone()),
        PgOutputValue::Binary(_) => {
            Err(format!("{role} column {column} arrived as binary pgoutput"))
        }
    }
}

fn take_pg_value_text(
    value: &mut PgOutputValue,
    column: &str,
    role: &str,
) -> Result<String, String> {
    match std::mem::replace(value, PgOutputValue::Null) {
        PgOutputValue::Text(text) => Ok(text),
        other => {
            let error = pg_value_text(&other, column, role)
                .err()
                .unwrap_or_else(|| format!("{role} column {column} is missing"));
            *value = other;
            Err(error)
        }
    }
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
    let cell = cell.trim();
    if cell.eq_ignore_ascii_case("t")
        || cell.eq_ignore_ascii_case("true")
        || cell == "1"
        || cell.eq_ignore_ascii_case("yes")
        || cell.eq_ignore_ascii_case("on")
    {
        return Ok(true);
    }
    if cell.eq_ignore_ascii_case("f")
        || cell.eq_ignore_ascii_case("false")
        || cell == "0"
        || cell.eq_ignore_ascii_case("no")
        || cell.eq_ignore_ascii_case("off")
    {
        return Ok(false);
    }
    Err(format!("async mirror PK boolean value `{cell}`"))
}

#[cfg(test)]
mod tests {
    use super::{
        order_column_text, parse_pk_bool, parse_pk_ints, pk_column_indexes, pk_identity,
        pk_type_oids, primary_key_cells, take_pk_cells_and_order_text, PkBindColumn, PkCell,
        PkIdentity, INT8OID,
    };
    use crate::mirror::r#async::pgoutput::{
        PgOutputColumn, PgOutputRelation, PgOutputTuple, PgOutputValue,
    };
    use std::collections::HashSet;
    use std::mem::size_of;

    fn relation(columns: &[(&str, bool)]) -> PgOutputRelation {
        relation_with_oids(columns.iter().map(|(name, key)| (*name, *key, 25)))
    }

    fn relation_with_oids<'a>(
        columns: impl IntoIterator<Item = (&'a str, bool, u32)>,
    ) -> PgOutputRelation {
        PgOutputRelation {
            id: 1,
            namespace: "public".into(),
            name: "items".into(),
            replica_identity: b'd',
            columns: columns
                .into_iter()
                .map(|(name, key, type_oid)| PgOutputColumn {
                    key,
                    name: name.into(),
                    type_oid,
                    typmod: -1,
                })
                .collect(),
        }
    }

    fn text_tuple(cells: &[&str]) -> PgOutputTuple {
        PgOutputTuple {
            values: cells
                .iter()
                .map(|cell| PgOutputValue::Text((*cell).to_string()))
                .collect(),
        }
    }

    fn extract_cells(
        relation: &PgOutputRelation,
        primary_key: &[String],
        tuple: &mut PgOutputTuple,
    ) -> Vec<PkCell> {
        let keys = pk_column_indexes(relation, primary_key).unwrap();
        let type_oids = pk_type_oids(relation, &keys).unwrap();
        primary_key_cells(relation, primary_key, &keys, &type_oids, tuple).unwrap()
    }

    #[test]
    fn pk_identity_int8_is_inline_copy_key() {
        assert_eq!(
            PkIdentity::from_cells(&[PkCell::Int8(42)]),
            PkIdentity::Int8(42)
        );
        assert!(
            size_of::<PkIdentity>() <= 24,
            "PkIdentity is {} bytes; box text/composite so HashSet keys stay small",
            size_of::<PkIdentity>()
        );
        let mut seen = HashSet::new();
        assert!(seen.insert(PkIdentity::Int8(42)));
        assert!(!seen.insert(PkIdentity::Int8(42)));
        assert!(seen.insert(PkIdentity::Int8(43)));
    }

    #[test]
    fn pk_identity_keeps_composite_cells_distinct() {
        let left = pk_identity(&[PkCell::Text("a".into()), PkCell::Text("t1".into())]);
        let right = pk_identity(&[PkCell::Text("at1".into())]);
        assert_ne!(left, right);
        assert_eq!(
            left,
            PkIdentity::Composite(vec![PkCell::Text("a".into()), PkCell::Text("t1".into())].into())
        );
        assert_eq!(right, PkIdentity::Text("at1".into()));
    }

    #[test]
    fn primary_key_cells_parses_int8_without_keeping_text() {
        let relation = relation_with_oids([("id", true, INT8OID), ("body", false, 25)]);
        let mut tuple = text_tuple(&["42"]);
        let cells = extract_cells(&relation, &["id".into()], &mut tuple);
        assert_eq!(cells, vec![PkCell::Int8(42)]);
        assert_eq!(pk_identity(&cells), PkIdentity::Int8(42));
    }

    #[test]
    fn primary_key_cells_reads_compact_old_tuple() {
        let relation = relation(&[("id", true), ("body", false)]);
        let mut tuple = text_tuple(&["42"]);
        let cells = extract_cells(&relation, &["id".into()], &mut tuple);
        assert_eq!(cells, vec![PkCell::Text("42".into())]);
    }

    #[test]
    fn primary_key_cells_reads_full_tuple_by_column_index() {
        let relation = relation(&[("body", false), ("id", true)]);
        let mut tuple = text_tuple(&["hello", "42"]);
        let cells = extract_cells(&relation, &["id".into()], &mut tuple);
        assert_eq!(cells, vec![PkCell::Text("42".into())]);
    }

    #[test]
    fn primary_key_cells_preserves_catalog_pk_order() {
        let relation = relation(&[("tenant", true), ("id", true), ("body", false)]);
        let mut tuple = text_tuple(&["acme", "7", "note"]);
        let cells = extract_cells(&relation, &["id".into(), "tenant".into()], &mut tuple);
        assert_eq!(
            cells,
            vec![PkCell::Text("7".into()), PkCell::Text("acme".into())]
        );
        assert_eq!(
            pk_identity(&cells),
            PkIdentity::Composite(
                vec![PkCell::Text("7".into()), PkCell::Text("acme".into())].into()
            )
        );
    }

    #[test]
    fn parse_pk_bool_accepts_pg_forms() {
        assert!(parse_pk_bool("t").unwrap());
        assert!(!parse_pk_bool("FALSE").unwrap());
        assert!(parse_pk_bool("maybe").is_err());
        assert_eq!(
            parse_pk_ints::<i32>(&["1".into(), "2".into()], "int4").unwrap(),
            vec![1, 2]
        );
    }

    #[test]
    fn pk_bind_column_parses_int8_once() {
        let mut column = PkBindColumn::with_capacity(20, 2);
        column.push_text("42".into(), "id").unwrap();
        column.push_text("-7".into(), "id").unwrap();
        assert_eq!(column, PkBindColumn::Int8(vec![42, -7]));
        assert!(column.push_text("nope".into(), "id").is_err());
    }

    #[test]
    fn pk_bind_column_accepts_parsed_int8_cell() {
        let mut column = PkBindColumn::with_capacity(INT8OID, 1);
        column.push_cell(PkCell::Int8(42), "id").unwrap();
        assert_eq!(column, PkBindColumn::Int8(vec![42]));
        assert!(column.push_cell(PkCell::Text("42".into()), "id").is_err());
    }

    #[test]
    fn pk_bind_column_parses_bool_and_keeps_non_builtin_as_text() {
        let mut flag = PkBindColumn::with_capacity(16, 1);
        flag.push_text("t".into(), "active").unwrap();
        assert_eq!(flag, PkBindColumn::Bool(vec![true]));

        let mut uuid = PkBindColumn::with_capacity(2950, 1);
        uuid.push_text("a".into(), "ext_id").unwrap();
        assert_eq!(uuid, PkBindColumn::Text(vec!["a".into()]));
    }

    #[test]
    fn pk_column_indexes_are_reusable_for_extract() {
        let relation = relation(&[("body", false), ("id", true)]);
        let keys = pk_column_indexes(&relation, &["id".into()]).unwrap();
        assert_eq!(keys, vec![1]);
        let mut tuple = text_tuple(&["hello", "42"]);
        let cells = extract_cells(&relation, &["id".into()], &mut tuple);
        assert_eq!(cells, vec![PkCell::Text("42".into())]);
    }

    #[test]
    fn taking_pk_nulls_overlapping_order_column_until_peeked_first() {
        let relation = relation_with_oids([("id", true, INT8OID)]);
        let mut tuple = text_tuple(&["42"]);
        let keys = pk_column_indexes(&relation, &["id".into()]).unwrap();
        let type_oids = pk_type_oids(&relation, &keys).unwrap();
        let _ =
            primary_key_cells(&relation, &["id".into()], &keys, &type_oids, &mut tuple).unwrap();
        assert!(
            order_column_text(&relation, "id", &tuple).is_err(),
            "taking the PK must not be the only extract when id is also the order column"
        );
    }

    #[test]
    fn overlapping_order_column_is_read_before_pk_take() {
        let relation = relation_with_oids([("id", true, INT8OID), ("body", false, 25)]);
        let mut tuple = text_tuple(&["42", "hello"]);
        let keys = pk_column_indexes(&relation, &["id".into()]).unwrap();
        let type_oids = pk_type_oids(&relation, &keys).unwrap();
        let (cells, order) = take_pk_cells_and_order_text(
            &relation,
            &["id".into()],
            &keys,
            &type_oids,
            Some("id"),
            &mut tuple,
        )
        .unwrap();
        assert_eq!(cells, vec![PkCell::Int8(42)]);
        assert_eq!(order.as_deref(), Some("42"));
    }

    #[test]
    fn non_overlapping_order_column_survives_pk_take() {
        let relation = relation(&[("id", true), ("created_at", false)]);
        let mut tuple = text_tuple(&["7", "2020-01-01"]);
        let keys = pk_column_indexes(&relation, &["id".into()]).unwrap();
        let type_oids = pk_type_oids(&relation, &keys).unwrap();
        let (cells, order) = take_pk_cells_and_order_text(
            &relation,
            &["id".into()],
            &keys,
            &type_oids,
            Some("created_at"),
            &mut tuple,
        )
        .unwrap();
        assert_eq!(cells, vec![PkCell::Text("7".into())]);
        assert_eq!(order.as_deref(), Some("2020-01-01"));
    }
}
