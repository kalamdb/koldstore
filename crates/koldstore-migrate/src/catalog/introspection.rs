//! PostgreSQL catalog introspection SQL plans for migration workflows.
//!
//! Owns read-only catalog probe statements and JSON decoding helpers used when
//! planning existing-table migration. SPI execution stays in `pg_koldstore`.

use serde::Deserialize;
use thiserror::Error;

use koldstore_common::SqlStatement;

use crate::constraints::{
    FkDirection, ForeignKeyShape, ManageTableConstraintsCatalog, UniqueConstraintShape,
};
use crate::order::{CatalogColumn, CatalogPrimaryKey};
use crate::plan::ExistingTableCatalog;
use crate::register::{PrimaryKeyShapeCatalogRow, RegistryError};
use crate::MigrationResult;

/// Catalog introspection error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IntrospectionError {
    /// Primary-key catalog JSON could not be decoded.
    #[error("primary key catalog decode failed: {0}")]
    PrimaryKeyDecode(String),
    /// Column catalog JSON could not be decoded.
    #[error("column catalog decode failed: {0}")]
    ColumnDecode(String),
    /// Indexed-column catalog JSON could not be decoded.
    #[error("indexed column catalog decode failed: {0}")]
    IndexedColumnDecode(String),
    /// Constraint catalog JSON could not be decoded.
    #[error("constraint catalog decode failed: {0}")]
    ConstraintDecode(String),
    /// SQL statement metadata could not be prepared.
    #[error("{0}")]
    Sql(String),
}

/// Plans the primary-key column name probe for one table OID.
///
/// # Errors
///
/// Returns an error when statement metadata cannot be prepared.
pub fn plan_primary_key_columns_probe() -> MigrationResult<SqlStatement> {
    SqlStatement::read(
        "migration primary key columns",
        r#"
SELECT COALESCE(jsonb_agg(a.attname ORDER BY key_position.ordinality)::text, '[]')
FROM pg_index i
JOIN unnest(i.indkey) WITH ORDINALITY AS key_position(attnum, ordinality) ON true
JOIN pg_attribute a
  ON a.attrelid = i.indrelid
 AND a.attnum = key_position.attnum
WHERE i.indrelid = $1::oid
  AND i.indisprimary
"#,
    )
    .map_err(|error| IntrospectionError::Sql(error.to_string()))
    .map_err(Into::into)
}

/// Plans the user-column metadata probe for one table OID.
///
/// # Errors
///
/// Returns an error when statement metadata cannot be prepared.
pub fn plan_table_columns_probe() -> MigrationResult<SqlStatement> {
    SqlStatement::read(
        "migration table columns",
        r#"
WITH pk AS (
    SELECT a.attname
    FROM pg_index i
    JOIN unnest(i.indkey) WITH ORDINALITY AS key_position(attnum, ordinality) ON true
    JOIN pg_attribute a
      ON a.attrelid = i.indrelid
     AND a.attnum = key_position.attnum
    WHERE i.indrelid = $1::oid
      AND i.indisprimary
)
SELECT COALESCE(
    jsonb_agg(
        jsonb_build_object(
            'name', a.attname,
            'type_name', format_type(a.atttypid, a.atttypmod),
            'is_primary_key', pk.attname IS NOT NULL,
            'identity', a.attidentity <> '',
            'generated', a.attgenerated <> '',
            'default_expr', pg_get_expr(d.adbin, d.adrelid)
        )
        ORDER BY a.attnum
    )::text,
    '[]'
)
FROM pg_attribute a
LEFT JOIN pg_attrdef d
  ON d.adrelid = a.attrelid
 AND d.adnum = a.attnum
LEFT JOIN pk
  ON pk.attname = a.attname
WHERE a.attrelid = $1::oid
  AND a.attnum > 0
  AND NOT a.attisdropped
"#,
    )
    .map_err(|error| IntrospectionError::Sql(error.to_string()))
    .map_err(Into::into)
}

/// Plans the indexed-column candidate probe for one table OID.
///
/// # Errors
///
/// Returns an error when statement metadata cannot be prepared.
pub fn plan_indexed_columns_probe() -> MigrationResult<SqlStatement> {
    SqlStatement::read(
        "migration indexed columns",
        r#"
WITH pk AS (
    SELECT a.attname
    FROM pg_index i
    JOIN unnest(i.indkey) WITH ORDINALITY AS key_position(attnum, ordinality) ON true
    JOIN pg_attribute a
      ON a.attrelid = i.indrelid
     AND a.attnum = key_position.attnum
    WHERE i.indrelid = $1::oid
      AND i.indisprimary
),
candidate AS (
    SELECT a.attname, i.indexrelid::bigint AS source_oid, key_position.ordinality
    FROM pg_index i
    JOIN unnest(i.indkey) WITH ORDINALITY AS key_position(attnum, ordinality) ON true
    JOIN pg_attribute a
      ON a.attrelid = i.indrelid
     AND a.attnum = key_position.attnum
    WHERE i.indrelid = $1::oid
      AND NOT i.indisprimary
      AND i.indexprs IS NULL
    UNION ALL
    SELECT a.attname, c.oid::bigint AS source_oid, key_position.ordinality
    FROM pg_constraint c
    JOIN unnest(c.conkey) WITH ORDINALITY AS key_position(attnum, ordinality) ON true
    JOIN pg_attribute a
      ON a.attrelid = c.conrelid
     AND a.attnum = key_position.attnum
    WHERE c.conrelid = $1::oid
      AND c.contype = 'f'
),
ranked AS (
    SELECT DISTINCT ON (candidate.attname)
        candidate.attname,
        candidate.source_oid,
        candidate.ordinality
    FROM candidate
    LEFT JOIN pk ON pk.attname = candidate.attname
    WHERE pk.attname IS NULL
    ORDER BY candidate.attname, candidate.source_oid, candidate.ordinality
)
SELECT COALESCE(jsonb_agg(attname ORDER BY source_oid, ordinality, attname)::text, '[]')
FROM ranked
"#,
    )
    .map_err(|error| IntrospectionError::Sql(error.to_string()))
    .map_err(Into::into)
}

/// Plans the unique/FK constraint probe used by `koldstore.manage_table`.
///
/// # Errors
///
/// Returns an error when statement metadata cannot be prepared.
pub fn plan_manage_table_constraints_probe() -> MigrationResult<SqlStatement> {
    SqlStatement::read(
        "manage table constraints",
        r#"
SELECT jsonb_build_object(
    'unique_constraints', COALESCE((
        SELECT jsonb_agg(
            jsonb_build_object(
                'name', entries.constraint_name,
                'columns', entries.columns
            )
            ORDER BY entries.constraint_name
        )
        FROM (
            SELECT
                c.conname AS constraint_name,
                jsonb_agg(a.attname ORDER BY key_position.ordinality) AS columns
            FROM pg_constraint c
            JOIN unnest(c.conkey) WITH ORDINALITY AS key_position(attnum, ordinality) ON true
            JOIN pg_attribute a
              ON a.attrelid = c.conrelid
             AND a.attnum = key_position.attnum
            WHERE c.conrelid = $1::oid
              AND c.contype = 'u'
            GROUP BY c.conname
            UNION ALL
            SELECT
                idx.relname AS constraint_name,
                jsonb_agg(a.attname ORDER BY key_position.ordinality) AS columns
            FROM pg_index i
            JOIN pg_class idx ON idx.oid = i.indexrelid
            JOIN unnest(i.indkey) WITH ORDINALITY AS key_position(attnum, ordinality) ON true
            JOIN pg_attribute a
              ON a.attrelid = i.indrelid
             AND a.attnum = key_position.attnum
            WHERE i.indrelid = $1::oid
              AND i.indisunique
              AND NOT i.indisprimary
              AND i.indexprs IS NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM pg_constraint c
                  WHERE c.conindid = i.indexrelid
              )
            GROUP BY idx.relname
        ) AS entries
    ), '[]'::jsonb),
    'foreign_keys', COALESCE((
        SELECT jsonb_agg(entry ORDER BY entry->>'name')
        FROM (
            SELECT jsonb_build_object(
                'name', c.conname,
                'direction', 'outbound',
                'columns', (
                    SELECT COALESCE(
                        jsonb_agg(a.attname ORDER BY key_position.ordinality),
                        '[]'::jsonb
                    )
                    FROM unnest(c.conkey) WITH ORDINALITY AS key_position(attnum, ordinality)
                    JOIN pg_attribute a
                      ON a.attrelid = c.conrelid
                     AND a.attnum = key_position.attnum
                ),
                'related_relation', format('%I.%I', ref_ns.nspname, ref_class.relname)
            ) AS entry
            FROM pg_constraint c
            JOIN pg_class ref_class ON ref_class.oid = c.confrelid
            JOIN pg_namespace ref_ns ON ref_ns.oid = ref_class.relnamespace
            WHERE c.conrelid = $1::oid
              AND c.contype = 'f'
            UNION ALL
            SELECT jsonb_build_object(
                'name', c.conname,
                'direction', 'inbound',
                'columns', (
                    SELECT COALESCE(
                        jsonb_agg(a.attname ORDER BY key_position.ordinality),
                        '[]'::jsonb
                    )
                    FROM unnest(c.conkey) WITH ORDINALITY AS key_position(attnum, ordinality)
                    JOIN pg_attribute a
                      ON a.attrelid = c.conrelid
                     AND a.attnum = key_position.attnum
                ),
                'related_relation', format('%I.%I', src_ns.nspname, src_class.relname)
            ) AS entry
            FROM pg_constraint c
            JOIN pg_class src_class ON src_class.oid = c.conrelid
            JOIN pg_namespace src_ns ON src_ns.oid = src_class.relnamespace
            WHERE c.confrelid = $1::oid
              AND c.contype = 'f'
        ) AS fk_entries
    ), '[]'::jsonb)
)::text
"#,
    )
    .map_err(|error| IntrospectionError::Sql(error.to_string()))
    .map_err(Into::into)
}

/// Decodes unique/FK constraint probe JSON.
///
/// # Errors
///
/// Returns an error when JSON decoding fails.
pub fn decode_manage_table_constraints_catalog(
    json: &str,
) -> Result<ManageTableConstraintsCatalog, IntrospectionError> {
    #[derive(Deserialize)]
    struct ConstraintWire {
        name: String,
        columns: Vec<String>,
        #[serde(default)]
        direction: Option<String>,
        #[serde(default)]
        related_relation: Option<String>,
    }

    #[derive(Deserialize)]
    struct CatalogWire {
        #[serde(default)]
        unique_constraints: Vec<ConstraintWire>,
        #[serde(default)]
        foreign_keys: Vec<ConstraintWire>,
    }

    let wire = serde_json::from_str::<CatalogWire>(json)
        .map_err(|error| IntrospectionError::ConstraintDecode(error.to_string()))?;

    Ok(ManageTableConstraintsCatalog {
        unique_constraints: wire
            .unique_constraints
            .into_iter()
            .map(|entry| UniqueConstraintShape {
                name: entry.name,
                columns: entry.columns,
            })
            .collect(),
        foreign_keys: wire
            .foreign_keys
            .into_iter()
            .map(|entry| ForeignKeyShape {
                name: entry.name,
                direction: match entry.direction.as_deref() {
                    Some("inbound") => FkDirection::Inbound,
                    _ => FkDirection::Outbound,
                },
                columns: entry.columns,
                related_relation: entry.related_relation,
            })
            .collect(),
    })
}

/// Decodes catalog probe JSON into migration planning metadata.
///
/// # Errors
///
/// Returns an error when any JSON payload cannot be decoded.
pub fn decode_existing_table_catalog(
    primary_key_json: &str,
    columns_json: &str,
    indexed_columns_json: &str,
) -> Result<ExistingTableCatalog, IntrospectionError> {
    let primary_key = serde_json::from_str::<Vec<String>>(primary_key_json)
        .map_err(|error| IntrospectionError::PrimaryKeyDecode(error.to_string()))?;
    let columns = serde_json::from_str::<Vec<CatalogColumn>>(columns_json)
        .map_err(|error| IntrospectionError::ColumnDecode(error.to_string()))?;
    let indexed_columns = serde_json::from_str::<Vec<String>>(indexed_columns_json)
        .map_err(|error| IntrospectionError::IndexedColumnDecode(error.to_string()))?;

    Ok(ExistingTableCatalog {
        primary_key: CatalogPrimaryKey {
            columns: primary_key,
        },
        columns,
        indexed_columns,
    })
}

impl From<IntrospectionError> for crate::MigrationError {
    fn from(error: IntrospectionError) -> Self {
        match error {
            IntrospectionError::Sql(message) => Self::Sql(message),
            other => Self::Ordering(other.to_string()),
        }
    }
}

/// Decodes primary-key shape catalog rows from JSON text.
///
/// # Errors
///
/// Returns an error when JSON decoding fails or the shape is unsupported.
pub fn decode_primary_key_shape_catalog(
    json: &str,
) -> Result<koldstore_common::PrimaryKeyShape, RegistryError> {
    let rows = serde_json::from_str::<Vec<PrimaryKeyShapeCatalogRow>>(json).map_err(|error| {
        RegistryError::Spi(format!("primary key shape catalog decode failed: {error}"))
    })?;
    crate::register::primary_key_shape_from_catalog_rows(rows)
}

/// Primary-key shape rows as returned by the catalog probe.
pub type PrimaryKeyShapeCatalogRows = Vec<PrimaryKeyShapeCatalogRow>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_manage_table_constraint_catalog_payload() {
        let catalog = decode_manage_table_constraints_catalog(
            r#"{
                "unique_constraints": [
                    {"name": "users_email_key", "columns": ["email"]}
                ],
                "foreign_keys": [
                    {
                        "name": "items_user_id_fkey",
                        "direction": "outbound",
                        "columns": ["user_id"],
                        "related_relation": "public.users"
                    }
                ]
            }"#,
        )
        .unwrap();

        assert_eq!(catalog.unique_constraints.len(), 1);
        assert_eq!(catalog.unique_constraints[0].columns, vec!["email"]);
        assert_eq!(catalog.foreign_keys[0].direction, FkDirection::Outbound);
        assert_eq!(
            catalog.foreign_keys[0].related_relation.as_deref(),
            Some("public.users")
        );
    }

    #[test]
    fn decodes_empty_catalog_payloads() {
        let catalog = decode_existing_table_catalog("[]", "[]", "[]").unwrap();
        assert!(catalog.primary_key.columns.is_empty());
        assert!(catalog.columns.is_empty());
        assert!(catalog.indexed_columns.is_empty());
    }
}
