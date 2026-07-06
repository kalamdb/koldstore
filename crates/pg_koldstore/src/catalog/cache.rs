//! Backend-local cache for stable managed-table metadata.

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use crate::migrate::QualifiedTableName;

/// Stable table-shape metadata for one managed table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedTableSnapshot {
    /// Source table OID.
    pub table_oid: u32,
    /// Active schema version.
    pub schema_version: i32,
    /// Whether this schema entry is active.
    pub active: bool,
    /// Mirror initialization state.
    pub initialization_state: String,
    /// Active change-log mirror relation.
    pub mirror_relation: QualifiedTableName,
    /// Preserved primary-key columns.
    pub primary_key_columns: Vec<String>,
    /// Hash of the exact primary-key shape JSON.
    pub primary_key_shape_hash: u64,
    /// Optional user-scope column.
    pub scope_column: Option<String>,
}

/// Decodes a stable managed-table snapshot from catalog JSON.
///
/// # Errors
///
/// Returns an error when required fields are missing or invalid.
pub fn decode_managed_table_snapshot(
    value: &serde_json::Value,
) -> Result<ManagedTableSnapshot, String> {
    let table_oid = value
        .get("table_oid")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| "missing integer field `table_oid`".to_string())
        .and_then(|oid| u32::try_from(oid).map_err(|error| error.to_string()))?;
    let schema_version = value
        .get("schema_version")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| "missing integer field `schema_version`".to_string())
        .and_then(|version| i32::try_from(version).map_err(|error| error.to_string()))?;
    let active = value
        .get("active")
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| "missing boolean field `active`".to_string())?;
    let initialization_state = value
        .get("initialization_state")
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| "missing string field `initialization_state`".to_string())?;
    let mirror_relation = value
        .get("mirror_relation")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "missing string field `mirror_relation`".to_string())?;
    let mirror_relation =
        QualifiedTableName::parse(mirror_relation).map_err(|error| error.to_string())?;
    let primary_key_columns = serde_json::from_value::<Vec<String>>(
        value
            .get("primary_key")
            .cloned()
            .ok_or_else(|| "missing array field `primary_key`".to_string())?,
    )
    .map_err(|error| error.to_string())?;
    let primary_key_shape = value
        .get("primary_key_shape")
        .ok_or_else(|| "missing field `primary_key_shape`".to_string())?;
    let mut hasher = DefaultHasher::new();
    primary_key_shape.to_string().hash(&mut hasher);
    let scope_column = match value.get("scope_column") {
        Some(scope) if scope.is_null() => None,
        Some(scope) => Some(
            scope
                .as_str()
                .ok_or_else(|| "field `scope_column` must be string or null".to_string())?
                .to_string(),
        ),
        None => None,
    };

    Ok(ManagedTableSnapshot {
        table_oid,
        schema_version,
        active,
        initialization_state,
        mirror_relation,
        primary_key_columns,
        primary_key_shape_hash: hasher.finish(),
        scope_column,
    })
}

#[cfg(feature = "pg")]
thread_local! {
    static MANAGED_TABLE_CACHE: std::cell::RefCell<
        std::collections::HashMap<u32, ManagedTableSnapshot>
    > = std::cell::RefCell::new(std::collections::HashMap::new());
}

/// Invalidates one managed-table snapshot in the current backend.
#[cfg(feature = "pg")]
pub fn invalidate_table(table_oid: pgrx::pg_sys::Oid) {
    MANAGED_TABLE_CACHE.with(|cache| {
        cache.borrow_mut().remove(&table_oid.to_u32());
    });
}

/// Invalidates all managed-table snapshots in the current backend.
#[cfg(feature = "pg")]
pub fn invalidate_all() {
    MANAGED_TABLE_CACHE.with(|cache| {
        cache.borrow_mut().clear();
    });
}

/// Loads a managed-table snapshot from cache or catalog.
///
/// # Errors
///
/// Returns an error when SPI execution or snapshot decoding fails.
#[cfg(feature = "pg")]
pub fn managed_table_snapshot(
    table_oid: pgrx::pg_sys::Oid,
) -> crate::spi::SpiResult<Option<ManagedTableSnapshot>> {
    let key = table_oid.to_u32();
    if let Some(snapshot) = MANAGED_TABLE_CACHE.with(|cache| cache.borrow().get(&key).cloned()) {
        return Ok(Some(snapshot));
    }

    let snapshot = load_managed_table_snapshot(table_oid)?;
    if let Some(snapshot) = snapshot.as_ref() {
        MANAGED_TABLE_CACHE.with(|cache| {
            cache.borrow_mut().insert(key, snapshot.clone());
        });
    }
    Ok(snapshot)
}

#[cfg(feature = "pg")]
fn load_managed_table_snapshot(
    table_oid: pgrx::pg_sys::Oid,
) -> crate::spi::SpiResult<Option<ManagedTableSnapshot>> {
    let statement = crate::catalog::queries::plan_managed_table_snapshot()?;
    let json = crate::spi::select_one::<String>(
        &statement,
        &[pgrx::datum::DatumWithOid::from(table_oid)],
    )?;
    json.map(|json| {
        serde_json::from_str::<serde_json::Value>(&json)
            .map_err(|error| crate::spi::map_spi_error(&statement.operation, &error.to_string()))
            .and_then(|value| {
                decode_managed_table_snapshot(&value)
                    .map_err(|error| crate::spi::map_spi_error(&statement.operation, &error))
            })
    })
    .transpose()
}
