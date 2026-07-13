//! Managed-table snapshot shapes for catalog caching.
//!
//! Runtime snapshots are assembled from `koldstore.schemas` rows. Schema
//! registry *writes* stay in `koldstore-migrate`; this module owns the
//! PG-free decode + in-process cache shape used by `pg_koldstore`.

use std::collections::HashMap;
use std::sync::Arc;

use koldstore_common::TableName;
use koldstore_schema::MirrorInitializationState;
use serde::Deserialize;

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
    pub initialization_state: MirrorInitializationState,
    /// Active change-log mirror relation.
    pub mirror_relation: TableName,
    /// Preserved primary-key columns.
    pub primary_key_columns: Vec<String>,
    /// Hash of the exact primary-key shape JSON.
    pub primary_key_shape_hash: u64,
    /// Optional user-scope column.
    pub scope_column: Option<String>,
}

/// Whether committed cold data can contribute rows to a managed scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColdVisibility {
    /// No published manifest segment exists, so the native hot child is complete.
    NoPublishedSegments,
    /// At least one segment is visible through the committed manifest generation.
    Published {
        /// Manifest generation that made the cold segments visible.
        manifest_generation: String,
    },
}

/// Cached planner/executor eligibility for one PostgreSQL relation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManagedScanEligibility {
    /// The relation is not managed by KoldStore.
    Unmanaged,
    /// The relation is managed, with its stable table shape and cold visibility.
    Managed {
        /// Shared managed-table metadata.
        snapshot: Arc<ManagedTableSnapshot>,
        /// Whether the scan must prepare the cold path.
        cold_visibility: ColdVisibility,
    },
}

impl ManagedScanEligibility {
    /// Returns the shared snapshot for managed relations.
    #[must_use]
    pub fn snapshot(&self) -> Option<&ManagedTableSnapshot> {
        match self {
            Self::Unmanaged => None,
            Self::Managed { snapshot, .. } => Some(snapshot.as_ref()),
        }
    }

    /// Returns whether a managed relation has no published cold segments.
    #[must_use]
    pub const fn is_hot_only(&self) -> bool {
        matches!(
            self,
            Self::Managed {
                cold_visibility: ColdVisibility::NoPublishedSegments,
                ..
            }
        )
    }
}

/// Positive and negative managed-scan entries keyed by relation OID.
#[derive(Debug, Default)]
pub struct ManagedScanEligibilityCache {
    entries: HashMap<u32, ManagedScanEligibility>,
}

impl ManagedScanEligibilityCache {
    /// Returns a cheap clone of an eligibility entry.
    #[must_use]
    pub fn get(&self, table_oid: u32) -> Option<ManagedScanEligibility> {
        self.entries.get(&table_oid).cloned()
    }

    /// Stores either a positive managed entry or an unmanaged negative entry.
    pub fn insert(&mut self, table_oid: u32, eligibility: ManagedScanEligibility) {
        self.entries.insert(table_oid, eligibility);
    }

    /// Removes one relation's eligibility.
    pub fn invalidate(&mut self, table_oid: u32) {
        self.entries.remove(&table_oid);
    }

    /// Clears every eligibility entry.
    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

/// Decodes a stable managed-table snapshot from catalog JSON text.
///
/// Prefer this over [`decode_managed_table_snapshot`] when the SPI payload is
/// already a JSON string — it avoids an intermediate `Value` clone.
///
/// # Errors
///
/// Returns an error when required fields are missing or invalid.
pub fn decode_managed_table_snapshot_str(json: &str) -> Result<ManagedTableSnapshot, String> {
    let wire: ManagedTableSnapshotWire =
        serde_json::from_str(json).map_err(|error| error.to_string())?;
    wire.try_into()
}

/// Decodes the compact planner eligibility payload for a managed relation.
///
/// # Errors
///
/// Returns an error when the managed snapshot or manifest generation is invalid.
pub fn decode_managed_scan_eligibility_str(json: &str) -> Result<ManagedScanEligibility, String> {
    let wire: ManagedTableSnapshotWire =
        serde_json::from_str(json).map_err(|error| error.to_string())?;
    let cold_visibility = match wire.manifest_generation.as_deref() {
        Some(generation) if !generation.trim().is_empty() => ColdVisibility::Published {
            manifest_generation: generation.to_string(),
        },
        _ => ColdVisibility::NoPublishedSegments,
    };
    Ok(ManagedScanEligibility::Managed {
        snapshot: Arc::new(wire.try_into()?),
        cold_visibility,
    })
}

/// Decodes a stable managed-table snapshot from catalog JSON.
///
/// # Errors
///
/// Returns an error when required fields are missing or invalid.
pub fn decode_managed_table_snapshot(
    value: &serde_json::Value,
) -> Result<ManagedTableSnapshot, String> {
    let wire: ManagedTableSnapshotWire =
        ManagedTableSnapshotWire::deserialize(value).map_err(|error| error.to_string())?;
    wire.try_into()
}

#[derive(Debug, Deserialize)]
struct ManagedTableSnapshotWire {
    table_oid: i64,
    schema_version: i64,
    active: bool,
    initialization_state: MirrorInitializationState,
    mirror_relation: String,
    primary_key: Vec<String>,
    primary_key_shape: serde_json::Value,
    #[serde(default)]
    scope_column: Option<serde_json::Value>,
    #[serde(default)]
    manifest_generation: Option<String>,
}

impl TryFrom<ManagedTableSnapshotWire> for ManagedTableSnapshot {
    type Error = String;

    fn try_from(wire: ManagedTableSnapshotWire) -> Result<Self, Self::Error> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let table_oid = u32::try_from(wire.table_oid).map_err(|error| error.to_string())?;
        let schema_version =
            i32::try_from(wire.schema_version).map_err(|error| error.to_string())?;
        let mirror_relation =
            TableName::parse(&wire.mirror_relation).map_err(|error| error.to_string())?;
        let scope_column = match wire.scope_column {
            None => None,
            Some(scope) if scope.is_null() => None,
            Some(scope) => Some(
                scope
                    .as_str()
                    .ok_or_else(|| "field `scope_column` must be string or null".to_string())?
                    .to_string(),
            ),
        };
        let mut hasher = DefaultHasher::new();
        wire.primary_key_shape.to_string().hash(&mut hasher);

        Ok(Self {
            table_oid,
            schema_version,
            active: wire.active,
            initialization_state: wire.initialization_state,
            mirror_relation,
            primary_key_columns: wire.primary_key,
            primary_key_shape_hash: hasher.finish(),
            scope_column,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        decode_managed_scan_eligibility_str, ColdVisibility, ManagedScanEligibility,
        ManagedScanEligibilityCache, ManagedTableSnapshot,
    };
    use koldstore_common::TableName;
    use koldstore_schema::MirrorInitializationState;
    use std::sync::Arc;

    fn snapshot(table_oid: u32) -> Arc<ManagedTableSnapshot> {
        Arc::new(ManagedTableSnapshot {
            table_oid,
            schema_version: 1,
            active: true,
            initialization_state: MirrorInitializationState::Complete,
            mirror_relation: TableName::parse("koldstore.events__cl").unwrap(),
            primary_key_columns: vec!["id".to_string()],
            primary_key_shape_hash: 42,
            scope_column: None,
        })
    }

    #[test]
    fn eligibility_cache_keeps_unmanaged_negative_entries() {
        let mut cache = ManagedScanEligibilityCache::default();

        cache.insert(41, ManagedScanEligibility::Unmanaged);

        assert_eq!(cache.get(41), Some(ManagedScanEligibility::Unmanaged));
    }

    #[test]
    fn eligibility_cache_keeps_managed_hot_only_entries_without_cloning_snapshot_data() {
        let mut cache = ManagedScanEligibilityCache::default();
        let snapshot = snapshot(42);
        cache.insert(
            42,
            ManagedScanEligibility::Managed {
                snapshot: Arc::clone(&snapshot),
                cold_visibility: ColdVisibility::NoPublishedSegments,
            },
        );

        let ManagedScanEligibility::Managed {
            snapshot: cached,
            cold_visibility,
        } = cache.get(42).unwrap()
        else {
            panic!("expected managed eligibility");
        };

        assert!(Arc::ptr_eq(&snapshot, &cached));
        assert_eq!(cold_visibility, ColdVisibility::NoPublishedSegments);
    }

    #[test]
    fn eligibility_cache_invalidates_one_table_or_all_tables() {
        let mut cache = ManagedScanEligibilityCache::default();
        cache.insert(41, ManagedScanEligibility::Unmanaged);
        cache.insert(
            42,
            ManagedScanEligibility::Managed {
                snapshot: snapshot(42),
                cold_visibility: ColdVisibility::Published {
                    manifest_generation: "generation-7".to_string(),
                },
            },
        );

        cache.invalidate(41);
        assert_eq!(cache.get(41), None);
        assert!(cache.get(42).is_some());

        cache.clear();
        assert_eq!(cache.get(42), None);
    }

    #[test]
    fn eligibility_decode_distinguishes_hot_only_from_published_cold() {
        let base = serde_json::json!({
            "table_oid": 42,
            "schema_version": 1,
            "active": true,
            "initialization_state": "complete",
            "mirror_relation": "koldstore.events__cl",
            "primary_key": ["id"],
            "primary_key_shape": [{"name": "id", "type_oid": 20}],
            "scope_column": null,
            "manifest_generation": null
        });

        let hot = decode_managed_scan_eligibility_str(&base.to_string()).unwrap();
        assert!(hot.is_hot_only());

        let mut cold_json = base;
        cold_json["manifest_generation"] = serde_json::json!("generation-7");
        let cold = decode_managed_scan_eligibility_str(&cold_json.to_string()).unwrap();
        assert!(matches!(
            cold,
            ManagedScanEligibility::Managed {
                cold_visibility: ColdVisibility::Published {
                    manifest_generation
                },
                ..
            } if manifest_generation == "generation-7"
        ));
    }
}
