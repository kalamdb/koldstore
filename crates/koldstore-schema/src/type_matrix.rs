//! PostgreSQL type support matrix.

use koldstore_common::canonical_postgres_type_name;
use serde::{Deserialize, Serialize};

/// PostgreSQL type class.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgTypeClass {
    pub name: String,
}

/// Type support entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeSupport {
    pub supported: bool,
    pub diagnostic: Option<String>,
}

/// Type support matrix.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeMatrix {
    pub entries: Vec<(PgTypeClass, TypeSupport)>,
}

impl TypeMatrix {
    /// Returns the default PostgreSQL 15+ MVP type support matrix.
    #[must_use]
    pub fn postgres_15_default() -> Self {
        let supported = [
            "bool",
            "int2",
            "int4",
            "int8",
            "float4",
            "float8",
            "text",
            "varchar",
            "uuid",
            "jsonb",
            "timestamptz",
        ];
        Self {
            entries: supported
                .into_iter()
                .map(|name| {
                    (
                        PgTypeClass {
                            name: name.to_string(),
                        },
                        TypeSupport {
                            supported: true,
                            diagnostic: None,
                        },
                    )
                })
                .collect(),
        }
    }

    /// Returns support for a PostgreSQL type name.
    #[must_use]
    pub fn support_for(&self, type_name: &str) -> TypeSupport {
        let normalized = canonical_postgres_type_name(type_name);
        self.entries
            .iter()
            .find(|(class, _)| class.name == normalized)
            .map(|(_, support)| support.clone())
            .unwrap_or_else(|| TypeSupport {
                supported: false,
                diagnostic: Some(format!(
                    "unsupported PostgreSQL type: {type_name}; see pg-koldstore type matrix"
                )),
            })
    }
}

/// Normalizes common PostgreSQL type aliases to canonical matrix names.
#[must_use]
pub fn normalize_type_name(type_name: &str) -> String {
    canonical_postgres_type_name(type_name)
}
