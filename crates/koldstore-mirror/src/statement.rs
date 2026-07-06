//! Pg-free SQL statement contract for mirror storage operations.

/// Mirror statement access mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MirrorAccess {
    /// Statement only reads mirror storage.
    ReadOnly,
    /// Statement mutates mirror storage.
    ReadWrite,
}

/// Pg-free SQL parameter type metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SqlParamType {
    /// PostgreSQL `bigint` / `int8`.
    BigInt,
    /// PostgreSQL `integer` / `int4`.
    Integer,
    /// PostgreSQL `text`.
    Text,
    /// PostgreSQL `jsonb`.
    Jsonb,
    /// PostgreSQL `oid`.
    Oid,
    /// PostgreSQL `uuid`.
    Uuid,
    /// PostgreSQL `boolean`.
    Boolean,
}

/// Planned mirror storage statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorStatement {
    /// Human-readable operation label.
    pub label: &'static str,
    /// SQL text with bind placeholders owned by the caller.
    pub sql: String,
    /// Expected access mode.
    pub access: MirrorAccess,
    /// Bind parameter types by one-based placeholder position.
    pub param_types: Vec<SqlParamType>,
}

impl MirrorStatement {
    /// Creates a read-only mirror statement.
    #[must_use]
    pub fn read(label: &'static str, sql: impl Into<String>) -> Self {
        Self::read_with_params(label, sql, [])
    }

    /// Creates a read-only mirror statement with parameter metadata.
    #[must_use]
    pub fn read_with_params(
        label: &'static str,
        sql: impl Into<String>,
        param_types: impl Into<Vec<SqlParamType>>,
    ) -> Self {
        Self {
            label,
            sql: sql.into(),
            access: MirrorAccess::ReadOnly,
            param_types: param_types.into(),
        }
    }

    /// Creates a read-write mirror statement.
    #[must_use]
    pub fn write(label: &'static str, sql: impl Into<String>) -> Self {
        Self::write_with_params(label, sql, [])
    }

    /// Creates a read-write mirror statement with parameter metadata.
    #[must_use]
    pub fn write_with_params(
        label: &'static str,
        sql: impl Into<String>,
        param_types: impl Into<Vec<SqlParamType>>,
    ) -> Self {
        Self {
            label,
            sql: sql.into(),
            access: MirrorAccess::ReadWrite,
            param_types: param_types.into(),
        }
    }
}
