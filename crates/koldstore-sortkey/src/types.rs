//! Sort Key V1 type taxonomy and value shapes.

use uuid::Uuid;

/// Persisted codec identity for KoldStore Sort Key V1.
///
/// Stored beside every `cold_segment_index` bound. Changing the encoding must
/// bump this constant; silent Storekey dependency upgrades must not rewrite
/// persisted bytes without an intentional codec bump.
pub const CODEC_VERSION: i16 = 1;

/// Days from the Unix epoch (1970-01-01) to the PostgreSQL epoch (2000-01-01).
pub const PG_EPOCH_DAYS_FROM_UNIX: i32 = 10_957;

/// Microseconds from the Unix epoch to the PostgreSQL epoch.
pub const PG_EPOCH_MICROS_FROM_UNIX: i64 = 946_684_800_000_000;

/// Allowlisted PostgreSQL types for Sort Key V1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SortKeyType {
    /// `boolean` / OID 16.
    Bool,
    /// `smallint` / OID 21.
    Int2,
    /// `integer` / OID 23.
    Int4,
    /// `bigint` / OID 20.
    Int8,
    /// `date` / OID 1082 (days since PostgreSQL epoch).
    Date,
    /// `timestamp` / OID 1114 (µs since PostgreSQL epoch).
    Timestamp,
    /// `timestamptz` / OID 1184 (UTC µs since PostgreSQL epoch).
    Timestamptz,
    /// `uuid` / OID 2950.
    Uuid,
}

impl SortKeyType {
    /// Maps a PostgreSQL type OID to a Sort Key V1 type.
    #[must_use]
    pub const fn from_type_oid(type_oid: u32) -> Option<Self> {
        match type_oid {
            16 => Some(Self::Bool),
            21 => Some(Self::Int2),
            23 => Some(Self::Int4),
            20 => Some(Self::Int8),
            1082 => Some(Self::Date),
            1114 => Some(Self::Timestamp),
            1184 => Some(Self::Timestamptz),
            2950 => Some(Self::Uuid),
            _ => None,
        }
    }

    /// Returns the canonical PostgreSQL type OID for this sort-key type.
    #[must_use]
    pub const fn type_oid(self) -> u32 {
        match self {
            Self::Bool => 16,
            Self::Int2 => 21,
            Self::Int4 => 23,
            Self::Int8 => 20,
            Self::Date => 1082,
            Self::Timestamp => 1114,
            Self::Timestamptz => 1184,
            Self::Uuid => 2950,
        }
    }

    /// Returns true when this type may be used as `segment_order_column_id`.
    #[must_use]
    pub const fn is_order_column_supported(self) -> bool {
        true
    }
}

/// Canonical in-memory value before Storekey encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortKeyValue {
    /// Boolean.
    Bool(bool),
    /// Signed 16-bit integer.
    Int2(i16),
    /// Signed 32-bit integer.
    Int4(i32),
    /// Signed 64-bit integer.
    Int8(i64),
    /// PostgreSQL date as days since 2000-01-01.
    Date(i32),
    /// PostgreSQL timestamp as µs since 2000-01-01.
    Timestamp(i64),
    /// PostgreSQL timestamptz as UTC µs since 2000-01-01.
    Timestamptz(i64),
    /// UUID.
    Uuid(Uuid),
}

impl SortKeyValue {
    /// Returns the Sort Key V1 type for this value.
    #[must_use]
    pub const fn sort_key_type(&self) -> SortKeyType {
        match self {
            Self::Bool(_) => SortKeyType::Bool,
            Self::Int2(_) => SortKeyType::Int2,
            Self::Int4(_) => SortKeyType::Int4,
            Self::Int8(_) => SortKeyType::Int8,
            Self::Date(_) => SortKeyType::Date,
            Self::Timestamp(_) => SortKeyType::Timestamp,
            Self::Timestamptz(_) => SortKeyType::Timestamptz,
            Self::Uuid(_) => SortKeyType::Uuid,
        }
    }
}
