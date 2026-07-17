//! Stable worker identity strings for activity probes and registration.

/// Database OID passed from the PostgreSQL adapter (not a `pg_sys` type).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DatabaseOid(u32);

impl DatabaseOid {
    /// Wraps a raw PostgreSQL database OID value.
    #[must_use]
    pub const fn new(oid: u32) -> Self {
        Self(oid)
    }

    /// Returns the raw OID.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Backend type / bgworker name for the async mirror WAL applier.
///
/// Kept as `koldstore async mirror {oid}` so existing e2e and storage probes
/// keep working. A future rename to a generic `koldstore db worker` is fine
/// once flush jobs share the same process and all probes are updated together.
#[must_use]
pub fn async_mirror_worker_type(database_oid: DatabaseOid) -> String {
    format!("koldstore async mirror {}", database_oid.get())
}

#[cfg(test)]
mod tests {
    use super::{async_mirror_worker_type, DatabaseOid};

    #[test]
    fn worker_type_is_stable_for_oid() {
        assert_eq!(
            async_mirror_worker_type(DatabaseOid::new(42)),
            "koldstore async mirror 42"
        );
    }
}
