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

/// Backend type / bgworker name for the ephemeral database-maintenance worker.
#[must_use]
pub fn maintenance_worker_type(database_oid: DatabaseOid) -> String {
    format!("koldstore maintenance {}", database_oid.get())
}

/// Backend type / bgworker name for a one-shot flush executor.
#[must_use]
pub fn flush_executor_worker_type(database_oid: DatabaseOid) -> String {
    format!("koldstore flush executor {}", database_oid.get())
}

#[cfg(test)]
mod tests {
    use super::{flush_executor_worker_type, maintenance_worker_type, DatabaseOid};

    #[test]
    fn maintenance_worker_type_is_stable_for_oid() {
        assert_eq!(
            maintenance_worker_type(DatabaseOid::new(42)),
            "koldstore maintenance 42"
        );
    }

    #[test]
    fn flush_executor_worker_type_is_stable_for_oid() {
        assert_eq!(
            flush_executor_worker_type(DatabaseOid::new(42)),
            "koldstore flush executor 42"
        );
    }
}
