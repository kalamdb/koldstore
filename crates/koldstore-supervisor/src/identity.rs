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

/// Parses the database OID suffix from a KoldStore worker `backend_type`.
///
/// Worker names embed the OID (`koldstore wal applier 12345`). Prefer this over
/// `pg_stat_activity.datid`, which can be NULL while a background worker is
/// still starting (PostgreSQL 18 initializes `st_databaseid` to InvalidOid until
/// `pgstat_bestart`).
#[must_use]
pub fn database_oid_from_worker_backend_type(backend_type: &str) -> Option<u32> {
    let oid = backend_type.rsplit_once(' ').map(|(_, suffix)| suffix)?;
    let parsed = oid.parse::<u32>().ok()?;
    (parsed != 0).then_some(parsed)
}

#[cfg(test)]
mod tests {
    use super::{
        database_oid_from_worker_backend_type, flush_executor_worker_type, maintenance_worker_type,
        DatabaseOid,
    };

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

    #[test]
    fn database_oid_from_worker_backend_type_reads_suffix() {
        assert_eq!(
            database_oid_from_worker_backend_type("koldstore wal applier 8285"),
            Some(8285)
        );
        assert_eq!(
            database_oid_from_worker_backend_type("koldstore maintenance 42"),
            Some(42)
        );
        assert_eq!(
            database_oid_from_worker_backend_type("koldstore flush executor 7"),
            Some(7)
        );
        assert_eq!(
            database_oid_from_worker_backend_type("client backend"),
            None
        );
        assert_eq!(
            database_oid_from_worker_backend_type("koldstore wal applier 0"),
            None
        );
    }
}
