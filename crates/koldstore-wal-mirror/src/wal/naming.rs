//! Stable naming for database-scoped WAL capture infrastructure.
//!
//! These strings are the durable identity of the publication, logical slot, and
//! flush-origin markers. PostgreSQL catalog creation stays in `pg_koldstore`.

/// Publication shared by async managed tables in one database.
pub const PUBLICATION_NAME: &str = "koldstore_async_mirror";

/// Prefix for flush-prune replication origins stamped on async cleanup WAL.
const FLUSH_REPLICATION_ORIGIN_PREFIX: &str = "koldstore_flush";

/// Returns the cluster-unique logical slot name for a database OID.
#[must_use]
pub fn slot_name(database_oid: u32) -> String {
    format!("koldstore_async_{database_oid}")
}

/// Returns the database-scoped flush replication origin name (PG15 prune path).
#[must_use]
pub fn flush_replication_origin_name(database_oid: u32) -> String {
    format!("{FLUSH_REPLICATION_ORIGIN_PREFIX}_{database_oid}")
}

/// Returns true when `name` is a flush-prune origin for `database_oid`.
#[must_use]
pub fn is_flush_replication_origin(name: &str, database_oid: u32) -> bool {
    name == flush_replication_origin_name(database_oid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_database_scoped_and_stable() {
        assert_eq!(slot_name(42), "koldstore_async_42");
        assert_eq!(flush_replication_origin_name(42), "koldstore_flush_42");
        assert!(is_flush_replication_origin("koldstore_flush_42", 42));
        assert!(!is_flush_replication_origin("koldstore_flush_43", 42));
        assert_eq!(PUBLICATION_NAME, "koldstore_async_mirror");
    }
}
