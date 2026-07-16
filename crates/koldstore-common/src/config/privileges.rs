//! Privilege policy helpers for GUC and role-class checks.
//!
//! Pure policy only — PostgreSQL role resolution stays in `pg_koldstore`.

/// Internal GUC names protected from application roles.
pub const INTERNAL_GUCS: &[&str] = &[
    "koldstore.internal_system_write",
    "koldstore.internal_flush_cleanup",
    "koldstore.internal_async_mirror_worker",
];

/// Role class used by privilege checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoleClass {
    /// Normal application role.
    Application,
    /// Extension administrator.
    Admin,
    /// PostgreSQL superuser.
    Superuser,
}

/// Returns whether a role class can set a GUC.
#[must_use]
pub fn can_set_guc(role: RoleClass, guc_name: &str) -> bool {
    !INTERNAL_GUCS.contains(&guc_name) || matches!(role, RoleClass::Admin | RoleClass::Superuser)
}

#[cfg(test)]
mod tests {
    use super::{can_set_guc, RoleClass};

    #[test]
    fn application_cannot_set_internal_gucs() {
        assert!(!can_set_guc(
            RoleClass::Application,
            "koldstore.internal_system_write"
        ));
        assert!(can_set_guc(
            RoleClass::Superuser,
            "koldstore.internal_system_write"
        ));
        assert!(can_set_guc(RoleClass::Application, "koldstore.user_id"));
    }
}
