//! Privilege checks.

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
