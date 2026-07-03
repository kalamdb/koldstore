//! DML hook and system-column guard integration.

/// Returns whether a column is managed system metadata.
#[must_use]
pub fn is_system_column(name: &str) -> bool {
    matches!(name, "_seq" | "_commit_seq" | "_deleted" | "_user_id")
}

/// Returns whether a user write to a system column should be rejected.
#[must_use]
pub fn rejects_system_column_write(name: &str, internal_guard_active: bool) -> bool {
    is_system_column(name) && !internal_guard_active
}

/// DML operations observed by the managed hook shell.
#[must_use]
pub const fn managed_dml_hook_names() -> &'static [&'static str] {
    &["INSERT", "UPDATE", "DELETE", "COPY"]
}

/// Returns whether standard SQL cold-only DELETE can use the exact local metadata path.
#[must_use]
pub const fn simple_pk_delete_supported(simple_pk_predicate: bool, exact_metadata: bool) -> bool {
    simple_pk_predicate && exact_metadata
}
