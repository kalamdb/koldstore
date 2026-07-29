//! Pure object-key join helpers (no object-store dependency).
//!
//! Template rendering stays in `koldstore-storage::PathTemplate`. These helpers
//! only normalize and join already-rendered prefixes with relative keys.

/// Normalizes a rendered table prefix to exactly one trailing slash.
#[must_use]
pub fn normalize_table_prefix(rendered: &str) -> String {
    let trimmed = rendered.trim_matches('/');
    if trimmed.is_empty() {
        String::new()
    } else {
        format!("{trimmed}/")
    }
}

/// Joins a relative object name below a table prefix.
#[must_use]
pub fn join_object_key(table_prefix: &str, relative: &str) -> String {
    let prefix = table_prefix.trim_matches('/');
    let relative = relative.trim_matches('/');
    match (prefix.is_empty(), relative.is_empty()) {
        (true, true) => String::new(),
        (true, false) => relative.to_string(),
        (false, true) => format!("{prefix}/"),
        (false, false) => format!("{prefix}/{relative}"),
    }
}

/// Returns the manifest object key below a table prefix.
#[must_use]
pub fn manifest_object_key(table_prefix: &str) -> String {
    join_object_key(table_prefix, "manifest.json")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_handles_empty_prefix_and_slashes() {
        assert_eq!(
            join_object_key("app/items/", "/001/a.parquet"),
            "app/items/001/a.parquet"
        );
        assert_eq!(join_object_key("", "manifest.json"), "manifest.json");
        assert_eq!(manifest_object_key("app/items"), "app/items/manifest.json");
        assert_eq!(normalize_table_prefix("/app/items/"), "app/items/");
    }
}
