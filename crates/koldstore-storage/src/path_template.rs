//! Object path templates.

/// Path template using pg-koldstore placeholder names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathTemplate {
    template: String,
}

impl PathTemplate {
    /// Creates a template without validation.
    #[must_use]
    pub fn new(template: impl Into<String>) -> Self {
        Self {
            template: template.into(),
        }
    }

    /// Returns the raw template.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.template
    }

    /// Renders the template for a namespace/table and optional scope.
    ///
    /// # Errors
    ///
    /// Returns an error when `{scopeId}` is required but no scope is supplied.
    pub fn render(
        &self,
        namespace: &str,
        table_name: &str,
        scope_id: Option<&str>,
    ) -> Result<String, String> {
        if self.template.contains("{scopeId}") && scope_id.is_none() {
            return Err("scopeId is required by path template".to_string());
        }
        let rendered = self
            .template
            .replace("{namespace}", namespace)
            .replace("{tableName}", table_name)
            .replace("{scopeId}", scope_id.unwrap_or(""));
        if rendered.contains('{') || rendered.contains('}') {
            return Err("path template contains unresolved placeholders".to_string());
        }
        Ok(rendered)
    }
}

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

/// Renders and normalizes a regular table prefix.
///
/// # Errors
///
/// Returns the template rendering error when placeholders are invalid.
pub fn render_regular_table_prefix(
    template: &PathTemplate,
    namespace: &str,
    table_name: &str,
) -> Result<String, String> {
    template
        .render(namespace, table_name, None)
        .map(|rendered| normalize_table_prefix(&rendered))
}
