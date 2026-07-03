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
