//! Extension bootstrap DDL loaded from the canonical install script.
//!
//! The setup crate owns the typed view of bootstrap objects. The PostgreSQL
//! extension still ships an install SQL file for pgrx, but tests can validate
//! that file against this typed plan instead of scattering raw string checks.

use std::path::Path;

/// Kind of extension-owned object created during bootstrap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapObjectKind {
    /// The extension catalog schema.
    Schema,
    /// Schema usage privilege.
    Grant,
    /// Composite SQL type returned by public functions.
    CompositeType,
    /// Internal catalog table.
    Table,
    /// Internal catalog index.
    Index,
    /// Monotonic sequence used by row/change ordering.
    Sequence,
    /// Public table access revocation.
    Revoke,
    /// Statement outside the modeled catalog object set.
    Other,
}

/// One modeled bootstrap statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapObjectPlan {
    /// Object kind inferred from the statement.
    pub kind: BootstrapObjectKind,
    /// Schema-qualified object name where the statement creates a named object.
    pub name: Option<String>,
    /// Secondary object target, such as an index table.
    pub target: Option<String>,
    /// SQL statement without the terminating semicolon.
    pub statement: String,
}

/// Full bootstrap install plan for the `koldstore` schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapPlan {
    /// SQL statements in dependency order.
    pub statements: Vec<String>,
    /// Typed statements in dependency order.
    pub objects: Vec<BootstrapObjectPlan>,
}

impl BootstrapPlan {
    /// Loads bootstrap statements from a SQL migration file.
    ///
    /// Statements are split on semicolons while preserving order.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read.
    pub fn from_sql_file(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let sql = std::fs::read_to_string(path)?;
        Ok(Self::from_sql(&sql))
    }

    /// Parses bootstrap statements from SQL text.
    #[must_use]
    pub fn from_sql(sql: &str) -> Self {
        let uncommented = strip_line_comments(sql);
        let statements = uncommented
            .split(';')
            .map(str::trim)
            .filter(|statement| !statement.is_empty())
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let objects = statements
            .iter()
            .map(|statement| classify_statement(statement))
            .collect();
        Self {
            statements,
            objects,
        }
    }

    /// Returns whether the plan contains a named object of the requested kind.
    #[must_use]
    pub fn contains_object(&self, kind: BootstrapObjectKind, name: &str) -> bool {
        self.objects
            .iter()
            .any(|object| object.kind == kind && object.name.as_deref() == Some(name))
    }

    /// Returns modeled objects of one kind in install order.
    #[must_use]
    pub fn objects_by_kind(&self, kind: BootstrapObjectKind) -> Vec<&BootstrapObjectPlan> {
        self.objects
            .iter()
            .filter(|object| object.kind == kind)
            .collect()
    }

    /// Returns repeated named objects, which usually indicate duplicate DDL.
    #[must_use]
    pub fn duplicate_object_names(&self) -> Vec<String> {
        let mut names = self
            .objects
            .iter()
            .filter_map(|object| {
                object
                    .name
                    .as_deref()
                    .map(|name| (format!("{:?}", object.kind), name.to_string()))
            })
            .collect::<Vec<_>>();
        names.sort();

        let mut duplicates = Vec::new();
        for window in names.windows(2) {
            if window[0] == window[1] && duplicates.last() != Some(&window[0]) {
                duplicates.push(window[0].clone());
            }
        }
        duplicates
            .into_iter()
            .map(|(_, name)| name)
            .collect::<Vec<_>>()
    }
}

fn strip_line_comments(sql: &str) -> String {
    sql.lines()
        .filter_map(|line| {
            let line = line.split_once("--").map_or(line, |(before, _)| before);
            let trimmed = line.trim();
            (!trimmed.is_empty()).then_some(trimmed)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn classify_statement(statement: &str) -> BootstrapObjectPlan {
    let normalized = statement.split_whitespace().collect::<Vec<_>>().join(" ");
    let lower = normalized.to_ascii_lowercase();

    let (kind, name, target) = if let Some(name) =
        object_after_prefix(&normalized, "CREATE SCHEMA IF NOT EXISTS ")
    {
        (BootstrapObjectKind::Schema, Some(name), None)
    } else if lower.starts_with("grant usage on schema ") {
        (
            BootstrapObjectKind::Grant,
            object_after_prefix(&normalized, "GRANT USAGE ON SCHEMA "),
            None,
        )
    } else if let Some(name) = object_after_prefix(&normalized, "CREATE TYPE ") {
        (BootstrapObjectKind::CompositeType, Some(name), None)
    } else if let Some(name) = object_after_prefix(&normalized, "CREATE TABLE IF NOT EXISTS ") {
        (BootstrapObjectKind::Table, Some(name), None)
    } else if let Some(name) =
        object_after_prefix(&normalized, "CREATE UNIQUE INDEX IF NOT EXISTS ")
    {
        (
            BootstrapObjectKind::Index,
            Some(name),
            index_target(&normalized),
        )
    } else if let Some(name) = object_after_prefix(&normalized, "CREATE INDEX IF NOT EXISTS ") {
        (
            BootstrapObjectKind::Index,
            Some(name),
            index_target(&normalized),
        )
    } else if let Some(name) = object_after_prefix(&normalized, "CREATE SEQUENCE IF NOT EXISTS ") {
        (BootstrapObjectKind::Sequence, Some(name), None)
    } else if let Some(name) = object_after_prefix(&normalized, "REVOKE ALL ON FUNCTION ") {
        (BootstrapObjectKind::Revoke, Some(name), None)
    } else if lower.starts_with("revoke all on ") {
        // Batch catalog-table revoke; keep a stable synthetic name for plan checks.
        (
            BootstrapObjectKind::Revoke,
            Some("koldstore.catalog_table_access".to_string()),
            None,
        )
    } else {
        (BootstrapObjectKind::Other, None, None)
    };

    BootstrapObjectPlan {
        kind,
        name,
        target,
        statement: statement.to_string(),
    }
}

fn object_after_prefix(statement: &str, prefix: &str) -> Option<String> {
    statement.strip_prefix(prefix).map(|remaining| {
        remaining
            .split(|character: char| character.is_whitespace() || character == '(')
            .next()
            .unwrap_or_default()
            .to_string()
    })
}

fn index_target(statement: &str) -> Option<String> {
    statement.split(" ON ").nth(1).map(|remaining| {
        remaining
            .split(|character: char| character.is_whitespace() || character == '(')
            .next()
            .unwrap_or_default()
            .to_string()
    })
}
