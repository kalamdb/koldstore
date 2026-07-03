//! Error and diagnostic types shared by pg-koldstore crates.

use thiserror::Error;

/// Convenient result alias for pure pg-koldstore crates.
pub type Result<T> = std::result::Result<T, KoldstoreError>;

/// Structured diagnostic intended for SQL DETAIL/HINT translation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnostic {
    /// Stable machine-readable code.
    pub code: &'static str,
    /// Human-readable detail.
    pub detail: String,
    /// Optional operator hint.
    pub hint: Option<String>,
}

impl Diagnostic {
    /// Creates a diagnostic without a hint.
    #[must_use]
    pub fn new(code: &'static str, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
            hint: None,
        }
    }

    /// Adds an operator hint.
    #[must_use]
    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }
}

/// Typed errors for pure pg-koldstore logic.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum KoldstoreError {
    /// A sequence-like value was invalid.
    #[error("invalid sequence value for {field}: {value}")]
    InvalidSequence { field: &'static str, value: i64 },

    /// A table kind string was not recognized.
    #[error("unsupported table kind: {0}")]
    UnsupportedTableKind(String),

    /// A SQL identifier-like value was invalid.
    #[error("invalid {kind}: {value}")]
    InvalidIdentifier {
        /// Identifier kind.
        kind: &'static str,
        /// Rejected value.
        value: String,
    },

    /// A primary-key definition or value set is invalid.
    #[error("invalid primary key: {0}")]
    InvalidPrimaryKey(String),

    /// A predicate cannot safely be pushed below merge resolution.
    #[error("unsafe predicate pushdown: {0}")]
    UnsafePredicate(String),

    /// A manifest or catalog state transition is invalid.
    #[error("invalid state transition from {from} to {to}")]
    InvalidStateTransition { from: String, to: String },

    /// A catalog object failed validation.
    #[error("catalog validation failed")]
    CatalogValidation { diagnostic: Diagnostic },

    /// JSON serialization or parsing failed.
    #[error("json error: {0}")]
    Json(String),
}

impl From<serde_json::Error> for KoldstoreError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value.to_string())
    }
}
