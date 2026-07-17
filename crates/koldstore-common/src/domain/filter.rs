//! Predicate classification for safe cold pruning.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{KoldstoreError, Result};

/// Column category used to decide predicate pushdown safety.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnClass {
    /// Logical primary-key column.
    PrimaryKey,
    /// Scope column or application user-id column.
    Scope,
    /// Mirror/cold `seq` metadata column.
    Seq,
    /// Commit-order cursor used during hot/cold merge.
    CommitSeq,
    /// Immutable or stats-only column recorded safe by schema metadata.
    Immutable,
    /// Mutable application column.
    Mutable,
    /// RLS/security qual.
    Security,
}

/// Predicate value shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PredicateValue {
    /// Equality.
    Eq(Value),
    /// Set membership.
    In(Vec<Value>),
    /// Inclusive integer range.
    Range { min: i64, max: i64 },
    /// Arbitrary expression.
    Expression(String),
}

/// A simplified predicate description from PostgreSQL quals.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Predicate {
    /// Column name.
    pub column: String,
    /// Column class.
    pub class: ColumnClass,
    /// Predicate value.
    pub value: PredicateValue,
}

/// Safe-pruning classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PredicateClass {
    /// May be used before hot/cold merge.
    SafePrune,
    /// Must be evaluated after winner resolution.
    Residual,
    /// Security predicate; enforce or fail closed.
    Security,
}

impl Predicate {
    /// Classifies predicate pushdown safety.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed range predicates.
    pub fn classify(&self) -> Result<PredicateClass> {
        if let PredicateValue::Range { min, max } = self.value {
            if min > max {
                return Err(KoldstoreError::UnsafePredicate(format!(
                    "range minimum exceeds maximum for {}",
                    self.column
                )));
            }
        }

        Ok(match self.class {
            ColumnClass::PrimaryKey
            | ColumnClass::Scope
            | ColumnClass::Seq
            | ColumnClass::CommitSeq
            | ColumnClass::Immutable => PredicateClass::SafePrune,
            ColumnClass::Mutable => PredicateClass::Residual,
            ColumnClass::Security => PredicateClass::Security,
        })
    }
}
