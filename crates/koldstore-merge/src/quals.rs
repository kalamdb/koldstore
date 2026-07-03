//! Qual classification helpers.

use koldstore_core::{ColumnClass, Predicate, PredicateClass, PredicateValue, Result};

/// Inclusive integer range extracted for safe row-group pruning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PruningRange {
    /// Column name.
    pub column: String,
    /// Inclusive minimum.
    pub min: i64,
    /// Inclusive maximum.
    pub max: i64,
}

/// Safe segment and row-group pruning plan.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PruningPlan {
    /// PK columns with equality or IN filters.
    pub pk_columns: Vec<String>,
    /// Scope columns with equality filters.
    pub scope_columns: Vec<String>,
    /// Optional `_seq` range.
    pub seq_range: Option<PruningRange>,
    /// Optional `_commit_seq` range.
    pub commit_seq_range: Option<PruningRange>,
    /// Immutable/stat-only columns safe for pre-merge pruning.
    pub immutable_stat_columns: Vec<String>,
    /// Columns that must remain post-merge residual filters.
    pub residual_columns: Vec<String>,
}

/// Classified predicates.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ClassifiedPredicates {
    pub safe: Vec<Predicate>,
    pub residual: Vec<Predicate>,
    pub security: Vec<Predicate>,
}

impl ClassifiedPredicates {
    /// Returns true when PostgreSQL must evaluate expressions after winner resolution.
    #[must_use]
    pub fn requires_post_merge_filtering(&self) -> bool {
        !self.residual.is_empty() || !self.security.is_empty()
    }

    /// Returns safe-pruning column names in classification order.
    #[must_use]
    pub fn safe_pruning_columns(&self) -> Vec<String> {
        self.safe
            .iter()
            .map(|predicate| predicate.column.clone())
            .collect()
    }
}

/// Classifies predicates by pushdown safety.
pub fn classify_predicates(predicates: &[Predicate]) -> Result<ClassifiedPredicates> {
    let mut classified = ClassifiedPredicates::default();
    for predicate in predicates {
        match predicate.classify()? {
            PredicateClass::SafePrune => classified.safe.push(predicate.clone()),
            PredicateClass::Residual => classified.residual.push(predicate.clone()),
            PredicateClass::Security => classified.security.push(predicate.clone()),
        }
    }
    Ok(classified)
}

/// Builds a safe segment/row-group pruning plan from predicates.
///
/// # Errors
///
/// Returns an error if any predicate is malformed, such as an inverted range.
pub fn build_pruning_plan(predicates: &[Predicate]) -> Result<PruningPlan> {
    let classified = classify_predicates(predicates)?;
    let mut plan = PruningPlan {
        residual_columns: classified
            .residual
            .iter()
            .chain(classified.security.iter())
            .map(|predicate| predicate.column.clone())
            .collect(),
        ..PruningPlan::default()
    };

    for predicate in classified.safe {
        match predicate.class {
            ColumnClass::PrimaryKey => plan.pk_columns.push(predicate.column),
            ColumnClass::Scope => plan.scope_columns.push(predicate.column),
            ColumnClass::Seq => {
                plan.seq_range = range_from_predicate(&predicate);
            }
            ColumnClass::CommitSeq => {
                plan.commit_seq_range = range_from_predicate(&predicate);
            }
            ColumnClass::Immutable => plan.immutable_stat_columns.push(predicate.column),
            ColumnClass::Mutable | ColumnClass::Security => {}
        }
    }

    Ok(plan)
}

fn range_from_predicate(predicate: &Predicate) -> Option<PruningRange> {
    match predicate.value {
        PredicateValue::Range { min, max } => Some(PruningRange {
            column: predicate.column.clone(),
            min,
            max,
        }),
        _ => None,
    }
}
