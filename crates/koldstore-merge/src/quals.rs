//! Qual classification helpers.

use koldstore_core::{Predicate, PredicateClass, Result};

/// Classified predicates.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ClassifiedPredicates {
    pub safe: Vec<Predicate>,
    pub residual: Vec<Predicate>,
    pub security: Vec<Predicate>,
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
