//! RLS classification and fail-closed boundaries for merged hot/cold reads.

use koldstore_common::{ColumnClass, Predicate, PredicateValue};

/// Returns the fail-closed error message for unsupported cold RLS.
#[must_use]
pub const fn unsupported_rls_error() -> &'static str {
    "koldstore cannot enforce this RLS policy on cold rows"
}

/// Plan for enforcing security quals after hot/cold winner resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecurityQualPlan {
    /// Whether all supplied security quals can be enforced.
    pub can_enforce: bool,
    /// Projection columns needed for output and security evaluation.
    pub required_projection: Vec<String>,
}

/// Enforces an RLS/security qual or fails closed.
///
/// # Errors
///
/// Returns an error when the qual cannot be enforced on cold rows.
pub fn enforce_or_fail_closed(can_enforce: bool) -> Result<(), &'static str> {
    if can_enforce {
        Ok(())
    } else {
        Err(unsupported_rls_error())
    }
}

/// Builds the required projection for cold-row RLS/security evaluation.
///
/// # Errors
///
/// Returns [`unsupported_rls_error`] for arbitrary security expressions that
/// cannot be evaluated from projected cold row columns.
pub fn plan_security_quals(
    security_quals: &[Predicate],
    base_projection: &[String],
) -> Result<SecurityQualPlan, &'static str> {
    let mut required_projection = base_projection.to_vec();
    for qual in security_quals {
        if qual.class != ColumnClass::Security {
            continue;
        }
        if matches!(qual.value, PredicateValue::Expression(_)) {
            return Err(unsupported_rls_error());
        }
        if !required_projection
            .iter()
            .any(|column| column == &qual.column)
        {
            required_projection.push(qual.column.clone());
        }
    }
    Ok(SecurityQualPlan {
        can_enforce: true,
        required_projection,
    })
}
