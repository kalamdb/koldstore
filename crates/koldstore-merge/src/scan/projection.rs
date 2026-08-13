//! Cold projection planning for ordered compete-then-body merge reads.

use koldstore_common::ColumnRef;

/// Narrow compete columns and deferred body columns for an ordered cold read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColdProjectionPlan {
    compete: Vec<ColumnRef>,
    body: Vec<ColumnRef>,
}

impl ColdProjectionPlan {
    /// Plans late materialization for one full projection.
    ///
    /// Returns `None` when compete already covers the full projection and a
    /// second body open would add overhead without reducing column I/O.
    #[must_use]
    pub fn for_ordered(
        full: &[ColumnRef],
        primary_key: &[ColumnRef],
        leading: &ColumnRef,
    ) -> Option<Self> {
        let mut compete = Vec::with_capacity(primary_key.len().saturating_add(1));
        compete.extend_from_slice(primary_key);
        compete.push(leading.clone());
        compete.sort_by_key(|column| column.column_id);
        compete.dedup_by_key(|column| column.column_id);

        let body = full
            .iter()
            .filter(|column| {
                compete
                    .binary_search_by_key(&column.column_id, |candidate| candidate.column_id)
                    .is_err()
            })
            .cloned()
            .collect::<Vec<_>>();

        if body.is_empty() || compete.len() >= full.len() {
            return None;
        }
        Some(Self { compete, body })
    }

    /// Order key plus primary-key columns used to select winners.
    #[must_use]
    pub fn compete(&self) -> &[ColumnRef] {
        &self.compete
    }

    /// Deferred application columns not needed for winner competition.
    #[must_use]
    pub fn body(&self) -> &[ColumnRef] {
        &self.body
    }

    /// Builds the body read projection with primary keys for winner hydration.
    #[must_use]
    pub fn body_with_primary_key(&self, primary_key: &[ColumnRef]) -> Vec<ColumnRef> {
        let mut columns = self.body.clone();
        for pk in primary_key {
            if !columns
                .iter()
                .any(|column| column.column_id == pk.column_id)
            {
                columns.push(pk.clone());
            }
        }
        columns
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use koldstore_common::ColumnId;

    fn column(id: i16, name: &str) -> ColumnRef {
        ColumnRef::new(ColumnId::from_attnum(id), name)
    }

    #[test]
    fn ordered_projection_deduplicates_pk_and_defers_body_in_full_order() {
        let id = column(1, "id");
        let tenant = column(2, "tenant_id");
        let created = column(3, "created_at");
        let body = column(4, "body");
        let status = column(5, "status");
        let full = vec![
            status.clone(),
            id.clone(),
            body.clone(),
            created.clone(),
            tenant.clone(),
        ];

        let plan = ColdProjectionPlan::for_ordered(&full, &[tenant.clone(), id.clone()], &created)
            .expect("wide projection uses late materialization");

        assert_eq!(plan.compete(), &[id.clone(), tenant.clone(), created]);
        assert_eq!(plan.body(), &[status.clone(), body.clone()]);
        assert_eq!(
            plan.body_with_primary_key(&[tenant.clone(), id.clone()]),
            vec![status, body, tenant, id]
        );
    }

    #[test]
    fn narrow_projection_uses_one_full_open() {
        let id = column(1, "id");

        assert!(ColdProjectionPlan::for_ordered(
            std::slice::from_ref(&id),
            std::slice::from_ref(&id),
            &id,
        )
        .is_none());
    }
}
