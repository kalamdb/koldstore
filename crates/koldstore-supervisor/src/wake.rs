//! Transaction-local dirty tracking for post-commit supervisor publication.

/// Backend-local dirty state that follows PostgreSQL subtransaction outcomes.
///
/// Only the earliest nesting level containing relevant work is needed.
/// Committing a savepoint promotes the state to its parent; aborting that level
/// clears work that existed only inside the aborted subtransaction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransactionDirty {
    earliest_level: u32,
}

impl TransactionDirty {
    /// Marks work at `nesting_level`.
    pub const fn mark(&mut self, nesting_level: u32) {
        let level = if nesting_level == 0 { 1 } else { nesting_level };
        if self.earliest_level == 0 || level < self.earliest_level {
            self.earliest_level = level;
        }
    }

    /// Promotes dirty work committed by a subtransaction into its parent.
    pub const fn commit_subtransaction(&mut self, nesting_level: u32) {
        if self.earliest_level >= nesting_level && nesting_level > 1 {
            self.earliest_level = nesting_level - 1;
        }
    }

    /// Discards dirty work whose owning subtransaction aborted.
    pub const fn abort_subtransaction(&mut self, nesting_level: u32) {
        if self.earliest_level >= nesting_level {
            self.earliest_level = 0;
        }
    }

    /// Clears and returns whether the top-level transaction contained work.
    pub const fn take(&mut self) -> bool {
        let dirty = self.earliest_level != 0;
        self.earliest_level = 0;
        dirty
    }

    /// Clears all transaction-local state.
    pub const fn clear(&mut self) {
        self.earliest_level = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::TransactionDirty;

    #[test]
    fn aborted_savepoint_does_not_publish_false_work() {
        let mut dirty = TransactionDirty::default();
        dirty.mark(2);
        dirty.abort_subtransaction(2);
        assert!(!dirty.take());
    }

    #[test]
    fn committed_savepoint_promotes_work_to_parent() {
        let mut dirty = TransactionDirty::default();
        dirty.mark(3);
        dirty.commit_subtransaction(3);
        dirty.commit_subtransaction(2);
        assert!(dirty.take());
        assert!(!dirty.take());
    }

    #[test]
    fn inner_abort_preserves_outer_work() {
        let mut dirty = TransactionDirty::default();
        dirty.mark(1);
        dirty.mark(2);
        dirty.abort_subtransaction(2);
        assert!(dirty.take());
    }
}
