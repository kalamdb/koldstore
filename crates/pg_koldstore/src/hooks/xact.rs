//! Transaction-scoped helpers for mirror capture coupling.

/// Transaction coupling mode for mirror capture.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MirrorCaptureTransactionScope {
    /// Mirror mutation executes as an ordinary row trigger in the user's transaction.
    SameUserTransaction,
}

/// Returns the clean-schema mirror capture transaction scope.
#[must_use]
pub const fn mirror_capture_transaction_scope() -> MirrorCaptureTransactionScope {
    MirrorCaptureTransactionScope::SameUserTransaction
}

/// Returns whether a capture scope rolls back with the user DML.
#[must_use]
pub const fn mirror_capture_rolls_back_with_user_transaction(
    scope: MirrorCaptureTransactionScope,
) -> bool {
    matches!(scope, MirrorCaptureTransactionScope::SameUserTransaction)
}
