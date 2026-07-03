//! Hot/cold merge resolver and change-feed helpers.

pub mod changelog;
pub mod quals;
pub mod resolver;
pub mod tombstone;

pub use changelog::{changes_since, ChangeCursor, ChangeGap};
pub use quals::{classify_predicates, ClassifiedPredicates};
pub use resolver::{resolve_rows, ResolvedRow, RowSource};
pub use tombstone::{tombstone_required, TombstoneDecision};
