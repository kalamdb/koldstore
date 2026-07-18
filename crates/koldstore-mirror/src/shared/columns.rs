//! Mirror metadata column contract.

/// Clean-schema mirror metadata column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MirrorColumn {
    /// Monotonic latest-state sequence.
    Seq,
    /// Mirror operation code.
    Op,
    /// Captured WAL position, when available.
    CommitLsn,
}

impl MirrorColumn {
    /// All metadata columns in storage order.
    pub const ALL: [Self; 3] = [Self::Seq, Self::Op, Self::CommitLsn];

    /// Stable SQL column name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Seq => "seq",
            Self::Op => "op",
            Self::CommitLsn => "commit_lsn",
        }
    }

    /// Quoted SQL column name.
    #[must_use]
    pub fn quoted_name(self) -> String {
        format!("\"{}\"", self.name())
    }

    /// Quoted metadata column names for INSERT/SELECT lists.
    #[must_use]
    pub fn insert_quoted_names() -> [String; 3] {
        Self::ALL.map(Self::quoted_name)
    }

    /// Stable SQL column type and nullability fragment.
    #[must_use]
    pub const fn definition(self) -> &'static str {
        match self {
            Self::Seq => "\"seq\" bigint NOT NULL",
            Self::Op => "\"op\" smallint NOT NULL",
            Self::CommitLsn => "\"commit_lsn\" pg_lsn NULL",
        }
    }
}
