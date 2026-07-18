//! Mirror metadata column contract.

/// Clean-schema mirror metadata column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MirrorColumn {
    /// Monotonic latest-state sequence.
    Seq,
    /// Mirror operation code.
    Op,
}

impl MirrorColumn {
    /// All metadata columns in storage order.
    pub const ALL: [Self; 2] = [Self::Seq, Self::Op];

    /// Stable SQL column name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Seq => "seq",
            Self::Op => "op",
        }
    }

    /// Quoted SQL column name.
    #[must_use]
    pub fn quoted_name(self) -> String {
        format!("\"{}\"", self.name())
    }

    /// Quoted metadata column names for INSERT/SELECT lists.
    #[must_use]
    pub fn insert_quoted_names() -> [String; 2] {
        Self::ALL.map(Self::quoted_name)
    }

    /// Stable SQL column type and nullability fragment.
    #[must_use]
    pub const fn definition(self) -> &'static str {
        match self {
            Self::Seq => "\"seq\" bigint NOT NULL",
            Self::Op => "\"op\" smallint NOT NULL",
        }
    }
}
