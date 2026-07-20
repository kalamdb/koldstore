//! Feature-pack selection for the stress harness.

use std::collections::BTreeSet;

use anyhow::{bail, Result};

/// Optional soak packs beyond the always-on chat core.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Pack {
    /// Always implied; wide chat messages + history + flush.
    Chat,
    /// Cold UPDATE/DELETE overlays on old message ids.
    ColdDml,
    /// Sibling managed tables + multi-relation flush.
    MultiTable,
    /// Hot+cold join readers.
    Joins,
    /// Force async mirror capture mode for the soak.
    Async,
}

impl Pack {
    fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "chat" => Ok(Self::Chat),
            "cold_dml" | "cold-dml" => Ok(Self::ColdDml),
            "multi_table" | "multi-table" => Ok(Self::MultiTable),
            "joins" | "join" => Ok(Self::Joins),
            "async" => Ok(Self::Async),
            "schema_evo" | "scheduler" | "s3" => bail!(
                "pack {raw:?} is not implemented in v1 (see design doc later packs)"
            ),
            other => bail!("unknown stress pack {other:?}"),
        }
    }

    /// Stable name for reports and env dumps.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Chat => "chat",
            Self::ColdDml => "cold_dml",
            Self::MultiTable => "multi_table",
            Self::Joins => "joins",
            Self::Async => "async",
        }
    }
}

/// Enabled pack set (`chat` always present).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackSet {
    packs: BTreeSet<Pack>,
}

impl PackSet {
    /// Parses `KOLDSTORE_STRESS_PACKS` (comma-separated). Empty ⇒ `chat` only.
    ///
    /// # Errors
    ///
    /// Returns an error for unknown or not-yet-implemented pack names.
    pub fn from_env() -> Result<Self> {
        let raw = std::env::var("KOLDSTORE_STRESS_PACKS").unwrap_or_default();
        Self::parse(&raw)
    }

    /// Parses a comma-separated pack list.
    ///
    /// # Errors
    ///
    /// Returns an error for unknown pack names.
    pub fn parse(raw: &str) -> Result<Self> {
        let mut packs = BTreeSet::new();
        packs.insert(Pack::Chat);
        for part in raw.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            packs.insert(Pack::parse(part)?);
        }
        // Joins need sibling tables.
        if packs.contains(&Pack::Joins) {
            packs.insert(Pack::MultiTable);
        }
        Ok(Self { packs })
    }

    #[must_use]
    pub fn contains(&self, pack: Pack) -> bool {
        self.packs.contains(&pack)
    }

    #[must_use]
    pub fn cold_dml(&self) -> bool {
        self.contains(Pack::ColdDml)
    }

    #[must_use]
    pub fn multi_table(&self) -> bool {
        self.contains(Pack::MultiTable)
    }

    #[must_use]
    pub fn joins(&self) -> bool {
        self.contains(Pack::Joins)
    }

    #[must_use]
    pub fn async_mirror(&self) -> bool {
        self.contains(Pack::Async)
    }

    /// Sorted pack names for logging / reports.
    #[must_use]
    pub fn names(&self) -> Vec<&'static str> {
        self.packs.iter().map(|p| p.as_str()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_defaults_to_chat() {
        let set = PackSet::parse("").unwrap();
        assert_eq!(set.names(), vec!["chat"]);
    }

    #[test]
    fn joins_implies_multi_table() {
        let set = PackSet::parse("joins").unwrap();
        assert!(set.joins());
        assert!(set.multi_table());
    }

    #[test]
    fn rejects_later_packs() {
        assert!(PackSet::parse("s3").is_err());
    }
}
