//! Formats and parses PostgreSQL LSN text (`HI/LO`).
//!
//! Also owns typed WAL fence / apply-boundary wrappers used by async mirror
//! flush coordination (PostgreSQL-free).

/// WAL upper boundary known durable for a bounded decode pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WalFenceLsn(u64);

impl WalFenceLsn {
    /// Wraps a raw LSN value.
    #[must_use]
    pub const fn new(lsn: u64) -> Self {
        Self(lsn)
    }

    /// Returns the raw LSN.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Formats as PostgreSQL `HI/LO` text.
    #[must_use]
    pub fn format(self) -> String {
        format_pg_lsn(self.0)
    }

    /// Parses PostgreSQL `HI/LO` text.
    ///
    /// # Errors
    ///
    /// Returns an error when the text is not a valid LSN.
    pub fn parse(text: &str) -> Result<Self, String> {
        parse_pg_lsn(text).map(Self)
    }
}

/// Exact source transaction end-LSN applied in a flush/apply path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AppliedWalBoundary(u64);

impl AppliedWalBoundary {
    /// Wraps a raw LSN value.
    #[must_use]
    pub const fn new(lsn: u64) -> Self {
        Self(lsn)
    }

    /// Returns the raw LSN.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Formats as PostgreSQL `HI/LO` text.
    #[must_use]
    pub fn format(self) -> String {
        format_pg_lsn(self.0)
    }

    /// Parses PostgreSQL `HI/LO` text.
    ///
    /// # Errors
    ///
    /// Returns an error when the text is not a valid LSN.
    pub fn parse(text: &str) -> Result<Self, String> {
        parse_pg_lsn(text).map(Self)
    }
}

/// Formats a 64-bit LSN as the PostgreSQL text form `HI/LO` (uppercase hex).
#[must_use]
pub fn format_pg_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xffff_ffff)
}

/// Parses a PostgreSQL LSN text form `HI/LO` into a 64-bit value.
///
/// # Errors
///
/// Returns an error when the text is not two hex halves separated by `/`.
pub fn parse_pg_lsn(text: &str) -> Result<u64, String> {
    let (hi, lo) = text
        .split_once('/')
        .ok_or_else(|| format!("invalid pg_lsn text '{text}'"))?;
    let hi = u64::from_str_radix(hi, 16).map_err(|error| format!("invalid pg_lsn hi: {error}"))?;
    let lo = u64::from_str_radix(lo, 16).map_err(|error| format!("invalid pg_lsn lo: {error}"))?;
    if hi > u64::from(u32::MAX) || lo > u64::from(u32::MAX) {
        return Err(format!("pg_lsn half out of range in '{text}'"));
    }
    Ok((hi << 32) | lo)
}

#[cfg(test)]
mod tests {
    use super::{format_pg_lsn, parse_pg_lsn};

    #[test]
    fn formats_zero_and_split_halves() {
        assert_eq!(format_pg_lsn(0), "0/0");
        assert_eq!(format_pg_lsn(0x0000_0001_0000_0002), "1/2");
    }

    #[test]
    fn parse_round_trips() {
        assert_eq!(parse_pg_lsn("0/0").unwrap(), 0);
        assert_eq!(parse_pg_lsn("1/2").unwrap(), 0x0000_0001_0000_0002);
        assert_eq!(
            parse_pg_lsn(&format_pg_lsn(0xABCD_EF01_2345_6789)).unwrap(),
            0xABCD_EF01_2345_6789
        );
    }
}
