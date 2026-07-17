//! PostgreSQL LSN formatting helpers (PostgreSQL-free).

/// Formats a 64-bit LSN as the PostgreSQL text form `HI/LO` (uppercase hex).
#[must_use]
pub fn format_pg_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xffff_ffff)
}

#[cfg(test)]
mod tests {
    use super::format_pg_lsn;

    #[test]
    fn formats_zero_and_split_halves() {
        assert_eq!(format_pg_lsn(0), "0/0");
        assert_eq!(format_pg_lsn(0x0000_0001_0000_0002), "1/2");
    }
}
