//! Hot-row sequence sentinel used during merge resolution.

/// Hot heap rows use a sentinel sequence during winner resolution so any live
/// hot row beats every cold candidate for the same primary key.
pub const HOT_SEQ_SENTINEL: i64 = i64::MAX;
