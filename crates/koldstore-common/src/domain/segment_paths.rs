//! Cold-segment object-path layout shared by flush, manifest, and parquet plans.
//!
//! Catalog stores table-relative keys
//! `{folder:03}/segment-{NNNN}-{token}.parquet`. Full object keys are
//! `join_object_key(table_prefix, relative)` in `koldstore-storage`.

/// Max segments per numeric folder before rolling to the next (`001/`, `002/`, …).
pub const SEGMENTS_PER_FOLDER: u32 = 100;

/// Hex characters from the segment id used in object names (32 bits).
pub const SEGMENT_PATH_TOKEN_LEN: usize = 8;

/// Folder number for a batch (`1` → `001/`, `101` → `002/`, …).
///
/// Uses 1-based batch numbering with [`SEGMENTS_PER_FOLDER`] segments per folder.
/// `batch_number <= 0` maps to folder `1` (test / edge paths).
#[must_use]
pub fn segment_folder_number(batch_number: i32) -> u32 {
    let n = u32::try_from(batch_number.max(1)).unwrap_or(1);
    (n - 1) / SEGMENTS_PER_FOLDER + 1
}

/// Short path token from a segment UUID (dashes ignored, first 8 hex chars).
///
/// Catalog identity stays a full UUID; object keys only need collision resistance
/// across retries at the same `batch_number`.
#[must_use]
pub fn segment_path_token(segment_id: impl std::fmt::Display) -> String {
    segment_id
        .to_string()
        .chars()
        .filter(|ch| *ch != '-')
        .take(SEGMENT_PATH_TOKEN_LEN)
        .collect()
}

/// Table-relative segment path (`001/segment-0001-{token}.parquet`).
///
/// `path_token` is typically [`segment_path_token`] of the catalog segment id.
#[must_use]
pub fn segment_relative_object_path(batch_number: i32, path_token: impl AsRef<str>) -> String {
    let folder = segment_folder_number(batch_number);
    let batch = u32::try_from(batch_number.max(0)).unwrap_or(0);
    let token = path_token.as_ref();
    format!("{folder:03}/segment-{batch:04}-{token}.parquet")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn folder_rolls_every_hundred_segments() {
        assert_eq!(segment_folder_number(1), 1);
        assert_eq!(segment_folder_number(100), 1);
        assert_eq!(segment_folder_number(101), 2);
        assert_eq!(segment_folder_number(0), 1);
    }

    #[test]
    fn relative_path_is_padded_and_tokenized() {
        assert_eq!(
            segment_relative_object_path(1, "a0dbcb97"),
            "001/segment-0001-a0dbcb97.parquet"
        );
        assert_eq!(
            segment_relative_object_path(101, "11111111"),
            "002/segment-0101-11111111.parquet"
        );
    }
}
