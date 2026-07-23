//! Minimal raw-SPI query lifecycle shared by merge-scan source readers.

use std::ffi::CString;

use pgrx::pg_sys;

/// Executes a read-only SPI query and exposes its tuple table to a decoder.
///
/// SPI is finished before returning, including when the decoder rejects a row.
///
/// # Errors
///
/// Returns an error when query text contains a NUL byte, an SPI lifecycle call
/// fails, the processed-row count cannot be represented, or decoding fails.
pub(super) unsafe fn with_read_query<T>(
    query: &str,
    decode: impl FnOnce(usize, *mut pg_sys::SPITupleTable) -> Result<T, String>,
) -> Result<T, String> {
    let query = CString::new(query).map_err(|error| error.to_string())?;
    let connect = pg_sys::SPI_connect();
    if connect < 0 {
        return Err(format!("SPI_connect failed with code {connect}"));
    }

    let execute = pg_sys::SPI_execute(query.as_ptr(), true, 0);
    if execute < 0 {
        let _ = pg_sys::SPI_finish();
        return Err(format!("SPI_execute failed with code {execute}"));
    }

    let decoded = usize::try_from(pg_sys::SPI_processed)
        .map_err(|error| error.to_string())
        .and_then(|processed| decode(processed, pg_sys::SPI_tuptable));
    let finish = pg_sys::SPI_finish();
    match decoded {
        Err(error) => Err(error),
        Ok(_) if finish < 0 => Err(format!("SPI_finish failed with code {finish}")),
        Ok(value) => Ok(value),
    }
}
