//! Resource-safe wrappers for merge-scan calls into PostgreSQL SPI and palloc.

use std::cell::Cell;
use std::ffi::CStr;
use std::os::raw::c_char;
use std::panic::AssertUnwindSafe;

use pgrx::pg_sys;

/// Runs `operation` inside one SPI connection and always closes that connection.
///
/// The `PgTryBuilder` finalizer covers ordinary Rust returns and unwinding from a
/// PostgreSQL `ERROR`, for which a Rust-only `Drop` guard is not sufficient.
///
/// # Errors
///
/// Returns an error when SPI cannot connect or finish, or when `operation` does.
pub(super) unsafe fn with_spi_connection<T>(
    operation: impl FnOnce() -> Result<T, String>,
) -> Result<T, String> {
    let connect = pg_sys::SPI_connect();
    if connect < 0 {
        return Err(format!("SPI_connect failed with code {connect}"));
    }

    let finish_status = Cell::new(None);
    let result = pgrx::PgTryBuilder::new(AssertUnwindSafe(operation))
        .finally(|| finish_status.set(Some(pg_sys::SPI_finish())))
        .execute();
    let finish = finish_status
        .get()
        .expect("SPI finalizer must record its finish status");
    if finish < 0 {
        return Err(match result {
            Ok(_) => format!("SPI_finish failed with code {finish}"),
            Err(error) => format!("{error}; SPI_finish failed with code {finish}"),
        });
    }
    result
}

/// Owns a non-null C string allocated in a PostgreSQL memory context.
pub(super) struct PgAllocatedCString(*mut c_char);

impl PgAllocatedCString {
    /// Takes ownership of `value`, which must have been allocated with palloc.
    ///
    /// # Safety
    ///
    /// `value` must be non-null, NUL-terminated, and safe to release with
    /// [`pg_sys::pfree`]. Ownership must not be transferred elsewhere.
    pub(super) unsafe fn from_raw(value: *mut c_char) -> Self {
        debug_assert!(!value.is_null());
        Self(value)
    }

    /// Borrows the owned value as a C string.
    pub(super) unsafe fn as_c_str(&self) -> &CStr {
        CStr::from_ptr(self.0)
    }
}

impl Drop for PgAllocatedCString {
    fn drop(&mut self) {
        unsafe {
            pg_sys::pfree(self.0.cast());
        }
    }
}
