//! Scoped owner identities for fixed internal executor reads.
//!
//! Managed-table catalogs are intentionally hidden from application roles
//! because storage rows can contain credentials. Planner/executor hooks still
//! need those catalogs while serving a non-owner `SELECT`, so only their fixed,
//! read-only SPI statements run under the extension owner's identity.

use pgrx::pg_sys;

/// Runs one internal catalog operation as the owner of the `koldstore` extension.
///
/// The caller must pass only extension-owned, fixed catalog operations. User
/// SQL and managed-table heap reads must stay under the invoking role so normal
/// permissions and RLS continue to apply.
///
/// # Errors
///
/// Returns an error when the extension or its owner cannot be resolved.
pub(crate) fn with_extension_owner<T>(operation: impl FnOnce() -> T) -> Result<T, String> {
    let owner = extension_owner()?;
    with_user_identity(
        owner,
        pg_sys::SECURITY_LOCAL_USERID_CHANGE as i32,
        operation,
    )
}

/// Reads the complete hot source relation as its owner for winner resolution.
///
/// PostgreSQL applies the invoking role's quals only after hot/cold winner
/// resolution. The fixed internal SPI read therefore has to bypass RLS,
/// including `FORCE ROW LEVEL SECURITY`, or a hidden newer hot version could
/// allow an older cold version to reappear. This mirrors PostgreSQL's internal
/// referential-integrity checks: relation owner plus `SECURITY_NOFORCE_RLS`.
///
/// # Errors
///
/// Returns an error produced by the fixed internal read.
pub(crate) fn with_relation_owner_for_merge<T>(
    owner: pg_sys::Oid,
    operation: impl FnOnce() -> Result<T, String>,
) -> Result<T, String> {
    if owner == pg_sys::InvalidOid {
        return Err("managed relation has no valid owner".to_string());
    }
    with_user_identity(
        owner,
        (pg_sys::SECURITY_LOCAL_USERID_CHANGE | pg_sys::SECURITY_NOFORCE_RLS) as i32,
        operation,
    )?
}

fn with_user_identity<T>(
    user: pg_sys::Oid,
    security_flags: i32,
    operation: impl FnOnce() -> T,
) -> Result<T, String> {
    let mut previous_user = pg_sys::InvalidOid;
    let mut previous_context = 0;
    unsafe {
        pg_sys::GetUserIdAndSecContext(&mut previous_user, &mut previous_context);
    }

    if user == previous_user && previous_context & security_flags == security_flags {
        return Ok(operation());
    }

    unsafe {
        pg_sys::SetUserIdAndSecContext(user, previous_context | security_flags);
    }
    let _guard = UserIdentityGuard {
        user: previous_user,
        context: previous_context,
    };
    Ok(operation())
}

struct UserIdentityGuard {
    user: pg_sys::Oid,
    context: i32,
}

impl Drop for UserIdentityGuard {
    fn drop(&mut self) {
        unsafe {
            pg_sys::SetUserIdAndSecContext(self.user, self.context);
        }
    }
}

fn extension_owner() -> Result<pg_sys::Oid, String> {
    // SAFETY: the extension name is a static NUL-terminated string. The syscache
    // tuple remains pinned until after the fixed-width `extowner` field is copied.
    let owner = unsafe {
        let extension_oid = pg_sys::get_extension_oid(c"koldstore".as_ptr(), true);
        if extension_oid == pg_sys::InvalidOid {
            return Err("koldstore extension is not installed".to_string());
        }

        let tuple =
            pg_sys::SearchSysCache1(extension_oid_cache_id(), pg_sys::Datum::from(extension_oid));
        if tuple.is_null() {
            return Err(format!(
                "could not resolve owner for koldstore extension oid {extension_oid}"
            ));
        }

        let extension = pg_sys::GETSTRUCT(tuple).cast::<pg_sys::FormData_pg_extension>();
        let owner = (*extension).extowner;
        pg_sys::ReleaseSysCache(tuple);
        owner
    };
    if owner == pg_sys::InvalidOid {
        Err("koldstore extension has no valid owner".to_string())
    } else {
        Ok(owner)
    }
}

#[cfg(feature = "pg17")]
const fn extension_oid_cache_id() -> i32 {
    // PostgreSQL 17's generated pgrx binding prefixes this syscache identifier
    // to avoid a bindgen name collision.
    pg_sys::SysCacheIdentifier::ZEXTENSIONOID as i32
}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg18"))]
const fn extension_oid_cache_id() -> i32 {
    pg_sys::SysCacheIdentifier::EXTENSIONOID as i32
}
