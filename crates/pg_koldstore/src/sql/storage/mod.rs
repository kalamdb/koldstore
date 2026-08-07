//! PostgreSQL storage registration SQL entrypoints.
//!
//! Domain adapter for [`koldstore_storage`] registration plans.

#[cfg(feature = "pg")]
use koldstore_storage::registration::*;

/// Registers a storage backend from SQL.
///
/// SQL contract:
/// `koldstore.register_storage(name, storage_type, base_path, credentials, config,
///   regular_path_tmpl, scoped_path_tmpl, check default true)`.
///
/// When `check` is true (default), opens the configured backend and performs a
/// put/delete probe (filesystem roots are created first). Pass `check => false`
/// to skip (for example when credentials or mounts will be available later).
///
/// Errors when `name` already exists.
#[cfg(feature = "pg")]
#[allow(clippy::too_many_arguments)]
#[pgrx::pg_extern(name = "register_storage", schema = "koldstore", security_definer)]
pub fn register_storage_pg(
    name: &str,
    storage_type: &str,
    base_path: &str,
    credentials: pgrx::JsonB,
    config: pgrx::JsonB,
    regular_path_tmpl: &str,
    scoped_path_tmpl: &str,
    check: pgrx::default!(bool, true),
) -> String {
    register_storage_pg_impl(
        name,
        storage_type,
        base_path,
        credentials,
        config,
        regular_path_tmpl,
        scoped_path_tmpl,
        check,
    )
}

/// Registers a storage backend from SQL using default path templates.
///
/// SQL contract:
/// `koldstore.register_storage(name, storage_type, base_path, credentials, config,
///   check default true)`.
///
/// When `check` is true (default), opens the configured backend and performs a
/// put/delete probe. Pass `check => false` to skip.
///
/// Errors when `name` already exists.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "register_storage", schema = "koldstore", security_definer)]
pub fn register_storage_pg_with_default_templates(
    name: &str,
    storage_type: &str,
    base_path: &str,
    credentials: pgrx::JsonB,
    config: pgrx::JsonB,
    check: pgrx::default!(bool, true),
) -> String {
    register_storage_pg_impl(
        name,
        storage_type,
        base_path,
        credentials,
        config,
        DEFAULT_REGULAR_PATH_TMPL,
        DEFAULT_SCOPED_PATH_TMPL,
        check,
    )
}

#[cfg(feature = "pg")]
#[allow(clippy::too_many_arguments)]
fn register_storage_pg_impl(
    name: &str,
    storage_type: &str,
    base_path: &str,
    credentials: pgrx::JsonB,
    config: pgrx::JsonB,
    regular_path_tmpl: &str,
    scoped_path_tmpl: &str,
    check: bool,
) -> String {
    use pgrx::datum::DatumWithOid;

    let registration = StorageRegistration {
        name: name.to_string(),
        storage_type: storage_type.to_string(),
        base_path: base_path.to_string(),
        credentials: credentials.0,
        config: config.0,
        regular_path_tmpl: regular_path_tmpl.to_string(),
        scoped_path_tmpl: scoped_path_tmpl.to_string(),
    };
    let plan = registration
        .register_plan()
        .unwrap_or_else(|error| pgrx::error!("{error}"));
    if check {
        ensure_storage_check(
            &plan.registration.storage_type,
            &plan.registration.base_path,
            &plan.registration.credentials,
            &plan.registration.config,
        );
    }
    let args = [
        DatumWithOid::from(plan.storage_id.as_str()),
        DatumWithOid::from(plan.registration.name.as_str()),
        DatumWithOid::from(plan.registration.storage_type.as_str()),
        DatumWithOid::from(plan.registration.base_path.as_str()),
        DatumWithOid::from(pgrx::JsonB(plan.registration.credentials)),
        DatumWithOid::from(pgrx::JsonB(plan.registration.config)),
        DatumWithOid::from(plan.registration.regular_path_tmpl.as_str()),
        DatumWithOid::from(plan.registration.scoped_path_tmpl.as_str()),
    ];

    let returned = pgrx::Spi::get_one_with_args::<String>(&plan.statement.sql, &args)
        .unwrap_or_else(|error| pgrx::error!("register storage failed: {error}"));

    match returned {
        Some(id) => id,
        None => pgrx::error!("{}", DdlError::StorageAlreadyExists(plan.registration.name)),
    }
}

/// Opens the backend and put/deletes a probe object; errors are raised to SQL.
#[cfg(feature = "pg")]
fn ensure_storage_check(
    storage_type: &str,
    base_path: &str,
    credentials: &serde_json::Value,
    config: &serde_json::Value,
) {
    if let Err(error) = koldstore_storage::ensure_storage_backend_writable(
        storage_type,
        base_path,
        credentials,
        config,
    ) {
        pgrx::error!("{error}");
    }
}

/// Rotates storage credentials from SQL without changing backend paths.
///
/// SQL contract: `koldstore.alter_storage_credentials(name, credentials)`.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(
    name = "alter_storage_credentials",
    schema = "koldstore",
    security_definer
)]
pub fn alter_storage_credentials_pg(name: &str, credentials: pgrx::JsonB) {
    use pgrx::datum::DatumWithOid;

    let plan = alter_storage_credentials_plan(name, credentials.0)
        .unwrap_or_else(|error| pgrx::error!("{error}"));
    let args = [
        DatumWithOid::from(plan.storage_name.as_str()),
        DatumWithOid::from(pgrx::JsonB(plan.credentials)),
    ];

    pgrx::Spi::run_with_args(&plan.statement.sql, &args)
        .unwrap_or_else(|error| pgrx::error!("alter storage credentials failed: {error}"));
}

/// Alters storage location/configuration from SQL without direct catalog DML.
///
/// SQL contract:
/// `koldstore.alter_storage_location(name, base_path, config, check default true)`.
///
/// When `check` is true (default), opens the backend with existing credentials
/// and probes put/delete at the new location. Pass `check => false` to skip.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(
    name = "alter_storage_location",
    schema = "koldstore",
    security_definer
)]
pub fn alter_storage_location_pg(
    name: &str,
    base_path: &str,
    config: pgrx::JsonB,
    check: pgrx::default!(bool, true),
) -> String {
    use pgrx::datum::DatumWithOid;

    let plan = alter_storage_location_plan(name, base_path, config.0)
        .unwrap_or_else(|error| pgrx::error!("{error}"));
    if check {
        let (storage_type, credentials) =
            load_storage_type_and_credentials(name).unwrap_or_else(|error| pgrx::error!("{error}"));
        ensure_storage_check(&storage_type, &plan.base_path, &credentials, &plan.config);
    }
    let args = [
        DatumWithOid::from(plan.storage_name.as_str()),
        DatumWithOid::from(plan.base_path.as_str()),
        DatumWithOid::from(pgrx::JsonB(plan.config)),
    ];

    pgrx::Spi::get_one_with_args::<String>(&plan.statement.sql, &args)
        .unwrap_or_else(|error| pgrx::error!("alter storage location failed: {error}"))
        .unwrap_or_else(|| pgrx::error!("storage `{}` does not exist", plan.storage_name))
}

#[cfg(feature = "pg")]
fn load_storage_type_and_credentials(name: &str) -> Result<(String, serde_json::Value), String> {
    use pgrx::datum::DatumWithOid;

    let row = pgrx::Spi::get_one_with_args::<String>(
        "SELECT CAST(jsonb_build_object(\
            'storage_type', storage_type,\
            'credentials', COALESCE(credentials, '{}'::jsonb)\
         ) AS text) FROM koldstore.storage WHERE name = $1",
        &[DatumWithOid::from(name)],
    )
    .map_err(|error| error.to_string())?
    .ok_or_else(|| format!("storage `{name}` does not exist"))?;
    let value: serde_json::Value = serde_json::from_str(&row).map_err(|error| error.to_string())?;
    let storage_type = value
        .get("storage_type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| format!("storage `{name}` missing storage_type"))?
        .to_string();
    let credentials = value
        .get("credentials")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    Ok((storage_type, credentials))
}
