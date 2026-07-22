//! PostgreSQL storage registration SQL entrypoints.

#[cfg(feature = "pg")]
use koldstore_storage::registration::*;

/// Registers a storage backend from SQL.
///
/// SQL contract:
/// `koldstore.register_storage(name, storage_type, base_path, credentials, config, regular_path_tmpl, scoped_path_tmpl)`.
///
/// Errors when `name` already exists.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "register_storage", schema = "koldstore", security_definer)]
pub fn register_storage_pg(
    name: &str,
    storage_type: &str,
    base_path: &str,
    credentials: pgrx::JsonB,
    config: pgrx::JsonB,
    regular_path_tmpl: &str,
    scoped_path_tmpl: &str,
) -> pgrx::Uuid {
    register_storage_pg_impl(
        name,
        storage_type,
        base_path,
        credentials,
        config,
        regular_path_tmpl,
        scoped_path_tmpl,
    )
}

/// Registers a storage backend from SQL using default path templates.
///
/// SQL contract:
/// `koldstore.register_storage(name, storage_type, base_path, credentials, config)`.
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
) -> pgrx::Uuid {
    register_storage_pg_impl(
        name,
        storage_type,
        base_path,
        credentials,
        config,
        DEFAULT_REGULAR_PATH_TMPL,
        DEFAULT_SCOPED_PATH_TMPL,
    )
}

#[cfg(feature = "pg")]
fn register_storage_pg_impl(
    name: &str,
    storage_type: &str,
    base_path: &str,
    credentials: pgrx::JsonB,
    config: pgrx::JsonB,
    regular_path_tmpl: &str,
    scoped_path_tmpl: &str,
) -> pgrx::Uuid {
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
    let storage_id = crate::spi::uuid_to_pgrx(plan.storage_id);

    let args = [
        DatumWithOid::from(storage_id),
        DatumWithOid::from(plan.registration.name.as_str()),
        DatumWithOid::from(plan.registration.storage_type.as_str()),
        DatumWithOid::from(plan.registration.base_path.as_str()),
        DatumWithOid::from(pgrx::JsonB(plan.registration.credentials)),
        DatumWithOid::from(pgrx::JsonB(plan.registration.config)),
        DatumWithOid::from(plan.registration.regular_path_tmpl.as_str()),
        DatumWithOid::from(plan.registration.scoped_path_tmpl.as_str()),
    ];

    let returned = pgrx::Spi::get_one_with_args::<pgrx::Uuid>(&plan.statement.sql, &args)
        .unwrap_or_else(|error| pgrx::error!("register storage failed: {error}"));

    match returned {
        Some(id) => id,
        None => pgrx::error!("{}", DdlError::StorageAlreadyExists(plan.registration.name)),
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
/// SQL contract: `koldstore.alter_storage_location(name, base_path, config)`.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(
    name = "alter_storage_location",
    schema = "koldstore",
    security_definer
)]
pub fn alter_storage_location_pg(name: &str, base_path: &str, config: pgrx::JsonB) -> pgrx::Uuid {
    use pgrx::datum::DatumWithOid;

    let plan = alter_storage_location_plan(name, base_path, config.0)
        .unwrap_or_else(|error| pgrx::error!("{error}"));
    let args = [
        DatumWithOid::from(plan.storage_name.as_str()),
        DatumWithOid::from(plan.base_path.as_str()),
        DatumWithOid::from(pgrx::JsonB(plan.config)),
    ];

    pgrx::Spi::get_one_with_args::<pgrx::Uuid>(&plan.statement.sql, &args)
        .unwrap_or_else(|error| pgrx::error!("alter storage location failed: {error}"))
        .unwrap_or_else(|| pgrx::error!("storage `{}` does not exist", plan.storage_name))
}
