//! DDL and ProcessUtility integration for KoldStore table options.
//!
//! DROP TABLE cleanup planning lives in `koldstore-migrate`; this module
//! re-exports those plans for the extension shell and installs the live
//! ProcessUtility hook used for `ALTER TABLE … SET/RESET` KoldStore options.

pub use koldstore_migrate::drop_table::{
    plan_drop_table_cleanup, DropTableCleanupError, DropTableCleanupOutcome, DropTableCleanupPlan,
    DropTableCleanupPolicy,
};

#[cfg(feature = "pg")]
mod process_utility {
    use std::collections::HashMap;
    use std::ffi::CStr;
    use std::sync::atomic::{AtomicBool, Ordering};

    use pgrx::pg_sys;

    static REGISTERED: AtomicBool = AtomicBool::new(false);
    static mut PREVIOUS: pg_sys::ProcessUtility_hook_type = None;

    pub(super) fn register() {
        if REGISTERED.swap(true, Ordering::AcqRel) {
            return;
        }
        unsafe {
            PREVIOUS = pg_sys::ProcessUtility_hook;
            pg_sys::ProcessUtility_hook = Some(hook);
        }
    }

    // Signature must match PostgreSQL ProcessUtility_hook_type.
    #[allow(clippy::too_many_arguments)]
    #[pgrx::pg_guard]
    unsafe extern "C-unwind" fn hook(
        pstmt: *mut pg_sys::PlannedStmt,
        query: *const core::ffi::c_char,
        read_only: bool,
        context: pg_sys::ProcessUtilityContext::Type,
        params: pg_sys::ParamListInfo,
        env: *mut pg_sys::QueryEnvironment,
        dest: *mut pg_sys::DestReceiver,
        qc: *mut pg_sys::QueryCompletion,
    ) {
        unsafe {
            let copied = pg_sys::copyObjectImpl(pstmt.cast()).cast::<pg_sys::PlannedStmt>();
            let mut captured = None;
            let mut has_standard_actions = true;
            if !copied.is_null()
                && !(*copied).utilityStmt.is_null()
                && (*(*copied).utilityStmt).type_ == pg_sys::NodeTag::T_AlterTableStmt
            {
                let stmt = (*copied).utilityStmt.cast::<pg_sys::AlterTableStmt>();
                captured = strip_options(stmt);
                has_standard_actions = !(*stmt).cmds.is_null();
            }
            if has_standard_actions {
                delegate(copied, query, read_only, context, params, env, dest, qc);
            } else if !qc.is_null() {
                (*qc).commandTag = pg_sys::CommandTag::CMDTAG_ALTER_TABLE;
                (*qc).nprocessed = 0;
            }
            if let Some((relation, options)) = captured {
                apply_options(relation, options);
            }
        }
    }

    // Forwards the fixed ProcessUtility hook arity to the previous hook / standard path.
    #[allow(clippy::too_many_arguments)]
    unsafe fn delegate(
        pstmt: *mut pg_sys::PlannedStmt,
        query: *const core::ffi::c_char,
        read_only: bool,
        context: pg_sys::ProcessUtilityContext::Type,
        params: pg_sys::ParamListInfo,
        env: *mut pg_sys::QueryEnvironment,
        dest: *mut pg_sys::DestReceiver,
        qc: *mut pg_sys::QueryCompletion,
    ) {
        unsafe {
            if let Some(previous) = PREVIOUS {
                previous(pstmt, query, read_only, context, params, env, dest, qc)
            } else {
                pg_sys::standard_ProcessUtility(
                    pstmt, query, read_only, context, params, env, dest, qc,
                )
            }
        }
    }

    unsafe fn strip_options(
        stmt: *mut pg_sys::AlterTableStmt,
    ) -> Option<(*mut pg_sys::RangeVar, HashMap<String, String>)> {
        unsafe {
            let commands = (*stmt).cmds;
            let command_count = if commands.is_null() {
                0
            } else {
                (*commands).length as usize
            };
            let mut kept: *mut pg_sys::List = std::ptr::null_mut();
            let mut found = HashMap::new();
            for index in 0..command_count {
                let cmd = (*(*commands).elements.add(index))
                    .ptr_value
                    .cast::<pg_sys::AlterTableCmd>();
                if (*cmd).subtype != pg_sys::AlterTableType::AT_SetRelOptions
                    && (*cmd).subtype != pg_sys::AlterTableType::AT_ResetRelOptions
                {
                    kept = pg_sys::lappend(kept, cmd.cast());
                    continue;
                }
                let reset = (*cmd).subtype == pg_sys::AlterTableType::AT_ResetRelOptions;
                let defs = (*cmd).def.cast::<pg_sys::List>();
                let def_count = if defs.is_null() {
                    0
                } else {
                    (*defs).length as usize
                };
                let mut standard: *mut pg_sys::List = std::ptr::null_mut();
                for d in 0..def_count {
                    let def = (*(*defs).elements.add(d))
                        .ptr_value
                        .cast::<pg_sys::DefElem>();
                    let name = CStr::from_ptr((*def).defname)
                        .to_string_lossy()
                        .into_owned();
                    if name.starts_with("koldstore_") {
                        if reset {
                            pgrx::error!("KoldStore RESET is not supported; set a replacement policy instead")
                        }
                        let value = CStr::from_ptr(pg_sys::defGetString(def))
                            .to_string_lossy()
                            .into_owned();
                        found.insert(name, value);
                    } else {
                        standard = pg_sys::lappend(standard, def.cast());
                    }
                }
                if !standard.is_null() {
                    (*cmd).def = standard.cast();
                    kept = pg_sys::lappend(kept, cmd.cast());
                }
            }
            (*stmt).cmds = kept;
            (!found.is_empty()).then_some(((*stmt).relation, found))
        }
    }

    unsafe fn apply_options(relation: *mut pg_sys::RangeVar, values: HashMap<String, String>) {
        let oid = unsafe {
            pg_sys::RangeVarGetRelidExtended(
                relation,
                pg_sys::AccessExclusiveLock as i32,
                0,
                None,
                std::ptr::null_mut(),
            )
        };
        let owns_relation = unsafe { relation_ownercheck(oid, pg_sys::GetUserId()) };
        if !owns_relation {
            pgrx::error!("must be owner of relation to configure KoldStore");
        }
        super::apply_management_options(oid, &values)
            .unwrap_or_else(|error| pgrx::error!("KoldStore ALTER TABLE failed: {error}"));
    }

    /// True when `role` owns relation `oid` (PG15 vs PG16+ ACL helper names differ).
    unsafe fn relation_ownercheck(oid: pg_sys::Oid, role: pg_sys::Oid) -> bool {
        #[cfg(feature = "pg15")]
        unsafe {
            pg_sys::pg_class_ownercheck(oid, role)
        }
        #[cfg(not(feature = "pg15"))]
        unsafe {
            pg_sys::object_ownercheck(pg_sys::RelationRelationId, oid, role)
        }
    }
}

#[cfg(feature = "pg")]
pub(crate) fn register_process_utility_hook() {
    process_utility::register();
}

#[cfg(feature = "pg")]
fn apply_management_options(
    table_oid: pgrx::pg_sys::Oid,
    values: &std::collections::HashMap<String, String>,
) -> Result<(), String> {
    use pgrx::datum::DatumWithOid;
    let get = |name: &str| values.get(name).map(String::as_str);
    if get("koldstore_move_when").is_some() {
        return Err("filter policy is not supported yet".into());
    }
    if get("koldstore_hot_row_limit").is_some() && get("koldstore_move_after").is_some() {
        return Err("hot_row_limit and move_after cannot be set together".into());
    }
    if get("koldstore_enabled")
        .is_some_and(|v| !matches!(v.to_ascii_lowercase().as_str(), "true" | "on" | "1"))
    {
        return Err(
            "koldstore_enabled=false is not supported; use koldstore.unmanage_table(...)".into(),
        );
    }
    let catalog_lookup = "SELECT (SELECT jsonb_build_object('storage', st.name, 'options', s.options) FROM koldstore.schemas s JOIN koldstore.storage st ON st.id=s.storage_id WHERE s.table_oid=$1)";
    let row = pgrx::Spi::get_one_with_args::<pgrx::JsonB>(
        catalog_lookup,
        &[DatumWithOid::from(table_oid)],
    )
    .map_err(|e| e.to_string())?;
    if row.is_none() {
        if get("koldstore_enabled") != Some("true") && get("koldstore_enabled") != Some("on") {
            return Err("initial management requires koldstore_enabled = true".into());
        }
        let storage =
            get("koldstore_storage").ok_or("initial management requires koldstore_storage")?;
        let hot = get("koldstore_hot_row_limit")
            .map(str::parse::<i64>)
            .transpose()
            .map_err(|_| "hot_row_limit must be a positive integer")?;
        if hot.is_none() && get("koldstore_move_after").is_none() {
            return Err("initial management requires hot_row_limit or move_after".into());
        }
        let min = get("koldstore_min_flush_rows")
            .unwrap_or("1000")
            .parse()
            .map_err(|_| "min_flush_rows must be a positive integer")?;
        let file = get("koldstore_max_rows_per_file")
            .unwrap_or("1000")
            .parse()
            .map_err(|_| "max_rows_per_file must be a positive integer")?;
        crate::sql::migrate_pg::manage_table_pg(
            table_oid,
            storage,
            hot.or(Some(1)),
            min,
            file,
            "shared",
            None,
            None,
            None,
            None,
            "strict",
            true,
        );
    }
    let current = pgrx::Spi::get_one_with_args::<pgrx::JsonB>(
        catalog_lookup,
        &[DatumWithOid::from(table_oid)],
    )
    .map_err(|e| e.to_string())?
    .ok_or("table management catalog row is missing")?;
    if let Some(requested) = get("koldstore_storage") {
        if current.0["storage"].as_str() != Some(requested) {
            return Err("storage cannot be changed after a table is managed".into());
        }
    }
    let mut options = koldstore_common::ManageTableOptions::from_value(&current.0["options"]);
    let old = options.flush_policy();
    let min = get("koldstore_min_flush_rows")
        .map(str::parse)
        .transpose()
        .map_err(|_| "min_flush_rows must be positive")?
        .unwrap_or_else(|| {
            old.as_ref()
                .map(koldstore_common::FlushPolicy::min_flush_rows)
                .unwrap_or(1_000)
        });
    let file = get("koldstore_max_rows_per_file")
        .map(str::parse)
        .transpose()
        .map_err(|_| "max_rows_per_file must be positive")?
        .unwrap_or_else(|| {
            old.as_ref()
                .map(koldstore_common::FlushPolicy::max_rows_per_file)
                .unwrap_or(koldstore_common::DEFAULT_MIN_MAX_ROWS_PER_FILE)
        });
    let max = get("koldstore_max_rows_per_flush")
        .map(str::parse)
        .transpose()
        .map_err(|_| "max_rows_per_flush must be positive")?
        .unwrap_or_else(|| {
            old.as_ref()
                .map(koldstore_common::FlushPolicy::max_rows_per_flush)
                .unwrap_or(koldstore_common::DEFAULT_MAX_ROWS_PER_FLUSH)
        });
    if min == 0 || file == 0 || max == 0 {
        return Err("flush batching settings must be greater than zero".into());
    }
    koldstore_common::validate_max_rows_per_file(
        file,
        u64::try_from(crate::guc::min_max_rows_per_file())
            .unwrap_or(koldstore_common::DEFAULT_MIN_MAX_ROWS_PER_FILE),
        None,
    )?;
    let changes_batching = get("koldstore_min_flush_rows").is_some()
        || get("koldstore_max_rows_per_file").is_some()
        || get("koldstore_max_rows_per_flush").is_some();
    if changes_batching
        && get("koldstore_hot_row_limit").is_none()
        && get("koldstore_move_after").is_none()
    {
        options.flush_policy = old.clone().map(|policy| match policy {
            koldstore_common::FlushPolicy::RowLimit { hot_row_limit, .. } => {
                koldstore_common::FlushPolicy::RowLimit {
                    hot_row_limit,
                    min_flush_rows: min,
                    max_rows_per_file: file,
                    max_rows_per_flush: max,
                }
            }
            koldstore_common::FlushPolicy::OlderThan { age, .. } => {
                koldstore_common::FlushPolicy::OlderThan {
                    age,
                    min_flush_rows: min,
                    max_rows_per_file: file,
                    max_rows_per_flush: max,
                }
            }
            policy @ koldstore_common::FlushPolicy::Filter { .. } => policy,
        });
    }
    if let Some(value) = get("koldstore_hot_row_limit") {
        let hot_row_limit = value
            .parse()
            .map_err(|_| "hot_row_limit must be positive")?;
        if hot_row_limit == 0 {
            return Err("hot_row_limit must be greater than zero".into());
        }
        options.flush_policy = Some(koldstore_common::FlushPolicy::RowLimit {
            hot_row_limit,
            min_flush_rows: min,
            max_rows_per_file: file,
            max_rows_per_flush: max,
        });
    }
    if let Some(value) = get("koldstore_move_after") {
        let interval = pgrx::Spi::get_one_with_args::<pgrx::datetime::Interval>(
            "SELECT $1::text::interval",
            &[DatumWithOid::from(value)],
        )
        .map_err(|e| e.to_string())?
        .ok_or("invalid move_after interval")?;
        let age = koldstore_common::MoveAfter::new(
            interval.months(),
            interval.days(),
            interval.micros(),
        )?;
        options.flush_policy = Some(koldstore_common::FlushPolicy::OlderThan {
            age,
            min_flush_rows: min,
            max_rows_per_file: file,
            max_rows_per_flush: max,
        });
    }
    let json = pgrx::JsonB(options.to_value());
    pgrx::Spi::run_with_args(
        "UPDATE koldstore.schemas SET options=$2 WHERE table_oid=$1",
        &[DatumWithOid::from(table_oid), DatumWithOid::from(json)],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}
