//! Extension packaging version contract tests.
//!
//! During pre-release development, catalog DDL changes go directly into
//! `sql/koldstore--0.1.0.sql`. Do not add `koldstore--<from>--<to>.sql` upgrade
//! edges until a supported upgrade path is intentionally introduced.

use std::fs;
use std::path::PathBuf;

fn sql_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("sql")
}

fn control_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("koldstore.control")
}

#[test]
fn control_default_version_tracks_cargo_package_version() {
    let control = fs::read_to_string(control_path()).expect("read koldstore.control");
    assert!(
        control.contains("default_version = '@CARGO_VERSION@'"),
        "koldstore.control must use @CARGO_VERSION@ so packaged extversion matches Cargo; got:\n{control}"
    );
}

#[test]
fn bootstrap_catalog_sql_exists() {
    let path = sql_dir().join("koldstore--0.1.0.sql");
    assert!(
        path.is_file(),
        "missing bootstrap catalog fragment {}",
        path.display()
    );
    let body = fs::read_to_string(&path).expect("read bootstrap sql");
    assert!(
        !body.trim().is_empty(),
        "bootstrap catalog fragment must not be empty"
    );
}

#[test]
fn no_extension_upgrade_sql_edges_during_development() {
    let entries = fs::read_dir(sql_dir()).expect("read sql dir");
    let upgrade_edges: Vec<String> = entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let name = entry.file_name().into_string().ok()?;
            // Packaged UPDATE edges look like koldstore--<from>--<to>.sql
            // (two version separators). The bootstrap file is koldstore--0.1.0.sql.
            let is_upgrade_edge = name.starts_with("koldstore--")
                && name.ends_with(".sql")
                && name.matches("--").count() >= 2;
            is_upgrade_edge.then_some(name)
        })
        .collect();
    assert!(
        upgrade_edges.is_empty(),
        "development builds edit koldstore--0.1.0.sql directly; remove upgrade edges: {upgrade_edges:?}"
    );
}
