//! Extension packaging version and upgrade-script contract tests.

use std::fs;
use std::path::PathBuf;

/// Last packaged SQL baseline that still needs a direct upgrade edge to current.
///
/// During pre-release (`*-beta.*`) bumps, keep a single
/// `sql/koldstore--{PREVIOUS}--{NEW}.sql` edge from this baseline and rename it
/// to the new Cargo version (do not accumulate beta→beta scripts). When cutting
/// a non-beta release, add a real edge from the prior Cargo version and update
/// this constant.
const PREVIOUS_EXTENSION_SQL_VERSION: &str = "0.1.0";

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
fn upgrade_sql_exists_from_previous_version_to_cargo_version() {
    let current = env!("CARGO_PKG_VERSION");
    let filename = format!("koldstore--{PREVIOUS_EXTENSION_SQL_VERSION}--{current}.sql");
    let path = sql_dir().join(&filename);
    assert!(
        path.is_file(),
        "missing upgrade script {filename} (required for ALTER EXTENSION koldstore UPDATE from {PREVIOUS_EXTENSION_SQL_VERSION} to {current})"
    );
    let body = fs::read_to_string(&path).expect("read upgrade sql");
    assert!(
        !body.trim().is_empty(),
        "upgrade script {filename} must not be empty"
    );
}
