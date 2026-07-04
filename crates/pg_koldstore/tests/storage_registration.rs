use pg_koldstore::spi::SpiAccess;
use pg_koldstore::sql::ddl::{
    alter_storage_credentials_plan, alter_storage_location_plan, StorageRegistration,
    DEFAULT_SHARED_PATH_TEMPLATE, DEFAULT_USER_PATH_TEMPLATE,
};
use uuid::Uuid;

fn example_registration() -> StorageRegistration {
    StorageRegistration {
        name: "local-minio".to_string(),
        storage_type: "s3".to_string(),
        base_path: "s3://koldstore-test".to_string(),
        credentials: serde_json::json!({
            "access_key_id": "minioadmin",
            "secret_access_key": "do-not-leak"
        }),
        config: serde_json::json!({
            "endpoint": "http://localhost:9000",
            "path_style": true
        }),
        shared_path_template: DEFAULT_SHARED_PATH_TEMPLATE.to_string(),
        user_path_template: DEFAULT_USER_PATH_TEMPLATE.to_string(),
    }
}

#[test]
fn sql_extension_exposes_storage_registration_and_redaction_contract() {
    let sql = include_str!("../sql/koldstore--0.1.0.sql");

    for needle in [
        "CREATE TABLE IF NOT EXISTS koldstore.storage",
        "storage_type text NOT NULL CHECK",
        "credentials jsonb NOT NULL DEFAULT '{}'::jsonb",
        "shared_path_template text NOT NULL",
        "user_path_template text NOT NULL",
        "REVOKE ALL ON",
        "koldstore.storage",
        "koldstore.manifest",
        "koldstore.cold_segments",
    ] {
        assert!(
            sql.contains(needle),
            "missing storage catalog fragment: {needle}"
        );
    }
}

#[test]
fn storage_registration_renders_shared_and_user_templates() {
    let registration = example_registration();

    assert_eq!(
        registration
            .render_shared_prefix("app", "shared_items")
            .unwrap(),
        "app/shared_items/"
    );
    assert_eq!(
        registration
            .render_user_prefix("app", "notes", "tenant-a")
            .unwrap(),
        "app/notes/tenant-a/"
    );
    assert!(registration
        .render_user_prefix("app", "notes", "  ")
        .is_err());
}

#[test]
fn storage_registration_redacts_credentials_without_losing_templates() {
    let registration = example_registration();

    let redacted = registration.redacted();

    assert_eq!(redacted.credentials, serde_json::json!({"redacted": true}));
    assert_eq!(
        redacted.shared_path_template,
        registration.shared_path_template
    );
    assert_eq!(redacted.user_path_template, registration.user_path_template);
    assert!(
        !redacted.credentials.to_string().contains("do-not-leak"),
        "redacted diagnostics must not leak storage secrets"
    );
}

#[test]
fn storage_registration_rejects_invalid_catalog_inputs() {
    let mut registration = example_registration();
    assert!(registration.validate().is_ok());

    registration.name = "  ".to_string();
    assert!(registration.validate().is_err());

    registration = example_registration();
    registration.storage_type = "ftp".to_string();
    assert!(registration.validate().is_err());

    registration = example_registration();
    registration.base_path = "\t".to_string();
    assert!(registration.validate().is_err());

    registration = example_registration();
    registration.user_path_template = "{namespace}/{tableName}/".to_string();
    assert!(registration.validate().is_err());
}

#[test]
fn storage_registration_builds_parameterized_catalog_upsert_plan() {
    let registration = example_registration();
    let plan = registration
        .register_plan_with_id(Uuid::from_u128(42))
        .unwrap();

    assert_eq!(plan.storage_id, Uuid::from_u128(42));
    assert_eq!(plan.statement.operation, "register storage");
    assert_eq!(plan.statement.access, SpiAccess::ReadWrite);
    assert!(plan.statement.sql.contains("INSERT INTO koldstore.storage"));
    assert!(plan.statement.sql.contains("ON CONFLICT (name) DO UPDATE"));
    assert!(plan.statement.sql.contains("RETURNING s.id"));

    for placeholder in ["$1", "$2", "$3", "$4", "$5", "$6", "$7", "$8"] {
        assert!(
            plan.statement.sql.contains(placeholder),
            "missing placeholder {placeholder}"
        );
    }
    for secret in ["minioadmin", "do-not-leak", "http://localhost:9000"] {
        assert!(
            !plan.statement.sql.contains(secret),
            "catalog SQL must not interpolate sensitive registration data"
        );
    }
}

#[test]
fn alter_storage_credentials_plan_updates_only_credentials() {
    let credentials = serde_json::json!({
        "access_key_id": "AKIA...",
        "secret_access_key": "new-secret"
    });
    let plan = alter_storage_credentials_plan("local-minio", credentials.clone()).unwrap();

    assert_eq!(plan.storage_name, "local-minio");
    assert_eq!(plan.credentials, credentials);
    assert_eq!(plan.statement.operation, "alter storage credentials");
    assert_eq!(plan.statement.access, SpiAccess::ReadWrite);
    assert!(plan.statement.sql.contains("UPDATE koldstore.storage"));
    assert!(plan.statement.sql.contains("SET credentials = $2"));
    assert!(plan.statement.sql.contains("WHERE name = $1"));
    assert!(!plan.statement.sql.contains("base_path"));
    assert!(!plan.statement.sql.contains("shared_path_template"));
    assert!(!plan.statement.sql.contains("user_path_template"));
    assert!(!plan.statement.sql.contains("new-secret"));
}

#[test]
fn alter_storage_location_plan_updates_base_path_and_config() {
    let config = serde_json::json!({"region": "us-east-1"});
    let plan =
        alter_storage_location_plan("local-minio", "s3://new-bucket", config.clone()).unwrap();

    assert_eq!(plan.storage_name, "local-minio");
    assert_eq!(plan.base_path, "s3://new-bucket");
    assert_eq!(plan.config, config);
    assert_eq!(plan.statement.operation, "alter storage location");
    assert_eq!(plan.statement.access, SpiAccess::ReadWrite);
    assert!(plan.statement.sql.contains("UPDATE koldstore.storage"));
    assert!(plan.statement.sql.contains("base_path = $2"));
    assert!(plan
        .statement
        .sql
        .contains("config = COALESCE($3::jsonb, config)"));
    assert!(plan.statement.sql.contains("RETURNING id"));
    assert!(!plan.statement.sql.contains("s3://new-bucket"));

    assert!(alter_storage_location_plan(" ", "s3://new-bucket", serde_json::json!({})).is_err());
    assert!(alter_storage_location_plan("local-minio", " ", serde_json::json!({})).is_err());
}
