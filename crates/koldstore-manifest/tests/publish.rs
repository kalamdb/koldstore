use koldstore_manifest::ManifestPublishPlan;
use koldstore_storage::{ConditionalPut, PathTemplate, PublishAction};

#[test]
fn publish_plan_uses_temp_final_then_manifest_visibility_boundary() {
    let plan = ManifestPublishPlan::for_segment(
        "app/items",
        "batch-7.parquet",
        "writer-1",
        "manifest.json",
    );

    assert_eq!(
        plan.temp_path,
        "app/items/.tmp/writer-1/batch-7.parquet.tmp"
    );
    assert_eq!(plan.final_path, "app/items/batch-7.parquet");
    assert_eq!(plan.manifest_path, "app/items/manifest.json");
    assert_eq!(
        plan.actions(),
        vec![
            PublishAction::PutTemp(plan.temp_path.clone()),
            PublishAction::CopyTempToFinal {
                temp: plan.temp_path.clone(),
                final_path: plan.final_path.clone(),
            },
            PublishAction::ValidateFinal(plan.final_path.clone()),
            PublishAction::DeleteTemp(plan.temp_path.clone()),
            PublishAction::PutManifest(plan.manifest_path.clone()),
        ]
    );
}

#[test]
fn conditional_put_never_claims_atomic_rename() {
    assert_eq!(
        ConditionalPut::IfAbsent.description(),
        "put only when target is absent"
    );
    assert_eq!(
        ConditionalPut::IfMatch.description(),
        "put only when identity matches"
    );
}

#[test]
fn path_template_expands_shared_and_user_placeholders() {
    let shared = PathTemplate::new("{namespace}/{tableName}/");
    assert_eq!(shared.render("app", "items", None).unwrap(), "app/items/");

    let user = PathTemplate::new("{namespace}/{tableName}/{scopeId}/");
    assert_eq!(
        user.render("app", "notes", Some("user-a")).unwrap(),
        "app/notes/user-a/"
    );

    assert!(user.render("app", "notes", None).is_err());
}
