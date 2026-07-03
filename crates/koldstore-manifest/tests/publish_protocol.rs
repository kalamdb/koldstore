use koldstore_manifest::ManifestPublishPlan;
use koldstore_storage::PublishAction;

#[test]
fn publish_protocol_commits_manifest_after_final_object() {
    let plan =
        ManifestPublishPlan::for_segment("app/items", "batch-0.parquet", "writer", "manifest.json");
    let actions = plan.actions();

    assert!(matches!(actions[0], PublishAction::PutTemp(_)));
    assert!(matches!(actions[1], PublishAction::CopyTempToFinal { .. }));
    assert!(matches!(actions[2], PublishAction::ValidateFinal(_)));
    assert!(matches!(actions[3], PublishAction::DeleteTemp(_)));
    assert!(matches!(actions[4], PublishAction::PutManifest(_)));
    assert!(actions
        .iter()
        .all(|action| !matches!(action, PublishAction::Rename { .. })));
}
