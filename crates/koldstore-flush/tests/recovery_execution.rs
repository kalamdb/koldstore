use std::collections::HashSet;

use koldstore_flush::recovery::{
    apply_recovery_plan, classify_orphan_object, discover_orphan_objects, plan_recovery_actions,
    RecoveryAction,
};
use koldstore_storage::{ObjectStoreClient, PutPrecondition, StorageClient, StorageClientError};

#[test]
fn nested_tmp_directory_is_classified_as_temporary() {
    assert_eq!(
        classify_orphan_object("app/items/.tmp/writer/batch.parquet", false),
        Some(RecoveryAction::DeleteTemp)
    );
}

#[test]
fn recovery_deletes_temps_and_quarantines_final_objects() {
    let client = ObjectStoreClient::in_memory();
    client
        .put(
            "app/items/.tmp/writer/batch.parquet",
            b"temporary",
            PutPrecondition::Overwrite,
        )
        .unwrap();
    client
        .put(
            "app/items/segment-0009.parquet",
            b"orphan",
            PutPrecondition::Overwrite,
        )
        .unwrap();
    client
        .put(
            "app/items/segment-0001.parquet",
            b"referenced",
            PutPrecondition::Overwrite,
        )
        .unwrap();

    let referenced = HashSet::from(["app/items/segment-0001.parquet".to_string()]);
    let objects = discover_orphan_objects(&client, "app/items", &referenced).unwrap();
    let plan = plan_recovery_actions(objects);
    assert_eq!(plan.actions.len(), 2);
    apply_recovery_plan(&client, &plan).unwrap();

    assert!(matches!(
        client.get("app/items/.tmp/writer/batch.parquet"),
        Err(StorageClientError::NotFound { .. })
    ));
    assert!(matches!(
        client.get("app/items/segment-0009.parquet"),
        Err(StorageClientError::NotFound { .. })
    ));
    assert_eq!(
        client.get("app/items/segment-0001.parquet").unwrap(),
        b"referenced"
    );
    let listed = client.list("app/items").unwrap();
    let quarantine = listed
        .iter()
        .find(|object| {
            object
                .key
                .starts_with("app/items/segment-0009.parquet.quarantine.")
        })
        .unwrap();
    assert_eq!(client.get(&quarantine.key).unwrap(), b"orphan");
}
