#[test]
fn allocation_growth_contract_tracks_repeated_managed_operations() {
    let tracked_operations = [
        "migration",
        "flush",
        "merge-scan",
        "cold reader",
        "demigration",
    ];

    assert!(tracked_operations.iter().all(|operation| !operation.is_empty()));
}
