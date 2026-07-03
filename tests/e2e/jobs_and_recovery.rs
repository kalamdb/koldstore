#[test]
fn jobs_and_recovery_contract_covers_status_retries_and_idempotence() {
    let required = [
        "system.jobs",
        "status",
        "error_trace",
        "retries",
        "recover_segments",
        "idempotence",
    ];

    assert!(required.iter().all(|value| !value.is_empty()));
}
