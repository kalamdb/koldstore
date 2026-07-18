use koldstore_flush::{cleanup, job, worker};
use uuid::Uuid;

#[test]
fn hot_cleanup_waits_for_manifest_commit_and_retains_needed_tombstones() {
    let before_commit = cleanup::plan_hot_cleanup(false, true);
    let after_commit_with_cold_pk = cleanup::plan_hot_cleanup(true, true);
    let after_commit_without_cold_pk = cleanup::plan_hot_cleanup(true, false);

    assert!(!before_commit.remove_live_hot_rows);
    assert!(before_commit.retain_tombstone);

    assert!(after_commit_with_cold_pk.remove_live_hot_rows);
    assert!(after_commit_with_cold_pk.retain_tombstone);

    assert!(after_commit_without_cold_pk.remove_live_hot_rows);
    assert!(!after_commit_without_cold_pk.retain_tombstone);
}

#[test]
fn flush_worker_registration_requires_shared_preload_for_launcher() {
    assert!(worker::requires_shared_preload());
}

#[test]
fn zero_sized_flush_batch_does_not_request_another_scan() {
    assert!(!job::should_continue_batch(0, 0));
    assert!(!job::should_continue_batch(10, 0));
}

#[test]
fn bounded_flush_batch_builder_stops_before_row_or_memory_limits() {
    use koldstore_common::{CommitSeq, SeqId, StablePkHash};
    use koldstore_flush::job::{
        FlushBatchBuilder, FlushBatchPush, FlushExecutionConfig, HotRowCandidate,
    };

    let config = FlushExecutionConfig::new(2, 128, 2).unwrap();
    let mut builder = FlushBatchBuilder::new(config);

    assert_eq!(
        builder.push(
            HotRowCandidate::live(
                StablePkHash::from_hex("01").unwrap(),
                SeqId::new(1).unwrap(),
                CommitSeq::new(11).unwrap(),
            ),
            64,
        ),
        FlushBatchPush::Accepted
    );
    assert_eq!(
        builder.push(
            HotRowCandidate::live(
                StablePkHash::from_hex("02").unwrap(),
                SeqId::new(2).unwrap(),
                CommitSeq::new(12).unwrap(),
            ),
            64,
        ),
        FlushBatchPush::Accepted
    );
    assert_eq!(
        builder.push(
            HotRowCandidate::live(
                StablePkHash::from_hex("03").unwrap(),
                SeqId::new(3).unwrap(),
                CommitSeq::new(13).unwrap(),
            ),
            1,
        ),
        FlushBatchPush::Full
    );

    let input = builder.finish();
    assert_eq!(input.batch_size, 2);
    assert_eq!(input.rows.len(), 2);

    let mut byte_limited = FlushBatchBuilder::new(FlushExecutionConfig::new(10, 96, 2).unwrap());
    assert_eq!(
        byte_limited.push(
            HotRowCandidate::live(
                StablePkHash::from_hex("04").unwrap(),
                SeqId::new(4).unwrap(),
                CommitSeq::new(14).unwrap(),
            ),
            80,
        ),
        FlushBatchPush::Accepted
    );
    assert_eq!(
        byte_limited.push(
            HotRowCandidate::live(
                StablePkHash::from_hex("05").unwrap(),
                SeqId::new(5).unwrap(),
                CommitSeq::new(15).unwrap(),
            ),
            32,
        ),
        FlushBatchPush::Full
    );
}

#[test]
fn flush_stats_use_latest_live_values_and_ignore_tombstones() {
    use koldstore_common::{CommitSeq, SeqId, StablePkHash};
    use koldstore_flush::job::{FlushBatchInput, HotRowCandidate};
    use serde_json::json;

    let pk_one = StablePkHash::from_hex("01").unwrap();
    let batch = FlushBatchInput {
        batch_size: 10,
        rows: vec![
            HotRowCandidate::live(
                pk_one.clone(),
                SeqId::new(1).unwrap(),
                CommitSeq::new(11).unwrap(),
            )
            .with_column_values([("score", json!(10)), ("status", json!("old"))]),
            HotRowCandidate::live(pk_one, SeqId::new(3).unwrap(), CommitSeq::new(13).unwrap())
                .with_column_values([("score", json!(30)), ("status", json!("new"))]),
            HotRowCandidate::tombstone(
                StablePkHash::from_hex("02").unwrap(),
                SeqId::new(4).unwrap(),
                CommitSeq::new(14).unwrap(),
            )
            .with_column_values([("score", json!(999)), ("status", json!("deleted"))]),
        ],
    }
    .plan();

    let stats = batch.segment_column_stats(["score", "status"]);

    assert_eq!(stats["score"].min, json!(30));
    assert_eq!(stats["score"].max, json!(30));
    assert_eq!(stats["status"].min, json!("new"));
    assert_eq!(stats["status"].max, json!("new"));
    assert_eq!(stats["seq"].min, json!(3));
    assert_eq!(stats["seq"].max, json!(3));
}

#[test]
fn flush_stats_omit_incomparable_columns_to_keep_pruning_conservative() {
    use koldstore_common::{CommitSeq, SeqId, StablePkHash};
    use koldstore_flush::job::{FlushBatchInput, HotRowCandidate};
    use serde_json::json;

    let batch = FlushBatchInput {
        batch_size: 10,
        rows: vec![
            HotRowCandidate::live(
                StablePkHash::from_hex("01").unwrap(),
                SeqId::new(1).unwrap(),
                CommitSeq::new(11).unwrap(),
            )
            .with_column_values([("mixed", json!(10))]),
            HotRowCandidate::live(
                StablePkHash::from_hex("02").unwrap(),
                SeqId::new(2).unwrap(),
                CommitSeq::new(12).unwrap(),
            )
            .with_column_values([("mixed", json!("ten"))]),
        ],
    }
    .plan();

    let stats = batch.column_stats(["mixed"]);

    assert!(!stats.contains_key("mixed"));
}

#[test]
fn flush_watermark_skips_rows_changed_after_claimed_seqid() {
    use koldstore_common::{CommitSeq, SeqId, StablePkHash};
    use koldstore_flush::job::{conditional_cleanup_allowed, FlushWatermark, HotRowCandidate};

    let watermark = FlushWatermark::new(SeqId::new(10).unwrap());
    let included = HotRowCandidate::live(
        StablePkHash::from_hex("10").unwrap(),
        SeqId::new(10).unwrap(),
        CommitSeq::new(110).unwrap(),
    );
    let changed_after_claim = HotRowCandidate::live(
        StablePkHash::from_hex("11").unwrap(),
        SeqId::new(11).unwrap(),
        CommitSeq::new(111).unwrap(),
    );

    assert!(watermark.includes(&included));
    assert!(!watermark.includes(&changed_after_claim));
    assert!(conditional_cleanup_allowed(
        &included,
        SeqId::new(10).unwrap(),
        CommitSeq::new(110).unwrap(),
        watermark,
    ));
    assert!(!conditional_cleanup_allowed(
        &included,
        SeqId::new(11).unwrap(),
        CommitSeq::new(111).unwrap(),
        watermark,
    ));
}

#[test]
fn mirror_flush_selection_set_persists_selected_keys_and_seq_cutoff() {
    use koldstore_common::{MirrorOperation, SeqId};
    use koldstore_flush::job::{MirrorFlushSelectedRow, MirrorFlushSelectionSet};
    use serde_json::json;

    let selection = MirrorFlushSelectionSet::new(vec![
        MirrorFlushSelectedRow {
            pk_json: json!({"id": 2}),
            seq: SeqId::new(20).unwrap(),
            operation: MirrorOperation::Delete,
        },
        MirrorFlushSelectedRow {
            pk_json: json!({"id": 1}),
            seq: SeqId::new(10).unwrap(),
            operation: MirrorOperation::Update,
        },
    ]);

    assert_eq!(selection.seq_cutoff.unwrap().get(), 20);
    assert_eq!(
        selection
            .rows
            .iter()
            .map(|row| row.seq.get())
            .collect::<Vec<_>>(),
        vec![10, 20]
    );
    assert_eq!(
        selection.to_payload_json(),
        json!([
            {"id": 1, "seq": 10, "op": 2},
            {"id": 2, "seq": 20, "op": 3}
        ])
    );
}
