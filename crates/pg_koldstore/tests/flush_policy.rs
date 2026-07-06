use std::time::{Duration, SystemTime};

use koldstore_common::SeqId;
use koldstore_flush::policy::{
    select_mirror_flush_candidates, FlushPolicy, FlushPolicyError, MirrorPolicyRow,
};

#[test]
fn parses_rows_policy_as_default_hot_row_limit() {
    let policy = FlushPolicy::parse("rows:1000").unwrap();

    assert_eq!(
        policy,
        FlushPolicy {
            row_limit: Some(1000),
            duration: None,
        }
    );
}

#[test]
fn parses_duration_policy_with_day_and_second_units() {
    let one_day = FlushPolicy::parse("duration:1d").unwrap();
    let five_days = FlushPolicy::parse("duration:5d").unwrap();
    let seconds = FlushPolicy::parse("duration:3600").unwrap();

    assert_eq!(one_day.duration, Some(Duration::from_secs(86_400)));
    assert_eq!(five_days.duration, Some(Duration::from_secs(432_000)));
    assert_eq!(seconds.duration, Some(Duration::from_secs(3_600)));
}

#[test]
fn parses_interval_as_duration_alias_not_last_flush_timer() {
    let policy = FlushPolicy::parse("interval:86400").unwrap();

    assert_eq!(policy.duration, Some(Duration::from_secs(86_400)));
    assert_eq!(policy.row_limit, None);
}

#[test]
fn parses_combined_rows_and_duration_policy() {
    let policy = FlushPolicy::parse("rows:1000,duration:1d").unwrap();

    assert_eq!(policy.row_limit, Some(1000));
    assert_eq!(policy.duration, Some(Duration::from_secs(86_400)));
}

#[test]
fn rejects_blank_unknown_or_zero_policy_values() {
    assert_eq!(FlushPolicy::parse(""), Err(FlushPolicyError::Blank));
    assert!(matches!(
        FlushPolicy::parse("cron:daily"),
        Err(FlushPolicyError::UnknownKey(_))
    ));
    assert_eq!(
        FlushPolicy::parse("rows:0"),
        Err(FlushPolicyError::InvalidNumber {
            key: "rows".to_string(),
            value: "0".to_string(),
        })
    );
    assert_eq!(
        FlushPolicy::parse("duration:0d"),
        Err(FlushPolicyError::InvalidDuration {
            value: "0d".to_string(),
        })
    );
}

#[test]
fn row_limit_policy_selects_oldest_excess_mirror_rows_by_seq() {
    let rows = (1..=5)
        .rev()
        .map(|seq| MirrorPolicyRow {
            pk_json: serde_json::json!({ "id": seq }),
            seq: SeqId::new(seq).unwrap(),
            changed_at: SystemTime::UNIX_EPOCH + Duration::from_secs(seq as u64),
        })
        .collect::<Vec<_>>();

    let selected = select_mirror_flush_candidates(
        &FlushPolicy {
            row_limit: Some(3),
            duration: None,
        },
        &rows,
        SystemTime::UNIX_EPOCH + Duration::from_secs(10),
    );

    assert_eq!(
        selected.iter().map(|row| row.seq.get()).collect::<Vec<_>>(),
        vec![1, 2]
    );
}

#[test]
fn duration_policy_selects_rows_older_than_threshold() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100_000);
    let rows = vec![
        MirrorPolicyRow {
            pk_json: serde_json::json!({ "id": 1 }),
            seq: SeqId::new(1).unwrap(),
            changed_at: now - Duration::from_secs(90_000),
        },
        MirrorPolicyRow {
            pk_json: serde_json::json!({ "id": 2 }),
            seq: SeqId::new(2).unwrap(),
            changed_at: now - Duration::from_secs(60),
        },
    ];

    let selected = select_mirror_flush_candidates(
        &FlushPolicy {
            row_limit: None,
            duration: Some(Duration::from_secs(86_400)),
        },
        &rows,
        now,
    );

    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].pk_json, serde_json::json!({ "id": 1 }));
}

#[test]
fn combined_policy_deduplicates_candidates_and_keeps_stable_seq_order() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100_000);
    let rows = vec![
        MirrorPolicyRow {
            pk_json: serde_json::json!({ "id": 3 }),
            seq: SeqId::new(3).unwrap(),
            changed_at: now - Duration::from_secs(60),
        },
        MirrorPolicyRow {
            pk_json: serde_json::json!({ "id": 1 }),
            seq: SeqId::new(1).unwrap(),
            changed_at: now - Duration::from_secs(90_000),
        },
        MirrorPolicyRow {
            pk_json: serde_json::json!({ "id": 2 }),
            seq: SeqId::new(2).unwrap(),
            changed_at: now - Duration::from_secs(90_000),
        },
    ];

    let selected = select_mirror_flush_candidates(
        &FlushPolicy {
            row_limit: Some(1),
            duration: Some(Duration::from_secs(86_400)),
        },
        &rows,
        now,
    );

    assert_eq!(
        selected.iter().map(|row| row.seq.get()).collect::<Vec<_>>(),
        vec![1, 2]
    );
}
