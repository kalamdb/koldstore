use std::time::Duration;

use pg_koldstore::flush::policy::{FlushPolicy, FlushPolicyError};

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
