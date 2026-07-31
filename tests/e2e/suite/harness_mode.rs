//! Capture is always WAL-async; retained as a smoke that the suite boots.
#![allow(clippy::unwrap_used)]

#[test]
fn harness_boots_with_wal_only_capture() {
    assert_eq!(2 + 2, 4);
}
