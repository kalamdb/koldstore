use crate::common;

#[test]
fn capture_mode_accepts_the_runner_values() {
    assert_eq!(
        common::MirrorCaptureMode::parse("strict").unwrap().as_str(),
        "strict"
    );
    assert_eq!(
        common::MirrorCaptureMode::parse("async").unwrap().as_str(),
        "async"
    );
    assert!(common::MirrorCaptureMode::parse("unsupported").is_err());
}
