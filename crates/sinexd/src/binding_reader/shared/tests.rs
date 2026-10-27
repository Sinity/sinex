use super::*;

#[devkit::test]
fn interruptible_reads_share_private_mode_checks_and_still_honor_cancellation() {
    let dir = tempfile::tempdir().unwrap();
    let mode = sinex_transport::private_mode::PrivateMode::new(dir.path().join("private"));
    let private = Arc::new(sinex_transport::private_mode::Cached::new(
        mode.clone(),
        std::time::Duration::from_secs(60),
    ));
    let stop = Arc::new(AtomicBool::new(false));
    let mut first = InterruptibleReader {
        input: std::io::repeat(1),
        stop: stop.clone(),
        private: private.clone(),
    };
    let mut second = InterruptibleReader {
        input: std::io::repeat(2),
        stop: stop.clone(),
        private,
    };
    let mut byte = [0];
    first.read_exact(&mut byte).unwrap();
    mode.set(true).unwrap();
    // Verification and reading share the cached decision within its interval.
    second.read_exact(&mut byte).unwrap();
    assert_eq!(byte, [2]);
    stop.store(true, Ordering::Relaxed);
    assert!(first.read(&mut byte).is_err());
    assert!(second.read(&mut byte).is_err());
}
