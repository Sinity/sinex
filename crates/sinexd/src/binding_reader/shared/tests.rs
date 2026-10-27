use super::*;

#[devkit::test]
fn checked_reads_share_private_mode_checks_and_still_honor_cancellation() {
    let dir = tempfile::tempdir().unwrap();
    let mode = sinex_transport::private_mode::PrivateMode::new(dir.path().join("private"));
    let private = Arc::new(std::sync::Mutex::new(
        sinex_transport::private_mode::Cached::new(
            mode.clone(),
            std::time::Duration::from_secs(60),
        ),
    ));
    let stop = Arc::new(AtomicBool::new(false));
    let mut first = Checked {
        input: std::io::repeat(1),
        stop: stop.clone(),
        private: private.clone(),
    };
    let mut second = Checked {
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

#[devkit::test]
#[ignore = "10,000-read private-mode measurement"]
fn measure_private_mode_reads() {
    let dir = tempfile::tempdir().unwrap();
    let mode = sinex_transport::private_mode::PrivateMode::new(dir.path().join("private"));
    let started = std::time::Instant::now();
    for _ in 0..10_000 {
        assert!(!mode.on());
    }
    let before = started.elapsed();
    let private = Arc::new(std::sync::Mutex::new(
        sinex_transport::private_mode::Cached::new(mode, std::time::Duration::from_millis(250)),
    ));
    let mut input = Checked {
        input: std::io::repeat(1),
        stop: Arc::new(AtomicBool::new(false)),
        private,
    };
    let started = std::time::Instant::now();
    let mut byte = [0];
    for _ in 0..10_000 {
        input.read_exact(&mut byte).unwrap();
    }
    eprintln!(
        "private-mode reads=10000 before={before:?} after={:?}",
        started.elapsed()
    );
}
