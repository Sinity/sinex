use super::*;

fn dir() -> PathBuf {
    static N: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let d = std::env::temp_dir().join(format!(
        "sinex-private-{}-{}",
        std::process::id(),
        N.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[devkit::test]
fn switch_on_and_off() {
    let m = PrivateMode::new(dir().join("private-mode"));
    assert!(!m.on());
    m.set(true).unwrap();
    assert!(m.on());
    m.set(false).unwrap();
    assert!(!m.on());
    m.set(false).unwrap();
}

#[devkit::test]
fn an_uncheckable_switch_means_on() {
    // A directory that cannot be created: its parent is a file.
    let file = dir().join("file");
    std::fs::write(&file, b"").unwrap();
    let m = PrivateMode::new(file.join("sub").join("private-mode"));
    assert!(m.on(), "a switch that cannot be checked counts as on");
    let State::Unchecked(reason) = m.state() else {
        panic!("expected an uncheckable switch, got {:?}", m.state());
    };
    assert!(reason.contains("Not a directory"), "{reason}");
}

#[devkit::test]
fn an_uncheckable_switch_is_reported_once_and_its_recovery_once() {
    let path = dir().join("reported").join("private-mode");
    let unchecked = State::Unchecked("cannot read: denied".into());
    let first = report(&path, &unchecked).expect("the first check is reported");
    assert!(
        first.contains("cannot be checked (cannot read: denied)"),
        "{first}"
    );
    assert_eq!(
        report(&path, &unchecked),
        None,
        "the same reason is not repeated"
    );
    let other = State::Unchecked("cannot create: read-only".into());
    assert!(report(&path, &other).is_some(), "a new reason is reported");
    assert_eq!(
        report(&path, &State::On),
        Some(format!(
            "private-mode switch {} can be checked again; capture follows it",
            path.display()
        ))
    );
    assert_eq!(report(&path, &State::Off), None);
}

#[devkit::test]
fn cached_rechecks_after_the_interval() {
    let m = PrivateMode::new(dir().join("private-mode"));
    let mut c = Cached::new(m.clone(), Duration::from_millis(0));
    assert!(!c.on());
    m.set(true).unwrap();
    assert!(c.on());
}
