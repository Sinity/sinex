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

#[test]
fn switch_on_and_off() {
    let m = PrivateMode::new(dir().join("private-mode"));
    assert!(!m.on());
    m.set(true).unwrap();
    assert!(m.on());
    m.set(false).unwrap();
    assert!(!m.on());
    m.set(false).unwrap();
}

#[test]
fn an_uncheckable_switch_means_on() {
    // A directory that cannot be created: its parent is a file.
    let file = dir().join("file");
    std::fs::write(&file, b"").unwrap();
    let m = PrivateMode::new(file.join("sub").join("private-mode"));
    assert!(m.on(), "a switch that cannot be checked counts as on");
}

#[test]
fn cached_rechecks_after_the_interval() {
    let m = PrivateMode::new(dir().join("private-mode"));
    let mut c = Cached::new(m.clone(), Duration::from_millis(0));
    assert!(!c.on());
    m.set(true).unwrap();
    assert!(c.on());
}
