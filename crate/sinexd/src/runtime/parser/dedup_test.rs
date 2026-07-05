use super::*;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn contains_returns_false_before_observe() -> TestResult<()> {
    let window = ContentHashWindow::default();
    assert!(!window.contains(b"line one"));
    Ok(())
}

#[sinex_test]
async fn observe_then_contains_returns_true() -> TestResult<()> {
    let mut window = ContentHashWindow::with_capacity(4);
    window.observe(b"line one");
    assert!(window.contains(b"line one"));
    assert!(!window.contains(b"line two"));
    Ok(())
}

#[sinex_test]
async fn observe_evicts_oldest_when_at_capacity() -> TestResult<()> {
    let mut window = ContentHashWindow::with_capacity(2);
    window.observe(b"a");
    window.observe(b"b");
    window.observe(b"c"); // evicts "a"
    assert!(!window.contains(b"a"));
    assert!(window.contains(b"b"));
    assert!(window.contains(b"c"));
    assert_eq!(window.len(), 2);
    Ok(())
}

#[sinex_test]
async fn capacity_zero_disables_dedup() -> TestResult<()> {
    let mut window = ContentHashWindow::with_capacity(0);
    window.observe(b"x");
    assert!(!window.contains(b"x"));
    assert!(window.is_empty());
    Ok(())
}

#[sinex_test]
async fn observe_is_idempotent_for_duplicates() -> TestResult<()> {
    let mut window = ContentHashWindow::with_capacity(4);
    window.observe(b"dup");
    window.observe(b"dup");
    window.observe(b"dup");
    assert_eq!(
        window.len(),
        1,
        "duplicate observations should not grow the window"
    );
    Ok(())
}

#[sinex_test]
async fn clear_drops_all_entries() -> TestResult<()> {
    let mut window = ContentHashWindow::with_capacity(4);
    window.observe(b"a");
    window.observe(b"b");
    window.clear();
    assert!(window.is_empty());
    assert!(!window.contains(b"a"));
    Ok(())
}

#[sinex_test]
async fn snapshot_round_trips_recent_hashes() -> TestResult<()> {
    let mut window = ContentHashWindow::with_capacity(3);
    window.observe(b"a");
    window.observe(b"b");
    window.observe(b"c");
    window.observe(b"d");

    let restored = ContentHashWindow::from_snapshot(window.snapshot())?;

    assert!(!restored.contains(b"a"));
    assert!(restored.contains(b"b"));
    assert!(restored.contains(b"c"));
    assert!(restored.contains(b"d"));
    assert_eq!(restored.len(), 3);
    Ok(())
}
