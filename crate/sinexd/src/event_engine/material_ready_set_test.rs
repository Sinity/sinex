use xtask::sandbox::sinex_test;
// Inline because testing TTL eviction cleanly needs access to the internal policy constructor.
use super::*;

#[sinex_test]
async fn stale_entries_are_evicted() -> TestResult<()> {
    let set = MaterialReadySet::with_policy(Duration::from_millis(1), 1);
    let material_id = Uuid::now_v7();

    set.mark_ready(material_id);
    std::thread::sleep(Duration::from_millis(5));

    assert!(!set.is_ready(&material_id));
    assert!(set.is_empty());
    Ok(())
}

#[sinex_test]
async fn purge_stale_removes_idle_entries_without_lookup() -> TestResult<()> {
    let set = MaterialReadySet::with_policy(Duration::from_millis(1), u64::MAX);
    let material_id = Uuid::now_v7();

    set.mark_ready(material_id);
    std::thread::sleep(Duration::from_millis(5));

    assert_eq!(set.purge_stale(), 1);
    assert!(set.is_empty());
    Ok(())
}
