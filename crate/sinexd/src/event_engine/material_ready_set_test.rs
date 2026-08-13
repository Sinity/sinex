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

#[sinex_test]
async fn refreshing_ready_material_reschedules_expiration() -> TestResult<()> {
    let set = MaterialReadySet::with_policy(Duration::from_millis(20), 1);
    let material_id = Uuid::now_v7();

    set.mark_ready(material_id);
    std::thread::sleep(Duration::from_millis(10));
    set.mark_ready(material_id);
    std::thread::sleep(Duration::from_millis(15));

    assert!(set.is_ready(&material_id));
    assert_eq!(set.purge_stale(), 0);

    std::thread::sleep(Duration::from_millis(10));
    assert_eq!(set.purge_stale(), 1);
    assert!(set.is_empty());
    Ok(())
}

#[test]
fn scale_probe_reports_bounded_cardinality_and_purge_cost() {
    let mut reports = Vec::new();
    for cardinality in [10_000_u64, 50_000, 100_000] {
        let set = MaterialReadySet::with_policy(Duration::from_hours(1), 1);
        let started = Instant::now();
        for index in 0..cardinality {
            set.mark_ready(Uuid::from_u128(index as u128 + 1));
        }
        let insert_wall_ns = started.elapsed().as_nanos();
        let insert_metrics = set.metrics_snapshot();

        let expiring = MaterialReadySet::with_policy(Duration::ZERO, 1);
        for index in 0..cardinality {
            expiring.mark_ready(Uuid::from_u128(index as u128 + 1));
        }
        let removed = expiring.purge_stale();
        let purge_metrics = expiring.metrics_snapshot();

        assert_eq!(set.len() as u64, cardinality);
        assert_eq!(insert_metrics.peak_len, cardinality);
        assert_eq!(removed as u64, cardinality);
        assert_eq!(purge_metrics.current_len, 0);
        reports.push(serde_json::json!({
            "cardinality": cardinality,
            "insert_wall_ns": insert_wall_ns,
            "insert_metrics": insert_metrics,
            "purge_metrics": purge_metrics,
        }));
    }
    println!(
        "material_ready_set_scale {}",
        serde_json::to_string(&reports).expect("scale report serializes")
    );
}
