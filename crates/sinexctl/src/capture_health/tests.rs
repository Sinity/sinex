use super::*;

#[devkit::test]
fn status_includes_the_local_capture_report_and_its_observation_time() {
    let root = std::env::temp_dir().join(format!("capture-health-{}", std::process::id()));
    std::fs::create_dir_all(root.join("positions")).unwrap();
    let report = json!({"observed_at":"2026-01-01T00:00:00Z", "spools":[{"binding":"synthetic", "unpublished":{"bytes":42,"frames":2}}]});
    std::fs::write(
        root.join("positions/health.json"),
        serde_json::to_vec(&report).unwrap(),
    )
    .unwrap();
    let mut status = json!({"writer":true});
    attach(&mut status, root.to_str());
    assert_eq!(status["local_capture"], report);
    std::fs::remove_dir_all(root).unwrap();
}
