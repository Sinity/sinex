use super::*;

#[test]
fn missing_or_invalid_source_times_never_become_zero() {
    assert_eq!(
        source_wall_seconds(&json!({"sources": {"a": {"wall_s": 2.0}, "b": {"wall_s": 3.0}}})),
        Some(5.0)
    );
    for value in [
        json!({}),
        json!({"sources": {}}),
        json!({"sources": {"a": {}}}),
        json!({"sources": {"a": {"wall_s": 2.0}, "b": {}}}),
        json!({"sources": {"a": {"wall_s": -1.0}}}),
    ] {
        assert_eq!(source_wall_seconds(&value), None);
    }
}

#[test]
fn an_unavailable_component_leaves_the_total_unknown() {
    let known = bench::linear_estimate(Some(2.0), &std::collections::BTreeMap::new(), Some(10.0));
    let unknown = bench::linear_estimate(None, &std::collections::BTreeMap::new(), Some(10.0));
    let result = combined_estimate(&[known.clone(), known.clone()]);
    assert_eq!(result["estimated_seconds"], 40.0);
    assert_eq!(result["calibrated"], false);
    assert!(combined_estimate(&[known, unknown])["estimated_seconds"].is_null());
    assert!(combined_estimate(&[])["estimated_seconds"].is_null());
}
