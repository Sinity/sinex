use super::*;
use crate::sandbox::sinex_test;

#[sinex_test]
async fn test_criticality_from_score() -> TestResult<()> {
    assert_eq!(Criticality::from_score(0.9), Criticality::Critical);
    assert_eq!(Criticality::from_score(0.6), Criticality::High);
    assert_eq!(Criticality::from_score(0.3), Criticality::Medium);
    assert_eq!(Criticality::from_score(0.1), Criticality::Low);
    Ok(())
}

#[sinex_test]
async fn test_impact_metrics_new() -> TestResult<()> {
    // 50 dependents out of 80 total → criticality 0.625 → High.
    let metrics = ImpactMetrics::new("test-pkg".to_string(), 50, 10, 80);
    assert_eq!(metrics.package, "test-pkg");
    assert_eq!(metrics.dependent_count, 50);
    assert_eq!(metrics.dependency_count, 10);
    assert_eq!(metrics.criticality_level(), Criticality::High);
    Ok(())
}

#[sinex_test]
async fn test_impact_metrics_new_zero_total() -> TestResult<()> {
    let metrics = ImpactMetrics::new("orphan".to_string(), 0, 0, 0);
    assert_eq!(metrics.criticality, 0.0);
    Ok(())
}
