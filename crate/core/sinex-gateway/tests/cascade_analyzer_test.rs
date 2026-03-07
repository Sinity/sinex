use std::collections::HashMap;

use sinex_gateway::{CascadeAnalysis, IntegrityViolation, Severity, ViolationType};
use uuid::Uuid;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn cascade_analysis_structure_holds_basic_invariants() -> color_eyre::Result<()> {
    let analysis = CascadeAnalysis {
        max_depth: 5,
        depth_histogram: HashMap::from([(0, 10), (1, 20), (2, 15)]),
        integrity_violations: vec![],
        total_affected: 45,
        circular_dependencies: vec![],
        memory_estimate: 11_520,
    };

    assert_eq!(analysis.max_depth, 5);
    assert_eq!(analysis.total_affected, 45);
    assert_eq!(analysis.depth_histogram.get(&1), Some(&20));

    Ok(())
}

#[sinex_test]
async fn violation_type_and_severity_round_trip() -> color_eyre::Result<()> {
    let violation = IntegrityViolation {
        archived_event_id: Uuid::now_v7(),
        live_event_id: Uuid::now_v7(),
        violation_type: ViolationType::LiveToArchived,
        severity: Severity::Critical,
    };

    assert!(matches!(
        violation.violation_type,
        ViolationType::LiveToArchived
    ));
    assert!(matches!(violation.severity, Severity::Critical));

    Ok(())
}
