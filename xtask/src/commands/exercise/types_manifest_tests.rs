use super::*;
use crate::sandbox::sinex_test;

fn make_manifest(entries: &[(&str, &str, bool)]) -> QaManifest {
    QaManifest {
        schema_version: QaManifest::SCHEMA_VERSION,
        exercises: entries
            .iter()
            .map(|(id, tier, passed)| QaManifestEntry {
                id: id.to_string(),
                tier: tier.to_string(),
                passed: *passed,
            })
            .collect(),
    }
}

#[sinex_test]
async fn no_regressions_when_identical() -> ::xtask::sandbox::TestResult<()> {
    let baseline = make_manifest(&[("t1.a", "T1", true), ("t1.b", "T1", true)]);
    let current = make_manifest(&[("t1.a", "T1", true), ("t1.b", "T1", true)]);
    assert!(current.regressions(&baseline).is_empty());
    Ok(())
}

#[sinex_test]
async fn regression_detected_when_passing_becomes_failing() -> ::xtask::sandbox::TestResult<()>
{
    let baseline = make_manifest(&[("t1.a", "T1", true), ("t1.b", "T1", true)]);
    let current = make_manifest(&[("t1.a", "T1", false), ("t1.b", "T1", true)]);
    let regressions = current.regressions(&baseline);
    assert_eq!(regressions, vec!["t1.a"]);
    Ok(())
}

#[sinex_test]
async fn no_regression_when_failing_stays_failing() -> ::xtask::sandbox::TestResult<()> {
    // Exercise was already failing in baseline — not a new regression.
    let baseline = make_manifest(&[("t1.a", "T1", false)]);
    let current = make_manifest(&[("t1.a", "T1", false)]);
    assert!(current.regressions(&baseline).is_empty());
    Ok(())
}

#[sinex_test]
async fn regression_when_exercise_disappears_from_run() -> ::xtask::sandbox::TestResult<()> {
    // Exercise was passing in baseline but is absent from current run.
    let baseline = make_manifest(&[("t1.a", "T1", true), ("t1.b", "T1", true)]);
    let current = make_manifest(&[("t1.a", "T1", true)]); // t1.b missing
    let regressions = current.regressions(&baseline);
    assert_eq!(regressions, vec!["t1.b"]);
    Ok(())
}

#[sinex_test]
async fn new_pass_detected() -> ::xtask::sandbox::TestResult<()> {
    let baseline = make_manifest(&[("t1.a", "T1", false)]);
    let current = make_manifest(&[("t1.a", "T1", true)]);
    let passes = current.new_passes(&baseline);
    assert_eq!(passes, vec!["t1.a"]);
    Ok(())
}

#[sinex_test]
async fn no_new_pass_when_already_passing_in_baseline() -> ::xtask::sandbox::TestResult<()> {
    let baseline = make_manifest(&[("t1.a", "T1", true)]);
    let current = make_manifest(&[("t1.a", "T1", true)]);
    assert!(current.new_passes(&baseline).is_empty());
    Ok(())
}

#[sinex_test]
async fn from_report_produces_correct_manifest() -> ::xtask::sandbox::TestResult<()> {
    let report = ExerciseReport {
        status: "partial".to_string(),
        total: 2,
        passed: 1,
        failed: 1,
        skipped: 0,
        duration_secs: 2.0,
        output_dir: "/tmp".to_string(),
        results: vec![
            ReportEntry {
                id: "t1.foo".to_string(),
                tier: "T1".to_string(),
                passed: true,
                duration_secs: 1.0,
                error: None,
                steps: vec![],
            },
            ReportEntry {
                id: "t1.bar".to_string(),
                tier: "T1".to_string(),
                passed: false,
                duration_secs: 1.0,
                error: Some("broke".to_string()),
                steps: vec![],
            },
        ],
    };
    let manifest = QaManifest::from_report(&report);
    assert_eq!(manifest.schema_version, QaManifest::SCHEMA_VERSION);
    assert_eq!(manifest.exercises.len(), 2);
    assert!(manifest.exercises[0].passed);
    assert!(!manifest.exercises[1].passed);
    Ok(())
}

#[sinex_test]
async fn manifest_roundtrips_json() -> ::xtask::sandbox::TestResult<()> {
    let manifest = make_manifest(&[("t1.x", "T1", true), ("t2.y", "T2", false)]);
    let json = serde_json::to_string(&manifest).unwrap();
    let roundtripped: QaManifest = serde_json::from_str(&json).unwrap();
    assert_eq!(roundtripped.exercises.len(), 2);
    assert_eq!(roundtripped.exercises[0].id, "t1.x");
    assert!(roundtripped.exercises[0].passed);
    assert_eq!(roundtripped.exercises[1].id, "t2.y");
    assert!(!roundtripped.exercises[1].passed);
    Ok(())
}
