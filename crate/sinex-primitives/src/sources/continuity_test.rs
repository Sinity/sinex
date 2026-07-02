use super::*;
use xtask::sandbox::prelude::sinex_test;

#[sinex_test]
async fn declared_coverage_contract_default_is_unknown() -> xtask::sandbox::TestResult<()> {
    let c = DeclaredCoverageContract::default();
    assert!(c.is_unknown());
    assert_eq!(c.kind, DeclaredCoverageContractKind::Unknown);
    assert!(c.expected_event_types.is_empty());
    assert!(c.declared_at.is_none());
    Ok(())
}

#[sinex_test]
async fn declared_coverage_contract_kind_strings_match_check_set()
-> xtask::sandbox::TestResult<()> {
    for kind in [
        DeclaredCoverageContractKind::Continuous,
        DeclaredCoverageContractKind::PeriodicDump,
        DeclaredCoverageContractKind::OpportunisticImport,
        DeclaredCoverageContractKind::FiniteOneShot,
        DeclaredCoverageContractKind::EphemeralStream,
        DeclaredCoverageContractKind::Unknown,
    ] {
        assert!(
            DeclaredCoverageContractKind::ALL.contains(&kind.as_str()),
            "kind {} missing from ALL",
            kind.as_str()
        );
    }
    assert_eq!(DeclaredCoverageContractKind::ALL.len(), 6);
    Ok(())
}

#[sinex_test]
async fn declared_coverage_contract_serializes_kind_pascal_case()
-> xtask::sandbox::TestResult<()> {
    let c = DeclaredCoverageContract {
        kind: DeclaredCoverageContractKind::PeriodicDump,
        ..Default::default()
    };
    let json = serde_json::to_value(&c).unwrap();
    assert_eq!(
        json["kind"],
        serde_json::Value::String("PeriodicDump".into())
    );
    Ok(())
}

#[sinex_test]
async fn privacy_class_round_trip_str() -> xtask::sandbox::TestResult<()> {
    for s in PrivacyClass::ALL {
        let parsed: PrivacyClass = s.parse().expect("parse known class");
        assert_eq!(parsed.as_str(), *s);
    }
    assert!("nope".parse::<PrivacyClass>().is_err());
    Ok(())
}

#[sinex_test]
async fn privacy_class_default_is_unknown() -> xtask::sandbox::TestResult<()> {
    assert_eq!(PrivacyClass::default(), PrivacyClass::Unknown);
    assert!(PrivacyClass::default().is_unknown());
    assert!(!PrivacyClass::default().is_private());
    Ok(())
}

#[sinex_test]
async fn privacy_class_is_private_excludes_public_and_unknown() -> xtask::sandbox::TestResult<()>
{
    assert!(!PrivacyClass::Public.is_private());
    assert!(!PrivacyClass::Unknown.is_private());
    assert!(PrivacyClass::Personal.is_private());
    assert!(PrivacyClass::Secret.is_private());
    assert!(PrivacyClass::Redacted.is_private());
    Ok(())
}

#[sinex_test]
async fn replayability_green_count() -> xtask::sandbox::TestResult<()> {
    let r = Replayability {
        raw_bytes_preserved: true,
        timing_quality: true,
        anchor_stability: false,
        parser_determinism: true,
        privacy_safe_replay: false,
        weak_points: vec!["anchor moves on re-export".into()],
    };
    assert_eq!(r.green_count(), 3);
    Ok(())
}

#[sinex_test]
async fn report_serializes_with_seam_kind_snake_case() -> xtask::sandbox::TestResult<()> {
    let report = SourceContinuityReport {
        source_family: SourceFamily::from_static("terminal"),
        coverage_contract: CoverageContract::Continuous,
        is_declared: true,
        replayability: Replayability {
            raw_bytes_preserved: true,
            timing_quality: true,
            anchor_stability: true,
            parser_determinism: true,
            privacy_safe_replay: true,
            weak_points: Vec::new(),
        },
        seams: vec![TemporalSeam {
            kind: SeamKind::ExpectedContinuation,
            before_ts: None,
            after_ts: None,
            evidence: serde_json::Value::Null,
        }],
        gaps: Vec::new(),
        earliest_ts: None,
        latest_ts: None,
        material_count: 0,
        event_count: 0,
    };
    let json = serde_json::to_value(&report).unwrap();
    assert_eq!(
        json["seams"][0]["kind"],
        serde_json::Value::String("expected_continuation".into())
    );
    assert_eq!(
        json["coverage_contract"],
        serde_json::Value::String("continuous".into())
    );
    Ok(())
}
